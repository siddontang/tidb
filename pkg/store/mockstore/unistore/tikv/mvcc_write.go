// Copyright 2019-present PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"slices"
	"sync/atomic"
	"time"

	"github.com/pingcap/badger"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/dbreader"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/kverrors"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/mvcc"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

// Prewrite implements the MVCCStore interface.
func (store *MVCCStore) Prewrite(reqCtx *requestCtx, req *kvrpcpb.PrewriteRequest) error {
	mutations := sortPrewrite(req)
	regCtx := reqCtx.regCtx
	hashVals := mutationsToHashVals(mutations)

	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)

	isPessimistic := req.ForUpdateTs > 0
	var err error
	if isPessimistic {
		err = store.prewritePessimistic(reqCtx, mutations, req)
	} else {
		err = store.prewriteOptimistic(reqCtx, mutations, req)
	}
	if err != nil {
		return err
	}

	if reqCtx.onePCCommitTS != 0 {
		// TODO: Is it correct to pass the hashVals directly here, considering that some of the keys may
		// have no pessimistic lock?
		if isPessimistic {
			store.lockWaiterManager.WakeUp(req.StartVersion, reqCtx.onePCCommitTS, hashVals)
			store.DeadlockDetectCli.CleanUp(req.StartVersion)
		}
	}
	return nil
}

func (store *MVCCStore) prewriteOptimistic(reqCtx *requestCtx, mutations []*kvrpcpb.Mutation, req *kvrpcpb.PrewriteRequest) error {
	startTS := req.StartVersion
	// Must check the LockStore first.
	for _, m := range mutations {
		lock, err := store.checkConflictInLockStore(reqCtx, m, startTS)
		if err != nil {
			return err
		}
		if lock != nil {
			// duplicated command
			return nil
		}
		if bytes.Equal(m.Key, req.PrimaryLock) {
			status := store.checkExtraTxnStatus(reqCtx, m.Key, req.StartVersion)
			if status.isRollback {
				return kverrors.ErrAlreadyRollback
			}
			if status.isOpLockCommitted() {
				// duplicated command
				return nil
			}
		}
	}
	items, err := store.getDBItems(reqCtx, mutations)
	if err != nil {
		return err
	}
	for i, m := range mutations {
		item := items[i]
		if item != nil {
			userMeta := mvcc.DBUserMeta(item.UserMeta())
			if userMeta.CommitTS() > startTS {
				return &kverrors.ErrConflict{
					StartTS:          startTS,
					ConflictTS:       userMeta.StartTS(),
					ConflictCommitTS: userMeta.CommitTS(),
					Key:              item.KeyCopy(nil),
					Reason:           kvrpcpb.WriteConflict_Optimistic,
				}
			}
		}
		// Op_CheckNotExists type requests should not add lock
		if m.Op == kvrpcpb.Op_CheckNotExists {
			if item != nil {
				val, err := item.Value()
				if err != nil {
					return err
				}
				if len(val) > 0 {
					return &kverrors.ErrKeyAlreadyExists{Key: m.Key}
				}
			}
			continue
		}
		// TODO add memory lock for async commit protocol.
	}
	return store.prewriteMutations(reqCtx, mutations, req, items)
}

func (store *MVCCStore) prewritePessimistic(reqCtx *requestCtx, mutations []*kvrpcpb.Mutation, req *kvrpcpb.PrewriteRequest) error {
	startTS := req.StartVersion

	expectedForUpdateTSMap := make(map[int]uint64, len(req.GetForUpdateTsConstraints()))
	for _, constraint := range req.GetForUpdateTsConstraints() {
		index := int(constraint.Index)
		if index >= len(mutations) {
			return errors.Errorf("prewrite request invalid: for_update_ts constraint set for index %v while %v mutations were given", index, len(mutations))
		}

		expectedForUpdateTSMap[index] = constraint.ExpectedForUpdateTs
	}

	reader := reqCtx.getDBReader()
	txn := reader.GetTxn()

	for i, m := range mutations {
		if m.Op == kvrpcpb.Op_CheckNotExists {
			return kverrors.ErrInvalidOp{Op: m.Op}
		}
		lock := store.getLock(reqCtx, m.Key)
		isPessimisticLock := len(req.PessimisticActions) > 0 && req.PessimisticActions[i] == kvrpcpb.PrewriteRequest_DO_PESSIMISTIC_CHECK
		needConstraintCheck := len(req.PessimisticActions) > 0 && req.PessimisticActions[i] == kvrpcpb.PrewriteRequest_DO_CONSTRAINT_CHECK
		lockExists := lock != nil
		lockMatch := lockExists && lock.StartTS == startTS
		lockConstraintPasses := true
		if expectedForUpdateTS, ok := expectedForUpdateTSMap[i]; ok {
			if lock.ForUpdateTS != expectedForUpdateTS {
				lockConstraintPasses = false
			}
		}
		if isPessimisticLock {
			valid := lockExists && lockMatch && lockConstraintPasses
			if !valid {
				return errors.New("pessimistic lock not found")
			}
			if lock.Op != uint8(kvrpcpb.Op_PessimisticLock) {
				// Duplicated command.
				return nil
			}
			// Do not overwrite lock ttl if prewrite ttl smaller than pessimisitc lock ttl
			if uint64(lock.TTL) > req.LockTtl {
				req.LockTtl = uint64(lock.TTL)
			}
		} else if needConstraintCheck {
			item, err := txn.Get(m.Key)
			if err != nil && err != badger.ErrKeyNotFound {
				return errors.Trace(err)
			}
			// check conflict
			if item != nil {
				userMeta := mvcc.DBUserMeta(item.UserMeta())
				if userMeta.CommitTS() > startTS {
					return &kverrors.ErrConflict{
						StartTS:          startTS,
						ConflictTS:       userMeta.StartTS(),
						ConflictCommitTS: userMeta.CommitTS(),
						Key:              item.KeyCopy(nil),
						Reason:           kvrpcpb.WriteConflict_LazyUniquenessCheck,
					}
				}
			}
		} else {
			// non pessimistic lock in pessimistic transaction, e.g. non-unique index.
			valid := !lockExists || lockMatch
			if !valid {
				// Safe to set TTL to zero because the transaction of the lock is committed
				// or rollbacked or must be rollbacked.
				lock.TTL = 0
				return kverrors.BuildLockErr(m.Key, lock)
			}
			if lockMatch {
				// Duplicate command.
				return nil
			}
		}
		// TODO add memory lock for async commit protocol.
	}
	items, err := store.getDBItems(reqCtx, mutations)
	if err != nil {
		return err
	}
	return store.prewriteMutations(reqCtx, mutations, req, items)
}

func (store *MVCCStore) prewriteMutations(reqCtx *requestCtx, mutations []*kvrpcpb.Mutation,
	req *kvrpcpb.PrewriteRequest, items []*badger.Item) error {
	var minCommitTS uint64
	if req.UseAsyncCommit || req.TryOnePc {
		// Get minCommitTS for async commit protocol. After all keys are locked in memory lock.
		physical, logical, tsErr := store.pdClient.GetTS(context.Background())
		if tsErr != nil {
			return tsErr
		}
		minCommitTS = uint64(physical)<<18 + uint64(logical)
		if req.MaxCommitTs > 0 && minCommitTS > req.MaxCommitTs {
			req.UseAsyncCommit = false
			req.TryOnePc = false
		}
		if req.UseAsyncCommit {
			reqCtx.asyncMinCommitTS = minCommitTS
		}
	}

	if req.UseAsyncCommit && minCommitTS > req.MinCommitTs {
		req.MinCommitTs = minCommitTS
	}

	if req.TryOnePc {
		committed, err := store.tryOnePC(reqCtx, mutations, req, items, minCommitTS, req.MaxCommitTs)
		if err != nil {
			return err
		}
		// If 1PC succeeded, exit immediately.
		if committed {
			return nil
		}
	}

	batch := store.dbWriter.NewWriteBatch(req.StartVersion, 0, reqCtx.rpcCtx)

	for i, m := range mutations {
		if m.Op == kvrpcpb.Op_CheckNotExists {
			continue
		}
		lock, err1 := store.buildPrewriteLock(reqCtx, m, items[i], req)
		if err1 != nil {
			return err1
		}
		batch.Prewrite(m.Key, lock)
	}

	return store.dbWriter.Write(batch)
}

// Flush implements the MVCCStore interface.
func (store *MVCCStore) Flush(reqCtx *requestCtx, req *kvrpcpb.FlushRequest) error {
	mutations := req.GetMutations()
	if !slices.IsSortedFunc(mutations, func(i, j *kvrpcpb.Mutation) int {
		return bytes.Compare(i.Key, j.Key)
	}) {
		return errors.New("mutations are not sorted")
	}

	regCtx := reqCtx.regCtx
	hashVals := mutationsToHashVals(mutations)
	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)

	startTS := req.StartTs
	// Only check the PK's status first.
	for _, m := range mutations {
		lock, err := store.checkConflictInLockStore(reqCtx, m, startTS)
		if err != nil {
			return err
		}
		if lock != nil {
			continue
		}
		if bytes.Equal(m.Key, req.PrimaryKey) {
			status := store.checkExtraTxnStatus(reqCtx, m.Key, startTS)
			if status.isRollback {
				return kverrors.ErrAlreadyRollback
			}
			if status.isOpLockCommitted() {
				// duplicated command
				return nil
			}
		}
	}
	items, err := store.getDBItems(reqCtx, mutations)
	if err != nil {
		return err
	}
	for i, m := range mutations {
		item := items[i]
		if item != nil {
			userMeta := mvcc.DBUserMeta(item.UserMeta())
			if userMeta.CommitTS() > startTS {
				return &kverrors.ErrConflict{
					StartTS:          startTS,
					ConflictTS:       userMeta.StartTS(),
					ConflictCommitTS: userMeta.CommitTS(),
					Key:              item.KeyCopy(nil),
					Reason:           kvrpcpb.WriteConflict_Optimistic,
				}
			}
		}
		// Op_CheckNotExists type requests should not add lock
		if m.Op == kvrpcpb.Op_CheckNotExists {
			if item != nil {
				val, err := item.Value()
				if err != nil {
					return err
				}
				if len(val) > 0 {
					return &kverrors.ErrKeyAlreadyExists{Key: m.Key}
				}
			}
			continue
		}
	}

	dummyPrewriteReq := &kvrpcpb.PrewriteRequest{
		PrimaryLock:  req.PrimaryKey,
		StartVersion: startTS,
	}
	batch := store.dbWriter.NewWriteBatch(startTS, 0, reqCtx.rpcCtx)
	for i, m := range mutations {
		if m.Op == kvrpcpb.Op_CheckNotExists {
			continue
		}
		lock, err1 := store.buildPrewriteLock(reqCtx, m, items[i], dummyPrewriteReq)
		if err1 != nil {
			return err1
		}
		batch.Prewrite(m.Key, lock)
	}

	return store.dbWriter.Write(batch)
}

func (store *MVCCStore) tryOnePC(reqCtx *requestCtx, mutations []*kvrpcpb.Mutation,
	req *kvrpcpb.PrewriteRequest, items []*badger.Item, minCommitTS uint64, maxCommitTS uint64) (bool, error) {
	if maxCommitTS != 0 && minCommitTS > maxCommitTS {
		log.Debug("1pc transaction fallbacks due to minCommitTS exceeds maxCommitTS",
			zap.Uint64("startTS", req.StartVersion),
			zap.Uint64("minCommitTS", minCommitTS),
			zap.Uint64("maxCommitTS", maxCommitTS))
		return false, nil
	}
	if minCommitTS < req.StartVersion {
		log.Fatal("1pc commitTS less than startTS", zap.Uint64("startTS", req.StartVersion), zap.Uint64("minCommitTS", minCommitTS))
	}

	reqCtx.onePCCommitTS = minCommitTS
	store.updateLatestTS(minCommitTS)
	batch := store.dbWriter.NewWriteBatch(req.StartVersion, minCommitTS, reqCtx.rpcCtx)

	for i, m := range mutations {
		if m.Op == kvrpcpb.Op_CheckNotExists {
			continue
		}
		lock, err1 := store.buildPrewriteLock(reqCtx, m, items[i], req)
		if err1 != nil {
			return false, err1
		}
		// batch.Commit will panic if the key is not locked. So there need to be a special function
		// for it to commit without deleting lock.
		batch.Commit(m.Key, lock)
	}

	if err := store.dbWriter.Write(batch); err != nil {
		return false, err
	}

	return true, nil
}

func encodeFromOldRow(oldRow, buf []byte) ([]byte, error) {
	var (
		colIDs []int64
		datums []types.Datum
	)
	for len(oldRow) > 1 {
		var d types.Datum
		var err error
		oldRow, d, err = codec.DecodeOne(oldRow)
		if err != nil {
			return nil, err
		}
		colID := d.GetInt64()
		oldRow, d, err = codec.DecodeOne(oldRow)
		if err != nil {
			return nil, err
		}
		colIDs = append(colIDs, colID)
		datums = append(datums, d)
	}
	var encoder rowcodec.Encoder
	buf = buf[:0]
	return encoder.Encode(time.UTC, colIDs, datums, nil, buf)
}

func (store *MVCCStore) buildPrewriteLock(reqCtx *requestCtx, m *kvrpcpb.Mutation, item *badger.Item,
	req *kvrpcpb.PrewriteRequest) (*mvcc.Lock, error) {
	lock := &mvcc.Lock{
		LockHdr: mvcc.LockHdr{
			StartTS:        req.StartVersion,
			TTL:            uint32(req.LockTtl),
			PrimaryLen:     uint16(len(req.PrimaryLock)),
			MinCommitTS:    req.MinCommitTs,
			UseAsyncCommit: req.UseAsyncCommit,
			SecondaryNum:   uint32(len(req.Secondaries)),
		},
		Primary:     req.PrimaryLock,
		Value:       m.Value,
		Secondaries: req.Secondaries,
	}
	// Note that this is not fully consistent with TiKV. TiKV doesn't always get the value from Write CF. In
	// AssertionLevel_Fast, TiKV skips checking assertion if Write CF is not read, in order not to harm the performance.
	// However, unistore can always check it. It's better not to assume the store's behavior about assertion when the
	// mode is set to AssertionLevel_Fast.
	if req.AssertionLevel != kvrpcpb.AssertionLevel_Off {
		if item == nil || item.IsEmpty() {
			if m.Assertion == kvrpcpb.Assertion_Exist {
				log.Error("ASSERTION FAIL!!! non-exist for must exist key", zap.Stringer("mutation", m))
				return nil, &kverrors.ErrAssertionFailed{
					StartTS:          req.StartVersion,
					Key:              m.Key,
					Assertion:        m.Assertion,
					ExistingStartTS:  0,
					ExistingCommitTS: 0,
				}
			}
		} else {
			if m.Assertion == kvrpcpb.Assertion_NotExist {
				log.Error("ASSERTION FAIL!!! exist for must non-exist key", zap.Stringer("mutation", m))
				userMeta := mvcc.DBUserMeta(item.UserMeta())
				return nil, &kverrors.ErrAssertionFailed{
					StartTS:          req.StartVersion,
					Key:              m.Key,
					Assertion:        m.Assertion,
					ExistingStartTS:  userMeta.StartTS(),
					ExistingCommitTS: userMeta.CommitTS(),
				}
			}
		}
	}
	var err error
	lock.Op = uint8(m.Op)
	if lock.Op == uint8(kvrpcpb.Op_Insert) {
		if item != nil && item.ValueSize() > 0 {
			return nil, &kverrors.ErrKeyAlreadyExists{Key: m.Key}
		}
		lock.Op = uint8(kvrpcpb.Op_Put)
	}
	// In the write path, remove the keyspace prefix
	// to ensure compatibility with the key parsing implemented in the mock.
	tempKey := rowcodec.RemoveKeyspacePrefix(m.Key)
	if rowcodec.IsRowKey(tempKey) && lock.Op == uint8(kvrpcpb.Op_Put) {
		if !rowcodec.IsNewFormat(m.Value) {
			reqCtx.buf, err = encodeFromOldRow(m.Value, reqCtx.buf)
			if err != nil {
				log.Error("encode data failed", zap.Binary("value", m.Value), zap.Binary("key", m.Key), zap.Stringer("op", m.Op), zap.Error(err))
				return nil, err
			}

			lock.Value = slices.Clone(reqCtx.buf)
		}
	}

	lock.ForUpdateTS = req.ForUpdateTs
	return lock, nil
}

func (store *MVCCStore) getLock(req *requestCtx, key []byte) *mvcc.Lock {
	req.buf = store.lockStore.Get(key, req.buf)
	if len(req.buf) == 0 {
		return nil
	}
	lock := mvcc.DecodeLock(req.buf)
	return &lock
}

func (store *MVCCStore) checkConflictInLockStore(
	req *requestCtx, mutation *kvrpcpb.Mutation, startTS uint64) (*mvcc.Lock, error) {
	req.buf = store.lockStore.Get(mutation.Key, req.buf)
	if len(req.buf) == 0 {
		return nil, nil
	}
	lock := mvcc.DecodeLock(req.buf)
	if lock.StartTS == startTS {
		// Same ts, no need to overwrite.
		return &lock, nil
	}
	return nil, kverrors.BuildLockErr(mutation.Key, &lock)
}

const maxSystemTS uint64 = math.MaxUint64

// Commit implements the MVCCStore interface.
func (store *MVCCStore) Commit(req *requestCtx, keys [][]byte, startTS, commitTS uint64) error {
	sortKeys(keys)
	store.updateLatestTS(commitTS)
	regCtx := req.regCtx
	hashVals := keysToHashVals(keys...)
	batch := store.dbWriter.NewWriteBatch(startTS, commitTS, req.rpcCtx)
	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)

	var buf []byte
	var tmpDiff int
	var isPessimisticTxn bool
	for _, key := range keys {
		var lockErr error
		var checkErr error
		var lock mvcc.Lock
		buf = store.lockStore.Get(key, buf)
		if len(buf) == 0 {
			// We never commit partial keys in Commit request, so if one lock is not found,
			// the others keys must not be found too.
			lockErr = kverrors.ErrLockNotFound
		} else {
			lock = mvcc.DecodeLock(buf)
			if lock.StartTS != startTS {
				lockErr = kverrors.ErrReplaced
			}
		}
		if lockErr != nil {
			// Maybe the secondary keys committed by other concurrent transactions using lock resolver,
			// check commit info from store
			checkErr = store.handleLockNotFound(req, key, startTS, commitTS)
			if checkErr == nil {
				continue
			}
			log.Error("commit failed, no correspond lock found",
				zap.Binary("key", key), zap.Uint64("start ts", startTS), zap.String("lock", fmt.Sprintf("%v", lock)), zap.Error(lockErr))
			return lockErr
		}
		if commitTS < lock.MinCommitTS {
			log.Info("trying to commit with smaller commitTs than minCommitTs",
				zap.Uint64("commit ts", commitTS), zap.Uint64("min commit ts", lock.MinCommitTS), zap.Binary("key", key))
			return &kverrors.ErrCommitExpire{
				StartTs:     startTS,
				CommitTs:    commitTS,
				MinCommitTs: lock.MinCommitTS,
				Key:         key,
			}
		}
		isPessimisticTxn = lock.ForUpdateTS > 0
		tmpDiff += len(key) + len(lock.Value)
		batch.Commit(key, &lock)
	}
	atomic.AddInt64(regCtx.Diff(), int64(tmpDiff))
	err := store.dbWriter.Write(batch)
	store.lockWaiterManager.WakeUp(startTS, commitTS, hashVals)
	if isPessimisticTxn {
		store.DeadlockDetectCli.CleanUp(startTS)
	}
	return err
}

func (store *MVCCStore) handleLockNotFound(reqCtx *requestCtx, key []byte, startTS, commitTS uint64) error {
	txn := reqCtx.getDBReader().GetTxn()
	txn.SetReadTS(commitTS)
	item, err := txn.Get(key)
	if err != nil && err != badger.ErrKeyNotFound {
		return errors.Trace(err)
	}
	if item == nil {
		return kverrors.ErrLockNotFound
	}
	userMeta := mvcc.DBUserMeta(item.UserMeta())
	if userMeta.StartTS() == startTS {
		// Already committed.
		return nil
	}
	return kverrors.ErrLockNotFound
}

const (
	rollbackStatusDone    = 0
	rollbackStatusNoLock  = 1
	rollbackStatusNewLock = 2
	rollbackPessimistic   = 3
	rollbackStatusLocked  = 4
)

// Rollback implements the MVCCStore interface.
func (store *MVCCStore) Rollback(reqCtx *requestCtx, keys [][]byte, startTS uint64) error {
	sortKeys(keys)
	hashVals := keysToHashVals(keys...)
	log.S().Debugf("%d rollback %v", startTS, hashVals)
	regCtx := reqCtx.regCtx
	batch := store.dbWriter.NewWriteBatch(startTS, 0, reqCtx.rpcCtx)

	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)

	statuses := make([]int, len(keys))
	for i, key := range keys {
		var rollbackErr error
		statuses[i], rollbackErr = store.rollbackKeyReadLock(reqCtx, batch, key, startTS, 0)
		if rollbackErr != nil {
			return errors.Trace(rollbackErr)
		}
	}
	for i, key := range keys {
		status := statuses[i]
		if status == rollbackStatusDone || status == rollbackPessimistic {
			// rollback pessimistic lock doesn't need to read db.
			continue
		}
		err := store.rollbackKeyReadDB(reqCtx, batch, key, startTS, statuses[i] == rollbackStatusNewLock)
		if err != nil {
			return err
		}
	}
	store.DeadlockDetectCli.CleanUp(startTS)
	err := store.dbWriter.Write(batch)
	return errors.Trace(err)
}

func (store *MVCCStore) rollbackKeyReadLock(reqCtx *requestCtx, batch mvcc.WriteBatch, key []byte,
	startTS, currentTs uint64) (int, error) {
	reqCtx.buf = store.lockStore.Get(key, reqCtx.buf)
	hasLock := len(reqCtx.buf) > 0
	if hasLock {
		lock := mvcc.DecodeLock(reqCtx.buf)
		if lock.StartTS < startTS {
			// The lock is old, means this is written by an old transaction, and the current transaction may not arrive.
			// We should write a rollback lock.
			batch.Rollback(key, false)
			return rollbackStatusDone, nil
		}
		if lock.StartTS == startTS {
			if currentTs > 0 && uint64(oracle.ExtractPhysical(lock.StartTS))+uint64(lock.TTL) >= uint64(oracle.ExtractPhysical(currentTs)) {
				return rollbackStatusLocked, kverrors.BuildLockErr(key, &lock)
			}
			// We can not simply delete the lock because the prewrite may be sent multiple times.
			// To prevent that we update it a rollback lock.
			batch.Rollback(key, true)
			return rollbackStatusDone, nil
		}
		// lock.startTS > startTS, go to DB to check if the key is committed.
		return rollbackStatusNewLock, nil
	}
	return rollbackStatusNoLock, nil
}

func (store *MVCCStore) rollbackKeyReadDB(req *requestCtx, batch mvcc.WriteBatch, key []byte, startTS uint64, hasLock bool) error {
	commitTS, err := store.checkCommitted(req.getDBReader(), key, startTS)
	if err != nil {
		return err
	}
	if commitTS != 0 {
		return kverrors.ErrAlreadyCommitted(commitTS)
	}
	// commit not found, rollback this key
	batch.Rollback(key, false)
	return nil
}

func (store *MVCCStore) checkCommitted(reader *dbreader.DBReader, key []byte, startTS uint64) (uint64, error) {
	txn := reader.GetTxn()
	item, err := txn.Get(key)
	if err != nil && err != badger.ErrKeyNotFound {
		return 0, errors.Trace(err)
	}
	if item == nil {
		return 0, nil
	}
	userMeta := mvcc.DBUserMeta(item.UserMeta())
	if userMeta.StartTS() == startTS {
		return userMeta.CommitTS(), nil
	}
	it := reader.GetIter()
	it.SetAllVersions(true)
	for it.Seek(key); it.Valid(); it.Next() {
		item = it.Item()
		if !bytes.Equal(item.Key(), key) {
			break
		}
		userMeta = mvcc.DBUserMeta(item.UserMeta())
		if userMeta.StartTS() == startTS {
			return userMeta.CommitTS(), nil
		}
	}
	return 0, nil
}
