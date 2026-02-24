// Copyright 2015 PingCAP, Inc.
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

package variable

import (
	"encoding/binary"
	"maps"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/tableutil"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/twmb/murmur3"
)

var (
	// PreparedStmtCount is exported for test.
	PreparedStmtCount int64
	// enableAdaptiveReplicaRead indicates whether closest adaptive replica read
	// can be enabled. We forces disable replica read when tidb server in missing
	// in regions that contains tikv server to avoid read traffic skew.
	enableAdaptiveReplicaRead uint32 = 1
)

// ConnStatusShutdown indicates that the connection status is closed by server.
// This code is put here because of package imports, and this value is the original server.connStatusShutdown.
const ConnStatusShutdown int32 = 2

// SetEnableAdaptiveReplicaRead set `enableAdaptiveReplicaRead` with given value.
// return true if the value is changed.
func SetEnableAdaptiveReplicaRead(enabled bool) bool {
	value := uint32(0)
	if enabled {
		value = 1
	}
	return atomic.SwapUint32(&enableAdaptiveReplicaRead, value) != value
}

// IsAdaptiveReplicaReadEnabled returns whether adaptive closest replica read can be enabled.
func IsAdaptiveReplicaReadEnabled() bool {
	return atomic.LoadUint32(&enableAdaptiveReplicaRead) > 0
}

// RetryInfo saves retry information.
type RetryInfo struct {
	Retrying               bool
	DroppedPreparedStmtIDs []uint32
	autoIncrementIDs       retryInfoAutoIDs
	autoRandomIDs          retryInfoAutoIDs
	LastRcReadTS           uint64
}

// Clean does some clean work.
func (r *RetryInfo) Clean() {
	r.autoIncrementIDs.clean()
	r.autoRandomIDs.clean()

	if len(r.DroppedPreparedStmtIDs) > 0 {
		r.DroppedPreparedStmtIDs = r.DroppedPreparedStmtIDs[:0]
	}
}

// ResetOffset resets the current retry offset.
func (r *RetryInfo) ResetOffset() {
	r.autoIncrementIDs.resetOffset()
	r.autoRandomIDs.resetOffset()
}

// AddAutoIncrementID adds id to autoIncrementIDs.
func (r *RetryInfo) AddAutoIncrementID(id int64) {
	r.autoIncrementIDs.autoIDs = append(r.autoIncrementIDs.autoIDs, id)
}

// GetCurrAutoIncrementID gets current autoIncrementID.
func (r *RetryInfo) GetCurrAutoIncrementID() (int64, bool) {
	return r.autoIncrementIDs.getCurrent()
}

// AddAutoRandomID adds id to autoRandomIDs.
func (r *RetryInfo) AddAutoRandomID(id int64) {
	r.autoRandomIDs.autoIDs = append(r.autoRandomIDs.autoIDs, id)
}

// GetCurrAutoRandomID gets current AutoRandomID.
func (r *RetryInfo) GetCurrAutoRandomID() (int64, bool) {
	return r.autoRandomIDs.getCurrent()
}

type retryInfoAutoIDs struct {
	currentOffset int
	autoIDs       []int64
}

func (r *retryInfoAutoIDs) resetOffset() {
	r.currentOffset = 0
}

func (r *retryInfoAutoIDs) clean() {
	r.currentOffset = 0
	if len(r.autoIDs) > 0 {
		r.autoIDs = r.autoIDs[:0]
	}
}

func (r *retryInfoAutoIDs) getCurrent() (int64, bool) {
	if r.currentOffset >= len(r.autoIDs) {
		return 0, false
	}
	id := r.autoIDs[r.currentOffset]
	r.currentOffset++
	return id, true
}

// TransactionContext is used to store variables that has transaction scope.
type TransactionContext struct {
	TxnCtxNoNeedToRestore
	TxnCtxNeedToRestore
}

// TxnCtxNeedToRestore stores transaction variables which need to be restored when rolling back to a savepoint.
type TxnCtxNeedToRestore struct {
	// TableDeltaMap is used in the schema validator for DDL changes in one table not to block others.
	// It's also used in the statistics updating.
	// Note: for the partitioned table, it stores all the partition IDs.
	TableDeltaMap map[int64]TableDelta

	// pessimisticLockCache is the cache for pessimistic locked keys,
	// The value never changes during the transaction.
	pessimisticLockCache map[string][]byte

	// CachedTables is not nil if the transaction write on cached table.
	CachedTables map[int64]any

	// InsertTTLRowsCount counts how many rows are inserted in this statement
	InsertTTLRowsCount int
}

// TxnCtxNoNeedToRestore stores transaction variables which do not need to restored when rolling back to a savepoint.
type TxnCtxNoNeedToRestore struct {
	forUpdateTS uint64
	Binlog      any
	InfoSchema  any
	History     any
	StartTS     uint64
	StaleReadTs uint64

	// unchangedKeys is used to store the unchanged keys that needs to lock for pessimistic transaction.
	unchangedKeys map[string]bool

	PessimisticCacheHit int

	// CreateTime For metrics.
	CreateTime     time.Time
	StatementCount int
	CouldRetry     bool
	IsPessimistic  bool
	// IsStaleness indicates whether the txn is read only staleness txn.
	IsStaleness bool
	// IsExplicit indicates whether the txn is an interactive txn, which is typically started with a BEGIN
	// or START TRANSACTION statement, or by setting autocommit to 0.
	IsExplicit bool
	Isolation  string
	LockExpire uint32
	ForUpdate  uint32
	// TxnScope indicates the value of txn_scope
	TxnScope string

	// Savepoints contains all definitions of the savepoint of a transaction at runtime, the order of the SavepointRecord is the same with the SAVEPOINT statements.
	// It is used for a lookup when running `ROLLBACK TO` statement.
	Savepoints []SavepointRecord

	// TableDeltaMap lock to prevent potential data race
	tdmLock sync.Mutex

	// TemporaryTables is used to store transaction-specific information for global temporary tables.
	// It can also be stored in sessionCtx with local temporary tables, but it's easier to clean this data after transaction ends.
	TemporaryTables map[int64]tableutil.TempTable
	// EnableMDL indicates whether to enable the MDL lock for the transaction.
	EnableMDL bool
	// relatedTableForMDL records the `lock` table for metadata lock. It maps from int64 to int64(version).
	relatedTableForMDL *sync.Map

	// FairLockingUsed marking whether at least one of the statements in the transaction was executed in
	// fair locking mode.
	FairLockingUsed bool
	// FairLockingEffective marking whether at least one of the statements in the transaction was executed in
	// fair locking mode, and it takes effect (which is determined according to whether lock-with-conflict
	// has occurred during execution of any statement).
	FairLockingEffective bool

	// CurrentStmtPessimisticLockCache is the cache for pessimistic locked keys in the current statement.
	// It is merged into `pessimisticLockCache` after a statement finishes.
	// Read results cannot be directly written into pessimisticLockCache because failed statement need to rollback
	// its pessimistic locks.
	CurrentStmtPessimisticLockCache map[string][]byte
}

// SavepointRecord indicates a transaction's savepoint record.
type SavepointRecord struct {
	// name is the name of the savepoint
	Name string
	// MemDBCheckpoint is the transaction's memdb checkpoint.
	MemDBCheckpoint *tikv.MemDBCheckpoint
	// TxnCtxSavepoint is the savepoint of TransactionContext
	TxnCtxSavepoint TxnCtxNeedToRestore
}

// RowIDShardGenerator is used to generate shard for row id.
type RowIDShardGenerator struct {
	// shardRand is used for generated rand shard
	shardRand *rand.Rand
	// shardStep indicates the max size of continuous rowid shard in one transaction.
	shardStep    int
	shardRemain  int
	currentShard int64
}

// NewRowIDShardGenerator creates a new RowIDShardGenerator.
func NewRowIDShardGenerator(shardRand *rand.Rand, step int) *RowIDShardGenerator {
	intest.AssertNotNil(shardRand)
	return &RowIDShardGenerator{
		shardRand: shardRand,
		shardStep: step,
	}
}

// SetShardStep sets the step of shard
func (s *RowIDShardGenerator) SetShardStep(step int) {
	s.shardStep = step
	s.shardRemain = 0
}

// GetShardStep returns the shard step
func (s *RowIDShardGenerator) GetShardStep() int {
	return s.shardStep
}

// GetCurrentShard returns the shard for the next `count` IDs.
func (s *RowIDShardGenerator) GetCurrentShard(count int) int64 {
	if s.shardRemain <= 0 {
		s.updateShard(s.shardRand)
		s.shardRemain = s.GetShardStep()
	}
	s.shardRemain -= count
	return s.currentShard
}

func (s *RowIDShardGenerator) updateShard(shardRand *rand.Rand) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], shardRand.Uint64())
	s.currentShard = int64(murmur3.Sum32(buf[:]))
}

// GetRowIDShardGenerator shard row id generator
func (s *SessionVars) GetRowIDShardGenerator() *RowIDShardGenerator {
	if s.shardGenerator != nil {
		return s.shardGenerator
	}

	intest.Assert(s.TxnCtx.StartTS > 0)
	r := rand.New(rand.NewSource(int64(s.TxnCtx.StartTS))) // #nosec G404
	s.shardGenerator = NewRowIDShardGenerator(r, int(s.ShardAllocateStep))
	return s.shardGenerator
}

// AddUnchangedKeyForLock adds an unchanged key for pessimistic lock.
func (tc *TransactionContext) AddUnchangedKeyForLock(key []byte, shared bool) {
	if tc.unchangedKeys == nil {
		tc.unchangedKeys = map[string]bool{}
	}
	k := string(key)
	alreadyShared, exist := tc.unchangedKeys[k]
	tc.unchangedKeys[k] = shared && (!exist || (exist && alreadyShared))
}

// CollectUnchangedKeysForXLock collects unchanged keys for pessimistic lock.
func (tc *TransactionContext) CollectUnchangedKeysForXLock(buf []kv.Key) []kv.Key {
	for key, shared := range tc.unchangedKeys {
		if !shared {
			buf = append(buf, kv.Key(key))
		}
	}
	return buf
}

// CollectUnchangedKeysForSLock collects unchanged keys for pessimistic lock in share mode.
func (tc *TransactionContext) CollectUnchangedKeysForSLock(buf []kv.Key) []kv.Key {
	for key, shared := range tc.unchangedKeys {
		if shared {
			buf = append(buf, kv.Key(key))
		}
	}
	return buf
}

// ResetUnchangedKeysForLock resets unchanged keys for lock.
func (tc *TransactionContext) ResetUnchangedKeysForLock() {
	tc.unchangedKeys = nil
}

// UpdateDeltaForTable updates the delta info for some table.
// The `cols` argument is used to update the delta size for cols.
// If `cols` is nil, it means that the delta size for cols is not changed.
func (tc *TransactionContext) UpdateDeltaForTable(
	physicalTableID int64,
	delta int64,
	count int64,
) {
	tc.tdmLock.Lock()
	defer tc.tdmLock.Unlock()
	if tc.TableDeltaMap == nil {
		tc.TableDeltaMap = make(map[int64]TableDelta)
	}
	item := tc.TableDeltaMap[physicalTableID]
	item.Delta += delta
	item.Count += count
	tc.TableDeltaMap[physicalTableID] = item
}

// GetKeyInPessimisticLockCache gets a key in pessimistic lock cache.
func (tc *TransactionContext) GetKeyInPessimisticLockCache(key kv.Key) (val []byte, ok bool) {
	if tc.pessimisticLockCache == nil && tc.CurrentStmtPessimisticLockCache == nil {
		return nil, false
	}
	if tc.CurrentStmtPessimisticLockCache != nil {
		val, ok = tc.CurrentStmtPessimisticLockCache[string(key)]
		if ok {
			tc.PessimisticCacheHit++
			return
		}
	}
	if tc.pessimisticLockCache != nil {
		val, ok = tc.pessimisticLockCache[string(key)]
		if ok {
			tc.PessimisticCacheHit++
		}
	}
	return
}

// SetPessimisticLockCache sets a key value pair in pessimistic lock cache.
// The value is buffered in the statement cache until the current statement finishes.
func (tc *TransactionContext) SetPessimisticLockCache(key kv.Key, val []byte) {
	if tc.CurrentStmtPessimisticLockCache == nil {
		tc.CurrentStmtPessimisticLockCache = make(map[string][]byte)
	}
	tc.CurrentStmtPessimisticLockCache[string(key)] = val
}

// Cleanup clears up transaction info that no longer use.
func (tc *TransactionContext) Cleanup() {
	// tc.InfoSchema = nil; we cannot do it now, because some operation like handleFieldList depend on this.
	tc.Binlog = nil
	tc.History = nil
	tc.tdmLock.Lock()
	tc.TableDeltaMap = nil
	tc.relatedTableForMDL = nil
	tc.tdmLock.Unlock()
	tc.pessimisticLockCache = nil
	tc.CurrentStmtPessimisticLockCache = nil
	tc.IsStaleness = false
	tc.Savepoints = nil
	tc.EnableMDL = false
}

// ClearDelta clears the delta map.
func (tc *TransactionContext) ClearDelta() {
	tc.tdmLock.Lock()
	tc.TableDeltaMap = nil
	tc.tdmLock.Unlock()
}

// GetForUpdateTS returns the ts for update.
func (tc *TransactionContext) GetForUpdateTS() uint64 {
	if tc.forUpdateTS > tc.StartTS {
		return tc.forUpdateTS
	}
	return tc.StartTS
}

// SetForUpdateTS sets the ts for update.
func (tc *TransactionContext) SetForUpdateTS(forUpdateTS uint64) {
	if forUpdateTS > tc.forUpdateTS {
		tc.forUpdateTS = forUpdateTS
	}
}

// GetCurrentSavepoint gets TransactionContext's savepoint.
func (tc *TransactionContext) GetCurrentSavepoint() TxnCtxNeedToRestore {
	tableDeltaMap := make(map[int64]TableDelta, len(tc.TableDeltaMap))
	for k, v := range tc.TableDeltaMap {
		tableDeltaMap[k] = v.Clone()
	}
	return TxnCtxNeedToRestore{
		TableDeltaMap:        tableDeltaMap,
		pessimisticLockCache: maps.Clone(tc.pessimisticLockCache),
		CachedTables:         maps.Clone(tc.CachedTables),
		InsertTTLRowsCount:   tc.InsertTTLRowsCount,
	}
}

// RestoreBySavepoint restores TransactionContext to the specify savepoint.
func (tc *TransactionContext) RestoreBySavepoint(savepoint TxnCtxNeedToRestore) {
	tc.TableDeltaMap = savepoint.TableDeltaMap
	tc.pessimisticLockCache = savepoint.pessimisticLockCache
	tc.CachedTables = savepoint.CachedTables
	tc.InsertTTLRowsCount = savepoint.InsertTTLRowsCount
}

// AddSavepoint adds a new savepoint.
func (tc *TransactionContext) AddSavepoint(name string, memdbCheckpoint *tikv.MemDBCheckpoint) {
	name = strings.ToLower(name)
	tc.DeleteSavepoint(name)

	record := SavepointRecord{
		Name:            name,
		MemDBCheckpoint: memdbCheckpoint,
		TxnCtxSavepoint: tc.GetCurrentSavepoint(),
	}
	tc.Savepoints = append(tc.Savepoints, record)
}

// DeleteSavepoint deletes the savepoint, return false indicate the savepoint name doesn't exists.
func (tc *TransactionContext) DeleteSavepoint(name string) bool {
	name = strings.ToLower(name)
	for i, sp := range tc.Savepoints {
		if sp.Name == name {
			tc.Savepoints = slices.Delete(tc.Savepoints, i, i+1)
			return true
		}
	}
	return false
}

// ReleaseSavepoint deletes the named savepoint and the later savepoints, return false indicate the named savepoint doesn't exists.
func (tc *TransactionContext) ReleaseSavepoint(name string) bool {
	name = strings.ToLower(name)
	for i, sp := range tc.Savepoints {
		if sp.Name == name {
			tc.Savepoints = tc.Savepoints[:i]
			return true
		}
	}
	return false
}

// RollbackToSavepoint rollbacks to the specified savepoint by name.
func (tc *TransactionContext) RollbackToSavepoint(name string) *SavepointRecord {
	name = strings.ToLower(name)
	for idx, sp := range tc.Savepoints {
		if name == sp.Name {
			tc.RestoreBySavepoint(sp.TxnCtxSavepoint)
			tc.Savepoints = tc.Savepoints[:idx+1]
			return &tc.Savepoints[idx]
		}
	}
	return nil
}

// FlushStmtPessimisticLockCache merges the current statement pessimistic lock cache into transaction pessimistic lock
// cache. The caller may need to clear the stmt cache itself.
func (tc *TransactionContext) FlushStmtPessimisticLockCache() {
	if tc.CurrentStmtPessimisticLockCache == nil {
		return
	}
	if tc.pessimisticLockCache == nil {
		tc.pessimisticLockCache = make(map[string][]byte)
	}
	maps.Copy(tc.pessimisticLockCache, tc.CurrentStmtPessimisticLockCache)
	tc.CurrentStmtPessimisticLockCache = nil
}

// WriteStmtBufs can be used by insert/replace/delete/update statement.
// TODO: use a common memory pool to replace this.
type WriteStmtBufs struct {
	// RowValBuf is used by tablecodec.EncodeRow, to reduce runtime.growslice.
	RowValBuf []byte
	// AddRowValues use to store temp insert rows value, to reduce memory allocations when importing data.
	AddRowValues []types.Datum

	// IndexValsBuf is used by index.FetchValues
	IndexValsBuf []types.Datum
	// IndexKeyBuf is used by index.GenIndexKey
	IndexKeyBuf []byte
}

func (ib *WriteStmtBufs) clean() {
	ib.RowValBuf = nil
	ib.AddRowValues = nil
	ib.IndexValsBuf = nil
	ib.IndexKeyBuf = nil
}

// TableSnapshot represents a data snapshot of the table contained in `information_schema`.
type TableSnapshot struct {
	Rows [][]types.Datum
	Err  error
}

type txnIsolationLevelOneShotState uint

// RewritePhaseInfo records some information about the rewrite phase
type RewritePhaseInfo struct {
	// DurationRewrite is the duration of rewriting the SQL.
	DurationRewrite time.Duration

	// DurationPreprocessSubQuery is the duration of pre-processing sub-queries.
	DurationPreprocessSubQuery time.Duration

	// PreprocessSubQueries is the number of pre-processed sub-queries.
	PreprocessSubQueries int
}

// Reset resets all fields in RewritePhaseInfo.
func (r *RewritePhaseInfo) Reset() {
	r.DurationRewrite = 0
	r.DurationPreprocessSubQuery = 0
	r.PreprocessSubQueries = 0
}

// TemporaryTableData is a interface to maintain temporary data in session
type TemporaryTableData interface {
	kv.Retriever
	// Staging create a new staging buffer inside the MemBuffer.
	// Subsequent writes will be temporarily stored in this new staging buffer.
	// When you think all modifications looks good, you can call `Release` to public all of them to the upper level buffer.
	Staging() kv.StagingHandle
	// Release publish all modifications in the latest staging buffer to upper level.
	Release(kv.StagingHandle)
	// Cleanup cleanups the resources referenced by the StagingHandle.
	// If the changes are not published by `Release`, they will be discarded.
	Cleanup(kv.StagingHandle)
	// GetTableSize get the size of a table
	GetTableSize(tblID int64) int64
	// DeleteTableKey removes the entry for key k from table
	DeleteTableKey(tblID int64, k kv.Key) error
	// SetTableKey sets the entry for k from table
	SetTableKey(tblID int64, k kv.Key, val []byte) error
}

// temporaryTableData is used for store temporary table data in session
type temporaryTableData struct {
	kv.MemBuffer
	tblSize map[int64]int64
}

// NewTemporaryTableData creates a new TemporaryTableData
func NewTemporaryTableData(memBuffer kv.MemBuffer) TemporaryTableData {
	return &temporaryTableData{
		MemBuffer: memBuffer,
		tblSize:   make(map[int64]int64),
	}
}

// GetTableSize get the size of a table
func (d *temporaryTableData) GetTableSize(tblID int64) int64 {
	if tblSize, ok := d.tblSize[tblID]; ok {
		return tblSize
	}
	return 0
}

// DeleteTableKey removes the entry for key k from table
func (d *temporaryTableData) DeleteTableKey(tblID int64, k kv.Key) error {
	bufferSize := d.MemBuffer.Size()
	defer d.updateTblSize(tblID, bufferSize)

	return d.MemBuffer.Delete(k)
}

// SetTableKey sets the entry for k from table
func (d *temporaryTableData) SetTableKey(tblID int64, k kv.Key, val []byte) error {
	bufferSize := d.MemBuffer.Size()
	defer d.updateTblSize(tblID, bufferSize)

	return d.MemBuffer.Set(k, val)
}

func (d *temporaryTableData) updateTblSize(tblID int64, beforeSize int) {
	delta := int64(d.MemBuffer.Size() - beforeSize)
	d.tblSize[tblID] = d.GetTableSize(tblID) + delta
}

const (
	// oneShotDef means default, that is tx_isolation_one_shot not set.
	oneShotDef txnIsolationLevelOneShotState = iota
	// oneShotSet means it's set in current transaction.
	oneShotSet
	// onsShotUse means it should be used in current transaction.
	oneShotUse
)

// ReadConsistencyLevel is the level of read consistency.
type ReadConsistencyLevel string

const (
	// ReadConsistencyStrict means read by strict consistency, default value.
	ReadConsistencyStrict ReadConsistencyLevel = "strict"
	// ReadConsistencyWeak means read can be weak consistency.
	ReadConsistencyWeak ReadConsistencyLevel = "weak"
)

// IsWeak returns true only if it's a weak-consistency read.
func (r ReadConsistencyLevel) IsWeak() bool {
	return r == ReadConsistencyWeak
}

func validateReadConsistencyLevel(val string) error {
	switch v := ReadConsistencyLevel(strings.ToLower(val)); v {
	case ReadConsistencyStrict, ReadConsistencyWeak:
		return nil
	default:
		return ErrWrongTypeForVar.GenWithStackByArgs(vardef.TiDBReadConsistency)
	}
}

// UserVarsReader is used to read user defined variables.
type UserVarsReader interface {
	// GetUserVarVal get user defined variables' value
	GetUserVarVal(name string) (types.Datum, bool)
	// GetUserVarType get user defined variables' type
	GetUserVarType(name string) (*types.FieldType, bool)
	// Clone clones the user vars
	Clone() UserVarsReader
}

// UserVars should implement UserVarsReader interface.
var _ UserVarsReader = &UserVars{}

// UserVars is used to provide user variable operations.
type UserVars struct {
	// lock is for user defined variables. values and types is read/write protected.
	lock sync.RWMutex
	// values stores the Datum for user variables
	values map[string]types.Datum
	// types stores the FieldType for user variables, it cannot be inferred from values when values have not been set yet.
	types map[string]*types.FieldType
}

// NewUserVars creates a new user UserVars object
func NewUserVars() *UserVars {
	return &UserVars{
		values: make(map[string]types.Datum),
		types:  make(map[string]*types.FieldType),
	}
}

// Clone clones the user vars
func (s *UserVars) Clone() UserVarsReader {
	cloned := NewUserVars()
	s.lock.Lock()
	defer s.lock.Unlock()
	for name, userVar := range s.values {
		cloned.values[name] = *userVar.Clone()
	}
	for name, userVarType := range s.types {
		cloned.types[name] = userVarType.Clone()
	}
	return cloned
}

// SetUserVarVal set user defined variables' value
func (s *UserVars) SetUserVarVal(name string, dt types.Datum) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.values[name] = dt
}

// UnsetUserVar unset an user defined variable by name.
func (s *UserVars) UnsetUserVar(varName string) {
	varName = strings.ToLower(varName)
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.values, varName)
	delete(s.types, varName)
}

// GetUserVarVal get user defined variables' value
func (s *UserVars) GetUserVarVal(name string) (types.Datum, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	dt, ok := s.values[name]
	return dt, ok
}

// SetUserVarType set user defined variables' type
func (s *UserVars) SetUserVarType(name string, ft *types.FieldType) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.types[name] = ft
}

// GetUserVarType get user defined variables' type
func (s *UserVars) GetUserVarType(name string) (*types.FieldType, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	ft, ok := s.types[name]
	return ft, ok
}
