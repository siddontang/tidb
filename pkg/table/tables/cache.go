// Copyright 2021 PingCAP, Inc.
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

package tables

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

var (
	_ table.CachedTable = &cachedTable{}
)

type cachedTable struct {
	TableCommon
	cacheData atomic.Pointer[cacheData]
	// invalidationEpoch tracks the latest local invalidation epoch observed by this table.
	invalidationEpoch uint64
	segments          *segmentIndex
	totalSize         int64
	hotAccessMu       sync.Mutex
	hotAccessCount    map[string]uint8
	// StateRemote is not thread-safe, this tokenLimit is used to keep only one visitor.
	tokenLimit
}

type tokenLimit chan StateRemote

func (t tokenLimit) TakeStateRemoteHandle() StateRemote {
	handle := <-t
	return handle
}

func (t tokenLimit) TakeStateRemoteHandleNoWait() StateRemote {
	select {
	case handle := <-t:
		return handle
	default:
		return nil
	}
}

func (t tokenLimit) PutStateRemoteHandle(handle StateRemote) {
	t <- handle
}

// cacheData pack the cache data and lease.
type cacheData struct {
	Start uint64
	Lease uint64
	Epoch uint64
	kv.MemBuffer
}

func (c *cachedTable) setCacheData(data *cacheData, totalSize int64) {
	if data != nil && data.Epoch == 0 {
		data.Epoch = atomic.LoadUint64(&c.invalidationEpoch)
	}
	c.cacheData.Store(data)

	if c.segments == nil || data == nil {
		return
	}
	// Keep the current full-table cache path represented as a single segment.
	// This allows follow-up refactor steps to switch read/invalidation logic to segments incrementally.
	_ = c.segments.upsert(cacheSegment{
		span:       keySpan{},
		epoch:      data.Epoch,
		startTS:    data.Start,
		leaseTS:    data.Lease,
		sizeBytes:  totalSize,
		lastAccess: time.Now(),
		memBuffer:  data.MemBuffer,
	})
	if data.MemBuffer != nil {
		atomic.StoreInt64(&c.totalSize, totalSize)
		c.resetHotAccessCount()
	}
}

func leaseFromTS(ts uint64, leaseDuration time.Duration) uint64 {
	physicalTime := oracle.GetTimeFromTS(ts)
	lease := oracle.GoTimeToTS(physicalTime.Add(leaseDuration))
	return lease
}

func newMemBuffer(store kv.Storage) (kv.MemBuffer, error) {
	// Here is a trick to get a MemBuffer data, because the internal API is not exposed.
	// Create a transaction with start ts 0, and take the MemBuffer out.
	buffTxn, err := store.Begin(tikv.WithStartTS(0))
	if err != nil {
		return nil, err
	}
	return buffTxn.GetMemBuffer(), nil
}

func (c *cachedTable) TryReadFromCache(ts uint64, leaseDuration time.Duration) (kv.MemBuffer, bool /*loading*/) {
	data := c.cacheData.Load()
	if data == nil {
		return nil, false
	}
	if data.Epoch < atomic.LoadUint64(&c.invalidationEpoch) {
		c.cacheData.Store(nil)
		return nil, false
	}
	if ts >= data.Start && ts < data.Lease {
		leaseTime := oracle.GetTimeFromTS(data.Lease)
		nowTime := oracle.GetTimeFromTS(ts)
		distance := leaseTime.Sub(nowTime)

		var triggerFailpoint bool
		failpoint.Inject("mockRenewLeaseABA1", func(_ failpoint.Value) {
			triggerFailpoint = true
		})

		if distance >= 0 && distance <= leaseDuration/2 || triggerFailpoint {
			if h := c.TakeStateRemoteHandleNoWait(); h != nil {
				go c.renewLease(h, ts, data, leaseDuration)
			}
		}
		// If data is not nil, but data.MemBuffer is nil, it means the data is being
		// loading by a background goroutine.
		return data.MemBuffer, data.MemBuffer == nil
	}
	return nil, false
}

// TryReadFromCacheByKey tries reading a specific row key from cached segments.
// It supports both full-table cache and key-range (hot-range) segments.
func (c *cachedTable) TryReadFromCacheByKey(ts uint64, leaseDuration time.Duration, key kv.Key) (kv.ValueEntry, bool, bool) {
	memBuffer, loading := c.TryReadFromCache(ts, leaseDuration)
	if memBuffer != nil {
		value, err := memBuffer.Get(context.Background(), key)
		if kv.ErrNotExist.Equal(err) {
			return kv.ValueEntry{}, true, false
		}
		if err != nil {
			return kv.ValueEntry{}, false, false
		}
		return value, true, false
	}
	if loading || c.segments == nil {
		return kv.ValueEntry{}, false, loading
	}

	seg, ok := c.segments.findByKey(key)
	if !ok {
		return kv.ValueEntry{}, false, false
	}
	currentEpoch := atomic.LoadUint64(&c.invalidationEpoch)
	if seg.epoch < currentEpoch || ts < seg.startTS || ts >= seg.leaseTS {
		return kv.ValueEntry{}, false, false
	}
	if seg.memBuffer == nil {
		return kv.ValueEntry{}, false, true
	}
	value, err := seg.memBuffer.Get(context.Background(), key)
	if kv.ErrNotExist.Equal(err) {
		return kv.ValueEntry{}, true, false
	}
	if err != nil {
		return kv.ValueEntry{}, false, false
	}
	return value, true, false
}

// StoreKeyInCache stores one row key/value as a tiny cache segment for hot-range point queries.
func (c *cachedTable) StoreKeyInCache(store kv.Storage, ts uint64, leaseDuration time.Duration, key kv.Key, value kv.ValueEntry) {
	if store == nil || len(key) == 0 || value.IsValueEmpty() || c.segments == nil {
		return
	}
	maxSegments := vardef.CachedTableHotRangeMaxSegments.Load()
	if maxSegments == 0 {
		return
	}
	admissionThreshold := vardef.CachedTableHotRangeAdmissionThreshold.Load()
	if admissionThreshold < 1 {
		admissionThreshold = 1
	}
	start := key.Clone()
	span := keySpan{start: start, end: start.Next()}
	if _, exist, err := c.segments.get(span); err == nil && !exist && int64(c.segments.hotRangeLen()) >= maxSegments {
		return
	}
	if !c.shouldAdmitHotRangeKey(start, maxSegments, admissionThreshold) {
		return
	}
	memBuffer, err := newMemBuffer(store)
	if err != nil {
		logutil.BgLogger().Warn("create mem buffer for cached key failed", zap.Error(err))
		return
	}
	if err = memBuffer.Set(key, value.Value); err != nil {
		logutil.BgLogger().Warn("store cached key value failed", zap.Error(err))
		return
	}
	seg := cacheSegment{
		span:       span,
		epoch:      atomic.LoadUint64(&c.invalidationEpoch),
		startTS:    ts,
		leaseTS:    leaseFromTS(ts, leaseDuration),
		sizeBytes:  int64(len(key) + len(value.Value)),
		hitCount:   1,
		lastAccess: time.Now(),
		memBuffer:  memBuffer,
	}
	if err = c.segments.upsert(seg); err != nil {
		logutil.BgLogger().Warn("upsert cached key segment failed", zap.Error(err))
	}
}

func (c *cachedTable) shouldAdmitHotRangeKey(key kv.Key, maxSegments int64, threshold int64) bool {
	if threshold <= 1 {
		return true
	}

	c.hotAccessMu.Lock()
	defer c.hotAccessMu.Unlock()

	if c.hotAccessCount == nil {
		c.hotAccessCount = make(map[string]uint8)
	}
	keyString := string(key)
	count := c.hotAccessCount[keyString] + 1
	if int64(count) >= threshold {
		delete(c.hotAccessCount, keyString)
		return true
	}
	c.hotAccessCount[keyString] = count

	trackLimit := int(maxSegments * 8)
	if trackLimit < 1024 {
		trackLimit = 1024
	}
	if len(c.hotAccessCount) > trackLimit {
		c.hotAccessCount = make(map[string]uint8)
	}
	return false
}

func (c *cachedTable) resetHotAccessCount() {
	c.hotAccessMu.Lock()
	defer c.hotAccessMu.Unlock()
	if len(c.hotAccessCount) == 0 {
		return
	}
	c.hotAccessCount = make(map[string]uint8)
}

// newCachedTable creates a new CachedTable Instance
func newCachedTable(tbl *TableCommon) (table.Table, error) {
	ret := &cachedTable{
		TableCommon: tbl.Copy(),
		segments:    newSegmentIndex(),
		tokenLimit:  make(chan StateRemote, 1),
	}
	return ret, nil
}

// Init is an extra operation for cachedTable after TableFromMeta,
// Because cachedTable need some additional parameter that can't be passed in TableFromMeta.
func (c *cachedTable) Init(exec sqlexec.SQLExecutor) error {
	raw, ok := exec.(sqlExec)
	if !ok {
		return errors.New("Need sqlExec rather than sqlexec.SQLExecutor")
	}
	handle := NewStateRemote(raw)
	c.PutStateRemoteHandle(handle)
	return nil
}

func (c *cachedTable) loadDataFromOriginalTable(store kv.Storage) (kv.MemBuffer, uint64, int64, error) {
	buffer, err := newMemBuffer(store)
	if err != nil {
		return nil, 0, 0, err
	}
	var startTS uint64
	totalSize := int64(0)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnCacheTable)
	err = kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
		prefix := tablecodec.GenTablePrefix(c.tableID)
		if err != nil {
			return errors.Trace(err)
		}
		startTS = txn.StartTS()
		it, err := txn.Iter(prefix, prefix.PrefixNext())
		if err != nil {
			return errors.Trace(err)
		}
		defer it.Close()

		for it.Valid() && it.Key().HasPrefix(prefix) {
			key := it.Key()
			value := it.Value()
			err = buffer.Set(key, value)
			if err != nil {
				return errors.Trace(err)
			}
			totalSize += int64(len(key))
			totalSize += int64(len(value))
			err = it.Next()
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, 0, totalSize, err
	}

	return buffer, startTS, totalSize, nil
}

func (c *cachedTable) UpdateLockForRead(ctx context.Context, store kv.Storage, ts uint64, leaseDuration time.Duration) {
	if h := c.TakeStateRemoteHandleNoWait(); h != nil {
		go c.updateLockForRead(ctx, h, store, ts, leaseDuration)
	}
}

func (c *cachedTable) updateLockForRead(ctx context.Context, handle StateRemote, store kv.Storage, ts uint64, leaseDuration time.Duration) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("panic in the recoverable goroutine",
				zap.Any("r", r),
				zap.Stack("stack trace"))
		}
		c.PutStateRemoteHandle(handle)
	}()

	// Load data from original table and the update lock information.
	tid := c.GetPhysicalID()
	lease := leaseFromTS(ts, leaseDuration)
	succ, err := handle.LockForRead(ctx, tid, lease)
	if err != nil {
		log.Warn("lock cached table for read", zap.Error(err))
		return
	}
	if succ {
		c.setCacheData(&cacheData{
			Start:     ts,
			Lease:     lease,
			MemBuffer: nil, // Async loading, this will be set later.
		}, atomic.LoadInt64(&c.totalSize))

		// Make the load data process async, in case that loading data takes longer the
		// lease duration, then the loaded data get staled and that process repeats forever.
		go func() {
			start := time.Now()
			mb, startTS, totalSize, err := c.loadDataFromOriginalTable(store)
			metrics.LoadTableCacheDurationHistogram.Observe(time.Since(start).Seconds())
			if err != nil {
				log.Info("load data from table fail", zap.Error(err))
				return
			}

			tmp := c.cacheData.Load()
			if tmp != nil && tmp.Start == ts {
				c.setCacheData(&cacheData{
					Start:     startTS,
					Lease:     tmp.Lease,
					MemBuffer: mb,
				}, totalSize)
			}
		}()
	}
	// Current status is not suitable to cache.
}

const cachedTableSizeLimit = 64 * (1 << 20)

// AddRecord implements the AddRecord method for the table.Table interface.
func (c *cachedTable) AddRecord(sctx table.MutateContext, txn kv.Transaction, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	if atomic.LoadInt64(&c.totalSize) > cachedTableSizeLimit {
		return nil, table.ErrOptOnCacheTable.GenWithStackByArgs("table too large")
	}
	txnCtxAddCachedTable(sctx, c.GetPhysicalID(), c)
	recordID, err = c.TableCommon.AddRecord(sctx, txn, r, opts...)
	if err == nil && recordID != nil {
		txnCtxAddCachedTableHandleRange(sctx, c.GetPhysicalID(), recordID)
	}
	return recordID, err
}

func txnCtxAddCachedTable(sctx table.MutateContext, tid int64, handle table.CachedTable) {
	if s, ok := sctx.GetCachedTableSupport(); ok {
		s.AddCachedTableHandleToTxn(tid, handle)
	}
}

func txnCtxAddCachedTableInvalidationRange(sctx table.MutateContext, tid int64, startKey, endKey kv.Key) {
	if s, ok := sctx.GetCachedTableSupport(); ok {
		s.AddCachedTableInvalidationRangeToTxn(tid, startKey, endKey)
	}
}

func txnCtxAddCachedTableHandleRange(sctx table.MutateContext, tid int64, h kv.Handle) {
	if h == nil {
		return
	}
	startKey := tablecodec.EncodeRowKeyWithHandle(tid, h)
	txnCtxAddCachedTableInvalidationRange(sctx, tid, startKey, startKey.Next())
}

// UpdateRecord implements table.Table
func (c *cachedTable) UpdateRecord(ctx table.MutateContext, txn kv.Transaction, h kv.Handle, oldData, newData []types.Datum, touched []bool, opts ...table.UpdateRecordOption) error {
	// Prevent furthur writing when the table is already too large.
	if atomic.LoadInt64(&c.totalSize) > cachedTableSizeLimit {
		return table.ErrOptOnCacheTable.GenWithStackByArgs("table too large")
	}
	txnCtxAddCachedTable(ctx, c.GetPhysicalID(), c)
	err := c.TableCommon.UpdateRecord(ctx, txn, h, oldData, newData, touched, opts...)
	if err == nil {
		txnCtxAddCachedTableHandleRange(ctx, c.GetPhysicalID(), h)
	}
	return err
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (c *cachedTable) RemoveRecord(sctx table.MutateContext, txn kv.Transaction, h kv.Handle, r []types.Datum, opts ...table.RemoveRecordOption) error {
	txnCtxAddCachedTable(sctx, c.GetPhysicalID(), c)
	err := c.TableCommon.RemoveRecord(sctx, txn, h, r, opts...)
	if err == nil {
		txnCtxAddCachedTableHandleRange(sctx, c.GetPhysicalID(), h)
	}
	return err
}

// TestMockRenewLeaseABA2 is used by test function TestRenewLeaseABAFailPoint.
var TestMockRenewLeaseABA2 chan struct{}

func (c *cachedTable) renewLease(handle StateRemote, ts uint64, data *cacheData, leaseDuration time.Duration) {
	failpoint.Inject("mockRenewLeaseABA2", func(_ failpoint.Value) {
		c.PutStateRemoteHandle(handle)
		<-TestMockRenewLeaseABA2
		c.TakeStateRemoteHandle()
	})

	defer c.PutStateRemoteHandle(handle)

	tid := c.GetPhysicalID()
	lease := leaseFromTS(ts, leaseDuration)
	newLease, err := handle.RenewReadLease(context.Background(), tid, data.Lease, lease)
	if err != nil {
		if !kv.IsTxnRetryableError(err) {
			log.Warn("Renew read lease error", zap.Error(err))
		}
		return
	}
	if newLease > 0 {
		c.setCacheData(&cacheData{
			Start:     data.Start,
			Lease:     newLease,
			MemBuffer: data.MemBuffer,
		}, atomic.LoadInt64(&c.totalSize))
	}

	failpoint.Inject("mockRenewLeaseABA2", func(_ failpoint.Value) {
		TestMockRenewLeaseABA2 <- struct{}{}
	})
}

const cacheTableWriteLease = 5 * time.Second

func (c *cachedTable) WriteLockAndKeepAlive(ctx context.Context, exit chan struct{}, leasePtr *uint64, wg chan error) {
	writeLockLease, err := c.lockForWrite(ctx)
	atomic.StoreUint64(leasePtr, writeLockLease)
	wg <- err
	if err != nil {
		logutil.Logger(ctx).Warn("lock for write lock fail", zap.String("category", "cached table"), zap.Error(err))
		return
	}

	t := time.NewTicker(cacheTableWriteLease / 2)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if err := c.renew(ctx, leasePtr); err != nil {
				logutil.Logger(ctx).Warn("renew write lock lease fail", zap.String("category", "cached table"), zap.Error(err))
				return
			}
		case <-exit:
			return
		}
	}
}

// ApplyLocalInvalidation implements table.CachedTable.
func (c *cachedTable) ApplyLocalInvalidation(epoch, commitTS uint64) int {
	return c.ApplyLocalInvalidationByRanges(epoch, commitTS, nil)
}

// ApplyLocalInvalidationByRanges applies local invalidation with optional row-key ranges.
func (c *cachedTable) ApplyLocalInvalidationByRanges(epoch, commitTS uint64, ranges []kv.KeyRange) int {
	spans := make([]keySpan, 0, len(ranges))
	for _, keyRange := range ranges {
		if len(keyRange.StartKey) == 0 || len(keyRange.EndKey) == 0 {
			spans = nil
			break
		}
		spans = append(spans, keySpan{
			start: keyRange.StartKey.Clone(),
			end:   keyRange.EndKey.Clone(),
		})
	}
	return c.applyInvalidation(cacheInvalidationEvent{
		tableID:    c.tableID,
		physicalID: c.GetPhysicalID(),
		epoch:      epoch,
		commitTS:   commitTS,
		spans:      spans,
	})
}

func (c *cachedTable) renew(ctx context.Context, leasePtr *uint64) error {
	oldLease := atomic.LoadUint64(leasePtr)
	physicalTime := oracle.GetTimeFromTS(oldLease)
	newLease := oracle.GoTimeToTS(physicalTime.Add(cacheTableWriteLease))

	h := c.TakeStateRemoteHandle()
	defer c.PutStateRemoteHandle(h)

	succ, err := h.RenewWriteLease(ctx, c.GetPhysicalID(), newLease)
	if err != nil {
		return errors.Trace(err)
	}
	if succ {
		atomic.StoreUint64(leasePtr, newLease)
	}
	return nil
}

func (c *cachedTable) lockForWrite(ctx context.Context) (uint64, error) {
	handle := c.TakeStateRemoteHandle()
	defer c.PutStateRemoteHandle(handle)

	return handle.LockForWrite(ctx, c.GetPhysicalID(), cacheTableWriteLease)
}
