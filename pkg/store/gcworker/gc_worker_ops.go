// Copyright 2017 PingCAP, Inc.
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

package gcworker

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/tablecodec"
	util2 "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	tikverr "github.com/tikv/client-go/v2/error"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/rangetask"
	tikvutil "github.com/tikv/client-go/v2/util"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/constants"
	"go.uber.org/zap"
)

func (w *GCWorker) deleteRanges(
	ctx context.Context,
	safePoint uint64,
	concurrency gcConcurrency,
) error {
	metrics.GCWorkerCounter.WithLabelValues("delete_range").Inc()

	s := createSession(w.store)
	defer s.Close()
	ranges, err := util.LoadDeleteRanges(ctx, s, safePoint)
	if err != nil {
		return errors.Trace(err)
	}

	v2, err := util.IsRaftKv2(ctx, s)
	if err != nil {
		return errors.Trace(err)
	}
	// Cache table ids on which placement rules have been GC-ed, to avoid redundantly GC the same table id multiple times.
	var gcPlacementRuleCache sync.Map

	logutil.Logger(ctx).Info("start delete ranges", zap.String("category", "gc worker"),
		zap.String("uuid", w.uuid),
		zap.Int("ranges", len(ranges)))
	startTime := time.Now()

	deleteRangeConcurrency := w.calcDeleteRangeConcurrency(concurrency, len(ranges))
	concurrencyLimiter := make(chan struct{}, deleteRangeConcurrency)

	f := func(r util.DelRangeTask) {
		var err error
		defer func() {
			<-concurrencyLimiter
		}()
		se := createSession(w.store)
		defer se.Close()

		startKey, endKey := r.Range()
		if v2 {
			// In raftstore-v2, we use delete range instead to avoid deletion omission
			task := rangetask.NewDeleteRangeTask(w.tikvStore, startKey, endKey, deleteRangeConcurrency)
			err = task.Execute(ctx)
		} else {
			err = w.doUnsafeDestroyRangeRequest(ctx, startKey, endKey)
		}
		failpoint.Inject("ignoreDeleteRangeFailed", func() {
			err = nil
		})

		if err != nil {
			logutil.Logger(ctx).Warn("delete range failed on range", zap.String("category", "gc worker"),
				zap.String("uuid", w.uuid),
				zap.Stringer("startKey", startKey),
				zap.Stringer("endKey", endKey),
				zap.Error(err))
			return
		}

		err = doGCPlacementRules(se, safePoint, r, &gcPlacementRuleCache)
		if err != nil {
			logutil.Logger(ctx).Warn("gc placement rules failed on range", zap.String("category", "gc worker"),
				zap.String("uuid", w.uuid),
				zap.Int64("jobID", r.JobID),
				zap.Int64("elementID", r.ElementID),
				zap.Error(err))
			return
		}
		// We only delete rules, so concurrently updating rules should not return errors.
		if err := w.doGCLabelRules(r); err != nil {
			logutil.Logger(ctx).Error("gc label rules failed on range", zap.String("category", "gc worker"),
				zap.String("uuid", w.uuid),
				zap.Int64("jobID", r.JobID),
				zap.Int64("elementID", r.ElementID),
				zap.Error(err))
			return
		}

		err = util.CompleteDeleteRange(se, r, !v2)
		if err != nil {
			logutil.Logger(ctx).Error("failed to mark delete range task done", zap.String("category", "gc worker"),
				zap.String("uuid", w.uuid),
				zap.Stringer("startKey", startKey),
				zap.Stringer("endKey", endKey),
				zap.Error(err))
			metrics.GCUnsafeDestroyRangeFailuresCounterVec.WithLabelValues("save").Inc()
		}
	}
	var wg util2.WaitGroupWrapper
	for i := range ranges {
		r := ranges[i]
		concurrencyLimiter <- struct{}{}
		wg.Run(func() { f(r) })
	}
	wg.Wait()

	logutil.Logger(ctx).Info("finish delete ranges", zap.String("category", "gc worker"),
		zap.String("uuid", w.uuid),
		zap.Int("num of ranges", len(ranges)),
		zap.Duration("cost time", time.Since(startTime)))
	metrics.GCHistogram.WithLabelValues("delete_ranges").Observe(time.Since(startTime).Seconds())
	return nil
}

const (
	// ConcurrencyDivisor reduces the input concurrency to avoid overwhelming the system
	ConcurrencyDivisor = 4
	// RequestsPerThread is the number of requests handled by a single thread
	RequestsPerThread = 100000
)

// calcDeleteRangeConcurrency calculates the concurrency of deleteRanges.
//
// There was only one concurrency for resolveLocks. When parallelizing deleteRanges, its concurrency is controlled by
// the same variable TiDBGCConcurrency. As requested by PM, the first priority is to ensure the stability of the system,
// so the concurrency of deleteRanges is reduced to avoid overwhelming the system.
//
// Assuming an average request takes 50ms:
// With ideal parallelism and sufficient concurrency,
// the maximum duration for a round of deleteRanges is 100,000 * 50ms = 5,000s.
// These values are conservatively chosen to minimize GC impact on foreground requests
func (w *GCWorker) calcDeleteRangeConcurrency(concurrency gcConcurrency, rangeNum int) int {
	maxConcurrency := max(1, concurrency.v/ConcurrencyDivisor)
	threadsBasedOnRequests := max(1, rangeNum/RequestsPerThread)
	if concurrency.isAuto {
		return min(maxConcurrency, threadsBasedOnRequests)
	}
	return maxConcurrency
}

// redoDeleteRanges checks all deleted ranges whose ts is at least `lifetime + 24h` ago. See TiKV RFC #2.
// `concurrency` specifies the concurrency to send NotifyDeleteRange.
func (w *GCWorker) redoDeleteRanges(ctx context.Context, safePoint uint64,
	concurrency gcConcurrency) error {
	metrics.GCWorkerCounter.WithLabelValues("redo_delete_range").Inc()

	// We check delete range records that are deleted about 24 hours ago.
	redoDeleteRangesTs := safePoint - oracle.ComposeTS(int64(gcRedoDeleteRangeDelay.Seconds())*1000, 0)

	se := createSession(w.store)
	ranges, err := util.LoadDoneDeleteRanges(ctx, se, redoDeleteRangesTs)
	se.Close()
	if err != nil {
		return errors.Trace(err)
	}

	logutil.Logger(ctx).Info("start redo-delete ranges", zap.String("category", "gc worker"),
		zap.String("uuid", w.uuid),
		zap.Int("num of ranges", len(ranges)))
	startTime := time.Now()

	deleteRangeConcurrency := w.calcDeleteRangeConcurrency(concurrency, len(ranges))
	concurrencyLimiter := make(chan struct{}, deleteRangeConcurrency)

	f := func(r util.DelRangeTask) {
		defer func() {
			<-concurrencyLimiter
		}()
		var err error
		startKey, endKey := r.Range()

		err = w.doUnsafeDestroyRangeRequest(ctx, startKey, endKey)
		if err != nil {
			logutil.Logger(ctx).Warn("redo-delete range failed on range", zap.String("category", "gc worker"),
				zap.String("uuid", w.uuid),
				zap.Stringer("startKey", startKey),
				zap.Stringer("endKey", endKey),
				zap.Error(err))
			return
		}
		se := createSession(w.store)
		err = util.DeleteDoneRecord(se, r)
		se.Close()
		if err != nil {
			logutil.Logger(ctx).Error("failed to remove delete_range_done record", zap.String("category", "gc worker"),
				zap.String("uuid", w.uuid),
				zap.Stringer("startKey", startKey),
				zap.Stringer("endKey", endKey),
				zap.Error(err))
			metrics.GCUnsafeDestroyRangeFailuresCounterVec.WithLabelValues("save_redo").Inc()
		}
	}
	var wg util2.WaitGroupWrapper
	for i := range ranges {
		r := ranges[i]
		concurrencyLimiter <- struct{}{}
		wg.Run(func() { f(r) })
	}
	wg.Wait()
	logutil.Logger(ctx).Info("finish redo-delete ranges", zap.String("category", "gc worker"),
		zap.String("uuid", w.uuid),
		zap.Int("num of ranges", len(ranges)),
		zap.Duration("cost time", time.Since(startTime)))
	metrics.GCHistogram.WithLabelValues("redo_delete_ranges").Observe(time.Since(startTime).Seconds())
	return nil
}

func (w *GCWorker) doUnsafeDestroyRangeRequest(
	ctx context.Context, startKey []byte, endKey []byte,
) error {
	// Get all stores every time deleting a region. So the store list is less probably to be stale.
	stores, err := w.getStoresForGC(ctx)
	if err != nil {
		logutil.Logger(ctx).Error("delete ranges: got an error while trying to get store list from PD", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCUnsafeDestroyRangeFailuresCounterVec.WithLabelValues("get_stores").Inc()
		return errors.Trace(err)
	}

	req := tikvrpc.NewRequest(tikvrpc.CmdUnsafeDestroyRange, &kvrpcpb.UnsafeDestroyRangeRequest{
		StartKey: startKey,
		EndKey:   endKey,
	}, kvrpcpb.Context{DiskFullOpt: kvrpcpb.DiskFullOpt_AllowedOnAlmostFull})

	var wg sync.WaitGroup
	errChan := make(chan error, len(stores))

	for _, store := range stores {
		address := store.Address
		storeID := store.Id
		wg.Add(1)
		go func() {
			defer wg.Done()

			resp, err1 := w.tikvStore.GetTiKVClient().SendRequest(ctx, address, req, unsafeDestroyRangeTimeout)
			if err1 == nil {
				if resp == nil || resp.Resp == nil {
					err1 = errors.Errorf("unsafe destroy range returns nil response from store %v", storeID)
				} else {
					errStr := (resp.Resp.(*kvrpcpb.UnsafeDestroyRangeResponse)).Error
					if len(errStr) > 0 {
						err1 = errors.Errorf("unsafe destroy range failed on store %v: %s", storeID, errStr)
					}
				}
			}

			if err1 != nil {
				metrics.GCUnsafeDestroyRangeFailuresCounterVec.WithLabelValues("send").Inc()
			}
			errChan <- err1
		}()
	}

	var errs []string
	for range stores {
		err1 := <-errChan
		if err1 != nil {
			errs = append(errs, err1.Error())
		}
	}

	wg.Wait()

	if len(errs) > 0 {
		return errors.Errorf("[gc worker] destroy range finished with errors: %v", errs)
	}

	return nil
}

// needsGCOperationForStore checks if the store-level requests related to GC needs to be sent to the store. The store-level
// requests includes UnsafeDestroyRange, PhysicalScanLock, etc.
func needsGCOperationForStore(store *metapb.Store) (bool, error) {
	// TombStone means the store has been removed from the cluster and there isn't any peer on the store, so needn't do GC for it.
	// Offline means the store is being removed from the cluster and it becomes tombstone after all peers are removed from it,
	// so we need to do GC for it.
	if store.State == metapb.StoreState_Tombstone {
		return false, nil
	}

	engineLabel := ""
	for _, label := range store.GetLabels() {
		if label.GetKey() == placement.EngineLabelKey {
			engineLabel = label.GetValue()
			break
		}
	}

	switch engineLabel {
	case placement.EngineLabelTiFlash:
		// For a TiFlash node, it uses other approach to delete dropped tables, so it's safe to skip sending
		// UnsafeDestroyRange requests; it has only learner peers and their data must exist in TiKV, so it's safe to
		// skip physical resolve locks for it.
		return false, nil

	case placement.EngineLabelTiFlashCompute:
		logutil.BgLogger().Debug("will ignore gc tiflash_compute node", zap.String("category", "gc worker"))
		return false, nil

	case placement.EngineLabelTiKV, "":
		// If no engine label is set, it should be a TiKV node.
		return true, nil

	default:
		return true, errors.Errorf("unsupported store engine \"%v\" with storeID %v, addr %v",
			engineLabel,
			store.GetId(),
			store.GetAddress())
	}
}

// getStoresForGC gets the list of stores that needs to be processed during GC.
func (w *GCWorker) getStoresForGC(ctx context.Context) ([]*metapb.Store, error) {
	stores, err := w.pdClient.GetAllStores(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	upStores := make([]*metapb.Store, 0, len(stores))
	for _, store := range stores {
		needsGCOp, err := needsGCOperationForStore(store)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if needsGCOp {
			upStores = append(upStores, store)
		}
	}
	return upStores, nil
}

func (w *GCWorker) loadGCConcurrencyWithDefault() (int, error) {
	str, err := w.loadValueFromSysTable(gcConcurrencyKey)
	if err != nil {
		return gcDefaultConcurrency, errors.Trace(err)
	}
	if str == "" {
		err = w.saveValueToSysTable(gcConcurrencyKey, strconv.Itoa(gcDefaultConcurrency))
		if err != nil {
			return gcDefaultConcurrency, errors.Trace(err)
		}
		return gcDefaultConcurrency, nil
	}

	jobConcurrency, err := strconv.Atoi(str)
	if err != nil {
		return gcDefaultConcurrency, err
	}

	if jobConcurrency < gcMinConcurrency {
		jobConcurrency = gcMinConcurrency
	}

	if jobConcurrency > gcMaxConcurrency {
		jobConcurrency = gcMaxConcurrency
	}

	return jobConcurrency, nil
}

// Central mode is deprecated in v5.0. This function will always return true.
func (w *GCWorker) checkUseDistributedGC() bool {
	mode, err := w.loadValueFromSysTable(gcModeKey)
	if err == nil && mode == "" {
		err = w.saveValueToSysTable(gcModeKey, gcModeDefault)
	}
	if err != nil {
		logutil.BgLogger().Error("failed to load gc mode, fall back to distributed mode", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("check_gc_mode").Inc()
	} else if strings.EqualFold(mode, gcModeCentral) {
		logutil.BgLogger().Warn("user configured to use central mode GC, which is no longer available. distributed mode will still be used", zap.String("category", "gc worker"))
	} else if !strings.EqualFold(mode, gcModeDistributed) {
		logutil.BgLogger().Warn("distributed mode will be used", zap.String("category", "gc worker"),
			zap.String("invalid gc mode", mode))
	}
	return true
}

func (w *GCWorker) resolveLocks(
	ctx context.Context,
	txnSafePoint uint64,
	concurrency int,
) error {
	metrics.GCWorkerCounter.WithLabelValues("resolve_locks").Inc()
	logutil.Logger(ctx).Info("start resolve locks", zap.String("category", "gc worker"),
		zap.String("uuid", w.uuid),
		zap.Uint64("txnSafePoint", txnSafePoint),
		zap.Int("concurrency", concurrency))
	startTime := time.Now()

	handler := func(ctx context.Context, r tikvstore.KeyRange) (rangetask.TaskStat, error) {
		scanLimit := uint32(tikv.GCScanLockLimit)
		failpoint.Inject("lowScanLockLimit", func() {
			scanLimit = 3
		})
		// ResolveLocksForRange accepts a `max_version`, instead of the txn safe point, which means the maximum
		// (inclusive) start ts of locks that should be resolved. But in our current definition, GC at some txn safe
		// point should guarantee transactions with start ts >= txn safe point to be valid. Therefore we pass
		// txnSafePoint - 1 to ResolveLocksForRange.
		return tikv.ResolveLocksForRange(ctx, w.regionLockResolver, txnSafePoint-1, r.StartKey, r.EndKey, tikv.NewGcResolveLockMaxBackoffer, scanLimit)
	}

	runnerName := "resolve-locks-runner"
	if w.keyspaceID != constants.NullKeyspaceID && w.store != nil {
		runnerName += "-" + w.store.GetCodec().GetKeyspaceMeta().GetName()
	}

	runner := rangetask.NewRangeTaskRunner(runnerName, w.tikvStore, concurrency, handler)

	// w.store may be nil in some test environments.
	isNullKeyspace := w.store == nil || w.store.GetCodec().GetKeyspace() == nil
	var keyspaceBatch []*keyspacepb.KeyspaceMeta

	// Failpoint to override the batch size for faster test
	loadKeyspacesBatchSize := uint32(loadAllKeyspacesForUnifiedGCBatchSize)
	failpoint.Inject("overrideLoadKeyspacesBatchSize", func(val failpoint.Value) {
		v, ok := val.(int)
		if !ok {
			panic(fmt.Sprintf("invalid argument for failpoint overrideLoadKeyspacesBatchSize: expected integer, got %T: %v", val, val))
		}
		loadKeyspacesBatchSize = uint32(v)
	})

	// Counter for tests to check how many batches was done during resolving locks.
	loadKeyspacesBatchCount := 0
	defer func() {
		failpoint.InjectCall("getLoadKeyspacesBatchCount", loadKeyspacesBatchCount)
	}()

	if isNullKeyspace {
		var err error
		keyspaceBatch, err = w.pdClient.GetAllKeyspaces(ctx, 0, loadKeyspacesBatchSize)
		loadKeyspacesBatchCount++
		if err != nil {
			return err
		}
	}

	// * If the current keyspace is not the null keyspace, then it must be a keyspace with keyspace level GC enabled.
	//   in this case, resolve locks on the unbounded range, and the keyspace prefix will be automatically
	//   attached.
	// * If there are no keyspaces in the cluster at all, resolve locks for the unbounded whole key range.
	if !isNullKeyspace || len(keyspaceBatch) == 0 {
		err := runner.RunOnRange(ctx, []byte(""), []byte(""))
		if err != nil {
			logutil.Logger(ctx).Warn("resolve locks failed", zap.String("category", "gc worker"),
				zap.String("uuid", w.uuid),
				zap.Uint64("txnSafePoint", txnSafePoint),
				zap.Error(err))
			return errors.Trace(err)
		}

		logutil.Logger(ctx).Info("finish resolve locks", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Uint64("txnSafePoint", txnSafePoint),
			zap.Int("regions", runner.CompletedRegions()))
		metrics.GCHistogram.WithLabelValues("resolve_locks").Observe(time.Since(startTime).Seconds())
		return nil
	}

	// Otherwise, the null keyspace, which is the current keyspace, has the responsibility to resolve locks for
	// other keyspaces that are configured running unified GC, but skip keyspaces that use keyspace level GC.

	isSuccessful := true

	// First, resolve locks for the null keyspace (txn key range).
	{
		nullKeyspaceExcludePrefixes := tikv.CodecV1ExcludePrefixes()
		nullKeyspaceKeyRanges := make([]tikvstore.KeyRange, 0, len(nullKeyspaceExcludePrefixes)+1)
		nextStartKey := []byte("")

		for _, prefix := range nullKeyspaceExcludePrefixes {
			nullKeyspaceKeyRanges = append(nullKeyspaceKeyRanges, tikvstore.KeyRange{
				StartKey: nextStartKey,
				EndKey:   prefix,
			})
			nextStartKey = tikvstore.PrefixNextKey(prefix)
		}

		// Add the remaining range that after the last excluded prefix and until the global end.
		// But there's theoretically a special case: if there are prefixes that has been processed but `nextStartKey`
		// is set to empty, it means that there exist a prefix containing only `0xff` bytes, causing the `PrefixNextKey`
		// giving the global end. In this case, there isn't such a remaining range.
		// Currently, there's no such kind of prefix in use, but we handle this special case for strictness.
		if !(len(nullKeyspaceExcludePrefixes) > 0 && len(nextStartKey) == 0) {
			nullKeyspaceKeyRanges = append(nullKeyspaceKeyRanges, tikvstore.KeyRange{
				StartKey: nextStartKey,
				EndKey:   []byte(""),
			})
		}

		for _, r := range nullKeyspaceKeyRanges {
			err := runner.RunOnRange(ctx, r.StartKey, r.EndKey)
			if err != nil {
				logutil.Logger(ctx).Warn("resolve locks for null keyspace sub-range failed", zap.String("category", "gc worker"),
					zap.String("uuid", w.uuid),
					zap.Uint64("txnSafePoint", txnSafePoint),
					zap.String("subRangeStartKey", hex.EncodeToString(r.StartKey)),
					zap.String("subRangeEndKey", hex.EncodeToString(r.EndKey)),
					zap.Error(err))
				isSuccessful = false
			}
		}
	}

	// Then, resolve locks for keyspaces with Unified GC enabled, if any.
	for {
		// The first batch has already been fetched. We fetch the next batch at the end of the outer loop.
		if len(keyspaceBatch) == 0 {
			break
		}

		for _, keyspace := range keyspaceBatch {
			if keyspace.GetState() != keyspacepb.KeyspaceState_ENABLED {
				continue
			}
			if pd.IsKeyspaceUsingKeyspaceLevelGC(keyspace) {
				continue
			}
			codecOfKeyspace, err := tikv.NewCodecV2(tikv.ModeTxn, keyspace)
			if err != nil {
				err = errors.Annotatef(err, "failed to find codec for keyspace when trying to resolve locks for it, keyspaceID: %v, keyspaceName: %v", keyspace.GetId(), keyspace.GetName())
				logutil.Logger(ctx).Warn("resolve locks for unified-GC keyspace failed", zap.String("category", "gc worker"),
					zap.String("uuid", w.uuid),
					zap.Uint64("txnSafePoint", txnSafePoint),
					zap.Error(err))
				isSuccessful = false
				continue
			}
			startKey, endKey := codecOfKeyspace.EncodeRange([]byte(""), []byte(""))
			err = runner.RunOnRange(ctx, startKey, endKey)
			if err != nil {
				logutil.Logger(ctx).Warn("resolve locks for unified-GC keyspace failed", zap.String("category", "gc worker"),
					zap.String("uuid", w.uuid),
					zap.Uint64("txnSafePoint", txnSafePoint),
					zap.Error(err))
				isSuccessful = false
				continue
			}
		}

		// The current batch of keyspaces has been processed. Continue next batch.
		// The keyspaceBatch must be non-nil here, otherwise the outer loop should have been finished.
		nextKeyspaceID := keyspaceBatch[len(keyspaceBatch)-1].GetId() + 1
		if nextKeyspaceID > constants.MaxKeyspaceID {
			break
		}
		var err error
		keyspaceBatch, err = w.pdClient.GetAllKeyspaces(ctx, nextKeyspaceID, loadKeyspacesBatchSize)
		loadKeyspacesBatchCount++
		if err != nil {
			return err
		}
	}

	if !isSuccessful {
		return errors.New("resolve locks is not completely successful")
	}
	return nil
}

const gcOneRegionMaxBackoff = 20000

func (w *GCWorker) broadcastGCSafePoint(ctx context.Context, gcSafePoint uint64) error {
	result, err := w.pdGCControllerClient.AdvanceGCSafePoint(ctx, gcSafePoint)
	if err != nil {
		logutil.Logger(ctx).Error("failed to broadcast gc safe point", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Uint64("gcSafePoint", gcSafePoint),
			zap.Error(err))
		return errors.Trace(err)
	}

	if result.NewGCSafePoint != gcSafePoint {
		logutil.Logger(ctx).Warn("gc safe point not advanced to the expected value",
			zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Uint64("target", gcSafePoint),
			zap.Uint64("oldGCSafePoint", result.OldGCSafePoint),
			zap.Uint64("newGCSafePoint", result.NewGCSafePoint))
	}

	return nil
}

// doGCForRegion used for gc for region.
// these two errors should not return together, for more, see the func 'doGC'
func (w *GCWorker) doGCForRegion(bo *tikv.Backoffer, safePoint uint64, region tikv.RegionVerID) (*errorpb.Error, error) {
	req := tikvrpc.NewRequest(tikvrpc.CmdGC, &kvrpcpb.GCRequest{
		SafePoint: safePoint,
	})

	resp, err := w.tikvStore.SendReq(bo, req, region, gcTimeout)
	if err != nil {
		return nil, errors.Trace(err)
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if regionErr != nil {
		return regionErr, nil
	}

	if resp.Resp == nil {
		return nil, errors.Trace(tikverr.ErrBodyMissing)
	}
	gcResp := resp.Resp.(*kvrpcpb.GCResponse)
	if gcResp.GetError() != nil {
		return nil, errors.Errorf("unexpected gc error: %s", gcResp.GetError())
	}

	return nil, nil
}

func (w *GCWorker) checkLeader(ctx context.Context) (bool, error) {
	var metricLabel string
	if !pd.IsKeyspaceUsingKeyspaceLevelGC(w.store.GetCodec().GetKeyspaceMeta()) {
		metricLabel = "check_leader"
	} else {
		metricLabel = "check_leader_keyspace"
	}
	metrics.GCWorkerCounter.WithLabelValues(metricLabel).Inc()
	se := createSession(w.store)
	defer se.Close()

	_, err := se.ExecuteInternal(ctx, "BEGIN")
	if err != nil {
		return false, errors.Trace(err)
	}
	leader, err := w.loadValueFromSysTable(gcLeaderUUIDKey)
	if err != nil {
		se.RollbackTxn(ctx)
		return false, errors.Trace(err)
	}
	logutil.BgLogger().Debug("got leader", zap.String("category", "gc worker"), zap.String("uuid", leader))
	if leader == w.uuid {
		err = w.saveTime(gcLeaderLeaseKey, time.Now().Add(gcWorkerLease))
		if err != nil {
			se.RollbackTxn(ctx)
			return false, errors.Trace(err)
		}
		err = se.CommitTxn(ctx)
		if err != nil {
			return false, errors.Trace(err)
		}
		return true, nil
	}

	se.RollbackTxn(ctx)

	_, err = se.ExecuteInternal(ctx, "BEGIN")
	if err != nil {
		return false, errors.Trace(err)
	}
	lease, err := w.loadTime(gcLeaderLeaseKey)
	if err != nil {
		se.RollbackTxn(ctx)
		return false, errors.Trace(err)
	}
	if lease == nil || lease.Before(time.Now()) {
		logutil.BgLogger().Debug("register as leader", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid))
		metrics.GCWorkerCounter.WithLabelValues("register_leader").Inc()

		err = w.saveValueToSysTable(gcLeaderUUIDKey, w.uuid)
		if err != nil {
			se.RollbackTxn(ctx)
			return false, errors.Trace(err)
		}
		err = w.saveValueToSysTable(gcLeaderDescKey, w.desc)
		if err != nil {
			se.RollbackTxn(ctx)
			return false, errors.Trace(err)
		}
		err = w.saveTime(gcLeaderLeaseKey, time.Now().Add(gcWorkerLease))
		if err != nil {
			se.RollbackTxn(ctx)
			return false, errors.Trace(err)
		}
		err = se.CommitTxn(ctx)
		if err != nil {
			return false, errors.Trace(err)
		}
		return true, nil
	}
	se.RollbackTxn(ctx)
	return false, nil
}

func (w *GCWorker) saveTime(key string, t time.Time) error {
	err := w.saveValueToSysTable(key, t.Format(tikvutil.GCTimeFormat))
	return errors.Trace(err)
}

func (w *GCWorker) loadTime(key string) (*time.Time, error) {
	str, err := w.loadValueFromSysTable(key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if str == "" {
		return nil, nil
	}
	t, err := tikvutil.CompatibleParseGCTime(str)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &t, nil
}

func (w *GCWorker) saveDuration(key string, d time.Duration) error {
	err := w.saveValueToSysTable(key, d.String())
	return errors.Trace(err)
}

func (w *GCWorker) loadDuration(key string) (*time.Duration, error) {
	str, err := w.loadValueFromSysTable(key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if str == "" {
		return nil, nil
	}
	d, err := time.ParseDuration(str)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &d, nil
}

func (w *GCWorker) loadDurationWithDefault(key string, def time.Duration) (*time.Duration, error) {
	d, err := w.loadDuration(key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if d == nil {
		err = w.saveDuration(key, def)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return &def, nil
	}
	return d, nil
}

func (w *GCWorker) loadValueFromSysTable(key string) (string, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnGC)
	se := createSession(w.store)
	defer se.Close()
	rs, err := se.ExecuteInternal(ctx, `SELECT HIGH_PRIORITY (variable_value) FROM mysql.tidb WHERE variable_name=%? FOR UPDATE`, key)
	if rs != nil {
		defer terror.Call(rs.Close)
	}
	if err != nil {
		return "", errors.Trace(err)
	}
	req := rs.NewChunk(nil)
	err = rs.Next(ctx, req)
	if err != nil {
		return "", errors.Trace(err)
	}
	if req.NumRows() == 0 {
		logutil.BgLogger().Debug("load kv", zap.String("category", "gc worker"),
			zap.String("key", key))
		return "", nil
	}
	value := req.GetRow(0).GetString(0)
	logutil.BgLogger().Debug("load kv", zap.String("category", "gc worker"),
		zap.String("key", key),
		zap.String("value", value))
	return value, nil
}

func (w *GCWorker) saveValueToSysTable(key, value string) error {
	const stmt = `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES (%?, %?, %?)
			       ON DUPLICATE KEY
			       UPDATE variable_value = %?, comment = %?`
	se := createSession(w.store)
	defer se.Close()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnGC)
	_, err := se.ExecuteInternal(ctx, stmt,
		key, value, gcVariableComments[key],
		value, gcVariableComments[key])
	logutil.BgLogger().Debug("save kv", zap.String("category", "gc worker"),
		zap.String("key", key),
		zap.String("value", value),
		zap.Error(err))
	return errors.Trace(err)
}

// GC placement rules when the partitions are removed by the GC worker.
// Placement rules cannot be removed immediately after drop table / truncate table,
// because the tables can be flashed back or recovered.
func doGCPlacementRules(se sessionapi.Session, _ uint64,
	dr util.DelRangeTask, gcPlacementRuleCache *sync.Map) (err error) {
	// Get the job from the job history
	var historyJob *model.Job
	failpoint.Inject("mockHistoryJobForGC", func(v failpoint.Value) {
		mockJ := &model.Job{
			Version: model.GetJobVerInUse(),
			ID:      dr.JobID,
			Type:    model.ActionDropTable,
			TableID: int64(v.(int)),
		}
		mockJ.FillFinishedArgs(&model.DropTableArgs{
			OldPartitionIDs: []int64{int64(v.(int))},
		})
		bytes, err1 := mockJ.Encode(true)
		if err1 != nil {
			return
		}
		historyJob = &model.Job{}
		err1 = historyJob.Decode(bytes)
		if err1 != nil {
			return
		}
	})
	if historyJob == nil {
		historyJob, err = ddl.GetHistoryJobByID(se, dr.JobID)
		if err != nil {
			return
		}
		if historyJob == nil {
			return dbterror.ErrDDLJobNotFound.GenWithStackByArgs(dr.JobID)
		}
	}

	// Notify PD to drop the placement rules of partition-ids and table-id, even if there may be no placement rules.
	var physicalTableIDs []int64
	switch historyJob.Type {
	case model.ActionDropTable:
		var args *model.DropTableArgs
		args, err = model.GetFinishedDropTableArgs(historyJob)
		if err != nil {
			return
		}
		physicalTableIDs = append(args.OldPartitionIDs, historyJob.TableID)
	case model.ActionTruncateTable, model.ActionTruncateTablePartition:
		var args *model.TruncateTableArgs
		args, err = model.GetFinishedTruncateTableArgs(historyJob)
		if err != nil {
			return
		}
		physicalTableIDs = args.OldPartitionIDs
		if historyJob.Type == model.ActionTruncateTable {
			physicalTableIDs = append(physicalTableIDs, historyJob.TableID)
		}
	case model.ActionDropTablePartition, model.ActionReorganizePartition,
		model.ActionRemovePartitioning, model.ActionAlterTablePartitioning:
		args, err2 := model.GetFinishedTablePartitionArgs(historyJob)
		if err2 != nil {
			return err2
		}
		physicalTableIDs = args.OldPhysicalTblIDs
	case model.ActionDropSchema:
		args, err2 := model.GetFinishedDropSchemaArgs(historyJob)
		if err2 != nil {
			return err2
		}
		physicalTableIDs = args.AllDroppedTableIDs
	}

	// Skip table ids that's already successfully handled.
	tmp := physicalTableIDs[:0]
	for _, id := range physicalTableIDs {
		if _, ok := gcPlacementRuleCache.Load(id); !ok {
			tmp = append(tmp, id)
		}
	}
	physicalTableIDs = tmp

	if len(physicalTableIDs) == 0 {
		return
	}

	if err := infosync.DeleteTiFlashPlacementRules(context.Background(), physicalTableIDs); err != nil {
		logutil.BgLogger().Error("delete placement rules failed", zap.Error(err), zap.Int64s("tableIDs", physicalTableIDs))
	}
	bundles := make([]*placement.Bundle, 0, len(physicalTableIDs))
	for _, id := range physicalTableIDs {
		bundles = append(bundles, placement.NewBundle(id))
	}
	err = infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), bundles)
	if err != nil {
		return
	}

	// Cache the table id if its related rule are deleted successfully.
	for _, id := range physicalTableIDs {
		gcPlacementRuleCache.Store(id, struct{}{})
	}
	return nil
}

func (w *GCWorker) doGCLabelRules(dr util.DelRangeTask) (err error) {
	// Get the job from the job history
	var historyJob *model.Job
	failpoint.Inject("mockHistoryJob", func(v failpoint.Value) {
		mockJ := &model.Job{
			Version: model.GetJobVerInUse(),
			ID:      dr.JobID,
			Type:    model.ActionDropTable,
		}
		mockJ.FillFinishedArgs(&model.DropTableArgs{
			OldRuleIDs: []string{v.(string)},
		})
		bytes, err1 := mockJ.Encode(true)
		if err1 != nil {
			return
		}
		historyJob = &model.Job{}
		if err1 = historyJob.Decode(bytes); err1 != nil {
			return
		}
	})
	if historyJob == nil {
		se := createSession(w.store)
		historyJob, err = ddl.GetHistoryJobByID(se, dr.JobID)
		se.Close()
		if err != nil {
			return
		}
		if historyJob == nil {
			return dbterror.ErrDDLJobNotFound.GenWithStackByArgs(dr.JobID)
		}
	}

	if historyJob.Type == model.ActionDropTable {
		var (
			args  *model.DropTableArgs
			rules map[string]*label.Rule
		)
		args, err = model.GetFinishedDropTableArgs(historyJob)
		if err != nil {
			return
		}
		physicalTableIDs, ruleIDs := args.OldPartitionIDs, args.OldRuleIDs
		// TODO: Here we need to get rules from PD and filter the rules which is not elegant. We should find a better way.
		rules, err = infosync.GetLabelRules(context.TODO(), ruleIDs)
		if err != nil {
			return
		}

		ruleIDs = getGCRules(append(physicalTableIDs, historyJob.TableID), rules)
		patch := label.NewRulePatch([]*label.Rule{}, ruleIDs)
		err = infosync.UpdateLabelRules(context.TODO(), patch)
	}
	return
}

func getGCRules(ids []int64, rules map[string]*label.Rule) []string {
	oldRange := make(map[string]struct{})
	for _, id := range ids {
		startKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(id)))
		endKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(id+1)))
		oldRange[startKey+endKey] = struct{}{}
	}

	var gcRules []string
	for _, rule := range rules {
		find := false
		for _, d := range rule.Data.([]any) {
			if r, ok := d.(map[string]any); ok {
				nowRange := fmt.Sprintf("%s%s", r["start_key"], r["end_key"])
				if _, ok := oldRange[nowRange]; ok {
					find = true
				}
			}
		}
		if find {
			gcRules = append(gcRules, rule.ID)
		}
	}
	return gcRules
}

// RunGCJob sends GC command to KV. It is exported for kv api, do not use it with GCWorker at the same time.
// only use for test
func RunGCJob(ctx context.Context, regionLockResolver tikv.RegionLockResolver, s tikv.Storage, pd pd.Client, safePoint uint64, identifier string, concurrency int) error {
	// Centralized GC is no longer available. Redirect to the distributed version silently.
	return RunDistributedGCJob(ctx, regionLockResolver, s, pd, safePoint, identifier, concurrency)
}

// RunDistributedGCJob notifies TiKVs to do GC. It is exported for kv api, do not use it with GCWorker at the same time.
// This function may not finish immediately because it may take some time to do resolveLocks.
// Param concurrency specifies the concurrency of resolveLocks phase.
func RunDistributedGCJob(ctx context.Context, regionLockResolver tikv.RegionLockResolver, s tikv.Storage, pd pd.Client, safePoint uint64, identifier string, concurrency int) error {
	gcWorker := &GCWorker{
		tikvStore:            s,
		uuid:                 identifier,
		keyspaceID:           constants.NullKeyspaceID,
		pdClient:             pd,
		pdGCControllerClient: pd.GetGCInternalController(constants.NullKeyspaceID),
		regionLockResolver:   regionLockResolver,
	}

	newTxnSafePoint, err := gcWorker.advanceTxnSafePoint(ctx, safePoint)
	if err != nil {
		return errors.Trace(err)
	}

	// Sync txn safe point
	time.Sleep(txnSafePointSyncWaitTime)

	err = gcWorker.resolveLocks(ctx, newTxnSafePoint, concurrency)
	if err != nil {
		return errors.Trace(err)
	}

	gcSafePoint := newTxnSafePoint

	err = gcWorker.broadcastGCSafePoint(ctx, gcSafePoint)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// RunResolveLocks resolves all locks before the safePoint.
// It is exported only for test, do not use it in the production environment.
func RunResolveLocks(ctx context.Context, s tikv.Storage, pd pd.Client, safePoint uint64, identifier string, concurrency int) error {
	gcWorker := &GCWorker{
		tikvStore:          s,
		uuid:               identifier,
		keyspaceID:         constants.NullKeyspaceID,
		pdClient:           pd,
		regionLockResolver: tikv.NewRegionLockResolver("test-resolver", s),
	}
	return gcWorker.resolveLocks(ctx, safePoint, concurrency)
}

// MockGCWorker is for test.
type MockGCWorker struct {
	worker *GCWorker
}

// NewMockGCWorker creates a MockGCWorker instance ONLY for test.
func NewMockGCWorker(store kv.Storage) (*MockGCWorker, error) {
	ver, err := store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return nil, errors.Trace(err)
	}
	hostName, err := os.Hostname()
	if err != nil {
		hostName = "unknown"
	}
	worker := &GCWorker{
		uuid:        strconv.FormatUint(ver.Ver, 16),
		desc:        fmt.Sprintf("host:%s, pid:%d, start at %s", hostName, os.Getpid(), time.Now()),
		keyspaceID:  constants.NullKeyspaceID,
		store:       store,
		tikvStore:   store.(tikv.Storage),
		gcIsRunning: false,
		lastFinish:  time.Now(),
		done:        make(chan error),
		pdClient:    store.(tikv.Storage).GetRegionCache().PDClient(),
	}
	return &MockGCWorker{worker: worker}, nil
}

// DeleteRanges calls deleteRanges internally, just for test.
func (w *MockGCWorker) DeleteRanges(ctx context.Context, safePoint uint64) error {
	logutil.Logger(ctx).Error("deleteRanges is called")
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnGC)
	return w.worker.deleteRanges(ctx, safePoint, gcConcurrency{1, false})
}
