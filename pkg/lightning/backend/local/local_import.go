// Copyright 2020 PingCAP, Inc.
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

package local

import (
	"bytes"
	"context"
	"encoding/hex"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/docker/go-units"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/ingestor/ingestcli"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/engine"
	"github.com/pingcap/tidb/pkg/util/intest"
	tidblogutil "github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/tikv/client-go/v2/oracle"
	tikvclient "github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/opt"
	"go.uber.org/zap"
)

func splitRangeBySizeProps(fullRange engineapi.Range, sizeProps *sizeProperties, sizeLimit int64, keysLimit int64) []engineapi.Range {
	ranges := make([]engineapi.Range, 0, sizeProps.totalSize/uint64(sizeLimit))
	curSize := uint64(0)
	curKeys := uint64(0)
	curKey := fullRange.Start

	sizeProps.iter(func(p *rangeProperty) bool {
		if bytes.Compare(p.Key, curKey) <= 0 {
			return true
		}
		if bytes.Compare(p.Key, fullRange.End) > 0 {
			return false
		}
		curSize += p.Size
		curKeys += p.Keys
		if int64(curSize) >= sizeLimit || int64(curKeys) >= keysLimit {
			ranges = append(ranges, engineapi.Range{Start: curKey, End: p.Key})
			curKey = p.Key
			curSize = 0
			curKeys = 0
		}
		return true
	})

	if bytes.Compare(curKey, fullRange.End) < 0 {
		// If the remaining range is too small, append it to last range.
		if len(ranges) > 0 && curKeys == 0 {
			ranges[len(ranges)-1].End = fullRange.End
		} else {
			ranges = append(ranges, engineapi.Range{Start: curKey, End: fullRange.End})
		}
	}
	return ranges
}

func getRegionSplitKeys(
	ctx context.Context,
	engine engineapi.Engine,
	sizeLimit int64,
	keysLimit int64,
) ([][]byte, error) {
	startKey, endKey, err := engine.GetKeyRange()
	if err != nil {
		return nil, err
	}
	if startKey == nil {
		return nil, errors.New("could not find first pair")
	}

	engineFileTotalSize, engineFileLength := engine.KVStatistics()

	if engineFileTotalSize <= sizeLimit && engineFileLength <= keysLimit {
		return [][]byte{startKey, endKey}, nil
	}

	logger := log.Wrap(tidblogutil.Logger(ctx)).With(zap.String("engine", engine.ID()))
	keys, err := engine.GetRegionSplitKeys()
	logger.Info("split engine key ranges",
		zap.Int64("totalSize", engineFileTotalSize),
		zap.Int64("totalCount", engineFileLength),
		logutil.Key("startKey", startKey), logutil.Key("endKey", endKey),
		zap.Int("len(keys)", len(keys)), zap.Error(err))
	return keys, err
}

// prepareAndSendJob will read the engine to get estimated key range, then split
// and scatter regions for these range and send region jobs to jobToWorkerCh.
func (local *Backend) prepareAndSendJob(
	ctx context.Context,
	engine engineapi.Engine,
	regionSplitKeys [][]byte,
	regionSplitSize, regionSplitKeyCnt int64,
	jobToWorkerCh chan<- *regionJob,
	jobWg *sync.WaitGroup,
) error {
	lfTotalSize, lfLength := engine.KVStatistics()
	splitRangesBatch := GetMaxBatchSplitRanges()
	maxRangesPerSec := GetMaxSplitRangePerSec()

	tidblogutil.Logger(ctx).Info("import engine ranges",
		zap.Int("len(regionSplitKeys)", len(regionSplitKeys)),
		zap.Int("splitRangesBatch", splitRangesBatch),
		zap.Float64("splitRangePerSec", maxRangesPerSec),
	)

	// if all the kv can fit in one region, skip split regions. TiDB will split one region for
	// the table when table is created.
	needSplit := len(regionSplitKeys) > 2 || lfTotalSize > regionSplitSize || lfLength > regionSplitKeyCnt
	// split region by given ranges
	failpoint.Inject("failToSplit", func(_ failpoint.Value) {
		needSplit = true
	})
	if needSplit {
		var err error
		logger := log.Wrap(tidblogutil.Logger(ctx)).With(zap.String("uuid", engine.ID())).Begin(zap.InfoLevel, "split and scatter ranges")
		backOffTime := 10 * time.Second
		maxbackoffTime := 120 * time.Second
		for i := range maxRetryTimes {
			failpoint.Inject("skipSplitAndScatter", func() {
				failpoint.Break()
			})

			err = local.splitAndScatterRegionInBatches(ctx, regionSplitKeys, splitRangesBatch, maxRangesPerSec)
			if err == nil || common.IsContextCanceledError(err) {
				break
			}

			tidblogutil.Logger(ctx).Warn("split and scatter failed in retry", zap.String("engine ID", engine.ID()),
				log.ShortError(err), zap.Int("retry", i))
			select {
			case <-time.After(backOffTime):
			case <-ctx.Done():
				return ctx.Err()
			}
			backOffTime *= 2
			if backOffTime > maxbackoffTime {
				backOffTime = maxbackoffTime
			}
		}
		logger.End(zap.ErrorLevel, err)
		if err != nil {
			return err
		}
	}

	return local.generateAndSendJob(
		ctx,
		engine,
		regionSplitSize,
		regionSplitKeyCnt,
		jobToWorkerCh,
		jobWg,
	)
}

// generateAndSendJob scans the region in ranges and send region jobs to jobToWorkerCh.
func (local *Backend) generateAndSendJob(
	ctx context.Context,
	engine engineapi.Engine,
	regionSplitSize, regionSplitKeys int64,
	jobToWorkerCh chan<- *regionJob,
	jobWg *sync.WaitGroup,
) error {
	eg, egCtx := util.NewErrorGroupWithRecoverWithCtx(ctx)

	dataAndRangeCh := make(chan engineapi.DataAndRanges)
	conn := int(local.WorkerConcurrency.Load())
	if _, ok := engine.(*external.Engine); ok {
		// currently external engine will generate a large IngestData, se we lower the
		// concurrency to pass backpressure to the LoadIngestData goroutine to avoid OOM
		conn = 1
	}
	for range conn {
		eg.Go(func() error {
			for {
				select {
				case <-egCtx.Done():
					return nil
				case p, ok := <-dataAndRangeCh:
					if !ok {
						return nil
					}

					failpoint.Inject("beforeGenerateJob", nil)
					failpoint.Inject("sendDummyJob", func(_ failpoint.Value) {
						// this is used to trigger worker failure, used together
						// with WriteToTiKVNotEnoughDiskSpace
						jobToWorkerCh <- &regionJob{}
						time.Sleep(5 * time.Second)
					})
					jobs, err := local.generateJobForRange(egCtx, p.Data, p.SortedRanges, regionSplitSize, regionSplitKeys)
					if err != nil {
						if common.IsContextCanceledError(err) {
							return nil
						}
						return err
					}
					// we need to increase the ref count before sending jobs to
					// jobToWorkerCh, in case some job finished quickly and decrease
					// the ref count to zero and cause the data being released.
					for _, job := range jobs {
						job.ref(jobWg)
					}
					for _, job := range jobs {
						select {
						case <-egCtx.Done():
							// this job is not put into jobToWorkerCh
							job.done(jobWg)
							// if the context is canceled, it means worker has error.
							return nil
						case jobToWorkerCh <- job:
						}
					}
				}
			}
		})
	}

	eg.Go(func() error {
		err := engine.LoadIngestData(egCtx, dataAndRangeCh)
		if err != nil {
			return errors.Trace(err)
		}
		close(dataAndRangeCh)
		return nil
	})

	return eg.Wait()
}

// fakeRegionJobs is used in test, the injected job can be found by (startKey, endKey).
var fakeRegionJobs map[[2]string]struct {
	jobs []*regionJob
	err  error
}

// generateJobForRange will scan the region in `keyRange` and generate region jobs.
// It will retry internally when scan region meet error.
func (local *Backend) generateJobForRange(
	ctx context.Context,
	data engineapi.IngestData,
	sortedJobRanges []engineapi.Range,
	regionSplitSize, regionSplitKeys int64,
) ([]*regionJob, error) {
	startOfAllRanges, endOfAllRanges := sortedJobRanges[0].Start, sortedJobRanges[len(sortedJobRanges)-1].End

	failpoint.Inject("fakeRegionJobs", func() {
		if ctx.Err() != nil {
			failpoint.Return(nil, ctx.Err())
		}
		key := [2]string{string(startOfAllRanges), string(endOfAllRanges)}
		injected := fakeRegionJobs[key]
		// overwrite the stage to regionScanned, because some time same sortedJobRanges
		// will be generated more than once.
		for _, job := range injected.jobs {
			job.stage = regionScanned
		}
		failpoint.Return(injected.jobs, injected.err)
	})

	pairStart, pairEnd, err := data.GetFirstAndLastKey(startOfAllRanges, endOfAllRanges)
	if err != nil {
		return nil, err
	}
	if pairStart == nil {
		logFn := tidblogutil.Logger(ctx).Info
		if _, ok := data.(*external.MemoryIngestData); ok {
			logFn = tidblogutil.Logger(ctx).Warn
		}
		logFn("There is no pairs in range",
			logutil.Key("startOfAllRanges", startOfAllRanges),
			logutil.Key("endOfAllRanges", endOfAllRanges))
		// trigger cleanup
		data.IncRef()
		data.DecRef()
		return nil, nil
	}

	startKey := codec.EncodeBytes([]byte{}, pairStart)
	endKey := codec.EncodeBytes([]byte{}, nextKey(pairEnd))
	regions, err := split.PaginateScanRegion(ctx, local.splitCli, startKey, endKey, scanRegionLimit)
	if err != nil {
		tidblogutil.Logger(ctx).Error("scan region failed",
			log.ShortError(err), zap.Int("region_len", len(regions)),
			logutil.Key("startKey", startKey),
			logutil.Key("endKey", endKey))
		return nil, err
	}

	jobs := newRegionJobs(regions, data, sortedJobRanges, regionSplitSize, regionSplitKeys, local.metrics)
	tidblogutil.Logger(ctx).Info("generate region jobs",
		zap.Int("len(jobs)", len(jobs)),
		zap.String("startOfAllRanges", hex.EncodeToString(startOfAllRanges)),
		zap.String("endOfAllRanges", hex.EncodeToString(endOfAllRanges)),
		zap.String("startKeyOfFirstRegion", hex.EncodeToString(regions[0].Region.GetStartKey())),
		zap.String("endKeyOfLastRegion", hex.EncodeToString(regions[len(regions)-1].Region.GetEndKey())),
	)
	return jobs, nil
}

func checkDiskAvail(ctx context.Context, store *pdhttp.StoreInfo) error {
	logger := log.Wrap(tidblogutil.Logger(ctx))
	capacity, err := units.RAMInBytes(store.Status.Capacity)
	if err != nil {
		logger.Warn("failed to parse capacity",
			zap.String("capacity", store.Status.Capacity), zap.Error(err))
		return nil
	}
	if capacity <= 0 {
		// PD will return a zero value StoreInfo if heartbeat is not received after
		// startup, skip temporarily.
		return nil
	}
	available, err := units.RAMInBytes(store.Status.Available)
	if err != nil {
		logger.Warn("failed to parse available",
			zap.String("available", store.Status.Available), zap.Error(err))
		return nil
	}
	ratio := available * 100 / capacity
	if ratio < 10 {
		storeType := "TiKV"
		if engine.IsTiFlashHTTPResp(&store.Store) {
			storeType = "TiFlash"
		}
		return errors.Errorf("the remaining storage capacity of %s(%s) is less than 10%%; please increase the storage capacity of %s and try again",
			storeType, store.Store.Address, storeType)
	}
	return nil
}

// GetExternalEngine returns the external engine by uuid.
// If the engine is not found or not an external engine, it returns nil.
// It's used to dynamically update the resource used by the external engine
func (local *Backend) GetExternalEngine(engineUUID uuid.UUID) *external.Engine {
	e, ok := local.engineMgr.getExternalEngine(engineUUID)
	if !ok {
		return nil
	}
	ext, ok := e.(*external.Engine)
	if !ok {
		return nil
	}
	return ext
}

// verifyImportedStatistics verifies the imported statistics for external engines.
// It checks if OnDuplicateKeyRecord or OnDuplicateKeyRemove is used (which are not yet implemented),
// and verifies that the imported KV count matches the expected one.
func verifyImportedStatistics(e engineapi.Engine, importedKVCount int64) error {
	// Check if OnDuplicateKeyRecord or OnDuplicateKeyRemove is used.
	// These options are not yet implemented for local backend, so we skip
	// the statistics verification and return an error to remind future
	// implementers to add support for this check.
	if extEngine, ok := e.(*external.Engine); ok {
		failpoint.Inject("skipOnDuplicateKeyCheck", func(_ failpoint.Value) {
			failpoint.Return(nil)
		})
		// Verify the imported statistics after import.
		// For external engine, use the total number of KVs loaded in LoadIngestData
		// (i.e., len(e.memKVsAndBuffers.kvs) across all batches) as the expected count.
		expectedKVCount := extEngine.GetTotalLoadedKVsCount()
		if importedKVCount != expectedKVCount {
			return errors.Errorf("imported length mismatch, expected %d, got %d", expectedKVCount, importedKVCount)
		}
	}
	return nil
}

// ImportEngine imports an engine to TiKV.
func (local *Backend) ImportEngine(
	ctx context.Context,
	engineUUID uuid.UUID,
	regionSplitSize, regionSplitKeys int64,
) error {
	kvRegionSplitSize, kvRegionSplitKeys, err := GetRegionSplitSizeKeys(ctx, local.pdCli, local.tls)
	if err == nil {
		if kvRegionSplitSize > regionSplitSize {
			regionSplitSize = kvRegionSplitSize
		}
		if kvRegionSplitKeys > regionSplitKeys {
			regionSplitKeys = kvRegionSplitKeys
		}
	} else {
		tidblogutil.Logger(ctx).Warn("fail to get region split keys and size", zap.Error(err))
	}

	var e engineapi.Engine
	if externalEngine, ok := local.engineMgr.getExternalEngine(engineUUID); ok {
		e = externalEngine
	} else {
		localEngine := local.engineMgr.lockEngine(engineUUID, importMutexStateImport)
		if localEngine == nil {
			// skip if engine not exist. See the comment of `CloseEngine` for more detail.
			return nil
		}
		defer localEngine.unlock()
		localEngine.regionSplitSize = regionSplitSize
		localEngine.regionSplitKeyCnt = regionSplitKeys
		e = localEngine
	}
	lfTotalSize, lfLength := e.KVStatistics()
	if lfTotalSize == 0 {
		// engine is empty, this is likes because it's a index engine but the table contains no index
		tidblogutil.Logger(ctx).Info("engine contains no kv, skip import", zap.Stringer("engine", engineUUID))
		return nil
	}

	// split sorted file into range about regionSplitSize per file
	splitKeys, err := getRegionSplitKeys(ctx, e, regionSplitSize, regionSplitKeys)
	if err != nil {
		return err
	}
	intest.Assert(len(splitKeys) > 0)
	startKey, endKey := splitKeys[0], splitKeys[len(splitKeys)-1]

	forceSplitThreshold := ForcePartitionRegionThreshold
	failpoint.InjectCall("ForcePartitionRegionThreshold", &forceSplitThreshold)
	// We only force partition range when the table is large enough (>= 100 regions).
	// This is to avoid unnecessary RPC calls for small tables.
	if kerneltype.IsClassic() && len(startKey) > 0 && len(endKey) > 0 && len(splitKeys)-1 >= forceSplitThreshold {
		tidblogutil.Logger(ctx).Info("force partition range",
			zap.String("startKey", redact.Key(startKey)),
			zap.String("endKey", redact.Key(endKey)))
		stores, err := local.pdCli.GetAllStores(ctx, opt.WithExcludeTombstone())
		if err != nil {
			return err
		}
		removeTableSplitRange := local.forceTableSplitRange(ctx, startKey, endKey, stores)
		defer removeTableSplitRange()
	}

	if local.PausePDSchedulerScope == config.PausePDSchedulerScopeTable {
		tidblogutil.Logger(ctx).Info("pause pd scheduler of table scope")
		subCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		var startKey, endKey []byte
		if len(splitKeys[0]) > 0 {
			startKey = codec.EncodeBytes(nil, splitKeys[0])
		}
		if len(splitKeys[len(splitKeys)-1]) > 0 {
			endKey = codec.EncodeBytes(nil, splitKeys[len(splitKeys)-1])
		}
		done, err := pdutil.PauseSchedulersByKeyRange(subCtx, local.pdHTTPCli, startKey, endKey)
		if err != nil {
			return errors.Trace(err)
		}
		defer func() {
			cancel()
			<-done
		}()
	}

	if local.BackendConfig.RaftKV2SwitchModeDuration > 0 {
		tidblogutil.Logger(ctx).Info("switch import mode of ranges",
			zap.String("startKey", redact.Key(startKey)),
			zap.String("endKey", redact.Key(endKey)))
		subCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		done, err := local.switchModeBySplitKeys(subCtx, splitKeys)
		if err != nil {
			return errors.Trace(err)
		}
		defer func() {
			cancel()
			<-done
		}()
	}

	maxReqInFlight := GetMaxIngestConcurrency()
	maxReqPerSec := GetMaxIngestPerSec()
	local.ingestLimiter.Store(newIngestLimiter(ctx, maxReqInFlight, maxReqPerSec))
	tidblogutil.Logger(ctx).Info("start import engine",
		zap.Stringer("uuid", engineUUID),
		zap.Int("region ranges", len(splitKeys)-1),
		zap.Int64("count", lfLength),
		zap.Int64("size", lfTotalSize),
		zap.Int("maxReqInFlight", maxReqInFlight),
		zap.Float64("maxReqPerSec", maxReqPerSec),
	)

	failpoint.InjectCall("ReadyForImportEngine")

	err = local.doImport(ctx, e, splitKeys, regionSplitSize, regionSplitKeys)
	if err == nil {
		importedSize, importedLength := e.ImportedStatistics()

		if err := verifyImportedStatistics(e, importedLength); err != nil {
			return err
		}

		tidblogutil.Logger(ctx).Info("import engine success",
			zap.Stringer("uuid", engineUUID),
			zap.Int64("size", lfTotalSize),
			zap.Int64("kvs", lfLength),
			zap.Int64("importedSize", importedSize),
			zap.Int64("importedCount", importedLength))
	}
	return err
}

// expose these variables to unit test.
var (
	testJobToWorkerCh = make(chan *regionJob)
	testJobWg         *sync.WaitGroup
)

func (local *Backend) doImport(
	ctx context.Context,
	engine engineapi.Engine,
	regionSplitKeys [][]byte,
	regionSplitSize, regionSplitKeyCnt int64,
) error {
	/*
	 ┌─────────────────┐                   ┌─────────────┐   ┌────────────┐
	 │prepareAndSendJob├───jobToWorkerCh──→│storeBalancer├──→│ workerpool │
	 └─────────────────┘         ↑         └─────────────┘   └─────┬──────┘
	                             │            (optional)           ↓
	                             │                          jobFromWorkerCh
	                             │                                 │
	                             │                                 ↓
	                    ┌────────┴─────────┐                ┌──────┴────────┐
	                    │ regionJobRetryer │←───────────────┤  dispatcher   │──→done
	                    └──────────────────┘                └───────────────┘
	*/

	// Above is the happy path workflow of region jobs. A job is generated by
	// prepareAndSendJob and terminated to "done" state by dispatchJobGoroutine. We
	// maintain an invariant that the number of generated jobs (after job.ref())
	// minus the number of "done" jobs (after job.done()) equals to jobWg. So we can
	// use jobWg to wait for all jobs to be finished.
	//
	// To handle the error case, we still maintain the invariant, but the workflow
	// becomes a bit more complex. When an error occurs, the corresponding component
	// need to convert the job to "done" state, or send all its owned jobs to next
	// components. The exit order is important because if the next component is
	// exited before the owner component, deadlock will happen.
	//
	// All components (except worker pool) are spawned by workGroup so the main goroutine
	// can wait all components to exit. Worker pool uses the subcontext of workerCtx, so it
	// can perceive the error from other components. Besides, it will also spawn a goroutine
	// in the workGroup to notify the error from itself.
	//
	// The exit order in happy path is:
	//
	// 1. prepareAndSendJob is finished, its goroutine will wait all jobs are
	// finished by jobWg.Wait(). Then the worker pool will be closed, and close
	// the output channel of this pool while quit the internal goroutine.
	//
	// 2. one-by-one, when each component see its input channel is closed, it knows
	// the workflow is finished. It will exit and close the output channel which is the
	// input channel of the next component.
	//
	// 3. Now all components are exited, the main goroutine can exit after
	// workGroup.Wait().
	//
	// Component exit order in error case is:
	//
	// 1. Error occurs in worker pool
	//      1.1 The subcontext will be canceled and cause its goroutine to exit.
	//      1.2 The error is then broadcasted to the workGroup.
	//      1.3 All other components exit due to canceled context. No need to close channels.
	//
	// 2. Error occurs in other components (e.g. retryer, dispatcher)
	//      2.1 The error component exits and causes workGroup's context to be canceled.
	//      2.2 All other components will exit because of the canceled context. No need to
	//          close channels.
	//      2.3 The worker pool will exit due to the canceled context too, we also need to
	//          wait these workers to exit in the main goroutine.
	//      2.4 The main goroutine can see the error and exit after workGroup.Wait().

	var (
		// workGroup is used to run all components in the workflow.
		workGroup, workerCtx = util.NewErrorGroupWithRecoverWithCtx(ctx)
		// jobToWorkerCh and jobFromWorkerCh are unbuffered so jobs will not be
		// owned by them.
		jobToWorkerCh   = make(chan *regionJob)
		jobFromWorkerCh = make(chan *regionJob)
		jobWg           sync.WaitGroup
		balancer        *storeBalancer
	)

	// storeBalancer does not have backpressure, it should not be used with external
	// engine to avoid OOM.
	if _, ok := engine.(*Engine); ok {
		balancer = newStoreBalancer(jobToWorkerCh, &jobWg)
		workGroup.Go(func() error {
			return balancer.run(workerCtx)
		})
	}

	failpoint.Inject("injectVariables", func() {
		jobToWorkerCh = testJobToWorkerCh
		testJobWg = &jobWg
	})

	retryer := newRegionJobRetryer(workerCtx, jobToWorkerCh, &jobWg)
	workGroup.Go(func() error {
		retryer.run()
		return nil
	})

	// dispatcher sends done jobs to retryer or marks them done.
	dispatcher := newDispatcher(workerCtx, jobFromWorkerCh, &jobWg, retryer)
	workGroup.Go(func() error {
		return dispatcher.run()
	})

	var clusterID uint64
	if local.pdCli != nil {
		clusterID = local.pdCli.GetClusterID(ctx)
	}

	pool := getRegionJobWorkerPool(
		workerCtx, &jobWg,
		local, balancer,
		jobToWorkerCh, jobFromWorkerCh,
		clusterID,
	)
	wctx := workerpool.NewContext(workerCtx)

	if e, ok := engine.(*external.Engine); ok {
		e.SetWorkerPool(pool)
	}

	failpoint.Inject("skipStartWorker", func() {
		failpoint.Goto("afterStartWorker")
	})

	workGroup.Go(func() error {
		pool.Start(wctx)
		<-wctx.Done()
		return wctx.OperatorErr()
	})

	failpoint.Label("afterStartWorker")

	workGroup.Go(func() error {
		err := local.prepareAndSendJob(
			workerCtx,
			engine,
			regionSplitKeys,
			regionSplitSize,
			regionSplitKeyCnt,
			jobToWorkerCh,
			&jobWg,
		)
		if err != nil {
			return err
		}

		jobWg.Wait()
		if balancer != nil {
			intest.AssertFunc(func() bool {
				allZero := true
				balancer.storeLoadMap.Range(func(_, value any) bool {
					if value.(int) != 0 {
						allZero = false
						return false
					}
					return true
				})
				return allZero
			})
		}

		// Close the pool, as well as the channel.
		wctx.Cancel()
		pool.Release()
		return nil
	})

	err := workGroup.Wait()
	if err != nil && !common.IsContextCanceledError(err) {
		tidblogutil.Logger(ctx).Error("do import meets error", zap.Error(err))
	}
	return err
}

func (local *Backend) newRegionJobWorker(
	ctx context.Context,
	clusterID uint64,
	toCh, jobFromWorkerCh chan *regionJob,
	jobWg *sync.WaitGroup,
	afterExecuteJob func([]*metapb.Peer),
) regionJobWorker {
	base := &regionJobBaseWorker{
		ctx:              ctx,
		jobInCh:          toCh,
		jobOutCh:         jobFromWorkerCh,
		jobWg:            jobWg,
		afterRunJobFn:    afterExecuteJob,
		regenerateJobsFn: local.generateJobForRange,
	}
	if kerneltype.IsNextGen() {
		tlsConfig := local.tls.TLSConfig()
		if local.nextgenHTTPCli == nil {
			local.nextgenHTTPCli = util.ClientWithTLS(tlsConfig)
		}
		isHTTPS := tlsConfig != nil
		cloudW := &objStoreRegionJobWorker{
			ingestCli:      ingestcli.NewClient(local.TiKVWorkerURL, clusterID, isHTTPS, local.nextgenHTTPCli, local.splitCli),
			writeBatchSize: local.KVWriteBatchSize,
			bufPool:        local.engineMgr.getBufferPool(),
			collector:      local.collector,
		}
		base.writeFn = cloudW.write
		base.ingestFn = cloudW.ingest
		base.preRunJobFn = cloudW.preRunJob
		cloudW.regionJobBaseWorker = base
		return cloudW
	}

	opWorker := &blkStoreRegionJobWorker{
		checkTiKVSpace: local.ShouldCheckTiKV,
		pdHTTPCli:      local.pdHTTPCli,
	}
	base.writeFn = local.doWrite
	base.ingestFn = local.ingest
	base.preRunJobFn = opWorker.preRunJob
	opWorker.regionJobBaseWorker = base
	return opWorker
}

// GetImportedKVCount returns the number of imported KV pairs of some engine.
func (local *Backend) GetImportedKVCount(engineUUID uuid.UUID) int64 {
	return local.engineMgr.getImportedKVCount(engineUUID)
}

// GetExternalEngineKVStatistics returns kv statistics of some engine.
func (local *Backend) GetExternalEngineKVStatistics(engineUUID uuid.UUID) (
	totalKVSize int64, totalKVCount int64) {
	return local.engineMgr.getExternalEngineKVStatistics(engineUUID)
}

// GetExternalEngineConflictInfo returns conflict info of some engine.
func (local *Backend) GetExternalEngineConflictInfo(engineUUID uuid.UUID) engineapi.ConflictInfo {
	return local.engineMgr.getExternalEngineConflictInfo(engineUUID)
}

// ResetEngineSkipAllocTS is like ResetEngine but the inner TS of the engine is
// invalid. Caller must use SetTSBeforeImportEngine to set a valid TS before import
// the engine.
func (local *Backend) ResetEngineSkipAllocTS(ctx context.Context, engineUUID uuid.UUID) error {
	return local.engineMgr.resetEngine(ctx, engineUUID, true)
}

// SetTSBeforeImportEngine allocates a new TS for the engine before it is imported.
// This is typically called after persisting the chosen TS of the engine to make
// sure TS is not changed after task failover.
func (local *Backend) SetTSBeforeImportEngine(ctx context.Context, engineUUID uuid.UUID, ts uint64) error {
	e := local.engineMgr.lockEngine(engineUUID, importMutexStateClose)
	if e == nil {
		return errors.Errorf("engine %s not found in SetTSBeforeImportEngine", engineUUID.String())
	}
	defer e.unlock()
	if ts == 0 {
		p, l, err := local.pdCli.GetTS(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		failpoint.Inject("afterSetTSBeforeImportEngine", func(_ failpoint.Value) {
			failpoint.Return(errors.Errorf("mock err"))
		})
		ts = oracle.ComposeTS(p, l)
	}
	e.engineMeta.TS = ts
	return e.saveEngineMeta()
}

// CleanupEngine cleanup the engine and reclaim the space.
func (local *Backend) CleanupEngine(ctx context.Context, engineUUID uuid.UUID) error {
	return local.engineMgr.cleanupEngine(ctx, engineUUID)
}

// GetDupeController returns a new dupe controller.
func (local *Backend) GetDupeController(dupeConcurrency int, errorMgr *errormanager.ErrorManager) *DupeController {
	return &DupeController{
		splitCli:            local.splitCli,
		tikvCli:             local.tikvCli,
		tikvCodec:           local.tikvCodec,
		errorMgr:            errorMgr,
		dupeConcurrency:     dupeConcurrency,
		duplicateDB:         local.engineMgr.getDuplicateDB(),
		keyAdapter:          local.engineMgr.getKeyAdapter(),
		importClientFactory: local.importClientFactory,
		resourceGroupName:   local.ResourceGroupName,
		taskType:            local.TaskType,
	}
}

// UnsafeImportAndReset forces the backend to import the content of an engine
// into the target and then reset the engine to empty. This method will not
// close the engine. Make sure the engine is flushed manually before calling
// this method.
func (local *Backend) UnsafeImportAndReset(ctx context.Context, engineUUID uuid.UUID, regionSplitSize, regionSplitKeys int64) error {
	// DO NOT call be.abstract.CloseEngine()! The engine should still be writable after
	// calling UnsafeImportAndReset().
	logger := log.Wrap(tidblogutil.Logger(ctx)).With(
		zap.String("engineTag", "<import-and-reset>"),
		zap.Stringer("engineUUID", engineUUID),
	)
	closedEngine := backend.NewClosedEngine(local, logger, engineUUID, 0)
	if err := closedEngine.Import(ctx, regionSplitSize, regionSplitKeys); err != nil {
		return err
	}
	return local.engineMgr.resetEngine(ctx, engineUUID, false)
}

func engineSSTDir(storeDir string, engineUUID uuid.UUID) string {
	return filepath.Join(storeDir, engineUUID.String()+".sst")
}

// LocalWriter returns a new local writer.
func (local *Backend) LocalWriter(ctx context.Context, cfg *backend.LocalWriterConfig, engineUUID uuid.UUID) (backend.EngineWriter, error) {
	return local.engineMgr.localWriter(ctx, cfg, engineUUID)
}

// switchModeBySplitKeys will switch tikv mode for regions in the specific keys
// for multirocksdb. This function will spawn a goroutine to keep switch mode
// periodically until the context is done. The return done channel is used to
// notify the caller that the background goroutine is exited.
func (local *Backend) switchModeBySplitKeys(
	ctx context.Context,
	splitKeys [][]byte,
) (<-chan struct{}, error) {
	switcher := NewTiKVModeSwitcher(local.tls.TLSConfig(), local.pdHTTPCli, tidblogutil.Logger(ctx))
	done := make(chan struct{})

	keyRange := &sst.Range{}
	if len(splitKeys[0]) > 0 {
		keyRange.Start = codec.EncodeBytes(nil, splitKeys[0])
	}
	if len(splitKeys[len(splitKeys)-1]) > 0 {
		keyRange.End = codec.EncodeBytes(nil, splitKeys[len(splitKeys)-1])
	}

	go func() {
		defer close(done)
		ticker := time.NewTicker(local.BackendConfig.RaftKV2SwitchModeDuration)
		defer ticker.Stop()
		switcher.ToImportMode(ctx, keyRange)
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			case <-ticker.C:
				switcher.ToImportMode(ctx, keyRange)
			}
		}
		// Use a new context to avoid the context is canceled by the caller.
		recoverCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		switcher.ToNormalMode(recoverCtx, keyRange)
	}()
	return done, nil
}

func openLocalWriter(cfg *backend.LocalWriterConfig, engine *Engine, tikvCodec tikvclient.Codec, cacheSize int64, kvBuffer *membuf.Buffer) (*Writer, error) {
	// pre-allocate a long enough buffer to avoid a lot of runtime.growslice
	// this can help save about 3% of CPU.
	var preAllocWriteBatch []common.KvPair
	if !cfg.Local.IsKVSorted {
		preAllocWriteBatch = make([]common.KvPair, units.MiB)
		// we want to keep the cacheSize as the whole limit of this local writer, but the
		// main memory usage comes from two member: kvBuffer and writeBatch, so we split
		// ~10% to writeBatch for !IsKVSorted, which means we estimate the average length
		// of KV pairs are 9 times than the size of common.KvPair (9*72B = 648B).
		cacheSize = cacheSize * 9 / 10
	}
	w := &Writer{
		engine:             engine,
		memtableSizeLimit:  cacheSize,
		kvBuffer:           kvBuffer,
		isKVSorted:         cfg.Local.IsKVSorted,
		isWriteBatchSorted: true,
		tikvCodec:          tikvCodec,
		writeBatch:         preAllocWriteBatch,
	}
	engine.localWriters.Store(w, nil)
	return w, nil
}

// return the smallest []byte that is bigger than current bytes.
// special case when key is empty, empty bytes means infinity in our context, so directly return itself.
func nextKey(key []byte) []byte {
	if len(key) == 0 {
		return []byte{}
	}

	// in tikv <= 4.x, tikv will truncate the row key, so we should fetch the next valid row key
	// See: https://github.com/tikv/tikv/blob/f7f22f70e1585d7ca38a59ea30e774949160c3e8/components/raftstore/src/coprocessor/split_observer.rs#L36-L41
	// we only do this for IntHandle, which is checked by length
	if tablecodec.IsRecordKey(key) && len(key) == tablecodec.RecordRowKeyLen {
		tableID, handle, _ := tablecodec.DecodeRecordKey(key)
		nextHandle := handle.Next()
		// int handle overflow, use the next table prefix as nextKey
		if nextHandle.Compare(handle) <= 0 {
			return tablecodec.EncodeTablePrefix(tableID + 1)
		}
		return tablecodec.EncodeRowKeyWithHandle(tableID, nextHandle)
	}

	// for index key and CommonHandle, directly append a 0x00 to the key.
	res := make([]byte, 0, len(key)+1)
	res = append(res, key...)
	res = append(res, 0)
	return res
}

// EngineFileSizes implements DiskUsage interface.
func (local *Backend) EngineFileSizes() (res []backend.EngineFileSize) {
	return local.engineMgr.engineFileSizes()
}

// GetTS implements StoreHelper interface.
func (local *Backend) GetTS(ctx context.Context) (physical, logical int64, err error) {
	return local.pdCli.GetTS(ctx)
}

// GetTiKVCodec implements StoreHelper interface.
func (local *Backend) GetTiKVCodec() tikvclient.Codec {
	return local.tikvCodec
}

// CloseEngineMgr close the engine manager.
// This function is used for test.
func (local *Backend) CloseEngineMgr() {
	local.engineMgr.close()
}

var getSplitConfFromStoreFunc = getSplitConfFromStore

// return region split size, region split keys, error
func getSplitConfFromStore(ctx context.Context, host string, tls *common.TLS) (
	splitSize int64, regionSplitKeys int64, err error) {
	var (
		nested struct {
			Coprocessor struct {
				RegionSplitSize string `json:"region-split-size"`
				RegionSplitKeys int64  `json:"region-split-keys"`
			} `json:"coprocessor"`
		}
	)
	if err := tls.WithHost(host).GetJSON(ctx, "/config", &nested); err != nil {
		return 0, 0, errors.Trace(err)
	}
	splitSize, err = units.FromHumanSize(nested.Coprocessor.RegionSplitSize)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	return splitSize, nested.Coprocessor.RegionSplitKeys, nil
}

// GetRegionSplitSizeKeys return region split size, region split keys, error
func GetRegionSplitSizeKeys(ctx context.Context, cli pd.Client, tls *common.TLS) (
	regionSplitSize int64, regionSplitKeys int64, err error) {
	stores, err := cli.GetAllStores(ctx, opt.WithExcludeTombstone())
	if err != nil {
		return 0, 0, err
	}
	for _, store := range stores {
		if store.StatusAddress == "" || engine.IsTiFlash(store) {
			continue
		}
		serverInfo := infoschema.ServerInfo{
			Address:    store.Address,
			StatusAddr: store.StatusAddress,
		}
		serverInfo.ResolveLoopBackAddr()
		regionSplitSize, regionSplitKeys, err := getSplitConfFromStoreFunc(ctx, serverInfo.StatusAddr, tls)
		if err == nil {
			return regionSplitSize, regionSplitKeys, nil
		}
		tidblogutil.Logger(ctx).Warn("get region split size and keys failed", zap.Error(err), zap.String("store", serverInfo.StatusAddr))
	}
	return 0, 0, errors.New("get region split size and keys failed")
}
