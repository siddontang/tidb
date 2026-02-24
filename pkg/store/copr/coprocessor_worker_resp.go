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

package copr

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/terror"
	copr_metrics "github.com/pingcap/tidb/pkg/store/copr/metrics"
	derr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/paging"
	"github.com/pingcap/tidb/pkg/util/traceevent"
	"github.com/pingcap/tidb/pkg/util/trxevents"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

func (worker *copIteratorWorker) logTimeCopTask(costTime time.Duration, task *copTask, bo *Backoffer, resp *coprocessor.Response) {
	logStr := fmt.Sprintf("[TIME_COP_PROCESS] resp_time:%s txnStartTS:%d region_id:%d store_addr:%s", costTime, worker.req.StartTs, task.region.GetID(), task.storeAddr)
	if worker.kvclient.Stats != nil {
		logStr += fmt.Sprintf(" stats:%s", worker.kvclient.Stats.String())
	}
	if bo.GetTotalSleep() > minLogBackoffTime {
		backoffTypes := strings.ReplaceAll(fmt.Sprintf("%v", bo.TiKVBackoffer().GetTypes()), " ", ",")
		logStr += fmt.Sprintf(" backoff_ms:%d backoff_types:%s", bo.GetTotalSleep(), backoffTypes)
	}
	if regionErr := getRegionError(bo.GetCtx(), resp); regionErr != nil {
		logStr += fmt.Sprintf(" region_err:%s", regionErr.String())
	}
	// resp might be nil, but it is safe to call resp.GetXXX here.
	detailV2 := resp.GetExecDetailsV2()
	detail := resp.GetExecDetails()
	var timeDetail *kvrpcpb.TimeDetail
	if detailV2 != nil && detailV2.TimeDetail != nil {
		timeDetail = detailV2.TimeDetail
	} else if detail != nil && detail.TimeDetail != nil {
		timeDetail = detail.TimeDetail
	}
	if timeDetail != nil {
		logStr += fmt.Sprintf(" kv_process_ms:%d", timeDetail.ProcessWallTimeMs)
		logStr += fmt.Sprintf(" kv_wait_ms:%d", timeDetail.WaitWallTimeMs)
		logStr += fmt.Sprintf(" kv_read_ms:%d", timeDetail.KvReadWallTimeMs)
		if timeDetail.ProcessWallTimeMs <= minLogKVProcessTime {
			logStr = strings.Replace(logStr, "TIME_COP_PROCESS", "TIME_COP_WAIT", 1)
		}
	}

	if detailV2 != nil && detailV2.ScanDetailV2 != nil {
		logStr += fmt.Sprintf(" processed_versions:%d", detailV2.ScanDetailV2.ProcessedVersions)
		logStr += fmt.Sprintf(" total_versions:%d", detailV2.ScanDetailV2.TotalVersions)
		logStr += fmt.Sprintf(" rocksdb_delete_skipped_count:%d", detailV2.ScanDetailV2.RocksdbDeleteSkippedCount)
		logStr += fmt.Sprintf(" rocksdb_key_skipped_count:%d", detailV2.ScanDetailV2.RocksdbKeySkippedCount)
		logStr += fmt.Sprintf(" rocksdb_cache_hit_count:%d", detailV2.ScanDetailV2.RocksdbBlockCacheHitCount)
		logStr += fmt.Sprintf(" rocksdb_read_count:%d", detailV2.ScanDetailV2.RocksdbBlockReadCount)
		logStr += fmt.Sprintf(" rocksdb_read_byte:%d", detailV2.ScanDetailV2.RocksdbBlockReadByte)
	} else if detail != nil && detail.ScanDetail != nil {
		logStr = appendScanDetail(logStr, "write", detail.ScanDetail.Write)
		logStr = appendScanDetail(logStr, "data", detail.ScanDetail.Data)
		logStr = appendScanDetail(logStr, "lock", detail.ScanDetail.Lock)
	}
	logutil.Logger(bo.GetCtx()).Info(logStr)
}

func appendScanDetail(logStr string, columnFamily string, scanInfo *kvrpcpb.ScanInfo) string {
	if scanInfo != nil {
		logStr += fmt.Sprintf(" scan_total_%s:%d", columnFamily, scanInfo.Total)
		logStr += fmt.Sprintf(" scan_processed_%s:%d", columnFamily, scanInfo.Processed)
	}
	return logStr
}

func (worker *copIteratorWorker) handleCopPagingResult(bo *Backoffer, rpcCtx *tikv.RPCContext, resp *copResponse, cacheKey []byte, cacheValue *coprCacheValue, task *copTask, costTime time.Duration) (*copTaskResult, error) {
	result, err := worker.handleCopResponse(bo, rpcCtx, resp, cacheKey, cacheValue, task, costTime)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if result != nil && len(result.remains) > 0 {
		// If there is region error or lock error, keep the paging size and retry.
		for _, remainedTask := range result.remains {
			remainedTask.pagingSize = task.pagingSize
		}
		return result, nil
	}
	pagingRange := resp.pbResp.Range
	// only paging requests need to calculate the next ranges
	if pagingRange == nil {
		// If the storage engine doesn't support paging protocol, it should have return all the region data.
		// So we finish here.
		return result, nil
	}

	// calculate next ranges and grow the paging size
	task.ranges = worker.calculateRemain(task.ranges, pagingRange, worker.req.Desc)
	if task.ranges.Len() == 0 {
		return result, nil
	}

	task.pagingSize = paging.GrowPagingSize(task.pagingSize, worker.req.Paging.MaxPagingSize)
	result.remains = []*copTask{task}
	return result, nil
}

// buildExceedsBoundDiagFields builds diagnostic log fields for "Request range exceeds bound" errors.
// Shared between the initial-retry path (retrying without buckets) and the persist-after-retry path
// (skipBuckets=true, bounded retries).
func buildExceedsBoundDiagFields(
	req *kv.Request,
	task *copTask,
	rpcCtx *tikv.RPCContext,
	regionCache *RegionCache,
	bo *Backoffer,
	otherErr string,
	latestBucketsVer uint64,
) []zap.Field {
	fields := []zap.Field{
		zap.Uint64("connID", req.ConnID),
		zap.String("connAlias", req.ConnAlias),
		zap.Uint64("txnStartTS", req.StartTs),
		zap.Uint64("regionID", task.region.GetID()),
		zap.Uint64("regionVer", task.region.GetVer()),
		zap.Uint64("regionConfVer", task.region.GetConfVer()),
		zap.Uint64("bucketsVer", task.bucketsVer),
		zap.Uint64("latestBucketsVer", latestBucketsVer),
		zap.String("storeType", task.storeType.Name()),
		zap.String("peerAddr", task.storeAddr),
		zap.Bool("skipBuckets", task.skipBuckets),
		zap.Int("exceedsBoundRetry", task.exceedsBoundRetry),
		zap.Int("maxExceedsBoundRetries", maxExceedsBoundRetries),
		zap.Int("rangeCount", task.ranges.Len()),
		zap.Any("rangeIssues", rangeIssuesForKeyRanges(task.ranges)),
		zap.String("error", otherErr),
	}

	if task.ranges.Len() > 0 {
		minStart, maxEnd := minStartAndMaxEndKeyOfKeyRanges(task.ranges)
		first := task.ranges.At(0)
		last := task.ranges.At(task.ranges.Len() - 1)
		fields = append(fields,
			keyField("minRangeStartKey", minStart),
			keyField("maxRangeEndKey", maxEnd),
			keyField("firstRangeStartKey", first.StartKey),
			keyField("firstRangeEndKey", first.EndKey),
			keyField("lastRangeStartKey", last.StartKey),
			keyField("lastRangeEndKey", last.EndKey),
		)
	}
	if len(task.buildLocStartKey) > 0 || len(task.buildLocEndKey) > 0 {
		fields = append(fields,
			keyField("buildLocationStartKey", task.buildLocStartKey),
			keyField("buildLocationEndKey", task.buildLocEndKey),
		)
	}

	var cachedStart, cachedEnd []byte
	var diagStart []byte
	if rpcCtx != nil && rpcCtx.Meta != nil {
		cachedStart = rpcCtx.Meta.GetStartKey()
		cachedEnd = rpcCtx.Meta.GetEndKey()
		fields = append(fields,
			keyField("cachedRegionStartKey", cachedStart),
			keyField("cachedRegionEndKey", cachedEnd),
		)
		badIdx, badRange, badReason := firstOutOfBoundKeyRangeInLocation(task.ranges, cachedStart, cachedEnd)
		if badIdx >= 0 {
			diagStart = badRange.StartKey
			fields = append(fields,
				zap.Int("outOfBoundRangeIndex", badIdx),
				zap.String("outOfBoundReason", badReason),
				keyField("outOfBoundRangeStartKey", badRange.StartKey),
				keyField("outOfBoundRangeEndKey", badRange.EndKey),
			)
		}
		if len(task.buildLocStartKey) > 0 || len(task.buildLocEndKey) > 0 {
			fields = append(fields,
				zap.Bool("buildBoundaryChangedVsCached",
					!bytes.Equal(task.buildLocStartKey, cachedStart) || !bytes.Equal(task.buildLocEndKey, cachedEnd)),
			)
		}
	} else {
		fields = append(fields, zap.Bool("cachedRegionMetaMissing", true))
	}
	if len(diagStart) == 0 && task.ranges.Len() > 0 {
		diagStart = task.ranges.At(0).StartKey
	}
	if len(diagStart) > 0 {
		cacheLoc := regionCache.TryLocateKey(diagStart)
		if cacheLoc == nil {
			fields = append(fields,
				keyField("diagStartKey", diagStart),
				zap.Bool("cacheLocateByDiagStartMissing", true),
			)
		} else {
			fields = append(fields,
				keyField("diagStartKey", diagStart),
				formatKeyLocation("cacheLocateByDiagStart", cacheLoc),
			)
		}
	}

	// Best-effort: query PD directly to compare region boundaries with cached info.
	pdLoc, pdErr := regionCache.LocateRegionByIDFromPD(bo.TiKVBackoffer(), task.region.GetID())
	if pdErr != nil {
		fields = append(fields, zap.Error(pdErr))
	} else {
		fields = append(fields,
			zap.Uint64("pdRegionVer", pdLoc.Region.GetVer()),
			zap.Uint64("pdRegionConfVer", pdLoc.Region.GetConfVer()),
			keyField("pdRegionStartKey", pdLoc.StartKey),
			keyField("pdRegionEndKey", pdLoc.EndKey),
		)
		if cachedStart != nil || cachedEnd != nil {
			fields = append(fields,
				zap.Bool("pdEpochChanged", pdLoc.Region.GetVer() != task.region.GetVer() || pdLoc.Region.GetConfVer() != task.region.GetConfVer()),
				zap.Bool("pdBoundaryChanged", !bytes.Equal(pdLoc.StartKey, cachedStart) || !bytes.Equal(pdLoc.EndKey, cachedEnd)),
			)
		}
		if len(task.buildLocStartKey) > 0 || len(task.buildLocEndKey) > 0 {
			fields = append(fields,
				zap.Bool("buildBoundaryChangedVsPD",
					!bytes.Equal(task.buildLocStartKey, pdLoc.StartKey) || !bytes.Equal(task.buildLocEndKey, pdLoc.EndKey)),
			)
		}
	}

	fields = append(fields,
		formatRanges(task.ranges),
		zap.Stack("stack"))
	return fields
}

// handleCopResponse checks coprocessor Response for region split and lock,
// returns more tasks when that happens, or handles the response if no error.
// if we're handling coprocessor paging response, lastRange is the range of last
// successful response, otherwise it's nil.
func (worker *copIteratorWorker) handleCopResponse(bo *Backoffer, rpcCtx *tikv.RPCContext, resp *copResponse, cacheKey []byte, cacheValue *coprCacheValue, task *copTask, costTime time.Duration) (*copTaskResult, error) {
	if ver := resp.pbResp.GetLatestBucketsVersion(); task.bucketsVer < ver {
		worker.store.GetRegionCache().UpdateBucketsIfNeeded(task.region, ver)
	}
	if regionErr := getRegionError(bo.GetCtx(), resp.pbResp); regionErr != nil {
		if rpcCtx != nil && task.storeType == kv.TiDB {
			resp.err = errors.Errorf("error: %v", regionErr)
			worker.checkRespOOM(resp)
			return &copTaskResult{resp: resp}, nil
		}
		errStr := fmt.Sprintf("region_id:%v, region_ver:%v, store_type:%s, peer_addr:%s, error:%s",
			task.region.GetID(), task.region.GetVer(), task.storeType.Name(), task.storeAddr, regionErr.String())
		if err := bo.Backoff(tikv.BoRegionMiss(), errors.New(errStr)); err != nil {
			return nil, errors.Trace(err)
		}
		// We may meet RegionError at the first packet, but not during visiting the stream.
		remains, err := buildCopTasks(bo, task.ranges, &buildCopTaskOpt{
			req:                         worker.req,
			cache:                       worker.store.GetRegionCache(),
			respChan:                    false,
			eventCb:                     task.eventCb,
			ignoreTiKVClientReadTimeout: true,
		})
		if err != nil {
			return nil, err
		}
		return worker.handleBatchRemainsOnErr(bo, rpcCtx, remains, resp.pbResp, task)
	}
	if lockErr := resp.pbResp.GetLocked(); lockErr != nil {
		if err := worker.handleLockErr(bo, lockErr, task); err != nil {
			return nil, err
		}
		task.meetLockFallback = true
		return worker.handleBatchRemainsOnErr(bo, rpcCtx, []*copTask{task}, resp.pbResp, task)
	}
	if otherErr := resp.pbResp.GetOtherError(); otherErr != "" {
		err := errors.Errorf("other error: %s", otherErr)

		// Handle "Request range exceeds bound" error from TiKV.
		// This can happen when bucket metadata is stale and causes TiDB to send
		// ranges outside the region boundary. Invalidate cache and retry.
		if strings.Contains(otherErr, "Request range exceeds bound") {
			// If this task was already built without bucket splitting and still got this error,
			// the problem isn't stale bucket metadata. We use bounded self-healing retries first,
			// then fail when retry budget is exhausted.
			if task.skipBuckets {
				fields := buildExceedsBoundDiagFields(worker.req, task, rpcCtx, worker.store.GetRegionCache(), bo, otherErr, resp.pbResp.GetLatestBucketsVersion())
				logutil.Logger(bo.GetCtx()).Error("Request range exceeds bound persists after bucket-less retry", fields...)
				if task.exceedsBoundRetry >= maxExceedsBoundRetries {
					return nil, errors.Errorf(
						"request range exceeds bound persists after bucket-less retry and exceeded retry budget, "+
							"region_id:%v, region_ver:%v, store_type:%s, peer_addr:%s, retry:%d/%d, error:%s",
						task.region.GetID(), task.region.GetVer(), task.storeType.Name(), task.storeAddr,
						task.exceedsBoundRetry, maxExceedsBoundRetries, otherErr)
				}

				logutil.Logger(bo.GetCtx()).Warn("Retrying persists-after-skipBuckets request range exceeds bound",
					zap.Uint64("connID", worker.req.ConnID),
					zap.String("connAlias", worker.req.ConnAlias),
					zap.Uint64("txnStartTS", worker.req.StartTs),
					zap.Uint64("regionID", task.region.GetID()),
					zap.Uint64("regionVer", task.region.GetVer()),
					zap.Uint64("regionConfVer", task.region.GetConfVer()),
					zap.Int("retry", task.exceedsBoundRetry+1),
					zap.Int("maxRetry", maxExceedsBoundRetries))

				// Self-healing retry: invalidate and rebuild in skip-buckets mode.
				worker.store.GetRegionCache().InvalidateCachedRegion(task.region)
				errStr := fmt.Sprintf("Request range exceeds bound persists after bucket-less retry: region_id:%v, region_ver:%v, store_type:%s, peer_addr:%s, retry:%d/%d, error:%s",
					task.region.GetID(), task.region.GetVer(), task.storeType.Name(), task.storeAddr,
					task.exceedsBoundRetry+1, maxExceedsBoundRetries, otherErr)
				if err := bo.Backoff(tikv.BoRegionMiss(), errors.New(errStr)); err != nil {
					return nil, errors.Trace(err)
				}
				remains, err := buildCopTasks(bo, task.ranges, &buildCopTaskOpt{
					req:                         worker.req,
					cache:                       worker.store.GetRegionCache(),
					respChan:                    false,
					eventCb:                     task.eventCb,
					ignoreTiKVClientReadTimeout: true,
					skipBuckets:                 true,
					exceedsBoundRetry:           task.exceedsBoundRetry + 1,
				})
				if err != nil {
					return nil, err
				}
				return worker.handleBatchRemainsOnErr(bo, rpcCtx, remains, resp.pbResp, task)
			}

			// Important: log enough context here even if the retry succeeds, so we can
			// still diagnose the root cause in production.
			fields := buildExceedsBoundDiagFields(worker.req, task, rpcCtx, worker.store.GetRegionCache(), bo, otherErr, resp.pbResp.GetLatestBucketsVersion())
			logutil.Logger(bo.GetCtx()).Warn("Request range exceeds bound - invalidating cache and retrying without buckets", fields...)

			// Invalidate the cached region to force refresh from PD
			worker.store.GetRegionCache().InvalidateCachedRegion(task.region)

			// Backoff before retry
			errStr := fmt.Sprintf("Request range exceeds bound: region_id:%v, region_ver:%v, store_type:%s, peer_addr:%s, error:%s",
				task.region.GetID(), task.region.GetVer(), task.storeType.Name(), task.storeAddr, otherErr)
			if err := bo.Backoff(tikv.BoRegionMiss(), errors.New(errStr)); err != nil {
				return nil, errors.Trace(err)
			}

			// Rebuild cop tasks with fresh region info, skipping bucket splitting
			// since buckets are suspected to be the cause of this error.
			// The new tasks will have skipBuckets=true set during construction.
			remains, err := buildCopTasks(bo, task.ranges, &buildCopTaskOpt{
				req:                         worker.req,
				cache:                       worker.store.GetRegionCache(),
				respChan:                    false,
				eventCb:                     task.eventCb,
				ignoreTiKVClientReadTimeout: true,
				skipBuckets:                 true,
				exceedsBoundRetry:           task.exceedsBoundRetry + 1,
			})
			if err != nil {
				return nil, err
			}
			return worker.handleBatchRemainsOnErr(bo, rpcCtx, remains, resp.pbResp, task)
		}

		otherErrFields := []zap.Field{
			zap.Uint64("connID", worker.req.ConnID),
			zap.String("connAlias", worker.req.ConnAlias),
			zap.Uint64("txnStartTS", worker.req.StartTs),
			zap.Uint64("regionID", task.region.GetID()),
			zap.Uint64("regionVer", task.region.GetVer()),
			zap.Uint64("regionConfVer", task.region.GetConfVer()),
			zap.Uint64("bucketsVer", task.bucketsVer),
			zap.Uint64("latestBucketsVer", resp.pbResp.GetLatestBucketsVersion()),
			zap.Int("rangeNums", task.ranges.Len()),
			zap.String("storeAddr", task.storeAddr),
			zap.String("error", otherErr),
		}
		if task.ranges.Len() > 0 {
			otherErrFields = append(otherErrFields,
				keyField("firstRangeStartKey", task.ranges.At(0).StartKey),
				keyField("lastRangeEndKey", task.ranges.At(task.ranges.Len()-1).EndKey),
			)
		}
		logutil.Logger(bo.GetCtx()).Warn("other error", otherErrFields...)

		if strings.Contains(err.Error(), "write conflict") {
			return nil, kv.ErrWriteConflict.FastGen("%s", otherErr)
		}
		return nil, errors.Trace(err)
	}
	// When the request is using paging API, the `Range` is not nil.
	if resp.pbResp.Range != nil {
		resp.startKey = resp.pbResp.Range.Start
	} else if task.ranges != nil && task.ranges.Len() > 0 {
		resp.startKey = task.ranges.At(0).StartKey
	}
	if err := worker.handleCollectExecutionInfo(bo, rpcCtx, resp); err != nil {
		return nil, err
	}
	resp.respTime = costTime

	if err := worker.handleCopCache(task, resp, cacheKey, cacheValue); err != nil {
		return nil, err
	}

	worker.checkRespOOM(resp)
	result := &copTaskResult{resp: resp}
	batchRespList, batchRemainTasks, err := worker.handleBatchCopResponse(bo, rpcCtx, resp.pbResp, task.batchTaskList)
	if err != nil {
		return result, err
	}
	result.batchRespList = batchRespList
	result.remains = batchRemainTasks
	return result, nil
}

func (worker *copIteratorWorker) handleBatchRemainsOnErr(bo *Backoffer, rpcCtx *tikv.RPCContext, remains []*copTask, resp *coprocessor.Response, task *copTask) (*copTaskResult, error) {
	if len(task.batchTaskList) == 0 {
		return &copTaskResult{remains: remains}, nil
	}
	batchedTasks := task.batchTaskList
	task.batchTaskList = nil
	batchRespList, remainTasks, err := worker.handleBatchCopResponse(bo, rpcCtx, resp, batchedTasks)
	if err != nil {
		return nil, err
	}
	return &copTaskResult{
		batchRespList: batchRespList,
		remains:       append(remains, remainTasks...),
	}, nil
}

func regionErrorDumpTriggerCheck(config *traceevent.DumpTriggerConfig) bool {
	return config.Event.Type == "region_error"
}

func getRegionError(ctx context.Context, resp interface{ GetRegionError() *errorpb.Error }) *errorpb.Error {
	err := resp.GetRegionError()
	if err != nil {
		traceevent.CheckFlightRecorderDumpTrigger(ctx, "dump_trigger.suspicious_event", regionErrorDumpTriggerCheck)
		return err
	}
	return nil
}

// handle the batched cop response.
// tasks will be changed, so the input tasks should not be used after calling this function.
func (worker *copIteratorWorker) handleBatchCopResponse(bo *Backoffer, rpcCtx *tikv.RPCContext, resp *coprocessor.Response,
	tasks map[uint64]*batchedCopTask) (batchRespList []*copResponse, remainTasks []*copTask, err error) {
	if len(tasks) == 0 {
		return nil, nil, nil
	}
	batchedNum := len(tasks)
	busyThresholdFallback := false
	defer func() {
		if err != nil {
			return
		}
		if !busyThresholdFallback {
			worker.storeBatchedNum.Add(uint64(batchedNum - len(remainTasks)))
			worker.storeBatchedFallbackNum.Add(uint64(len(remainTasks)))
		}
	}()
	appendRemainTasks := func(tasks ...*copTask) {
		if remainTasks == nil {
			// allocate size of remain length
			remainTasks = make([]*copTask, 0, len(tasks))
		}
		remainTasks = append(remainTasks, tasks...)
	}
	// need Addr for recording details.
	var dummyRPCCtx *tikv.RPCContext
	if rpcCtx != nil {
		dummyRPCCtx = &tikv.RPCContext{
			Addr: rpcCtx.Addr,
		}
	}
	batchResps := resp.GetBatchResponses()
	batchRespList = make([]*copResponse, 0, len(batchResps))
	for _, batchResp := range batchResps {
		taskID := batchResp.GetTaskId()
		batchedTask, ok := tasks[taskID]
		if !ok {
			return batchRespList, nil, errors.Errorf("task id %d not found", batchResp.GetTaskId())
		}
		delete(tasks, taskID)
		resp := &copResponse{
			pbResp: &coprocessor.Response{
				Data:          batchResp.Data,
				ExecDetailsV2: batchResp.ExecDetailsV2,
			},
		}
		task := batchedTask.task
		failpoint.Inject("batchCopRegionError", func() {
			batchResp.RegionError = &errorpb.Error{}
		})
		if regionErr := getRegionError(bo.GetCtx(), batchResp); regionErr != nil {
			errStr := fmt.Sprintf("region_id:%v, region_ver:%v, store_type:%s, peer_addr:%s, error:%s",
				task.region.GetID(), task.region.GetVer(), task.storeType.Name(), task.storeAddr, regionErr.String())
			if err := bo.Backoff(tikv.BoRegionMiss(), errors.New(errStr)); err != nil {
				return batchRespList, nil, errors.Trace(err)
			}
			remains, err := buildCopTasks(bo, task.ranges, &buildCopTaskOpt{
				req:                         worker.req,
				cache:                       worker.store.GetRegionCache(),
				respChan:                    false,
				eventCb:                     task.eventCb,
				ignoreTiKVClientReadTimeout: true,
				skipBuckets:                 task.skipBuckets,
				exceedsBoundRetry:           task.exceedsBoundRetry,
			})
			if err != nil {
				return batchRespList, nil, err
			}
			appendRemainTasks(remains...)
			continue
		}
		//TODO: handle locks in batch
		if lockErr := batchResp.GetLocked(); lockErr != nil {
			if err := worker.handleLockErr(bo, resp.pbResp.GetLocked(), task); err != nil {
				return batchRespList, nil, err
			}
			task.meetLockFallback = true
			appendRemainTasks(task)
			continue
		}
		if otherErr := batchResp.GetOtherError(); otherErr != "" {
			err := errors.Errorf("other error: %s", otherErr)

			firstRangeStartKey := task.ranges.At(0).StartKey
			lastRangeEndKey := task.ranges.At(task.ranges.Len() - 1).EndKey

			logutil.Logger(bo.GetCtx()).Warn("other error",
				zap.Uint64("txnStartTS", worker.req.StartTs),
				zap.Uint64("regionID", task.region.GetID()),
				zap.Uint64("regionVer", task.region.GetVer()),
				zap.Uint64("regionConfVer", task.region.GetConfVer()),
				zap.Uint64("bucketsVer", task.bucketsVer),
				// TODO: add bucket version in log
				//zap.Uint64("latestBucketsVer", batchResp.GetLatestBucketsVersion()),
				zap.Int("rangeNums", task.ranges.Len()),
				zap.ByteString("firstRangeStartKey", firstRangeStartKey),
				zap.ByteString("lastRangeEndKey", lastRangeEndKey),
				zap.String("storeAddr", task.storeAddr),
				zap.Error(err))
			if strings.Contains(err.Error(), "write conflict") {
				return batchRespList, nil, kv.ErrWriteConflict.FastGen("%s", otherErr)
			}
			return batchRespList, nil, errors.Trace(err)
		}
		if err := worker.handleCollectExecutionInfo(bo, dummyRPCCtx, resp); err != nil {
			return batchRespList, nil, err
		}
		worker.checkRespOOM(resp)
		batchRespList = append(batchRespList, resp)
	}
	for _, t := range tasks {
		task := t.task
		// when the error is generated by client or a load-based server busy,
		// response is empty by design, skip warning for this case.
		if len(batchResps) != 0 {
			firstRangeStartKey := task.ranges.At(0).StartKey
			lastRangeEndKey := task.ranges.At(task.ranges.Len() - 1).EndKey
			logutil.Logger(bo.GetCtx()).Error("response of batched task missing",
				zap.Uint64("id", task.taskID),
				zap.Uint64("txnStartTS", worker.req.StartTs),
				zap.Uint64("regionID", task.region.GetID()),
				zap.Uint64("regionVer", task.region.GetVer()),
				zap.Uint64("regionConfVer", task.region.GetConfVer()),
				zap.Uint64("bucketsVer", task.bucketsVer),
				zap.Int("rangeNums", task.ranges.Len()),
				zap.ByteString("firstRangeStartKey", firstRangeStartKey),
				zap.ByteString("lastRangeEndKey", lastRangeEndKey),
				zap.String("storeAddr", task.storeAddr))
		}
		appendRemainTasks(t.task)
	}
	if regionErr := getRegionError(bo.GetCtx(), resp); regionErr != nil && regionErr.ServerIsBusy != nil &&
		regionErr.ServerIsBusy.EstimatedWaitMs > 0 && len(remainTasks) != 0 {
		if len(batchResps) != 0 {
			return batchRespList, nil, errors.New("store batched coprocessor with server is busy error shouldn't contain responses")
		}
		busyThresholdFallback = true
		handler := newBatchTaskBuilder(bo, worker.req, worker.store.GetRegionCache(), kv.ReplicaReadFollower)
		for _, task := range remainTasks {
			// do not set busy threshold again.
			task.busyThreshold = 0
			if err = handler.handle(task); err != nil {
				return batchRespList, nil, err
			}
		}
		remainTasks = handler.build()
	}
	return batchRespList, remainTasks, nil
}

func (worker *copIteratorWorker) handleLockErr(bo *Backoffer, lockErr *kvrpcpb.LockInfo, task *copTask) error {
	if lockErr == nil {
		return nil
	}
	resolveLockDetail := worker.getLockResolverDetails()
	// Be care that we didn't redact the SQL statement because the log is DEBUG level.
	if task.eventCb != nil {
		task.eventCb(trxevents.WrapCopMeetLock(&trxevents.CopMeetLock{
			LockInfo: lockErr,
		}))
	} else {
		logutil.Logger(bo.GetCtx()).Debug("coprocessor encounters lock",
			zap.Stringer("lock", lockErr))
	}
	var locks []*txnlock.Lock
	if sharedLocks := lockErr.GetSharedLockInfos(); len(sharedLocks) > 0 {
		locks = make([]*txnlock.Lock, 0, len(sharedLocks))
		for _, l := range sharedLocks {
			locks = append(locks, txnlock.NewLock(l))
		}
	} else {
		locks = []*txnlock.Lock{txnlock.NewLock(lockErr)}
	}
	resolveLocksOpts := txnlock.ResolveLocksOptions{
		CallerStartTS: worker.req.StartTs,
		Locks:         locks,
		Detail:        resolveLockDetail,
	}
	resolveLocksRes, err1 := worker.kvclient.ResolveLocksWithOpts(bo.TiKVBackoffer(), resolveLocksOpts)
	err1 = derr.ToTiDBErr(err1)
	if err1 != nil {
		return errors.Trace(err1)
	}
	msBeforeExpired := resolveLocksRes.TTL
	if msBeforeExpired > 0 {
		if err := bo.BackoffWithMaxSleepTxnLockFast(int(msBeforeExpired), errors.New(lockErr.String())); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (worker *copIteratorWorker) buildCacheKey(task *copTask, copReq *coprocessor.Request) (cacheKey []byte, cacheValue *coprCacheValue) {
	// If there are many ranges, it is very likely to be a TableLookupRequest. They are not worth to cache since
	// computing is not the main cost. Ignore requests with many ranges directly to avoid slowly building the cache key.
	if task.cmdType == tikvrpc.CmdCop && worker.store.coprCache != nil && worker.req.Cacheable && worker.store.coprCache.CheckRequestAdmission(len(copReq.Ranges)) {
		cKey, err := coprCacheBuildKey(copReq)
		if err == nil {
			cacheKey = cKey
			cValue := worker.store.coprCache.Get(cKey)
			copReq.IsCacheEnabled = true

			if cValue != nil && cValue.RegionID == task.region.GetID() && cValue.TimeStamp <= worker.req.StartTs {
				// Append cache version to the request to skip Coprocessor computation if possible
				// when request result is cached
				copReq.CacheIfMatchVersion = cValue.RegionDataVersion
				cacheValue = cValue
			} else {
				copReq.CacheIfMatchVersion = 0
			}
		} else {
			logutil.BgLogger().Warn("Failed to build copr cache key", zap.Error(err))
		}
	}
	return
}

func (worker *copIteratorWorker) handleCopCache(task *copTask, resp *copResponse, cacheKey []byte, cacheValue *coprCacheValue) error {
	if resp.pbResp.IsCacheHit {
		if cacheValue == nil {
			return errors.New("Internal error: received illegal TiKV response")
		}
		copr_metrics.CoprCacheCounterHit.Add(1)
		// Cache hit and is valid: use cached data as response data and we don't update the cache.
		data := slices.Clone(cacheValue.Data)
		resp.pbResp.Data = data
		if worker.req.Paging.Enable {
			var start, end []byte
			if cacheValue.PageStart != nil {
				start = slices.Clone(cacheValue.PageStart)
			}
			if cacheValue.PageEnd != nil {
				end = slices.Clone(cacheValue.PageEnd)
			}
			// When paging protocol is used, the response key range is part of the cache data.
			if start != nil || end != nil {
				resp.pbResp.Range = &coprocessor.KeyRange{
					Start: start,
					End:   end,
				}
			} else {
				resp.pbResp.Range = nil
			}
		}
		// `worker.enableCollectExecutionInfo` is loaded from the instance's config. Because it's not related to the request,
		// the cache key can be same when `worker.enableCollectExecutionInfo` is true or false.
		// When `worker.enableCollectExecutionInfo` is false, the `resp.detail` is nil, and hit cache is still possible.
		// Check `resp.detail` to avoid panic.
		// Details: https://github.com/pingcap/tidb/issues/48212
		if resp.detail != nil {
			resp.detail.CoprCacheHit = true
		}
		return nil
	}
	copr_metrics.CoprCacheCounterMiss.Add(1)
	// Cache not hit or cache hit but not valid: update the cache if the response can be cached.
	if cacheKey != nil && resp.pbResp.CanBeCached && resp.pbResp.CacheLastVersion > 0 {
		if resp.detail != nil {
			if worker.store.coprCache.CheckResponseAdmission(resp.pbResp.Data.Size(), resp.detail.TimeDetail.ProcessTime, task.pagingTaskIdx) {
				data := slices.Clone(resp.pbResp.Data)

				newCacheValue := coprCacheValue{
					Data:              data,
					TimeStamp:         worker.req.StartTs,
					RegionID:          task.region.GetID(),
					RegionDataVersion: resp.pbResp.CacheLastVersion,
				}
				// When paging protocol is used, the response key range is part of the cache data.
				if r := resp.pbResp.GetRange(); r != nil {
					newCacheValue.PageStart = slices.Clone(r.GetStart())
					newCacheValue.PageEnd = slices.Clone(r.GetEnd())
				}
				worker.store.coprCache.Set(cacheKey, &newCacheValue)
			}
		}
	}
	return nil
}

func (worker *copIteratorWorker) getLockResolverDetails() *util.ResolveLockDetail {
	if worker.stats == nil {
		return nil
	}
	return &util.ResolveLockDetail{}
}

func (worker *copIteratorWorker) handleCollectExecutionInfo(bo *Backoffer, rpcCtx *tikv.RPCContext, resp *copResponse) error {
	if worker.stats == nil {
		return nil
	}
	failpoint.Inject("disable-collect-execution", func(val failpoint.Value) {
		if val.(bool) {
			panic("shouldn't reachable")
		}
	})
	if resp.detail == nil {
		resp.detail = new(CopRuntimeStats)
		resp.detail.ScanDetail = &util.ScanDetail{}
	}
	return worker.collectCopRuntimeStats(resp.detail, bo, rpcCtx, resp)
}

func (worker *copIteratorWorker) collectCopRuntimeStats(copStats *CopRuntimeStats, bo *Backoffer, rpcCtx *tikv.RPCContext, resp *copResponse) error {
	worker.collectKVClientRuntimeStats(copStats, bo, rpcCtx)
	if resp == nil {
		return nil
	}
	if pbDetails := resp.pbResp.ExecDetailsV2; pbDetails != nil {
		// Take values in `ExecDetailsV2` first.
		if pbDetails.TimeDetail != nil || pbDetails.TimeDetailV2 != nil {
			copStats.TimeDetail.MergeFromTimeDetail(pbDetails.TimeDetailV2, pbDetails.TimeDetail)
		}
		if scanDetailV2 := pbDetails.ScanDetailV2; scanDetailV2 != nil {
			copStats.ScanDetail.MergeFromScanDetailV2(scanDetailV2)
		}
	} else if pbDetails := resp.pbResp.ExecDetails; pbDetails != nil {
		if timeDetail := pbDetails.TimeDetail; timeDetail != nil {
			copStats.TimeDetail.MergeFromTimeDetail(nil, timeDetail)
		}
		if scanDetail := pbDetails.ScanDetail; scanDetail != nil {
			if scanDetail.Write != nil {
				copStats.ScanDetail.ProcessedKeys = scanDetail.Write.Processed
				copStats.ScanDetail.TotalKeys = scanDetail.Write.Total
			}
		}
	}

	if worker.req.RunawayChecker != nil {
		var ruDetail *util.RUDetails
		if ruDetailRaw := bo.GetCtx().Value(util.RUDetailsCtxKey); ruDetailRaw != nil {
			ruDetail = ruDetailRaw.(*util.RUDetails)
		}
		if err := worker.req.RunawayChecker.CheckThresholds(ruDetail, copStats.ScanDetail.ProcessedKeys, nil); err != nil {
			return err
		}
	}
	return nil
}

func (worker *copIteratorWorker) collectKVClientRuntimeStats(copStats *CopRuntimeStats, bo *Backoffer, rpcCtx *tikv.RPCContext) {
	if rpcCtx != nil {
		copStats.CalleeAddress = rpcCtx.Addr
	}
	if worker.kvclient.Stats == nil {
		return
	}
	defer func() {
		worker.kvclient.Stats = nil
	}()
	copStats.ReqStats = worker.kvclient.Stats
	backoffTimes := bo.GetBackoffTimes()
	if len(backoffTimes) > 0 {
		copStats.BackoffTime = time.Duration(bo.GetTotalSleep()) * time.Millisecond
		copStats.BackoffSleep = make(map[string]time.Duration, len(backoffTimes))
		copStats.BackoffTimes = make(map[string]int, len(backoffTimes))
		for backoff := range backoffTimes {
			copStats.BackoffTimes[backoff] = backoffTimes[backoff]
			copStats.BackoffSleep[backoff] = time.Duration(bo.GetBackoffSleepMS()[backoff]) * time.Millisecond
		}
	}
}

func (worker *copIteratorWorker) collectUnconsumedCopRuntimeStats(bo *Backoffer, rpcCtx *tikv.RPCContext) {
	if worker.kvclient.Stats != nil && worker.stats != nil {
		copStats := &CopRuntimeStats{}
		worker.collectKVClientRuntimeStats(copStats, bo, rpcCtx)
		worker.stats.Lock()
		worker.stats.stats = append(worker.stats.stats, copStats)
		worker.stats.Unlock()
	}
}

// CopRuntimeStats contains execution detail information.
type CopRuntimeStats struct {
	execdetails.CopExecDetails
	ReqStats *tikv.RegionRequestRuntimeStats

	CoprCacheHit bool
}

type copIteratorRuntimeStats struct {
	sync.Mutex
	stats []*CopRuntimeStats
}

func (worker *copIteratorWorker) handleTiDBSendReqErr(err error, task *copTask) (*copTaskResult, error) {
	errCode := errno.ErrUnknown
	errMsg := err.Error()
	if terror.ErrorEqual(err, derr.ErrTiKVServerTimeout) {
		errCode = errno.ErrTiKVServerTimeout
		errMsg = "TiDB server timeout, address is " + task.storeAddr
	}
	if terror.ErrorEqual(err, derr.ErrTiFlashServerTimeout) {
		errCode = errno.ErrTiFlashServerTimeout
		errMsg = "TiDB server timeout, address is " + task.storeAddr
	}
	selResp := tipb.SelectResponse{
		Warnings: []*tipb.Error{
			{
				Code: int32(errCode),
				Msg:  errMsg,
			},
		},
	}
	data, err := proto.Marshal(&selResp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp := &copResponse{
		pbResp: &coprocessor.Response{
			Data: data,
		},
		detail: &CopRuntimeStats{},
	}
	worker.checkRespOOM(resp)
	return &copTaskResult{resp: resp}, nil
}

// calculateRetry splits the input ranges into two, and take one of them according to desc flag.
// It's used in paging API, to calculate which range is consumed and what needs to be retry.
// For example:
// ranges: [r1 --> r2) [r3 --> r4)
// split:      [s1   -->   s2)
// In normal scan order, all data before s1 is consumed, so the retry ranges should be [s1 --> r2) [r3 --> r4)
// In reverse scan order, all data after s2 is consumed, so the retry ranges should be [r1 --> r2) [r3 --> s2)
func (worker *copIteratorWorker) calculateRetry(ranges *KeyRanges, split *coprocessor.KeyRange, desc bool) *KeyRanges {
	if split == nil {
		return ranges
	}
	if desc {
		left, _ := ranges.Split(split.End)
		return left
	}
	_, right := ranges.Split(split.Start)
	return right
}

// calculateRemain calculates the remain ranges to be processed, it's used in paging API.
// For example:
// ranges: [r1 --> r2) [r3 --> r4)
// split:      [s1   -->   s2)
// In normal scan order, all data before s2 is consumed, so the remained ranges should be [s2 --> r4)
// In reverse scan order, all data after s1 is consumed, so the remained ranges should be [r1 --> s1)
func (worker *copIteratorWorker) calculateRemain(ranges *KeyRanges, split *coprocessor.KeyRange, desc bool) *KeyRanges {
	if split == nil {
		return ranges
	}
	if desc {
		left, _ := ranges.Split(split.Start)
		return left
	}
	_, right := ranges.Split(split.End)
	return right
}

// finished checks the flags and finished channel, it tells whether the worker is finished.
func (worker *copIteratorWorker) finished() bool {
	if worker.vars != nil && worker.vars.Killed != nil {
		killed := atomic.LoadUint32(worker.vars.Killed)
		if killed != 0 {
			logutil.BgLogger().Info(
				"a killed signal is received in copIteratorWorker",
				zap.Uint32("signal", killed),
			)
			return true
		}
	}
	select {
	case <-worker.finishCh:
		return true
	default:
		return false
	}
}

func (it *copIterator) Close() error {
	if atomic.CompareAndSwapUint32(&it.closed, 0, 1) {
		close(it.finishCh)
	}
	it.rpcCancel.CancelAll()
	it.actionOnExceed.close()
	it.wg.Wait()
	return nil
}

// copErrorResponse returns error when calling Next()
type copErrorResponse struct{ error }

func (it copErrorResponse) Next(ctx context.Context) (kv.ResultSubset, error) {
	return nil, it.error
}

func (it copErrorResponse) Close() error {
	return nil
}

// rateLimitAction an OOM Action which is used to control the token if OOM triggered. The token number should be
// set on initial. Each time the Action is triggered, one token would be destroyed. If the count of the token is less
// than 2, the action would be delegated to the fallback action.
type rateLimitAction struct {
	memory.BaseOOMAction
	// enabled indicates whether the rateLimitAction is permitted to Action. 1 means permitted, 0 denied.
	enabled uint32
	// totalTokenNum indicates the total token at initial
	totalTokenNum uint
	cond          struct {
		sync.Mutex
		// exceeded indicates whether have encountered OOM situation.
		exceeded bool
		// remainingTokenNum indicates the count of tokens which still exists
		remainingTokenNum uint
		once              sync.Once
		// triggerCountForTest indicates the total count of the rateLimitAction's Action being executed
		triggerCountForTest uint
	}
}

func newRateLimitAction(totalTokenNumber uint) *rateLimitAction {
	return &rateLimitAction{
		totalTokenNum: totalTokenNumber,
		cond: struct {
			sync.Mutex
			exceeded            bool
			remainingTokenNum   uint
			once                sync.Once
			triggerCountForTest uint
		}{
			Mutex:             sync.Mutex{},
			exceeded:          false,
			remainingTokenNum: totalTokenNumber,
			once:              sync.Once{},
		},
	}
}

// Action implements ActionOnExceed.Action
func (e *rateLimitAction) Action(t *memory.Tracker) {
	if !e.isEnabled() {
		if fallback := e.GetFallback(); fallback != nil {
			fallback.Action(t)
		}
		return
	}
	e.conditionLock()
	defer e.conditionUnlock()
	e.cond.once.Do(func() {
		if e.cond.remainingTokenNum < 2 {
			e.setEnabled(false)
			logutil.BgLogger().Info("memory exceeds quota, rateLimitAction delegate to fallback action",
				zap.Uint("total token count", e.totalTokenNum))
			if fallback := e.GetFallback(); fallback != nil {
				fallback.Action(t)
			}
			return
		}
		failpoint.Inject("testRateLimitActionMockConsumeAndAssert", func(val failpoint.Value) {
			if val.(bool) {
				if e.cond.triggerCountForTest+e.cond.remainingTokenNum != e.totalTokenNum {
					panic("triggerCount + remainingTokenNum not equal to totalTokenNum")
				}
			}
		})
		logutil.BgLogger().Info("memory exceeds quota, destroy one token now.",
			zap.Int64("consumed", t.BytesConsumed()),
			zap.Int64("quota", t.GetBytesLimit()),
			zap.Uint("total token count", e.totalTokenNum),
			zap.Uint("remaining token count", e.cond.remainingTokenNum))
		e.cond.exceeded = true
		e.cond.triggerCountForTest++
	})
}

// GetPriority get the priority of the Action.
func (e *rateLimitAction) GetPriority() int64 {
	return memory.DefRateLimitPriority
}

// destroyTokenIfNeeded will check the `exceed` flag after copWorker finished one task.
// If the exceed flag is true and there is no token been destroyed before, one token will be destroyed,
// or the token would be return back.
func (e *rateLimitAction) destroyTokenIfNeeded(returnToken func()) {
	if !e.isEnabled() {
		returnToken()
		return
	}
	e.conditionLock()
	defer e.conditionUnlock()
	if !e.cond.exceeded {
		returnToken()
		return
	}
	// If actionOnExceed has been triggered and there is no token have been destroyed before,
	// destroy one token.
	e.cond.remainingTokenNum = e.cond.remainingTokenNum - 1
	e.cond.exceeded = false
	e.cond.once = sync.Once{}
}

func (e *rateLimitAction) conditionLock() {
	e.cond.Lock()
}

func (e *rateLimitAction) conditionUnlock() {
	e.cond.Unlock()
}

func (e *rateLimitAction) close() {
	if !e.isEnabled() {
		return
	}
	e.setEnabled(false)
	e.conditionLock()
	defer e.conditionUnlock()
	e.cond.exceeded = false
	e.SetFinished()
}

func (e *rateLimitAction) setEnabled(enabled bool) {
	newValue := uint32(0)
	if enabled {
		newValue = uint32(1)
	}
	atomic.StoreUint32(&e.enabled, newValue)
}

func (e *rateLimitAction) isEnabled() bool {
	return atomic.LoadUint32(&e.enabled) > 0
}

// priorityToPB converts priority type to wire type.
func priorityToPB(pri int) kvrpcpb.CommandPri {
	switch pri {
	case kv.PriorityLow:
		return kvrpcpb.CommandPri_Low
	case kv.PriorityHigh:
		return kvrpcpb.CommandPri_High
	default:
		return kvrpcpb.CommandPri_Normal
	}
}

func isolationLevelToPB(level kv.IsoLevel) kvrpcpb.IsolationLevel {
	switch level {
	case kv.RC:
		return kvrpcpb.IsolationLevel_RC
	case kv.SI:
		return kvrpcpb.IsolationLevel_SI
	case kv.RCCheckTS:
		return kvrpcpb.IsolationLevel_RCCheckTS
	default:
		return kvrpcpb.IsolationLevel_SI
	}
}

// BuildKeyRanges is used for test, quickly build key ranges from paired keys.
func BuildKeyRanges(keys ...string) []kv.KeyRange {
	var ranges []kv.KeyRange
	for i := 0; i < len(keys); i += 2 {
		ranges = append(ranges, kv.KeyRange{
			StartKey: []byte(keys[i]),
			EndKey:   []byte(keys[i+1]),
		})
	}
	return ranges
}

func optRowHint(req *kv.Request) bool {
	opt := true
	if req.StoreType == kv.TiDB {
		return false
	}
	if req.RequestSource.RequestSourceInternal || req.Tp != kv.ReqTypeDAG {
		// disable extra concurrency for internal tasks.
		return false
	}
	failpoint.Inject("disableFixedRowCountHint", func(_ failpoint.Value) {
		opt = false
	})
	return opt
}

func checkStoreBatchCopr(req *kv.Request) bool {
	if req.Tp != kv.ReqTypeDAG || req.StoreType != kv.TiKV {
		return false
	}
	// TODO: support keep-order batch
	if req.ReplicaRead != kv.ReplicaReadLeader || req.KeepOrder {
		// Disable batch copr for follower read
		return false
	}
	// Disable batch copr when paging is enabled.
	if req.Paging.Enable {
		return false
	}
	// Disable it for internal requests to avoid regression.
	if req.RequestSource.RequestSourceInternal {
		return false
	}
	return true
}
