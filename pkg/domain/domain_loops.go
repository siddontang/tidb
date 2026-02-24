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

package domain

import (
	"context"
	"encoding/json"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/config"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	metrics2 "github.com/pingcap/tidb/pkg/planner/core/metrics"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/resourcegroup/runaway"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/telemetry"
	"github.com/pingcap/tidb/pkg/ttl/ttlworker"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/pingcap/tidb/pkg/util/expensivequery"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memoryusagealarm"
	"github.com/pingcap/tidb/pkg/util/replayer"
	"github.com/pingcap/tidb/pkg/util/servermemorylimit"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/pingcap/tidb/pkg/workloadlearning"
	"github.com/tikv/client-go/v2/tikv"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func (do *Domain) decodePrivilegeEvent(resp clientv3.WatchResponse) PrivilegeEvent {
	var msg PrivilegeEvent
	isNewVersionEvents := false
	for _, event := range resp.Events {
		if event.Kv != nil {
			val := event.Kv.Value
			if len(val) > 0 {
				var tmp PrivilegeEvent
				err := json.Unmarshal(val, &tmp)
				if err != nil {
					logutil.BgLogger().Warn("decodePrivilegeEvent unmarshal fail", zap.Error(err))
					break
				}
				isNewVersionEvents = true
				if do.ServerID() != 0 && tmp.ServerID == do.ServerID() {
					// Skip the events from this TiDB-Server
					continue
				}
				if tmp.All {
					msg.All = true
					break
				}
				// duplicated users in list is ok.
				msg.UserList = append(msg.UserList, tmp.UserList...)
			}
		}
	}

	// In case old version triggers the event, the event value is empty,
	// Then we fall back to the old way: reload all the users.
	if len(msg.UserList) == 0 && !isNewVersionEvents {
		msg.All = true
	}
	return msg
}

func (do *Domain) batchReadMoreData(ch clientv3.WatchChan, event PrivilegeEvent) PrivilegeEvent {
	timer := time.NewTimer(5 * time.Millisecond)
	defer timer.Stop()
	const maxBatchSize = 128
	for range maxBatchSize {
		select {
		case resp, ok := <-ch:
			if !ok {
				return event
			}
			tmp := do.decodePrivilegeEvent(resp)
			if tmp.All {
				event.All = true
			} else {
				if !event.All {
					event.UserList = append(event.UserList, tmp.UserList...)
				}
			}
			succ := timer.Reset(5 * time.Millisecond)
			if !succ {
				return event
			}
		case <-timer.C:
			return event
		}
	}
	return event
}

// LoadPrivilegeLoop create a goroutine loads privilege tables in a loop, it
// should be called only once in BootstrapSession.
func (do *Domain) LoadPrivilegeLoop(sctx sessionctx.Context) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	sctx.GetSessionVars().InRestrictedSQL = true
	_, err := sctx.GetSQLExecutor().ExecuteInternal(ctx, "set @@autocommit = 1")
	if err != nil {
		return err
	}
	do.privHandle = privileges.NewHandle(do.SysSessionPool(), sctx.GetSessionVars().GlobalVarsAccessor)

	var watchCh clientv3.WatchChan
	duration := 5 * time.Minute
	if do.etcdClient != nil {
		watchCh = do.etcdClient.Watch(do.ctx, privilegeKey)
		duration = 10 * time.Minute
	}

	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Info("loadPrivilegeInLoop exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "loadPrivilegeInLoop", nil, false)

		var count int
		for {
			var event PrivilegeEvent
			select {
			case <-do.exit:
				return
			case resp, ok := <-watchCh:
				if ok {
					count = 0
					event = do.decodePrivilegeEvent(resp)
					event = do.batchReadMoreData(watchCh, event)
				} else {
					if do.ctx.Err() == nil {
						logutil.BgLogger().Warn("load privilege loop watch channel closed")
						watchCh = do.etcdClient.Watch(do.ctx, privilegeKey)
						count++
						if count > 10 {
							time.Sleep(time.Duration(count) * time.Second)
						}
						continue
					}
				}
			case <-time.After(duration):
				event.All = true
				event = do.batchReadMoreData(watchCh, event)
			}

			// All events are from this TiDB-Server, skip them
			if !event.All && len(event.UserList) == 0 {
				continue
			}

			err := privReloadEvent(do.privHandle, &event)
			metrics.LoadPrivilegeCounter.WithLabelValues(metrics.RetLabel(err)).Inc()
			if err != nil {
				logutil.BgLogger().Error("load privilege failed", zap.Error(err))
			}
		}
	}, "loadPrivilegeInLoop")
	return nil
}

func privReloadEvent(h *privileges.Handle, event *PrivilegeEvent) (err error) {
	switch {
	case !vardef.AccelerateUserCreationUpdate.Load():
		err = h.UpdateAll()
	case event.All:
		err = h.UpdateAllActive()
	default:
		err = h.Update(event.UserList)
	}
	return
}

// LoadSysVarCacheLoop create a goroutine loads sysvar cache in a loop,
// it should be called only once in BootstrapSession.
func (do *Domain) LoadSysVarCacheLoop(ctx sessionctx.Context) error {
	ctx.GetSessionVars().InRestrictedSQL = true
	err := do.rebuildSysVarCache(ctx)
	if err != nil {
		return err
	}
	var watchCh clientv3.WatchChan
	duration := 30 * time.Second
	if do.etcdClient != nil {
		watchCh = do.etcdClient.Watch(context.Background(), sysVarCacheKey)
	}

	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Info("LoadSysVarCacheLoop exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "LoadSysVarCacheLoop", nil, false)

		var count int
		for {
			ok := true
			select {
			case <-do.exit:
				return
			case _, ok = <-watchCh:
			case <-time.After(duration):
			}

			failpoint.Inject("skipLoadSysVarCacheLoop", func(val failpoint.Value) {
				// In some pkg integration test, there are many testSuite, and each testSuite has separate storage and
				// `LoadSysVarCacheLoop` background goroutine. Then each testSuite `RebuildSysVarCache` from it's
				// own storage.
				// Each testSuit will also call `checkEnableServerGlobalVar` to update some local variables.
				// That's the problem, each testSuit use different storage to update some same local variables.
				// So just skip `RebuildSysVarCache` in some integration testing.
				if val.(bool) {
					failpoint.Continue()
				}
			})

			if !ok {
				logutil.BgLogger().Warn("LoadSysVarCacheLoop loop watch channel closed")
				watchCh = do.etcdClient.Watch(context.Background(), sysVarCacheKey)
				count++
				if count > 10 {
					time.Sleep(time.Duration(count) * time.Second)
				}
				continue
			}
			count = 0
			logutil.BgLogger().Debug("Rebuilding sysvar cache from etcd watch event.")
			err := do.rebuildSysVarCache(ctx)
			metrics.LoadSysVarCacheCounter.WithLabelValues(metrics.RetLabel(err)).Inc()
			if err != nil {
				logutil.BgLogger().Warn("LoadSysVarCacheLoop failed", zap.Error(err))
			}
		}
	}, "LoadSysVarCacheLoop")
	return nil
}

// WatchTiFlashComputeNodeChange create a routine to watch if the topology of tiflash_compute node is changed.
// TODO: tiflashComputeNodeKey is not put to etcd yet(finish this when AutoScaler is done)
//
//	store cache will only be invalidated every n seconds.
func (do *Domain) WatchTiFlashComputeNodeChange() error {
	var watchCh clientv3.WatchChan
	if do.etcdClient != nil {
		watchCh = do.etcdClient.Watch(context.Background(), tiflashComputeNodeKey)
	}
	duration := 10 * time.Second
	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Info("WatchTiFlashComputeNodeChange exit")
		}()
		defer util.Recover(metrics.LabelDomain, "WatchTiFlashComputeNodeChange", nil, false)

		var count int
		var logCount int
		for {
			ok := true
			var watched bool
			select {
			case <-do.exit:
				return
			case _, ok = <-watchCh:
				watched = true
			case <-time.After(duration):
			}
			if !ok {
				logutil.BgLogger().Error("WatchTiFlashComputeNodeChange watch channel closed")
				watchCh = do.etcdClient.Watch(context.Background(), tiflashComputeNodeKey)
				count++
				if count > 10 {
					time.Sleep(time.Duration(count) * time.Second)
				}
				continue
			}
			count = 0
			switch s := do.store.(type) {
			case tikv.Storage:
				logCount++
				s.GetRegionCache().InvalidateTiFlashComputeStores()
				if logCount == 6 {
					// Print log every 6*duration seconds.
					logutil.BgLogger().Debug("tiflash_compute store cache invalied, will update next query", zap.Bool("watched", watched))
					logCount = 0
				}
			default:
				logutil.BgLogger().Debug("No need to watch tiflash_compute store cache for non-tikv store")
				return
			}
		}
	}, "WatchTiFlashComputeNodeChange")
	return nil
}

// PrivilegeHandle returns the MySQLPrivilege.
func (do *Domain) PrivilegeHandle() *privileges.Handle {
	return do.privHandle
}

// BindingHandle returns domain's bindHandle.
func (do *Domain) BindingHandle() bindinfo.BindingHandle {
	v := do.bindHandle.Load()
	if v == nil {
		return nil
	}
	return v.(bindinfo.BindingHandle)
}

// InitBindingHandle create a goroutine loads BindInfo in a loop, it should
// be called only once in BootstrapSession.
func (do *Domain) InitBindingHandle() error {
	do.bindHandle.Store(bindinfo.NewBindingHandle(do.sysSessionPool))
	err := do.BindingHandle().LoadFromStorageToCache(true, false)
	if err != nil || bindinfo.Lease == 0 {
		return err
	}

	owner := do.NewOwnerManager(bindinfo.Prompt, bindinfo.OwnerKey)
	err = owner.CampaignOwner()
	if err != nil {
		logutil.BgLogger().Warn("campaign owner failed", zap.Error(err))
		return err
	}
	do.globalBindHandleWorkerLoop(owner)
	return nil
}

func (do *Domain) globalBindHandleWorkerLoop(owner owner.Manager) {
	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Info("globalBindHandleWorkerLoop exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "globalBindHandleWorkerLoop", nil, false)

		bindWorkerTicker := time.NewTicker(bindinfo.Lease)
		gcBindTicker := time.NewTicker(100 * bindinfo.Lease)
		writeBindingUsageTicker := time.NewTicker(100 * bindinfo.Lease)
		defer func() {
			bindWorkerTicker.Stop()
			gcBindTicker.Stop()
			writeBindingUsageTicker.Stop()
		}()
		for {
			select {
			case <-do.exit:
				do.BindingHandle().Close()
				owner.Close()
				return
			case <-bindWorkerTicker.C:
				bindHandle := do.BindingHandle()
				err := bindHandle.LoadFromStorageToCache(false, false)
				if err != nil {
					logutil.BgLogger().Error("update bindinfo failed", zap.Error(err))
				}
			case <-gcBindTicker.C:
				if !owner.IsOwner() {
					continue
				}
				err := do.BindingHandle().GCBinding()
				if err != nil {
					logutil.BgLogger().Error("GC bind record failed", zap.Error(err))
				}
			case <-writeBindingUsageTicker.C:
				bindHandle := do.BindingHandle()
				err := bindHandle.UpdateBindingUsageInfoToStorage()
				if err != nil {
					logutil.BgLogger().Warn("BindingHandle.UpdateBindingUsageInfoToStorage", zap.Error(err))
				}
				// randomize the next write interval to avoid thundering herd problem
				// if there are many tidb servers. The next write interval is [3h, 6h].
				writeBindingUsageTicker.Reset(
					randomDuration(
						3*60*60, // 3h
						6*60*60, // 6h
					),
				)
			}
		}
	}, "globalBindHandleWorkerLoop")
}

func randomDuration(minSeconds, maxSeconds int) time.Duration {
	randomIntervalSeconds := rand.Intn(maxSeconds-minSeconds+1) + minSeconds
	newDuration := time.Duration(randomIntervalSeconds) * time.Second
	return newDuration
}

// TelemetryLoop create a goroutine that reports usage data in a loop, it should be called only once
// in BootstrapSession.
func (do *Domain) TelemetryLoop(ctx sessionctx.Context) {
	ctx.GetSessionVars().InRestrictedSQL = true
	err := telemetry.InitialRun(ctx)
	if err != nil {
		logutil.BgLogger().Warn("Initial telemetry run failed", zap.Error(err))
	}

	reportTicker := time.NewTicker(telemetry.ReportInterval)
	subWindowTicker := time.NewTicker(telemetry.SubWindowSize)

	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Info("TelemetryReportLoop exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "TelemetryReportLoop", nil, false)

		for {
			select {
			case <-do.exit:
				return
			case <-reportTicker.C:
				err := telemetry.ReportUsageData(ctx)
				if err != nil {
					logutil.BgLogger().Warn("TelemetryLoop retports usaged data failed", zap.Error(err))
				}
			case <-subWindowTicker.C:
				telemetry.RotateSubWindow()
			}
		}
	}, "TelemetryLoop")
}

// SetupPlanReplayerHandle setup plan replayer handle
func (do *Domain) SetupPlanReplayerHandle(collectorSctx sessionctx.Context, workersSctxs []sessionctx.Context) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	do.planReplayerHandle = &planReplayerHandle{}
	do.planReplayerHandle.planReplayerTaskCollectorHandle = &planReplayerTaskCollectorHandle{
		ctx:  ctx,
		sctx: collectorSctx,
	}
	taskCH := make(chan *PlanReplayerDumpTask, 16)
	taskStatus := &planReplayerDumpTaskStatus{}
	taskStatus.finishedTaskMu.finishedTask = map[replayer.PlanReplayerTaskKey]struct{}{}
	taskStatus.runningTaskMu.runningTasks = map[replayer.PlanReplayerTaskKey]struct{}{}

	do.planReplayerHandle.planReplayerTaskDumpHandle = &planReplayerTaskDumpHandle{
		taskCH: taskCH,
		status: taskStatus,
	}
	do.planReplayerHandle.planReplayerTaskDumpHandle.workers = make([]*planReplayerTaskDumpWorker, 0)
	for i := range workersSctxs {
		worker := &planReplayerTaskDumpWorker{
			ctx:    ctx,
			sctx:   workersSctxs[i],
			taskCH: taskCH,
			status: taskStatus,
		}
		do.planReplayerHandle.planReplayerTaskDumpHandle.workers = append(do.planReplayerHandle.planReplayerTaskDumpHandle.workers, worker)
	}
}

// RunawayManager returns the runaway manager.
func (do *Domain) RunawayManager() *runaway.Manager {
	return do.runawayManager
}

// ResourceGroupsController returns the resource groups controller.
func (do *Domain) ResourceGroupsController() *rmclient.ResourceGroupsController {
	return do.resourceGroupsController.Load()
}

// SetResourceGroupsController is only used in test.
func (do *Domain) SetResourceGroupsController(controller *rmclient.ResourceGroupsController) {
	do.resourceGroupsController.Store(controller)
}

// SetupHistoricalStatsWorker setups worker
func (do *Domain) SetupHistoricalStatsWorker(ctx sessionctx.Context) {
	do.historicalStatsWorker = &HistoricalStatsWorker{
		tblCH: make(chan int64, 16),
		sctx:  ctx,
	}
}

// SetupDumpFileGCChecker setup sctx
func (do *Domain) SetupDumpFileGCChecker(ctx sessionctx.Context) {
	do.dumpFileGcChecker.setupSctx(ctx)
	do.dumpFileGcChecker.planReplayerTaskStatus = do.planReplayerHandle.status
}

// SetupExtractHandle setups extract handler
func (do *Domain) SetupExtractHandle(sctxs []sessionctx.Context) {
	do.extractTaskHandle = newExtractHandler(do.ctx, sctxs)
}

var planReplayerHandleLease atomic.Uint64

func init() {
	planReplayerHandleLease.Store(uint64(10 * time.Second))
	enableDumpHistoricalStats.Store(true)
}

// DisablePlanReplayerBackgroundJob4Test disable plan replayer handle for test
func DisablePlanReplayerBackgroundJob4Test() {
	planReplayerHandleLease.Store(0)
}

// DisableDumpHistoricalStats4Test disable historical dump worker for test
func DisableDumpHistoricalStats4Test() {
	enableDumpHistoricalStats.Store(false)
}

// StartPlanReplayerHandle start plan replayer handle job
func (do *Domain) StartPlanReplayerHandle() {
	lease := planReplayerHandleLease.Load()
	if lease < 1 {
		return
	}
	do.wg.Run(func() {
		logutil.BgLogger().Info("PlanReplayerTaskCollectHandle started")
		tikcer := time.NewTicker(time.Duration(lease))
		defer func() {
			tikcer.Stop()
			logutil.BgLogger().Info("PlanReplayerTaskCollectHandle exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "PlanReplayerTaskCollectHandle", nil, false)

		for {
			select {
			case <-do.exit:
				return
			case <-tikcer.C:
				err := do.planReplayerHandle.CollectPlanReplayerTask()
				if err != nil {
					logutil.BgLogger().Warn("plan replayer handle collect tasks failed", zap.Error(err))
				}
			}
		}
	}, "PlanReplayerTaskCollectHandle")

	do.wg.Run(func() {
		logutil.BgLogger().Info("PlanReplayerTaskDumpHandle started")
		defer func() {
			logutil.BgLogger().Info("PlanReplayerTaskDumpHandle exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "PlanReplayerTaskDumpHandle", nil, false)

		for _, worker := range do.planReplayerHandle.planReplayerTaskDumpHandle.workers {
			go worker.run()
		}
		<-do.exit
		do.planReplayerHandle.planReplayerTaskDumpHandle.Close()
	}, "PlanReplayerTaskDumpHandle")
}

// GetPlanReplayerHandle returns plan replayer handle
func (do *Domain) GetPlanReplayerHandle() *planReplayerHandle {
	return do.planReplayerHandle
}

// GetExtractHandle returns extract handle
func (do *Domain) GetExtractHandle() *ExtractHandle {
	return do.extractTaskHandle
}

// GetDumpFileGCChecker returns dump file GC checker for plan replayer and plan trace
func (do *Domain) GetDumpFileGCChecker() *dumpFileGcChecker {
	return do.dumpFileGcChecker
}

// DumpFileGcCheckerLoop creates a goroutine that handles `exit` and `gc`.
func (do *Domain) DumpFileGcCheckerLoop() {
	do.wg.Run(func() {
		logutil.BgLogger().Info("dumpFileGcChecker started")
		gcTicker := time.NewTicker(do.dumpFileGcChecker.gcLease)
		defer func() {
			gcTicker.Stop()
			logutil.BgLogger().Info("dumpFileGcChecker exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "dumpFileGcCheckerLoop", nil, false)

		for {
			select {
			case <-do.exit:
				return
			case <-gcTicker.C:
				do.dumpFileGcChecker.GCDumpFiles(do.ctx, time.Hour, time.Hour*24*7)
			}
		}
	}, "dumpFileGcChecker")
}

// GetHistoricalStatsWorker gets historical workers
func (do *Domain) GetHistoricalStatsWorker() *HistoricalStatsWorker {
	return do.historicalStatsWorker
}

// EnableDumpHistoricalStats used to control whether enable dump stats for unit test
var enableDumpHistoricalStats atomic.Bool

// StartHistoricalStatsWorker start historical workers running
func (do *Domain) StartHistoricalStatsWorker() {
	if !enableDumpHistoricalStats.Load() {
		return
	}
	do.wg.Run(func() {
		logutil.BgLogger().Info("HistoricalStatsWorker started")
		defer func() {
			logutil.BgLogger().Info("HistoricalStatsWorker exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "HistoricalStatsWorkerLoop", nil, false)

		for {
			select {
			case <-do.exit:
				close(do.historicalStatsWorker.tblCH)
				return
			case tblID, ok := <-do.historicalStatsWorker.tblCH:
				if !ok {
					return
				}
				err := do.historicalStatsWorker.DumpHistoricalStats(tblID, do.StatsHandle())
				if err != nil {
					logutil.BgLogger().Warn("dump historical stats failed", zap.Error(err), zap.Int64("tableID", tblID))
				}
			}
		}
	}, "HistoricalStatsWorker")
}

// ExpensiveQueryHandle returns the expensive query handle.
func (do *Domain) ExpensiveQueryHandle() *expensivequery.Handle {
	return do.expensiveQueryHandle
}

// MemoryUsageAlarmHandle returns the memory usage alarm handle.
func (do *Domain) MemoryUsageAlarmHandle() *memoryusagealarm.Handle {
	return do.memoryUsageAlarmHandle
}

// ServerMemoryLimitHandle returns the expensive query handle.
func (do *Domain) ServerMemoryLimitHandle() *servermemorylimit.Handle {
	return do.serverMemoryLimitHandle
}

const (
	privilegeKey          = "/tidb/privilege"
	sysVarCacheKey        = "/tidb/sysvars"
	tiflashComputeNodeKey = "/tiflash/new_tiflash_compute_nodes"
)

// PrivilegeEvent is the message definition for NotifyUpdatePrivilege(), encoded in json.
// TiDB old version do not use no such message.
type PrivilegeEvent struct {
	All      bool
	ServerID uint64
	UserList []string
}

// NotifyUpdateAllUsersPrivilege updates privilege key in etcd, TiDB client that watches
// the key will get notification.
func (do *Domain) NotifyUpdateAllUsersPrivilege() error {
	return do.notifyUpdatePrivilege(PrivilegeEvent{All: true})
}

// NotifyUpdatePrivilege updates privilege key in etcd, TiDB client that watches
// the key will get notification.
func (do *Domain) NotifyUpdatePrivilege(userList []string) error {
	return do.notifyUpdatePrivilege(PrivilegeEvent{UserList: userList})
}

func (do *Domain) notifyUpdatePrivilege(event PrivilegeEvent) error {
	// No matter skip-grant-table is configured or not, sending an etcd message is required.
	// Because we need to tell other TiDB instances to update privilege data, say, we're changing the
	// password using a special TiDB instance and want the new password to take effect.
	if do.etcdClient != nil {
		event.ServerID = do.serverID
		data, err := json.Marshal(event)
		if err != nil {
			return errors.Trace(err)
		}
		if uint64(len(data)) > size.MB {
			logutil.BgLogger().Warn("notify update privilege message too large", zap.ByteString("value", data))
		}
		err = ddlutil.PutKVToEtcd(do.ctx, do.etcdClient, etcd.KeyOpDefaultRetryCnt, privilegeKey, string(data))
		if err != nil {
			logutil.BgLogger().Warn("notify update privilege failed", zap.Error(err))
		}
	}

	// If skip-grant-table is configured, do not flush privileges.
	// Because LoadPrivilegeLoop does not run and the privilege Handle is nil,
	// the call to do.PrivilegeHandle().Update would panic.
	if config.GetGlobalConfig().Security.SkipGrantTable {
		return nil
	}

	return privReloadEvent(do.PrivilegeHandle(), &event)
}

// NotifyUpdateSysVarCache updates the sysvar cache key in etcd, which other TiDB
// clients are subscribed to for updates. For the caller, the cache is also built
// synchronously so that the effect is immediate.
func (do *Domain) NotifyUpdateSysVarCache(updateLocal bool) {
	if do.etcdClient != nil {
		err := ddlutil.PutKVToEtcd(context.Background(), do.etcdClient, etcd.KeyOpDefaultRetryCnt, sysVarCacheKey, "")
		if err != nil {
			logutil.BgLogger().Warn("notify update sysvar cache failed", zap.Error(err))
		}
	}
	// update locally
	if updateLocal {
		if err := do.rebuildSysVarCache(nil); err != nil {
			logutil.BgLogger().Error("rebuilding sysvar cache failed", zap.Error(err))
		}
	}
}

// LoadSigningCertLoop loads the signing cert periodically to make sure it's fresh new.
func (do *Domain) LoadSigningCertLoop(signingCert, signingKey string) {
	sessionstates.SetCertPath(signingCert)
	sessionstates.SetKeyPath(signingKey)

	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Debug("loadSigningCertLoop exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "LoadSigningCertLoop", nil, false)

		for {
			select {
			case <-time.After(sessionstates.LoadCertInterval):
				sessionstates.ReloadSigningCert()
			case <-do.exit:
				return
			}
		}
	}, "loadSigningCertLoop")
}

// StartTTLJobManager creates and starts the ttl job manager
func (do *Domain) StartTTLJobManager() {
	ttlJobManager := ttlworker.NewJobManager(do.ddl.GetID(), do.advancedSysSessionPool, do.store, do.etcdClient, do.ddl.OwnerManager().IsOwner)
	do.ttlJobManager.Store(ttlJobManager)
	ttlJobManager.Start()
}

// TTLJobManager returns the ttl job manager on this domain
func (do *Domain) TTLJobManager() *ttlworker.JobManager {
	return do.ttlJobManager.Load()
}

// StopAutoAnalyze stops (*Domain).autoAnalyzeWorker to launch new auto analyze jobs.
func (do *Domain) StopAutoAnalyze() {
	do.stopAutoAnalyze.Store(true)
}

// InitInstancePlanCache initializes the instance level plan cache for this Domain.
func (do *Domain) InitInstancePlanCache() {
	hardLimit := vardef.InstancePlanCacheMaxMemSize.Load()
	softLimit := float64(hardLimit) * (1 - vardef.InstancePlanCacheReservedPercentage.Load())
	do.instancePlanCache = NewInstancePlanCache(int64(softLimit), hardLimit)
	// use a separate goroutine to avoid the eviction blocking other operations.
	do.wg.Run(do.planCacheEvictTrigger, "planCacheEvictTrigger")
	do.wg.Run(do.planCacheMetricsAndVars, "planCacheMetricsAndVars")
}

// GetInstancePlanCache returns the instance level plan cache in this Domain.
func (do *Domain) GetInstancePlanCache() sessionctx.InstancePlanCache {
	return do.instancePlanCache
}

// planCacheMetricsAndVars updates metrics and variables for Instance Plan Cache periodically.
func (do *Domain) planCacheMetricsAndVars() {
	defer util.Recover(metrics.LabelDomain, "planCacheMetricsAndVars", nil, false)
	ticker := time.NewTicker(time.Second * 15) // 15s by default
	defer func() {
		ticker.Stop()
		logutil.BgLogger().Info("planCacheMetricsAndVars exited.")
	}()

	for {
		select {
		case <-ticker.C:
			// update limits
			hardLimit := vardef.InstancePlanCacheMaxMemSize.Load()
			softLimit := int64(float64(hardLimit) * (1 - vardef.InstancePlanCacheReservedPercentage.Load()))
			curSoft, curHard := do.instancePlanCache.GetLimits()
			if curSoft != softLimit || curHard != hardLimit {
				do.instancePlanCache.SetLimits(softLimit, hardLimit)
			}

			// update the metrics
			size := do.instancePlanCache.Size()
			memUsage := do.instancePlanCache.MemUsage()
			metrics2.GetPlanCacheInstanceNumCounter(true).Set(float64(size))
			metrics2.GetPlanCacheInstanceMemoryUsage(true).Set(float64(memUsage))
		case <-do.exit:
			return
		}
	}
}

// planCacheEvictTrigger triggers the plan cache eviction periodically.
func (do *Domain) planCacheEvictTrigger() {
	defer util.Recover(metrics.LabelDomain, "planCacheEvictTrigger", nil, false)
	ticker := time.NewTicker(time.Second * 30) // 30s by default
	defer func() {
		ticker.Stop()
		logutil.BgLogger().Info("planCacheEvictTrigger exited.")
	}()

	for {
		select {
		case <-ticker.C:
			// trigger the eviction
			begin := time.Now()
			enabled := vardef.EnableInstancePlanCache.Load()
			detailInfo, numEvicted := do.instancePlanCache.Evict(!enabled) // evict all if the plan cache is disabled
			metrics2.GetPlanCacheInstanceEvict().Set(float64(numEvicted))
			if numEvicted > 0 {
				logutil.BgLogger().Info("instance plan eviction",
					zap.String("detail", detailInfo),
					zap.Int64("num_evicted", int64(numEvicted)),
					zap.Duration("time_spent", time.Since(begin)))
			}
		case <-do.exit:
			return
		}
	}
}

// SetupWorkloadBasedLearningWorker sets up all of the workload based learning workers.
func (do *Domain) SetupWorkloadBasedLearningWorker() {
	wbLearningHandle := workloadlearning.NewWorkloadLearningHandle(do.sysSessionPool)
	wbCacheWorker := workloadlearning.NewWLCacheWorker(do.sysSessionPool)
	// Start the workload based learning worker to analyze the read workload by statement_summary.
	do.wg.Run(
		func() {
			do.readTableCostWorker(wbLearningHandle, wbCacheWorker)
		},
		"readTableCostWorker",
	)
	// TODO: Add more workers for other workload based learning tasks.
}

// readTableCostWorker is a background worker that periodically analyze the read path table cost by statement_summary.
func (do *Domain) readTableCostWorker(wbLearningHandle *workloadlearning.Handle, wbCacheWorker *workloadlearning.WLCacheWorker) {
	// Recover the panic and log the error when worker exit.
	defer util.Recover(metrics.LabelDomain, "readTableCostWorker", nil, false)
	readTableCostTicker := time.NewTicker(vardef.WorkloadBasedLearningInterval.Load())
	defer func() {
		readTableCostTicker.Stop()
		logutil.BgLogger().Info("readTableCostWorker exited.")
	}()
	for {
		select {
		case <-readTableCostTicker.C:
			if vardef.EnableWorkloadBasedLearning.Load() && do.statsOwner.IsOwner() {
				wbLearningHandle.HandleTableReadCost(do.InfoSchema())
				wbCacheWorker.UpdateTableReadCostCache()
			}
		case <-do.exit:
			return
		}
	}
}

func init() {
	initByLDFlagsForGlobalKill()
	telemetry.GetDomainInfoSchema = func(ctx sessionctx.Context) infoschema.InfoSchema {
		return GetDomain(ctx).InfoSchema()
	}
}

var (
	// ErrInfoSchemaExpired returns the error that information schema is out of date.
	ErrInfoSchemaExpired = dbterror.ClassDomain.NewStd(errno.ErrInfoSchemaExpired)
	// ErrInfoSchemaChanged returns the error that information schema is changed.
	ErrInfoSchemaChanged = dbterror.ClassDomain.NewStdErr(errno.ErrInfoSchemaChanged,
		mysql.Message(errno.MySQLErrName[errno.ErrInfoSchemaChanged].Raw+". "+kv.TxnRetryableMark, nil))
)

// SysProcesses holds the sys processes infos
type SysProcesses struct {
	mu      *sync.RWMutex
	procMap map[uint64]sysproctrack.TrackProc
}

// Track tracks the sys process into procMap
func (s *SysProcesses) Track(id uint64, proc sysproctrack.TrackProc) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if oldProc, ok := s.procMap[id]; ok && oldProc != proc {
		return errors.Errorf("The ID is in use: %v", id)
	}
	s.procMap[id] = proc
	proc.GetSessionVars().ConnectionID = id
	proc.GetSessionVars().SQLKiller.Reset()
	return nil
}

// UnTrack removes the sys process from procMap
func (s *SysProcesses) UnTrack(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if proc, ok := s.procMap[id]; ok {
		delete(s.procMap, id)
		proc.GetSessionVars().ConnectionID = 0
		proc.GetSessionVars().SQLKiller.Reset()
	}
}

// GetSysProcessList gets list of system ProcessInfo
func (s *SysProcesses) GetSysProcessList() map[uint64]*sessmgr.ProcessInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rs := make(map[uint64]*sessmgr.ProcessInfo)
	for connID, proc := range s.procMap {
		// if session is still tracked in this map, it's not returned to sysSessionPool yet
		if pi := proc.ShowProcess(); pi != nil && pi.ID == connID {
			rs[connID] = pi
		}
	}
	return rs
}

// KillSysProcess kills sys process with specified ID
func (s *SysProcesses) KillSysProcess(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if proc, ok := s.procMap[id]; ok {
		proc.GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)
	}
}
