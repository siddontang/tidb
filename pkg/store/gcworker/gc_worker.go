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
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	pdgc "github.com/tikv/pd/client/clients/gc"
	"go.uber.org/zap"
)

// GCWorker periodically triggers GC process on tikv server.
type GCWorker struct {
	uuid                 string
	desc                 string
	keyspaceID           uint32
	store                kv.Storage
	tikvStore            tikv.Storage
	pdClient             pd.Client
	pdGCControllerClient pdgc.InternalController
	gcIsRunning          bool
	lastFinish           time.Time
	cancel               context.CancelFunc
	done                 chan error
	regionLockResolver   tikv.RegionLockResolver
}

// NewGCWorker creates a GCWorker instance.
func NewGCWorker(store kv.Storage, pdClient pd.Client) (*GCWorker, error) {
	ver, err := store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return nil, errors.Trace(err)
	}
	hostName, err := os.Hostname()
	if err != nil {
		hostName = "unknown"
	}
	tikvStore, ok := store.(tikv.Storage)
	if !ok {
		return nil, errors.New("GC should run against TiKV storage")
	}
	uuid := strconv.FormatUint(ver.Ver, 16)
	resolverIdentifier := fmt.Sprintf("gc-worker-%s", uuid)
	keyspaceID := uint32(store.GetCodec().GetKeyspaceID())
	worker := &GCWorker{
		uuid:                 uuid,
		desc:                 fmt.Sprintf("host:%s, pid:%d, start at %s", hostName, os.Getpid(), time.Now()),
		keyspaceID:           keyspaceID,
		store:                store,
		tikvStore:            tikvStore,
		pdClient:             pdClient,
		pdGCControllerClient: pdClient.GetGCInternalController(keyspaceID),
		gcIsRunning:          false,
		lastFinish:           time.Now(),
		regionLockResolver:   tikv.NewRegionLockResolver(resolverIdentifier, tikvStore),
		done:                 make(chan error),
	}
	variable.RegisterStatistics(worker)
	return worker, nil
}

// Start starts the worker.
func (w *GCWorker) Start() {
	var ctx context.Context
	ctx, w.cancel = context.WithCancel(context.Background())
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnGC)
	var wg sync.WaitGroup
	wg.Add(1)
	go w.start(ctx, &wg)
	wg.Wait() // Wait create session finish in worker, some test code depend on this to avoid race.
}

// Close stops background goroutines.
func (w *GCWorker) Close() {
	w.cancel()
}

const (
	booleanTrue  = "true"
	booleanFalse = "false"

	gcWorkerTickInterval = time.Minute
	gcWorkerLease        = time.Minute * 2
	gcLeaderUUIDKey      = "tikv_gc_leader_uuid"
	gcLeaderDescKey      = "tikv_gc_leader_desc"
	gcLeaderLeaseKey     = "tikv_gc_leader_lease"

	gcLastRunTimeKey       = "tikv_gc_last_run_time"
	gcRunIntervalKey       = "tikv_gc_run_interval"
	gcDefaultRunInterval   = time.Minute * 10
	gcWaitTime             = time.Minute * 1
	gcRedoDeleteRangeDelay = 24 * time.Hour

	gcLifeTimeKey        = "tikv_gc_life_time"
	gcDefaultLifeTime    = time.Minute * 10
	gcMinLifeTime        = time.Minute * 10
	gcSafePointKey       = "tikv_gc_safe_point"
	gcConcurrencyKey     = "tikv_gc_concurrency"
	gcDefaultConcurrency = 2
	gcMinConcurrency     = 1
	gcMaxConcurrency     = 128

	gcEnableKey          = "tikv_gc_enable"
	gcDefaultEnableValue = true

	gcModeKey         = "tikv_gc_mode"
	gcModeCentral     = "central"
	gcModeDistributed = "distributed"
	gcModeDefault     = gcModeDistributed

	gcScanLockModeKey = "tikv_gc_scan_lock_mode"

	gcAutoConcurrencyKey     = "tikv_gc_auto_concurrency"
	gcDefaultAutoConcurrency = true

	gcWorkerServiceSafePointID = "gc_worker"

	// Status var names start with tidb_%
	tidbGCLastRunTime = "tidb_gc_last_run_time"
	tidbGCLeaderDesc  = "tidb_gc_leader_desc"
	tidbGCLeaderLease = "tidb_gc_leader_lease"
	tidbGCLeaderUUID  = "tidb_gc_leader_uuid"
	tidbGCSafePoint   = "tidb_gc_safe_point"

	loadAllKeyspacesForUnifiedGCBatchSize = 50
)

var txnSafePointSyncWaitTime = tikv.GcStateCacheInterval

var gcVariableComments = map[string]string{
	gcLeaderUUIDKey:      "Current GC worker leader UUID. (DO NOT EDIT)",
	gcLeaderDescKey:      "Host name and pid of current GC leader. (DO NOT EDIT)",
	gcLeaderLeaseKey:     "Current GC worker leader lease. (DO NOT EDIT)",
	gcLastRunTimeKey:     "The time when last GC starts. (DO NOT EDIT)",
	gcRunIntervalKey:     "GC run interval, at least 10m, in Go format.",
	gcLifeTimeKey:        "All versions within life time will not be collected by GC, at least 10m, in Go format.",
	gcSafePointKey:       "All versions after safe point can be accessed. (DO NOT EDIT)",
	gcConcurrencyKey:     "How many goroutines used to do GC parallel, [1, 128], default 2",
	gcEnableKey:          "Current GC enable status",
	gcModeKey:            "Mode of GC, \"central\" or \"distributed\". (Obsolete and no longer effective, the actual GC procedure is always \"distributed\")",
	gcAutoConcurrencyKey: "Let TiDB pick the concurrency automatically. If set false, tikv_gc_concurrency will be used",
	gcScanLockModeKey:    "Mode of scanning locks, \"physical\" or \"legacy\".(Deprecated)",
}

const (
	unsafeDestroyRangeTimeout = 5 * time.Minute
	gcTimeout                 = 5 * time.Minute
)

func (w *GCWorker) start(ctx context.Context, wg *sync.WaitGroup) {
	logutil.Logger(ctx).Info("start", zap.String("category", "gc worker"),
		zap.String("uuid", w.uuid))

	w.tick(ctx) // Immediately tick once to initialize configs.
	wg.Done()

	ticker := time.NewTicker(gcWorkerTickInterval)
	defer ticker.Stop()
	defer func() {
		r := recover()
		if r != nil {
			logutil.Logger(ctx).Error("gcWorker",
				zap.Any("r", r),
				zap.Stack("stack"))
			metrics.PanicCounter.WithLabelValues(metrics.LabelGCWorker).Inc()
		}
	}()
	for {
		select {
		case <-ticker.C:
			w.tick(ctx)
		case err := <-w.done:
			w.gcIsRunning = false
			w.lastFinish = time.Now()
			if err != nil {
				logutil.Logger(ctx).Error("runGCJob", zap.String("category", "gc worker"), zap.Error(err))
			}
		case <-ctx.Done():
			logutil.Logger(ctx).Info("quit", zap.String("category", "gc worker"), zap.String("uuid", w.uuid))
			return
		}
	}
}

func createSession(store kv.Storage) sessionapi.Session {
	for {
		se, err := session.CreateSession(store)
		if err != nil {
			logutil.BgLogger().Warn("create session", zap.String("category", "gc worker"), zap.Error(err))
			continue
		}
		// Disable privilege check for gc worker session.
		privilege.BindPrivilegeManager(se, nil)
		se.GetSessionVars().CommonGlobalLoaded = true
		se.GetSessionVars().InRestrictedSQL = true
		se.GetSessionVars().SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
		return se
	}
}

// GetScope gets the status variables scope.
func (w *GCWorker) GetScope(status string) vardef.ScopeFlag {
	return variable.DefaultStatusVarScopeFlag
}

// Stats returns the server statistics.
func (w *GCWorker) Stats(vars *variable.SessionVars) (map[string]any, error) {
	m := make(map[string]any)
	if v, err := w.loadValueFromSysTable(gcLeaderUUIDKey); err == nil {
		m[tidbGCLeaderUUID] = v
	}
	if v, err := w.loadValueFromSysTable(gcLeaderDescKey); err == nil {
		m[tidbGCLeaderDesc] = v
	}
	if v, err := w.loadValueFromSysTable(gcLeaderLeaseKey); err == nil {
		m[tidbGCLeaderLease] = v
	}
	if v, err := w.loadValueFromSysTable(gcLastRunTimeKey); err == nil {
		m[tidbGCLastRunTime] = v
	}
	if v, err := w.loadValueFromSysTable(gcSafePointKey); err == nil {
		m[tidbGCSafePoint] = v
	}
	return m, nil
}

func (w *GCWorker) tick(ctx context.Context) {
	isLeader, err := w.checkLeader(ctx)
	if err != nil {
		logutil.Logger(ctx).Warn("check leader", zap.String("category", "gc worker"), zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("check_leader").Inc()
		return
	}
	if isLeader {
		err = w.leaderTick(ctx)
		if err != nil {
			logutil.Logger(ctx).Warn("leader tick", zap.String("category", "gc worker"), zap.Error(err))
		}
	} else {
		// Config metrics should always be updated by leader, set them to 0 when current instance is not leader.
		metrics.GCConfigGauge.WithLabelValues(gcRunIntervalKey).Set(0)
		metrics.GCConfigGauge.WithLabelValues(gcLifeTimeKey).Set(0)
	}
}

// getGCSafePoint returns the current gc safe point.
func getGCSafePoint(ctx context.Context, pdClient pd.Client) (uint64, error) {
	// If there is try to set gc safepoint is 0, the interface will not set gc safepoint to 0,
	// it will return current gc safepoint.
	safePoint, err := pdClient.UpdateGCSafePoint(ctx, 0)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return safePoint, nil
}

func (w *GCWorker) logIsGCSafePointTooEarly(ctx context.Context, safePoint uint64) error {
	now, err := w.getOracleTime()
	if err != nil {
		return errors.Trace(err)
	}

	checkTs := oracle.GoTimeToTS(now.Add(-gcDefaultLifeTime * 2))
	if checkTs > safePoint {
		logutil.Logger(ctx).Info("gc safepoint is too early. "+
			"Maybe there is a bit BR/Lightning/CDC task, "+
			"or a long transaction is running "+
			"or need a tidb without setting keyspace-name to calculate and update gc safe point.",
			zap.String("category", "gc worker"))
	}
	return nil
}

func (w *GCWorker) runKeyspaceDeleteRange(ctx context.Context, concurrency gcConcurrency) error {
	// Get safe point from PD.
	// The GC safe point is updated only after the global GC have done resolveLocks phase globally.
	// So, in the following code, resolveLocks must have been done by the global GC on the ranges to be deleted,
	// so its safe to delete the ranges.
	safePoint, err := getGCSafePoint(ctx, w.pdClient)
	if err != nil {
		logutil.Logger(ctx).Info("get gc safe point error", zap.String("category", "gc worker"), zap.Error(errors.Trace(err)))
		return nil
	}

	if safePoint == 0 {
		logutil.Logger(ctx).Info("skip keyspace delete range, because gc safe point is 0", zap.String("category", "gc worker"))
		return nil
	}

	err = w.logIsGCSafePointTooEarly(ctx, safePoint)
	if err != nil {
		logutil.Logger(ctx).Info("log is gc safe point is too early error", zap.String("category", "gc worker"), zap.Error(errors.Trace(err)))
		return nil
	}

	keyspaceID := w.store.GetCodec().GetKeyspaceID()
	logutil.Logger(ctx).Info("start keyspace delete range", zap.String("category", "gc worker"),
		zap.String("uuid", w.uuid),
		zap.Int("concurrency", concurrency.v),
		zap.Uint32("keyspaceID", uint32(keyspaceID)),
		zap.Uint64("GCSafepoint", safePoint))

	// Do deleteRanges.
	err = w.deleteRanges(ctx, safePoint, concurrency)
	if err != nil {
		logutil.Logger(ctx).Error("delete range returns an error", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("delete_range").Inc()
		return errors.Trace(err)
	}

	// Do redoDeleteRanges.
	err = w.redoDeleteRanges(ctx, safePoint, concurrency)
	if err != nil {
		logutil.Logger(ctx).Error("redo-delete range returns an error", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("redo_delete_range").Inc()
		return errors.Trace(err)
	}

	return nil
}

// leaderTick of GC worker checks if it should start a GC job every tick.
func (w *GCWorker) leaderTick(ctx context.Context) error {
	if w.gcIsRunning {
		logutil.Logger(ctx).Info("there's already a gc job running, skipped", zap.String("category", "gc worker"),
			zap.String("leaderTick on", w.uuid))
		return nil
	}

	concurrency, err := w.getGCConcurrency(ctx)
	if err != nil {
		logutil.Logger(ctx).Info("failed to get gc concurrency.", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Error(err))
		return errors.Trace(err)
	}

	// For different keyspace configurations, there are two different GC procedure for them:
	// * Null keyspace (keyspace not used), or keyspaces with keyspace level GC enabled:
	//   The keyspace should manage the procedure totally by itself.
	// * Keyspaces with keyspace level GC disabled, or to say, using unified GC mode:
	//   The GC procedure only includes polling the GC safe point from the null keyspace
	keyspaceMeta := w.store.GetCodec().GetKeyspaceMeta()
	if keyspaceMeta != nil && !pd.IsKeyspaceUsingKeyspaceLevelGC(keyspaceMeta) {
		err = w.runKeyspaceGCJobInUnifiedGCMode(ctx, concurrency)
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	}

	ok, safePoint, err := w.prepare(ctx)
	if err != nil {
		metrics.GCJobFailureCounter.WithLabelValues("prepare").Inc()
		return errors.Trace(err)
	} else if !ok {
		return nil
	}
	// When the worker is just started, or an old GC job has just finished,
	// wait a while before starting a new job.
	if time.Since(w.lastFinish) < gcWaitTime {
		logutil.Logger(ctx).Info("another gc job has just finished, skipped.", zap.String("category", "gc worker"),
			zap.String("leaderTick on ", w.uuid))
		return nil
	}

	w.gcIsRunning = true
	logutil.Logger(ctx).Info("starts the whole job", zap.String("category", "gc worker"),
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint),
		zap.Int("concurrency", concurrency.v))
	go func() {
		w.done <- w.runGCJob(ctx, safePoint, concurrency)
	}()
	return nil
}

func (w *GCWorker) runKeyspaceGCJobInUnifiedGCMode(ctx context.Context, concurrency gcConcurrency) error {
	// When the worker is just started, or an old GC job has just finished,
	// wait a while before starting a new job.
	if time.Since(w.lastFinish) < gcWaitTime {
		logutil.Logger(ctx).Info("another keyspace gc job has just finished, skipped.", zap.String("category", "gc worker"),
			zap.String("leaderTick on ", w.uuid))
		return nil
	}

	now, err := w.getOracleTime()
	if err != nil {
		return errors.Trace(err)
	}
	ok, err := w.checkGCInterval(now)
	if err != nil || !ok {
		return errors.Trace(err)
	}

	go func() {
		w.done <- w.runKeyspaceDeleteRange(ctx, concurrency)
	}()

	err = w.saveTime(gcLastRunTimeKey, now)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// prepare checks preconditions for starting a GC job. It returns a bool
// that indicates whether the GC job should start and the new safePoint.
func (w *GCWorker) prepare(ctx context.Context) (bool, uint64, error) {
	// Add a transaction here is to prevent following situations:
	// 1. GC check gcEnable is true, continue to do GC
	// 2. The user sets gcEnable to false
	// 3. The user gets `tikv_gc_safe_point` value is t1, then the user thinks the data after time t1 won't be clean by GC.
	// 4. GC update `tikv_gc_safe_point` value to t2, continue do GC in this round.
	// Then the data record that has been dropped between time t1 and t2, will be cleaned by GC, but the user thinks the data after t1 won't be clean by GC.
	se := createSession(w.store)
	defer se.Close()
	_, err := se.ExecuteInternal(ctx, "BEGIN")
	if err != nil {
		return false, 0, errors.Trace(err)
	}
	doGC, safePoint, err := w.checkPrepare(ctx)
	if doGC {
		err = se.CommitTxn(ctx)
		if err != nil {
			return false, 0, errors.Trace(err)
		}
	} else {
		se.RollbackTxn(ctx)
	}
	return doGC, safePoint, errors.Trace(err)
}

func (w *GCWorker) checkPrepare(ctx context.Context) (bool, uint64, error) {
	enable, err := w.checkGCEnable()
	if err != nil {
		return false, 0, errors.Trace(err)
	}

	if !enable {
		logutil.Logger(ctx).Warn("gc status is disabled.", zap.String("category", "gc worker"))
		return false, 0, nil
	}
	now, err := w.getOracleTime()
	if err != nil {
		return false, 0, errors.Trace(err)
	}
	ok, err := w.checkGCInterval(now)
	if err != nil || !ok {
		return false, 0, errors.Trace(err)
	}
	newSafePoint, newSafePointValue, err := w.calcNewTxnSafePoint(ctx, now)
	if err != nil || newSafePoint == nil {
		return false, 0, errors.Trace(err)
	}
	err = w.saveTime(gcLastRunTimeKey, now)
	if err != nil {
		return false, 0, errors.Trace(err)
	}
	err = w.saveTime(gcSafePointKey, *newSafePoint)
	if err != nil {
		return false, 0, errors.Trace(err)
	}
	return true, newSafePointValue, nil
}

func (w *GCWorker) getOracleTime() (time.Time, error) {
	currentVer, err := w.store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return time.Time{}, errors.Trace(err)
	}
	return oracle.GetTimeFromTS(currentVer.Ver), nil
}

func (w *GCWorker) checkGCEnable() (bool, error) {
	return w.loadBooleanWithDefault(gcEnableKey, gcDefaultEnableValue)
}

func (w *GCWorker) checkUseAutoConcurrency() (bool, error) {
	return w.loadBooleanWithDefault(gcAutoConcurrencyKey, gcDefaultAutoConcurrency)
}

func (w *GCWorker) loadBooleanWithDefault(key string, defaultValue bool) (bool, error) {
	str, err := w.loadValueFromSysTable(key)
	if err != nil {
		return false, errors.Trace(err)
	}
	if str == "" {
		// Save default value for gc enable key. The default value is always true.
		defaultValueStr := booleanFalse
		if defaultValue {
			defaultValueStr = booleanTrue
		}
		err = w.saveValueToSysTable(key, defaultValueStr)
		if err != nil {
			return defaultValue, errors.Trace(err)
		}
		return defaultValue, nil
	}
	return strings.EqualFold(str, booleanTrue), nil
}

type gcConcurrency struct {
	v      int
	isAuto bool
}

func (w *GCWorker) getGCConcurrency(ctx context.Context) (gcConcurrency, error) {
	useAutoConcurrency, err := w.checkUseAutoConcurrency()
	if err != nil {
		logutil.Logger(ctx).Error("failed to load config gc_auto_concurrency. use default value.", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Error(err))
		useAutoConcurrency = gcDefaultAutoConcurrency
	}
	if !useAutoConcurrency {
		v, err := w.loadGCConcurrencyWithDefault()
		return gcConcurrency{v, useAutoConcurrency}, err
	}

	stores, err := w.getStoresForGC(ctx)
	concurrency := len(stores)
	if err != nil {
		logutil.Logger(ctx).Error("failed to get up stores to calculate concurrency. use config.", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Error(err))

		concurrency, err = w.loadGCConcurrencyWithDefault()
		if err != nil {
			logutil.Logger(ctx).Error("failed to load gc concurrency from config. use default value.", zap.String("category", "gc worker"),
				zap.String("uuid", w.uuid),
				zap.Error(err))
			concurrency = gcDefaultConcurrency
		}
	}

	if concurrency == 0 {
		logutil.Logger(ctx).Error("no store is up", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid))
		return gcConcurrency{0, useAutoConcurrency}, errors.New("[gc worker] no store is up")
	}

	return gcConcurrency{concurrency, useAutoConcurrency}, nil
}

func (w *GCWorker) checkGCInterval(now time.Time) (bool, error) {
	runInterval, err := w.loadDurationWithDefault(gcRunIntervalKey, gcDefaultRunInterval)
	if err != nil {
		return false, errors.Trace(err)
	}
	metrics.GCConfigGauge.WithLabelValues(gcRunIntervalKey).Set(runInterval.Seconds())
	lastRun, err := w.loadTime(gcLastRunTimeKey)
	if err != nil {
		return false, errors.Trace(err)
	}

	if lastRun != nil && lastRun.Add(*runInterval).After(now) {
		logutil.BgLogger().Debug("skipping garbage collection because gc interval hasn't elapsed since last run", zap.String("category", "gc worker"),
			zap.String("leaderTick on", w.uuid),
			zap.Duration("interval", *runInterval),
			zap.Time("last run", *lastRun))
		return false, nil
	}

	return true, nil
}

// validateGCLifeTime checks whether life time is small than min gc life time.
func (w *GCWorker) validateGCLifeTime(lifeTime time.Duration) (time.Duration, error) {
	if lifeTime >= gcMinLifeTime {
		return lifeTime, nil
	}

	logutil.BgLogger().Info("invalid gc life time", zap.String("category", "gc worker"),
		zap.Duration("get gc life time", lifeTime),
		zap.Duration("min gc life time", gcMinLifeTime))

	err := w.saveDuration(gcLifeTimeKey, gcMinLifeTime)
	return gcMinLifeTime, err
}

func (w *GCWorker) calcNewTxnSafePoint(ctx context.Context, now time.Time) (*time.Time, uint64, error) {
	lifeTime, err := w.loadDurationWithDefault(gcLifeTimeKey, gcDefaultLifeTime)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	*lifeTime, err = w.validateGCLifeTime(*lifeTime)
	if err != nil {
		return nil, 0, err
	}
	metrics.GCConfigGauge.WithLabelValues(gcLifeTimeKey).Set(lifeTime.Seconds())

	// The target value we try to advance the txn safe point.
	target := oracle.GoTimeToTS(now.Add(-*lifeTime))

	newTxnSafePoint, err := w.advanceTxnSafePoint(ctx, target)
	if err != nil {
		// Temporary solution. This code should be refactored when errors returned by PD client can be typed.
		if strings.Contains(err.Error(), "PD:gc:ErrDecreasingTxnSafePoint") {
			logutil.BgLogger().Info("set gc worker service safe point is causing decreasing, the GC will be skipped", zap.Error(err))
			return nil, 0, nil
		}
		return nil, 0, errors.Trace(err)
	}
	if newTxnSafePoint == 0 {
		return nil, 0, nil
	}

	// safe point is recorded in time.Time format which strips the logical part of the timestamp.
	// To prevent the GC worker from keeping working due to the loss of logical part when the
	// safe point isn't changed, we should compare them in time.Time format.
	txnSafePointTime := oracle.GetTimeFromTS(newTxnSafePoint)
	// We should never decrease safePoint.
	return &txnSafePointTime, newTxnSafePoint, nil
}

func (w *GCWorker) advanceTxnSafePoint(ctx context.Context, target uint64) (newTxnSafePoint uint64, err error) {
	result, err := w.pdGCControllerClient.AdvanceTxnSafePoint(ctx, target)
	if err != nil {
		return 0, errors.Trace(err)
	}

	if result.NewTxnSafePoint <= result.OldTxnSafePoint {
		logutil.BgLogger().Info("txn safe point not advanced, GC will be skipped. this may be caused by GC being blocked, or user enlarged GC life time.",
			zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Uint64("target", target),
			zap.Uint64("oldTxnSafePoint", result.OldTxnSafePoint),
			zap.Uint64("newTxnSafePoint", result.NewTxnSafePoint),
			zap.String("blockerDesc", result.BlockerDescription))
		return 0, nil
	} else if result.NewTxnSafePoint != target {
		logutil.BgLogger().Info("txn safe point not advanced to the expected value",
			zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Uint64("target", target),
			zap.Uint64("oldTxnSafePoint", result.OldTxnSafePoint),
			zap.Uint64("newTxnSafePoint", result.NewTxnSafePoint),
			zap.String("blockerDesc", result.BlockerDescription))
	}

	return result.NewTxnSafePoint, nil
}

// setGCWorkerServiceSafePoint sets the given safePoint as TiDB's service safePoint to PD, and returns the current minimal
// service safePoint among all services.
func (w *GCWorker) setGCWorkerServiceSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	// Sets TTL to MAX to make it permanently valid.
	minSafePoint, err := w.pdClient.UpdateServiceGCSafePoint(ctx, gcWorkerServiceSafePointID, math.MaxInt64, safePoint)
	if err != nil {
		logutil.Logger(ctx).Error("failed to update service safe point", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("update_service_safe_point").Inc()
		return 0, errors.Trace(err)
	}
	if minSafePoint < safePoint {
		logutil.Logger(ctx).Info("there's another service in the cluster requires an earlier safe point. "+
			"gc will continue with the earlier one",
			zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Uint64("ourSafePoint", safePoint),
			zap.Uint64("minSafePoint", minSafePoint),
		)
		safePoint = minSafePoint
	}
	return safePoint, nil
}

func (w *GCWorker) runGCJob(ctx context.Context, safePoint uint64, concurrency gcConcurrency) error {
	// During GC, the parameter `safePoint` will be used as the new txn safe point and the new GC safe point
	// respectively. Note that here the term "safe point" is divided into two different sub-concepts:
	// * Txn safe point / transaction safe point: All transactions whose start_ts is greater than or equal to the txn
	//   safe point should be guaranteed to be safe to proceed; otherwise it won't be guaranteed.
	// * GC safe point: When GC clean data, all snapshots whose ts is greater than or equal to the GC safe point should
	//   be guaranteed to be consistent; older snapshots can be safely discarded by GC.
	// For details of the terms and concepts, refer to:
	// https://github.com/tikv/pd/blob/53805884a0162f4186d1a933eb28479a269c7d2c/pkg/gc/gc_state_manager.go#L39

	startTime := time.Now()

	failpoint.Inject("mockRunGCJobFail", func() {
		failpoint.Return(errors.New("mock failure of runGCJoB"))
	})
	metrics.GCWorkerCounter.WithLabelValues("run_job").Inc()

	// ----------*--------------------*--------------------> time
	//                                ^ Txn safe point (not synced)
	//           ^ GC safe point (last value)

	// The txn safe point should be guaranteed to be synchronized over all necessary components after
	// `txnSafePointSyncWaitTime`. We wait for that time before performing any operation about the GC.
	time.Sleep(txnSafePointSyncWaitTime)

	// ----------*--------------------*--------------------> time
	//                                ^ Txn safe point (synced)
	//           ^ GC safe point (last value)

	// Resolve locks: make all transactions that started before the txn safe point to be determined, as a step to
	// prevent those transactions from running.
	// safePoint used as the txn safe point conceptually.
	txnSafePoint := safePoint
	err := w.resolveLocks(ctx, txnSafePoint, concurrency.v)
	if err != nil {
		logutil.Logger(ctx).Warn("resolve locks returns an error", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Uint64("txnSafePoint", txnSafePoint),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("resolve_lock").Inc()
		return errors.Trace(err)
	}

	// After both synchronizing the txn safe point and resolving locks, next all component should guarantee that no more
	// transaction started before the txn safe point should proceed.
	// The following steps are all about clearing data, and the safePoint is used as the GC safe point conceptually.
	gcSafePoint := safePoint

	// ----------*--------------------*--------------------> time
	//                                ^ Txn safe point (synced)
	//                                ^ GC safe point (updated)

	// Delete ranges: delete those data that are continuous in range caused by dropping/truncating tables or indices.

	err = w.deleteRanges(ctx, gcSafePoint, concurrency)
	if err != nil {
		logutil.Logger(ctx).Error("delete range returns an error", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Uint64("gcSafePoint", gcSafePoint),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("delete_range").Inc()
		return errors.Trace(err)
	}
	err = w.redoDeleteRanges(ctx, gcSafePoint, concurrency)
	if err != nil {
		logutil.Logger(ctx).Error("redo-delete range returns an error", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Uint64("gcSafePoint", gcSafePoint),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("redo_delete_range").Inc()
		return errors.Trace(err)
	}

	// Next, broadcast the GC safe point to acknowledge TiKV and TiFlash (and possibly other storage types in the
	// future) that the snapshots before the GC safe point can be safely dropped.

	// ----------*--------------------*--------------------> time
	//                                ^ Txn safe point (synced)
	//                                ^ GC safe point (broadcasted)

	err = w.broadcastGCSafePoint(ctx, gcSafePoint)
	if err != nil {
		logutil.Logger(ctx).Error("failed to upload safe point to PD", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("upload_safe_point").Inc()
		return errors.Trace(err)
	}

	metrics.GCHistogram.WithLabelValues(metrics.StageTotal).Observe(time.Since(startTime).Seconds())
	return nil
}

// deleteRanges processes all delete range records whose ts < safePoint in table `gc_delete_range`
// `concurrency` specifies the concurrency to send NotifyDeleteRange.
