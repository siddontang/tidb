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
	"math"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/executor/join/joinversion"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/pingcap/tidb/pkg/sessionctx/slowlogrule"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pingcap/tidb/pkg/util/tiflashcompute"
	tikvstore "github.com/tikv/client-go/v2/kv"
	atomic2 "go.uber.org/atomic"
)

// PartitionPruneMode presents the prune mode used.
type PartitionPruneMode string

const (
	// Static indicates only prune at plan phase.
	Static PartitionPruneMode = "static"
	// Dynamic indicates only prune at execute phase.
	Dynamic PartitionPruneMode = "dynamic"

	// Don't use out-of-date mode.

	// StaticOnly is out-of-date.
	StaticOnly PartitionPruneMode = "static-only"
	// DynamicOnly is out-of-date.
	DynamicOnly PartitionPruneMode = "dynamic-only"
	// StaticButPrepareDynamic is out-of-date.
	StaticButPrepareDynamic PartitionPruneMode = "static-collect-dynamic"
)

// Valid indicate PruneMode is validated.
func (p PartitionPruneMode) Valid() bool {
	switch p {
	case Static, Dynamic, StaticOnly, DynamicOnly:
		return true
	default:
		return false
	}
}

// Update updates out-of-date PruneMode.
func (p PartitionPruneMode) Update() PartitionPruneMode {
	switch p {
	case StaticOnly, StaticButPrepareDynamic:
		return Static
	case DynamicOnly:
		return Dynamic
	default:
		return p
	}
}

// PlanCacheParamList stores the parameters for plan cache.
// Use attached methods to access or modify parameter values instead of accessing them directly.
type PlanCacheParamList struct {
	paramValues     []types.Datum
	forNonPrepCache bool
}

// NewPlanCacheParamList creates a new PlanCacheParams.
func NewPlanCacheParamList() *PlanCacheParamList {
	p := &PlanCacheParamList{paramValues: make([]types.Datum, 0, 8)}
	p.Reset()
	return p
}

// Reset resets the PlanCacheParams.
func (p *PlanCacheParamList) Reset() {
	p.paramValues = p.paramValues[:0]
	p.forNonPrepCache = false
}

// String implements the fmt.Stringer interface.
func (p *PlanCacheParamList) String() string {
	if p == nil || len(p.paramValues) == 0 ||
		p.forNonPrepCache { // hide non-prep parameter values by default
		return ""
	}
	return " [arguments: " + types.DatumsToStrNoErrSmart(p.paramValues) + "]"
}

// Append appends a parameter value to the PlanCacheParams.
func (p *PlanCacheParamList) Append(vs ...types.Datum) {
	p.paramValues = append(p.paramValues, vs...)
}

// SetForNonPrepCache sets the flag forNonPrepCache.
func (p *PlanCacheParamList) SetForNonPrepCache(flag bool) {
	p.forNonPrepCache = flag
}

// GetParamValue returns the value of the parameter at the specified index.
func (p *PlanCacheParamList) GetParamValue(idx int) types.Datum {
	return p.paramValues[idx]
}

// AllParamValues returns all parameter values.
func (p *PlanCacheParamList) AllParamValues() []types.Datum {
	return p.paramValues
}

// LazyStmtText represents the sql text of a stmt that used in log. It's lazily evaluated to reduce the mem allocs.
type LazyStmtText struct {
	text   *string
	SQL    string
	Redact string
	Params PlanCacheParamList
	Format func(string) string
}

// SetText sets the text directly.
func (s *LazyStmtText) SetText(text string) {
	s.text = &text
}

// Update resets the lazy text and leads to re-eval for next `s.String()`. It copies params so it's safe to use
// `SessionVars.PlanCacheParams` directly without worrying about the params get reset later.
func (s *LazyStmtText) Update(redact string, sql string, params *PlanCacheParamList) {
	s.text = nil
	s.SQL = sql
	s.Redact = redact
	s.Params.Reset()
	if params != nil {
		s.Params.forNonPrepCache = params.forNonPrepCache
		s.Params.paramValues = append(s.Params.paramValues, params.paramValues...)
	}
}

// String implements fmt.Stringer.
func (s *LazyStmtText) String() string {
	if s == nil {
		return ""
	}
	if s.text == nil {
		text := redact.String(s.Redact, s.SQL+s.Params.String())
		if s.Format != nil {
			text = s.Format(text)
		}
		s.text = &text
	}
	return *s.text
}

// ConnectionInfo presents the connection information, which is mainly used by audit logs.
type ConnectionInfo struct {
	ConnectionID      uint64
	ConnectionType    string
	Host              string
	ClientIP          string
	ClientPort        string
	ServerID          int
	ServerIP          string
	ServerPort        int
	Duration          float64
	User              string
	ServerOSLoginUser string
	OSVersion         string
	ClientVersion     string
	ServerVersion     string
	SSLVersion        string
	PID               int
	DB                string
	AuthMethod        string
	Attributes        map[string]string
}

const (
	// ConnTypeSocket indicates socket without TLS.
	ConnTypeSocket string = "TCP"
	// ConnTypeUnixSocket indicates Unix Socket.
	ConnTypeUnixSocket string = "UnixSocket"
	// ConnTypeTLS indicates socket with TLS.
	ConnTypeTLS string = "SSL/TLS"
)

// IsSecureTransport checks whether the connection is secure.
func (connInfo *ConnectionInfo) IsSecureTransport() bool {
	switch connInfo.ConnectionType {
	case ConnTypeUnixSocket, ConnTypeTLS:
		return true
	}
	return false
}

// NewSessionVars creates a session vars object.
func NewSessionVars(hctx HookContext) *SessionVars {
	vars := &SessionVars{
		UserVars:                      NewUserVars(),
		systems:                       make(map[string]string),
		PreparedStmts:                 make(map[uint32]any),
		PreparedStmtNameToID:          make(map[string]uint32),
		PlanCacheParams:               NewPlanCacheParamList(),
		TxnCtx:                        &TransactionContext{},
		RetryInfo:                     &RetryInfo{},
		ActiveRoles:                   make([]*auth.RoleIdentity, 0, 10),
		AutoIncrementIncrement:        vardef.DefAutoIncrementIncrement,
		AutoIncrementOffset:           vardef.DefAutoIncrementOffset,
		StmtCtx:                       stmtctx.NewStmtCtx(),
		AllowAggPushDown:              false,
		AllowCartesianBCJ:             vardef.DefOptCartesianBCJ,
		MPPOuterJoinFixedBuildSide:    vardef.DefOptMPPOuterJoinFixedBuildSide,
		BroadcastJoinThresholdSize:    vardef.DefBroadcastJoinThresholdSize,
		BroadcastJoinThresholdCount:   vardef.DefBroadcastJoinThresholdCount,
		OptimizerSelectivityLevel:     vardef.DefTiDBOptimizerSelectivityLevel,
		OptIndexPruneThreshold:        vardef.DefTiDBOptIndexPruneThreshold,
		RiskScaleNDVSkewRatio:         vardef.DefOptRiskScaleNDVSkewRatio,
		RiskGroupNDVSkewRatio:         vardef.DefOptRiskGroupNDVSkewRatio,
		AlwaysKeepJoinKey:             vardef.DefOptAlwaysKeepJoinKey,
		CartesianJoinOrderThreshold:   vardef.DefOptCartesianJoinOrderThreshold,
		EnableOuterJoinReorder:        vardef.DefTiDBEnableOuterJoinReorder,
		EnableNoDecorrelateInSelect:   vardef.DefOptEnableNoDecorrelateInSelect,
		EnableSemiJoinRewrite:         vardef.DefOptEnableSemiJoinRewrite,
		RetryLimit:                    vardef.DefTiDBRetryLimit,
		DisableTxnAutoRetry:           vardef.DefTiDBDisableTxnAutoRetry,
		DDLReorgPriority:              kv.PriorityLow,
		allowInSubqToJoinAndAgg:       vardef.DefOptInSubqToJoinAndAgg,
		preferRangeScan:               vardef.DefOptPreferRangeScan,
		EnableCorrelationAdjustment:   vardef.DefOptEnableCorrelationAdjustment,
		LimitPushDownThreshold:        vardef.DefOptLimitPushDownThreshold,
		CorrelationThreshold:          vardef.DefOptCorrelationThreshold,
		CorrelationExpFactor:          vardef.DefOptCorrelationExpFactor,
		RiskEqSkewRatio:               vardef.DefOptRiskEqSkewRatio,
		RiskRangeSkewRatio:            vardef.DefOptRiskRangeSkewRatio,
		cpuFactor:                     vardef.DefOptCPUFactor,
		copCPUFactor:                  vardef.DefOptCopCPUFactor,
		CopTiFlashConcurrencyFactor:   vardef.DefOptTiFlashConcurrencyFactor,
		networkFactor:                 vardef.DefOptNetworkFactor,
		scanFactor:                    vardef.DefOptScanFactor,
		descScanFactor:                vardef.DefOptDescScanFactor,
		seekFactor:                    vardef.DefOptSeekFactor,
		memoryFactor:                  vardef.DefOptMemoryFactor,
		diskFactor:                    vardef.DefOptDiskFactor,
		concurrencyFactor:             vardef.DefOptConcurrencyFactor,
		IndexScanCostFactor:           vardef.DefOptIndexScanCostFactor,
		IndexReaderCostFactor:         vardef.DefOptIndexReaderCostFactor,
		TableReaderCostFactor:         vardef.DefOptTableReaderCostFactor,
		TableFullScanCostFactor:       vardef.DefOptTableFullScanCostFactor,
		TableRangeScanCostFactor:      vardef.DefOptTableRangeScanCostFactor,
		TableRowIDScanCostFactor:      vardef.DefOptTableRowIDScanCostFactor,
		TableTiFlashScanCostFactor:    vardef.DefOptTableTiFlashScanCostFactor,
		IndexLookupCostFactor:         vardef.DefOptIndexLookupCostFactor,
		IndexMergeCostFactor:          vardef.DefOptIndexMergeCostFactor,
		SortCostFactor:                vardef.DefOptSortCostFactor,
		TopNCostFactor:                vardef.DefOptTopNCostFactor,
		LimitCostFactor:               vardef.DefOptLimitCostFactor,
		StreamAggCostFactor:           vardef.DefOptStreamAggCostFactor,
		HashAggCostFactor:             vardef.DefOptHashAggCostFactor,
		MergeJoinCostFactor:           vardef.DefOptMergeJoinCostFactor,
		HashJoinCostFactor:            vardef.DefOptHashJoinCostFactor,
		IndexJoinCostFactor:           vardef.DefOptIndexJoinCostFactor,
		SelectivityFactor:             vardef.DefOptSelectivityFactor,
		enableForceInlineCTE:          vardef.DefOptForceInlineCTE,
		EnableVectorizedExpression:    vardef.DefEnableVectorizedExpression,
		CommandValue:                  uint32(mysql.ComSleep),
		TiDBOptJoinReorderThreshold:   vardef.DefTiDBOptJoinReorderThreshold,
		TiDBOptJoinReorderThroughSel:  vardef.DefTiDBOptJoinReorderThroughSel,
		SlowQueryFile:                 config.GetGlobalConfig().Log.SlowQueryFile,
		WaitSplitRegionFinish:         vardef.DefTiDBWaitSplitRegionFinish,
		WaitSplitRegionTimeout:        vardef.DefWaitSplitRegionTimeout,
		enableIndexMerge:              vardef.DefTiDBEnableIndexMerge,
		NoopFuncsMode:                 TiDBOptOnOffWarn(vardef.DefTiDBEnableNoopFuncs),
		replicaRead:                   kv.ReplicaReadLeader,
		AllowRemoveAutoInc:            vardef.DefTiDBAllowRemoveAutoInc,
		UsePlanBaselines:              vardef.DefTiDBUsePlanBaselines,
		EvolvePlanBaselines:           vardef.DefTiDBEvolvePlanBaselines,
		EnableExtendedStats:           false,
		IsolationReadEngines:          make(map[kv.StoreType]struct{}),
		LockWaitTimeout:               vardef.DefInnodbLockWaitTimeout * 1000,
		MetricSchemaStep:              vardef.DefTiDBMetricSchemaStep,
		MetricSchemaRangeDuration:     vardef.DefTiDBMetricSchemaRangeDuration,
		SequenceState:                 NewSequenceState(),
		WindowingUseHighPrecision:     true,
		PrevFoundInPlanCache:          vardef.DefTiDBFoundInPlanCache,
		FoundInPlanCache:              vardef.DefTiDBFoundInPlanCache,
		PrevFoundInBinding:            vardef.DefTiDBFoundInBinding,
		FoundInBinding:                vardef.DefTiDBFoundInBinding,
		SelectLimit:                   math.MaxUint64,
		AllowAutoRandExplicitInsert:   vardef.DefTiDBAllowAutoRandExplicitInsert,
		EnableClusteredIndex:          vardef.DefTiDBEnableClusteredIndex,
		EnableParallelApply:           vardef.DefTiDBEnableParallelApply,
		ShardAllocateStep:             vardef.DefTiDBShardAllocateStep,
		EnablePointGetCache:           vardef.DefTiDBPointGetCache,
		PartitionPruneMode:            *atomic2.NewString(vardef.DefTiDBPartitionPruneMode),
		TxnScope:                      kv.NewDefaultTxnScopeVar(),
		EnabledRateLimitAction:        vardef.DefTiDBEnableRateLimitAction,
		EnableAsyncCommit:             vardef.DefTiDBEnableAsyncCommit,
		Enable1PC:                     vardef.DefTiDBEnable1PC,
		GuaranteeLinearizability:      vardef.DefTiDBGuaranteeLinearizability,
		AnalyzeVersion:                vardef.DefTiDBAnalyzeVersion,
		EnableIndexMergeJoin:          vardef.DefTiDBEnableIndexMergeJoin,
		AllowFallbackToTiKV:           make(map[kv.StoreType]struct{}),
		CTEMaxRecursionDepth:          vardef.DefCTEMaxRecursionDepth,
		TMPTableSize:                  vardef.DefTiDBTmpTableMaxSize,
		MPPStoreFailTTL:               vardef.DefTiDBMPPStoreFailTTL,
		Rng:                           mathutil.NewWithTime(),
		EnableLegacyInstanceScope:     vardef.DefEnableLegacyInstanceScope,
		RemoveOrderbyInSubquery:       vardef.DefTiDBRemoveOrderbyInSubquery,
		EnableSkewDistinctAgg:         vardef.DefTiDBSkewDistinctAgg,
		Enable3StageDistinctAgg:       vardef.DefTiDB3StageDistinctAgg,
		MaxAllowedPacket:              vardef.DefMaxAllowedPacket,
		TiFlashFastScan:               vardef.DefTiFlashFastScan,
		EnableTiFlashReadForWriteStmt: true,
		ForeignKeyChecks:              vardef.DefTiDBForeignKeyChecks,
		HookContext:                   hctx,
		EnableReuseChunk:              vardef.DefTiDBEnableReusechunk,
		preUseChunkAlloc:              vardef.DefTiDBUseAlloc,
		chunkPool:                     nil,
		mppExchangeCompressionMode:    vardef.DefaultExchangeCompressionMode,
		mppVersion:                    kv.MppVersionUnspecified,
		EnableLateMaterialization:     vardef.DefTiDBOptEnableLateMaterialization,
		TiFlashComputeDispatchPolicy:  tiflashcompute.DispatchPolicyConsistentHash,
		ResourceGroupName:             resourcegroup.DefaultResourceGroupName,
		DefaultCollationForUTF8MB4:    mysql.DefaultCollationName,
		GroupConcatMaxLen:             vardef.DefGroupConcatMaxLen,
		EnableRedactLog:               vardef.DefTiDBRedactLog,
		EnableWindowFunction:          vardef.DefEnableWindowFunction,
		CostModelVersion:              vardef.DefTiDBCostModelVer,
		OptimizerEnableNAAJ:           vardef.DefTiDBEnableNAAJ,
		OptOrderingIdxSelRatio:        vardef.DefTiDBOptOrderingIdxSelRatio,
		RegardNULLAsPoint:             vardef.DefTiDBRegardNULLAsPoint,
		AllowProjectionPushDown:       vardef.DefOptEnableProjectionPushDown,
		SkipMissingPartitionStats:     vardef.DefTiDBSkipMissingPartitionStats,
		IndexLookUpPushDownPolicy:     vardef.DefTiDBIndexLookUpPushDownPolicy,
		OptPartialOrderedIndexForTopN: vardef.DefTiDBOptPartialOrderedIndexForTopN,
	}
	vars.TiFlashFineGrainedShuffleBatchSize = vardef.DefTiFlashFineGrainedShuffleBatchSize
	vars.status.Store(uint32(mysql.ServerStatusAutocommit))
	vars.StmtCtx.ResourceGroupName = resourcegroup.DefaultResourceGroupName
	vars.KVVars = tikvstore.NewVariables(&vars.SQLKiller.Signal)
	vars.Concurrency = Concurrency{
		indexLookupConcurrency:            vardef.DefIndexLookupConcurrency,
		indexLookupJoinConcurrency:        vardef.DefIndexLookupJoinConcurrency,
		hashJoinConcurrency:               vardef.DefTiDBHashJoinConcurrency,
		projectionConcurrency:             vardef.DefTiDBProjectionConcurrency,
		distSQLScanConcurrency:            vardef.DefDistSQLScanConcurrency,
		analyzeDistSQLScanConcurrency:     vardef.DefAnalyzeDistSQLScanConcurrency,
		hashAggPartialConcurrency:         vardef.DefTiDBHashAggPartialConcurrency,
		hashAggFinalConcurrency:           vardef.DefTiDBHashAggFinalConcurrency,
		windowConcurrency:                 vardef.DefTiDBWindowConcurrency,
		mergeJoinConcurrency:              vardef.DefTiDBMergeJoinConcurrency,
		streamAggConcurrency:              vardef.DefTiDBStreamAggConcurrency,
		indexMergeIntersectionConcurrency: vardef.DefTiDBIndexMergeIntersectionConcurrency,
		ExecutorConcurrency:               vardef.DefExecutorConcurrency,
	}
	vars.MemQuota = MemQuota{
		MemQuotaQuery:      vardef.DefTiDBMemQuotaQuery,
		MemQuotaApplyCache: vardef.DefTiDBMemQuotaApplyCache,
	}
	vars.BatchSize = BatchSize{
		IndexJoinBatchSize: vardef.DefIndexJoinBatchSize,
		IndexLookupSize:    vardef.DefIndexLookupSize,
		InitChunkSize:      vardef.DefInitChunkSize,
		MaxChunkSize:       vardef.DefMaxChunkSize,
		MinPagingSize:      vardef.DefMinPagingSize,
		MaxPagingSize:      vardef.DefMaxPagingSize,
	}
	vars.DMLBatchSize = vardef.DefDMLBatchSize
	vars.AllowBatchCop = vardef.DefTiDBAllowBatchCop
	vars.allowMPPExecution = vardef.DefTiDBAllowMPPExecution
	vars.HashExchangeWithNewCollation = vardef.DefTiDBHashExchangeWithNewCollation
	vars.enforceMPPExecution = vardef.DefTiDBEnforceMPPExecution
	vars.TiFlashMaxThreads = vardef.DefTiFlashMaxThreads
	vars.TiFlashMaxBytesBeforeExternalJoin = vardef.DefTiFlashMaxBytesBeforeExternalJoin
	vars.TiFlashMaxBytesBeforeExternalGroupBy = vardef.DefTiFlashMaxBytesBeforeExternalGroupBy
	vars.TiFlashMaxBytesBeforeExternalSort = vardef.DefTiFlashMaxBytesBeforeExternalSort
	vars.TiFlashMaxQueryMemoryPerNode = vardef.DefTiFlashMemQuotaQueryPerNode
	vars.TiFlashQuerySpillRatio = vardef.DefTiFlashQuerySpillRatio
	vars.TiFlashHashJoinVersion = vardef.DefTiFlashHashJoinVersion
	vars.MPPStoreFailTTL = vardef.DefTiDBMPPStoreFailTTL
	vars.DiskTracker = disk.NewTracker(memory.LabelForSession, -1)
	vars.MemTracker = memory.NewTracker(memory.LabelForSession, vars.MemQuotaQuery)
	vars.MemTracker.IsRootTrackerOfSess = true
	vars.MemTracker.Killer = &vars.SQLKiller
	vars.StatsLoadSyncWait.Store(vardef.StatsLoadSyncWait.Load())
	vars.UseHashJoinV2 = joinversion.IsOptimizedVersion(vardef.DefTiDBHashJoinVersion)
	vars.SlowLogRules = slowlogrule.NewSessionSlowLogRules(nil)

	for _, engine := range config.GetGlobalConfig().IsolationRead.Engines {
		switch engine {
		case kv.TiFlash.Name():
			vars.IsolationReadEngines[kv.TiFlash] = struct{}{}
		case kv.TiKV.Name():
			vars.IsolationReadEngines[kv.TiKV] = struct{}{}
		case kv.TiDB.Name():
			vars.IsolationReadEngines[kv.TiDB] = struct{}{}
		}
	}
	if !vardef.EnableLocalTxn.Load() {
		vars.TxnScope = kv.NewGlobalTxnScopeVar()
	}
	if vardef.EnableRowLevelChecksum.Load() {
		vars.EnableRowLevelChecksum = true
	}
	vars.systems[vardef.CharacterSetConnection], vars.systems[vardef.CollationConnection] = charset.GetDefaultCharsetAndCollate()
	return vars
}
