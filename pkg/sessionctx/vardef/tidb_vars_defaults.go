// Copyright 2025 PingCAP, Inc.
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

package vardef

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	goatomic "sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/executor/join/joinversion"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/slowlogrule"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/paging"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"
)

// Default TiDB system variable values.
const (
	DefHostname                             = "localhost"
	DefIndexLookupConcurrency               = ConcurrencyUnset
	DefIndexLookupJoinConcurrency           = ConcurrencyUnset
	DefIndexSerialScanConcurrency           = 1
	DefIndexJoinBatchSize                   = 25000
	DefIndexLookupSize                      = 20000
	DefDistSQLScanConcurrency               = 15
	DefAnalyzeDistSQLScanConcurrency        = 4
	DefBuildStatsConcurrency                = 2
	DefBuildSamplingStatsConcurrency        = 2
	DefAutoAnalyzeRatio                     = 0.5
	DefAutoAnalyzeStartTime                 = "00:00 +0000"
	DefAutoAnalyzeEndTime                   = "23:59 +0000"
	DefAutoIncrementIncrement               = 1
	DefAutoIncrementOffset                  = 1
	DefChecksumTableConcurrency             = 4
	DefSkipUTF8Check                        = false
	DefSkipASCIICheck                       = false
	DefOptAggPushDown                       = false
	DefOptDeriveTopN                        = false
	DefOptCartesianBCJ                      = 1
	DefOptMPPOuterJoinFixedBuildSide        = false
	DefOptWriteRowID                        = false
	DefOptEnableCorrelationAdjustment       = true
	DefOptLimitPushDownThreshold            = 5000
	DefOptCorrelationThreshold              = 0.9
	DefOptCorrelationExpFactor              = 1
	DefOptRiskEqSkewRatio                   = 0.0
	DefOptRiskRangeSkewRatio                = 0.0
	DefOptRiskScaleNDVSkewRatio             = 1.0
	DefOptRiskGroupNDVSkewRatio             = 0.0
	DefOptAlwaysKeepJoinKey                 = true
	DefOptCartesianJoinOrderThreshold       = 0.0
	DefOptCPUFactor                         = 3.0
	DefOptCopCPUFactor                      = 3.0
	DefOptTiFlashConcurrencyFactor          = 24.0
	DefOptNetworkFactor                     = 1.0
	DefOptScanFactor                        = 1.5
	DefOptDescScanFactor                    = 3.0
	DefOptSeekFactor                        = 20.0
	DefOptMemoryFactor                      = 0.001
	DefOptDiskFactor                        = 1.5
	DefOptConcurrencyFactor                 = 3.0
	DefOptIndexScanCostFactor               = 1.0
	DefOptIndexReaderCostFactor             = 1.0
	DefOptTableReaderCostFactor             = 1.0
	DefOptTableFullScanCostFactor           = 1.0
	DefOptTableRangeScanCostFactor          = 1.0
	DefOptTableRowIDScanCostFactor          = 1.0
	DefOptTableTiFlashScanCostFactor        = 1.0
	DefOptIndexLookupCostFactor             = 1.0
	DefOptIndexMergeCostFactor              = 1.0
	DefOptSortCostFactor                    = 1.0
	DefOptTopNCostFactor                    = 1.0
	DefOptLimitCostFactor                   = 1.0
	DefOptStreamAggCostFactor               = 1.0
	DefOptHashAggCostFactor                 = 1.0
	DefOptMergeJoinCostFactor               = 1.0
	DefOptHashJoinCostFactor                = 1.0
	DefOptIndexJoinCostFactor               = 1.0
	DefOptSelectivityFactor                 = 0.8
	DefOptForceInlineCTE                    = false
	DefOptInSubqToJoinAndAgg                = true
	DefOptPreferRangeScan                   = true
	DefOptEnableNoDecorrelateInSelect       = false
	DefOptEnableSemiJoinRewrite             = false
	DefBatchInsert                          = false
	DefBatchDelete                          = false
	DefBatchCommit                          = false
	DefCurretTS                             = 0
	DefInitChunkSize                        = 32
	DefMinPagingSize                        = int(paging.MinPagingSize)
	DefMaxPagingSize                        = int(paging.MaxPagingSize)
	DefMaxChunkSize                         = 1024
	DefDMLBatchSize                         = 0
	DefMaxPreparedStmtCount                 = -1
	DefWaitTimeout                          = 28800
	DefTiDBMemQuotaApplyCache               = 32 << 20 // 32MB.
	DefTiDBMemQuotaBindingCache             = 64 << 20 // 64MB.
	DefTiDBGeneralLog                       = false
	DefTiDBTraceEvent                       = ""
	DefTiDBPProfSQLCPU                      = 0
	DefTiDBRetryLimit                       = 10
	DefTiDBDisableTxnAutoRetry              = true
	DefTiDBConstraintCheckInPlace           = false
	DefTiDBHashJoinConcurrency              = ConcurrencyUnset
	DefTiDBProjectionConcurrency            = ConcurrencyUnset
	DefBroadcastJoinThresholdSize           = 100 * 1024 * 1024
	DefBroadcastJoinThresholdCount          = 10 * 1024
	DefPreferBCJByExchangeDataSize          = false
	DefTiDBOptimizerSelectivityLevel        = 0
	DefTiDBOptIndexPruneThreshold           = 20
	DefTiDBOptimizerEnableNewOFGB           = false
	DefTiDBEnableOuterJoinReorder           = true
	DefTiDBEnableNAAJ                       = true
	DefTiDBAllowBatchCop                    = 1
	DefShardRowIDBits                       = 0
	DefPreSplitRegions                      = 0
	DefBlockEncryptionMode                  = "aes-128-ecb"
	DefTiDBAllowMPPExecution                = true
	DefTiDBAllowTiFlashCop                  = false
	DefTiDBHashExchangeWithNewCollation     = true
	DefTiDBEnforceMPPExecution              = false
	DefTiFlashMaxThreads                    = -1
	DefTiFlashMaxBytesBeforeExternalJoin    = -1
	DefTiFlashMaxBytesBeforeExternalGroupBy = -1
	DefTiFlashMaxBytesBeforeExternalSort    = -1
	DefTiFlashMemQuotaQueryPerNode          = 0
	DefTiFlashQuerySpillRatio               = 0.7
	DefTiFlashHashJoinVersion               = joinversion.TiFlashHashJoinVersionDefVal
	DefTiDBEnableTiFlashPipelineMode        = true
	DefTiDBMPPStoreFailTTL                  = "0s"
	DefTiDBTxnMode                          = PessimisticTxnMode
	DefTiDBRowFormatV1                      = 1
	DefTiDBRowFormatV2                      = 2
	DefTiDBDDLReorgWorkerCount              = 4
	DefTiDBDDLReorgBatchSize                = 256
	DefTiDBDDLFlashbackConcurrency          = 64
	DefTiDBDDLErrorCountLimit               = 512
	DefTiDBDDLReorgMaxWriteSpeed            = 0
	DefTiDBMaxDeltaSchemaCount              = 1024
	DefTiDBPlacementMode                    = PlacementModeStrict
	DefTiDBEnableAutoIncrementInGenerated   = false
	DefTiDBHashAggPartialConcurrency        = ConcurrencyUnset
	DefTiDBHashAggFinalConcurrency          = ConcurrencyUnset
	DefTiDBWindowConcurrency                = ConcurrencyUnset
	DefTiDBMergeJoinConcurrency             = 1 // disable optimization by default
	DefTiDBStreamAggConcurrency             = 1
	DefTiDBForcePriority                    = mysql.NoPriority
	DefEnableWindowFunction                 = true
	DefEnablePipelinedWindowFunction        = true
	DefEnableStrictDoubleTypeCheck          = true
	DefEnableVectorizedExpression           = true
	DefTiDBOptJoinReorderThreshold          = 0
	DefTiDBOptJoinReorderThroughSel         = false
	DefTiDBDDLSlowOprThreshold              = 300
	DefTiDBUseFastAnalyze                   = false
	DefTiDBSkipIsolationLevelCheck          = false
	DefTiDBExpensiveQueryTimeThreshold      = 60      // 60s
	DefTiDBExpensiveTxnTimeThreshold        = 60 * 10 // 10 minutes
	DefTiDBScatterRegion                    = ScatterOff
	DefTiDBWaitSplitRegionFinish            = true
	DefWaitSplitRegionTimeout               = 300 // 300s
	DefTiDBEnableNoopFuncs                  = Off
	DefTiDBEnableNoopVariables              = true
	DefTiDBAllowRemoveAutoInc               = false
	DefTiDBUsePlanBaselines                 = true
	DefTiDBEvolvePlanBaselines              = false
	DefTiDBEvolvePlanTaskMaxTime            = 600 // 600s
	DefTiDBEvolvePlanTaskStartTime          = "00:00 +0000"
	DefTiDBEvolvePlanTaskEndTime            = "23:59 +0000"
	DefInnodbLockWaitTimeout                = 50 // 50s
	DefTiDBStoreLimit                       = 0
	DefTiDBMetricSchemaStep                 = 60 // 60s
	DefTiDBMetricSchemaRangeDuration        = 60 // 60s
	DefTiDBFoundInPlanCache                 = false
	DefTiDBFoundInBinding                   = false
	DefTiDBEnableCollectExecutionInfo       = true
	DefTiDBAllowAutoRandExplicitInsert      = false
	DefTiDBEnableClusteredIndex             = ClusteredIndexDefModeOn
	DefTiDBRedactLog                        = Off
	DefTiDBRestrictedReadOnly               = false
	DefTiDBSuperReadOnly                    = false
	DefTiDBShardAllocateStep                = math.MaxInt64
	DefTiDBPointGetCache                    = false
	DefTiDBEnableTelemetry                  = true
	DefTiDBEnableParallelApply              = false
	DefTiDBPartitionPruneMode               = "dynamic"
	DefTiDBEnableRateLimitAction            = false
	DefTiDBEnableAsyncCommit                = false
	DefTiDBEnable1PC                        = false
	DefTiDBGuaranteeLinearizability         = true
	DefTiDBAnalyzeVersion                   = 2
	// Deprecated: This variable is deprecated, please do not use this variable.
	DefTiDBAutoAnalyzePartitionBatchSize              = mysql.PartitionCountLimit
	DefTiDBEnableIndexMergeJoin                       = false
	DefTiDBTrackAggregateMemoryUsage                  = true
	DefCTEMaxRecursionDepth                           = 1000
	DefTiDBTmpTableMaxSize                            = 64 << 20 // 64MB.
	DefTiDBEnableLocalTxn                             = false
	DefTiDBTSOClientBatchMaxWaitTime                  = 0.0 // 0ms
	DefTiDBEnableTSOFollowerProxy                     = false
	DefPDEnableFollowerHandleRegion                   = true
	DefTiDBEnableBatchQueryRegion                     = false
	DefTiDBEnableOrderedResultMode                    = false
	DefTiDBEnablePseudoForOutdatedStats               = false
	DefTiDBRegardNULLAsPoint                          = true
	DefEnablePlacementCheck                           = true
	DefTimestamp                                      = "0"
	DefTimestampFloat                                 = 0.0
	DefTiDBEnableStmtSummary                          = true
	DefTiDBStmtSummaryInternalQuery                   = false
	DefTiDBStmtSummaryRefreshInterval                 = 1800
	DefTiDBStmtSummaryHistorySize                     = 24
	DefTiDBStmtSummaryMaxStmtCount                    = 3000
	DefTiDBStmtSummaryMaxSQLLength                    = 32768
	DefTiDBCapturePlanBaseline                        = Off
	DefTiDBIgnoreInlistPlanDigest                     = false
	DefTiDBEnableIndexMerge                           = true
	DefEnableLegacyInstanceScope                      = true
	DefTiDBTableCacheLease                            = 3 // 3s
	DefTiDBPersistAnalyzeOptions                      = true
	DefTiDBStatsLoadSyncWait                          = 100
	DefTiDBStatsLoadPseudoTimeout                     = true
	DefSysdateIsNow                                   = false
	DefTiDBEnableParallelHashaggSpill                 = true
	DefTiDBEnableMutationChecker                      = false
	DefTiDBTxnAssertionLevel                          = AssertionOffStr
	DefTiDBIgnorePreparedCacheCloseStmt               = false
	DefTiDBBatchPendingTiFlashCount                   = 4000
	DefRCReadCheckTS                                  = false
	DefTiDBRemoveOrderbyInSubquery                    = true
	DefTiDBSkewDistinctAgg                            = false
	DefTiDB3StageDistinctAgg                          = true
	DefTiDB3StageMultiDistinctAgg                     = false
	DefTiDBOptExplainEvaledSubquery                   = false
	DefTiDBReadStaleness                              = 0
	DefTiDBGCMaxWaitTime                              = 24 * 60 * 60
	DefMaxAllowedPacket                        uint64 = 67108864
	DefTiDBEnableBatchDML                             = false
	DefTiDBMemQuotaQuery                              = memory.DefMemQuotaQuery // 1GB
	DefTiDBStatsCacheMemQuota                         = 0
	MaxTiDBStatsCacheMemQuota                         = 1024 * 1024 * 1024 * 1024 // 1TB
	DefTiDBQueryLogMaxLen                             = 4096
	DefRequireSecureTransport                         = false
	DefTiDBCommitterConcurrency                       = 128
	DefTiDBPipelinedDmlResourcePolicy                 = StrategyStandard
	DefTiDBBatchDMLIgnoreError                        = false
	DefTiDBMemQuotaAnalyze                            = -1
	DefTiDBEnableAutoAnalyze                          = true
	DefTiDBEnableAutoAnalyzePriorityQueue             = true
	DefTiDBAnalyzeColumnOptions                       = "ALL"
	DefTiDBMemOOMAction                               = "CANCEL"
	DefTiDBMaxAutoAnalyzeTime                         = 12 * 60 * 60
	DefTiDBAutoAnalyzeConcurrency                     = 1
	DefTiDBEnablePrepPlanCache                        = true
	DefTiDBPrepPlanCacheSize                          = 100
	DefTiDBSessionPlanCacheSize                       = 100
	DefTiDBEnablePrepPlanCacheMemoryMonitor           = true
	DefTiDBPrepPlanCacheMemoryGuardRatio              = 0.1
	DefTiDBEnableWorkloadBasedLearning                = false
	DefTiDBWorkloadBasedLearningInterval              = 24 * time.Hour
	DefTiDBEnableDistTask                             = true
	DefTiDBMaxDistTaskNodes                           = -1
	DefTiDBEnableFastCreateTable                      = true
	DefTiDBSimplifiedMetrics                          = false
	DefTiDBEnablePaging                               = true
	DefTiFlashFineGrainedShuffleStreamCount           = 0
	DefStreamCountWhenMaxThreadsNotSet                = 8
	DefTiFlashFineGrainedShuffleBatchSize             = 8192
	DefAdaptiveClosestReadThreshold                   = 4096
	DefTiDBEnableAnalyzeSnapshot                      = false
	DefTiDBGenerateBinaryPlan                         = true
	DefTiDBEnableDDLAnalyze                           = false
	DefEnableTiDBGCAwareMemoryTrack                   = false
	DefTiDBDefaultStrMatchSelectivity                 = 0.8
	DefTiDBEnableTmpStorageOnOOM                      = true
	DefTiDBEnableMDL                                  = true
	DefTiFlashFastScan                                = false
	DefMemoryUsageAlarmRatio                          = 0.7
	DefMemoryUsageAlarmKeepRecordNum                  = 5
	DefTiDBEnableFastReorg                            = true
	DefTiDBDDLDiskQuota                               = 100 * 1024 * 1024 * 1024 // 100GB
	DefExecutorConcurrency                            = 5
	DefTiDBEnableNonPreparedPlanCache                 = false
	DefTiDBEnableNonPreparedPlanCacheForDML           = true
	DefTiDBNonPreparedPlanCacheSize                   = 100
	DefTiDBPlanCacheMaxPlanSize                       = 2 * size.MB
	DefTiDBInstancePlanCacheMaxMemSize                = 100 * size.MB
	MinTiDBInstancePlanCacheMemSize                   = 100 * size.MB
	DefTiDBInstancePlanCacheReservedPercentage        = 0.1
	// MaxDDLReorgBatchSize is exported for testing.
	MaxDDLReorgBatchSize                  int32  = 10240
	MinDDLReorgBatchSize                  int32  = 32
	MinExpensiveQueryTimeThreshold        uint64 = 10 // 10s
	MinExpensiveTxnTimeThreshold          uint64 = 60 // 60s
	DefTiDBAutoBuildStatsConcurrency             = 1
	DefTiDBSysProcScanConcurrency                = 1
	DefTiDBRcWriteCheckTs                        = false
	DefTiDBForeignKeyChecks                      = true
	DefTiDBForeignKeyCheckInSharedLock           = false
	DefTiDBOptAdvancedJoinHint                   = true
	DefTiDBAnalyzePartitionConcurrency           = 2
	DefTiDBOptRangeMaxSize                       = 64 * int64(size.MB) // 64 MB
	DefTiDBCostModelVer                          = 2
	DefTiDBServerMemoryLimitSessMinSize          = 128 << 20
	DefTiDBMergePartitionStatsConcurrency        = 1
	DefTiDBServerMemoryLimitGCTrigger            = 0.7
	DefTiDBEnableGOGCTuner                       = true
	// DefTiDBGOGCTunerThreshold is to limit TiDBGOGCTunerThreshold.
	DefTiDBGOGCTunerThreshold                 float64 = 0.6
	DefTiDBGOGCMaxValue                               = 500
	DefTiDBGOGCMinValue                               = 100
	DefTiDBOptPrefixIndexSingleScan                   = true
	DefTiDBOptPartialOrderedIndexForTopN              = "DISABLE"
	DefTiDBEnableAsyncMergeGlobalStats                = true
	DefTiDBExternalTS                                 = 0
	DefTiDBEnableExternalTSRead                       = false
	DefTiDBEnableReusechunk                           = true
	DefTiDBUseAlloc                                   = false
	DefTiDBEnablePlanReplayerCapture                  = true
	DefTiDBIndexMergeIntersectionConcurrency          = ConcurrencyUnset
	DefTiDBTTLJobEnable                               = true
	DefTiDBTTLScanBatchSize                           = 500
	DefTiDBTTLScanBatchMaxSize                        = 10240
	DefTiDBTTLScanBatchMinSize                        = 1
	DefTiDBTTLDeleteBatchSize                         = 100
	DefTiDBTTLDeleteBatchMaxSize                      = 10240
	DefTiDBTTLDeleteBatchMinSize                      = 1
	DefTiDBTTLDeleteRateLimit                         = 0
	DefTiDBTTLRunningTasks                            = -1
	DefPasswordReuseHistory                           = 0
	DefPasswordReuseTime                              = 0
	DefMaxUserConnections                             = 0
	DefTiDBStoreBatchSize                             = 4
	DefTiDBHistoricalStatsDuration                    = 7 * 24 * time.Hour
	DefTiDBEnableHistoricalStatsForCapture            = false
	DefTiDBTTLJobScheduleWindowStartTime              = "00:00 +0000"
	DefTiDBTTLJobScheduleWindowEndTime                = "23:59 +0000"
	DefTiDBTTLScanWorkerCount                         = 4
	DefTiDBTTLDeleteWorkerCount                       = 4
	DefaultExchangeCompressionMode                    = ExchangeCompressionModeUnspecified
	DefTiDBEnableResourceControl                      = true
	DefTiDBResourceControlStrictMode                  = true
	DefTiDBPessimisticTransactionFairLocking          = false
	DefTiDBEnablePlanCacheForParamLimit               = true
	DefTiDBEnableINLJoinMultiPattern                  = true
	DefTiFlashComputeDispatchPolicy                   = DispatchPolicyConsistentHashStr
	DefTiDBEnablePlanCacheForSubquery                 = true
	DefTiDBLoadBasedReplicaReadThreshold              = time.Second
	DefTiDBOptEnableLateMaterialization               = true
	DefTiDBOptOrderingIdxSelThresh                    = 0.0
	DefTiDBOptOrderingIdxSelRatio                     = 0.01
	DefTiDBOptEnableMPPSharedCTEExecution             = false
	DefTiDBPlanCacheInvalidationOnFreshStats          = true
	DefTiDBEnableRowLevelChecksum                     = false
	DefAuthenticationLDAPSASLAuthMethodName           = "SCRAM-SHA-1"
	DefAuthenticationLDAPSASLServerPort               = 389
	DefAuthenticationLDAPSASLTLS                      = false
	DefAuthenticationLDAPSASLUserSearchAttr           = "uid"
	DefAuthenticationLDAPSASLInitPoolSize             = 10
	DefAuthenticationLDAPSASLMaxPoolSize              = 1000
	DefAuthenticationLDAPSimpleAuthMethodName         = "SIMPLE"
	DefAuthenticationLDAPSimpleServerPort             = 389
	DefAuthenticationLDAPSimpleTLS                    = false
	DefAuthenticationLDAPSimpleUserSearchAttr         = "uid"
	DefAuthenticationLDAPSimpleInitPoolSize           = 10
	DefAuthenticationLDAPSimpleMaxPoolSize            = 1000
	DefTiFlashReplicaRead                             = AllReplicaStr
	DefTiDBEnableFastCheckTable                       = true
	DefRuntimeFilterType                              = "IN"
	DefRuntimeFilterMode                              = "OFF"
	DefTiDBLockUnchangedKeys                          = true
	DefTiDBEnableCheckConstraint                      = false
	DefTiDBSkipMissingPartitionStats                  = true
	DefTiDBOptEnableHashJoin                          = true
	DefTiDBHashJoinVersion                            = joinversion.HashJoinVersionOptimized
	DefTiDBOptIndexJoinBuild                          = true
	DefTiDBOptObjective                               = OptObjectiveModerate
	DefTiDBSchemaVersionCacheLimit                    = 16
	DefTiDBIdleTransactionTimeout                     = 0
	DefTiDBTxnEntrySizeLimit                          = 0
	DefTiDBSchemaCacheSize                            = 512 * 1024 * 1024
	DefTiDBLowResolutionTSOUpdateInterval             = 2000
	DefDivPrecisionIncrement                          = 4
	DefTiDBDMLType                                    = "STANDARD"
	DefGroupConcatMaxLen                              = uint64(1024)
	DefDefaultWeekFormat                              = "0"
	DefTiFlashPreAggMode                              = ForcePreAggStr
	DefTiDBEnableLazyCursorFetch                      = false
	DefOptEnableProjectionPushDown                    = true
	DefTiDBEnableSharedLockPromotion                  = false
	DefTiDBTSOClientRPCMode                           = TSOClientRPCModeDefault
	DefTiDBCircuitBreakerPDMetaErrorRateRatio         = 0.0
	DefTiDBAccelerateUserCreationUpdate               = false
	DefTiDBEnableTSValidation                         = true
	DefTiDBLoadBindingTimeout                         = 200
	DefTiDBEnableBindingUsage                         = true
	DefTiDBAdvancerCheckPointLagLimit                 = 48 * time.Hour
	DefTiDBMemArbitratorSoftLimitText                 = memory.ArbitratorSoftLimitModDisableName
	DefTiDBMemArbitratorModeText                      = memory.ArbitratorModeDisableName
	DefTiDBMemArbitratorQueryReservedText             = "0"
	DefTiDBMemArbitratorWaitAverse                    = "0"
	DefTiDBIndexLookUpPushDownPolicy                  = IndexLookUpPushDownPolicyHintOnly
)

// Process global variables.
var (
	ProcessGeneralLog              = atomic.NewBool(false)
	RunAutoAnalyze                 = atomic.NewBool(DefTiDBEnableAutoAnalyze)
	EnableAutoAnalyzePriorityQueue = atomic.NewBool(DefTiDBEnableAutoAnalyzePriorityQueue)
	// AnalyzeColumnOptions is a global variable that indicates the default column choice for ANALYZE.
	// The value of this variable is a string that can be one of the following values:
	// "PREDICATE", "ALL".
	// The behavior of the analyze operation depends on the value of `tidb_persist_analyze_options`:
	// 1. If `tidb_persist_analyze_options` is enabled and the column choice from the analyze options record is set to `default`,
	//    the value of `tidb_analyze_column_options` determines the behavior of the analyze operation.
	// 2. If `tidb_persist_analyze_options` is disabled, `tidb_analyze_column_options` is used directly to decide
	//    whether to analyze all columns or just the predicate columns.
	AnalyzeColumnOptions           = atomic.NewString(DefTiDBAnalyzeColumnOptions)
	GlobalLogMaxDays               = atomic.NewInt32(int32(config.GetGlobalConfig().Log.File.MaxDays))
	QueryLogMaxLen                 = atomic.NewInt32(DefTiDBQueryLogMaxLen)
	EnablePProfSQLCPU              = atomic.NewBool(false)
	EnableBatchDML                 = atomic.NewBool(false)
	EnableTmpStorageOnOOM          = atomic.NewBool(DefTiDBEnableTmpStorageOnOOM)
	DDLReorgWorkerCounter    int32 = DefTiDBDDLReorgWorkerCount
	DDLReorgBatchSize        int32 = DefTiDBDDLReorgBatchSize
	DDLFlashbackConcurrency  int32 = DefTiDBDDLFlashbackConcurrency
	DDLErrorCountLimit       int64 = DefTiDBDDLErrorCountLimit
	DDLReorgRowFormat        int64 = DefTiDBRowFormatV2
	DDLReorgMaxWriteSpeed          = atomic.NewInt64(DefTiDBDDLReorgMaxWriteSpeed)
	MaxDeltaSchemaCount      int64 = DefTiDBMaxDeltaSchemaCount
	GlobalSlowLogRateLimiter       = rate.NewLimiter(rate.Inf, 1)
	// DDLSlowOprThreshold is the threshold for ddl slow operations, uint is millisecond.
	DDLSlowOprThreshold = config.GetGlobalConfig().Instance.DDLSlowOprThreshold
	GlobalSlowLogRules  = atomic.NewPointer[slowlogrule.GlobalSlowLogRules](
		&slowlogrule.GlobalSlowLogRules{RulesMap: make(map[int64]*slowlogrule.SlowLogRules)})
	ForcePriority                        = int32(DefTiDBForcePriority)
	MaxOfMaxAllowedPacket         uint64 = 1073741824
	ExpensiveQueryTimeThreshold   uint64 = DefTiDBExpensiveQueryTimeThreshold
	ExpensiveTxnTimeThreshold     uint64 = DefTiDBExpensiveTxnTimeThreshold
	MemoryUsageAlarmRatio                = atomic.NewFloat64(DefMemoryUsageAlarmRatio)
	MemoryUsageAlarmKeepRecordNum        = atomic.NewInt64(DefMemoryUsageAlarmKeepRecordNum)
	EnableLocalTxn                       = atomic.NewBool(DefTiDBEnableLocalTxn)
	MaxTSOBatchWaitInterval              = atomic.NewFloat64(DefTiDBTSOClientBatchMaxWaitTime)
	EnableTSOFollowerProxy               = atomic.NewBool(DefTiDBEnableTSOFollowerProxy)
	EnablePDFollowerHandleRegion         = atomic.NewBool(DefPDEnableFollowerHandleRegion)
	EnableBatchQueryRegion               = atomic.NewBool(DefTiDBEnableBatchQueryRegion)
	RestrictedReadOnly                   = atomic.NewBool(DefTiDBRestrictedReadOnly)
	VarTiDBSuperReadOnly                 = atomic.NewBool(DefTiDBSuperReadOnly)
	PersistAnalyzeOptions                = atomic.NewBool(DefTiDBPersistAnalyzeOptions)
	TableCacheLease                      = atomic.NewInt64(DefTiDBTableCacheLease)
	StatsLoadSyncWait                    = atomic.NewInt64(DefTiDBStatsLoadSyncWait)
	StatsLoadPseudoTimeout               = atomic.NewBool(DefTiDBStatsLoadPseudoTimeout)
	MemQuotaBindingCache                 = atomic.NewInt64(DefTiDBMemQuotaBindingCache)
	GCMaxWaitTime                        = atomic.NewInt64(DefTiDBGCMaxWaitTime)
	StatsCacheMemQuota                   = atomic.NewInt64(DefTiDBStatsCacheMemQuota)
	OOMAction                            = atomic.NewString(DefTiDBMemOOMAction)
	MaxAutoAnalyzeTime                   = atomic.NewInt64(DefTiDBMaxAutoAnalyzeTime)
	// variables for plan cache
	PreparedPlanCacheMemoryGuardRatio   = atomic.NewFloat64(DefTiDBPrepPlanCacheMemoryGuardRatio)
	EnableInstancePlanCache             = atomic.NewBool(false)
	InstancePlanCacheReservedPercentage = atomic.NewFloat64(0.1)
	InstancePlanCacheMaxMemSize         = atomic.NewInt64(int64(DefTiDBInstancePlanCacheMaxMemSize))
	EnableDistTask                      = atomic.NewBool(DefTiDBEnableDistTask)
	EnableFastCreateTable               = atomic.NewBool(DefTiDBEnableFastCreateTable)
	EnableNoopVariables                 = atomic.NewBool(DefTiDBEnableNoopVariables)
	enableMDL                           = atomic.NewBool(false)
	AutoAnalyzePartitionBatchSize       = atomic.NewInt64(DefTiDBAutoAnalyzePartitionBatchSize)
	AutoAnalyzeConcurrency              = atomic.NewInt32(DefTiDBAutoAnalyzeConcurrency)
	// TODO: set value by session variable
	EnableWorkloadBasedLearning   = atomic.NewBool(DefTiDBEnableWorkloadBasedLearning)
	WorkloadBasedLearningInterval = atomic.NewDuration(DefTiDBWorkloadBasedLearningInterval)
	// EnableFastReorg indicates whether to use lightning to enhance DDL reorg performance.
	EnableFastReorg = atomic.NewBool(DefTiDBEnableFastReorg)
	// DDLDiskQuota is the temporary variable for set disk quota for lightning
	DDLDiskQuota = atomic.NewUint64(DefTiDBDDLDiskQuota)
	// EnableForeignKey indicates whether to enable foreign key feature.
	EnableForeignKey    = atomic.NewBool(true)
	EnableRCReadCheckTS = atomic.NewBool(false)
	// EnableRowLevelChecksum indicates whether to append checksum to row values.
	EnableRowLevelChecksum         = atomic.NewBool(DefTiDBEnableRowLevelChecksum)
	LowResolutionTSOUpdateInterval = atomic.NewUint32(DefTiDBLowResolutionTSOUpdateInterval)

	// DefTiDBServerMemoryLimit indicates the default value of TiDBServerMemoryLimit(TotalMem * 80%).
	// It should be a const and shouldn't be modified after tidb is started.
	DefTiDBServerMemoryLimit           = serverMemoryLimitDefaultValue()
	GOGCTunerThreshold                 = atomic.NewFloat64(DefTiDBGOGCTunerThreshold)
	PasswordValidationLength           = atomic.NewInt32(8)
	PasswordValidationMixedCaseCount   = atomic.NewInt32(1)
	PasswordValidtaionNumberCount      = atomic.NewInt32(1)
	PasswordValidationSpecialCharCount = atomic.NewInt32(1)
	EnableTTLJob                       = atomic.NewBool(DefTiDBTTLJobEnable)
	TTLScanBatchSize                   = atomic.NewInt64(DefTiDBTTLScanBatchSize)
	TTLDeleteBatchSize                 = atomic.NewInt64(DefTiDBTTLDeleteBatchSize)
	TTLDeleteRateLimit                 = atomic.NewInt64(DefTiDBTTLDeleteRateLimit)
	TTLJobScheduleWindowStartTime      = atomic.NewTime(
		mustParseTime(
			FullDayTimeFormat,
			DefTiDBTTLJobScheduleWindowStartTime,
		),
	)
	TTLJobScheduleWindowEndTime = atomic.NewTime(
		mustParseTime(
			FullDayTimeFormat,
			DefTiDBTTLJobScheduleWindowEndTime,
		),
	)
	TTLScanWorkerCount              = atomic.NewInt32(DefTiDBTTLScanWorkerCount)
	TTLDeleteWorkerCount            = atomic.NewInt32(DefTiDBTTLDeleteWorkerCount)
	PasswordHistory                 = atomic.NewInt64(DefPasswordReuseHistory)
	PasswordReuseInterval           = atomic.NewInt64(DefPasswordReuseTime)
	IsSandBoxModeEnabled            = atomic.NewBool(false)
	MaxUserConnectionsValue         = atomic.NewUint32(DefMaxUserConnections)
	MaxPreparedStmtCountValue       = atomic.NewInt64(DefMaxPreparedStmtCount)
	HistoricalStatsDuration         = atomic.NewDuration(DefTiDBHistoricalStatsDuration)
	EnableHistoricalStatsForCapture = atomic.NewBool(DefTiDBEnableHistoricalStatsForCapture)
	TTLRunningTasks                 = atomic.NewInt32(DefTiDBTTLRunningTasks)
	// always set the default value to false because the resource control in kv-client is not inited
	// It will be initialized to the right value after the first call of `rebuildSysVarCache`
	EnableResourceControl           = atomic.NewBool(false)
	EnableResourceControlStrictMode = atomic.NewBool(true)
	EnableCheckConstraint           = atomic.NewBool(DefTiDBEnableCheckConstraint)
	SkipMissingPartitionStats       = atomic.NewBool(DefTiDBSkipMissingPartitionStats)
	TiFlashEnablePipelineMode       = atomic.NewBool(DefTiDBEnableTiFlashPipelineMode)
	ServiceScope                    = atomic.NewString("")
	SchemaVersionCacheLimit         = atomic.NewInt64(DefTiDBSchemaVersionCacheLimit)
	CloudStorageURI                 = atomic.NewString("")
	IgnoreInlistPlanDigest          = atomic.NewBool(DefTiDBIgnoreInlistPlanDigest)
	TxnEntrySizeLimit               = atomic.NewUint64(DefTiDBTxnEntrySizeLimit)

	SchemaCacheSize              = atomic.NewUint64(DefTiDBSchemaCacheSize)
	SchemaCacheSizeOriginText    = atomic.NewString(strconv.Itoa(DefTiDBSchemaCacheSize))
	AccelerateUserCreationUpdate = atomic.NewBool(DefTiDBAccelerateUserCreationUpdate)

	CircuitBreakerPDMetadataErrorRateThresholdRatio = atomic.NewFloat64(0.0)

	AdvancerCheckPointLagLimit = atomic.NewDuration(DefTiDBAdvancerCheckPointLagLimit)
	EnableBindingUsage         = atomic.NewBool(DefTiDBEnableBindingUsage)
)

func serverMemoryLimitDefaultValue() string {
	total, err := memory.MemTotal()
	if err == nil && total != 0 {
		return "80%"
	}
	return "0"
}

func mustParseTime(layout string, str string) time.Time {
	time, err := time.ParseInLocation(layout, str, time.UTC)
	if err != nil {
		panic(fmt.Sprintf("%s is not in %s duration format", str, layout))
	}

	return time
}

const (
	// OptObjectiveModerate is a possible value and the default value for TiDBOptObjective.
	// Please see comments of SessionVars.OptObjective for details.
	OptObjectiveModerate string = "moderate"
	// OptObjectiveDeterminate is a possible value for TiDBOptObjective.
	OptObjectiveDeterminate = "determinate"
)

// ForcePreAggStr means 1st hashagg will be pre aggregated.
// AutoStr means TiFlash will decide which policy for 1st hashagg.
// ForceStreamingStr means 1st hashagg will for pass through all blocks.
const (
	ForcePreAggStr    = "force_preagg"
	AutoStr           = "auto"
	ForceStreamingStr = "force_streaming"
)

const (
	// AllReplicaStr is the string value of AllReplicas.
	AllReplicaStr = "all_replicas"
	// ClosestAdaptiveStr is the string value of ClosestAdaptive.
	ClosestAdaptiveStr = "closest_adaptive"
	// ClosestReplicasStr is the string value of ClosestReplicas.
	ClosestReplicasStr = "closest_replicas"
)

const (
	// DispatchPolicyRRStr is string value for DispatchPolicyRR.
	DispatchPolicyRRStr = "round_robin"
	// DispatchPolicyConsistentHashStr is string value for DispatchPolicyConsistentHash.
	DispatchPolicyConsistentHashStr = "consistent_hash"
	// DispatchPolicyInvalidStr is string value for DispatchPolicyInvalid.
	DispatchPolicyInvalidStr = "invalid"
)

// ConcurrencyUnset means the value the of the concurrency related variable is unset.
const ConcurrencyUnset = -1

// ExchangeCompressionMode means the compress method used in exchange operator
type ExchangeCompressionMode int

const (
	// ExchangeCompressionModeNONE indicates no compression
	ExchangeCompressionModeNONE ExchangeCompressionMode = iota
	// ExchangeCompressionModeFast indicates fast compression/decompression speed, compression ratio is lower than HC mode
	ExchangeCompressionModeFast
	// ExchangeCompressionModeHC indicates high compression (HC) ratio mode
	ExchangeCompressionModeHC
	// ExchangeCompressionModeUnspecified indicates unspecified compress method, let TiDB choose one
	ExchangeCompressionModeUnspecified

	// RecommendedExchangeCompressionMode indicates recommended compression mode
	RecommendedExchangeCompressionMode ExchangeCompressionMode = ExchangeCompressionModeFast

	exchangeCompressionModeUnspecifiedName string = "UNSPECIFIED"
)

// Name returns the name of ExchangeCompressionMode
func (t ExchangeCompressionMode) Name() string {
	if t == ExchangeCompressionModeUnspecified {
		return exchangeCompressionModeUnspecifiedName
	}
	return t.ToTipbCompressionMode().String()
}

// ToExchangeCompressionMode returns the ExchangeCompressionMode from name
func ToExchangeCompressionMode(name string) (ExchangeCompressionMode, bool) {
	name = strings.ToUpper(name)
	if name == exchangeCompressionModeUnspecifiedName {
		return ExchangeCompressionModeUnspecified, true
	}
	value, ok := tipb.CompressionMode_value[name]
	if ok {
		return ExchangeCompressionMode(value), true
	}
	return ExchangeCompressionModeNONE, false
}

// ToTipbCompressionMode returns tipb.CompressionMode from kv.ExchangeCompressionMode
func (t ExchangeCompressionMode) ToTipbCompressionMode() tipb.CompressionMode {
	switch t {
	case ExchangeCompressionModeNONE:
		return tipb.CompressionMode_NONE
	case ExchangeCompressionModeFast:
		return tipb.CompressionMode_FAST
	case ExchangeCompressionModeHC:
		return tipb.CompressionMode_HIGH_COMPRESSION
	}
	return tipb.CompressionMode_NONE
}

// ScopeFlag is for system variable whether can be changed in global/session dynamically or not.
type ScopeFlag uint8

// TypeFlag is the SysVar type, which doesn't exactly match MySQL types.
type TypeFlag byte

const (
	// ScopeNone means the system variable can not be changed dynamically.
	ScopeNone ScopeFlag = 0
	// ScopeGlobal means the system variable can be changed globally.
	ScopeGlobal ScopeFlag = 1 << 0
	// ScopeSession means the system variable can only be changed in current session.
	ScopeSession ScopeFlag = 1 << 1
	// ScopeInstance means it is similar to global but doesn't propagate to other TiDB servers.
	ScopeInstance ScopeFlag = 1 << 2

	// TypeStr is the default
	TypeStr TypeFlag = iota
	// TypeBool for boolean
	TypeBool
	// TypeInt for integer
	TypeInt
	// TypeEnum for Enum
	TypeEnum
	// TypeFloat for Double
	TypeFloat
	// TypeUnsigned for Unsigned integer
	TypeUnsigned
	// TypeTime for time of day (a TiDB extension)
	TypeTime
	// TypeDuration for a golang duration (a TiDB extension)
	TypeDuration

	// On is the canonical string for ON
	On = "ON"
	// Off is the canonical string for OFF
	Off = "OFF"
	// Warn means return warnings
	Warn = "WARN"
	// IntOnly means enable for int type
	IntOnly = "INT_ONLY"
	// Marker is a special log redact behavior
	Marker = "MARKER"

	// AssertionStrictStr is a choice of variable TiDBTxnAssertionLevel that means full assertions should be performed,
	// even if the performance might be slowed down.
	AssertionStrictStr = "STRICT"
	// AssertionFastStr is a choice of variable TiDBTxnAssertionLevel that means assertions that doesn't affect
	// performance should be performed.
	AssertionFastStr = "FAST"
	// AssertionOffStr is a choice of variable TiDBTxnAssertionLevel that means no assertion should be performed.
	AssertionOffStr = "OFF"
	// OOMActionCancel constants represents the valid action configurations for OOMAction "CANCEL".
	OOMActionCancel = "CANCEL"
	// OOMActionLog constants represents the valid action configurations for OOMAction "LOG".
	OOMActionLog = "LOG"

	// TSOClientRPCModeDefault is a choice of variable TiDBTSOClientRPCMode. In this mode, the TSO client sends batched
	// TSO requests serially.
	TSOClientRPCModeDefault = "DEFAULT"
	// TSOClientRPCModeParallel is a choice of variable TiDBTSOClientRPCMode. In this mode, the TSO client tries to
	// keep approximately 2 batched TSO requests running in parallel. This option tries to reduce the batch-waiting time
	// by half, at the expense of about twice the amount of TSO RPC calls.
	TSOClientRPCModeParallel = "PARALLEL"
	// TSOClientRPCModeParallelFast is a choice of variable TiDBTSOClientRPCMode. In this mode, the TSO client tries to
	// keep approximately 4 batched TSO requests running in parallel. This option tries to reduce the batch-waiting time
	// by 3/4, at the expense of about 4 times the amount of TSO RPC calls.
	TSOClientRPCModeParallelFast = "PARALLEL-FAST"

	// StrategyStandard is a choice of variable TiDBPipelinedDmlResourcePolicy,
	// the best performance policy
	StrategyStandard = "standard"
	// StrategyConservative is a choice of variable TiDBPipelinedDmlResourcePolicy,
	// a rather conservative policy
	StrategyConservative = "conservative"
	// StrategyCustom is a choice of variable TiDBPipelinedDmlResourcePolicy,
	StrategyCustom = "custom"

	// IndexLookUpPushDownPolicyHintOnly indicates only use the hint to decide whether to push down the index lookup or not.
	IndexLookUpPushDownPolicyHintOnly = "hint-only"
	// IndexLookUpPushDownPolicyAffinityForce indicates to force push down the index lookup for table with affinity options.
	IndexLookUpPushDownPolicyAffinityForce = "affinity-force"
	// IndexLookUpPushDownPolicyForce indicates to force push down the index lookup for all tables.
	IndexLookUpPushDownPolicyForce = "force"
)

// Global config name list.
const (
	GlobalConfigEnableTopSQL = "enable_resource_metering"
	GlobalConfigSourceID     = "source_id"
)

func (s ScopeFlag) String() string {
	var scopes []string
	if s == ScopeNone {
		return "NONE"
	}
	if s&ScopeSession != 0 {
		scopes = append(scopes, "SESSION")
	}
	if s&ScopeGlobal != 0 {
		scopes = append(scopes, "GLOBAL")
	}
	if s&ScopeInstance != 0 {
		scopes = append(scopes, "INSTANCE")
	}
	return strings.Join(scopes, ",")
}

// ClusteredIndexDefMode controls the default clustered property for primary key.
type ClusteredIndexDefMode int

const (
	// ClusteredIndexDefModeIntOnly indicates only single int primary key will default be clustered.
	ClusteredIndexDefModeIntOnly ClusteredIndexDefMode = 0
	// ClusteredIndexDefModeOn indicates primary key will default be clustered.
	ClusteredIndexDefModeOn ClusteredIndexDefMode = 1
	// ClusteredIndexDefModeOff indicates primary key will default be non-clustered.
	ClusteredIndexDefModeOff ClusteredIndexDefMode = 2
)

// TiDBOptEnableClustered converts enable clustered options to ClusteredIndexDefMode.
func TiDBOptEnableClustered(opt string) ClusteredIndexDefMode {
	switch opt {
	case On:
		return ClusteredIndexDefModeOn
	case Off:
		return ClusteredIndexDefModeOff
	default:
		return ClusteredIndexDefModeIntOnly
	}
}

const (
	// ScatterOff means default, will not scatter region
	ScatterOff string = ""
	// ScatterTable means scatter region at table level
	ScatterTable string = "table"
	// ScatterGlobal means scatter region at global level
	ScatterGlobal string = "global"
)

const (
	// PlacementModeStrict indicates all placement operations should be checked strictly in ddl
	PlacementModeStrict string = "STRICT"
	// PlacementModeIgnore indicates ignore all placement operations in ddl
	PlacementModeIgnore string = "IGNORE"
)

const (
	// LocalDayTimeFormat is the local format of analyze start time and end time.
	LocalDayTimeFormat = "15:04"
	// FullDayTimeFormat is the full format of analyze start time and end time.
	FullDayTimeFormat = "15:04 -0700"
)

// SetDDLReorgWorkerCounter sets DDLReorgWorkerCounter count.
// Sysvar validation enforces the range to already be correct.
func SetDDLReorgWorkerCounter(cnt int32) {
	goatomic.StoreInt32(&DDLReorgWorkerCounter, cnt)
}

// GetDDLReorgWorkerCounter gets DDLReorgWorkerCounter.
func GetDDLReorgWorkerCounter() int32 {
	return goatomic.LoadInt32(&DDLReorgWorkerCounter)
}

// SetDDLFlashbackConcurrency sets DDLFlashbackConcurrency count.
// Sysvar validation enforces the range to already be correct.
func SetDDLFlashbackConcurrency(cnt int32) {
	goatomic.StoreInt32(&DDLFlashbackConcurrency, cnt)
}

// GetDDLFlashbackConcurrency gets DDLFlashbackConcurrency count.
func GetDDLFlashbackConcurrency() int32 {
	return goatomic.LoadInt32(&DDLFlashbackConcurrency)
}

// SetDDLReorgBatchSize sets DDLReorgBatchSize size.
// Sysvar validation enforces the range to already be correct.
func SetDDLReorgBatchSize(cnt int32) {
	goatomic.StoreInt32(&DDLReorgBatchSize, cnt)
}

// GetDDLReorgBatchSize gets DDLReorgBatchSize.
func GetDDLReorgBatchSize() int32 {
	return goatomic.LoadInt32(&DDLReorgBatchSize)
}

// SetDDLErrorCountLimit sets ddlErrorCountlimit size.
func SetDDLErrorCountLimit(cnt int64) {
	goatomic.StoreInt64(&DDLErrorCountLimit, cnt)
}

// GetDDLErrorCountLimit gets ddlErrorCountlimit size.
func GetDDLErrorCountLimit() int64 {
	return goatomic.LoadInt64(&DDLErrorCountLimit)
}

// SetDDLReorgRowFormat sets DDLReorgRowFormat version.
func SetDDLReorgRowFormat(format int64) {
	goatomic.StoreInt64(&DDLReorgRowFormat, format)
}

// GetDDLReorgRowFormat gets DDLReorgRowFormat version.
func GetDDLReorgRowFormat() int64 {
	return goatomic.LoadInt64(&DDLReorgRowFormat)
}

// SetMaxDeltaSchemaCount sets MaxDeltaSchemaCount size.
func SetMaxDeltaSchemaCount(cnt int64) {
	goatomic.StoreInt64(&MaxDeltaSchemaCount, cnt)
}

// GetMaxDeltaSchemaCount gets MaxDeltaSchemaCount size.
func GetMaxDeltaSchemaCount() int64 {
	return goatomic.LoadInt64(&MaxDeltaSchemaCount)
}

// IsMDLEnabled returns if MDL is enabled.
func IsMDLEnabled() bool {
	if kerneltype.IsNextGen() {
		// MDL is very useful to avoid the 'Information schema is changed' error,
		// in next-gen TiDB, MDL is always enabled, as we don't have the compatibility
		// debts.
		// some tests might call SetEnableMDL(false) to disable MDL, but it is not
		// expected in nextgen, we use this branch to ensure MDL is always enabled,
		// even in test.
		return true
	}
	return enableMDL.Load()
}

// SetEnableMDL sets the MDL enable status.
func SetEnableMDL(enabled bool) {
	enableMDL.Store(enabled)
}

// GetDefaultTxnAssertionLevel returns the default assertion level based on kernel type.
// For next-gen, we use strict assertion level to prevent correctness risks.
// For classic, we use off to maintain compatibility.
func GetDefaultTxnAssertionLevel() string {
	if kerneltype.IsNextGen() {
		return AssertionStrictStr
	}
	return AssertionOffStr
}
