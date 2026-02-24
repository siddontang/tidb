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
	"crypto/tls"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/slowlogrule"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/kvcache"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/ppcpuusage"
	"github.com/pingcap/tidb/pkg/util/replayer"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/pingcap/tidb/pkg/util/stmtsummary"
	"github.com/pingcap/tidb/pkg/util/tiflash"
	"github.com/pingcap/tidb/pkg/util/tiflashcompute"
	tikvstore "github.com/tikv/client-go/v2/kv"
	atomic2 "go.uber.org/atomic"
)

// HookContext contains the necessary variables for executing set/get hook
type HookContext interface {
	GetStore() kv.Storage
}

// SessionVarsProvider provides the session variables.
type SessionVarsProvider interface {
	GetSessionVars() *SessionVars
}

// SessionVars should implement `SessionVarsProvider`
var _ SessionVarsProvider = &SessionVars{}

// SessionVars is to handle user-defined or global variables in the current session.
type SessionVars struct {
	Concurrency
	MemQuota
	BatchSize
	PipelinedDMLConfig
	// DMLBatchSize indicates the number of rows batch-committed for a statement.
	// It will be used when using LOAD DATA or BatchInsert or BatchDelete is on.
	DMLBatchSize        int
	RetryLimit          int64
	DisableTxnAutoRetry bool
	*UserVars
	// systems variables, don't modify it directly, use GetSystemVar/SetSystemVar method.
	systems map[string]string
	// SysWarningCount is the system variable "warning_count", because it is on the hot path, so we extract it from the systems
	SysWarningCount int
	// SysErrorCount is the system variable "error_count", because it is on the hot path, so we extract it from the systems
	SysErrorCount uint16
	// nonPreparedPlanCacheStmts stores PlanCacheStmts for non-prepared plan cache.
	nonPreparedPlanCacheStmts *kvcache.SimpleLRUCache
	// PreparedStmts stores prepared statement.
	PreparedStmts        map[uint32]any
	PreparedStmtNameToID map[string]uint32
	// preparedStmtID is id of prepared statement.
	preparedStmtID uint32
	// Parameter values for plan cache.
	PlanCacheParams   *PlanCacheParamList
	LastUpdateTime4PC types.Time

	// The Cached Plan for this execution, it should be *plannercore.PlanCacheValue.
	PlanCacheValue any

	// ActiveRoles stores active roles for current user
	ActiveRoles []*auth.RoleIdentity

	RetryInfo *RetryInfo
	// TxnCtx Should be reset on transaction finished.
	TxnCtx *TransactionContext
	// TxnCtxMu is used to protect TxnCtx.
	TxnCtxMu sync.Mutex

	// TxnManager is used to manage txn context in session
	TxnManager any

	// KVVars is the variables for KV storage.
	KVVars *tikvstore.Variables

	// txnIsolationLevelOneShot is used to implements "set transaction isolation level ..."
	txnIsolationLevelOneShot struct {
		state txnIsolationLevelOneShotState
		value string
	}

	// status stands for the session status. e.g. in transaction or not, auto commit is on or off, and so on.
	status atomic.Uint32

	// ShardRowIDBits is the number of shard bits for user table row ID.
	ShardRowIDBits uint64

	// PreSplitRegions is the number of regions that should be pre-split for the table.
	PreSplitRegions uint64

	// ClientCapability is client's capability.
	ClientCapability uint32

	// TLSConnectionState is the TLS connection state (nil if not using TLS).
	TLSConnectionState *tls.ConnectionState

	// ConnectionID is the connection id of the current session.
	ConnectionID uint64

	// SQLCPUUsages records tidb/tikv cpu usages for current sql
	SQLCPUUsages ppcpuusage.SQLCPUUsages

	// PlanID is the unique id of logical and physical plan.
	PlanID atomic.Int32

	// PlanColumnID is the unique id for column when building plan.
	PlanColumnID atomic.Int64

	// MapScalarSubQ maps the scalar sub queries from its ID to its struct.
	MapScalarSubQ []any

	// MapHashCode2UniqueID4ExtendedCol map the expr's hash code to specified unique ID.
	MapHashCode2UniqueID4ExtendedCol map[string]int

	// User is the user identity with which the session login.
	User *auth.UserIdentity

	// Port is the port of the connected socket
	Port string

	// CurrentDB is the default database of this session.
	CurrentDB string

	// CurrentDBChanged indicates if the CurrentDB has been updated, and if it is we should print it into
	// the slow log to make it be compatible with MySQL, https://github.com/pingcap/tidb/issues/17846.
	CurrentDBChanged bool

	// CommonGlobalLoaded indicates if common global variable has been loaded for this session.
	CommonGlobalLoaded bool

	// InRestrictedSQL indicates if the session is handling restricted SQL execution.
	InRestrictedSQL bool

	// InExplainExplore indicates if this statement is under EXPLAIN EXPLORE.
	InExplainExplore bool

	// SnapshotTS is used for reading history data. For simplicity, SnapshotTS only supports distsql request.
	SnapshotTS uint64

	// LastCommitTS is the commit_ts of the last successful transaction in this session.
	LastCommitTS uint64

	// PrevTraceID stores the trace ID of the previous statement for chaining.
	// This allows trace events to link statements together via prev_trace_id field.
	PrevTraceID []byte

	// TxnReadTS is used for staleness transaction, it provides next staleness transaction startTS.
	TxnReadTS *TxnReadTS

	// SnapshotInfoschema is used with SnapshotTS, when the schema version at snapshotTS less than current schema
	// version, we load an old version schema for query.
	SnapshotInfoschema any

	// GlobalVarsAccessor is used to set and get global variables.
	GlobalVarsAccessor GlobalVarAccessor

	// LastFoundRows is the number of found rows of last query statement
	LastFoundRows uint64

	// StmtCtx holds variables for current executing statement.
	StmtCtx *stmtctx.StatementContext

	// RefCountOfStmtCtx indicates the reference count of StmtCtx. When the
	// StmtCtx is accessed by other sessions, e.g. oom-alarm-handler/expensive-query-handler, add one first.
	// Note: this variable should be accessed and updated by atomic operations.
	RefCountOfStmtCtx stmtctx.ReferenceCount

	// AllowAggPushDown can be set to false to forbid aggregation push down.
	AllowAggPushDown bool

	// AllowDeriveTopN is used to enable/disable derived TopN optimization.
	AllowDeriveTopN bool

	// AllowCartesianBCJ means allow broadcast CARTESIAN join, 0 means not allow, 1 means allow broadcast CARTESIAN join
	// but the table size should under the broadcast threshold, 2 means allow broadcast CARTESIAN join even if the table
	// size exceeds the broadcast threshold
	AllowCartesianBCJ int

	// MPPOuterJoinFixedBuildSide means in MPP plan, always use right(left) table as build side for left(right) out join
	MPPOuterJoinFixedBuildSide bool

	// AllowDistinctAggPushDown can be set true to allow agg with distinct push down to tikv/tiflash.
	AllowDistinctAggPushDown bool

	// EnableSkewDistinctAgg can be set true to allow skew distinct aggregate rewrite
	EnableSkewDistinctAgg bool

	// Enable3StageDistinctAgg indicates whether to allow 3 stage distinct aggregate
	Enable3StageDistinctAgg bool

	// Enable3StageMultiDistinctAgg indicates whether to allow 3 stage multi distinct aggregate
	Enable3StageMultiDistinctAgg bool

	ExplainNonEvaledSubQuery bool

	// MultiStatementMode permits incorrect client library usage. Not recommended to be turned on.
	MultiStatementMode int

	// InMultiStmts indicates whether the statement is a multi-statement like `update t set a=1; update t set b=2;`.
	InMultiStmts bool

	// AllowWriteRowID variable is currently not recommended to be turned on.
	AllowWriteRowID bool

	// AllowBatchCop means if we should send batch coprocessor to TiFlash. Default value is 1, means to use batch cop in case of aggregation and join.
	// Value set to 2 means to force to send batch cop for any query. Value set to 0 means never use batch cop.
	AllowBatchCop int

	// allowMPPExecution means if we should use mpp way to execute query.
	// Default value is `true`, means to be determined by the optimizer.
	// Value set to `false` means never use mpp.
	allowMPPExecution bool

	// allowTiFlashCop means if we must use mpp way to execute query.
	// Default value is `false`, means to be determined by the optimizer.
	// Value set to `true` means we may fall back to TiFlash cop if possible.
	allowTiFlashCop bool

	// HashExchangeWithNewCollation means if we support hash exchange when new collation is enabled.
	// Default value is `true`, means support hash exchange when new collation is enabled.
	// Value set to `false` means not use hash exchange when new collation is enabled.
	HashExchangeWithNewCollation bool

	// enforceMPPExecution means if we should enforce mpp way to execute query.
	// Default value is `false`, means to be determined by variable `allowMPPExecution`.
	// Value set to `true` means enforce use mpp.
	// Note if you want to set `enforceMPPExecution` to `true`, you must set `allowMPPExecution` to `true` first.
	enforceMPPExecution bool

	// TiFlashMaxThreads is the maximum number of threads to execute the request which is pushed down to tiflash.
	// Default value is -1, means it will not be pushed down to tiflash.
	// If the value is bigger than -1, it will be pushed down to tiflash and used to create db context in tiflash.
	TiFlashMaxThreads int64

	// TiFlashMaxBytesBeforeExternalJoin is the maximum bytes used by a TiFlash join before spill to disk
	// Default value is -1, means it will not be pushed down to TiFlash
	// If the value is bigger than -1, it will be pushed down to TiFlash, and if the value is 0, it means
	// not limit and spill will never happen
	TiFlashMaxBytesBeforeExternalJoin int64

	// TiFlashMaxBytesBeforeExternalGroupBy is the maximum bytes used by a TiFlash hash aggregation before spill to disk
	// Default value is -1, means it will not be pushed down to TiFlash
	// If the value is bigger than -1, it will be pushed down to TiFlash, and if the value is 0, it means
	// not limit and spill will never happen
	TiFlashMaxBytesBeforeExternalGroupBy int64

	// TiFlashMaxBytesBeforeExternalSort is the maximum bytes used by a TiFlash sort/TopN before spill to disk
	// Default value is -1, means it will not be pushed down to TiFlash
	// If the value is bigger than -1, it will be pushed down to TiFlash, and if the value is 0, it means
	// not limit and spill will never happen
	TiFlashMaxBytesBeforeExternalSort int64

	// TiFlash max query memory per node, -1 and 0 means no limit, and the default value is 0
	// If TiFlashMaxQueryMemoryPerNode > 0 && TiFlashQuerySpillRatio > 0, it will trigger auto spill in TiFlash side, and when auto spill
	// is triggered, per executor's memory usage threshold set by TiFlashMaxBytesBeforeExternalJoin/TiFlashMaxBytesBeforeExternalGroupBy/TiFlashMaxBytesBeforeExternalSort will be ignored.
	TiFlashMaxQueryMemoryPerNode int64

	// TiFlashQuerySpillRatio is the percentage threshold to trigger auto spill in TiFlash if TiFlashMaxQueryMemoryPerNode is set
	TiFlashQuerySpillRatio float64

	// TiFlashHashJoinVersion controls the hash join version in TiFlash.
	// "optimized" enables hash join v2, while "legacy" uses the original version.
	TiFlashHashJoinVersion string

	// TiDBAllowAutoRandExplicitInsert indicates whether explicit insertion on auto_random column is allowed.
	AllowAutoRandExplicitInsert bool

	// BroadcastJoinThresholdSize is used to limit the size of smaller table.
	// It's unit is bytes, if the size of small table is larger than it, we will not use bcj.
	BroadcastJoinThresholdSize int64

	// BroadcastJoinThresholdCount is used to limit the total count of smaller table.
	// If we can't estimate the size of one side of join child, we will check if its row number exceeds this limitation.
	BroadcastJoinThresholdCount int64

	// PreferBCJByExchangeDataSize indicates the method used to choose mpp broadcast join
	// false: choose mpp broadcast join by `BroadcastJoinThresholdSize` and `BroadcastJoinThresholdCount`
	// true: compare data exchange size of join and choose the smallest one
	PreferBCJByExchangeDataSize bool

	// LimitPushDownThreshold determines if push Limit or TopN down to TiKV forcibly.
	LimitPushDownThreshold int64

	// CorrelationThreshold is the guard to enable row count estimation using column order correlation.
	CorrelationThreshold float64

	// EnableCorrelationAdjustment is used to indicate if correlation adjustment is enabled.
	EnableCorrelationAdjustment bool

	// CorrelationExpFactor is used to control the heuristic approach of row count estimation when CorrelationThreshold is not met.
	CorrelationExpFactor int

	// RiskEqSkewRatio is used to control the ratio of skew that is applied to equal predicates not found in TopN/buckets.
	RiskEqSkewRatio float64

	// RiskRangeSkewRatio is used to control the ratio of skew that is applied to range predicates that fall within a single bucket or outside the histogram bucket range.
	RiskRangeSkewRatio float64

	// RiskScaleNDVSkewRatio controls the NDV estimation risk strategy for scaling NDV estimation.
	RiskScaleNDVSkewRatio float64

	// TiDBOptRiskGroupNDVSkewRatio controls the NDV estimation risk strategy for multi-column operations
	// including GROUP BY, JOIN, and DISTINCT operations.
	// When 0: uses conservative estimate (max of individual column NDVs, production default)
	// When > 0: blends conservative and exponential backoff estimates (0.1=mostly conservative, 1.0=full exponential)
	RiskGroupNDVSkewRatio float64

	// AlwaysKeepJoinKey indicates the optimizer to always keep join keys during optimization.
	// Join keys are crucial for join optimization like Join Order and Join Algorithm selection, removing
	// join keys might lead to suboptimal plans in some cases.
	AlwaysKeepJoinKey bool

	// CartesianJoinOrderThreshold controls whether to allow do Cartesian Join first in Join Reorder.
	// This variable is used as a penalty to trade off the risk and join order quality.
	// When 0: never do Cartesian Join first.
	// When > 0: allow Cartesian Join if cost(cartesian join) * threshold < cost(non cartesian join).
	CartesianJoinOrderThreshold float64

	// cpuFactor is the CPU cost of processing one expression for one row.
	cpuFactor float64
	// copCPUFactor is the CPU cost of processing one expression for one row in coprocessor.
	copCPUFactor float64
	// networkFactor is the network cost of transferring 1 byte data.
	networkFactor float64
	// ScanFactor is the IO cost of scanning 1 byte data on TiKV and TiFlash.
	scanFactor float64
	// descScanFactor is the IO cost of scanning 1 byte data on TiKV and TiFlash in desc order.
	descScanFactor float64
	// seekFactor is the IO cost of seeking the start value of a range in TiKV or TiFlash.
	seekFactor float64
	// memoryFactor is the memory cost of storing one tuple.
	memoryFactor float64
	// diskFactor is the IO cost of reading/writing one byte to temporary disk.
	diskFactor float64
	// concurrencyFactor is the CPU cost of additional one goroutine.
	concurrencyFactor float64

	// Optimizer cost model factors for each physical operator
	IndexScanCostFactor        float64
	IndexReaderCostFactor      float64
	TableReaderCostFactor      float64
	TableFullScanCostFactor    float64
	TableRangeScanCostFactor   float64
	TableRowIDScanCostFactor   float64
	TableTiFlashScanCostFactor float64
	IndexLookupCostFactor      float64
	IndexMergeCostFactor       float64
	SortCostFactor             float64
	TopNCostFactor             float64
	LimitCostFactor            float64
	StreamAggCostFactor        float64
	HashAggCostFactor          float64
	MergeJoinCostFactor        float64
	HashJoinCostFactor         float64
	IndexJoinCostFactor        float64
	SelectivityFactor          float64

	// enableForceInlineCTE is used to enable/disable force inline CTE.
	enableForceInlineCTE bool

	// CopTiFlashConcurrencyFactor is the concurrency number of computation in tiflash coprocessor.
	CopTiFlashConcurrencyFactor float64

	// CurrInsertValues is used to record current ValuesExpr's values.
	// See http://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
	CurrInsertValues chunk.Row

	// In https://github.com/pingcap/tidb/issues/14164, we can see that MySQL can enter the column that is not in the insert's SELECT's output.
	// We store the extra columns in this variable.
	CurrInsertBatchExtraCols [][]types.Datum

	// Per-connection time zones. Each client that connects has its own time zone setting, given by the session time_zone variable.
	// See https://dev.mysql.com/doc/refman/5.7/en/time-zone-support.html
	TimeZone *time.Location

	SQLMode mysql.SQLMode

	// AutoIncrementIncrement and AutoIncrementOffset indicates the autoID's start value and increment.
	AutoIncrementIncrement int

	AutoIncrementOffset int

	/* TiDB system variables */

	// SkipASCIICheck check on input value.
	SkipASCIICheck bool

	// SkipUTF8Check check on input value.
	SkipUTF8Check bool

	// DefaultCollationForUTF8MB4 indicates the default collation of UTF8MB4.
	DefaultCollationForUTF8MB4 string

	// BatchInsert indicates if we should split insert data into multiple batches.
	BatchInsert bool

	// BatchDelete indicates if we should split delete data into multiple batches.
	BatchDelete bool

	// BatchCommit indicates if we should split the transaction into multiple batches.
	BatchCommit bool

	// OptimizerSelectivityLevel defines the level of the selectivity estimation in plan.
	OptimizerSelectivityLevel int

	// OptIndexPruneThreshold defines the threshold for index pruning optimization.
	OptIndexPruneThreshold int

	// OptimizerEnableNewOnlyFullGroupByCheck enables the new only_full_group_by check which is implemented by maintaining functional dependency.
	OptimizerEnableNewOnlyFullGroupByCheck bool

	// EnableOuterJoinWithJoinReorder enables TiDB to involve the outer join into the join reorder.
	EnableOuterJoinReorder bool

	// OptimizerEnableNAAJ enables TiDB to use null-aware anti join.
	OptimizerEnableNAAJ bool

	// EnableCascadesPlanner enables the cascades planner.
	EnableCascadesPlanner bool

	// EnableWindowFunction enables the window function.
	EnableWindowFunction bool

	// EnablePipelinedWindowExec enables executing window functions in a pipelined manner.
	EnablePipelinedWindowExec bool

	// EnableNoDecorrelateInSelect enables the NO_DECORRELATE hint for subqueries in the select list.
	EnableNoDecorrelateInSelect bool

	// EnableSemiJoinRewrite enables the SEMI_JOIN_REWRITE hint for subqueries in the where clause.
	EnableSemiJoinRewrite bool

	// AllowProjectionPushDown enables pushdown projection on TiKV.
	AllowProjectionPushDown bool

	// EnableStrictDoubleTypeCheck enables table field double type check.
	EnableStrictDoubleTypeCheck bool

	// EnableVectorizedExpression  enables the vectorized expression evaluation.
	EnableVectorizedExpression bool

	// DDLReorgPriority is the operation priority of adding indices.
	DDLReorgPriority int

	// EnableAutoIncrementInGenerated is used to control whether to allow auto incremented columns in generated columns.
	EnableAutoIncrementInGenerated bool

	// EnablePointGetCache is used to cache value for point get for read only scenario.
	EnablePointGetCache bool

	// PlacementMode the placement mode we use
	//   strict: Check placement settings strictly in ddl operations
	//   ignore: Ignore all placement settings in ddl operations
	PlacementMode string

	// WaitSplitRegionFinish defines the split region behaviour is sync or async.
	WaitSplitRegionFinish bool

	// WaitSplitRegionTimeout defines the split region timeout.
	WaitSplitRegionTimeout uint64

	// EnableChunkRPC indicates whether the coprocessor request can use chunk API.
	EnableChunkRPC bool

	writeStmtBufs WriteStmtBufs

	// ConstraintCheckInPlace indicates whether to check the constraint when the SQL executing.
	ConstraintCheckInPlace bool

	// CommandValue indicates which command current session is doing.
	CommandValue uint32

	// TiDBOptJoinReorderThreshold defines the minimal number of join nodes
	// to use the greedy join reorder algorithm.
	TiDBOptJoinReorderThreshold int

	// TiDBOptJoinReorderThroughSel enables pushing selection conditions down to
	// reordered join trees when applicable.
	TiDBOptJoinReorderThroughSel bool

	// SlowQueryFile indicates which slow query log file for SLOW_QUERY table to parse.
	SlowQueryFile string

	// SlowLogRules holds the set of user-defined rules that determine whether a SQL execution should be logged in the slow log.
	// This allows flexible and fine-grained control over slow logging beyond the traditional single-threshold approach.
	SlowLogRules *slowlogrule.SessionSlowLogRules

	// EnableFastAnalyze indicates whether to take fast analyze.
	EnableFastAnalyze bool

	// TxnMode indicates should be pessimistic or optimistic.
	TxnMode string

	// lowResolutionTSO is used for reading data with low resolution TSO which is updated once every two seconds.
	// Do not use it directly, use the `UseLowResolutionTSO` method below.
	lowResolutionTSO bool

	// MaxExecutionTime is the timeout for select statement, in milliseconds.
	// If the value is 0, timeouts are not enabled.
	// See https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_max_execution_time
	MaxExecutionTime uint64

	// LoadBindingTimeout is the timeout for loading the bind info.
	LoadBindingTimeout uint64

	// TiKVClientReadTimeout is the timeout for readonly kv request in milliseconds, 0 means using default value
	// See https://github.com/pingcap/tidb/blob/7105505a78fc886c33258caa5813baf197b15247/docs/design/2023-06-30-configurable-kv-timeout.md?plain=1#L14-L15
	TiKVClientReadTimeout uint64

	// SQLKiller is a flag to indicate that this query is killed.
	SQLKiller sqlkiller.SQLKiller

	// ConnectionStatus indicates current connection status.
	ConnectionStatus int32

	// ConnectionInfo indicates current connection info used by current session.
	ConnectionInfo *ConnectionInfo

	// NoopFuncsMode allows OFF/ON/WARN values as 0/1/2.
	NoopFuncsMode int

	// StartTime is the start time of the last query. It's set after the query is parsed and before the query is compiled.
	StartTime time.Time

	// DurationParse is the duration of parsing SQL string to AST of the last query.
	DurationParse time.Duration

	// DurationCompile is the duration of compiling AST to execution plan of the last query.
	DurationCompile time.Duration

	// RewritePhaseInfo records all information about the rewriting phase.
	RewritePhaseInfo

	// DurationOptimizer aggregates timing metrics used by the optimizer.
	DurationOptimizer struct {
		Total            time.Duration // total time spent in query optimization
		BindingMatch     time.Duration // time spent matching plan bindings
		StatsSyncWait    time.Duration // time spent waiting for stats load to complete
		LogicalOpt       time.Duration // time spent in logical optimization
		PhysicalOpt      time.Duration // time spent in physical optimization
		StatsDerive      time.Duration // time spent deriving/estimating statistics
		TiFlashInfoFetch time.Duration // time spent fetching TiFlash replica information
	}

	// DurationWaitTS is the duration of waiting for a snapshot TS
	DurationWaitTS time.Duration

	// PrevStmt is used to store the previous executed statement in the current session.
	PrevStmt *LazyStmtText

	// prevStmtDigest is used to store the digest of the previous statement in the current session.
	prevStmtDigest string

	// AllowRemoveAutoInc indicates whether a user can drop the auto_increment column attribute or not.
	AllowRemoveAutoInc bool

	// UsePlanBaselines indicates whether we will use plan baselines to adjust plan.
	UsePlanBaselines bool

	// EvolvePlanBaselines indicates whether we will evolve the plan baselines.
	EvolvePlanBaselines bool

	// EnableExtendedStats indicates whether we enable the extended statistics feature.
	EnableExtendedStats bool

	// Unexported fields should be accessed and set through interfaces like GetReplicaRead() and SetReplicaRead().

	// allowInSubqToJoinAndAgg can be set to false to forbid rewriting the semi join to inner join with agg.
	allowInSubqToJoinAndAgg bool

	// preferRangeScan allows optimizer to always prefer range scan over table scan.
	preferRangeScan bool

	// EnableIndexMerge enables the generation of IndexMergePath.
	enableIndexMerge bool

	// replicaRead is used for reading data from replicas, only follower is supported at this time.
	replicaRead kv.ReplicaReadType
	// ReplicaClosestReadThreshold is the minimum response body size that a cop request should be sent to the closest replica.
	// this variable only take effect when `tidb_follower_read` = 'closest-adaptive'
	ReplicaClosestReadThreshold int64

	// IsolationReadEngines is used to isolation read, tidb only read from the stores whose engine type is in the engines.
	IsolationReadEngines map[kv.StoreType]struct{}

	mppVersion kv.MppVersion

	mppExchangeCompressionMode vardef.ExchangeCompressionMode

	PlannerSelectBlockAsName atomic.Pointer[[]ast.HintTable]

	// LockWaitTimeout is the duration waiting for pessimistic lock in milliseconds
	LockWaitTimeout int64

	// MetricSchemaStep indicates the step when query metric schema.
	MetricSchemaStep int64

	// CDCWriteSource indicates the following data is written by TiCDC if it is not 0.
	CDCWriteSource uint64

	// MetricSchemaRangeDuration indicates the step when query metric schema.
	MetricSchemaRangeDuration int64

	// Some data of cluster-level memory tables will be retrieved many times in different inspection rules,
	// and the cost of retrieving some data is expensive. We use the `TableSnapshot` to cache those data
	// and obtain them lazily, and provide a consistent view of inspection tables for each inspection rules.
	// All cached snapshots will be released at the end of retrieving
	InspectionTableCache map[string]TableSnapshot

	// RowEncoder is reused in session for encode row data.
	RowEncoder rowcodec.Encoder

	// SequenceState cache all sequence's latest value accessed by lastval() builtins. It's a session scoped
	// variable, and all public methods of SequenceState are currently-safe.
	SequenceState *SequenceState

	// WindowingUseHighPrecision determines whether to compute window operations without loss of precision.
	// see https://dev.mysql.com/doc/refman/8.0/en/window-function-optimization.html for more details.
	WindowingUseHighPrecision bool

	// FoundInPlanCache indicates whether this statement was found in plan cache.
	FoundInPlanCache bool
	// PrevFoundInPlanCache indicates whether the last statement was found in plan cache.
	PrevFoundInPlanCache bool

	// FoundInBinding indicates whether the execution plan is matched with the hints in the binding.
	FoundInBinding bool
	// PrevFoundInBinding indicates whether the last execution plan is matched with the hints in the binding.
	PrevFoundInBinding bool

	// OptimizerUseInvisibleIndexes indicates whether optimizer can use invisible index
	OptimizerUseInvisibleIndexes bool

	// SelectLimit limits the max counts of select statement's output
	SelectLimit uint64

	// EnableClusteredIndex indicates whether to enable clustered index when creating a new table.
	EnableClusteredIndex vardef.ClusteredIndexDefMode

	// EnableParallelApply indicates that whether to use parallel apply.
	EnableParallelApply bool

	// EnableRedactLog indicates that whether redact log. Possible values are 'OFF', 'ON', 'MARKER'.
	EnableRedactLog string

	// ShardAllocateStep indicates the max size of continuous rowid shard in one transaction.
	ShardAllocateStep int64

	// LastTxnInfo keeps track the info of last committed transaction.
	LastTxnInfo string

	// LastQueryInfo keeps track the info of last query.
	LastQueryInfo sessionstates.QueryInfo

	// LastDDLInfo keeps track the info of last DDL.
	LastDDLInfo sessionstates.LastDDLInfo

	// PartitionPruneMode indicates how and when to prune partitions.
	PartitionPruneMode atomic2.String

	// TxnScope indicates the scope of the transactions. It should be `global` or equal to the value of key `zone` in config.Labels.
	TxnScope kv.TxnScopeVar

	// EnabledRateLimitAction indicates whether enabled ratelimit action during coprocessor
	EnabledRateLimitAction bool

	// EnableAsyncCommit indicates whether to enable the async commit feature.
	EnableAsyncCommit bool

	// Enable1PC indicates whether to enable the one-phase commit feature.
	Enable1PC bool

	// GuaranteeLinearizability indicates whether to guarantee linearizability
	GuaranteeLinearizability bool

	// AnalyzeVersion indicates how TiDB collect and use analyzed statistics.
	AnalyzeVersion int

	// DisableHashJoin indicates whether to disable hash join.
	DisableHashJoin bool

	// UseHashJoinV2 indicates whether to use hash join v2.
	UseHashJoinV2 bool

	// EnableHistoricalStats indicates whether to enable historical statistics.
	EnableHistoricalStats bool

	// EnableIndexMergeJoin indicates whether to enable index merge join.
	EnableIndexMergeJoin bool

	// TrackAggregateMemoryUsage indicates whether to track the memory usage of aggregate function.
	TrackAggregateMemoryUsage bool

	// TiDBEnableExchangePartition indicates whether to enable exchange partition
	TiDBEnableExchangePartition bool

	// AllowFallbackToTiKV indicates the engine types whose unavailability triggers fallback to TiKV.
	// Now we only support TiFlash.
	AllowFallbackToTiKV map[kv.StoreType]struct{}

	// CTEMaxRecursionDepth indicates The common table expression (CTE) maximum recursion depth.
	// see https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_cte_max_recursion_depth
	CTEMaxRecursionDepth int

	// The temporary table size threshold, which is different from MySQL. See https://github.com/pingcap/tidb/issues/28691.
	TMPTableSize int64

	// EnableStableResultMode if stabilize query results.
	EnableStableResultMode bool

	// EnablePseudoForOutdatedStats if using pseudo for outdated stats
	EnablePseudoForOutdatedStats bool

	// RegardNULLAsPoint if regard NULL as Point
	RegardNULLAsPoint bool

	// LocalTemporaryTables is *infoschema.LocalTemporaryTables, use interface to avoid circle dependency.
	// It's nil if there is no local temporary table.
	LocalTemporaryTables any

	// TemporaryTableData stores committed kv values for temporary table for current session.
	TemporaryTableData TemporaryTableData

	// MPPStoreFailTTL indicates the duration that protect TiDB from sending task to a new recovered TiFlash.
	MPPStoreFailTTL string

	// ReadStaleness indicates the staleness duration for the following query
	ReadStaleness time.Duration

	// cachedStmtCtx is used to optimize the object allocation.
	cachedStmtCtx [2]stmtctx.StatementContext

	// Rng stores the rand_seed1 and rand_seed2 for Rand() function
	Rng *mathutil.MysqlRng

	// EnablePaging indicates whether enable paging in coprocessor requests.
	EnablePaging bool

	// EnableLegacyInstanceScope says if SET SESSION can be used to set an instance
	// scope variable. The default is TRUE.
	EnableLegacyInstanceScope bool

	// ReadConsistency indicates the read consistency requirement.
	ReadConsistency ReadConsistencyLevel

	// StatsLoadSyncWait indicates how long to wait for stats load before timeout.
	StatsLoadSyncWait atomic.Int64

	// EnableParallelHashaggSpill indicates if parallel hash agg could spill.
	EnableParallelHashaggSpill bool

	// SysdateIsNow indicates whether Sysdate is an alias of Now function
	SysdateIsNow bool
	// EnableMutationChecker indicates whether to check data consistency for mutations
	EnableMutationChecker bool
	// AssertionLevel controls how strict the assertions on data mutations should be.
	AssertionLevel AssertionLevel
	// IgnorePreparedCacheCloseStmt controls if ignore the close-stmt command for prepared statement.
	IgnorePreparedCacheCloseStmt bool
	// CostModelVersion is a internal switch to indicates the Cost Model Version.
	CostModelVersion int
	// IndexJoinDoubleReadPenaltyCostRate indicates whether to add some penalty cost to IndexJoin and how much of it.
	IndexJoinDoubleReadPenaltyCostRate float64

	// BatchPendingTiFlashCount shows the threshold of pending TiFlash tables when batch adding.
	BatchPendingTiFlashCount int
	// RcWriteCheckTS indicates whether some special write statements don't get latest tso from PD at RC
	RcWriteCheckTS bool
	// RemoveOrderbyInSubquery indicates whether to remove ORDER BY in subquery.
	RemoveOrderbyInSubquery bool
	// NonTransactionalIgnoreError indicates whether to ignore error in non-transactional statements.
	// When set to false, returns immediately when it meets the first error.
	NonTransactionalIgnoreError bool

	// MaxAllowedPacket indicates the maximum size of a packet for the MySQL protocol.
	MaxAllowedPacket uint64

	// TiFlash related optimization, only for MPP.
	TiFlashFineGrainedShuffleStreamCount int64
	TiFlashFineGrainedShuffleBatchSize   uint64

	// RequestSourceType is the type of inner request.
	RequestSourceType string
	// ExplicitRequestSourceType is the type of origin external request.
	ExplicitRequestSourceType string

	// MemoryDebugModeMinHeapInUse indicated the minimum heapInUse threshold that triggers the memoryDebugMode.
	MemoryDebugModeMinHeapInUse int64
	// MemoryDebugModeAlarmRatio indicated the allowable bias ratio of memory tracking accuracy check.
	// When `(memory trakced by tidb) * (1+MemoryDebugModeAlarmRatio) < actual heapInUse`, an alarm log will be recorded.
	MemoryDebugModeAlarmRatio int64

	// EnableAnalyzeSnapshot indicates whether to read data on snapshot when collecting statistics.
	// When it is false, ANALYZE reads the latest data.
	// When it is true, ANALYZE reads data on the snapshot at the beginning of ANALYZE.
	EnableAnalyzeSnapshot bool

	// EnableDDLAnalyze is a sysVar to indicate create index or reorg index with embedded analyze.
	EnableDDLAnalyze bool

	// EnableDDLAnalyzeExecOpt is a internal flag to notify internal session whether we do ddl analyze.
	// It is not controlled by user behavior, and is always default off.
	EnableDDLAnalyzeExecOpt bool

	// DefaultStrMatchSelectivity adjust the estimation strategy for string matching expressions that can't be estimated by building into range.
	// when > 0: it's the selectivity for the expression.
	// when = 0: try to use TopN to evaluate the like expression to estimate the selectivity.
	DefaultStrMatchSelectivity float64

	// TiFlashFastScan indicates whether use fast scan in TiFlash
	TiFlashFastScan bool

	// PrimaryKeyRequired indicates if sql_require_primary_key sysvar is set
	PrimaryKeyRequired bool

	// EnablePreparedPlanCache indicates whether to enable prepared plan cache.
	EnablePreparedPlanCache bool

	// PreparedPlanCacheSize controls the size of prepared plan cache.
	PreparedPlanCacheSize uint64

	// PreparedPlanCacheMonitor indicates whether to enable prepared plan cache monitor.
	EnablePreparedPlanCacheMemoryMonitor bool

	// EnablePlanCacheForParamLimit controls whether the prepare statement with parameterized limit can be cached
	EnablePlanCacheForParamLimit bool

	// EnablePlanCacheForSubquery controls whether the prepare statement with sub query can be cached
	EnablePlanCacheForSubquery bool

	// EnableNonPreparedPlanCache indicates whether to enable non-prepared plan cache.
	EnableNonPreparedPlanCache bool

	// EnableNonPreparedPlanCacheForDML indicates whether to enable non-prepared plan cache for DML statements.
	EnableNonPreparedPlanCacheForDML bool

	// EnableFuzzyBinding indicates whether to enable fuzzy binding.
	EnableFuzzyBinding bool

	// PlanCacheInvalidationOnFreshStats controls if plan cache will be invalidated automatically when
	// related stats are analyzed after the plan cache is generated.
	PlanCacheInvalidationOnFreshStats bool

	// NonPreparedPlanCacheSize controls the size of non-prepared plan cache.
	NonPreparedPlanCacheSize uint64

	// PlanCacheMaxPlanSize controls the maximum size of a plan that can be cached.
	PlanCacheMaxPlanSize uint64

	// SessionPlanCacheSize controls the size of session plan cache.
	SessionPlanCacheSize uint64

	// ConstraintCheckInPlacePessimistic controls whether to skip the locking of some keys in pessimistic transactions.
	// Postpone the conflict check and constraint check to prewrite or later pessimistic locking requests.
	ConstraintCheckInPlacePessimistic bool

	// EnableTiFlashReadForWriteStmt indicates whether to enable TiFlash to read for write statements.
	EnableTiFlashReadForWriteStmt bool

	// EnableUnsafeSubstitute indicates whether to enable generate column takes unsafe substitute.
	EnableUnsafeSubstitute bool

	// ForeignKeyChecks indicates whether to enable foreign key constraint check.
	ForeignKeyChecks bool

	// ForeignKeyCheckInSharedLock indicates whether to use shared lock for foreign key check.
	ForeignKeyCheckInSharedLock bool

	// RangeMaxSize is the max memory limit for ranges. When the optimizer estimates that the memory usage of complete
	// ranges would exceed the limit, it chooses less accurate ranges such as full range. 0 indicates that there is no
	// memory limit for ranges.
	RangeMaxSize int64

	// LastPlanReplayerToken indicates the last plan replayer token
	LastPlanReplayerToken string

	// InPlanReplayer means we are now executing a statement for a PLAN REPLAYER SQL.
	// Note that PLAN REPLAYER CAPTURE is not included here.
	InPlanReplayer bool

	// AnalyzePartitionConcurrency indicates concurrency for partitions in Analyze
	AnalyzePartitionConcurrency int
	// AnalyzePartitionMergeConcurrency indicates concurrency for merging partition stats
	AnalyzePartitionMergeConcurrency int

	// EnableAsyncMergeGlobalStats indicates whether to enable async merge global stats
	EnableAsyncMergeGlobalStats bool

	// EnableExternalTSRead indicates whether to enable read through external ts
	EnableExternalTSRead bool

	HookContext

	// MemTracker indicates the memory tracker of current session.
	MemTracker *memory.Tracker
	// MemDBDBFootprint tracks the memory footprint of memdb, and is attached to `MemTracker`
	MemDBFootprint *memory.Tracker
	DiskTracker    *memory.Tracker

	// OptPrefixIndexSingleScan indicates whether to do some optimizations to avoid double scan for prefix index.
	// When set to true, `col is (not) null`(`col` is index prefix column) is regarded as index filter rather than table filter.
	OptPrefixIndexSingleScan bool
	// OptPartialOrderedIndexForTopN indicates whether to enable partial ordered index optimization for TOPN queries.
	// Valid values: "DISABLE" (no optimization), "COST" (enable optimization with cost-based selection)
	OptPartialOrderedIndexForTopN string

	// chunkPool Several chunks and columns are cached
	chunkPool chunk.Allocator
	// EnableReuseChunk indicates  request chunk whether use chunk alloc
	EnableReuseChunk bool

	// EnableAdvancedJoinHint indicates whether the join method hint is compatible with join order hint.
	EnableAdvancedJoinHint bool

	// preuseChunkAlloc indicates whether pre statement use chunk alloc
	// like select @@last_sql_use_alloc
	preUseChunkAlloc bool

	// EnablePlanReplayerCapture indicates whether enabled plan replayer capture
	EnablePlanReplayerCapture bool

	// EnablePlanReplayedContinuesCapture indicates whether enabled plan replayer continues capture
	EnablePlanReplayedContinuesCapture bool

	// PlanReplayerFinishedTaskKey used to record the finished plan replayer task key in order not to record the
	// duplicate task in plan replayer continues capture
	PlanReplayerFinishedTaskKey map[replayer.PlanReplayerTaskKey]struct{}

	// StoreBatchSize indicates the batch size limit of store batch, set this field to 0 to disable store batch.
	StoreBatchSize int

	// shardGenerator indicates to generate shard for row id.
	shardGenerator *RowIDShardGenerator

	// Resource group name
	// NOTE: all statement relate operation should use StmtCtx.ResourceGroupName instead.
	// NOTE: please don't change it directly. Use `SetResourceGroupName`, because it'll need to inc/dec the metrics
	ResourceGroupName string

	// PessimisticTransactionFairLocking controls whether fair locking for pessimistic transaction
	// is enabled.
	PessimisticTransactionFairLocking bool

	// EnableINLJoinInnerMultiPattern indicates whether enable multi pattern for index join inner side
	// For now it is not public to user
	EnableINLJoinInnerMultiPattern bool

	// EnhanceIndexJoinBuildV2 indicates whether to enhance index join build.
	EnhanceIndexJoinBuildV2 bool

	// Enable late materialization: push down some selection condition to tablescan.
	EnableLateMaterialization bool

	// EnableRowLevelChecksum indicates whether row level checksum is enabled.
	EnableRowLevelChecksum bool

	// TiFlashComputeDispatchPolicy indicates how to dipatch task to tiflash_compute nodes.
	// Only for disaggregated-tiflash mode.
	TiFlashComputeDispatchPolicy tiflashcompute.DispatchPolicy

	// SlowTxnThreshold is the threshold of slow transaction logs
	SlowTxnThreshold uint64

	// LoadBasedReplicaReadThreshold is the threshold for the estimated wait duration of a store.
	// If exceeding the threshold, try other stores using replica read.
	LoadBasedReplicaReadThreshold time.Duration

	// OptOrderingIdxSelThresh is the threshold for optimizer to consider the ordering index.
	// If there exists an index whose estimated selectivity is smaller than this threshold, the optimizer won't
	// use the ExpectedCnt to adjust the estimated row count for index scan.
	OptOrderingIdxSelThresh float64

	// OptOrderingIdxSelRatio is the ratio for optimizer to determine when qualified rows from filtering outside
	// of the index will be found during the scan of an ordering index.
	// If all filtering is applied as matching on the ordering index, this ratio will have no impact.
	// Value < 0 disables this enhancement.
	// Value 0 will estimate row(s) found immediately.
	// 0 > value <= 1 applies that percentage as the estimate when rows are found. For example 0.1 = 10%.
	OptOrderingIdxSelRatio float64

	// RecordRelevantOptVarsAndFixes indicates whether to record optimizer variables/fixes relevant to this query.
	RecordRelevantOptVarsAndFixes bool

	// RelevantOptVars is a map of relevant optimizer variables to be recorded.
	RelevantOptVars map[string]struct{}

	// RelevantOptFixes is a map of relevant optimizer fixes to be recorded.
	RelevantOptFixes map[uint64]struct{}

	// EnableMPPSharedCTEExecution indicates whether we enable the shared CTE execution strategy on MPP side.
	EnableMPPSharedCTEExecution bool

	// OptimizerFixControl control some details of the optimizer behavior through the tidb_opt_fix_control variable.
	OptimizerFixControl map[uint64]string

	// FastCheckTable is used to control whether fast check table is enabled.
	FastCheckTable bool

	// HypoIndexes are for the Index Advisor.
	HypoIndexes map[string]map[string]map[string]*model.IndexInfo // dbName -> tblName -> idxName -> idxInfo

	// TiFlashReplicaRead indicates the policy of TiFlash node selection when the query needs the TiFlash engine.
	TiFlashReplicaRead tiflash.ReplicaRead

	// HypoTiFlashReplicas are for the Index Advisor.
	HypoTiFlashReplicas map[string]map[string]struct{} // dbName -> tblName -> whether to have replicas

	// Runtime Filter Group
	// Runtime filter type: only support IN or MIN_MAX now.
	// Runtime filter type can take multiple values at the same time.
	runtimeFilterTypes []RuntimeFilterType
	// Runtime filter mode: only support OFF, LOCAL now
	runtimeFilterMode RuntimeFilterMode

	// Whether to lock duplicate keys in INSERT IGNORE and REPLACE statements,
	// or unchanged unique keys in UPDATE statements, see PR #42210 and #42713
	LockUnchangedKeys bool

	// AnalyzeSkipColumnTypes indicates the column types whose statistics would not be collected when executing the ANALYZE command.
	AnalyzeSkipColumnTypes map[string]struct{}

	// SkipMissingPartitionStats controls how to handle missing partition stats when merging partition stats to global stats.
	// When set to true, skip missing partition stats and continue to merge other partition stats to global stats.
	// When set to false, give up merging partition stats to global stats.
	SkipMissingPartitionStats bool

	// SessionAlias is the identifier of the session
	SessionAlias string

	// OptObjective indicates whether the optimizer should be more stable, predictable or more aggressive.
	// For now, the possible values and corresponding behaviors are:
	// OptObjectiveModerate: The default value. The optimizer considers the real-time stats (real-time row count, modify count).
	// OptObjectiveDeterminate: The optimizer doesn't consider the real-time stats.
	OptObjective string

	CompressionAlgorithm int
	CompressionLevel     int

	// TxnEntrySizeLimit indicates indicates the max size of a entry in membuf. The default limit (from config) will be
	// overwritten if this value is not 0.
	TxnEntrySizeLimit uint64

	// DivPrecisionIncrement indicates the number of digits by which to increase the scale of the result
	// of division operations performed with the / operator.
	DivPrecisionIncrement int

	// allowed when tikv disk full happened.
	DiskFullOpt kvrpcpb.DiskFullOpt

	// GroupConcatMaxLen represents the maximum length of the result of GROUP_CONCAT.
	GroupConcatMaxLen uint64

	// TiFlashPreAggMode indicates the policy of pre aggregation.
	TiFlashPreAggMode string

	// EnableLazyCursorFetch defines whether to enable the lazy cursor fetch.
	EnableLazyCursorFetch bool

	// SharedLockPromotion indicates whether the `select for lock` statements would be executed as the
	// `select for update` statements which do acquire pessimsitic locks.
	SharedLockPromotion bool

	// ScatterRegion will scatter the regions for DDLs when it is "table" or "global", "" indicates not trigger scatter.
	ScatterRegion string

	// CacheStmtExecInfo is a cache for the statement execution information, used to reduce the overhead of memory allocation.
	CacheStmtExecInfo *stmtsummary.StmtExecInfo

	// BulkDMLEnabled indicates whether to enable bulk DML in pipelined mode.
	BulkDMLEnabled bool

	// InternalSQLScanUserTable indicates whether to use user table for internal SQL. it will be used by TTL scan
	InternalSQLScanUserTable bool

	// MemArbitrator represents the properties to be controlled by the memory arbitrator.
	MemArbitrator struct {
		WaitAverse    MemArbitratorWaitAverseMode
		QueryReserved int64
	}

	// InPacketBytes records the total incoming packet bytes from clients for current session.
	InPacketBytes atomic.Uint64

	// OutPacketBytes records the total outcoming packet bytes to clients for current session.
	OutPacketBytes atomic.Uint64

	// IndexLookUpPushDownPolicy indicates the policy of index look up push down.
	IndexLookUpPushDownPolicy string
}

// ResetRelevantOptVarsAndFixes resets the relevant optimizer variables and fixes.
func (s *SessionVars) ResetRelevantOptVarsAndFixes(record bool) {
	s.RecordRelevantOptVarsAndFixes = record
	s.RelevantOptVars = nil
	s.RelevantOptFixes = nil
}

// RecordRelevantOptVar records the optimizer variable that is relevant to the current query.
func (s *SessionVars) RecordRelevantOptVar(varName string) {
	if !s.RecordRelevantOptVarsAndFixes {
		return
	}
	if s.RelevantOptVars == nil {
		s.RelevantOptVars = make(map[string]struct{})
	}
	s.RelevantOptVars[varName] = struct{}{}
}

// RecordRelevantOptFix records the optimizer fix that is relevant to the current query.
func (s *SessionVars) RecordRelevantOptFix(fixID uint64) {
	if !s.RecordRelevantOptVarsAndFixes {
		return
	}
	if s.RelevantOptFixes == nil {
		s.RelevantOptFixes = make(map[uint64]struct{})
	}
	s.RelevantOptFixes[fixID] = struct{}{}
}

// GetSessionVars implements the `SessionVarsProvider` interface.
func (s *SessionVars) GetSessionVars() *SessionVars {
	return s
}

// GetOptimizerFixControlMap returns the specified value of the optimizer fix control.
func (s *SessionVars) GetOptimizerFixControlMap() map[uint64]string {
	return s.OptimizerFixControl
}

// planReplayerSessionFinishedTaskKeyLen is used to control the max size for the finished plan replayer task key in session
// in order to control the used memory
const planReplayerSessionFinishedTaskKeyLen = 128

// AddPlanReplayerFinishedTaskKey record finished task key in session
func (s *SessionVars) AddPlanReplayerFinishedTaskKey(key replayer.PlanReplayerTaskKey) {
	if len(s.PlanReplayerFinishedTaskKey) >= planReplayerSessionFinishedTaskKeyLen {
		s.initializePlanReplayerFinishedTaskKey()
	}
	s.PlanReplayerFinishedTaskKey[key] = struct{}{}
}

func (s *SessionVars) initializePlanReplayerFinishedTaskKey() {
	s.PlanReplayerFinishedTaskKey = make(map[replayer.PlanReplayerTaskKey]struct{}, planReplayerSessionFinishedTaskKeyLen)
}

// CheckPlanReplayerFinishedTaskKey check whether the key exists
func (s *SessionVars) CheckPlanReplayerFinishedTaskKey(key replayer.PlanReplayerTaskKey) bool {
	if s.PlanReplayerFinishedTaskKey == nil {
		s.initializePlanReplayerFinishedTaskKey()
		return false
	}
	_, ok := s.PlanReplayerFinishedTaskKey[key]
	return ok
}

// IsPlanReplayerCaptureEnabled indicates whether capture or continues capture enabled
func (s *SessionVars) IsPlanReplayerCaptureEnabled() bool {
	return s.EnablePlanReplayerCapture || s.EnablePlanReplayedContinuesCapture
}

// GetChunkAllocator returns a valid chunk allocator.
func (s *SessionVars) GetChunkAllocator() chunk.Allocator {
	if s.chunkPool == nil {
		return chunk.NewEmptyAllocator()
	}

	return s.chunkPool
}

// ExchangeChunkStatus give the status to preUseChunkAlloc
func (s *SessionVars) ExchangeChunkStatus() {
	s.preUseChunkAlloc = s.GetUseChunkAlloc()
}

// GetUseChunkAlloc return useChunkAlloc status
func (s *SessionVars) GetUseChunkAlloc() bool {
	return s.StmtCtx.GetUseChunkAllocStatus()
}

// SetAlloc Attempt to set the buffer pool address
func (s *SessionVars) SetAlloc(alloc chunk.Allocator) {
	if !s.EnableReuseChunk {
		s.chunkPool = nil
		return
	}
	if alloc == nil {
		s.chunkPool = nil
		return
	}
	s.chunkPool = chunk.NewReuseHookAllocator(
		chunk.NewSyncAllocator(alloc),
		func() {
			s.StmtCtx.SetUseChunkAlloc()
		},
	)
}

// IsAllocValid check if chunk reuse is enable or chunkPool is inused.
func (s *SessionVars) IsAllocValid() bool {
	if !s.EnableReuseChunk {
		return false
	}
	return s.chunkPool != nil
}

// ClearAlloc indicates stop reuse chunk. If `hasErr` is true, it'll also recreate the `alloc` in parameter.
func (s *SessionVars) ClearAlloc(alloc *chunk.Allocator, hasErr bool) {
	if !hasErr {
		s.chunkPool = nil
		return
	}

	s.chunkPool = nil
	*alloc = chunk.NewAllocator()
}

// GetPreparedStmtByName returns the prepared statement specified by stmtName.
func (s *SessionVars) GetPreparedStmtByName(stmtName string) (any, error) {
	stmtID, ok := s.PreparedStmtNameToID[stmtName]
	if !ok {
		return nil, plannererrors.ErrStmtNotFound
	}
	return s.GetPreparedStmtByID(stmtID)
}

// GetPreparedStmtByID returns the prepared statement specified by stmtID.
func (s *SessionVars) GetPreparedStmtByID(stmtID uint32) (any, error) {
	stmt, ok := s.PreparedStmts[stmtID]
	if !ok {
		return nil, plannererrors.ErrStmtNotFound
	}
	return stmt, nil
}

// InitStatementContext initializes a StatementContext, the object is reused to reduce allocation.
func (s *SessionVars) InitStatementContext() *stmtctx.StatementContext {
	sc := &s.cachedStmtCtx[0]
	if sc == s.StmtCtx {
		sc = &s.cachedStmtCtx[1]
	}
	if s.RefCountOfStmtCtx.TryFreeze() {
		succ := sc.Reset()
		s.RefCountOfStmtCtx.UnFreeze()
		if !succ {
			sc = stmtctx.NewStmtCtx()
		}
	} else {
		sc = stmtctx.NewStmtCtx()
	}
	return sc
}

// IsMPPAllowed returns whether mpp execution is allowed.
func (s *SessionVars) IsMPPAllowed() bool {
	return s.allowMPPExecution
}

// IsTiFlashCopBanned returns whether cop execution is allowed.
func (s *SessionVars) IsTiFlashCopBanned() bool {
	return !s.allowTiFlashCop
}

// IsMPPEnforced returns whether mpp execution is enforced.
func (s *SessionVars) IsMPPEnforced() bool {
	return s.allowMPPExecution && s.enforceMPPExecution
}

// ChooseMppVersion indicates the mpp-version used to build mpp plan, if mpp-version is unspecified, use the latest version.
func (s *SessionVars) ChooseMppVersion() kv.MppVersion {
	if s.mppVersion == kv.MppVersionUnspecified {
		return kv.GetNewestMppVersion()
	}
	return s.mppVersion
}

// ChooseMppExchangeCompressionMode indicates the data compression method in mpp exchange operator
func (s *SessionVars) ChooseMppExchangeCompressionMode() vardef.ExchangeCompressionMode {
	if s.mppExchangeCompressionMode == vardef.ExchangeCompressionModeUnspecified {
		// If unspecified, use recommended mode
		return vardef.RecommendedExchangeCompressionMode
	}
	return s.mppExchangeCompressionMode
}

// RaiseWarningWhenMPPEnforced will raise a warning when mpp mode is enforced and executing explain statement.
// TODO: Confirm whether this function will be inlined and
// omit the overhead of string construction when calling with false condition.
func (s *SessionVars) RaiseWarningWhenMPPEnforced(warning string) {
	if !s.IsMPPEnforced() {
		return
	}
	if s.StmtCtx.InExplainStmt {
		s.StmtCtx.AppendWarning(errors.NewNoStackError(warning))
	} else {
		s.StmtCtx.AppendExtraWarning(errors.NewNoStackError(warning))
	}
}

// CheckAndGetTxnScope will return the transaction scope we should use in the current session.
func (s *SessionVars) CheckAndGetTxnScope() string {
	if s.InRestrictedSQL || !vardef.EnableLocalTxn.Load() {
		return kv.GlobalTxnScope
	}
	if s.TxnScope.GetVarValue() == kv.LocalTxnScope {
		return s.TxnScope.GetTxnScope()
	}
	return kv.GlobalTxnScope
}

// IsDynamicPartitionPruneEnabled indicates whether dynamic partition prune enabled
// Note that: IsDynamicPartitionPruneEnabled only indicates whether dynamic partition prune mode is enabled according to
// session variable, it isn't guaranteed to be used during query due to other conditions checking.
func (s *SessionVars) IsDynamicPartitionPruneEnabled() bool {
	return PartitionPruneMode(s.PartitionPruneMode.Load()) == Dynamic
}

// IsRowLevelChecksumEnabled indicates whether row level checksum is enabled for current session, that is
// tidb_enable_row_level_checksum is on and tidb_row_format_version is 2 and it's not a internal session.
func (s *SessionVars) IsRowLevelChecksumEnabled() bool {
	return s.EnableRowLevelChecksum && s.RowEncoder.Enable && !s.InRestrictedSQL
}

// BuildParserConfig generate parser.ParserConfig for initial parser
func (s *SessionVars) BuildParserConfig() parser.ParserConfig {
	return parser.ParserConfig{
		EnableWindowFunction:        s.EnableWindowFunction,
		EnableStrictDoubleTypeCheck: s.EnableStrictDoubleTypeCheck,
		SkipPositionRecording:       true,
	}
}

// AllocNewPlanID alloc new ID
func (s *SessionVars) AllocNewPlanID() int {
	return int(s.PlanID.Add(1))
}

// GetTotalCostDuration returns the total cost duration of the last statement in the current session.
func (s *SessionVars) GetTotalCostDuration() time.Duration {
	return time.Since(s.StartTime) + s.DurationParse
}

// GetExecuteDuration returns the execute duration of the last statement in the current session.
func (s *SessionVars) GetExecuteDuration() time.Duration {
	return time.Since(s.StartTime) - s.DurationCompile
}

// IsPartialOrderedIndexForTopNEnabled indicates whether the partial ordered index optimization for TOPN queries is enabled.
// TODO: consider more options other than "COST" in the future.
func (s *SessionVars) IsPartialOrderedIndexForTopNEnabled() bool {
	return s.OptPartialOrderedIndexForTopN == "COST"
}
