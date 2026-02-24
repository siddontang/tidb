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
	"net"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/infoschema/issyncer/mdldef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/oracle"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"
)

// TableDelta stands for the changed count for one table or partition.
type TableDelta struct {
	Delta    int64
	Count    int64
	InitTime time.Time // InitTime is the time that this delta is generated.
}

// MergeFrom merges another delta into the receiver and keeps the earliest InitTime.
func (td *TableDelta) MergeFrom(incoming TableDelta) {
	td.Delta += incoming.Delta
	td.Count += incoming.Count
	if td.InitTime.IsZero() {
		td.InitTime = incoming.InitTime
	} else if !incoming.InitTime.IsZero() && incoming.InitTime.Before(td.InitTime) { // This can happen when merges arrive out of order (e.g., overlapping dump/merge runs).
		td.InitTime = incoming.InitTime
	}
}

// Clone returns a cloned TableDelta.
func (td TableDelta) Clone() TableDelta {
	return TableDelta{
		Delta:    td.Delta,
		Count:    td.Count,
		InitTime: td.InitTime,
	}
}

// Concurrency defines concurrency values.
type Concurrency struct {
	// indexLookupConcurrency is the number of concurrent index lookup worker.
	// indexLookupConcurrency is deprecated, use ExecutorConcurrency instead.
	indexLookupConcurrency int

	// indexLookupJoinConcurrency is the number of concurrent index lookup join inner worker.
	// indexLookupJoinConcurrency is deprecated, use ExecutorConcurrency instead.
	indexLookupJoinConcurrency int

	// distSQLScanConcurrency is the number of concurrent dist SQL scan worker.
	distSQLScanConcurrency int

	// analyzeDistSQLScanConcurrency is the number of concurrent dist SQL scan worker when to analyze.
	analyzeDistSQLScanConcurrency int

	// hashJoinConcurrency is the number of concurrent hash join outer worker.
	// hashJoinConcurrency is deprecated, use ExecutorConcurrency instead.
	hashJoinConcurrency int

	// projectionConcurrency is the number of concurrent projection worker.
	// projectionConcurrency is deprecated, use ExecutorConcurrency instead.
	projectionConcurrency int

	// hashAggPartialConcurrency is the number of concurrent hash aggregation partial worker.
	// hashAggPartialConcurrency is deprecated, use ExecutorConcurrency instead.
	hashAggPartialConcurrency int

	// hashAggFinalConcurrency is the number of concurrent hash aggregation final worker.
	// hashAggFinalConcurrency is deprecated, use ExecutorConcurrency instead.
	hashAggFinalConcurrency int

	// windowConcurrency is the number of concurrent window worker.
	// windowConcurrency is deprecated, use ExecutorConcurrency instead.
	windowConcurrency int

	// mergeJoinConcurrency is the number of concurrent merge join worker
	mergeJoinConcurrency int

	// streamAggConcurrency is the number of concurrent stream aggregation worker.
	// streamAggConcurrency is deprecated, use ExecutorConcurrency instead.
	streamAggConcurrency int

	// indexMergeIntersectionConcurrency is the number of indexMergeProcessWorker
	// Only meaningful for dynamic pruned partition table.
	indexMergeIntersectionConcurrency int

	// ExecutorConcurrency is the number of concurrent worker for all executors.
	ExecutorConcurrency int

	// SourceAddr is the source address of request. Available in coprocessor ONLY.
	SourceAddr net.TCPAddr

	// IdleTransactionTimeout indicates the maximum time duration a transaction could be idle, unit is second.
	IdleTransactionTimeout int
}

// SetIndexLookupConcurrency set the number of concurrent index lookup worker.
func (c *Concurrency) SetIndexLookupConcurrency(n int) {
	c.indexLookupConcurrency = n
}

// SetIndexLookupJoinConcurrency set the number of concurrent index lookup join inner worker.
func (c *Concurrency) SetIndexLookupJoinConcurrency(n int) {
	c.indexLookupJoinConcurrency = n
}

// SetDistSQLScanConcurrency set the number of concurrent dist SQL scan worker.
func (c *Concurrency) SetDistSQLScanConcurrency(n int) {
	c.distSQLScanConcurrency = n
}

// SetAnalyzeDistSQLScanConcurrency set the number of concurrent dist SQL scan worker when to analyze.
func (c *Concurrency) SetAnalyzeDistSQLScanConcurrency(n int) {
	c.analyzeDistSQLScanConcurrency = n
}

// SetHashJoinConcurrency set the number of concurrent hash join outer worker.
func (c *Concurrency) SetHashJoinConcurrency(n int) {
	c.hashJoinConcurrency = n
}

// SetProjectionConcurrency set the number of concurrent projection worker.
func (c *Concurrency) SetProjectionConcurrency(n int) {
	c.projectionConcurrency = n
}

// SetHashAggPartialConcurrency set the number of concurrent hash aggregation partial worker.
func (c *Concurrency) SetHashAggPartialConcurrency(n int) {
	c.hashAggPartialConcurrency = n
}

// SetHashAggFinalConcurrency set the number of concurrent hash aggregation final worker.
func (c *Concurrency) SetHashAggFinalConcurrency(n int) {
	c.hashAggFinalConcurrency = n
}

// SetWindowConcurrency set the number of concurrent window worker.
func (c *Concurrency) SetWindowConcurrency(n int) {
	c.windowConcurrency = n
}

// SetMergeJoinConcurrency set the number of concurrent merge join worker.
func (c *Concurrency) SetMergeJoinConcurrency(n int) {
	c.mergeJoinConcurrency = n
}

// SetStreamAggConcurrency set the number of concurrent stream aggregation worker.
func (c *Concurrency) SetStreamAggConcurrency(n int) {
	c.streamAggConcurrency = n
}

// SetIndexMergeIntersectionConcurrency set the number of concurrent intersection process worker.
func (c *Concurrency) SetIndexMergeIntersectionConcurrency(n int) {
	c.indexMergeIntersectionConcurrency = n
}

// IndexLookupConcurrency return the number of concurrent index lookup worker.
func (c *Concurrency) IndexLookupConcurrency() int {
	if c.indexLookupConcurrency != vardef.ConcurrencyUnset {
		return c.indexLookupConcurrency
	}
	return c.ExecutorConcurrency
}

// IndexLookupJoinConcurrency return the number of concurrent index lookup join inner worker.
func (c *Concurrency) IndexLookupJoinConcurrency() int {
	if c.indexLookupJoinConcurrency != vardef.ConcurrencyUnset {
		return c.indexLookupJoinConcurrency
	}
	return c.ExecutorConcurrency
}

// DistSQLScanConcurrency return the number of concurrent dist SQL scan worker.
func (c *Concurrency) DistSQLScanConcurrency() int {
	return c.distSQLScanConcurrency
}

// AnalyzeDistSQLScanConcurrency return the number of concurrent dist SQL scan worker when to analyze.
func (c *Concurrency) AnalyzeDistSQLScanConcurrency() int {
	return c.analyzeDistSQLScanConcurrency
}

// HashJoinConcurrency return the number of concurrent hash join outer worker.
func (c *Concurrency) HashJoinConcurrency() int {
	if c.hashJoinConcurrency != vardef.ConcurrencyUnset {
		return c.hashJoinConcurrency
	}
	return c.ExecutorConcurrency
}

// ProjectionConcurrency return the number of concurrent projection worker.
func (c *Concurrency) ProjectionConcurrency() int {
	if c.projectionConcurrency != vardef.ConcurrencyUnset {
		return c.projectionConcurrency
	}
	return c.ExecutorConcurrency
}

// HashAggPartialConcurrency return the number of concurrent hash aggregation partial worker.
func (c *Concurrency) HashAggPartialConcurrency() int {
	if c.hashAggPartialConcurrency != vardef.ConcurrencyUnset {
		return c.hashAggPartialConcurrency
	}
	return c.ExecutorConcurrency
}

// HashAggFinalConcurrency return the number of concurrent hash aggregation final worker.
func (c *Concurrency) HashAggFinalConcurrency() int {
	if c.hashAggFinalConcurrency != vardef.ConcurrencyUnset {
		return c.hashAggFinalConcurrency
	}
	return c.ExecutorConcurrency
}

// WindowConcurrency return the number of concurrent window worker.
func (c *Concurrency) WindowConcurrency() int {
	if c.windowConcurrency != vardef.ConcurrencyUnset {
		return c.windowConcurrency
	}
	return c.ExecutorConcurrency
}

// MergeJoinConcurrency return the number of concurrent merge join worker.
func (c *Concurrency) MergeJoinConcurrency() int {
	if c.mergeJoinConcurrency != vardef.ConcurrencyUnset {
		return c.mergeJoinConcurrency
	}
	return c.ExecutorConcurrency
}

// StreamAggConcurrency return the number of concurrent stream aggregation worker.
func (c *Concurrency) StreamAggConcurrency() int {
	if c.streamAggConcurrency != vardef.ConcurrencyUnset {
		return c.streamAggConcurrency
	}
	return c.ExecutorConcurrency
}

// IndexMergeIntersectionConcurrency return the number of concurrent process worker.
func (c *Concurrency) IndexMergeIntersectionConcurrency() int {
	if c.indexMergeIntersectionConcurrency != vardef.ConcurrencyUnset {
		return c.indexMergeIntersectionConcurrency
	}
	return c.ExecutorConcurrency
}

// UnionConcurrency return the num of concurrent union worker.
func (c *Concurrency) UnionConcurrency() int {
	return c.ExecutorConcurrency
}

// MemQuota defines memory quota values.
type MemQuota struct {
	// MemQuotaQuery defines the memory quota for a query.
	MemQuotaQuery int64
	// MemQuotaApplyCache defines the memory capacity for apply cache.
	MemQuotaApplyCache int64
}

// BatchSize defines batch size values.
type BatchSize struct {
	// IndexJoinBatchSize is the batch size of a index lookup join.
	IndexJoinBatchSize int

	// IndexLookupSize is the number of handles for an index lookup task in index double read executor.
	IndexLookupSize int

	// InitChunkSize defines init row count of a Chunk during query execution.
	InitChunkSize int

	// MaxChunkSize defines max row count of a Chunk during query execution.
	MaxChunkSize int

	// MinPagingSize defines the min size used by the coprocessor paging protocol.
	MinPagingSize int

	// MinPagingSize defines the max size used by the coprocessor paging protocol.
	MaxPagingSize int
}

// PipelinedDMLConfig defines the configuration for pipelined DML.
type PipelinedDMLConfig struct {
	// PipelinedFLushConcurrency indicates the number of concurrent worker for pipelined flush.
	PipelinedFlushConcurrency int

	// PipelinedResolveLockConcurrency indicates the number of concurrent worker for pipelined resolve lock.
	PipelinedResolveLockConcurrency int

	// PipelinedWriteThrottleRatio defines how the flush process is throttled
	// by adding sleep intervals between flushes, to avoid overwhelming the storage layer.
	// It is defined as: throttle_ratio =  T_sleep / (T_sleep + T_flush)
	PipelinedWriteThrottleRatio float64
}

// GenerateBinaryPlan decides whether we should record binary plan in slow log and stmt summary.
// It's controlled by the global variable `tidb_generate_binary_plan`.
var GenerateBinaryPlan atomic2.Bool

// TxnReadTS indicates the value and used situation for tx_read_ts
type TxnReadTS struct {
	readTS uint64
	used   bool
}

// NewTxnReadTS creates TxnReadTS
func NewTxnReadTS(ts uint64) *TxnReadTS {
	return &TxnReadTS{
		readTS: ts,
		used:   false,
	}
}

// UseTxnReadTS returns readTS, and mark used as true
func (t *TxnReadTS) UseTxnReadTS() uint64 {
	if t == nil {
		return 0
	}
	t.used = true
	return t.readTS
}

// SetTxnReadTS update readTS, and refresh used
func (t *TxnReadTS) SetTxnReadTS(ts uint64) {
	if t == nil {
		return
	}
	t.used = false
	t.readTS = ts
}

// PeakTxnReadTS returns readTS
func (t *TxnReadTS) PeakTxnReadTS() uint64 {
	if t == nil {
		return 0
	}
	return t.readTS
}

// CleanupTxnReadTSIfUsed cleans txnReadTS if used
func (s *SessionVars) CleanupTxnReadTSIfUsed() {
	if s.TxnReadTS == nil {
		return
	}
	if s.TxnReadTS.used && s.TxnReadTS.readTS > 0 {
		s.TxnReadTS = NewTxnReadTS(0)
		s.SnapshotInfoschema = nil
	}
}

// GetCPUFactor returns the session variable cpuFactor
func (s *SessionVars) GetCPUFactor() float64 {
	return s.cpuFactor
}

// GetCopCPUFactor returns the session variable copCPUFactor
func (s *SessionVars) GetCopCPUFactor() float64 {
	return s.copCPUFactor
}

// GetMemoryFactor returns the session variable memoryFactor
func (s *SessionVars) GetMemoryFactor() float64 {
	return s.memoryFactor
}

// GetDiskFactor returns the session variable diskFactor
func (s *SessionVars) GetDiskFactor() float64 {
	return s.diskFactor
}

// GetConcurrencyFactor returns the session variable concurrencyFactor
func (s *SessionVars) GetConcurrencyFactor() float64 {
	return s.concurrencyFactor
}

// GetNetworkFactor returns the session variable networkFactor
// returns 0 when tbl is a temporary table.
func (s *SessionVars) GetNetworkFactor(tbl *model.TableInfo) float64 {
	if tbl != nil {
		if tbl.TempTableType != model.TempTableNone {
			return 0
		}
	}
	return s.networkFactor
}

// GetScanFactor returns the session variable scanFactor
// returns 0 when tbl is a temporary table.
func (s *SessionVars) GetScanFactor(tbl *model.TableInfo) float64 {
	if tbl != nil {
		if tbl.TempTableType != model.TempTableNone {
			return 0
		}
	}
	return s.scanFactor
}

// GetDescScanFactor returns the session variable descScanFactor
// returns 0 when tbl is a temporary table.
func (s *SessionVars) GetDescScanFactor(tbl *model.TableInfo) float64 {
	if tbl != nil {
		if tbl.TempTableType != model.TempTableNone {
			return 0
		}
	}
	return s.descScanFactor
}

// GetSeekFactor returns the session variable seekFactor
// returns 0 when tbl is a temporary table.
func (s *SessionVars) GetSeekFactor(tbl *model.TableInfo) float64 {
	if tbl != nil {
		if tbl.TempTableType != model.TempTableNone {
			return 0
		}
	}
	return s.seekFactor
}

// EnableEvalTopNEstimationForStrMatch means if we need to evaluate expression with TopN to improve estimation.
// Currently, it's only for string matching functions (like and regexp).
func (s *SessionVars) EnableEvalTopNEstimationForStrMatch() bool {
	return s.DefaultStrMatchSelectivity == 0
}

// GetStrMatchDefaultSelectivity means the default selectivity for like and regexp.
// Note: 0 is a special value, which means the default selectivity is 0.1 and TopN assisted estimation is enabled.
func (s *SessionVars) GetStrMatchDefaultSelectivity() float64 {
	if s.DefaultStrMatchSelectivity == 0 {
		return 0.1
	}
	return s.DefaultStrMatchSelectivity
}

// GetNegateStrMatchDefaultSelectivity means the default selectivity for not like and not regexp.
// Note:
//
//	  0 is a special value, which means the default selectivity is 0.9 and TopN assisted estimation is enabled.
//	  0.8 (the default value) is also a special value. For backward compatibility, when the variable is set to 0.8, we
//	keep the default selectivity of like/regexp and not like/regexp all 0.8.
func (s *SessionVars) GetNegateStrMatchDefaultSelectivity() float64 {
	if s.DefaultStrMatchSelectivity == vardef.DefTiDBDefaultStrMatchSelectivity {
		return vardef.DefTiDBDefaultStrMatchSelectivity
	}
	return 1 - s.GetStrMatchDefaultSelectivity()
}

// GetRelatedTableForMDL gets the related table for metadata lock.
func (s *SessionVars) GetRelatedTableForMDL() *sync.Map {
	mu := &s.TxnCtx.tdmLock
	mu.Lock()
	defer mu.Unlock()
	if s.TxnCtx.relatedTableForMDL == nil {
		s.TxnCtx.relatedTableForMDL = new(sync.Map)
	}
	return s.TxnCtx.relatedTableForMDL
}

// ClearRelatedTableForMDL clears the related table for MDL.
// related tables for MDL is filled during build logical plan or Preprocess for all DataSources,
// even for queries inside DDLs like `create view as select xxx` and `create table as select xxx`.
// it should be cleared before we execute the DDL statement.
func (s *SessionVars) ClearRelatedTableForMDL() {
	s.TxnCtx.tdmLock.Lock()
	defer s.TxnCtx.tdmLock.Unlock()
	s.TxnCtx.relatedTableForMDL = nil
}

// EnableForceInlineCTE returns the session variable enableForceInlineCTE
func (s *SessionVars) EnableForceInlineCTE() bool {
	return s.enableForceInlineCTE
}

// IsRuntimeFilterEnabled return runtime filter mode whether OFF
func (s *SessionVars) IsRuntimeFilterEnabled() bool {
	return s.runtimeFilterMode != RFOff
}

// GetRuntimeFilterTypes return the session variable runtimeFilterTypes
func (s *SessionVars) GetRuntimeFilterTypes() []RuntimeFilterType {
	return s.runtimeFilterTypes
}

// GetRuntimeFilterMode return the session variable runtimeFilterMode
func (s *SessionVars) GetRuntimeFilterMode() RuntimeFilterMode {
	return s.runtimeFilterMode
}

// GetMaxExecutionTime get the max execution timeout value for select statement.
// Make sure this function is called after s.StmtCtx is already set, otherwise it will always return 0
func (s *SessionVars) GetMaxExecutionTime() uint64 {
	// Since maxExecutionTime is used only for SELECT statements, here we limit its scope.
	if !s.StmtCtx.InSelectStmt {
		return 0
	}
	if s.StmtCtx.HasMaxExecutionTime {
		return s.StmtCtx.MaxExecutionTime
	}
	return s.MaxExecutionTime
}

// GetTiKVClientReadTimeout returns readonly kv request timeout, prefer query hint over session variable
func (s *SessionVars) GetTiKVClientReadTimeout() uint64 {
	return s.TiKVClientReadTimeout
}

// SetDiskFullOpt sets the session variable DiskFullOpt
func (s *SessionVars) SetDiskFullOpt(level kvrpcpb.DiskFullOpt) {
	s.DiskFullOpt = level
}

// GetDiskFullOpt returns the value of DiskFullOpt in the current session.
func (s *SessionVars) GetDiskFullOpt() kvrpcpb.DiskFullOpt {
	return s.DiskFullOpt
}

// ClearDiskFullOpt resets the session variable DiskFullOpt to DiskFullOpt_NotAllowedOnFull.
func (s *SessionVars) ClearDiskFullOpt() {
	s.DiskFullOpt = kvrpcpb.DiskFullOpt_NotAllowedOnFull
}

// RuntimeFilterType type of runtime filter "IN"
type RuntimeFilterType int64

// In type of runtime filter, like "t.k1 in (?)"
// MinMax type of runtime filter, like "t.k1 < ? and t.k1 > ?"
const (
	In RuntimeFilterType = iota
	MinMax
	// todo BloomFilter, bf/in
)

// String convert Runtime Filter Type to String name
func (rfType RuntimeFilterType) String() string {
	switch rfType {
	case In:
		return "IN"
	case MinMax:
		return "MIN_MAX"
	default:
		return ""
	}
}

// RuntimeFilterTypeStringToType convert RuntimeFilterTypeNameString to RuntimeFilterType
// If name is legal, it will return Runtime Filter Type and true
// Else, it will return -1 and false
// The second param means the convert is ok or not. True is ok, false means it is illegal name
// At present, we only support two names: "IN" and "MIN_MAX"
func RuntimeFilterTypeStringToType(name string) (RuntimeFilterType, bool) {
	switch name {
	case "IN":
		return In, true
	case "MIN_MAX":
		return MinMax, true
	default:
		return -1, false
	}
}

// ToRuntimeFilterType convert session var value to RuntimeFilterType list
// If sessionVarValue is legal, it will return RuntimeFilterType list and true
// The second param means the convert is ok or not. True is ok, false means it is illegal value
// The legal value should be comma-separated, eg: "IN,MIN_MAX"
func ToRuntimeFilterType(sessionVarValue string) ([]RuntimeFilterType, bool) {
	typeNameList := strings.Split(sessionVarValue, ",")
	rfTypeMap := make(map[RuntimeFilterType]bool)
	for _, typeName := range typeNameList {
		rfType, ok := RuntimeFilterTypeStringToType(strings.ToUpper(typeName))
		if !ok {
			return nil, ok
		}
		rfTypeMap[rfType] = true
	}
	rfTypeList := make([]RuntimeFilterType, 0, len(rfTypeMap))
	for rfType := range rfTypeMap {
		rfTypeList = append(rfTypeList, rfType)
	}
	return rfTypeList, true
}

// RuntimeFilterMode the mode of runtime filter "OFF", "LOCAL"
type RuntimeFilterMode int64

// RFOff disable runtime filter
// RFLocal enable local runtime filter
// RFGlobal enable local and global runtime filter
const (
	RFOff RuntimeFilterMode = iota + 1
	RFLocal
	RFGlobal
)

// String convert Runtime Filter Mode to String name
func (rfMode RuntimeFilterMode) String() string {
	switch rfMode {
	case RFOff:
		return "OFF"
	case RFLocal:
		return "LOCAL"
	case RFGlobal:
		return "GLOBAL"
	default:
		return ""
	}
}

// RuntimeFilterModeStringToMode convert RuntimeFilterModeString to RuntimeFilterMode
// If name is legal, it will return Runtime Filter Mode and true
// Else, it will return -1 and false
// The second param means the convert is ok or not. True is ok, false means it is illegal name
// At present, we only support one name: "OFF", "LOCAL"
func RuntimeFilterModeStringToMode(name string) (RuntimeFilterMode, bool) {
	switch name {
	case "OFF":
		return RFOff, true
	case "LOCAL":
		return RFLocal, true
	default:
		return -1, false
	}
}

// GetOptObjective return the session variable "tidb_opt_objective".
// Please see comments of SessionVars.OptObjective for details.
func (s *SessionVars) GetOptObjective() string {
	return s.OptObjective
}

// ValidTiFlashPreAggMode returns all valid modes.
func ValidTiFlashPreAggMode() string {
	return vardef.ForcePreAggStr + ", " + vardef.AutoStr + ", " + vardef.ForceStreamingStr
}

// ToTiPBTiFlashPreAggMode return the corresponding tipb value of preaggregation mode.
func ToTiPBTiFlashPreAggMode(mode string) (tipb.TiFlashPreAggMode, bool) {
	switch mode {
	case vardef.ForcePreAggStr:
		return tipb.TiFlashPreAggMode_ForcePreAgg, true
	case vardef.ForceStreamingStr:
		return tipb.TiFlashPreAggMode_ForceStreaming, true
	case vardef.AutoStr:
		return tipb.TiFlashPreAggMode_Auto, true
	default:
		return tipb.TiFlashPreAggMode_ForcePreAgg, false
	}
}

// UseLowResolutionTSO indicates whether low resolution tso could be used for execution.
// After `tidb_low_resolution_tso` supports the global scope, this variable is expected to only affect
// user sessions and not impact internal background sessions and tasks.
// Currently, one of the problems is that the determination of whether a session is an internal task
// session within TiDB is quite inconsistent and chaotic, posing risks. Some internal sessions rely on
// upper-level users correctly using `ExecuteInternal` or `ExecuteRestrictedSQL` for assurance.
// Additionally, the BR code also contains some session-related encapsulation and usage.
//
// TODO: There needs to be a more comprehensive and unified entry point to ensure that all internal
// sessions and global user sessions/variables are isolated and do not affect each other.
func (s *SessionVars) UseLowResolutionTSO() bool {
	return !s.InRestrictedSQL && s.lowResolutionTSO && s.ConnectionID > 0
}

// PessimisticLockEligible indicates whether pessimistic lock should not be ignored for the current
// statement execution. There are cases the `for update` clause should not take effect, like being
// executed in an autocommit session.
func (s *SessionVars) PessimisticLockEligible() bool {
	if s.StmtCtx.ForShareLockEnabledByNoop {
		return false
	}
	// Pessimistic locks is needed for DML statements even they are executed in auto-commit mode.
	if !s.IsAutocommit() || s.InTxn() ||
		s.StmtCtx.InInsertStmt || s.StmtCtx.InUpdateStmt || s.StmtCtx.InDeleteStmt {
		return true
	}
	return false
}

// RemoveLockDDLJobs removes the DDL jobs which doesn't get the metadata lock from jobs.
func RemoveLockDDLJobs(sv *SessionVars, jobs map[int64]*mdldef.JobMDL, printLog bool) {
	if sv.InRestrictedSQL {
		return
	}
	sv.TxnCtxMu.Lock()
	defer sv.TxnCtxMu.Unlock()
	if sv.TxnCtx == nil {
		return
	}
	sv.GetRelatedTableForMDL().Range(func(tblID, value any) bool {
		for jobID, jobMDL := range jobs {
			if _, ok := jobMDL.TableIDs[tblID.(int64)]; ok && value.(int64) < jobMDL.Ver {
				delete(jobs, jobID)
				elapsedTime := time.Since(oracle.GetTimeFromTS(sv.TxnCtx.StartTS))
				logFn := logutil.BgLogger().Debug
				if elapsedTime > time.Minute && printLog {
					logFn = logutil.BgLogger().Info
				}
				logFn("old running transaction block DDL",
					zap.Int64("table ID", tblID.(int64)),
					zap.Int64("jobID", jobID),
					zap.Uint64("connection ID", sv.ConnectionID),
					zap.Duration("elapsed time", elapsedTime))
			}
		}
		return true
	})
}

// MemArbitratorWaitAverseMode is the definition for global memory arbitrator to handle wait-averse mode.
type MemArbitratorWaitAverseMode int

// NoLimit indicates that the memory arbitrator will not control current session
// Enable indicates that the session will be controlled by the wait-averse mode
const (
	MemArbitratorWaitAverseDisable MemArbitratorWaitAverseMode = iota
	MemArbitratorWaitAverseEnable
	MemArbitratorNolimit
)

// Error definitions for memory arbitrator session variables.
var (
	ErrTiDBMemArbitratorSoftLimit     = errors.New(vardef.TiDBMemArbitratorSoftLimit + ": 0 (default); (0, 1.0] float-rate * server-limit; (1, server-limit] integer bytes; auto;")
	ErrTiDBMemArbitratorWaitAverse    = errors.New(vardef.TiDBMemArbitratorWaitAverse + ": 0 (disable); 1 (enable); nolimit;")
	ErrTiDBMemArbitratorQueryReserved = errors.New(vardef.TiDBMemArbitratorQueryReserved + ": 0 (default); (1, server-limit] integer bytes;")
)
