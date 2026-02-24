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
	"context"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	ptypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/kvcache"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/pingcap/tidb/pkg/util/tableutil"
	"github.com/pingcap/tidb/pkg/util/timeutil"
)

func (s *SessionVars) GetAllowInSubqToJoinAndAgg() bool {
	if s.StmtCtx.HasAllowInSubqToJoinAndAggHint {
		return s.StmtCtx.AllowInSubqToJoinAndAgg
	}
	return s.allowInSubqToJoinAndAgg
}

// SetAllowInSubqToJoinAndAgg set SessionVars.allowInSubqToJoinAndAgg.
func (s *SessionVars) SetAllowInSubqToJoinAndAgg(val bool) {
	s.allowInSubqToJoinAndAgg = val
}

// GetAllowPreferRangeScan get preferRangeScan from SessionVars.preferRangeScan.
func (s *SessionVars) GetAllowPreferRangeScan() bool {
	s.RecordRelevantOptVar(vardef.TiDBOptPreferRangeScan)
	return s.preferRangeScan
}

// SetAllowPreferRangeScan set SessionVars.preferRangeScan.
func (s *SessionVars) SetAllowPreferRangeScan(val bool) {
	s.preferRangeScan = val
}

// GetEnableCascadesPlanner get EnableCascadesPlanner from sql hints and SessionVars.EnableCascadesPlanner.
func (s *SessionVars) GetEnableCascadesPlanner() bool {
	if s.StmtCtx.HasEnableCascadesPlannerHint {
		return s.StmtCtx.EnableCascadesPlanner
	}
	return s.EnableCascadesPlanner
}

// SetEnableCascadesPlanner set SessionVars.EnableCascadesPlanner.
func (s *SessionVars) SetEnableCascadesPlanner(val bool) {
	s.EnableCascadesPlanner = val
}

// GetEnableIndexMerge get EnableIndexMerge from SessionVars.enableIndexMerge.
func (s *SessionVars) GetEnableIndexMerge() bool {
	return s.enableIndexMerge
}

// SetEnableIndexMerge set SessionVars.enableIndexMerge.
func (s *SessionVars) SetEnableIndexMerge(val bool) {
	s.enableIndexMerge = val
}

// GetEnablePseudoForOutdatedStats get EnablePseudoForOutdatedStats from SessionVars.EnablePseudoForOutdatedStats.
func (s *SessionVars) GetEnablePseudoForOutdatedStats() bool {
	return s.EnablePseudoForOutdatedStats
}

// SetEnablePseudoForOutdatedStats set SessionVars.EnablePseudoForOutdatedStats.
func (s *SessionVars) SetEnablePseudoForOutdatedStats(val bool) {
	s.EnablePseudoForOutdatedStats = val
}

// GetReplicaRead get ReplicaRead from sql hints and SessionVars.replicaRead with adjusted.
func (s *SessionVars) GetReplicaRead() kv.ReplicaReadType {
	// For test purpose, you can enable this failpoint to get the unadjusted replica read.
	failpoint.Inject("GetReplicaReadUnadjusted", func(_ failpoint.Value) {
		failpoint.Return(s.replicaRead)
	})
	// Replica read only works for read-only statements.
	if !s.StmtCtx.IsReadOnly {
		if s.StmtCtx.HasReplicaReadHint {
			const warnMsg = "Ignore replica read hint for non-read-only statement"
			existWarnings := s.StmtCtx.GetWarnings()
			hasWarning := false
			for _, warn := range existWarnings {
				if warn.Err.Error() == warnMsg {
					hasWarning = true
					break
				}
			}
			if !hasWarning {
				s.StmtCtx.AppendWarning(errors.New(warnMsg))
			}
		}
		return kv.ReplicaReadLeader
	}
	if s.StmtCtx.RCCheckTS || s.RcWriteCheckTS {
		return kv.ReplicaReadLeader
	}
	if s.StmtCtx.HasReplicaReadHint {
		return kv.ReplicaReadType(s.StmtCtx.ReplicaRead)
	}
	// if closest-adaptive is unavailable, fallback to leader read
	if s.replicaRead == kv.ReplicaReadClosestAdaptive && !IsAdaptiveReplicaReadEnabled() {
		return kv.ReplicaReadLeader
	}
	return s.replicaRead
}

// SetReplicaRead set SessionVars.replicaRead.
func (s *SessionVars) SetReplicaRead(val kv.ReplicaReadType) {
	s.replicaRead = val
}

// IsReplicaReadClosestAdaptive returns whether adaptive closest replica can be enabled.
func (s *SessionVars) IsReplicaReadClosestAdaptive() bool {
	return s.replicaRead == kv.ReplicaReadClosestAdaptive && IsAdaptiveReplicaReadEnabled()
}

// GetWriteStmtBufs get pointer of SessionVars.writeStmtBufs.
func (s *SessionVars) GetWriteStmtBufs() *WriteStmtBufs {
	return &s.writeStmtBufs
}

// GetSplitRegionTimeout gets split region timeout.
func (s *SessionVars) GetSplitRegionTimeout() time.Duration {
	return time.Duration(s.WaitSplitRegionTimeout) * time.Second
}

// GetIsolationReadEngines gets isolation read engines.
func (s *SessionVars) GetIsolationReadEngines() map[kv.StoreType]struct{} {
	return s.IsolationReadEngines
}

// CleanBuffers cleans the temporary bufs
func (s *SessionVars) CleanBuffers() {
	s.GetWriteStmtBufs().clean()
}

// AllocPlanColumnID allocates column id for plan.
func (s *SessionVars) AllocPlanColumnID() int64 {
	return s.PlanColumnID.Add(1)
}

// RegisterScalarSubQ register a scalar sub query into the map. This will be used for EXPLAIN.
func (s *SessionVars) RegisterScalarSubQ(scalarSubQ any) {
	s.MapScalarSubQ = append(s.MapScalarSubQ, scalarSubQ)
}

// GetCharsetInfo gets charset and collation for current context.
// What character set should the server translate a statement to after receiving it?
// For this, the server uses the character_set_connection and collation_connection system variables.
// It converts statements sent by the client from character_set_client to character_set_connection
// (except for string literals that have an introducer such as _latin1 or _utf8).
// collation_connection is important for comparisons of literal strings.
// For comparisons of strings with column values, collation_connection does not matter because columns
// have their own collation, which has a higher collation precedence.
// See https://dev.mysql.com/doc/refman/5.7/en/charset-connection.html
func (s *SessionVars) GetCharsetInfo() (charset, collation string) {
	charset = s.systems[vardef.CharacterSetConnection]
	collation = s.systems[vardef.CollationConnection]
	return
}

// GetParseParams gets the parse parameters from session variables.
func (s *SessionVars) GetParseParams() []parser.ParseParam {
	chs, coll := s.GetCharsetInfo()
	cli, err := s.GetSessionOrGlobalSystemVar(context.Background(), vardef.CharacterSetClient)
	if err != nil {
		cli = ""
	}
	return []parser.ParseParam{
		parser.CharsetConnection(chs),
		parser.CollationConnection(coll),
		parser.CharsetClient(cli),
	}
}

// SetStringUserVar set the value and collation for user defined variable.
func (s *SessionVars) SetStringUserVar(name string, strVal string, collation string) {
	name = strings.ToLower(name)
	if len(collation) > 0 {
		s.SetUserVarVal(name, types.NewCollationStringDatum(stringutil.Copy(strVal), collation))
	} else {
		_, collation = s.GetCharsetInfo()
		s.SetUserVarVal(name, types.NewCollationStringDatum(stringutil.Copy(strVal), collation))
	}
}

// SetLastInsertID saves the last insert id to the session context.
// TODO: we may store the result for last_insert_id sys var later.
func (s *SessionVars) SetLastInsertID(insertID uint64) {
	s.StmtCtx.LastInsertIDSet = true
	s.StmtCtx.LastInsertID = insertID
}

// SetStatusFlag sets the session server status variable.
// If on is true sets the flag in session status,
// otherwise removes the flag.
func (s *SessionVars) SetStatusFlag(flag uint16, on bool) {
	if on {
		for {
			status := s.status.Load()
			if status&uint32(flag) == uint32(flag) {
				break
			}
			if s.status.CompareAndSwap(status, status|uint32(flag)) {
				break
			}
		}
		return
	}
	for {
		status := s.status.Load()
		if status&uint32(flag) == 0 {
			break
		}
		if s.status.CompareAndSwap(status, status&^uint32(flag)) {
			break
		}
	}
}

// HasStatusFlag gets the session server status variable, returns true if it is on.
func (s *SessionVars) HasStatusFlag(flag uint16) bool {
	return s.status.Load()&uint32(flag) > 0
}

// Status returns the server status.
func (s *SessionVars) Status() uint16 {
	return uint16(s.status.Load())
}

// SetInTxn sets whether the session is in transaction.
// It also updates the IsExplicit flag in TxnCtx if val is true.
func (s *SessionVars) SetInTxn(val bool) {
	s.SetStatusFlag(mysql.ServerStatusInTrans, val)
	if val {
		s.TxnCtx.IsExplicit = val
	}
}

// InTxn returns if the session is in transaction.
func (s *SessionVars) InTxn() bool {
	return s.HasStatusFlag(mysql.ServerStatusInTrans)
}

// IsAutocommit returns if the session is set to autocommit.
func (s *SessionVars) IsAutocommit() bool {
	return s.HasStatusFlag(mysql.ServerStatusAutocommit)
}

// IsIsolation if true it means the transaction is at that isolation level.
func (s *SessionVars) IsIsolation(isolation string) bool {
	if s.TxnCtx.Isolation != "" {
		return s.TxnCtx.Isolation == isolation
	}
	if s.txnIsolationLevelOneShot.state == oneShotUse {
		s.TxnCtx.Isolation = s.txnIsolationLevelOneShot.value
	}
	if s.TxnCtx.Isolation == "" {
		s.TxnCtx.Isolation, _ = s.GetSystemVar(vardef.TxnIsolation)
	}
	return s.TxnCtx.Isolation == isolation
}

// IsolationLevelForNewTxn returns the isolation level if we want to enter a new transaction
func (s *SessionVars) IsolationLevelForNewTxn() (isolation string) {
	if s.InTxn() {
		if s.txnIsolationLevelOneShot.state == oneShotSet {
			isolation = s.txnIsolationLevelOneShot.value
		}
	} else {
		if s.txnIsolationLevelOneShot.state == oneShotUse {
			isolation = s.txnIsolationLevelOneShot.value
		}
	}

	if isolation == "" {
		isolation, _ = s.GetSystemVar(vardef.TxnIsolation)
	}

	return
}

// SetTxnIsolationLevelOneShotStateForNextTxn sets the txnIsolationLevelOneShot.state for next transaction.
func (s *SessionVars) SetTxnIsolationLevelOneShotStateForNextTxn() {
	if isoLevelOneShot := &s.txnIsolationLevelOneShot; isoLevelOneShot.state != oneShotDef {
		switch isoLevelOneShot.state {
		case oneShotSet:
			isoLevelOneShot.state = oneShotUse
		case oneShotUse:
			isoLevelOneShot.state = oneShotDef
			isoLevelOneShot.value = ""
		}
	}
}

// IsPessimisticReadConsistency if true it means the statement is in a read consistency pessimistic transaction.
func (s *SessionVars) IsPessimisticReadConsistency() bool {
	return s.TxnCtx.IsPessimistic && s.IsIsolation(ast.ReadCommitted)
}

// GetNextPreparedStmtID generates and returns the next session scope prepared statement id.
func (s *SessionVars) GetNextPreparedStmtID() uint32 {
	s.preparedStmtID++
	return s.preparedStmtID
}

// SetNextPreparedStmtID sets the next prepared statement id. It's only used in restoring session states.
func (s *SessionVars) SetNextPreparedStmtID(preparedStmtID uint32) {
	s.preparedStmtID = preparedStmtID
}

// Location returns the value of time_zone session variable. If it is nil, then return time.Local.
func (s *SessionVars) Location() *time.Location {
	loc := s.TimeZone
	if loc == nil {
		loc = timeutil.SystemLocation()
	}
	return loc
}

// GetSystemVar gets the string value of a system variable.
func (s *SessionVars) GetSystemVar(name string) (string, bool) {
	if name == vardef.WarningCount {
		return strconv.Itoa(s.SysWarningCount), true
	} else if name == vardef.ErrorCount {
		return strconv.Itoa(int(s.SysErrorCount)), true
	}
	val, ok := s.systems[name]
	return val, ok
}

func (s *SessionVars) setDDLReorgPriority(val string) {
	val = strings.ToLower(val)
	switch val {
	case "priority_low":
		s.DDLReorgPriority = kv.PriorityLow
	case "priority_normal":
		s.DDLReorgPriority = kv.PriorityNormal
	case "priority_high":
		s.DDLReorgPriority = kv.PriorityHigh
	default:
		s.DDLReorgPriority = kv.PriorityLow
	}
}

type planCacheStmtKey string

func (k planCacheStmtKey) Hash() []byte {
	return []byte(k)
}

// AddNonPreparedPlanCacheStmt adds this PlanCacheStmt into non-preapred plan-cache stmt cache
func (s *SessionVars) AddNonPreparedPlanCacheStmt(sql string, stmt any) {
	if s.nonPreparedPlanCacheStmts == nil {
		s.nonPreparedPlanCacheStmts = kvcache.NewSimpleLRUCache(uint(s.SessionPlanCacheSize), 0, 0)
	}
	s.nonPreparedPlanCacheStmts.Put(planCacheStmtKey(sql), stmt)
}

// GetNonPreparedPlanCacheStmt gets the PlanCacheStmt.
func (s *SessionVars) GetNonPreparedPlanCacheStmt(sql string) any {
	if s.nonPreparedPlanCacheStmts == nil {
		return nil
	}
	stmt, _ := s.nonPreparedPlanCacheStmts.Get(planCacheStmtKey(sql))
	return stmt
}

// AddPreparedStmt adds prepareStmt to current session and count in global.
func (s *SessionVars) AddPreparedStmt(stmtID uint32, stmt any) error {
	if _, exists := s.PreparedStmts[stmtID]; !exists {
		maxPreparedStmtCount := vardef.MaxPreparedStmtCountValue.Load()
		newPreparedStmtCount := atomic.AddInt64(&PreparedStmtCount, 1)
		if maxPreparedStmtCount >= 0 && newPreparedStmtCount > maxPreparedStmtCount {
			atomic.AddInt64(&PreparedStmtCount, -1)
			return ErrMaxPreparedStmtCountReached.GenWithStackByArgs(maxPreparedStmtCount)
		}
		metrics.PreparedStmtGauge.Set(float64(newPreparedStmtCount))
	}
	s.PreparedStmts[stmtID] = stmt
	return nil
}

// RemovePreparedStmt removes preparedStmt from current session and decrease count in global.
func (s *SessionVars) RemovePreparedStmt(stmtID uint32) {
	_, exists := s.PreparedStmts[stmtID]
	if !exists {
		return
	}
	delete(s.PreparedStmts, stmtID)
	afterMinus := atomic.AddInt64(&PreparedStmtCount, -1)
	metrics.PreparedStmtGauge.Set(float64(afterMinus))
}

// WithdrawAllPreparedStmt remove all preparedStmt in current session and decrease count in global.
func (s *SessionVars) WithdrawAllPreparedStmt() {
	psCount := len(s.PreparedStmts)
	if psCount == 0 {
		return
	}
	afterMinus := atomic.AddInt64(&PreparedStmtCount, -int64(psCount))
	metrics.PreparedStmtGauge.Set(float64(afterMinus))
}

// GetSessionOrGlobalSystemVar gets a system variable.
// If it is a session only variable, use the default value defined in code.
// Returns error if there is no such variable.
func (s *SessionVars) GetSessionOrGlobalSystemVar(ctx context.Context, name string) (string, error) {
	sv := GetSysVar(name)
	if sv == nil {
		return "", ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	if sv.HasNoneScope() {
		return sv.Value, nil
	}
	if sv.HasSessionScope() {
		// Populate the value to s.systems if it is not there already.
		// in future should be already loaded on session init
		if sv.GetSession != nil {
			// shortcut to the getter, we won't use the value
			return sv.GetSessionFromHook(s)
		}
		if _, ok := s.systems[sv.Name]; !ok {
			if sv.HasGlobalScope() {
				if val, err := s.GlobalVarsAccessor.GetGlobalSysVar(sv.Name); err == nil {
					s.systems[sv.Name] = val
				}
			} else {
				s.systems[sv.Name] = sv.Value // no global scope, use default
			}
		}
		return sv.GetSessionFromHook(s)
	}
	return sv.GetGlobalFromHook(ctx, s)
}

// GetSessionStatesSystemVar gets the session variable value for session states.
// It's only used for encoding session states when migrating a session.
// The returned boolean indicates whether to keep this value in the session states.
func (s *SessionVars) GetSessionStatesSystemVar(name string) (string, bool, error) {
	sv := GetSysVar(name)
	if sv == nil {
		return "", false, ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	// Call GetStateValue first if it exists. Otherwise, call GetSession.
	if sv.GetStateValue != nil {
		return sv.GetStateValue(s)
	}
	if sv.GetSession != nil {
		val, err := sv.GetSessionFromHook(s)
		return val, err == nil, err
	}
	// Only get the cached value. No need to check the global or default value.
	if val, ok := s.systems[sv.Name]; ok {
		return val, true, nil
	}
	return "", false, nil
}

// GetGlobalSystemVar gets a global system variable.
func (s *SessionVars) GetGlobalSystemVar(ctx context.Context, name string) (string, error) {
	sv := GetSysVar(name)
	if sv == nil {
		return "", ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	return sv.GetGlobalFromHook(ctx, s)
}

// SetSystemVar sets the value of a system variable for session scope.
// Values are automatically normalized (i.e. oN / on / 1 => ON)
// and the validation function is run. To set with less validation, see
// SetSystemVarWithRelaxedValidation.
func (s *SessionVars) SetSystemVar(name string, val string) error {
	sv := GetSysVar(name)
	if sv == nil {
		return ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	val, err := sv.Validate(s, val, vardef.ScopeSession)
	if err != nil {
		return err
	}
	return sv.SetSessionFromHook(s, val)
}

// SetSystemVarWithOldStateAsRet is wrapper of SetSystemVar. Return the old value for later use.
func (s *SessionVars) SetSystemVarWithOldStateAsRet(name string, val string) (string, error) {
	sv := GetSysVar(name)
	if sv == nil {
		return "", ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	val, err := sv.Validate(s, val, vardef.ScopeSession)
	if err != nil {
		return "", err
	}

	var oldV string

	// Call GetStateValue first if it exists. Otherwise, call GetSession.
	if sv.GetStateValue != nil {
		oldV, _ /* not_default */, err = sv.GetStateValue(s)
		if err != nil {
			return "", err
		}
	} else {
		// The map s.systems[sv.Name] is lazy initialized. If we directly read it, we might read empty result.
		// Since this code path is not a hot path, we directly call GetSessionOrGlobalSystemVar to get the value safely.
		oldV, err = s.GetSessionOrGlobalSystemVar(context.Background(), sv.Name)
		if err != nil {
			return "", err
		}
	}

	return oldV, sv.SetSessionFromHook(s, val)
}

// SetSystemVarWithoutValidation sets the value of a system variable for session scope.
// Deprecated: Values are NOT normalized or Validated.
func (s *SessionVars) SetSystemVarWithoutValidation(name string, val string) error {
	sv := GetSysVar(name)
	if sv == nil {
		return ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	return sv.SetSessionFromHook(s, val)
}

// SetSystemVarWithRelaxedValidation sets the value of a system variable for session scope.
// Validation functions are called, but scope validation is skipped.
// Errors are not expected to be returned because this could cause upgrade issues.
func (s *SessionVars) SetSystemVarWithRelaxedValidation(name string, val string) error {
	sv := GetSysVar(name)
	if sv == nil {
		return ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	val = sv.ValidateWithRelaxedValidation(s, val, vardef.ScopeSession)
	return sv.SetSessionFromHook(s, val)
}

// GetReadableTxnMode returns the session variable TxnMode but rewrites it to "OPTIMISTIC" when it's empty.
func (s *SessionVars) GetReadableTxnMode() string {
	txnMode := s.TxnMode
	if txnMode == "" {
		txnMode = ast.Optimistic
	}
	return txnMode
}

// SetPrevStmtDigest sets the digest of the previous statement.
func (s *SessionVars) SetPrevStmtDigest(prevStmtDigest string) {
	s.prevStmtDigest = prevStmtDigest
}

// GetPrevStmtDigest returns the digest of the previous statement.
func (s *SessionVars) GetPrevStmtDigest() string {
	// Because `prevStmt` may be truncated, so it's senseless to normalize it.
	// Even if `prevStmtDigest` is empty but `prevStmt` is not, just return it anyway.
	return s.prevStmtDigest
}

// GetDivPrecisionIncrement returns the specified value of DivPrecisionIncrement.
func (s *SessionVars) GetDivPrecisionIncrement() int {
	return s.DivPrecisionIncrement
}

// GetTemporaryTable returns a TempTable by tableInfo.
func (s *SessionVars) GetTemporaryTable(tblInfo *model.TableInfo) tableutil.TempTable {
	if tblInfo.TempTableType != model.TempTableNone {
		s.TxnCtxMu.Lock()
		defer s.TxnCtxMu.Unlock()
		if s.TxnCtx.TemporaryTables == nil {
			s.TxnCtx.TemporaryTables = make(map[int64]tableutil.TempTable)
		}
		tempTables := s.TxnCtx.TemporaryTables
		tempTable, ok := tempTables[tblInfo.ID]
		if !ok {
			tempTable = tableutil.TempTableFromMeta(tblInfo)
			tempTables[tblInfo.ID] = tempTable
		}
		return tempTable
	}

	return nil
}

// EncodeSessionStates saves session states into SessionStates.
func (s *SessionVars) EncodeSessionStates(_ context.Context, sessionStates *sessionstates.SessionStates) (err error) {
	// Encode user-defined variables.
	s.UserVars.lock.RLock()
	sessionStates.UserVars = make(map[string]*types.Datum, len(s.UserVars.values))
	sessionStates.UserVarTypes = make(map[string]*ptypes.FieldType, len(s.UserVars.types))
	for name, userVar := range s.UserVars.values {
		sessionStates.UserVars[name] = userVar.Clone()
	}
	for name, userVarType := range s.UserVars.types {
		sessionStates.UserVarTypes[name] = userVarType.Clone()
	}
	s.UserVars.lock.RUnlock()

	// Encode other session contexts.
	sessionStates.PreparedStmtID = s.preparedStmtID
	sessionStates.Status = s.status.Load()
	sessionStates.CurrentDB = s.CurrentDB
	sessionStates.LastTxnInfo = s.LastTxnInfo
	if s.LastQueryInfo.StartTS != 0 {
		sessionStates.LastQueryInfo = &s.LastQueryInfo
	}
	if s.LastDDLInfo.SeqNum != 0 {
		sessionStates.LastDDLInfo = &s.LastDDLInfo
	}
	sessionStates.LastFoundRows = s.LastFoundRows
	sessionStates.SequenceLatestValues = s.SequenceState.GetAllStates()
	sessionStates.FoundInPlanCache = s.PrevFoundInPlanCache
	sessionStates.FoundInBinding = s.PrevFoundInBinding
	sessionStates.HypoIndexes = s.HypoIndexes
	sessionStates.HypoTiFlashReplicas = s.HypoTiFlashReplicas

	// Encode StatementContext. We encode it here to avoid circle dependency.
	sessionStates.LastAffectedRows = s.StmtCtx.PrevAffectedRows
	sessionStates.LastInsertID = s.StmtCtx.PrevLastInsertID
	sessionStates.Warnings = s.StmtCtx.GetWarnings()
	return
}

// DecodeSessionStates restores session states from SessionStates.
func (s *SessionVars) DecodeSessionStates(_ context.Context, sessionStates *sessionstates.SessionStates) (err error) {
	// Decode user-defined variables.
	for name, userVar := range sessionStates.UserVars {
		s.SetUserVarVal(name, *userVar.Clone())
	}
	for name, userVarType := range sessionStates.UserVarTypes {
		s.SetUserVarType(name, userVarType.Clone())
	}

	// Decode other session contexts.
	s.preparedStmtID = sessionStates.PreparedStmtID
	s.status.Store(sessionStates.Status)
	s.CurrentDB = sessionStates.CurrentDB
	s.LastTxnInfo = sessionStates.LastTxnInfo
	if sessionStates.LastQueryInfo != nil {
		s.LastQueryInfo = *sessionStates.LastQueryInfo
	}
	if sessionStates.LastDDLInfo != nil {
		s.LastDDLInfo = *sessionStates.LastDDLInfo
	}
	s.LastFoundRows = sessionStates.LastFoundRows
	s.SequenceState.SetAllStates(sessionStates.SequenceLatestValues)
	s.FoundInPlanCache = sessionStates.FoundInPlanCache
	s.FoundInBinding = sessionStates.FoundInBinding
	s.HypoIndexes = sessionStates.HypoIndexes
	s.HypoTiFlashReplicas = sessionStates.HypoTiFlashReplicas

	// Decode StatementContext.
	s.StmtCtx.SetAffectedRows(uint64(sessionStates.LastAffectedRows))
	s.StmtCtx.PrevLastInsertID = sessionStates.LastInsertID
	s.StmtCtx.SetWarnings(sessionStates.Warnings)
	return
}

// SetResourceGroupName changes the resource group name and inc/dec the metrics accordingly.
func (s *SessionVars) SetResourceGroupName(groupName string) {
	if s.ResourceGroupName != groupName {
		metrics.ConnGauge.WithLabelValues(s.ResourceGroupName).Dec()
		metrics.ConnGauge.WithLabelValues(groupName).Inc()
	}
	s.ResourceGroupName = groupName
}
