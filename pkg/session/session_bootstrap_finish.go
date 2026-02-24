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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package session

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemactx "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/infoschema/validatorapi"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/session/cursor"
	session_metrics "github.com/pingcap/tidb/pkg/session/metrics"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage/indexusage"
	"github.com/pingcap/tidb/pkg/table/temptable"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pingcap/tidb/pkg/util/sem"
	"github.com/pingcap/tidb/pkg/util/sli"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/tikv/client-go/v2/oracle"
	tikvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func finishBootstrap(store kv.Storage) {
	store.SetOption(StoreBootstrappedKey, true)

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	err := kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMutator(txn)
		err := t.FinishBootstrap(currentBootstrapVersion)
		return err
	})
	if err != nil {
		logutil.BgLogger().Fatal("finish bootstrap failed",
			zap.Error(err))
	}
}

const quoteCommaQuote = "', '"

// loadCommonGlobalVariablesIfNeeded loads and applies commonly used global variables for the session.
func (s *session) loadCommonGlobalVariablesIfNeeded() error {
	vars := s.sessionVars
	if vars.CommonGlobalLoaded {
		return nil
	}
	if s.Value(sessionctx.Initing) != nil {
		// When running bootstrap or upgrade, we should not access global storage.
		// But we need to init max_allowed_packet to use concat function during bootstrap or upgrade.
		err := vars.SetSystemVar(vardef.MaxAllowedPacket, strconv.FormatUint(vardef.DefMaxAllowedPacket, 10))
		if err != nil {
			logutil.BgLogger().Error("set system variable max_allowed_packet error", zap.Error(err))
		}
		return nil
	}

	vars.CommonGlobalLoaded = true

	// Deep copy sessionvar cache
	sessionCache, err := domain.GetDomain(s).GetSessionCache()
	if err != nil {
		return err
	}
	for varName, varVal := range sessionCache {
		if _, ok := vars.GetSystemVar(varName); !ok {
			err = vars.SetSystemVarWithRelaxedValidation(varName, varVal)
			if err != nil {
				if variable.ErrUnknownSystemVar.Equal(err) {
					continue // sessionCache is stale; sysvar has likely been unregistered
				}
				return err
			}
		}
	}
	// when client set Capability Flags CLIENT_INTERACTIVE, init wait_timeout with interactive_timeout
	if vars.ClientCapability&mysql.ClientInteractive > 0 {
		if varVal, ok := vars.GetSystemVar(vardef.InteractiveTimeout); ok {
			if err := vars.SetSystemVar(vardef.WaitTimeout, varVal); err != nil {
				return err
			}
		}
	}
	return nil
}

// PrepareTxnCtx begins a transaction, and creates a new transaction context.
// When stmt is provided, it determines transaction mode based on the statement.
// When stmt is nil, it uses the session's default transaction mode.
func (s *session) PrepareTxnCtx(ctx context.Context, stmt ast.StmtNode) error {
	s.currentCtx = ctx
	if s.txn.validOrPending() {
		return nil
	}

	txnMode := s.decideTxnMode(stmt)

	return sessiontxn.GetTxnManager(s).EnterNewTxn(ctx, &sessiontxn.EnterNewTxnRequest{
		Type:    sessiontxn.EnterNewTxnBeforeStmt,
		TxnMode: txnMode,
	})
}

// decideTxnMode determines whether to use pessimistic or optimistic transaction mode
// based on the current session state, configuration, and the statement being executed.
// When stmt is nil, it uses the session's default transaction mode.
func (s *session) decideTxnMode(stmt ast.StmtNode) string {
	if s.sessionVars.RetryInfo.Retrying {
		return ast.Pessimistic
	}

	if s.sessionVars.TxnMode != ast.Pessimistic {
		return ast.Optimistic
	}

	if !s.sessionVars.IsAutocommit() {
		return s.sessionVars.TxnMode
	}

	if stmt != nil && s.shouldUsePessimisticAutoCommit(stmt) {
		return ast.Pessimistic
	}

	return ast.Optimistic
}

// shouldUsePessimisticAutoCommit checks if pessimistic-auto-commit should be applied
// for the current statement.
func (s *session) shouldUsePessimisticAutoCommit(stmtNode ast.StmtNode) bool {
	// Check if pessimistic-auto-commit is enabled globally
	if !config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Load() {
		return false
	}

	// Disabled for bulk DML operations
	if s.GetSessionVars().BulkDMLEnabled {
		return false
	}

	if s.isInternal() {
		return false
	}

	// Use direct AST inspection to determine if this is a DML statement
	return s.isDMLStatement(stmtNode)
}

// isDMLStatement checks if the given statement should use pessimistic-auto-commit.
// It handles EXECUTE unwrapping and properly handles EXPLAIN statements by checking their inner statement.
func (s *session) isDMLStatement(stmtNode ast.StmtNode) bool {
	if stmtNode == nil {
		return false
	}

	// Handle EXECUTE statements - unwrap to get the actual prepared statement
	actualStmt := stmtNode
	if execStmt, ok := stmtNode.(*ast.ExecuteStmt); ok {
		prepareStmt, err := plannercore.GetPreparedStmt(execStmt, s.GetSessionVars())
		if err != nil || prepareStmt == nil {
			return false
		}
		actualStmt = prepareStmt.PreparedAst.Stmt
	}

	// For EXPLAIN statements, check the underlying statement
	// This ensures EXPLAIN shows the correct plan that would be used if the statement were executed
	if explainStmt, ok := actualStmt.(*ast.ExplainStmt); ok {
		return s.isDMLStatement(explainStmt.Stmt)
	}

	// Only these DML statements should use pessimistic-auto-commit
	// Note: LOAD DATA and IMPORT are intentionally excluded
	switch actualStmt.(type) {
	case *ast.InsertStmt, *ast.UpdateStmt, *ast.DeleteStmt:
		return true
	default:
		return false
	}
}

// PrepareTSFuture uses to try to get ts future.
func (s *session) PrepareTSFuture(ctx context.Context, future oracle.Future, scope string) error {
	if s.txn.Valid() {
		return errors.New("cannot prepare ts future when txn is valid")
	}

	failpoint.Inject("assertTSONotRequest", func() {
		if _, ok := future.(sessiontxn.ConstantFuture); !ok && !s.isInternal() {
			panic("tso shouldn't be requested")
		}
	})

	failpoint.InjectContext(ctx, "mockGetTSFail", func() {
		future = txnFailFuture{}
	})

	s.txn.changeToPending(&txnFuture{
		future:                          future,
		store:                           s.store,
		txnScope:                        scope,
		pipelined:                       s.usePipelinedDmlOrWarn(ctx),
		pipelinedFlushConcurrency:       s.GetSessionVars().PipelinedFlushConcurrency,
		pipelinedResolveLockConcurrency: s.GetSessionVars().PipelinedResolveLockConcurrency,
		pipelinedWriteThrottleRatio:     s.GetSessionVars().PipelinedWriteThrottleRatio,
	})
	return nil
}

// GetPreparedTxnFuture returns the TxnFuture if it is valid or pending.
// It returns nil otherwise.
func (s *session) GetPreparedTxnFuture() sessionctx.TxnFuture {
	if !s.txn.validOrPending() {
		return nil
	}
	return &s.txn
}

// RefreshTxnCtx implements context.RefreshTxnCtx interface.
func (s *session) RefreshTxnCtx(ctx context.Context) error {
	var commitDetail *tikvutil.CommitDetails
	ctx = context.WithValue(ctx, tikvutil.CommitDetailCtxKey, &commitDetail)
	err := s.doCommit(ctx)
	if commitDetail != nil {
		s.GetSessionVars().StmtCtx.MergeExecDetails(commitDetail)
	}
	if err != nil {
		return err
	}

	s.updateStatsDeltaToCollector()

	return sessiontxn.NewTxn(ctx, s)
}

// GetStore gets the store of session.
func (s *session) GetStore() kv.Storage {
	return s.store
}

func (s *session) ShowProcess() *sessmgr.ProcessInfo {
	return s.processInfo.Load()
}

// GetStartTSFromSession returns the startTS in the session `se`
func GetStartTSFromSession(se any) (startTS, processInfoID uint64) {
	tmp, ok := se.(*session)
	if !ok {
		logutil.BgLogger().Error("GetStartTSFromSession failed, can't transform to session struct")
		return 0, 0
	}
	txnInfo := tmp.TxnInfo()
	if txnInfo != nil {
		startTS = txnInfo.StartTS
		if txnInfo.ProcessInfo != nil {
			processInfoID = txnInfo.ProcessInfo.ConnectionID
		}
	}
	logutil.BgLogger().Debug(
		"GetStartTSFromSession getting startTS of internal session",
		zap.Uint64("startTS", startTS), zap.Time("start time", oracle.GetTimeFromTS(startTS)))

	return startTS, processInfoID
}

// logStmt logs some crucial SQL including: CREATE USER/GRANT PRIVILEGE/CHANGE PASSWORD/DDL etc and normal SQL
// if variable.ProcessGeneralLog is set.
func logStmt(execStmt *executor.ExecStmt, s *session) {
	vars := s.GetSessionVars()
	isCrucial := false
	switch stmt := execStmt.StmtNode.(type) {
	case *ast.DropIndexStmt:
		isCrucial = true
		if stmt.IsHypo {
			isCrucial = false
		}
	case *ast.CreateIndexStmt:
		isCrucial = true
		if stmt.IndexOption != nil && stmt.IndexOption.Tp == ast.IndexTypeHypo {
			isCrucial = false
		}
	case *ast.CreateUserStmt, *ast.DropUserStmt, *ast.AlterUserStmt, *ast.SetPwdStmt, *ast.GrantStmt,
		*ast.RevokeStmt, *ast.AlterTableStmt, *ast.CreateDatabaseStmt, *ast.CreateTableStmt,
		*ast.DropDatabaseStmt, *ast.DropTableStmt, *ast.RenameTableStmt, *ast.TruncateTableStmt,
		*ast.RenameUserStmt, *ast.CreateBindingStmt, *ast.DropBindingStmt, *ast.SetBindingStmt, *ast.BRIEStmt:
		isCrucial = true
	}

	if isCrucial {
		user := vars.User
		schemaVersion := s.GetInfoSchema().SchemaMetaVersion()
		if ss, ok := execStmt.StmtNode.(ast.SensitiveStmtNode); ok {
			logutil.BgLogger().Info("CRUCIAL OPERATION",
				zap.Uint64("conn", vars.ConnectionID),
				zap.Int64("schemaVersion", schemaVersion),
				zap.String("secure text", ss.SecureText()),
				zap.Stringer("user", user))
		} else {
			logutil.BgLogger().Info("CRUCIAL OPERATION",
				zap.Uint64("conn", vars.ConnectionID),
				zap.Int64("schemaVersion", schemaVersion),
				zap.String("cur_db", vars.CurrentDB),
				zap.String("sql", execStmt.StmtNode.Text()),
				zap.Stringer("user", user))
		}
	} else {
		logGeneralQuery(execStmt, s, false)
	}
}

func logGeneralQuery(execStmt *executor.ExecStmt, s *session, isPrepared bool) {
	vars := s.GetSessionVars()
	if vardef.ProcessGeneralLog.Load() && !vars.InRestrictedSQL {
		var query string
		if isPrepared {
			query = execStmt.OriginText()
		} else {
			query = execStmt.GetTextToLog(false)
		}

		query = executor.QueryReplacer.Replace(query)
		if vars.EnableRedactLog != errors.RedactLogEnable {
			query += redact.String(vars.EnableRedactLog, vars.PlanCacheParams.String())
		}

		fields := []zapcore.Field{
			zap.Uint64("conn", vars.ConnectionID),
			zap.String("session_alias", vars.SessionAlias),
			zap.String("user", vars.User.LoginString()),
			zap.Int64("schemaVersion", s.GetInfoSchema().SchemaMetaVersion()),
			zap.Uint64("txnStartTS", vars.TxnCtx.StartTS),
			zap.Uint64("forUpdateTS", vars.TxnCtx.GetForUpdateTS()),
			zap.Bool("isReadConsistency", vars.IsIsolation(ast.ReadCommitted)),
			zap.String("currentDB", vars.CurrentDB),
			zap.Bool("isPessimistic", vars.TxnCtx.IsPessimistic),
			zap.String("sessionTxnMode", vars.GetReadableTxnMode()),
			zap.String("sql", query),
		}
		if ot := execStmt.OriginText(); ot != execStmt.Text() {
			fields = append(fields, zap.String("originText", strconv.Quote(ot)))
		}
		logutil.GeneralLogger.Info("GENERAL_LOG", fields...)
	}
}

func (s *session) recordOnTransactionExecution(err error, counter int, duration float64, isInternal bool) {
	if s.sessionVars.TxnCtx.IsPessimistic {
		if err != nil {
			if isInternal {
				session_metrics.TransactionDurationPessimisticAbortInternal.Observe(duration)
				session_metrics.StatementPerTransactionPessimisticErrorInternal.Observe(float64(counter))
			} else {
				session_metrics.TransactionDurationPessimisticAbortGeneral.Observe(duration)
				session_metrics.StatementPerTransactionPessimisticErrorGeneral.Observe(float64(counter))
			}
		} else {
			if isInternal {
				session_metrics.TransactionDurationPessimisticCommitInternal.Observe(duration)
				session_metrics.StatementPerTransactionPessimisticOKInternal.Observe(float64(counter))
			} else {
				session_metrics.TransactionDurationPessimisticCommitGeneral.Observe(duration)
				session_metrics.StatementPerTransactionPessimisticOKGeneral.Observe(float64(counter))
			}
		}
	} else {
		if err != nil {
			if isInternal {
				session_metrics.TransactionDurationOptimisticAbortInternal.Observe(duration)
				session_metrics.StatementPerTransactionOptimisticErrorInternal.Observe(float64(counter))
			} else {
				session_metrics.TransactionDurationOptimisticAbortGeneral.Observe(duration)
				session_metrics.StatementPerTransactionOptimisticErrorGeneral.Observe(float64(counter))
			}
		} else {
			if isInternal {
				session_metrics.TransactionDurationOptimisticCommitInternal.Observe(duration)
				session_metrics.StatementPerTransactionOptimisticOKInternal.Observe(float64(counter))
			} else {
				session_metrics.TransactionDurationOptimisticCommitGeneral.Observe(duration)
				session_metrics.StatementPerTransactionOptimisticOKGeneral.Observe(float64(counter))
			}
		}
	}
}

func (s *session) checkPlacementPolicyBeforeCommit(ctx context.Context) error {
	var err error
	// Get the txnScope of the transaction we're going to commit.
	txnScope := s.GetSessionVars().TxnCtx.TxnScope
	if txnScope == "" {
		txnScope = kv.GlobalTxnScope
	}
	if txnScope != kv.GlobalTxnScope {
		is := s.GetInfoSchema().(infoschema.InfoSchema)
		deltaMap := s.GetSessionVars().TxnCtx.TableDeltaMap
		for physicalTableID := range deltaMap {
			var tableName string
			var partitionName string
			tblInfo, _, partInfo := is.FindTableByPartitionID(physicalTableID)
			if tblInfo != nil && partInfo != nil {
				tableName = tblInfo.Meta().Name.String()
				partitionName = partInfo.Name.String()
			} else {
				tblInfo, _ := is.TableByID(ctx, physicalTableID)
				tableName = tblInfo.Meta().Name.String()
			}
			bundle, ok := is.PlacementBundleByPhysicalTableID(physicalTableID)
			if !ok {
				errMsg := fmt.Sprintf("table %v doesn't have placement policies with txn_scope %v",
					tableName, txnScope)
				if len(partitionName) > 0 {
					errMsg = fmt.Sprintf("table %v's partition %v doesn't have placement policies with txn_scope %v",
						tableName, partitionName, txnScope)
				}
				err = dbterror.ErrInvalidPlacementPolicyCheck.GenWithStackByArgs(errMsg)
				break
			}
			dcLocation, ok := bundle.GetLeaderDC(placement.DCLabelKey)
			if !ok {
				errMsg := fmt.Sprintf("table %v's leader placement policy is not defined", tableName)
				if len(partitionName) > 0 {
					errMsg = fmt.Sprintf("table %v's partition %v's leader placement policy is not defined", tableName, partitionName)
				}
				err = dbterror.ErrInvalidPlacementPolicyCheck.GenWithStackByArgs(errMsg)
				break
			}
			if dcLocation != txnScope {
				errMsg := fmt.Sprintf("table %v's leader location %v is out of txn_scope %v", tableName, dcLocation, txnScope)
				if len(partitionName) > 0 {
					errMsg = fmt.Sprintf("table %v's partition %v's leader location %v is out of txn_scope %v",
						tableName, partitionName, dcLocation, txnScope)
				}
				err = dbterror.ErrInvalidPlacementPolicyCheck.GenWithStackByArgs(errMsg)
				break
			}
			// FIXME: currently we assume the physicalTableID is the partition ID. In future, we should consider the situation
			// if the physicalTableID belongs to a Table.
			partitionID := physicalTableID
			tbl, _, partitionDefInfo := is.FindTableByPartitionID(partitionID)
			if tbl != nil {
				tblInfo := tbl.Meta()
				state := tblInfo.Partition.GetStateByID(partitionID)
				if state == model.StateGlobalTxnOnly {
					err = dbterror.ErrInvalidPlacementPolicyCheck.GenWithStackByArgs(
						fmt.Sprintf("partition %s of table %s can not be written by local transactions when its placement policy is being altered",
							tblInfo.Name, partitionDefInfo.Name))
					break
				}
			}
		}
	}
	return err
}

func (s *session) SetPort(port string) {
	s.sessionVars.Port = port
}

// GetTxnWriteThroughputSLI implements the Context interface.
func (s *session) GetTxnWriteThroughputSLI() *sli.TxnWriteThroughputSLI {
	return &s.txn.writeSLI
}

// GetInfoSchema returns snapshotInfoSchema if snapshot schema is set.
// Transaction infoschema is returned if inside an explicit txn.
// Otherwise the latest infoschema is returned.
func (s *session) GetInfoSchema() infoschemactx.MetaOnlyInfoSchema {
	vars := s.GetSessionVars()
	var is infoschema.InfoSchema
	if snap, ok := vars.SnapshotInfoschema.(infoschema.InfoSchema); ok {
		logutil.BgLogger().Info("use snapshot schema", zap.Uint64("conn", vars.ConnectionID), zap.Int64("schemaVersion", snap.SchemaMetaVersion()))
		is = snap
	} else {
		vars.TxnCtxMu.Lock()
		if vars.TxnCtx != nil {
			if tmp, ok := vars.TxnCtx.InfoSchema.(infoschema.InfoSchema); ok {
				is = tmp
			}
		}
		vars.TxnCtxMu.Unlock()
	}

	if is == nil {
		is = s.infoCache.GetLatest()
	}

	// Override the infoschema if the session has temporary table.
	return temptable.AttachLocalTemporaryTableInfoSchema(s, is)
}

func (s *session) GetLatestInfoSchema() infoschemactx.MetaOnlyInfoSchema {
	is := s.infoCache.GetLatest()
	extIs := &infoschema.SessionExtendedInfoSchema{InfoSchema: is}
	return temptable.AttachLocalTemporaryTableInfoSchema(s, extIs)
}

func (s *session) GetLatestISWithoutSessExt() infoschemactx.MetaOnlyInfoSchema {
	return s.infoCache.GetLatest()
}

func (s *session) GetSQLServer() sqlsvrapi.Server {
	return s.dom.(sqlsvrapi.Server)
}

func (s *session) IsCrossKS() bool {
	return s.crossKS
}

func (s *session) GetSchemaValidator() validatorapi.Validator {
	return s.schemaValidator
}

func getSnapshotInfoSchema(s sessionctx.Context, snapshotTS uint64) (infoschema.InfoSchema, error) {
	is, err := domain.GetDomain(s).GetSnapshotInfoSchema(snapshotTS)
	if err != nil {
		return nil, err
	}
	// Set snapshot does not affect the witness of the local temporary table.
	// The session always see the latest temporary tables.
	return temptable.AttachLocalTemporaryTableInfoSchema(s, is), nil
}

func (s *session) updateTelemetryMetric(es *executor.ExecStmt) {
	if es.Ti == nil {
		return
	}
	if s.isInternal() {
		return
	}

	ti := es.Ti
	if ti.UseRecursive {
		session_metrics.TelemetryCTEUsageRecurCTE.Inc()
	} else if ti.UseNonRecursive {
		session_metrics.TelemetryCTEUsageNonRecurCTE.Inc()
	} else {
		session_metrics.TelemetryCTEUsageNotCTE.Inc()
	}

	if ti.UseIndexMerge {
		session_metrics.TelemetryIndexMerge.Inc()
	}

	if ti.UseMultiSchemaChange {
		session_metrics.TelemetryMultiSchemaChangeUsage.Inc()
	}

	if ti.UseFlashbackToCluster {
		session_metrics.TelemetryFlashbackClusterUsage.Inc()
	}

	if ti.UseExchangePartition {
		session_metrics.TelemetryExchangePartitionUsage.Inc()
	}

	if ti.PartitionTelemetry != nil {
		if ti.PartitionTelemetry.UseTablePartition {
			session_metrics.TelemetryTablePartitionUsage.Inc()
			session_metrics.TelemetryTablePartitionMaxPartitionsUsage.Add(float64(ti.PartitionTelemetry.TablePartitionMaxPartitionsNum))
		}
		if ti.PartitionTelemetry.UseTablePartitionList {
			session_metrics.TelemetryTablePartitionListUsage.Inc()
		}
		if ti.PartitionTelemetry.UseTablePartitionRange {
			session_metrics.TelemetryTablePartitionRangeUsage.Inc()
		}
		if ti.PartitionTelemetry.UseTablePartitionHash {
			session_metrics.TelemetryTablePartitionHashUsage.Inc()
		}
		if ti.PartitionTelemetry.UseTablePartitionRangeColumns {
			session_metrics.TelemetryTablePartitionRangeColumnsUsage.Inc()
		}
		if ti.PartitionTelemetry.UseTablePartitionRangeColumnsGt1 {
			session_metrics.TelemetryTablePartitionRangeColumnsGt1Usage.Inc()
		}
		if ti.PartitionTelemetry.UseTablePartitionRangeColumnsGt2 {
			session_metrics.TelemetryTablePartitionRangeColumnsGt2Usage.Inc()
		}
		if ti.PartitionTelemetry.UseTablePartitionRangeColumnsGt3 {
			session_metrics.TelemetryTablePartitionRangeColumnsGt3Usage.Inc()
		}
		if ti.PartitionTelemetry.UseTablePartitionListColumns {
			session_metrics.TelemetryTablePartitionListColumnsUsage.Inc()
		}
		if ti.PartitionTelemetry.UseCreateIntervalPartition {
			session_metrics.TelemetryTablePartitionCreateIntervalUsage.Inc()
		}
		if ti.PartitionTelemetry.UseAddIntervalPartition {
			session_metrics.TelemetryTablePartitionAddIntervalUsage.Inc()
		}
		if ti.PartitionTelemetry.UseDropIntervalPartition {
			session_metrics.TelemetryTablePartitionDropIntervalUsage.Inc()
		}
		if ti.PartitionTelemetry.UseCompactTablePartition {
			session_metrics.TelemetryTableCompactPartitionUsage.Inc()
		}
		if ti.PartitionTelemetry.UseReorganizePartition {
			session_metrics.TelemetryReorganizePartitionUsage.Inc()
		}
	}

	if ti.AccountLockTelemetry != nil {
		session_metrics.TelemetryLockUserUsage.Add(float64(ti.AccountLockTelemetry.LockUser))
		session_metrics.TelemetryUnlockUserUsage.Add(float64(ti.AccountLockTelemetry.UnlockUser))
		session_metrics.TelemetryCreateOrAlterUserUsage.Add(float64(ti.AccountLockTelemetry.CreateOrAlterUser))
	}

	if ti.UseTableLookUp.Load() && s.sessionVars.StoreBatchSize > 0 {
		session_metrics.TelemetryStoreBatchedUsage.Inc()
	}
}

// GetBuiltinFunctionUsage returns the replica of counting of builtin function usage
func (s *session) GetBuiltinFunctionUsage() map[string]uint32 {
	replica := make(map[string]uint32)
	s.functionUsageMu.RLock()
	defer s.functionUsageMu.RUnlock()
	for key, value := range s.functionUsageMu.builtinFunctionUsage {
		replica[key] = value
	}
	return replica
}

// BuiltinFunctionUsageInc increase the counting of the builtin function usage
func (s *session) BuiltinFunctionUsageInc(scalarFuncSigName string) {
	s.functionUsageMu.Lock()
	defer s.functionUsageMu.Unlock()
	s.functionUsageMu.builtinFunctionUsage.Inc(scalarFuncSigName)
}

func (s *session) GetStmtStats() *stmtstats.StatementStats {
	return s.stmtStats
}

// SetMemoryFootprintChangeHook sets the hook that is called when the memdb changes its size.
// Call this after s.txn becomes valid, since TxnInfo is initialized when the txn becomes valid.
func (s *session) SetMemoryFootprintChangeHook() {
	if s.txn.MemHookSet() {
		return
	}
	if config.GetGlobalConfig().Performance.TxnTotalSizeLimit != config.DefTxnTotalSizeLimit {
		// if the user manually specifies the config, don't involve the new memory tracker mechanism, let the old config
		// work as before.
		return
	}
	hook := func(mem uint64) {
		if s.sessionVars.MemDBFootprint == nil {
			tracker := memory.NewTracker(memory.LabelForMemDB, -1)
			tracker.AttachTo(s.sessionVars.MemTracker)
			s.sessionVars.MemDBFootprint = tracker
		}
		s.sessionVars.MemDBFootprint.ReplaceBytesUsed(int64(mem))
	}
	s.txn.SetMemoryFootprintChangeHook(hook)
}

func (s *session) EncodeStates(ctx context.Context,
	sessionStates *sessionstates.SessionStates) error {
	// Transaction status is hard to encode, so we do not support it.
	s.txn.mu.Lock()
	valid := s.txn.Valid()
	s.txn.mu.Unlock()
	if valid {
		return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs("session has an active transaction")
	}
	// Data in local temporary tables is hard to encode, so we do not support it.
	// Check temporary tables here to avoid circle dependency.
	if s.sessionVars.LocalTemporaryTables != nil {
		localTempTables := s.sessionVars.LocalTemporaryTables.(*infoschema.SessionTables)
		if localTempTables.Count() > 0 {
			return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs("session has local temporary tables")
		}
	}
	// The advisory locks will be released when the session is closed.
	if len(s.advisoryLocks) > 0 {
		return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs("session has advisory locks")
	}
	// The TableInfo stores session ID and server ID, so the session cannot be migrated.
	if len(s.lockedTables) > 0 {
		return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs("session has locked tables")
	}
	// It's insecure to migrate sandBoxMode because users can fake it.
	if s.InSandBoxMode() {
		return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs("session is in sandbox mode")
	}

	if err := s.sessionVars.EncodeSessionStates(ctx, sessionStates); err != nil {
		return err
	}
	sessionStates.ResourceGroupName = s.sessionVars.ResourceGroupName

	hasRestrictVarPriv := false
	checker := privilege.GetPrivilegeManager(s)
	if checker == nil || checker.RequestDynamicVerification(s.sessionVars.ActiveRoles, "RESTRICTED_VARIABLES_ADMIN", false) {
		hasRestrictVarPriv = true
	}
	// Encode session variables. We put it here instead of SessionVars to avoid cycle import.
	sessionStates.SystemVars = make(map[string]string)
	for _, sv := range variable.GetSysVars() {
		switch {
		case sv.HasNoneScope(), !sv.HasSessionScope():
			// Hidden attribute is deprecated.
			// None-scoped variables cannot be modified.
			// Noop variables should also be migrated even if they are noop.
			continue
		case sv.ReadOnly:
			// Skip read-only variables here. We encode them into SessionStates manually.
			continue
		}
		// Get all session variables because the default values may change between versions.
		val, keep, err := s.sessionVars.GetSessionStatesSystemVar(sv.Name)
		switch {
		case err != nil:
			return err
		case !keep:
			continue
		case !hasRestrictVarPriv && sem.IsEnabled() && sem.IsInvisibleSysVar(sv.Name):
			// If the variable has a global scope, it should be the same with the global one.
			// Otherwise, it should be the same with the default value.
			defaultVal := sv.Value
			if sv.HasGlobalScope() {
				// If the session value is the same with the global one, skip it.
				if defaultVal, err = sv.GetGlobalFromHook(ctx, s.sessionVars); err != nil {
					return err
				}
			}
			if val != defaultVal {
				// Case 1: the RESTRICTED_VARIABLES_ADMIN is revoked after setting the session variable.
				// Case 2: the global variable is updated after the session is created.
				// In any case, the variable can't be set in the new session, so give up.
				return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs(fmt.Sprintf("session has set invisible variable '%s'", sv.Name))
			}
		default:
			sessionStates.SystemVars[sv.Name] = val
		}
	}

	// Encode prepared statements and sql bindings.
	for _, handler := range s.sessionStatesHandlers {
		if err := handler.EncodeSessionStates(ctx, s, sessionStates); err != nil {
			return err
		}
	}
	return nil
}

func (s *session) DecodeStates(ctx context.Context,
	sessionStates *sessionstates.SessionStates) error {
	// Decode prepared statements and sql bindings.
	for _, handler := range s.sessionStatesHandlers {
		if err := handler.DecodeSessionStates(ctx, s, sessionStates); err != nil {
			return err
		}
	}

	// Decode session variables.
	names := variable.OrderByDependency(sessionStates.SystemVars)
	// Some variables must be set before others, e.g. tidb_enable_noop_functions should be before noop variables.
	for _, name := range names {
		val := sessionStates.SystemVars[name]
		// Experimental system variables may change scope, data types, or even be removed.
		// We just ignore the errors and continue.
		if err := s.sessionVars.SetSystemVar(name, val); err != nil {
			logutil.Logger(ctx).Warn("set session variable during decoding session states error",
				zap.String("name", name), zap.String("value", val), zap.Error(err))
		}
	}

	// Put resource group privilege check from sessionVars to session to avoid circular dependency.
	if sessionStates.ResourceGroupName != s.sessionVars.ResourceGroupName {
		hasPriv := true
		if vardef.EnableResourceControlStrictMode.Load() {
			checker := privilege.GetPrivilegeManager(s)
			if checker != nil {
				hasRgAdminPriv := checker.RequestDynamicVerification(s.sessionVars.ActiveRoles, "RESOURCE_GROUP_ADMIN", false)
				hasRgUserPriv := checker.RequestDynamicVerification(s.sessionVars.ActiveRoles, "RESOURCE_GROUP_USER", false)
				hasPriv = hasRgAdminPriv || hasRgUserPriv
			}
		}
		if hasPriv {
			s.sessionVars.SetResourceGroupName(sessionStates.ResourceGroupName)
		} else {
			logutil.Logger(ctx).Warn("set session states error, no privilege to set resource group, skip changing resource group",
				zap.String("source_resource_group", s.sessionVars.ResourceGroupName), zap.String("target_resource_group", sessionStates.ResourceGroupName))
		}
	}

	// Decoding session vars / prepared statements may override stmt ctx, such as warnings,
	// so we decode stmt ctx at last.
	return s.sessionVars.DecodeSessionStates(ctx, sessionStates)
}

func (s *session) setRequestSource(ctx context.Context, stmtLabel string, stmtNode ast.StmtNode) {
	if !s.isInternal() {
		if txn, _ := s.Txn(false); txn != nil && txn.Valid() {
			if txn.IsPipelined() {
				stmtLabel = "pdml"
			}
			txn.SetOption(kv.RequestSourceType, stmtLabel)
		}
		s.sessionVars.RequestSourceType = stmtLabel
		return
	}
	if source := ctx.Value(kv.RequestSourceKey); source != nil {
		requestSource := source.(kv.RequestSource)
		if requestSource.RequestSourceType != "" {
			s.sessionVars.RequestSourceType = requestSource.RequestSourceType
			return
		}
	}
	// panic in test mode in case there are requests without source in the future.
	// log warnings in production mode.
	if intest.EnableInternalCheck {
		panic("unexpected no source type context, if you see this error, " +
			"the `RequestSourceTypeKey` is missing in your context")
	}
	logutil.Logger(ctx).Warn("unexpected no source type context, if you see this warning, "+
		"the `RequestSourceTypeKey` is missing in the context",
		zap.Bool("internal", s.isInternal()),
		zap.String("sql", stmtNode.Text()))
}

// NewStmtIndexUsageCollector creates a new `*indexusage.StmtIndexUsageCollector` based on the internal session index
// usage collector
func (s *session) NewStmtIndexUsageCollector() *indexusage.StmtIndexUsageCollector {
	if s.idxUsageCollector == nil {
		return nil
	}

	return indexusage.NewStmtIndexUsageCollector(s.idxUsageCollector)
}

// usePipelinedDmlOrWarn returns the current statement can be executed as a pipelined DML.
func (s *session) usePipelinedDmlOrWarn(ctx context.Context) bool {
	if !s.sessionVars.BulkDMLEnabled {
		return false
	}
	stmtCtx := s.sessionVars.StmtCtx
	if stmtCtx == nil {
		return false
	}
	if stmtCtx.IsReadOnly {
		return false
	}
	vars := s.GetSessionVars()
	if !vars.TxnCtx.EnableMDL {
		stmtCtx.AppendWarning(
			errors.New(
				"Pipelined DML can not be used without Metadata Lock. Fallback to standard mode",
			),
		)
		return false
	}
	if (vars.BatchCommit || vars.BatchInsert || vars.BatchDelete) && vars.DMLBatchSize > 0 && vardef.EnableBatchDML.Load() {
		stmtCtx.AppendWarning(errors.New("Pipelined DML can not be used with the deprecated Batch DML. Fallback to standard mode"))
		return false
	}
	if !(stmtCtx.InInsertStmt || stmtCtx.InDeleteStmt || stmtCtx.InUpdateStmt) {
		if !stmtCtx.IsReadOnly {
			stmtCtx.AppendWarning(errors.New("Pipelined DML can only be used for auto-commit INSERT, REPLACE, UPDATE or DELETE. Fallback to standard mode"))
		}
		return false
	}
	if s.isInternal() {
		stmtCtx.AppendWarning(errors.New("Pipelined DML can not be used for internal SQL. Fallback to standard mode"))
		return false
	}
	if vars.InTxn() {
		stmtCtx.AppendWarning(errors.New("Pipelined DML can not be used in transaction. Fallback to standard mode"))
		return false
	}
	if !vars.IsAutocommit() {
		stmtCtx.AppendWarning(errors.New("Pipelined DML can only be used in autocommit mode. Fallback to standard mode"))
		return false
	}
	if s.GetSessionVars().ConstraintCheckInPlace {
		// we enforce that pipelined DML must lazily check key.
		stmtCtx.AppendWarning(
			errors.New(
				"Pipelined DML can not be used when tidb_constraint_check_in_place=ON. " +
					"Fallback to standard mode",
			),
		)
		return false
	}
	is, ok := s.GetLatestInfoSchema().(infoschema.InfoSchema)
	if !ok {
		stmtCtx.AppendWarning(errors.New("Pipelined DML failed to get latest InfoSchema. Fallback to standard mode"))
		return false
	}
	for _, t := range stmtCtx.Tables {
		// get table schema from current infoschema
		tbl, err := is.TableByName(ctx, ast.NewCIStr(t.DB), ast.NewCIStr(t.Table))
		if err != nil {
			stmtCtx.AppendWarning(errors.New("Pipelined DML failed to get table schema. Fallback to standard mode"))
			return false
		}
		if tbl.Meta().IsView() {
			stmtCtx.AppendWarning(errors.New("Pipelined DML can not be used on view. Fallback to standard mode"))
			return false
		}
		if tbl.Meta().IsSequence() {
			stmtCtx.AppendWarning(errors.New("Pipelined DML can not be used on sequence. Fallback to standard mode"))
			return false
		}
		if vars.ForeignKeyChecks && (len(tbl.Meta().ForeignKeys) > 0 || len(is.GetTableReferredForeignKeys(t.DB, t.Table)) > 0) {
			stmtCtx.AppendWarning(
				errors.New(
					"Pipelined DML can not be used on table with foreign keys when foreign_key_checks = ON. Fallback to standard mode",
				),
			)
			return false
		}
		if tbl.Meta().TempTableType != model.TempTableNone {
			stmtCtx.AppendWarning(
				errors.New(
					"Pipelined DML can not be used on temporary tables. " +
						"Fallback to standard mode",
				),
			)
			return false
		}
		if tbl.Meta().TableCacheStatusType != model.TableCacheStatusDisable {
			stmtCtx.AppendWarning(
				errors.New(
					"Pipelined DML can not be used on cached tables. " +
						"Fallback to standard mode",
				),
			)
			return false
		}
	}

	// tidb_dml_type=bulk will invalidate the config pessimistic-auto-commit.
	// The behavior is as if the config is set to false. But we generate a warning for it.
	if config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Load() {
		stmtCtx.AppendWarning(
			errors.New(
				"pessimistic-auto-commit config is ignored in favor of Pipelined DML",
			),
		)
	}
	return true
}

// GetDBNames gets the sql layer database names from the session.
func GetDBNames(seVar *variable.SessionVars) []string {
	dbNames := make(map[string]struct{})
	if seVar == nil || !config.GetGlobalConfig().Status.RecordDBLabel {
		return []string{""}
	}
	if seVar.StmtCtx != nil {
		for _, t := range seVar.StmtCtx.Tables {
			dbNames[t.DB] = struct{}{}
		}
	}
	if len(dbNames) == 0 {
		dbNames[strings.ToLower(seVar.CurrentDB)] = struct{}{}
	}
	ns := make([]string, 0, len(dbNames))
	for n := range dbNames {
		ns = append(ns, n)
	}
	return ns
}

// GetCursorTracker returns the internal `cursor.Tracker`
func (s *session) GetCursorTracker() cursor.Tracker {
	return s.cursorTracker
}

// GetCommitWaitGroup returns the internal `sync.WaitGroup` for async commit and secondary key lock cleanup
func (s *session) GetCommitWaitGroup() *sync.WaitGroup {
	return &s.commitWaitGroup
}

// GetDomain get domain from session.
func (s *session) GetDomain() any {
	return s.dom
}
