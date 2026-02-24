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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	stderrs "errors"
	"fmt"
	"iter"
	"regexp"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/config"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/executor/staticrecordset"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/expression/sessionexpr"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/infoschema/validatorapi"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/param"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	planctx "github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/session/cursor"
	session_metrics "github.com/pingcap/tidb/pkg/session/metrics"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/session/txninfo"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage/indexusage"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tblctx"
	"github.com/pingcap/tidb/pkg/table/tblsession"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/telemetry"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/logutil/consistency"
	"github.com/pingcap/tidb/pkg/util/memory"
	parserutil "github.com/pingcap/tidb/pkg/util/parser"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	"github.com/pingcap/tidb/pkg/util/topsql"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/pingcap/tidb/pkg/util/traceevent"
	"github.com/pingcap/tidb/pkg/util/tracing"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/trace"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	tikvutil "github.com/tikv/client-go/v2/util"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
	"go.uber.org/zap"
)

func init() {
	executor.CreateSession = func(ctx sessionctx.Context) (sessionctx.Context, error) {
		return CreateSession(ctx.GetStore())
	}
	executor.CloseSession = func(ctx sessionctx.Context) {
		if se, ok := ctx.(sessionapi.Session); ok {
			se.Close()
		}
	}
}

var _ sessionapi.Session = (*session)(nil)

type stmtRecord struct {
	st      sqlexec.Statement
	stmtCtx *stmtctx.StatementContext
}

// StmtHistory holds all histories of statements in a txn.
type StmtHistory struct {
	history []*stmtRecord
}

// Add appends a stmt to history list.
func (h *StmtHistory) Add(st sqlexec.Statement, stmtCtx *stmtctx.StatementContext) {
	s := &stmtRecord{
		st:      st,
		stmtCtx: stmtCtx,
	}
	h.history = append(h.history, s)
}

// Count returns the count of the history.
func (h *StmtHistory) Count() int {
	return len(h.history)
}

type session struct {
	// processInfo is used by ShowProcess(), and should be modified atomically.
	processInfo atomic.Pointer[sessmgr.ProcessInfo]
	txn         LazyTxn

	mu struct {
		sync.RWMutex
		values map[fmt.Stringer]any
	}

	currentCtx  context.Context // only use for runtime.trace, Please NEVER use it.
	currentPlan base.Plan

	// dom is *domain.Domain, use `any` to avoid import cycle.
	// cross keyspace session doesn't have domain set.
	dom any
	// we cannot compare dom == nil, as dom is untyped, golang will always return false.
	crossKS         bool
	schemaValidator validatorapi.Validator
	infoCache       *infoschema.InfoCache
	store           kv.Storage

	sessionPlanCache sessionctx.SessionPlanCache

	sessionVars    *variable.SessionVars
	sessionManager sessmgr.Manager

	pctx    *planContextImpl
	exprctx *sessionexpr.ExprContext
	tblctx  *tblsession.MutateContext

	statsCollector *usage.SessionStatsItem
	// ddlOwnerManager is used in `select tidb_is_ddl_owner()` statement;
	ddlOwnerManager owner.Manager
	// lockedTables use to record the table locks hold by the session.
	lockedTables map[int64]model.TableLockTpInfo

	// client shared coprocessor client per session
	client kv.Client

	mppClient kv.MPPClient

	// indexUsageCollector collects index usage information.
	idxUsageCollector *indexusage.SessionIndexUsageCollector

	functionUsageMu struct {
		syncutil.RWMutex
		builtinFunctionUsage telemetry.BuiltinFunctionsUsage
	}

	// StmtStats is used to count various indicators of each SQL in this session
	// at each point in time. These data will be periodically taken away by the
	// background goroutine. The background goroutine will continue to aggregate
	// all the local data in each session, and finally report them to the remote
	// regularly.
	stmtStats *stmtstats.StatementStats

	// Used to encode and decode each type of session states.
	sessionStatesHandlers map[sessionstates.SessionStateType]sessionctx.SessionStatesHandler

	// Contains a list of sessions used to collect advisory locks.
	advisoryLocks map[string]*advisoryLock

	extensions *extension.SessionExtensions

	sandBoxMode bool

	cursorTracker cursor.Tracker

	// Used to wait for all async commit background jobs to finish.
	commitWaitGroup sync.WaitGroup
}

// GetTraceCtx returns the trace context of the session.
func (s *session) GetTraceCtx() context.Context {
	return s.currentCtx
}

// AddTableLock adds table lock to the session lock map.
func (s *session) AddTableLock(locks []model.TableLockTpInfo) {
	for _, l := range locks {
		// read only lock is session unrelated, skip it when adding lock to session.
		if l.Tp != ast.TableLockReadOnly {
			s.lockedTables[l.TableID] = l
		}
	}
}

// ReleaseTableLocks releases table lock in the session lock map.
func (s *session) ReleaseTableLocks(locks []model.TableLockTpInfo) {
	for _, l := range locks {
		delete(s.lockedTables, l.TableID)
	}
}

// ReleaseTableLockByTableIDs releases table lock in the session lock map by table ID.
func (s *session) ReleaseTableLockByTableIDs(tableIDs []int64) {
	for _, tblID := range tableIDs {
		delete(s.lockedTables, tblID)
	}
}

// CheckTableLocked checks the table lock.
func (s *session) CheckTableLocked(tblID int64) (bool, ast.TableLockType) {
	lt, ok := s.lockedTables[tblID]
	if !ok {
		return false, ast.TableLockNone
	}
	return true, lt.Tp
}

// GetAllTableLocks gets all table locks table id and db id hold by the session.
func (s *session) GetAllTableLocks() []model.TableLockTpInfo {
	lockTpInfo := make([]model.TableLockTpInfo, 0, len(s.lockedTables))
	for _, tl := range s.lockedTables {
		lockTpInfo = append(lockTpInfo, tl)
	}
	return lockTpInfo
}

// HasLockedTables uses to check whether this session locked any tables.
// If so, the session can only visit the table which locked by self.
func (s *session) HasLockedTables() bool {
	b := len(s.lockedTables) > 0
	return b
}

// ReleaseAllTableLocks releases all table locks hold by the session.
func (s *session) ReleaseAllTableLocks() {
	s.lockedTables = make(map[int64]model.TableLockTpInfo)
}

// IsDDLOwner checks whether this session is DDL owner.
func (s *session) IsDDLOwner() bool {
	return s.ddlOwnerManager.IsOwner()
}

func (s *session) cleanRetryInfo() {
	if s.sessionVars.RetryInfo.Retrying {
		return
	}

	retryInfo := s.sessionVars.RetryInfo
	defer retryInfo.Clean()
	if len(retryInfo.DroppedPreparedStmtIDs) == 0 {
		return
	}

	planCacheEnabled := s.GetSessionVars().EnablePreparedPlanCache
	var cacheKey string
	var err error
	var preparedObj *plannercore.PlanCacheStmt
	if planCacheEnabled {
		firstStmtID := retryInfo.DroppedPreparedStmtIDs[0]
		if preparedPointer, ok := s.sessionVars.PreparedStmts[firstStmtID]; ok {
			preparedObj, ok = preparedPointer.(*plannercore.PlanCacheStmt)
			if ok {
				cacheKey, _, _, _, err = plannercore.NewPlanCacheKey(s, preparedObj)
				if err != nil {
					logutil.Logger(s.currentCtx).Warn("clean cached plan failed", zap.Error(err))
					return
				}
			}
		}
	}
	for i, stmtID := range retryInfo.DroppedPreparedStmtIDs {
		if planCacheEnabled {
			if i > 0 && preparedObj != nil {
				cacheKey, _, _, _, err = plannercore.NewPlanCacheKey(s, preparedObj)
				if err != nil {
					logutil.Logger(s.currentCtx).Warn("clean cached plan failed", zap.Error(err))
					return
				}
			}
			if !s.sessionVars.IgnorePreparedCacheCloseStmt { // keep the plan in cache
				s.GetSessionPlanCache().Delete(cacheKey)
			}
		}
		s.sessionVars.RemovePreparedStmt(stmtID)
	}
}

func (s *session) Status() uint16 {
	return s.sessionVars.Status()
}

func (s *session) LastInsertID() uint64 {
	if s.sessionVars.StmtCtx.LastInsertID > 0 {
		return s.sessionVars.StmtCtx.LastInsertID
	}
	return s.sessionVars.StmtCtx.InsertID
}

func (s *session) LastMessage() string {
	return s.sessionVars.StmtCtx.GetMessage()
}

func (s *session) AffectedRows() uint64 {
	return s.sessionVars.StmtCtx.AffectedRows()
}

func (s *session) SetClientCapability(capability uint32) {
	s.sessionVars.ClientCapability = capability
}

func (s *session) SetConnectionID(connectionID uint64) {
	s.sessionVars.ConnectionID = connectionID
}

func (s *session) SetTLSState(tlsState *tls.ConnectionState) {
	// If user is not connected via TLS, then tlsState == nil.
	if tlsState != nil {
		s.sessionVars.TLSConnectionState = tlsState
	}
}

func (s *session) SetCompressionAlgorithm(ca int) {
	s.sessionVars.CompressionAlgorithm = ca
}

func (s *session) SetCompressionLevel(level int) {
	s.sessionVars.CompressionLevel = level
}

func (s *session) SetCommandValue(command byte) {
	atomic.StoreUint32(&s.sessionVars.CommandValue, uint32(command))
}

func (s *session) SetCollation(coID int) error {
	cs, co, err := charset.GetCharsetInfoByID(coID)
	if err != nil {
		return err
	}
	// If new collations are enabled, switch to the default
	// collation if this one is not supported.
	co = collate.SubstituteMissingCollationToDefault(co)
	for _, v := range vardef.SetNamesVariables {
		terror.Log(s.sessionVars.SetSystemVarWithoutValidation(v, cs))
	}
	return s.sessionVars.SetSystemVarWithoutValidation(vardef.CollationConnection, co)
}

func (s *session) GetSessionPlanCache() sessionctx.SessionPlanCache {
	// use the prepared plan cache
	if !s.GetSessionVars().EnablePreparedPlanCache && !s.GetSessionVars().EnableNonPreparedPlanCache {
		return nil
	}
	if s.sessionPlanCache == nil { // lazy construction
		s.sessionPlanCache = plannercore.NewLRUPlanCache(uint(s.GetSessionVars().SessionPlanCacheSize),
			vardef.PreparedPlanCacheMemoryGuardRatio.Load(), plannercore.PreparedPlanCacheMaxMemory.Load(), s, false)
	}
	return s.sessionPlanCache
}

func (s *session) SetSessionManager(sm sessmgr.Manager) {
	s.sessionManager = sm
}

func (s *session) GetSessionManager() sessmgr.Manager {
	return s.sessionManager
}

func (s *session) UpdateColStatsUsage(colStatsUsage iter.Seq[model.TableItemID]) {
	if s.statsCollector == nil {
		return
	}
	t := time.Now()
	s.statsCollector.UpdateColStatsUsage(colStatsUsage, t)
}

// FieldList returns fields list of a table.
func (s *session) FieldList(tableName string) ([]*resolve.ResultField, error) {
	is := s.GetInfoSchema().(infoschema.InfoSchema)
	dbName := ast.NewCIStr(s.GetSessionVars().CurrentDB)
	tName := ast.NewCIStr(tableName)
	pm := privilege.GetPrivilegeManager(s)
	if pm != nil && s.sessionVars.User != nil {
		if !pm.RequestVerification(s.sessionVars.ActiveRoles, dbName.O, tName.O, "", mysql.AllPrivMask) {
			user := s.sessionVars.User
			u := user.Username
			h := user.Hostname
			if len(user.AuthUsername) > 0 && len(user.AuthHostname) > 0 {
				u = user.AuthUsername
				h = user.AuthHostname
			}
			return nil, plannererrors.ErrTableaccessDenied.GenWithStackByArgs("SELECT", u, h, tableName)
		}
	}
	table, err := is.TableByName(context.Background(), dbName, tName)
	if err != nil {
		return nil, err
	}

	cols := table.Cols()
	fields := make([]*resolve.ResultField, 0, len(cols))
	for _, col := range table.Cols() {
		rf := &resolve.ResultField{
			ColumnAsName: col.Name,
			TableAsName:  tName,
			DBName:       dbName,
			Table:        table.Meta(),
			Column:       col.ColumnInfo,
		}
		fields = append(fields, rf)
	}
	return fields, nil
}

// TxnInfo returns a pointer to a *copy* of the internal TxnInfo, thus is *read only*
// Process field may not initialize if this is a session used internally.
func (s *session) TxnInfo() *txninfo.TxnInfo {
	s.txn.mu.RLock()
	// Copy on read to get a snapshot, this API shouldn't be frequently called.
	txnInfo := s.txn.mu.TxnInfo
	s.txn.mu.RUnlock()

	if txnInfo.StartTS == 0 {
		return nil
	}

	processInfo := s.ShowProcess()
	if processInfo == nil {
		return &txnInfo
	}
	txnInfo.ProcessInfo = &txninfo.ProcessInfo{
		ConnectionID:    processInfo.ID,
		Username:        processInfo.User,
		CurrentDB:       processInfo.DB,
		RelatedTableIDs: make(map[int64]struct{}),
	}
	s.GetSessionVars().GetRelatedTableForMDL().Range(func(key, _ any) bool {
		txnInfo.ProcessInfo.RelatedTableIDs[key.(int64)] = struct{}{}
		return true
	})
	return &txnInfo
}

func (s *session) doCommit(ctx context.Context) error {
	if !s.txn.Valid() {
		return nil
	}

	defer func() {
		s.txn.changeToInvalid()
		s.sessionVars.SetInTxn(false)
		s.sessionVars.ClearDiskFullOpt()
	}()
	// check if the transaction is read-only
	if s.txn.IsReadOnly() {
		return nil
	}
	// check if the cluster is read-only
	if !s.sessionVars.InRestrictedSQL && (vardef.RestrictedReadOnly.Load() || vardef.VarTiDBSuperReadOnly.Load()) {
		// It is not internal SQL, and the cluster has one of RestrictedReadOnly or SuperReadOnly
		// We need to privilege check again: a privilege check occurred during planning, but we need
		// to prevent the case that a long running auto-commit statement is now trying to commit.
		pm := privilege.GetPrivilegeManager(s)
		roles := s.sessionVars.ActiveRoles
		if pm != nil && !pm.HasExplicitlyGrantedDynamicPrivilege(roles, "RESTRICTED_REPLICA_WRITER_ADMIN", false) {
			s.RollbackTxn(ctx)
			return plannererrors.ErrSQLInReadOnlyMode
		}
	}
	err := s.checkPlacementPolicyBeforeCommit(ctx)
	if err != nil {
		return err
	}
	// mockCommitError and mockGetTSErrorInRetry use to test PR #8743.
	failpoint.Inject("mockCommitError", func(val failpoint.Value) {
		if val.(bool) {
			if _, err := failpoint.Eval("tikvclient/mockCommitErrorOpt"); err == nil {
				failpoint.Return(kv.ErrTxnRetryable)
			}
		}
	})

	sessVars := s.GetSessionVars()

	var commitTSChecker func(uint64) bool
	if tables := sessVars.TxnCtx.CachedTables; len(tables) > 0 {
		c := cachedTableRenewLease{tables: tables}
		now := time.Now()
		err := c.start(ctx)
		defer c.stop(ctx)
		sessVars.StmtCtx.WaitLockLeaseTime += time.Since(now)
		if err != nil {
			return errors.Trace(err)
		}
		commitTSChecker = c.commitTSCheck
	}
	if err = sessiontxn.GetTxnManager(s).SetOptionsBeforeCommit(s.txn.Transaction, commitTSChecker); err != nil {
		return err
	}

	err = s.commitTxnWithTemporaryData(tikvutil.SetSessionID(ctx, sessVars.ConnectionID), &s.txn)
	if err != nil {
		err = s.handleAssertionFailure(ctx, err)
	}
	return err
}

type cachedTableRenewLease struct {
	tables map[int64]any
	lease  []uint64 // Lease for each visited cached tables.
	exit   chan struct{}
}

func (c *cachedTableRenewLease) start(ctx context.Context) error {
	c.exit = make(chan struct{})
	c.lease = make([]uint64, len(c.tables))
	wg := make(chan error, len(c.tables))
	ith := 0
	for _, raw := range c.tables {
		tbl := raw.(table.CachedTable)
		go tbl.WriteLockAndKeepAlive(ctx, c.exit, &c.lease[ith], wg)
		ith++
	}

	// Wait for all LockForWrite() return, this function can return.
	var err error
	for ; ith > 0; ith-- {
		tmp := <-wg
		if tmp != nil {
			err = tmp
		}
	}
	return err
}

func (c *cachedTableRenewLease) stop(_ context.Context) {
	close(c.exit)
}

func (c *cachedTableRenewLease) commitTSCheck(commitTS uint64) bool {
	for i := range c.lease {
		lease := atomic.LoadUint64(&c.lease[i])
		if commitTS >= lease {
			// Txn fails to commit because the write lease is expired.
			return false
		}
	}
	return true
}

// handleAssertionFailure extracts the possible underlying assertionFailed error,
// gets the corresponding MVCC history and logs it.
// If it's not an assertion failure, returns the original error.
func (s *session) handleAssertionFailure(ctx context.Context, err error) error {
	var assertionFailure *tikverr.ErrAssertionFailed
	if !stderrs.As(err, &assertionFailure) {
		return err
	}
	key := assertionFailure.Key
	newErr := kv.ErrAssertionFailed.GenWithStackByArgs(
		hex.EncodeToString(key), assertionFailure.Assertion.String(), assertionFailure.StartTs,
		assertionFailure.ExistingStartTs, assertionFailure.ExistingCommitTs,
	)

	rmode := s.GetSessionVars().EnableRedactLog
	if rmode == errors.RedactLogEnable {
		return newErr
	}

	var decodeFunc func(kv.Key, *kvrpcpb.MvccGetByKeyResponse, map[string]any)
	// if it's a record key or an index key, decode it
	if infoSchema, ok := s.sessionVars.TxnCtx.InfoSchema.(infoschema.InfoSchema); ok &&
		infoSchema != nil && (tablecodec.IsRecordKey(key) || tablecodec.IsIndexKey(key)) {
		tableOrPartitionID := tablecodec.DecodeTableID(key)
		tbl, ok := infoSchema.TableByID(ctx, tableOrPartitionID)
		if !ok {
			tbl, _, _ = infoSchema.FindTableByPartitionID(tableOrPartitionID)
		}
		if tbl == nil {
			logutil.Logger(ctx).Warn("cannot find table by id", zap.Int64("tableID", tableOrPartitionID), zap.String("key", hex.EncodeToString(key)))
			return newErr
		}

		if tablecodec.IsRecordKey(key) {
			decodeFunc = consistency.DecodeRowMvccData(tbl.Meta())
		} else {
			tableInfo := tbl.Meta()
			_, indexID, _, e := tablecodec.DecodeIndexKey(key)
			if e != nil {
				logutil.Logger(ctx).Error("assertion failed but cannot decode index key", zap.Error(e))
				return newErr
			}
			var indexInfo *model.IndexInfo
			for _, idx := range tableInfo.Indices {
				if idx.ID == indexID {
					indexInfo = idx
					break
				}
			}
			if indexInfo == nil {
				return newErr
			}
			decodeFunc = consistency.DecodeIndexMvccData(indexInfo)
		}
	}
	if store, ok := s.store.(helper.Storage); ok {
		content := consistency.GetMvccByKey(store, key, decodeFunc)
		logutil.Logger(ctx).Error("assertion failed", zap.String("message", newErr.Error()), zap.String("mvcc history", redact.String(rmode, content)))
	}
	return newErr
}

func (s *session) commitTxnWithTemporaryData(ctx context.Context, txn kv.Transaction) error {
	sessVars := s.sessionVars
	txnTempTables := sessVars.TxnCtx.TemporaryTables
	if len(txnTempTables) == 0 {
		failpoint.Inject("mockSleepBeforeTxnCommit", func(v failpoint.Value) {
			ms := v.(int)
			time.Sleep(time.Millisecond * time.Duration(ms))
		})
		return txn.Commit(ctx)
	}

	sessionData := sessVars.TemporaryTableData
	var (
		stage           kv.StagingHandle
		localTempTables *infoschema.SessionTables
	)

	if sessVars.LocalTemporaryTables != nil {
		localTempTables = sessVars.LocalTemporaryTables.(*infoschema.SessionTables)
	} else {
		localTempTables = new(infoschema.SessionTables)
	}

	defer func() {
		// stage != kv.InvalidStagingHandle means error occurs, we need to cleanup sessionData
		if stage != kv.InvalidStagingHandle {
			sessionData.Cleanup(stage)
		}
	}()

	for tblID, tbl := range txnTempTables {
		if !tbl.GetModified() {
			continue
		}

		if tbl.GetMeta().TempTableType != model.TempTableLocal {
			continue
		}
		if _, ok := localTempTables.TableByID(tblID); !ok {
			continue
		}

		if stage == kv.InvalidStagingHandle {
			stage = sessionData.Staging()
		}

		tblPrefix := tablecodec.EncodeTablePrefix(tblID)
		endKey := tablecodec.EncodeTablePrefix(tblID + 1)

		txnMemBuffer := s.txn.GetMemBuffer()
		iter, err := txnMemBuffer.Iter(tblPrefix, endKey)
		if err != nil {
			return err
		}

		for iter.Valid() {
			key := iter.Key()
			if !bytes.HasPrefix(key, tblPrefix) {
				break
			}

			value := iter.Value()
			if len(value) == 0 {
				err = sessionData.DeleteTableKey(tblID, key)
			} else {
				err = sessionData.SetTableKey(tblID, key, iter.Value())
			}

			if err != nil {
				return err
			}

			err = iter.Next()
			if err != nil {
				return err
			}
		}
	}

	err := txn.Commit(ctx)
	if err != nil {
		return err
	}

	if stage != kv.InvalidStagingHandle {
		sessionData.Release(stage)
		stage = kv.InvalidStagingHandle
	}

	return nil
}

// errIsNoisy is used to filter DUPLICATE KEY errors.
// These can observed by users in INFORMATION_SCHEMA.CLIENT_ERRORS_SUMMARY_GLOBAL instead.
//
// The rationale for filtering these errors is because they are "client generated errors". i.e.
// of the errors defined in kv/error.go, these look to be clearly related to a client-inflicted issue,
// and the server is only responsible for handling the error correctly. It does not need to log.
func errIsNoisy(err error) bool {
	if kv.ErrKeyExists.Equal(err) {
		return true
	}
	if storeerr.ErrLockAcquireFailAndNoWaitSet.Equal(err) {
		return true
	}
	return false
}

func (s *session) doCommitWithRetry(ctx context.Context) error {
	defer func() {
		s.GetSessionVars().SetTxnIsolationLevelOneShotStateForNextTxn()
		s.txn.changeToInvalid()
		s.cleanRetryInfo()
		sessiontxn.GetTxnManager(s).OnTxnEnd()
	}()
	if !s.txn.Valid() {
		// If the transaction is invalid, maybe it has already been rolled back by the client.
		return nil
	}
	isInternalTxn := false
	if internal := s.txn.GetOption(kv.RequestSourceInternal); internal != nil && internal.(bool) {
		isInternalTxn = true
	}
	var err error
	txnSize := s.txn.Size()
	isPessimistic := s.sessionVars.TxnCtx.IsPessimistic
	isPipelined := s.txn.IsPipelined()
	r, ctx := tracing.StartRegionEx(ctx, "session.doCommitWithRetry")
	defer r.End()

	// Emit txn.commit.start trace event
	startTS := s.sessionVars.TxnCtx.StartTS
	if traceevent.IsEnabled(traceevent.TxnLifecycle) {
		traceevent.TraceEvent(ctx, traceevent.TxnLifecycle, "txn.commit.start",
			zap.Uint64("start_ts", startTS),
			zap.Bool("pessimistic", isPessimistic),
			zap.Bool("pipelined", isPipelined),
			zap.Int("txn_size", txnSize),
			zap.Uint64("conn_id", s.sessionVars.ConnectionID),
		)
	}

	// Defer txn.commit.finish to capture final result
	defer func() {
		if traceevent.IsEnabled(traceevent.TxnLifecycle) {
			fields := []zap.Field{
				zap.Uint64("start_ts", startTS),
				zap.Bool("pessimistic", isPessimistic),
				zap.Bool("pipelined", isPipelined),
				zap.Uint64("conn_id", s.sessionVars.ConnectionID),
			}
			if s.txn.lastCommitTS > 0 {
				fields = append(fields, zap.Uint64("commit_ts", s.txn.lastCommitTS))
			}
			if err != nil {
				fields = append(fields, zap.Error(err))
			}
			traceevent.TraceEvent(ctx, traceevent.TxnLifecycle, "txn.commit.finish", fields...)
		}
	}()

	err = s.doCommit(ctx)
	if err != nil {
		// polish the Write Conflict error message
		newErr := s.tryReplaceWriteConflictError(ctx, err)
		if newErr != nil {
			err = newErr
		}

		commitRetryLimit := s.sessionVars.RetryLimit
		if !s.sessionVars.TxnCtx.CouldRetry {
			commitRetryLimit = 0
		}
		// Don't retry in BatchInsert mode. As a counter-example, insert into t1 select * from t2,
		// BatchInsert already commit the first batch 1000 rows, then it commit 1000-2000 and retry the statement,
		// Finally t1 will have more data than t2, with no errors return to user!
		if s.isTxnRetryableError(err) && !s.sessionVars.BatchInsert && commitRetryLimit > 0 && !isPessimistic && !isPipelined {
			logutil.Logger(ctx).Warn("sql",
				zap.String("label", s.GetSQLLabel()),
				zap.Error(err),
				zap.String("txn", s.txn.GoString()))
			// Transactions will retry 2 ~ commitRetryLimit times.
			// We make larger transactions retry less times to prevent cluster resource outage.
			txnSizeRate := float64(txnSize) / float64(kv.TxnTotalSizeLimit.Load())
			maxRetryCount := commitRetryLimit - int64(float64(commitRetryLimit-1)*txnSizeRate)
			err = s.retry(ctx, uint(maxRetryCount))
		} else if !errIsNoisy(err) {
			logutil.Logger(ctx).Warn("can not retry txn",
				zap.String("label", s.GetSQLLabel()),
				zap.Error(err),
				zap.Bool("IsBatchInsert", s.sessionVars.BatchInsert),
				zap.Bool("IsPessimistic", isPessimistic),
				zap.Bool("InRestrictedSQL", s.sessionVars.InRestrictedSQL),
				zap.Int64("tidb_retry_limit", s.sessionVars.RetryLimit),
				zap.Bool("tidb_disable_txn_auto_retry", s.sessionVars.DisableTxnAutoRetry))
		}
	}
	counter := s.sessionVars.TxnCtx.StatementCount
	duration := time.Since(s.GetSessionVars().TxnCtx.CreateTime).Seconds()
	s.recordOnTransactionExecution(err, counter, duration, isInternalTxn)

	if err != nil {
		if !errIsNoisy(err) {
			logutil.Logger(ctx).Warn("commit failed",
				zap.String("finished txn", s.txn.GoString()),
				zap.Error(err))
		}
		return err
	}
	s.updateStatsDeltaToCollector()
	return nil
}

// adds more information about the table in the error message
// precondition: oldErr is a 9007:WriteConflict Error
func (s *session) tryReplaceWriteConflictError(ctx context.Context, oldErr error) (newErr error) {
	if !kv.ErrWriteConflict.Equal(oldErr) {
		return nil
	}
	if errors.RedactLogEnabled.Load() == errors.RedactLogEnable {
		return nil
	}
	originErr := errors.Cause(oldErr)
	inErr, _ := originErr.(*errors.Error)
	// we don't want to modify the oldErr, so copy the args list
	oldArgs := inErr.Args()
	args := slices.Clone(oldArgs)
	is := sessiontxn.GetTxnManager(s).GetTxnInfoSchema()
	if is == nil {
		return nil
	}
	newKeyTableField, ok := addTableNameInTableIDField(ctx, args[3], is)
	if ok {
		args[3] = newKeyTableField
	}
	newPrimaryKeyTableField, ok := addTableNameInTableIDField(ctx, args[5], is)
	if ok {
		args[5] = newPrimaryKeyTableField
	}
	return kv.ErrWriteConflict.FastGenByArgs(args...)
}

// precondition: is != nil
func addTableNameInTableIDField(ctx context.Context, tableIDField any, is infoschema.InfoSchema) (enhancedMsg string, done bool) {
	keyTableID, ok := tableIDField.(string)
	if !ok {
		return "", false
	}
	stringsInTableIDField := strings.Split(keyTableID, "=")
	if len(stringsInTableIDField) == 0 {
		return "", false
	}
	tableIDStr := stringsInTableIDField[len(stringsInTableIDField)-1]
	tableID, err := strconv.ParseInt(tableIDStr, 10, 64)
	if err != nil {
		return "", false
	}
	var tableName string
	tbl, ok := is.TableByID(ctx, tableID)
	if !ok {
		tableName = "unknown"
	} else {
		dbInfo, ok := infoschema.SchemaByTable(is, tbl.Meta())
		if !ok {
			tableName = "unknown." + tbl.Meta().Name.String()
		} else {
			tableName = dbInfo.Name.String() + "." + tbl.Meta().Name.String()
		}
	}
	enhancedMsg = keyTableID + ", tableName=" + tableName
	return enhancedMsg, true
}

func (s *session) updateStatsDeltaToCollector() {
	mapper := s.GetSessionVars().TxnCtx.TableDeltaMap
	if s.statsCollector != nil && mapper != nil {
		for tableID, item := range mapper {
			if tableID > 0 {
				s.statsCollector.Update(tableID, item.Delta, item.Count)
			}
		}
	}
}

func (s *session) CommitTxn(ctx context.Context) error {
	r, ctx := tracing.StartRegionEx(ctx, "session.CommitTxn")
	defer r.End()

	s.setLastTxnInfoBeforeTxnEnd()
	var commitDetail *tikvutil.CommitDetails
	ctx = context.WithValue(ctx, tikvutil.CommitDetailCtxKey, &commitDetail)
	err := s.doCommitWithRetry(ctx)
	if commitDetail != nil {
		s.sessionVars.StmtCtx.MergeExecDetails(commitDetail)
	}

	if err == nil && s.txn.lastCommitTS > 0 {
		// lastCommitTS could be the same, e.g. when the txn is considered readonly
		if s.txn.lastCommitTS < s.sessionVars.LastCommitTS {
			logutil.BgLogger().Error("check lastCommitTS failed",
				zap.Uint64("sessionLastCommitTS", s.sessionVars.LastCommitTS),
				zap.Uint64("txnLastCommitTS", s.txn.lastCommitTS),
				zap.String("sql", redact.String(s.sessionVars.EnableRedactLog, s.sessionVars.StmtCtx.OriginalSQL)),
			)
			return fmt.Errorf("txn commit_ts:%d is before session last_commit_ts:%d",
				s.txn.lastCommitTS, s.sessionVars.LastCommitTS)
		}
		s.sessionVars.LastCommitTS = s.txn.lastCommitTS
	}

	// record the TTLInsertRows in the metric
	metrics.TTLInsertRowsCount.Add(float64(s.sessionVars.TxnCtx.InsertTTLRowsCount))
	metrics.DDLCommitTempIndexWrite(s.sessionVars.ConnectionID)

	failpoint.Inject("keepHistory", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(err)
		}
	})
	s.sessionVars.TxnCtx.Cleanup()
	s.sessionVars.CleanupTxnReadTSIfUsed()
	return err
}

func (s *session) RollbackTxn(ctx context.Context) {
	r, ctx := tracing.StartRegionEx(ctx, "session.RollbackTxn")
	defer r.End()

	// Emit txn.rollback trace event
	if traceevent.IsEnabled(traceevent.TxnLifecycle) {
		startTS := s.sessionVars.TxnCtx.StartTS
		stmtCount := uint64(s.sessionVars.TxnCtx.StatementCount)
		traceevent.TraceEvent(ctx, traceevent.TxnLifecycle, "txn.rollback",
			zap.Uint64("start_ts", startTS),
			zap.Uint64("stmt_count", stmtCount),
		)
	}

	s.setLastTxnInfoBeforeTxnEnd()
	if s.txn.Valid() {
		terror.Log(s.txn.Rollback())
	}
	if ctx.Value(inCloseSession{}) == nil {
		s.cleanRetryInfo()
	}
	s.txn.changeToInvalid()
	s.sessionVars.TxnCtx.Cleanup()
	s.sessionVars.CleanupTxnReadTSIfUsed()
	s.sessionVars.SetInTxn(false)
	sessiontxn.GetTxnManager(s).OnTxnEnd()
	metrics.DDLRollbackTempIndexWrite(s.sessionVars.ConnectionID)
}

// setLastTxnInfoBeforeTxnEnd sets the @@last_txn_info variable before commit/rollback the transaction.
// The `LastTxnInfo` updated with a JSON string that contains start_ts, for_update_ts, etc.
// The `LastTxnInfo` is updated without the `commit_ts` fields because it is unknown
// until the commit is done (or do not need to commit for readonly or a rollback transaction).
// The non-readonly transaction will overwrite the `LastTxnInfo` again after commit to update the `commit_ts` field.
func (s *session) setLastTxnInfoBeforeTxnEnd() {
	txnCtx := s.GetSessionVars().TxnCtx
	if txnCtx.StartTS == 0 {
		// If the txn is not active, for example, executing "SELECT 1", skip setting the last txn info.
		return
	}

	lastTxnInfo, err := json.Marshal(transaction.TxnInfo{
		TxnScope: txnCtx.TxnScope,
		StartTS:  txnCtx.StartTS,
	})
	terror.Log(err)
	s.GetSessionVars().LastTxnInfo = string(lastTxnInfo)
}

func (s *session) GetClient() kv.Client {
	return s.client
}

func (s *session) GetMPPClient() kv.MPPClient {
	return s.mppClient
}

func (s *session) String() string {
	// TODO: how to print binded context in values appropriately?
	sessVars := s.sessionVars
	data := map[string]any{
		"id":         sessVars.ConnectionID,
		"user":       sessVars.User,
		"currDBName": sessVars.CurrentDB,
		"status":     sessVars.Status(),
		"strictMode": sessVars.SQLMode.HasStrictMode(),
	}
	if s.txn.Valid() {
		// if txn is committed or rolled back, txn is nil.
		data["txn"] = s.txn.String()
	}
	if sessVars.SnapshotTS != 0 {
		data["snapshotTS"] = sessVars.SnapshotTS
	}
	if sessVars.StmtCtx.LastInsertID > 0 {
		data["lastInsertID"] = sessVars.StmtCtx.LastInsertID
	}
	if len(sessVars.PreparedStmts) > 0 {
		data["preparedStmtCount"] = len(sessVars.PreparedStmts)
	}
	b, err := json.MarshalIndent(data, "", "  ")
	terror.Log(errors.Trace(err))
	return string(b)
}

const sqlLogMaxLen = 1024

// SchemaChangedWithoutRetry is used for testing.
var SchemaChangedWithoutRetry uint32

func (s *session) GetSQLLabel() string {
	if s.sessionVars.InRestrictedSQL {
		return metrics.LblInternal
	}
	return metrics.LblGeneral
}

func (s *session) isInternal() bool {
	return s.sessionVars.InRestrictedSQL
}

func (*session) isTxnRetryableError(err error) bool {
	if atomic.LoadUint32(&SchemaChangedWithoutRetry) == 1 {
		return kv.IsTxnRetryableError(err)
	}
	return kv.IsTxnRetryableError(err) || domain.ErrInfoSchemaChanged.Equal(err)
}

func isEndTxnStmt(stmt ast.StmtNode, vars *variable.SessionVars) (bool, error) {
	switch n := stmt.(type) {
	case *ast.RollbackStmt, *ast.CommitStmt:
		return true, nil
	case *ast.ExecuteStmt:
		ps, err := plannercore.GetPreparedStmt(n, vars)
		if err != nil {
			return false, err
		}
		return isEndTxnStmt(ps.PreparedAst.Stmt, vars)
	}
	return false, nil
}

func (s *session) checkTxnAborted(stmt sqlexec.Statement) error {
	if atomic.LoadUint32(&s.GetSessionVars().TxnCtx.LockExpire) == 0 {
		return nil
	}
	// If the transaction is aborted, the following statements do not need to execute, except `commit` and `rollback`,
	// because they are used to finish the aborted transaction.
	if ok, err := isEndTxnStmt(stmt.(*executor.ExecStmt).StmtNode, s.sessionVars); err == nil && ok {
		return nil
	} else if err != nil {
		return err
	}
	return kv.ErrLockExpire
}

func (s *session) retry(ctx context.Context, maxCnt uint) (err error) {
	var retryCnt uint
	defer func() {
		s.sessionVars.RetryInfo.Retrying = false
		// retryCnt only increments on retryable error, so +1 here.
		if s.sessionVars.InRestrictedSQL {
			session_metrics.TransactionRetryInternal.Observe(float64(retryCnt + 1))
		} else {
			session_metrics.TransactionRetryGeneral.Observe(float64(retryCnt + 1))
		}
		s.sessionVars.SetInTxn(false)
		if err != nil {
			s.RollbackTxn(ctx)
		}
		s.txn.changeToInvalid()
	}()

	connID := s.sessionVars.ConnectionID
	s.sessionVars.RetryInfo.Retrying = true
	if atomic.LoadUint32(&s.sessionVars.TxnCtx.ForUpdate) == 1 {
		err = ErrForUpdateCantRetry.GenWithStackByArgs(connID)
		return err
	}

	nh := GetHistory(s)
	var schemaVersion int64
	sessVars := s.GetSessionVars()
	orgStartTS := sessVars.TxnCtx.StartTS
	label := s.GetSQLLabel()
	for {
		if err = s.PrepareTxnCtx(ctx, nil); err != nil {
			return err
		}
		s.sessionVars.RetryInfo.ResetOffset()
		for i, sr := range nh.history {
			st := sr.st
			s.sessionVars.StmtCtx = sr.stmtCtx
			s.sessionVars.StmtCtx.CTEStorageMap = map[int]*executor.CTEStorages{}
			s.sessionVars.StmtCtx.ResetForRetry()
			s.sessionVars.PlanCacheParams.Reset()
			schemaVersion, err = st.RebuildPlan(ctx)
			if err != nil {
				return err
			}

			if retryCnt == 0 {
				// We do not have to log the query every time.
				// We print the queries at the first try only.
				sql := sqlForLog(st.GetTextToLog(false))
				if sessVars.EnableRedactLog != errors.RedactLogEnable {
					sql += redact.String(sessVars.EnableRedactLog, sessVars.PlanCacheParams.String())
				}
				logutil.Logger(ctx).Warn("retrying",
					zap.Int64("schemaVersion", schemaVersion),
					zap.Uint("retryCnt", retryCnt),
					zap.Int("queryNum", i),
					zap.String("sql", sql))
			} else {
				logutil.Logger(ctx).Warn("retrying",
					zap.Int64("schemaVersion", schemaVersion),
					zap.Uint("retryCnt", retryCnt),
					zap.Int("queryNum", i))
			}
			_, digest := s.sessionVars.StmtCtx.SQLDigest()
			s.txn.onStmtStart(digest.String())
			if err = sessiontxn.GetTxnManager(s).OnStmtStart(ctx, st.GetStmtNode()); err == nil {
				_, err = st.Exec(ctx)
			}
			s.txn.onStmtEnd()
			if err != nil {
				s.StmtRollback(ctx, false)
				break
			}
			s.StmtCommit(ctx)
		}
		logutil.Logger(ctx).Warn("transaction association",
			zap.Uint64("retrying txnStartTS", s.GetSessionVars().TxnCtx.StartTS),
			zap.Uint64("original txnStartTS", orgStartTS))
		failpoint.Inject("preCommitHook", func() {
			hook, ok := ctx.Value("__preCommitHook").(func())
			if ok {
				hook()
			}
		})
		if err == nil {
			err = s.doCommit(ctx)
			if err == nil {
				break
			}
		}
		if !s.isTxnRetryableError(err) {
			logutil.Logger(ctx).Warn("sql",
				zap.String("label", label),
				zap.Stringer("session", s),
				zap.Error(err))
			metrics.SessionRetryErrorCounter.WithLabelValues(label, metrics.LblUnretryable).Inc()
			return err
		}
		retryCnt++
		if retryCnt >= maxCnt {
			logutil.Logger(ctx).Warn("sql",
				zap.String("label", label),
				zap.Uint("retry reached max count", retryCnt))
			metrics.SessionRetryErrorCounter.WithLabelValues(label, metrics.LblReachMax).Inc()
			return err
		}
		logutil.Logger(ctx).Warn("sql",
			zap.String("label", label),
			zap.Error(err),
			zap.String("txn", s.txn.GoString()))
		kv.BackOff(retryCnt)
		s.txn.changeToInvalid()
		s.sessionVars.SetInTxn(false)
	}
	return err
}

func sqlForLog(sql string) string {
	if len(sql) > sqlLogMaxLen {
		sql = sql[:sqlLogMaxLen] + fmt.Sprintf("(len:%d)", len(sql))
	}
	return executor.QueryReplacer.Replace(sql)
}

func (s *session) sysSessionPool() util.SessionPool {
	return domain.GetDomain(s).SysSessionPool()
}

func getSessionFactory(store kv.Storage) pools.Factory {
	facWithDom := getSessionFactoryInternal(store, func(store kv.Storage, _ *domain.Domain) (*session, error) {
		return createSession(store)
	})
	return func() (pools.Resource, error) {
		return facWithDom(nil)
	}
}

func getSessionFactoryWithDom(store kv.Storage) func(*domain.Domain) (pools.Resource, error) {
	return getSessionFactoryInternal(store, CreateSessionWithDomain)
}

func getCrossKSSessionFactory(currKSStore kv.Storage, targetKS string, schemaValidator validatorapi.Validator) pools.Factory {
	facWithDom := getSessionFactoryInternal(currKSStore, func(store kv.Storage, _ *domain.Domain) (*session, error) {
		return createCrossKSSession(store, targetKS, schemaValidator)
	})
	return func() (pools.Resource, error) {
		return facWithDom(nil)
	}
}

func getSessionFactoryInternal(store kv.Storage, createSessFn func(store kv.Storage, dom *domain.Domain) (*session, error)) func(*domain.Domain) (pools.Resource, error) {
	return func(dom *domain.Domain) (pools.Resource, error) {
		se, err := createSessFn(store, dom)
		if err != nil {
			return nil, err
		}
		err = se.sessionVars.SetSystemVar(vardef.AutoCommit, "1")
		if err != nil {
			return nil, err
		}
		err = se.sessionVars.SetSystemVar(vardef.MaxExecutionTime, "0")
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = se.sessionVars.SetSystemVar(vardef.MaxAllowedPacket, strconv.FormatUint(vardef.DefMaxAllowedPacket, 10))
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = se.sessionVars.SetSystemVar(vardef.TiDBConstraintCheckInPlacePessimistic, vardef.On)
		if err != nil {
			return nil, errors.Trace(err)
		}
		se.sessionVars.CommonGlobalLoaded = true
		se.sessionVars.InRestrictedSQL = true
		// Internal session uses default format to prevent memory leak problem.
		se.sessionVars.EnableChunkRPC = false
		return se, nil
	}
}

func drainRecordSet(ctx context.Context, se *session, rs sqlexec.RecordSet, alloc chunk.Allocator) ([]chunk.Row, error) {
	var rows []chunk.Row
	var req *chunk.Chunk
	req = rs.NewChunk(alloc)
	for {
		err := rs.Next(ctx, req)
		if err != nil || req.NumRows() == 0 {
			return rows, err
		}
		iter := chunk.NewIterator4Chunk(req)
		for r := iter.Begin(); r != iter.End(); r = iter.Next() {
			rows = append(rows, r)
		}
		req = chunk.Renew(req, se.sessionVars.MaxChunkSize)
	}
}

// getTableValue executes restricted sql and the result is one column.
// It returns a string value.
func (s *session) getTableValue(ctx context.Context, tblName string, varName string) (string, error) {
	if ctx.Value(kv.RequestSourceKey) == nil {
		ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnSysVar)
	}
	rows, fields, err := s.ExecRestrictedSQL(ctx, nil, "SELECT VARIABLE_VALUE FROM %n.%n WHERE VARIABLE_NAME=%?", mysql.SystemDB, tblName, varName)
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", errResultIsEmpty
	}
	d := rows[0].GetDatum(0, &fields[0].Column.FieldType)
	value, err := d.ToString()
	if err != nil {
		return "", err
	}
	return value, nil
}

// replaceGlobalVariablesTableValue executes restricted sql updates the variable value
// It will then notify the etcd channel that the value has changed.
func (s *session) replaceGlobalVariablesTableValue(ctx context.Context, varName, val string, updateLocal bool) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnSysVar)
	_, _, err := s.ExecRestrictedSQL(ctx, nil, `REPLACE INTO %n.%n (variable_name, variable_value) VALUES (%?, %?)`, mysql.SystemDB, mysql.GlobalVariablesTable, varName, val)
	if err != nil {
		return err
	}
	domain.GetDomain(s).NotifyUpdateSysVarCache(updateLocal)
	return err
}

// GetGlobalSysVar implements GlobalVarAccessor.GetGlobalSysVar interface.
func (s *session) GetGlobalSysVar(name string) (string, error) {
	if s.Value(sessionctx.Initing) != nil {
		// When running bootstrap or upgrade, we should not access global storage.
		return "", nil
	}

	sv := variable.GetSysVar(name)
	if sv == nil {
		// It might be a recently unregistered sysvar. We should return unknown
		// since GetSysVar is the canonical version, but we can update the cache
		// so the next request doesn't attempt to load this.
		logutil.BgLogger().Info("sysvar does not exist. sysvar cache may be stale", zap.String("name", name))
		return "", variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}

	sysVar, err := domain.GetDomain(s).GetGlobalVar(name)
	if err != nil {
		// The sysvar exists, but there is no cache entry yet.
		// This might be because the sysvar was only recently registered.
		// In which case it is safe to return the default, but we can also
		// update the cache for the future.
		logutil.BgLogger().Info("sysvar not in cache yet. sysvar cache may be stale", zap.String("name", name))
		sysVar, err = s.getTableValue(context.TODO(), mysql.GlobalVariablesTable, name)
		if err != nil {
			return sv.Value, nil
		}
	}
	// It might have been written from an earlier TiDB version, so we should do type validation
	// See https://github.com/pingcap/tidb/issues/30255 for why we don't do full validation.
	// If validation fails, we should return the default value:
	// See: https://github.com/pingcap/tidb/pull/31566
	sysVar, err = sv.ValidateFromType(s.GetSessionVars(), sysVar, vardef.ScopeGlobal)
	if err != nil {
		return sv.Value, nil
	}
	return sysVar, nil
}

// SetGlobalSysVar implements GlobalVarAccessor.SetGlobalSysVar interface.
func (s *session) SetGlobalSysVar(ctx context.Context, name string, value string) (err error) {
	sv := variable.GetSysVar(name)
	if sv == nil {
		return variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	if value, err = sv.Validate(s.sessionVars, value, vardef.ScopeGlobal); err != nil {
		return err
	}
	if err = sv.SetGlobalFromHook(ctx, s.sessionVars, value, false); err != nil {
		return err
	}
	if sv.GlobalConfigName != "" {
		domain.GetDomain(s).NotifyGlobalConfigChange(sv.GlobalConfigName, variable.OnOffToTrueFalse(value))
	}
	return s.replaceGlobalVariablesTableValue(context.TODO(), sv.Name, value, true)
}

// SetGlobalSysVarOnly updates the sysvar, but does not call the validation function or update aliases.
// This is helpful to prevent duplicate warnings being appended from aliases, or recursion.
// updateLocal indicates whether to rebuild the local SysVar Cache. This is helpful to prevent recursion.
func (s *session) SetGlobalSysVarOnly(ctx context.Context, name string, value string, updateLocal bool) (err error) {
	sv := variable.GetSysVar(name)
	if sv == nil {
		return variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	if err = sv.SetGlobalFromHook(ctx, s.sessionVars, value, true); err != nil {
		return err
	}
	return s.replaceGlobalVariablesTableValue(ctx, sv.Name, value, updateLocal)
}

// SetInstanceSysVar implements InstanceVarAccessor.SetInstanceSysVar interface.
func (s *session) SetInstanceSysVar(ctx context.Context, name string, value string) (err error) {
	sv := variable.GetSysVar(name)
	if sv == nil {
		return variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	if value, err = sv.Validate(s.sessionVars, value, vardef.ScopeInstance); err != nil {
		return err
	}
	return sv.SetGlobalFromHook(ctx, s.sessionVars, value, false)
}

// SetTiDBTableValue implements GlobalVarAccessor.SetTiDBTableValue interface.
func (s *session) SetTiDBTableValue(name, value, comment string) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnSysVar)
	_, _, err := s.ExecRestrictedSQL(ctx, nil, `REPLACE INTO mysql.tidb (variable_name, variable_value, comment) VALUES (%?, %?, %?)`, name, value, comment)
	return err
}

// GetTiDBTableValue implements GlobalVarAccessor.GetTiDBTableValue interface.
func (s *session) GetTiDBTableValue(name string) (string, error) {
	return s.getTableValue(context.TODO(), mysql.TiDBTable, name)
}

var _ sqlexec.SQLParser = &session{}

const (
	coreSQLToken = 1 << iota
	bypassSQLToken
	isSelectSQLToken

	defOOMRiskCheckDur   = time.Millisecond * 100 // 100ms: sleep duration when mem-arbitrator is at memory risk
	defSuffixSplitDot    = ", "
	defSuffixParseSQL    = defSuffixSplitDot + "path=ParseSQL"
	defSuffixCompilePlan = defSuffixSplitDot + "path=CompilePlan"

	// mem quota for compiling plan per token.
	// 1. prepare tpc-c
	// 2. run tpc-c workload with multiple threads for a few minutes
	// 3. observe the memory consumption of TiDB instance without copr-cache
	// 4. calculate the average memory consumption of compiling plan per token:
	//    executor.(*Compiler).Compile / threads / avg token count per SQL / 2 * 1.2(more 20%)
	defCompilePlanQuotaPerToken = 63091 * 12 / 10

	// mem quota for parsing SQL per token (similar method as above)
	//    session.(*session).ParseSQL / threads / avg token count per SQL / 2 * 1.2(more 20%)
	defParseSQLQuotaPerToken = 12036 * 12 / 10
)

var keySQLToken = map[string]int{
	"select": isSelectSQLToken,
	"from":   coreSQLToken, "insert": coreSQLToken, "update": coreSQLToken, "delete": coreSQLToken, "replace": coreSQLToken,
	// ignore prepare / execute statements
	"explain": bypassSQLToken, "desc": bypassSQLToken, "analyze": bypassSQLToken,
}

// approximate memory quota related token count for parsing a SQL statement which covers most DML statements
// 1. ignore comments
// 2. count keywords, identifiers, numbers, "?", string/identifier literals as one token
// 3. if the SQL has `select` clause, it must have `from` clause: ignore SQL like `select expr()` or `select @@var`
// 4. return 0 if the SQL has NO core token (e.g. `set`, `use`, `begin`, `commit`, `rollback`, etc)
func approxParseSQLTokenCnt(sql string) (tokenCnt int64) {
	f := false
	buffer := struct {
		d [10]byte
		n int
	}{}

	hitCoreToken := false
	hasSelect := false
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		if 'a' <= c && c <= 'z' || '0' <= c && c <= '9' || c == '_' {
			f = true
			if !hitCoreToken {
				if buffer.n < len(buffer.d) {
					buffer.d[buffer.n] = c
					buffer.n++
				}
			}
			continue
		}
		if f {
			f = false
			tokenCnt++
			if !hitCoreToken {
				token := keySQLToken[string(buffer.d[:buffer.n])]
				if token&isSelectSQLToken > 0 {
					hasSelect = true
				} else if token&coreSQLToken > 0 {
					hitCoreToken = true
				} else if token&bypassSQLToken == 0 {
					if !hasSelect {
						return 0
					}
					// expect `from` after `select`
				}
				buffer.n = 0
			}
		}
		if sql[i] == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			i += 2 // skip "/*"
			for i+1 < len(sql) && !(sql[i] == '*' && sql[i+1] == '/') {
				i++
			}
			i++ // skip "*/"
			continue
		}
		if sql[i] == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			i += 2 // skip "--"
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			continue
		}
		if sql[i] == '#' {
			i++ // skip "#"
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			continue
		}
		if sql[i] == '"' || sql[i] == '\'' {
			quote := sql[i]
			i++ // skip quote
			for i < len(sql) && sql[i] != quote {
				if sql[i] == '\\' && i+1 < len(sql) {
					i++ // skip escape character
				}
				i++
			}
			tokenCnt++
			continue
		}
		if sql[i] == '`' {
			i++ // skip "`"
			for i < len(sql) && sql[i] != '`' {
				if sql[i] == '\\' && i+1 < len(sql) {
					i++ // skip escape character
				}
				i++
			}
			tokenCnt++
			continue
		}
		if sql[i] == '?' {
			tokenCnt++
			continue
		}
	}
	if f {
		tokenCnt++
	}
	if !hitCoreToken {
		return 0
	}
	return
}

// approximate memory quota related token count for compiling plan of a `Normalized` SQL statement
// if the SQL has `select` clause, it must have `from` clause.
func approxCompilePlanTokenCnt(sql string, hasSelect bool) (tokenCnt int64) {
	const tokenFrom = "from"
	const lenTokenFrom = len(tokenFrom)
	n := 0
	hasSelectFrom := false
	for i, c := range sql {
		if 'a' <= c && c <= 'z' || '0' <= c && c <= '9' || c == '_' || c == '`' || c == '.' {
			n++
			continue
		}
		if n > 0 {
			tokenCnt++
			if hasSelect && !hasSelectFrom && n == lenTokenFrom && sql[i-lenTokenFrom:i] == tokenFrom {
				hasSelectFrom = true
			}
			n = 0
		}
		if c == '?' {
			tokenCnt++
			continue
		}
	}
	if n > 0 {
		tokenCnt++
	}
	if hasSelect && !hasSelectFrom {
		return 0
	}
	return
}

// approximate memory quota for parsing SQL
func approxParseSQLMemQuota(sql string) int64 {
	tokenCnt := approxParseSQLTokenCnt(sql)
	return tokenCnt * defParseSQLQuotaPerToken
}

// approximate memory quota for compiling plan of a `Normalized` SQL statement
func approxCompilePlanMemQuota(sql string, hasSelect bool) int64 {
	tokenCnt := approxCompilePlanTokenCnt(sql, hasSelect)
	return tokenCnt * defCompilePlanQuotaPerToken
}

func (s *session) ParseSQL(ctx context.Context, sql string, params ...parser.ParseParam) ([]ast.StmtNode, []error, error) {
	globalMemArbitrator := memory.GlobalMemArbitrator()
	execUseArbitrator := false
	parseSQLMemQuota := int64(0)
	if globalMemArbitrator != nil && s.sessionVars.ConnectionID != 0 {
		if s.sessionVars.MemArbitrator.WaitAverse != variable.MemArbitratorNolimit {
			parseSQLMemQuota = approxParseSQLMemQuota(sql)
			execUseArbitrator = parseSQLMemQuota > 0
		}
	}

	if execUseArbitrator {
		uid := s.sessionVars.ConnectionID

		if globalMemArbitrator.AtMemRisk() {
			if s.sessionPlanCache != nil {
				s.sessionPlanCache.DeleteAll()
			}
			for globalMemArbitrator.AtMemRisk() {
				if globalMemArbitrator.AtOOMRisk() {
					metrics.GlobalMemArbitratorSubTasks.ForceKillParse.Inc()
					return nil, nil, exeerrors.ErrQueryExecStopped.GenWithStackByArgs(memory.ArbitratorOOMRiskKill.String()+defSuffixParseSQL, uid)
				}
				time.Sleep(defOOMRiskCheckDur)
			}
		}

		globalMemArbitrator.ConsumeQuotaFromAwaitFreePool(uid, parseSQLMemQuota)
		defer globalMemArbitrator.ConsumeQuotaFromAwaitFreePool(uid, -parseSQLMemQuota)
	}

	defer tracing.StartRegion(ctx, "ParseSQL").End()
	p := parserutil.GetParser()
	defer func() {
		parserutil.DestroyParser(p)
	}()

	sqlMode := s.sessionVars.SQLMode
	if s.isInternal() {
		sqlMode = mysql.DelSQLMode(sqlMode, mysql.ModeNoBackslashEscapes)
	}
	p.SetSQLMode(sqlMode)
	p.SetParserConfig(s.sessionVars.BuildParserConfig())
	tmp, warn, err := p.ParseSQL(sql, params...)
	// The []ast.StmtNode is referenced by the parser, to reuse the parser, make a copy of the result.
	res := slices.Clone(tmp)
	return res, warn, err
}

func (s *session) SetProcessInfo(sql string, t time.Time, command byte, maxExecutionTime uint64) {
	// If command == mysql.ComSleep, it means the SQL execution is finished. The processinfo is reset to SLEEP.
	// If the SQL finished and the session is not in transaction, the current start timestamp need to reset to 0.
	// Otherwise, it should be set to the transaction start timestamp.
	// Why not reset the transaction start timestamp to 0 when transaction committed?
	// Because the select statement and other statements need this timestamp to read data,
	// after the transaction is committed. e.g. SHOW MASTER STATUS;
	var curTxnStartTS uint64
	var curTxnCreateTime time.Time
	if command != mysql.ComSleep || s.GetSessionVars().InTxn() {
		curTxnStartTS = s.sessionVars.TxnCtx.StartTS
		curTxnCreateTime = s.sessionVars.TxnCtx.CreateTime

		// For stale read and autocommit path, the `TxnCtx.StartTS` is 0.
		if curTxnStartTS == 0 {
			curTxnStartTS = s.sessionVars.TxnCtx.StaleReadTs
		}
	}
	// Set curTxnStartTS to SnapshotTS directly when the session is trying to historic read.
	// It will avoid the session meet GC lifetime too short error.
	if s.GetSessionVars().SnapshotTS != 0 {
		curTxnStartTS = s.GetSessionVars().SnapshotTS
	}
	p := s.currentPlan
	if explain, ok := p.(*plannercore.Explain); ok && explain.Analyze && explain.TargetPlan != nil {
		p = explain.TargetPlan
	}

	sqlCPUUsages := &s.sessionVars.SQLCPUUsages
	// If command == mysql.ComSleep, it means the SQL execution is finished. Then cpu usages should be nil.
	if command == mysql.ComSleep {
		sqlCPUUsages = nil
	}

	pi := sessmgr.ProcessInfo{
		ID:                    s.sessionVars.ConnectionID,
		Port:                  s.sessionVars.Port,
		DB:                    s.sessionVars.CurrentDB,
		Command:               command,
		Plan:                  p,
		BriefBinaryPlan:       plannercore.GetBriefBinaryPlan(p),
		RuntimeStatsColl:      s.sessionVars.StmtCtx.RuntimeStatsColl,
		Time:                  t,
		State:                 s.Status(),
		Info:                  sql,
		CurTxnStartTS:         curTxnStartTS,
		CurTxnCreateTime:      curTxnCreateTime,
		StmtCtx:               s.sessionVars.StmtCtx,
		SQLCPUUsage:           sqlCPUUsages,
		RefCountOfStmtCtx:     &s.sessionVars.RefCountOfStmtCtx,
		MemTracker:            s.sessionVars.MemTracker,
		DiskTracker:           s.sessionVars.DiskTracker,
		RunawayChecker:        s.sessionVars.StmtCtx.RunawayChecker,
		StatsInfo:             physicalop.GetStatsInfo,
		OOMAlarmVariablesInfo: s.getOomAlarmVariablesInfo(),
		TableIDs:              s.sessionVars.StmtCtx.TableIDs,
		IndexNames:            s.sessionVars.StmtCtx.IndexNames,
		MaxExecutionTime:      maxExecutionTime,
		RedactSQL:             s.sessionVars.EnableRedactLog,
		ResourceGroupName:     s.sessionVars.StmtCtx.ResourceGroupName,
		SessionAlias:          s.sessionVars.SessionAlias,
		CursorTracker:         s.cursorTracker,
	}
	oldPi := s.ShowProcess()
	if p == nil {
		// Store the last valid plan when the current plan is nil.
		// This is for `explain for connection` statement has the ability to query the last valid plan.
		if oldPi != nil && oldPi.Plan != nil && len(oldPi.BriefBinaryPlan) > 0 {
			pi.Plan = oldPi.Plan
			pi.RuntimeStatsColl = oldPi.RuntimeStatsColl
			pi.BriefBinaryPlan = oldPi.BriefBinaryPlan
		}
	}
	// We set process info before building plan, so we extended execution time.
	if oldPi != nil && oldPi.Info == pi.Info && oldPi.Command == pi.Command {
		pi.Time = oldPi.Time
	}
	if oldPi != nil && oldPi.CurTxnStartTS != 0 && oldPi.CurTxnStartTS == pi.CurTxnStartTS {
		// Keep the last expensive txn log time, avoid print too many expensive txn logs.
		pi.ExpensiveTxnLogTime = oldPi.ExpensiveTxnLogTime
	}
	_, digest := s.sessionVars.StmtCtx.SQLDigest()
	pi.Digest = digest.String()
	// DO NOT reset the currentPlan to nil until this query finishes execution, otherwise reentrant calls
	// of SetProcessInfo would override Plan and BriefBinaryPlan to nil.
	if command == mysql.ComSleep {
		s.currentPlan = nil
	}
	if s.sessionVars.User != nil {
		pi.User = s.sessionVars.User.Username
		pi.Host = s.sessionVars.User.Hostname
	}
	s.processInfo.Store(&pi)
}

// UpdateProcessInfo updates the session's process info for the running statement.
func (s *session) UpdateProcessInfo() {
	pi := s.ShowProcess()
	if pi == nil || pi.CurTxnStartTS != 0 {
		return
	}
	// do not modify this two fields in place, see issue: issues/50607
	shallowCP := pi.Clone()
	// Update the current transaction start timestamp.
	shallowCP.CurTxnStartTS = s.sessionVars.TxnCtx.StartTS
	if shallowCP.CurTxnStartTS == 0 {
		// For stale read and autocommit path, the `TxnCtx.StartTS` is 0.
		shallowCP.CurTxnStartTS = s.sessionVars.TxnCtx.StaleReadTs
	}
	shallowCP.CurTxnCreateTime = s.sessionVars.TxnCtx.CreateTime
	s.processInfo.Store(shallowCP)
}

func (s *session) getOomAlarmVariablesInfo() sessmgr.OOMAlarmVariablesInfo {
	return sessmgr.OOMAlarmVariablesInfo{
		SessionAnalyzeVersion:         s.sessionVars.AnalyzeVersion,
		SessionEnabledRateLimitAction: s.sessionVars.EnabledRateLimitAction,
		SessionMemQuotaQuery:          s.sessionVars.MemQuotaQuery,
	}
}

func (s *session) ExecuteInternal(ctx context.Context, sql string, args ...any) (rs sqlexec.RecordSet, err error) {
	if sink := tracing.GetSink(ctx); sink == nil {
		trace := traceevent.NewTrace()
		ctx = tracing.WithFlightRecorder(ctx, trace)
		defer trace.DiscardOrFlush(ctx)

		// A developer debugging event so we can see what trace is missing!
		if traceevent.IsEnabled(tracing.DevDebug) {
			traceevent.TraceEvent(ctx, tracing.DevDebug, "ExecuteInternal missing trace ctx",
				zap.String("sql", sql),
				zap.Stack("stack"))
			traceevent.CheckFlightRecorderDumpTrigger(ctx, "dump_trigger.suspicious_event.dev_debug", func(config *traceevent.DumpTriggerConfig) bool {
				return config.Event.DevDebug.Type == traceevent.DevDebugTypeExecuteInternalTraceMissing
			})
		}
	}

	rs, err = s.executeInternalImpl(ctx, sql, args...)
	return rs, err
}

func (s *session) executeInternalImpl(ctx context.Context, sql string, args ...any) (rs sqlexec.RecordSet, err error) {
	origin := s.sessionVars.InRestrictedSQL
	s.sessionVars.InRestrictedSQL = true
	defer func() {
		s.sessionVars.InRestrictedSQL = origin
		// Restore the goroutine label by using the original ctx after execution is finished.
		pprof.SetGoroutineLabels(ctx)
	}()

	r, ctx := tracing.StartRegionEx(ctx, "session.ExecuteInternal")
	defer r.End()
	logutil.Eventf(ctx, "execute: %s", sql)

	stmtNode, err := s.ParseWithParams(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	rs, err = s.ExecuteStmt(ctx, stmtNode)
	if err != nil {
		s.sessionVars.StmtCtx.AppendError(err)
	}
	if rs == nil {
		return nil, err
	}

	return rs, err
}

// Execute is deprecated, we can remove it as soon as plugins are migrated.
func (s *session) Execute(ctx context.Context, sql string) (recordSets []sqlexec.RecordSet, err error) {
	r, ctx := tracing.StartRegionEx(ctx, "session.Execute")
	defer r.End()
	logutil.Eventf(ctx, "execute: %s", sql)

	stmtNodes, err := s.Parse(ctx, sql)
	if err != nil {
		return nil, err
	}
	if len(stmtNodes) != 1 {
		return nil, errors.New("Execute() API doesn't support multiple statements any more")
	}

	rs, err := s.ExecuteStmt(ctx, stmtNodes[0])
	if err != nil {
		s.sessionVars.StmtCtx.AppendError(err)
	}
	if rs == nil {
		return nil, err
	}
	return []sqlexec.RecordSet{rs}, err
}

type sqlRegexp struct {
	regexp string
}

func (s sqlRegexp) sqlRegexpDumpTriggerCheck(cfg *traceevent.DumpTriggerConfig) bool {
	// TODO: pre-compile the regexp to improve performance
	match, err := regexp.MatchString(cfg.UserCommand.SQLRegexp, s.regexp)
	return err == nil && match
}

// Parse parses a query string to raw ast.StmtNode.
func (s *session) Parse(ctx context.Context, sql string) ([]ast.StmtNode, error) {
	logutil.Logger(ctx).Debug("parse", zap.String("sql", sql))
	parseStartTime := time.Now()

	traceevent.CheckFlightRecorderDumpTrigger(ctx, "dump_trigger.user_command.sql_regexp", sqlRegexp{sql}.sqlRegexpDumpTriggerCheck)

	// Load the session variables to the context.
	// This is necessary for the parser to get the current sql_mode.
	if err := s.loadCommonGlobalVariablesIfNeeded(); err != nil {
		return nil, err
	}

	stmts, warns, err := s.ParseSQL(ctx, sql, s.sessionVars.GetParseParams()...)
	if err != nil {
		s.rollbackOnError(ctx)
		err = util.SyntaxError(err)

		// Only print log message when this SQL is from the user.
		// Mute the warning for internal SQLs.
		if !s.sessionVars.InRestrictedSQL {
			logutil.Logger(ctx).Warn("parse SQL failed", zap.Error(err), zap.String("SQL", redact.String(s.sessionVars.EnableRedactLog, sql)))
			s.sessionVars.StmtCtx.AppendError(err)
		}
		return nil, err
	}

	durParse := time.Since(parseStartTime)
	s.GetSessionVars().DurationParse = durParse
	isInternal := s.isInternal()
	if isInternal {
		session_metrics.SessionExecuteParseDurationInternal.Observe(durParse.Seconds())
	} else {
		session_metrics.SessionExecuteParseDurationGeneral.Observe(durParse.Seconds())
	}
	for _, warn := range warns {
		s.sessionVars.StmtCtx.AppendWarning(util.SyntaxWarn(warn))
	}
	return stmts, nil
}

// ParseWithParams parses a query string, with arguments, to raw ast.StmtNode.
// Note that it will not do escaping if no variable arguments are passed.
func (s *session) ParseWithParams(ctx context.Context, sql string, args ...any) (ast.StmtNode, error) {
	var err error
	if len(args) > 0 {
		sql, err = sqlescape.EscapeSQL(sql, args...)
		if err != nil {
			return nil, err
		}
	}

	internal := s.isInternal()

	var stmts []ast.StmtNode
	var warns []error
	parseStartTime := time.Now()
	if internal {
		// Do no respect the settings from clients, if it is for internal usage.
		// Charsets from clients may give chance injections.
		// Refer to https://stackoverflow.com/questions/5741187/sql-injection-that-gets-around-mysql-real-escape-string/12118602.
		stmts, warns, err = s.ParseSQL(ctx, sql)
	} else {
		stmts, warns, err = s.ParseSQL(ctx, sql, s.sessionVars.GetParseParams()...)
	}
	if len(stmts) != 1 && err == nil {
		err = errors.New("run multiple statements internally is not supported")
	}
	if err != nil {
		s.rollbackOnError(ctx)
		logSQL := sql[:min(500, len(sql))]
		logutil.Logger(ctx).Warn("parse SQL failed", zap.Error(err), zap.String("SQL", redact.String(s.sessionVars.EnableRedactLog, logSQL)))
		return nil, util.SyntaxError(err)
	}
	durParse := time.Since(parseStartTime)
	if internal {
		session_metrics.SessionExecuteParseDurationInternal.Observe(durParse.Seconds())
	} else {
		session_metrics.SessionExecuteParseDurationGeneral.Observe(durParse.Seconds())
	}
	for _, warn := range warns {
		s.sessionVars.StmtCtx.AppendWarning(util.SyntaxWarn(warn))
	}
	if topsqlstate.TopProfilingEnabled() {
		normalized, digest := parser.NormalizeDigest(sql)
		if digest != nil {
			// Reset the goroutine label when internal sql execute finish.
			// Specifically reset in ExecRestrictedStmt function.
			s.sessionVars.StmtCtx.IsSQLRegistered.Store(true)
			topsql.AttachAndRegisterSQLInfo(ctx, normalized, digest, s.sessionVars.InRestrictedSQL)
		}
	}
	return stmts[0], nil
}

// GetAdvisoryLock acquires an advisory lock of lockName.
// Note that a lock can be acquired multiple times by the same session,
// in which case we increment a reference count.
// Each lock needs to be held in a unique session because
// we need to be able to ROLLBACK in any arbitrary order
// in order to release the locks.
func (s *session) GetAdvisoryLock(lockName string, timeout int64) error {
	if lock, ok := s.advisoryLocks[lockName]; ok {
		lock.IncrReferences()
		return nil
	}
	se, clean, err := s.getInternalSession(sqlexec.GetExecOption(nil))
	if err != nil {
		return err
	}
	lock := &advisoryLock{session: se, ctx: context.TODO(), owner: s.ShowProcess().ID, clean: clean}
	err = lock.GetLock(lockName, timeout)
	if err != nil {
		return err
	}
	s.advisoryLocks[lockName] = lock
	return nil
}

// IsUsedAdvisoryLock checks if a lockName is already in use
func (s *session) IsUsedAdvisoryLock(lockName string) uint64 {
	// Same session
	if lock, ok := s.advisoryLocks[lockName]; ok {
		return lock.owner
	}

	// Check for transaction on advisory_locks table
	se, clean, err := s.getInternalSession(sqlexec.GetExecOption(nil))
	if err != nil {
		return 0
	}
	lock := &advisoryLock{session: se, ctx: context.TODO(), owner: s.ShowProcess().ID, clean: clean}
	err = lock.IsUsedLock(lockName)
	if err != nil {
		// TODO: Return actual owner pid
		// TODO: Check for mysql.ErrLockWaitTimeout and DeadLock
		return 1
	}
	return 0
}

// ReleaseAdvisoryLock releases an advisory locks held by the session.
// It returns FALSE if no lock by this name was held (by this session),
// and TRUE if a lock was held and "released".
// Note that the lock is not actually released if there are multiple
// references to the same lockName by the session, instead the reference
// count is decremented.
func (s *session) ReleaseAdvisoryLock(lockName string) (released bool) {
	if lock, ok := s.advisoryLocks[lockName]; ok {
		lock.DecrReferences()
		if lock.ReferenceCount() <= 0 {
			lock.Close()
			delete(s.advisoryLocks, lockName)
		}
		return true
	}
	return false
}

// ReleaseAllAdvisoryLocks releases all advisory locks held by the session
// and returns a count of the locks that were released.
// The count is based on unique locks held, so multiple references
// to the same lock do not need to be accounted for.
func (s *session) ReleaseAllAdvisoryLocks() int {
	var count int
	for lockName, lock := range s.advisoryLocks {
		lock.Close()
		count += lock.ReferenceCount()
		delete(s.advisoryLocks, lockName)
	}
	return count
}

// GetExtensions returns the `*extension.SessionExtensions` object
func (s *session) GetExtensions() *extension.SessionExtensions {
	return s.extensions
}

// SetExtensions sets the `*extension.SessionExtensions` object
func (s *session) SetExtensions(extensions *extension.SessionExtensions) {
	s.extensions = extensions
}

// InSandBoxMode indicates that this session is in sandbox mode
func (s *session) InSandBoxMode() bool {
	return s.sandBoxMode
}

// EnableSandBoxMode enable the sandbox mode.
func (s *session) EnableSandBoxMode() {
	s.sandBoxMode = true
}

// DisableSandBoxMode enable the sandbox mode.
func (s *session) DisableSandBoxMode() {
	s.sandBoxMode = false
}

// ParseWithParams4Test wrapper (s *session) ParseWithParams for test
func ParseWithParams4Test(ctx context.Context, s sessionapi.Session,
	sql string, args ...any) (ast.StmtNode, error) {
	return s.(*session).ParseWithParams(ctx, sql, args)
}

var _ sqlexec.RestrictedSQLExecutor = &session{}
var _ sqlexec.SQLExecutor = &session{}

// ExecRestrictedStmt implements RestrictedSQLExecutor interface.
func (s *session) ExecRestrictedStmt(ctx context.Context, stmtNode ast.StmtNode, opts ...sqlexec.OptionFuncAlias) (
	[]chunk.Row, []*resolve.ResultField, error) {
	defer pprof.SetGoroutineLabels(ctx)
	execOption := sqlexec.GetExecOption(opts)
	var se *session
	var clean func()
	var err error
	if execOption.UseCurSession {
		se, clean, err = s.useCurrentSession(execOption)
	} else {
		se, clean, err = s.getInternalSession(execOption)
	}
	if err != nil {
		return nil, nil, err
	}
	defer clean()

	startTime := time.Now()
	metrics.SessionRestrictedSQLCounter.Inc()
	ctx = execdetails.ContextWithInitializedExecDetails(ctx)
	rs, err := se.ExecuteStmt(ctx, stmtNode)
	if err != nil {
		se.sessionVars.StmtCtx.AppendError(err)
	}
	if rs == nil {
		return nil, nil, err
	}
	defer func() {
		if closeErr := rs.Close(); closeErr != nil {
			err = closeErr
		}
	}()
	var rows []chunk.Row
	rows, err = drainRecordSet(ctx, se, rs, nil)
	if err != nil {
		return nil, nil, err
	}

	vars := se.GetSessionVars()
	for _, dbName := range GetDBNames(vars) {
		metrics.QueryDurationHistogram.WithLabelValues(metrics.LblInternal, dbName, vars.StmtCtx.ResourceGroupName).Observe(time.Since(startTime).Seconds())
	}
	return rows, rs.Fields(), err
}

// ExecRestrictedStmt4Test wrapper `(s *session) ExecRestrictedStmt` for test.
func ExecRestrictedStmt4Test(ctx context.Context, s sessionapi.Session,
	stmtNode ast.StmtNode, opts ...sqlexec.OptionFuncAlias) (
	[]chunk.Row, []*resolve.ResultField, error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	return s.(*session).ExecRestrictedStmt(ctx, stmtNode, opts...)
}

// only set and clean session with execOption
func (s *session) useCurrentSession(execOption sqlexec.ExecOption) (*session, func(), error) {
	var err error
	orgSnapshotInfoSchema, orgSnapshotTS := s.sessionVars.SnapshotInfoschema, s.sessionVars.SnapshotTS
	if execOption.SnapshotTS != 0 {
		if err = s.sessionVars.SetSystemVar(vardef.TiDBSnapshot, strconv.FormatUint(execOption.SnapshotTS, 10)); err != nil {
			return nil, nil, err
		}
		s.sessionVars.SnapshotInfoschema, err = getSnapshotInfoSchema(s, execOption.SnapshotTS)
		if err != nil {
			return nil, nil, err
		}
	}
	prevStatsVer := s.sessionVars.AnalyzeVersion
	if execOption.AnalyzeVer != 0 {
		s.sessionVars.AnalyzeVersion = execOption.AnalyzeVer
	}
	prevAnalyzeSnapshot := s.sessionVars.EnableAnalyzeSnapshot
	if execOption.AnalyzeSnapshot != nil {
		s.sessionVars.EnableAnalyzeSnapshot = *execOption.AnalyzeSnapshot
	}
	s.sessionVars.EnableDDLAnalyzeExecOpt = execOption.EnableDDLAnalyze
	prePruneMode := s.sessionVars.PartitionPruneMode.Load()
	if len(execOption.PartitionPruneMode) > 0 {
		s.sessionVars.PartitionPruneMode.Store(execOption.PartitionPruneMode)
	}
	prevSQL := s.sessionVars.StmtCtx.OriginalSQL
	prevStmtType := s.sessionVars.StmtCtx.StmtType
	prevTables := s.sessionVars.StmtCtx.Tables
	return s, func() {
		s.sessionVars.AnalyzeVersion = prevStatsVer
		s.sessionVars.EnableAnalyzeSnapshot = prevAnalyzeSnapshot
		if err := s.sessionVars.SetSystemVar(vardef.TiDBSnapshot, ""); err != nil {
			logutil.BgLogger().Error("set tidbSnapshot error", zap.Error(err))
		}
		s.sessionVars.SnapshotInfoschema = orgSnapshotInfoSchema
		s.sessionVars.SnapshotTS = orgSnapshotTS
		s.sessionVars.PartitionPruneMode.Store(prePruneMode)
		s.sessionVars.StmtCtx.OriginalSQL = prevSQL
		s.sessionVars.StmtCtx.StmtType = prevStmtType
		s.sessionVars.StmtCtx.Tables = prevTables
		s.sessionVars.MemTracker.Detach()
	}, nil
}

func (s *session) getInternalSession(execOption sqlexec.ExecOption) (*session, func(), error) {
	tmp, err := s.sysSessionPool().Get()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	se := tmp.(*session)

	// The special session will share the `InspectionTableCache` with current session
	// if the current session in inspection mode.
	if cache := s.sessionVars.InspectionTableCache; cache != nil {
		se.sessionVars.InspectionTableCache = cache
	}
	se.sessionVars.OptimizerUseInvisibleIndexes = s.sessionVars.OptimizerUseInvisibleIndexes

	preSkipStats := s.sessionVars.SkipMissingPartitionStats
	se.sessionVars.SkipMissingPartitionStats = s.sessionVars.SkipMissingPartitionStats

	if execOption.SnapshotTS != 0 {
		if err := se.sessionVars.SetSystemVar(vardef.TiDBSnapshot, strconv.FormatUint(execOption.SnapshotTS, 10)); err != nil {
			return nil, nil, err
		}
		se.sessionVars.SnapshotInfoschema, err = getSnapshotInfoSchema(s, execOption.SnapshotTS)
		if err != nil {
			return nil, nil, err
		}
	}

	prevStatsVer := se.sessionVars.AnalyzeVersion
	if execOption.AnalyzeVer != 0 {
		se.sessionVars.AnalyzeVersion = execOption.AnalyzeVer
	}

	prevAnalyzeSnapshot := se.sessionVars.EnableAnalyzeSnapshot
	if execOption.AnalyzeSnapshot != nil {
		se.sessionVars.EnableAnalyzeSnapshot = *execOption.AnalyzeSnapshot
	}

	prePruneMode := se.sessionVars.PartitionPruneMode.Load()
	if len(execOption.PartitionPruneMode) > 0 {
		se.sessionVars.PartitionPruneMode.Store(execOption.PartitionPruneMode)
	}
	se.sessionVars.EnableDDLAnalyzeExecOpt = execOption.EnableDDLAnalyze
	return se, func() {
		se.sessionVars.AnalyzeVersion = prevStatsVer
		se.sessionVars.EnableAnalyzeSnapshot = prevAnalyzeSnapshot
		if err := se.sessionVars.SetSystemVar(vardef.TiDBSnapshot, ""); err != nil {
			logutil.BgLogger().Error("set tidbSnapshot error", zap.Error(err))
		}
		se.sessionVars.SnapshotInfoschema = nil
		se.sessionVars.SnapshotTS = 0
		if !execOption.IgnoreWarning {
			if se != nil && se.GetSessionVars().StmtCtx.WarningCount() > 0 {
				warnings := se.GetSessionVars().StmtCtx.GetWarnings()
				s.GetSessionVars().StmtCtx.AppendWarnings(warnings)
			}
		}
		se.sessionVars.PartitionPruneMode.Store(prePruneMode)
		se.sessionVars.OptimizerUseInvisibleIndexes = false
		se.sessionVars.SkipMissingPartitionStats = preSkipStats
		se.sessionVars.InspectionTableCache = nil
		se.sessionVars.MemTracker.Detach()
		s.sysSessionPool().Put(tmp)
	}, nil
}

func (s *session) withRestrictedSQLExecutor(ctx context.Context, opts []sqlexec.OptionFuncAlias, fn func(context.Context, *session) ([]chunk.Row, []*resolve.ResultField, error)) ([]chunk.Row, []*resolve.ResultField, error) {
	execOption := sqlexec.GetExecOption(opts)
	var se *session
	var clean func()
	var err error
	if execOption.UseCurSession {
		se, clean, err = s.useCurrentSession(execOption)
	} else {
		se, clean, err = s.getInternalSession(execOption)
	}
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	defer clean()
	if execOption.TrackSysProcID > 0 {
		err = execOption.TrackSysProc(execOption.TrackSysProcID, se)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		// unTrack should be called before clean (return sys session)
		defer execOption.UnTrackSysProc(execOption.TrackSysProcID)
	}
	return fn(ctx, se)
}

func (s *session) ExecRestrictedSQL(ctx context.Context, opts []sqlexec.OptionFuncAlias, sql string, params ...any) ([]chunk.Row, []*resolve.ResultField, error) {
	return s.withRestrictedSQLExecutor(ctx, opts, func(ctx context.Context, se *session) ([]chunk.Row, []*resolve.ResultField, error) {
		stmt, err := se.ParseWithParams(ctx, sql, params...)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		defer pprof.SetGoroutineLabels(ctx)
		startTime := time.Now()
		metrics.SessionRestrictedSQLCounter.Inc()
		ctx = execdetails.ContextWithInitializedExecDetails(ctx)
		rs, err := se.ExecuteInternalStmt(ctx, stmt)
		if err != nil {
			se.sessionVars.StmtCtx.AppendError(err)
		}
		if rs == nil {
			return nil, nil, err
		}
		defer func() {
			if closeErr := rs.Close(); closeErr != nil {
				err = closeErr
			}
		}()
		var rows []chunk.Row
		rows, err = drainRecordSet(ctx, se, rs, nil)
		if err != nil {
			return nil, nil, err
		}

		vars := se.GetSessionVars()
		for _, dbName := range GetDBNames(vars) {
			metrics.QueryDurationHistogram.WithLabelValues(metrics.LblInternal, dbName, vars.StmtCtx.ResourceGroupName).Observe(time.Since(startTime).Seconds())
		}
		return rows, rs.Fields(), err
	})
}

// ExecuteInternalStmt execute internal stmt
func (s *session) ExecuteInternalStmt(ctx context.Context, stmtNode ast.StmtNode) (sqlexec.RecordSet, error) {
	origin := s.sessionVars.InRestrictedSQL
	s.sessionVars.InRestrictedSQL = true
	defer func() {
		s.sessionVars.InRestrictedSQL = origin
		// Restore the goroutine label by using the original ctx after execution is finished.
		pprof.SetGoroutineLabels(ctx)
	}()
	return s.ExecuteStmt(ctx, stmtNode)
}

func queryFailDumpTriggerCheck(config *traceevent.DumpTriggerConfig) bool {
	return config.Event.Type == "query_fail"
}

type isInternalAlias struct {
	bool
}

// isInternalAlias is not intuitive, but it is defined to avoid allocation.
// If the code is written as
//
//		traceevent.CheckFlightRecorderDumpTrigger(ctx, "dump_trigger.suspicious_event.is_internal", func(conf *traceevent.DumpTriggerConfig) {
//	     	conf.Event.IsInternal = conf.Event.IsInternal
//		})
//
// It's uncertain whether the Go compiler escape analysis is powerful enough to avoid allocation for the closure object.
// isInternalAlias is defined to help the compiler, this coding style will not cause closure object allocation.
//
//	traceevent.CheckFlightRecorderDumpTrigger(ctx, "dump_trigger.suspicious_event.is_internal", isInternalAlias{s.isInternal()}.isInternalDumpTriggerCheck)
func (i isInternalAlias) isInternalDumpTriggerCheck(config *traceevent.DumpTriggerConfig) bool {
	return config.Event.IsInternal == i.bool
}

func (s *session) ExecuteStmt(ctx context.Context, stmtNode ast.StmtNode) (sqlexec.RecordSet, error) {
	if fr := traceevent.GetFlightRecorder(); fr != nil {
		traceevent.CheckFlightRecorderDumpTrigger(ctx, "dump_trigger.sampling", fr.CheckSampling)
	}
	rs, err := s.executeStmtImpl(ctx, stmtNode)
	if err != nil {
		traceevent.CheckFlightRecorderDumpTrigger(ctx, "dump_trigger.suspicious_event", queryFailDumpTriggerCheck)
	}
	return rs, err
}

func (s *session) executeStmtImpl(ctx context.Context, stmtNode ast.StmtNode) (sqlexec.RecordSet, error) {
	r, ctx := tracing.StartRegionEx(ctx, "session.ExecuteStmt")
	defer r.End()

	if err := s.PrepareTxnCtx(ctx, stmtNode); err != nil {
		return nil, err
	}
	if err := s.loadCommonGlobalVariablesIfNeeded(); err != nil {
		return nil, err
	}

	sessVars := s.sessionVars
	sessVars.StartTime = time.Now()
	traceevent.CheckFlightRecorderDumpTrigger(ctx, "dump_trigger.suspicious_event.is_internal", isInternalAlias{s.isInternal()}.isInternalDumpTriggerCheck)

	// Some executions are done in compile stage, so we reset them before compile.
	if err := executor.ResetContextOfStmt(s, stmtNode); err != nil {
		return nil, err
	}

	if execStmt, ok := stmtNode.(*ast.ExecuteStmt); ok {
		if binParam, ok := execStmt.BinaryArgs.([]param.BinaryParam); ok {
			args, err := expression.ExecBinaryParam(s.GetSessionVars().StmtCtx.TypeCtx(), binParam)
			if err != nil {
				return nil, err
			}
			execStmt.BinaryArgs = args
		}
	}

	normalizedSQL, digest := s.sessionVars.StmtCtx.SQLDigest()
	cmdByte := byte(atomic.LoadUint32(&s.GetSessionVars().CommandValue))
	if topsqlstate.TopProfilingEnabled() {
		s.sessionVars.StmtCtx.IsSQLRegistered.Store(true)
		ctx = topsql.AttachAndRegisterSQLInfo(ctx, normalizedSQL, digest, s.sessionVars.InRestrictedSQL)
	}

	if err := s.validateStatementInTxn(stmtNode); err != nil {
		return nil, err
	}

	if err := s.validateStatementReadOnlyInStaleness(stmtNode); err != nil {
		return nil, err
	}

	// Uncorrelated subqueries will execute once when building plan, so we reset process info before building plan.
	s.currentPlan = nil // reset current plan
	s.SetProcessInfo(stmtNode.Text(), time.Now(), cmdByte, 0)
	s.txn.onStmtStart(digest.String())
	defer sessiontxn.GetTxnManager(s).OnStmtEnd()
	defer s.txn.onStmtEnd()

	if err := s.onTxnManagerStmtStartOrRetry(ctx, stmtNode); err != nil {
		return nil, err
	}

	failpoint.Inject("mockStmtSlow", func(val failpoint.Value) {
		if strings.Contains(stmtNode.Text(), "/* sleep */") {
			v, _ := val.(int)
			time.Sleep(time.Duration(v) * time.Millisecond)
		}
	})

	var stmtLabel string
	if execStmt, ok := stmtNode.(*ast.ExecuteStmt); ok {
		prepareStmt, err := plannercore.GetPreparedStmt(execStmt, s.sessionVars)
		if err == nil && prepareStmt.PreparedAst != nil {
			stmtLabel = stmtctx.GetStmtLabel(ctx, prepareStmt.PreparedAst.Stmt)
		}
	}
	if stmtLabel == "" {
		stmtLabel = stmtctx.GetStmtLabel(ctx, stmtNode)
	}
	s.setRequestSource(ctx, stmtLabel, stmtNode)

	globalMemArbitrator := memory.GlobalMemArbitrator()
	arbitratorMode := globalMemArbitrator.WorkMode()
	enableWaitAverse := sessVars.MemArbitrator.WaitAverse == variable.MemArbitratorWaitAverseEnable
	execUseArbitrator := globalMemArbitrator != nil && sessVars.ConnectionID != 0 &&
		sessVars.MemArbitrator.WaitAverse != variable.MemArbitratorNolimit &&
		sessVars.StmtCtx.MemSensitive
	compilePlanMemQuota := int64(0) // mem quota for compiler & optimizer
	if execUseArbitrator {
		compilePlanMemQuota = approxCompilePlanMemQuota(normalizedSQL, sessVars.StmtCtx.InSelectStmt)
		execUseArbitrator = compilePlanMemQuota > 0
	}

	releaseCommonQuota := func() { // release common quota
		if compilePlanMemQuota > 0 {
			_ = globalMemArbitrator.ConsumeQuotaFromAwaitFreePool(sessVars.ConnectionID, -compilePlanMemQuota)
			compilePlanMemQuota = 0
		}
	}

	if execUseArbitrator {
		intoErrBeforeExec := func() error {
			if enableWaitAverse {
				metrics.GlobalMemArbitratorSubTasks.CancelWaitAversePlan.Inc()
				return exeerrors.ErrQueryExecStopped.GenWithStackByArgs(memory.ArbitratorWaitAverseCancel.String()+defSuffixCompilePlan, sessVars.ConnectionID)
			}
			if arbitratorMode == memory.ArbitratorModeStandard {
				metrics.GlobalMemArbitratorSubTasks.CancelStandardModePlan.Inc()
				return exeerrors.ErrQueryExecStopped.GenWithStackByArgs(memory.ArbitratorStandardCancel.String()+defSuffixCompilePlan, sessVars.ConnectionID)
			}
			return nil
		}

		if globalMemArbitrator.AtMemRisk() {
			if s.sessionPlanCache != nil {
				s.sessionPlanCache.DeleteAll()
			}
			if err := intoErrBeforeExec(); err != nil {
				return nil, err
			}
			for globalMemArbitrator.AtMemRisk() {
				if globalMemArbitrator.AtOOMRisk() {
					metrics.GlobalMemArbitratorSubTasks.ForceKillPlan.Inc()
					return nil, exeerrors.ErrQueryExecStopped.GenWithStackByArgs(memory.ArbitratorOOMRiskKill.String()+defSuffixCompilePlan, sessVars.ConnectionID)
				}
				time.Sleep(defOOMRiskCheckDur)
			}
		}

		arbitratorOutOfQuota := !globalMemArbitrator.ConsumeQuotaFromAwaitFreePool(sessVars.ConnectionID, compilePlanMemQuota)
		defer releaseCommonQuota()

		if arbitratorOutOfQuota { // for SQL which needs to be controlled by mem-arbitrator
			if s.sessionPlanCache != nil && s.sessionPlanCache.Size() > 0 {
				s.sessionPlanCache.DeleteAll()
				// one more chance to get quota after clearing plan cache
			} else if err := intoErrBeforeExec(); err != nil {
				return nil, err
			}
		}
	}

	var stmt *executor.ExecStmt
	var err error

	{
		// Transform abstract syntax tree to a physical plan(stored in executor.ExecStmt).
		compiler := executor.Compiler{Ctx: s}
		stmt, err = compiler.Compile(ctx, stmtNode)
		// TODO: report precise tracked heap inuse to the global mem-arbitrator if necessary
	}

	// check if resource group hint is valid, can't do this in planner.Optimize because we can access
	// infoschema there.
	if sessVars.StmtCtx.ResourceGroupName != sessVars.ResourceGroupName {
		// if target resource group doesn't exist, fallback to the origin resource group.
		if _, ok := s.infoCache.GetLatest().ResourceGroupByName(ast.NewCIStr(sessVars.StmtCtx.ResourceGroupName)); !ok {
			logutil.Logger(ctx).Warn("Unknown resource group from hint", zap.String("name", sessVars.StmtCtx.ResourceGroupName))
			sessVars.StmtCtx.ResourceGroupName = sessVars.ResourceGroupName
			if txn, err := s.Txn(false); err == nil && txn != nil && txn.Valid() {
				kv.SetTxnResourceGroup(txn, sessVars.ResourceGroupName)
			}
		}
	}

	if err != nil {
		s.rollbackOnError(ctx)

		// Only print log message when this SQL is from the user.
		// Mute the warning for internal SQLs.
		if !s.sessionVars.InRestrictedSQL {
			if !variable.ErrUnknownSystemVar.Equal(err) {
				sql := stmtNode.Text()
				sql = parser.Normalize(sql, s.sessionVars.EnableRedactLog)
				logutil.Logger(ctx).Warn("compile SQL failed", zap.Error(err),
					zap.String("SQL", sql))
			}
		}
		return nil, err
	}

	durCompile := time.Since(s.sessionVars.StartTime)
	s.GetSessionVars().DurationCompile = durCompile
	if s.isInternal() {
		session_metrics.SessionExecuteCompileDurationInternal.Observe(durCompile.Seconds())
	} else {
		session_metrics.SessionExecuteCompileDurationGeneral.Observe(durCompile.Seconds())
	}
	s.currentPlan = stmt.Plan
	if execStmt, ok := stmtNode.(*ast.ExecuteStmt); ok {
		if execStmt.Name == "" {
			// for exec-stmt on bin-protocol, ignore the plan detail in `show process` to gain performance benefits.
			s.currentPlan = nil
		}
	}

	// Execute the physical plan.
	defer logStmt(stmt, s) // defer until txnStartTS is set

	if sessVars.MemArbitrator.WaitAverse == variable.MemArbitratorNolimit {
		metrics.GlobalMemArbitratorSubTasks.NoLimit.Inc()
	}
	if execUseArbitrator {
		releaseCommonQuota()

		reserveSize := min(sessVars.MemArbitrator.QueryReserved, memory.DefMaxLimit)

		memPriority := memory.ArbitrationPriorityMedium

		if sg, ok := domain.GetDomain(s).InfoSchema().ResourceGroupByName(ast.NewCIStr(sessVars.StmtCtx.ResourceGroupName)); ok {
			switch sg.Priority {
			case ast.LowPriorityValue:
				memPriority = memory.ArbitrationPriorityLow
			case ast.MediumPriorityValue:
				memPriority = memory.ArbitrationPriorityMedium
			case ast.HighPriorityValue:
				memPriority = memory.ArbitrationPriorityHigh
			}
		}

		digestKey := normalizedSQL
		// digestKey := sessVars.StmtCtx.OriginalSQL

		tracker := sessVars.StmtCtx.MemTracker
		if !tracker.InitMemArbitrator(
			globalMemArbitrator,
			sessVars.MemTracker.Killer,
			digestKey,
			memPriority,
			enableWaitAverse,
			reserveSize,
			s.isInternal(),
		) {
			return nil, errors.New("failed to init mem-arbitrator")
		}

		defer func() { // detach mem-arbitrator and rethrow panic if any
			if r := recover(); r != nil {
				tracker.DetachMemArbitrator(true)
				panic(r)
			}
		}()
	}

	var recordSet sqlexec.RecordSet
	if stmt.PsStmt != nil { // point plan short path
		ctx, prevTraceID := resetStmtTraceID(ctx, s)

		// Emit stmt.start trace event (simplified for point-get fast path)
		if traceevent.IsEnabled(traceevent.StmtLifecycle) {
			fields := []zap.Field{
				zap.Uint64("conn_id", s.sessionVars.ConnectionID),
			}
			// Include previous trace ID to create statement chain
			if len(prevTraceID) > 0 {
				fields = append(fields, zap.String("prev_trace_id", redact.Key(prevTraceID)))
			}
			traceevent.TraceEvent(ctx, traceevent.StmtLifecycle, "stmt.start", fields...)
		}

		// Defer stmt.finish trace event (simplified for point-get fast path)
		defer func() {
			if traceevent.IsEnabled(traceevent.StmtLifecycle) {
				fields := []zap.Field{
					zap.Uint64("conn_id", s.sessionVars.ConnectionID),
				}
				if err != nil {
					fields = append(fields, zap.Error(err))
				}
				traceevent.TraceEvent(ctx, traceevent.StmtLifecycle, "stmt.finish", fields...)
			}
		}()

		recordSet, err = stmt.PointGet(ctx)
		s.setLastTxnInfoBeforeTxnEnd()
		s.txn.changeToInvalid()
	} else {
		recordSet, err = runStmt(ctx, s, stmt)
	}

	// Observe the resource group query total counter if the resource control is enabled and the
	// current session is attached with a resource group.
	resourceGroupName := s.GetSessionVars().StmtCtx.ResourceGroupName
	if len(resourceGroupName) > 0 {
		metrics.ResourceGroupQueryTotalCounter.WithLabelValues(resourceGroupName, resourceGroupName).Inc()
	}

	if err != nil {
		if !errIsNoisy(err) {
			logutil.Logger(ctx).Warn("run statement failed",
				zap.Int64("schemaVersion", s.GetInfoSchema().SchemaMetaVersion()),
				zap.Error(err),
				zap.String("session", s.String()))
		}
		return recordSet, err
	}
	if !s.isInternal() && config.GetGlobalConfig().EnableTelemetry {
		telemetry.CurrentExecuteCount.Inc()
		tiFlashPushDown, tiFlashExchangePushDown := plannercore.IsTiFlashContained(stmt.Plan)
		if tiFlashPushDown {
			telemetry.CurrentTiFlashPushDownCount.Inc()
		}
		if tiFlashExchangePushDown {
			telemetry.CurrentTiFlashExchangePushDownCount.Inc()
		}
	}
	return recordSet, nil
}

func (s *session) GetSQLExecutor() sqlexec.SQLExecutor {
	return s
}

func (s *session) GetRestrictedSQLExecutor() sqlexec.RestrictedSQLExecutor {
	return s
}

func (s *session) onTxnManagerStmtStartOrRetry(ctx context.Context, node ast.StmtNode) error {
	if s.sessionVars.RetryInfo.Retrying {
		return sessiontxn.GetTxnManager(s).OnStmtRetry(ctx)
	}
	return sessiontxn.GetTxnManager(s).OnStmtStart(ctx, node)
}

func (s *session) validateStatementInTxn(stmtNode ast.StmtNode) error {
	vars := s.GetSessionVars()
	if _, ok := stmtNode.(*ast.ImportIntoStmt); ok && vars.InTxn() {
		return errors.New("cannot run IMPORT INTO in explicit transaction")
	}
	return nil
}

func (s *session) validateStatementReadOnlyInStaleness(stmtNode ast.StmtNode) error {
	vars := s.GetSessionVars()
	if !vars.TxnCtx.IsStaleness && vars.TxnReadTS.PeakTxnReadTS() == 0 && !vars.EnableExternalTSRead || vars.InRestrictedSQL {
		return nil
	}
	errMsg := "only support read-only statement during read-only staleness transactions"
	node := stmtNode.(ast.Node)
	switch v := node.(type) {
	case *ast.SplitRegionStmt:
		return nil
	case *ast.SelectStmt:
		// select lock statement needs start a transaction which will be conflict to stale read,
		// we forbid select lock statement in stale read for now.
		if v.LockInfo != nil {
			return errors.New("select lock hasn't been supported in stale read yet")
		}
		if !plannercore.IsReadOnly(stmtNode, vars) {
			return errors.New(errMsg)
		}
		return nil
	case *ast.ExplainStmt, *ast.DoStmt, *ast.ShowStmt, *ast.SetOprStmt, *ast.ExecuteStmt, *ast.SetOprSelectList:
		if !plannercore.IsReadOnly(stmtNode, vars) {
			return errors.New(errMsg)
		}
		return nil
	default:
	}
	// covered DeleteStmt/InsertStmt/UpdateStmt/CallStmt/LoadDataStmt
	if _, ok := stmtNode.(ast.DMLNode); ok {
		return errors.New(errMsg)
	}
	return nil
}

// fileTransInConnKeys contains the keys of queries that will be handled by handleFileTransInConn.
var fileTransInConnKeys = []fmt.Stringer{
	executor.LoadDataVarKey,
	executor.LoadStatsVarKey,
	executor.PlanReplayerLoadVarKey,
}

func (s *session) hasFileTransInConn() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, k := range fileTransInConnKeys {
		v := s.mu.values[k]
		if v != nil {
			return true
		}
	}
	return false
}

type sqlDigestAlias struct {
	Digest string
}

func (digest sqlDigestAlias) sqlDigestDumpTriggerCheck(config *traceevent.DumpTriggerConfig) bool {
	return config.UserCommand.SQLDigest == digest.Digest
}

type userAlias struct {
	user string
}

func (u userAlias) byUserDumpTriggerCheck(config *traceevent.DumpTriggerConfig) bool {
	return config.UserCommand.ByUser == u.user
}

type stmtLabelAlias struct {
	label string
}

func (s stmtLabelAlias) stmtLabelDumpTriggerCheck(config *traceevent.DumpTriggerConfig) bool {
	return config.UserCommand.StmtLabel == s.label
}

// resetStmtTraceID generates a new trace ID for the current statement,
// injects it into the session context for cross-statement correlation, and returns the previous trace ID.
func resetStmtTraceID(ctx context.Context, se *session) (context.Context, []byte) {
	// Capture previous trace ID from session variables (for statement chaining)
	// We store it in session variables instead of context because the context
	// is recreated for each statement and doesn't persist across executions
	prevTraceID := se.sessionVars.PrevTraceID

	// Inject trace ID into context for correlation across TiDB -> client-go -> TiKV
	// This enables trace events to be correlated by trace_id field
	// The trace ID is generated from transaction start_ts and statement count
	startTS := se.sessionVars.TxnCtx.StartTS
	stmtCount := uint64(se.sessionVars.TxnCtx.StatementCount)
	traceID := traceevent.GenerateTraceID(ctx, startTS, stmtCount)
	ctx = trace.ContextWithTraceID(ctx, traceID)
	se.currentCtx = ctx
	// Store trace ID for next statement
	se.sessionVars.PrevTraceID = traceID

	return ctx, prevTraceID
}

// runStmt executes the sqlexec.Statement and commit or rollback the current transaction.
func runStmt(ctx context.Context, se *session, s sqlexec.Statement) (rs sqlexec.RecordSet, err error) {
	failpoint.Inject("assertTxnManagerInRunStmt", func() {
		sessiontxn.RecordAssert(se, "assertTxnManagerInRunStmt", true)
		if stmt, ok := s.(*executor.ExecStmt); ok {
			sessiontxn.AssertTxnManagerInfoSchema(se, stmt.InfoSchema)
		}
	})

	r, ctx := tracing.StartRegionEx(ctx, "session.runStmt")
	defer r.End()
	if r.Span != nil {
		r.Span.LogKV("sql", s.Text())
	}

	ctx, prevTraceID := resetStmtTraceID(ctx, se)
	stmtCtx := se.sessionVars.StmtCtx
	sqlDigest, _ := stmtCtx.SQLDigest()
	// Make sure StmtType is filled even if succ is false.
	if stmtCtx.StmtType == "" {
		stmtCtx.StmtType = stmtctx.GetStmtLabel(ctx, s.GetStmtNode())
	}

	// Emit stmt.start trace event
	if traceevent.IsEnabled(traceevent.StmtLifecycle) {
		fields := []zap.Field{
			zap.String("sql_digest", sqlDigest),
			zap.Bool("autocommit", se.sessionVars.IsAutocommit()),
			zap.Uint64("conn_id", se.sessionVars.ConnectionID),
		}
		// Include previous trace ID to create statement chain
		if len(prevTraceID) > 0 {
			fields = append(fields, zap.String("prev_trace_id", redact.Key(prevTraceID)))
		}
		traceevent.TraceEvent(ctx, traceevent.StmtLifecycle, "stmt.start", fields...)
	}
	// Not using closure to avoid unnecessary memory allocation.
	traceevent.CheckFlightRecorderDumpTrigger(ctx, "dump_trigger.user_command.sql_digest", sqlDigestAlias{sqlDigest}.sqlDigestDumpTriggerCheck)
	if se.sessionVars.User != nil && se.sessionVars.User.Username != "" {
		traceevent.CheckFlightRecorderDumpTrigger(ctx, "dump_trigger.user_command.by_user", userAlias{se.sessionVars.User.Username}.byUserDumpTriggerCheck)
	}
	traceevent.CheckFlightRecorderDumpTrigger(ctx, "dump_trigger.user_command.stmt_label", stmtLabelAlias{stmtCtx.StmtType}.stmtLabelDumpTriggerCheck)

	// Defer stmt.finish trace event to capture final state including errors
	defer func() {
		if traceevent.IsEnabled(traceevent.StmtLifecycle) {
			stmtCtx := se.sessionVars.StmtCtx
			sqlDigest, _ := stmtCtx.SQLDigest()
			_, planDigest := stmtCtx.GetPlanDigest()
			var planDigestHex string
			if planDigest != nil {
				planDigestHex = hex.EncodeToString(planDigest.Bytes())
			}
			fields := []zap.Field{
				zap.String("sql_digest", sqlDigest),
				zap.String("plan_digest", planDigestHex),
				zap.Bool("autocommit", se.sessionVars.IsAutocommit()),
				zap.Uint64("conn_id", se.sessionVars.ConnectionID),
				zap.Int("retry_count", se.sessionVars.TxnCtx.StatementCount),
			}
			if err != nil {
				fields = append(fields, zap.Error(err))
			}
			traceevent.TraceEvent(ctx, traceevent.StmtLifecycle, "stmt.finish", fields...)
		}
	}()

	se.SetValue(sessionctx.QueryString, s.Text())
	if _, ok := s.(*executor.ExecStmt).StmtNode.(ast.DDLNode); ok {
		se.SetValue(sessionctx.LastExecuteDDL, true)
	} else {
		se.ClearValue(sessionctx.LastExecuteDDL)
	}

	sessVars := se.sessionVars

	// Save origTxnCtx here to avoid it reset in the transaction retry.
	origTxnCtx := sessVars.TxnCtx
	err = se.checkTxnAborted(s)
	if err != nil {
		return nil, err
	}
	if sessVars.TxnCtx.CouldRetry && !s.IsReadOnly(sessVars) {
		// Only when the txn is could retry and the statement is not read only, need to do stmt-count-limit check,
		// otherwise, the stmt won't be add into stmt history, and also don't need check.
		// About `stmt-count-limit`, see more in https://docs.pingcap.com/tidb/stable/tidb-configuration-file#stmt-count-limit
		if err := checkStmtLimit(ctx, se, false); err != nil {
			return nil, err
		}
	}

	rs, err = s.Exec(ctx)

	if se.txn.Valid() && se.txn.IsPipelined() {
		// Pipelined-DMLs can return assertion errors and write conflicts here because they flush
		// during execution, handle these errors as we would handle errors after a commit.
		if err != nil {
			err = se.handleAssertionFailure(ctx, err)
		}
		newErr := se.tryReplaceWriteConflictError(ctx, err)
		if newErr != nil {
			err = newErr
		}
	}

	se.updateTelemetryMetric(s.(*executor.ExecStmt))
	sessVars.TxnCtx.StatementCount++
	if rs != nil {
		if se.GetSessionVars().StmtCtx.IsExplainAnalyzeDML {
			if !sessVars.InTxn() {
				se.StmtCommit(ctx)
				if err := se.CommitTxn(ctx); err != nil {
					return nil, err
				}
			}
		}
		return &execStmtResult{
			RecordSet: rs,
			sql:       s,
			se:        se,
		}, err
	}

	err = finishStmt(ctx, se, err, s)
	if se.hasFileTransInConn() {
		// The query will be handled later in handleFileTransInConn,
		// then should call the ExecStmt.FinishExecuteStmt to finish this statement.
		se.SetValue(ExecStmtVarKey, s.(*executor.ExecStmt))
	} else {
		// If it is not a select statement or special query, we record its slow log here,
		// then it could include the transaction commit time.
		s.(*executor.ExecStmt).FinishExecuteStmt(origTxnCtx.StartTS, err, false)
	}
	return nil, err
}

// ExecStmtVarKeyType is a dummy type to avoid naming collision in context.
type ExecStmtVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (ExecStmtVarKeyType) String() string {
	return "exec_stmt_var_key"
}

// ExecStmtVarKey is a variable key for ExecStmt.
const ExecStmtVarKey ExecStmtVarKeyType = 0

// execStmtResult is the return value of ExecuteStmt and it implements the sqlexec.RecordSet interface.
// Why we need a struct to wrap a RecordSet and provide another RecordSet?
// This is because there are so many session state related things that definitely not belongs to the original
// RecordSet, so this struct exists and RecordSet.Close() is overridden to handle that.
type execStmtResult struct {
	sqlexec.RecordSet
	se     *session
	sql    sqlexec.Statement
	once   sync.Once
	closed bool
}

func (rs *execStmtResult) Finish() error {
	var err error
	rs.once.Do(func() {
		var err1 error
		if f, ok := rs.RecordSet.(interface{ Finish() error }); ok {
			err1 = f.Finish()
		}
		err2 := finishStmt(context.Background(), rs.se, err, rs.sql)
		if err1 != nil {
			err = err1
		} else {
			err = err2
		}
	})
	return err
}

func (rs *execStmtResult) Close() error {
	if rs.closed {
		return nil
	}
	err1 := rs.Finish()
	err2 := rs.RecordSet.Close()
	rs.closed = true
	if err1 != nil {
		return err1
	}
	return err2
}

func (rs *execStmtResult) TryDetach() (sqlexec.RecordSet, bool, error) {
	// If `TryDetach` is called, the connection must have set `mysql.ServerStatusCursorExists`, or
	// the `StatementContext` will be re-used and cause data race.
	intest.Assert(rs.se.GetSessionVars().HasStatusFlag(mysql.ServerStatusCursorExists))

	if !rs.sql.IsReadOnly(rs.se.GetSessionVars()) {
		return nil, false, nil
	}
	if !plannercore.IsAutoCommitTxn(rs.se.GetSessionVars()) {
		return nil, false, nil
	}

	drs, ok := rs.RecordSet.(sqlexec.DetachableRecordSet)
	if !ok {
		return nil, false, nil
	}
	detachedRS, ok, err := drs.TryDetach()
	if !ok || err != nil {
		return nil, ok, err
	}
	cursorHandle := rs.se.GetCursorTracker().NewCursor(
		cursor.State{StartTS: rs.se.GetSessionVars().TxnCtx.StartTS},
	)
	crs := staticrecordset.WrapRecordSetWithCursor(cursorHandle, detachedRS)

	// Now, a transaction is not needed for the detached record set, so we commit the transaction and cleanup
	// the session state.
	err = finishStmt(context.Background(), rs.se, nil, rs.sql)
	if err != nil {
		err2 := detachedRS.Close()
		if err2 != nil {
			logutil.BgLogger().Error("close detached record set failed", zap.Error(err2))
		}
		return nil, true, err
	}

	return crs, true, nil
}

// GetExecutor4Test exports the internal executor for test purpose.
func (rs *execStmtResult) GetExecutor4Test() any {
	return rs.RecordSet.(interface{ GetExecutor4Test() any }).GetExecutor4Test()
}

// rollbackOnError makes sure the next statement starts a new transaction with the latest InfoSchema.
func (s *session) rollbackOnError(ctx context.Context) {
	if !s.sessionVars.InTxn() {
		s.RollbackTxn(ctx)
	}
}

// PrepareStmt is used for executing prepare statement in binary protocol
func (s *session) PrepareStmt(sql string) (stmtID uint32, paramCount int, fields []*resolve.ResultField, err error) {
	defer func() {
		if s.sessionVars.StmtCtx != nil {
			s.sessionVars.StmtCtx.DetachMemDiskTracker()
		}
	}()
	if s.sessionVars.TxnCtx.InfoSchema == nil {
		// We don't need to create a transaction for prepare statement, just get information schema will do.
		s.sessionVars.TxnCtx.InfoSchema = s.infoCache.GetLatest()
	}
	err = s.loadCommonGlobalVariablesIfNeeded()
	if err != nil {
		return
	}

	ctx := context.Background()
	// NewPrepareExec may need startTS to build the executor, for example prepare statement has subquery in int.
	// So we have to call PrepareTxnCtx here.
	if err = s.PrepareTxnCtx(ctx, nil); err != nil {
		return
	}

	prepareStmt := &ast.PrepareStmt{SQLText: sql}
	if err = s.onTxnManagerStmtStartOrRetry(ctx, prepareStmt); err != nil {
		return
	}

	if err = sessiontxn.GetTxnManager(s).AdviseWarmup(); err != nil {
		return
	}
	prepareExec := executor.NewPrepareExec(s, sql)
	err = prepareExec.Next(ctx, nil)
	// Rollback even if err is nil.
	s.rollbackOnError(ctx)

	if err != nil {
		return
	}
	return prepareExec.ID, prepareExec.ParamCount, prepareExec.Fields, nil
}

// ExecutePreparedStmt executes a prepared statement.
func (s *session) ExecutePreparedStmt(ctx context.Context, stmtID uint32, params []expression.Expression) (sqlexec.RecordSet, error) {
	prepStmt, err := s.sessionVars.GetPreparedStmtByID(stmtID)
	if err != nil {
		err = plannererrors.ErrStmtNotFound
		logutil.Logger(ctx).Error("prepared statement not found", zap.Uint32("stmtID", stmtID))
		return nil, err
	}
	stmt, ok := prepStmt.(*plannercore.PlanCacheStmt)
	if !ok {
		return nil, errors.Errorf("invalid PlanCacheStmt type")
	}
	execStmt := &ast.ExecuteStmt{
		BinaryArgs: params,
		PrepStmt:   stmt,
		PrepStmtId: stmtID,
	}
	return s.ExecuteStmt(ctx, execStmt)
}

func (s *session) DropPreparedStmt(stmtID uint32) error {
	vars := s.sessionVars
	if _, ok := vars.PreparedStmts[stmtID]; !ok {
		return plannererrors.ErrStmtNotFound
	}
	vars.RetryInfo.DroppedPreparedStmtIDs = append(vars.RetryInfo.DroppedPreparedStmtIDs, stmtID)
	return nil
}

func (s *session) Txn(active bool) (kv.Transaction, error) {
	if !active {
		return &s.txn, nil
	}
	_, err := sessiontxn.GetTxnManager(s).ActivateTxn()
	s.SetMemoryFootprintChangeHook()
	return &s.txn, err
}

func (s *session) SetValue(key fmt.Stringer, value any) {
	s.mu.Lock()
	s.mu.values[key] = value
	s.mu.Unlock()
}

func (s *session) Value(key fmt.Stringer) any {
	s.mu.RLock()
	value := s.mu.values[key]
	s.mu.RUnlock()
	return value
}

func (s *session) ClearValue(key fmt.Stringer) {
	s.mu.Lock()
	delete(s.mu.values, key)
	s.mu.Unlock()
}

type inCloseSession struct{}

// Close function does some clean work when session end.
// Close should release the table locks which hold by the session.
func (s *session) Close() {
	// TODO: do clean table locks when session exited without execute Close.
	// TODO: do clean table locks when tidb-server was `kill -9`.
	if s.HasLockedTables() && config.TableLockEnabled() {
		if ds := config.TableLockDelayClean(); ds > 0 {
			time.Sleep(time.Duration(ds) * time.Millisecond)
		}
		lockedTables := s.GetAllTableLocks()
		err := domain.GetDomain(s).DDLExecutor().UnlockTables(s, lockedTables)
		if err != nil {
			logutil.BgLogger().Error("release table lock failed", zap.Uint64("conn", s.sessionVars.ConnectionID))
		}
	}
	s.ReleaseAllAdvisoryLocks()
	if s.statsCollector != nil {
		s.statsCollector.Delete()
	}
	if s.idxUsageCollector != nil {
		s.idxUsageCollector.Flush()
	}
	telemetry.GlobalBuiltinFunctionsUsage.Collect(s.GetBuiltinFunctionUsage())
	bindValue := s.Value(bindinfo.SessionBindInfoKeyType)
	if bindValue != nil {
		bindValue.(bindinfo.SessionBindingHandle).Close()
	}
	ctx := context.WithValue(context.TODO(), inCloseSession{}, struct{}{})
	s.RollbackTxn(ctx)
	s.sessionVars.WithdrawAllPreparedStmt()
	if s.stmtStats != nil {
		s.stmtStats.SetFinished()
	}
	s.sessionVars.ClearDiskFullOpt()
	if s.sessionPlanCache != nil {
		s.sessionPlanCache.Close()
	}
	if s.sessionVars.ConnectionID != 0 {
		memory.RemovePoolFromGlobalMemArbitrator(s.sessionVars.ConnectionID)
	}
	// Detach session trackers during session cleanup.
	// ANALYZE attaches session MemTracker to GlobalAnalyzeMemoryTracker; without
	// detachment, closed sessions cannot be garbage collected.
	if s.sessionVars.MemTracker != nil {
		s.sessionVars.MemTracker.Detach()
	}
	if s.sessionVars.DiskTracker != nil {
		s.sessionVars.DiskTracker.Detach()
	}
}

// GetSessionVars implements the context.Context interface.
func (s *session) GetSessionVars() *variable.SessionVars {
	return s.sessionVars
}

// GetPlanCtx returns the PlanContext.
func (s *session) GetPlanCtx() planctx.PlanContext {
	return s.pctx
}

// GetExprCtx returns the expression context of the session.
func (s *session) GetExprCtx() exprctx.ExprContext {
	return s.exprctx
}

// GetTableCtx returns the table.MutateContext
func (s *session) GetTableCtx() tblctx.MutateContext {
	return s.tblctx
}

// GetDistSQLCtx returns the context used in DistSQL
func (s *session) GetDistSQLCtx() *distsqlctx.DistSQLContext {
	vars := s.GetSessionVars()
	sc := vars.StmtCtx

	dctx := sc.GetOrInitDistSQLFromCache(func() *distsqlctx.DistSQLContext {
		// cross ks session does not have domain.
		dom := s.GetDomain().(*domain.Domain)
		var rgCtl *rmclient.ResourceGroupsController
		if dom != nil {
			rgCtl = dom.ResourceGroupsController()
		}
		return &distsqlctx.DistSQLContext{
			WarnHandler:     sc.WarnHandler,
			InRestrictedSQL: sc.InRestrictedSQL,
			Client:          s.GetClient(),

			EnabledRateLimitAction: vars.EnabledRateLimitAction,
			EnableChunkRPC:         vars.EnableChunkRPC,
			OriginalSQL:            sc.OriginalSQL,
			KVVars:                 vars.KVVars,
			KvExecCounter:          sc.KvExecCounter,
			SessionMemTracker:      vars.MemTracker,

			Location:         sc.TimeZone(),
			RuntimeStatsColl: sc.RuntimeStatsColl,
			SQLKiller:        &vars.SQLKiller,
			CPUUsage:         &vars.SQLCPUUsages,
			ErrCtx:           sc.ErrCtx(),

			TiFlashReplicaRead:                   vars.TiFlashReplicaRead,
			TiFlashMaxThreads:                    vars.TiFlashMaxThreads,
			TiFlashMaxBytesBeforeExternalJoin:    vars.TiFlashMaxBytesBeforeExternalJoin,
			TiFlashMaxBytesBeforeExternalGroupBy: vars.TiFlashMaxBytesBeforeExternalGroupBy,
			TiFlashMaxBytesBeforeExternalSort:    vars.TiFlashMaxBytesBeforeExternalSort,
			TiFlashMaxQueryMemoryPerNode:         vars.TiFlashMaxQueryMemoryPerNode,
			TiFlashQuerySpillRatio:               vars.TiFlashQuerySpillRatio,
			TiFlashHashJoinVersion:               vars.TiFlashHashJoinVersion,

			DistSQLConcurrency:            vars.DistSQLScanConcurrency(),
			ReplicaReadType:               vars.GetReplicaRead(),
			WeakConsistency:               sc.WeakConsistency,
			RCCheckTS:                     sc.RCCheckTS,
			NotFillCache:                  sc.NotFillCache,
			TaskID:                        sc.TaskID,
			Priority:                      sc.Priority,
			ResourceGroupTagger:           sc.GetResourceGroupTagger(),
			EnablePaging:                  vars.EnablePaging,
			MinPagingSize:                 vars.MinPagingSize,
			MaxPagingSize:                 vars.MaxPagingSize,
			RequestSourceType:             vars.RequestSourceType,
			ExplicitRequestSourceType:     vars.ExplicitRequestSourceType,
			StoreBatchSize:                vars.StoreBatchSize,
			ResourceGroupName:             sc.ResourceGroupName,
			LoadBasedReplicaReadThreshold: vars.LoadBasedReplicaReadThreshold,
			RunawayChecker:                sc.RunawayChecker,
			RUConsumptionReporter:         rgCtl,
			TiKVClientReadTimeout:         vars.GetTiKVClientReadTimeout(),
			MaxExecutionTime:              vars.GetMaxExecutionTime(),

			ReplicaClosestReadThreshold: vars.ReplicaClosestReadThreshold,
			ConnectionID:                vars.ConnectionID,
			SessionAlias:                vars.SessionAlias,

			ExecDetails: &sc.SyncExecDetails,
		}
	})

	// Check if the runaway checker is updated. This is to avoid that evaluating a non-correlated subquery
	// during the optimization phase will cause the `*distsqlctx.DistSQLContext` to be created before the
	// runaway checker is set later at the execution phase.
	// Ref: https://github.com/pingcap/tidb/issues/61899
	if dctx.RunawayChecker != sc.RunawayChecker {
		dctx.RunawayChecker = sc.RunawayChecker
	}

	return dctx
}

// GetRangerCtx returns the context used in `ranger` related functions
func (s *session) GetRangerCtx() *rangerctx.RangerContext {
	vars := s.GetSessionVars()
	sc := vars.StmtCtx

	rctx := sc.GetOrInitRangerCtxFromCache(func() any {
		return &rangerctx.RangerContext{
			ExprCtx: s.GetExprCtx(),
			TypeCtx: s.GetSessionVars().StmtCtx.TypeCtx(),
			ErrCtx:  s.GetSessionVars().StmtCtx.ErrCtx(),

			RegardNULLAsPoint:        s.GetSessionVars().RegardNULLAsPoint,
			OptPrefixIndexSingleScan: s.GetSessionVars().OptPrefixIndexSingleScan,
			OptimizerFixControl:      s.GetSessionVars().OptimizerFixControl,

			PlanCacheTracker:     &s.GetSessionVars().StmtCtx.PlanCacheTracker,
			RangeFallbackHandler: &s.GetSessionVars().StmtCtx.RangeFallbackHandler,
		}
	})

	return rctx.(*rangerctx.RangerContext)
}

// GetBuildPBCtx returns the context used in `ToPB` method
func (s *session) GetBuildPBCtx() *planctx.BuildPBContext {
	vars := s.GetSessionVars()
	sc := vars.StmtCtx

	bctx := sc.GetOrInitBuildPBCtxFromCache(func() any {
		return &planctx.BuildPBContext{
			ExprCtx: s.GetExprCtx(),
			Client:  s.GetClient(),

			TiFlashFastScan:                    s.GetSessionVars().TiFlashFastScan,
			TiFlashFineGrainedShuffleBatchSize: s.GetSessionVars().TiFlashFineGrainedShuffleBatchSize,

			// the following fields are used to build `expression.PushDownContext`.
			// TODO: it'd be better to embed `expression.PushDownContext` in `BuildPBContext`. But `expression` already
			// depends on this package, so we need to move `expression.PushDownContext` to a standalone package first.
			GroupConcatMaxLen: s.GetSessionVars().GroupConcatMaxLen,
			InExplainStmt:     s.GetSessionVars().StmtCtx.InExplainStmt,
			WarnHandler:       s.GetSessionVars().StmtCtx.WarnHandler,
			ExtraWarnghandler: s.GetSessionVars().StmtCtx.ExtraWarnHandler,
		}
	})

	return bctx.(*planctx.BuildPBContext)
}

func (s *session) AuthPluginForUser(ctx context.Context, user *auth.UserIdentity) (string, error) {
	pm := privilege.GetPrivilegeManager(s)
	authplugin, err := pm.GetAuthPluginForConnection(ctx, user.Username, user.Hostname)
	if err != nil {
		return "", err
	}
	return authplugin, nil
}

const (
	notBootstrapped = 0
)

func mustGetStoreBootstrapVersion(store kv.Storage) int64 {
	var ver int64
	// check in kv store
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	err := kv.RunInNewTxn(ctx, store, false, func(_ context.Context, txn kv.Transaction) error {
		var err error
		t := meta.NewReader(txn)
		ver, err = t.GetBootstrapVersion()
		return err
	})
	if err != nil {
		logutil.BgLogger().Fatal("get store bootstrap version failed", zap.Error(err))
	}
	return ver
}

func getStoreBootstrapVersionWithCache(store kv.Storage) int64 {
	// check in memory
	_, ok := store.GetOption(StoreBootstrappedKey)
	if ok {
		return currentBootstrapVersion
	}

	ver := mustGetStoreBootstrapVersion(store)

	if ver > notBootstrapped {
		// here mean memory is not ok, but other server has already finished it
		store.SetOption(StoreBootstrappedKey, true)
	}

	modifyBootstrapVersionForTest(ver)
	return ver
}
