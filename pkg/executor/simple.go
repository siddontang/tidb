// Copyright 2016 PingCAP, Inc.
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

package executor

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	executor_metrics "github.com/pingcap/tidb/pkg/executor/metrics"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"go.uber.org/zap"
)

const notSpecified = -1

// SimpleExec represents simple statement executor.
// For statements do simple execution.
// includes `UseStmt`, 'SetStmt`, `DoStmt`,
// `BeginStmt`, `CommitStmt`, `RollbackStmt`.
// TODO: list all simple statements.
type SimpleExec struct {
	exec.BaseExecutor

	Statement  ast.StmtNode
	ResolveCtx *resolve.Context
	// IsFromRemote indicates whether the statement IS FROM REMOTE TiDB instance in cluster,
	//   and executing in coprocessor.
	//   Used for `global kill`. See https://github.com/pingcap/tidb/blob/master/docs/design/2020-06-01-global-kill.md.
	IsFromRemote bool
	done         bool
	is           infoschema.InfoSchema

	// staleTxnStartTS is the StartTS that is used to execute the staleness txn during a read-only begin statement.
	staleTxnStartTS uint64
}

// resourceOptionsInfo represents the resource infomations to limit user.
// It contains 'MAX_QUERIES_PER_HOUR', 'MAX_UPDATES_PER_HOUR', 'MAX_CONNECTIONS_PER_HOUR' and 'MAX_USER_CONNECTIONS'.
// It only implements the option of 'MAX_USER_CONNECTIONS' now.
// To do: implement the other three options.
type resourceOptionsInfo struct {
	maxQueriesPerHour     int64
	maxUpdatesPerHour     int64
	maxConnectionsPerHour int64
	maxUserConnections    int64
}

type passwordOrLockOptionsInfo struct {
	lockAccount                 string
	passwordExpired             string
	passwordLifetime            any
	passwordHistory             int64
	passwordHistoryChange       bool
	passwordReuseInterval       int64
	passwordReuseIntervalChange bool
	failedLoginAttempts         int64
	passwordLockTime            int64
	failedLoginAttemptsChange   bool
	passwordLockTimeChange      bool
}

type passwordReuseInfo struct {
	passwordHistory       int64
	passwordReuseInterval int64
}

type userInfo struct {
	host       string
	user       string
	pLI        *passwordOrLockOptionsInfo
	pwd        string
	authString string
}

// Next implements the Executor Next interface.
func (e *SimpleExec) Next(ctx context.Context, _ *chunk.Chunk) (err error) {
	if e.done {
		return nil
	}

	if e.autoNewTxn() {
		// Commit the old transaction, like DDL.
		if err := sessiontxn.NewTxnInStmt(ctx, e.Ctx()); err != nil {
			return err
		}
		defer func() { e.Ctx().GetSessionVars().SetInTxn(false) }()
	}

	switch x := e.Statement.(type) {
	case *ast.GrantRoleStmt:
		err = e.executeGrantRole(ctx, x)
	case *ast.UseStmt:
		err = e.executeUse(x)
	case *ast.FlushStmt:
		err = e.executeFlush(ctx, x)
	case *ast.AlterInstanceStmt:
		err = e.executeAlterInstance(x)
	case *ast.BeginStmt:
		err = e.executeBegin(ctx, x)
	case *ast.CommitStmt:
		e.executeCommit()
	case *ast.SavepointStmt:
		err = e.executeSavepoint(x)
	case *ast.ReleaseSavepointStmt:
		err = e.executeReleaseSavepoint(x)
	case *ast.RollbackStmt:
		err = e.executeRollback(x)
	case *ast.CreateUserStmt:
		err = e.executeCreateUser(ctx, x)
	case *ast.AlterUserStmt:
		err = e.executeAlterUser(ctx, x)
	case *ast.DropUserStmt:
		err = e.executeDropUser(ctx, x)
	case *ast.RenameUserStmt:
		err = e.executeRenameUser(x)
	case *ast.SetPwdStmt:
		err = e.executeSetPwd(ctx, x)
	case *ast.SetSessionStatesStmt:
		err = e.executeSetSessionStates(ctx, x)
	case *ast.KillStmt:
		err = e.executeKillStmt(ctx, x)
	case *ast.RefreshStatsStmt:
		err = e.executeRefreshStats(ctx, x)
	case *ast.BinlogStmt:
		// We just ignore it.
		return nil
	case *ast.DropStatsStmt:
		err = e.executeDropStats(ctx, x)
	case *ast.SetRoleStmt:
		err = e.executeSetRole(ctx, x)
	case *ast.RevokeRoleStmt:
		err = e.executeRevokeRole(ctx, x)
	case *ast.SetDefaultRoleStmt:
		err = e.executeSetDefaultRole(ctx, x)
	case *ast.ShutdownStmt:
		err = e.executeShutdown()
	case *ast.AdminStmt:
		err = e.executeAdmin(x)
	case *ast.SetResourceGroupStmt:
		err = e.executeSetResourceGroupName(x)
	case *ast.AlterRangeStmt:
		err = e.executeAlterRange(x)
	case *ast.DropQueryWatchStmt:
		err = e.executeDropQueryWatch(x)
	}
	e.done = true
	return err
}

func (e *SimpleExec) setDefaultRoleNone(s *ast.SetDefaultRoleStmt) error {
	restrictedCtx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	defer e.ReleaseSysSession(ctx, restrictedCtx)
	sqlExecutor := restrictedCtx.GetSQLExecutor()
	if _, err := sqlExecutor.ExecuteInternal(ctx, "begin"); err != nil {
		return err
	}
	sql := new(strings.Builder)
	for _, u := range s.UserList {
		sql.Reset()
		sqlescape.MustFormatSQL(sql, "DELETE IGNORE FROM mysql.default_roles WHERE USER=%? AND HOST=%?;", u.Username, u.Hostname)
		if _, err := sqlExecutor.ExecuteInternal(ctx, sql.String()); err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
			if _, rollbackErr := sqlExecutor.ExecuteInternal(ctx, "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
	}
	if _, err := sqlExecutor.ExecuteInternal(ctx, "commit"); err != nil {
		return err
	}
	return nil
}

func (e *SimpleExec) setDefaultRoleRegular(ctx context.Context, s *ast.SetDefaultRoleStmt) error {
	for _, user := range s.UserList {
		exists, err := userExists(ctx, e.Ctx(), user.Username, user.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return exeerrors.ErrCannotUser.GenWithStackByArgs("SET DEFAULT ROLE", user.String())
		}
	}
	for _, role := range s.RoleList {
		exists, err := userExists(ctx, e.Ctx(), role.Username, role.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return exeerrors.ErrCannotUser.GenWithStackByArgs("SET DEFAULT ROLE", role.String())
		}
	}

	restrictedCtx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	defer e.ReleaseSysSession(internalCtx, restrictedCtx)
	sqlExecutor := restrictedCtx.GetSQLExecutor()
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "begin"); err != nil {
		return err
	}
	sql := new(strings.Builder)
	for _, user := range s.UserList {
		sql.Reset()
		sqlescape.MustFormatSQL(sql, "DELETE IGNORE FROM mysql.default_roles WHERE USER=%? AND HOST=%?;", user.Username, user.Hostname)
		if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
			if _, rollbackErr := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
		for _, role := range s.RoleList {
			checker := privilege.GetPrivilegeManager(e.Ctx())
			ok := checker.FindEdge(ctx, role, user)
			if !ok {
				if _, rollbackErr := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); rollbackErr != nil {
					return rollbackErr
				}
				return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(role.String(), user.String())
			}
			sql.Reset()
			sqlescape.MustFormatSQL(sql, "INSERT IGNORE INTO mysql.default_roles values(%?, %?, %?, %?);", user.Hostname, user.Username, role.Hostname, role.Username)
			if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
				logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
				if _, rollbackErr := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); rollbackErr != nil {
					return rollbackErr
				}
				return err
			}
		}
	}
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "commit"); err != nil {
		return err
	}
	return nil
}

func (e *SimpleExec) setDefaultRoleAll(ctx context.Context, s *ast.SetDefaultRoleStmt) error {
	for _, user := range s.UserList {
		exists, err := userExists(ctx, e.Ctx(), user.Username, user.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return exeerrors.ErrCannotUser.GenWithStackByArgs("SET DEFAULT ROLE", user.String())
		}
	}
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	restrictedCtx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(internalCtx, restrictedCtx)
	sqlExecutor := restrictedCtx.GetSQLExecutor()
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "begin"); err != nil {
		return err
	}
	sql := new(strings.Builder)
	for _, user := range s.UserList {
		sql.Reset()
		sqlescape.MustFormatSQL(sql, "DELETE IGNORE FROM mysql.default_roles WHERE USER=%? AND HOST=%?;", user.Username, user.Hostname)
		if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
			if _, rollbackErr := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
		sql.Reset()
		sqlescape.MustFormatSQL(sql, "INSERT IGNORE INTO mysql.default_roles(HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER) SELECT TO_HOST,TO_USER,FROM_HOST,FROM_USER FROM mysql.role_edges WHERE TO_HOST=%? AND TO_USER=%?;", user.Hostname, user.Username)
		if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
			if _, rollbackErr := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
	}
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "commit"); err != nil {
		return err
	}
	return nil
}

func (e *SimpleExec) setDefaultRoleForCurrentUser(ctx context.Context, s *ast.SetDefaultRoleStmt) (err error) {
	checker := privilege.GetPrivilegeManager(e.Ctx())
	user := s.UserList[0]
	restrictedCtx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnPrivilege)
	defer e.ReleaseSysSession(ctx, restrictedCtx)
	sqlExecutor := restrictedCtx.GetSQLExecutor()

	if _, err := sqlExecutor.ExecuteInternal(ctx, "begin"); err != nil {
		return err
	}

	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, "DELETE IGNORE FROM mysql.default_roles WHERE USER=%? AND HOST=%?;", user.Username, user.Hostname)
	if _, err := sqlExecutor.ExecuteInternal(ctx, sql.String()); err != nil {
		logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
		if _, rollbackErr := sqlExecutor.ExecuteInternal(ctx, "rollback"); rollbackErr != nil {
			return rollbackErr
		}
		return err
	}

	sql.Reset()
	switch s.SetRoleOpt {
	case ast.SetRoleNone:
		sqlescape.MustFormatSQL(sql, "DELETE IGNORE FROM mysql.default_roles WHERE USER=%? AND HOST=%?;", user.Username, user.Hostname)
	case ast.SetRoleAll:
		sqlescape.MustFormatSQL(sql, "INSERT IGNORE INTO mysql.default_roles(HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER) SELECT TO_HOST,TO_USER,FROM_HOST,FROM_USER FROM mysql.role_edges WHERE TO_HOST=%? AND TO_USER=%?;", user.Hostname, user.Username)
	case ast.SetRoleRegular:
		sqlescape.MustFormatSQL(sql, "INSERT IGNORE INTO mysql.default_roles values")
		for i, role := range s.RoleList {
			if i > 0 {
				sqlescape.MustFormatSQL(sql, ",")
			}
			ok := checker.FindEdge(ctx, role, user)
			if !ok {
				return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(role.String(), user.String())
			}
			sqlescape.MustFormatSQL(sql, "(%?, %?, %?, %?)", user.Hostname, user.Username, role.Hostname, role.Username)
		}
	}

	if _, err := sqlExecutor.ExecuteInternal(ctx, sql.String()); err != nil {
		logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
		if _, rollbackErr := sqlExecutor.ExecuteInternal(ctx, "rollback"); rollbackErr != nil {
			return rollbackErr
		}
		return err
	}
	if _, err := sqlExecutor.ExecuteInternal(ctx, "commit"); err != nil {
		return err
	}
	return nil
}

func userIdentityToUserList(specs []*auth.UserIdentity) []string {
	users := make([]string, 0, len(specs))
	for _, user := range specs {
		users = append(users, user.Username)
	}
	return users
}

func (e *SimpleExec) executeSetDefaultRole(ctx context.Context, s *ast.SetDefaultRoleStmt) (err error) {
	sessionVars := e.Ctx().GetSessionVars()
	checker := privilege.GetPrivilegeManager(e.Ctx())
	if checker == nil {
		return errors.New("miss privilege checker")
	}

	if len(s.UserList) == 1 && sessionVars.User != nil {
		u, h := s.UserList[0].Username, s.UserList[0].Hostname
		if u == sessionVars.User.Username && h == sessionVars.User.AuthHostname {
			err = e.setDefaultRoleForCurrentUser(ctx, s)
			if err != nil {
				return err
			}
			users := userIdentityToUserList(s.UserList)
			return domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege(users)
		}
	}

	activeRoles := sessionVars.ActiveRoles
	if !checker.RequestVerification(activeRoles, mysql.SystemDB, mysql.DefaultRoleTable, "", mysql.UpdatePriv) {
		if !checker.RequestVerification(activeRoles, "", "", "", mysql.CreateUserPriv) {
			return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
		}
	}

	switch s.SetRoleOpt {
	case ast.SetRoleAll:
		err = e.setDefaultRoleAll(ctx, s)
	case ast.SetRoleNone:
		err = e.setDefaultRoleNone(s)
	case ast.SetRoleRegular:
		err = e.setDefaultRoleRegular(ctx, s)
	}
	if err != nil {
		return
	}
	users := userIdentityToUserList(s.UserList)
	return domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege(users)
}

func (e *SimpleExec) setRoleRegular(ctx context.Context, s *ast.SetRoleStmt) error {
	// Deal with SQL like `SET ROLE role1, role2;`
	checkDup := make(map[string]*auth.RoleIdentity, len(s.RoleList))
	// Check whether RoleNameList contain duplicate role name.
	for _, r := range s.RoleList {
		key := r.String()
		checkDup[key] = r
	}
	roleList := make([]*auth.RoleIdentity, 0, 10)
	for _, v := range checkDup {
		roleList = append(roleList, v)
	}

	checker := privilege.GetPrivilegeManager(e.Ctx())
	ok, roleName := checker.ActiveRoles(ctx, e.Ctx(), roleList)
	if !ok {
		u := e.Ctx().GetSessionVars().User
		return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return nil
}

func (e *SimpleExec) setRoleAll(ctx context.Context) error {
	// Deal with SQL like `SET ROLE ALL;`
	checker := privilege.GetPrivilegeManager(e.Ctx())
	user, host := e.Ctx().GetSessionVars().User.AuthUsername, e.Ctx().GetSessionVars().User.AuthHostname
	roles := checker.GetAllRoles(user, host)
	ok, roleName := checker.ActiveRoles(ctx, e.Ctx(), roles)
	if !ok {
		u := e.Ctx().GetSessionVars().User
		return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return nil
}

func (e *SimpleExec) setRoleAllExcept(ctx context.Context, s *ast.SetRoleStmt) error {
	// Deal with SQL like `SET ROLE ALL EXCEPT role1, role2;`
	for _, r := range s.RoleList {
		if r.Hostname == "" {
			r.Hostname = "%"
		}
	}
	checker := privilege.GetPrivilegeManager(e.Ctx())
	user, host := e.Ctx().GetSessionVars().User.AuthUsername, e.Ctx().GetSessionVars().User.AuthHostname
	roles := checker.GetAllRoles(user, host)

	filter := func(arr []*auth.RoleIdentity, f func(*auth.RoleIdentity) bool) []*auth.RoleIdentity {
		i, j := 0, 0
		for i = range arr {
			if f(arr[i]) {
				arr[j] = arr[i]
				j++
			}
		}
		return arr[:j]
	}
	banned := func(r *auth.RoleIdentity) bool {
		for _, ban := range s.RoleList {
			if ban.Hostname == r.Hostname && ban.Username == r.Username {
				return false
			}
		}
		return true
	}

	afterExcept := filter(roles, banned)
	ok, roleName := checker.ActiveRoles(ctx, e.Ctx(), afterExcept)
	if !ok {
		u := e.Ctx().GetSessionVars().User
		return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return nil
}

func (e *SimpleExec) setRoleDefault(ctx context.Context) error {
	// Deal with SQL like `SET ROLE DEFAULT;`
	checker := privilege.GetPrivilegeManager(e.Ctx())
	user, host := e.Ctx().GetSessionVars().User.AuthUsername, e.Ctx().GetSessionVars().User.AuthHostname
	roles := checker.GetDefaultRoles(ctx, user, host)
	ok, roleName := checker.ActiveRoles(ctx, e.Ctx(), roles)
	if !ok {
		u := e.Ctx().GetSessionVars().User
		return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return nil
}

func (e *SimpleExec) setRoleNone(ctx context.Context) error {
	// Deal with SQL like `SET ROLE NONE;`
	checker := privilege.GetPrivilegeManager(e.Ctx())
	roles := make([]*auth.RoleIdentity, 0)
	ok, roleName := checker.ActiveRoles(ctx, e.Ctx(), roles)
	if !ok {
		u := e.Ctx().GetSessionVars().User
		return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return nil
}

func (e *SimpleExec) executeSetRole(ctx context.Context, s *ast.SetRoleStmt) error {
	switch s.SetRoleOpt {
	case ast.SetRoleRegular:
		return e.setRoleRegular(ctx, s)
	case ast.SetRoleAll:
		return e.setRoleAll(ctx)
	case ast.SetRoleAllExcept:
		return e.setRoleAllExcept(ctx, s)
	case ast.SetRoleNone:
		return e.setRoleNone(ctx)
	case ast.SetRoleDefault:
		return e.setRoleDefault(ctx)
	}
	return nil
}

func (e *SimpleExec) dbAccessDenied(dbname string) error {
	user := e.Ctx().GetSessionVars().User
	u := user.Username
	h := user.Hostname
	if len(user.AuthUsername) > 0 && len(user.AuthHostname) > 0 {
		u = user.AuthUsername
		h = user.AuthHostname
	}
	return exeerrors.ErrDBaccessDenied.GenWithStackByArgs(u, h, dbname)
}

func (e *SimpleExec) executeUse(s *ast.UseStmt) error {
	dbname := ast.NewCIStr(s.DBName)

	checker := privilege.GetPrivilegeManager(e.Ctx())
	if checker != nil && e.Ctx().GetSessionVars().User != nil {
		if !checker.DBIsVisible(e.Ctx().GetSessionVars().ActiveRoles, dbname.String()) {
			return e.dbAccessDenied(dbname.O)
		}
	}

	dbinfo, exists := e.is.SchemaByName(dbname)
	if !exists {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbname)
	}
	e.Ctx().GetSessionVars().CurrentDBChanged = dbname.O != e.Ctx().GetSessionVars().CurrentDB
	e.Ctx().GetSessionVars().CurrentDB = dbname.O
	sessionVars := e.Ctx().GetSessionVars()
	dbCollate := dbinfo.Collate
	if dbCollate == "" {
		dbCollate = getDefaultCollate(dbinfo.Charset)
	}
	// If new collations are enabled, switch to the default
	// collation if this one is not supported.
	// The SetSystemVar will also update the CharsetDatabase
	dbCollate = collate.SubstituteMissingCollationToDefault(dbCollate)
	return sessionVars.SetSystemVarWithoutValidation(vardef.CollationDatabase, dbCollate)
}

func (e *SimpleExec) executeBegin(ctx context.Context, s *ast.BeginStmt) error {
	// If `START TRANSACTION READ ONLY` is the first statement in TxnCtx, we should
	// always create a new Txn instead of reusing it.
	if s.ReadOnly {
		noopFuncsMode := e.Ctx().GetSessionVars().NoopFuncsMode
		if s.AsOf == nil && noopFuncsMode != variable.OnInt {
			err := expression.ErrFunctionsNoopImpl.FastGenByArgs("READ ONLY")
			if noopFuncsMode == variable.OffInt {
				return errors.Trace(err)
			}
			e.Ctx().GetSessionVars().StmtCtx.AppendWarning(err)
		}
		if s.AsOf != nil {
			// start transaction read only as of failed due to we set tx_read_ts before
			if e.Ctx().GetSessionVars().TxnReadTS.PeakTxnReadTS() > 0 {
				return errors.New("start transaction read only as of is forbidden after set transaction read only as of")
			}
		}
	}

	return sessiontxn.GetTxnManager(e.Ctx()).EnterNewTxn(ctx, &sessiontxn.EnterNewTxnRequest{
		Type:                  sessiontxn.EnterNewTxnWithBeginStmt,
		TxnMode:               s.Mode,
		CausalConsistencyOnly: s.CausalConsistencyOnly,
		StaleReadTS:           e.staleTxnStartTS,
	})
}

func (e *SimpleExec) executeSavepoint(s *ast.SavepointStmt) error {
	sessVars := e.Ctx().GetSessionVars()
	txnCtx := sessVars.TxnCtx
	if !sessVars.InTxn() && sessVars.IsAutocommit() {
		return nil
	}
	if !sessVars.ConstraintCheckInPlacePessimistic && sessVars.TxnCtx.IsPessimistic {
		return errors.New("savepoint is not supported in pessimistic transactions when in-place constraint check is disabled")
	}
	txn, err := e.Ctx().Txn(true)
	if err != nil {
		return err
	}
	memDBCheckpoint := txn.GetMemDBCheckpoint()
	txnCtx.AddSavepoint(s.Name, memDBCheckpoint)
	return nil
}

func (e *SimpleExec) executeReleaseSavepoint(s *ast.ReleaseSavepointStmt) error {
	deleted := e.Ctx().GetSessionVars().TxnCtx.ReleaseSavepoint(s.Name)
	if !deleted {
		return exeerrors.ErrSavepointNotExists.GenWithStackByArgs("SAVEPOINT", s.Name)
	}
	return nil
}

func (e *SimpleExec) setCurrentUser(users []*auth.UserIdentity) {
	sessionVars := e.Ctx().GetSessionVars()
	for i, user := range users {
		if user.CurrentUser {
			users[i].Username = sessionVars.User.AuthUsername
			users[i].Hostname = sessionVars.User.AuthHostname
		}
	}
}

func (e *SimpleExec) executeRevokeRole(ctx context.Context, s *ast.RevokeRoleStmt) error {
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)

	//Fix revoke role from current_user results error.
	e.setCurrentUser(s.Users)

	for _, role := range s.Roles {
		exists, err := userExists(ctx, e.Ctx(), role.Username, role.Hostname)
		if err != nil {
			return errors.Trace(err)
		}
		if !exists {
			return exeerrors.ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", role.String())
		}
	}

	restrictedCtx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(internalCtx, restrictedCtx)
	sqlExecutor := restrictedCtx.GetSQLExecutor()

	// begin a transaction to insert role graph edges.
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "begin"); err != nil {
		return errors.Trace(err)
	}
	sql := new(strings.Builder)
	// when an active role of current user is revoked,
	// it should be removed from activeRoles
	activeRoles, curUser, curHost := e.Ctx().GetSessionVars().ActiveRoles, "", ""
	if user := e.Ctx().GetSessionVars().User; user != nil {
		curUser, curHost = user.AuthUsername, user.AuthHostname
	}
	for _, user := range s.Users {
		exists, err := userExists(ctx, e.Ctx(), user.Username, user.Hostname)
		if err != nil {
			return errors.Trace(err)
		}
		if !exists {
			if _, err := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); err != nil {
				return errors.Trace(err)
			}
			return exeerrors.ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", user.String())
		}
		for _, role := range s.Roles {
			if role.Hostname == "" {
				role.Hostname = "%"
			}
			sql.Reset()
			sqlescape.MustFormatSQL(sql, `DELETE IGNORE FROM %n.%n WHERE FROM_HOST=%? and FROM_USER=%? and TO_HOST=%? and TO_USER=%?`, mysql.SystemDB, mysql.RoleEdgeTable, role.Hostname, role.Username, user.Hostname, user.Username)
			if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
				if _, err := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); err != nil {
					return errors.Trace(err)
				}
				return exeerrors.ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", role.String())
			}

			sql.Reset()
			sqlescape.MustFormatSQL(sql, `DELETE IGNORE FROM %n.%n WHERE DEFAULT_ROLE_HOST=%? and DEFAULT_ROLE_USER=%? and HOST=%? and USER=%?`, mysql.SystemDB, mysql.DefaultRoleTable, role.Hostname, role.Username, user.Hostname, user.Username)
			if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
				if _, err := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); err != nil {
					return errors.Trace(err)
				}
				return exeerrors.ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", role.String())
			}

			// delete from activeRoles
			if curUser == user.Username && curHost == user.Hostname {
				for i := range activeRoles {
					if activeRoles[i].Username == role.Username && activeRoles[i].Hostname == role.Hostname {
						activeRoles = slices.Delete(activeRoles, i, i+1)
						break
					}
				}
			}
		}
	}
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "commit"); err != nil {
		return err
	}
	checker := privilege.GetPrivilegeManager(e.Ctx())
	if checker == nil {
		return errors.New("miss privilege checker")
	}
	if ok, roleName := checker.ActiveRoles(ctx, e.Ctx(), activeRoles); !ok {
		u := e.Ctx().GetSessionVars().User
		return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	userList := userIdentityToUserList(s.Users)
	return domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege(userList)
}

func (e *SimpleExec) executeCommit() {
	e.Ctx().GetSessionVars().SetInTxn(false)
}

func (e *SimpleExec) executeRollback(s *ast.RollbackStmt) error {
	sessVars := e.Ctx().GetSessionVars()
	logutil.BgLogger().Debug("execute rollback statement", zap.Uint64("conn", sessVars.ConnectionID))
	txn, err := e.Ctx().Txn(false)
	if err != nil {
		return err
	}
	if s.SavepointName != "" {
		if !txn.Valid() {
			return exeerrors.ErrSavepointNotExists.GenWithStackByArgs("SAVEPOINT", s.SavepointName)
		}
		savepointRecord := sessVars.TxnCtx.RollbackToSavepoint(s.SavepointName)
		if savepointRecord == nil {
			return exeerrors.ErrSavepointNotExists.GenWithStackByArgs("SAVEPOINT", s.SavepointName)
		}
		txn.RollbackMemDBToCheckpoint(savepointRecord.MemDBCheckpoint)
		return nil
	}

	sessVars.SetInTxn(false)
	if txn.Valid() {
		duration := time.Since(sessVars.TxnCtx.CreateTime).Seconds()
		isInternal := false
		if internal := txn.GetOption(kv.RequestSourceInternal); internal != nil && internal.(bool) {
			isInternal = true
		}
		if isInternal && sessVars.TxnCtx.IsPessimistic {
			executor_metrics.TransactionDurationPessimisticRollbackInternal.Observe(duration)
		} else if isInternal && !sessVars.TxnCtx.IsPessimistic {
			executor_metrics.TransactionDurationOptimisticRollbackInternal.Observe(duration)
		} else if !isInternal && sessVars.TxnCtx.IsPessimistic {
			executor_metrics.TransactionDurationPessimisticRollbackGeneral.Observe(duration)
		} else if !isInternal && !sessVars.TxnCtx.IsPessimistic {
			executor_metrics.TransactionDurationOptimisticRollbackGeneral.Observe(duration)
		}
		sessVars.TxnCtx.ClearDelta()
		return txn.Rollback()
	}
	return nil
}
