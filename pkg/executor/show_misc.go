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
	gjson "encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	fstorage "github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/plugin"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/pd/client/errs"
	pdHttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

// fetchShowCreateUser composes 'show create user' result.
func (e *ShowExec) fetchShowCreateUser(ctx context.Context) error {
	checker := privilege.GetPrivilegeManager(e.Ctx())
	if checker == nil {
		return errors.New("miss privilege checker")
	}
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnPrivilege)

	userName, hostName := e.User.Username, e.User.Hostname
	sessVars := e.Ctx().GetSessionVars()
	if e.User.CurrentUser {
		userName = sessVars.User.AuthUsername
		hostName = sessVars.User.AuthHostname
	} else {
		// Show create user requires the SELECT privilege on mysql.user.
		// Ref https://dev.mysql.com/doc/refman/5.7/en/show-create-user.html
		activeRoles := sessVars.ActiveRoles
		if !checker.RequestVerification(activeRoles, mysql.SystemDB, mysql.UserTable, "", mysql.SelectPriv) {
			return e.tableAccessDenied("SELECT", mysql.UserTable)
		}
	}

	exec := e.Ctx().GetRestrictedSQLExecutor()

	rows, _, err := exec.ExecRestrictedSQL(ctx, nil,
		`SELECT plugin, Account_locked, user_attributes->>'$.metadata', Token_issuer,
        Password_reuse_history, Password_reuse_time, Password_expired, Password_lifetime,
        user_attributes->>'$.Password_locking.failed_login_attempts',
        user_attributes->>'$.Password_locking.password_lock_time_days', authentication_string,
        Max_user_connections
		FROM %n.%n WHERE User=%? AND Host=%?`,
		mysql.SystemDB, mysql.UserTable, userName, strings.ToLower(hostName))
	if err != nil {
		return errors.Trace(err)
	}

	if len(rows) == 0 {
		// FIXME: the error returned is not escaped safely
		return exeerrors.ErrCannotUser.GenWithStackByArgs("SHOW CREATE USER",
			fmt.Sprintf("'%s'@'%s'", e.User.Username, e.User.Hostname))
	}

	authPlugin, err := e.Ctx().GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(vardef.DefaultAuthPlugin)
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) == 1 && rows[0].GetString(0) != "" {
		authPlugin = rows[0].GetString(0)
	}

	accountLockedRaw := rows[0].GetString(1)
	accountLocked := "LOCK"
	if accountLockedRaw[len(accountLockedRaw)-1:] == "N" {
		accountLocked = "UNLOCK"
	}

	userAttributes := rows[0].GetString(2)
	if len(userAttributes) > 0 {
		userAttributes = fmt.Sprintf(" ATTRIBUTE '%s'", userAttributes)
	}

	tokenIssuer := rows[0].GetString(3)
	if len(tokenIssuer) > 0 {
		tokenIssuer = " token_issuer " + tokenIssuer
	}

	var passwordHistory string
	if rows[0].IsNull(4) {
		passwordHistory = "DEFAULT"
	} else {
		passwordHistory = strconv.FormatUint(rows[0].GetUint64(4), 10)
	}

	var passwordReuseInterval string
	if rows[0].IsNull(5) {
		passwordReuseInterval = "DEFAULT"
	} else {
		passwordReuseInterval = strconv.FormatUint(rows[0].GetUint64(5), 10) + " DAY"
	}

	passwordExpired := rows[0].GetEnum(6).String()
	passwordLifetime := int64(-1)
	if !rows[0].IsNull(7) {
		passwordLifetime = rows[0].GetInt64(7)
	}
	passwordExpiredStr := "PASSWORD EXPIRE DEFAULT"
	if passwordExpired == "Y" {
		passwordExpiredStr = "PASSWORD EXPIRE"
	} else if passwordLifetime == 0 {
		passwordExpiredStr = "PASSWORD EXPIRE NEVER"
	} else if passwordLifetime > 0 {
		passwordExpiredStr = fmt.Sprintf("PASSWORD EXPIRE INTERVAL %d DAY", passwordLifetime)
	}

	failedLoginAttempts := rows[0].GetString(8)
	if len(failedLoginAttempts) > 0 {
		failedLoginAttempts = " FAILED_LOGIN_ATTEMPTS " + failedLoginAttempts
	}

	passwordLockTimeDays := rows[0].GetString(9)
	if len(passwordLockTimeDays) > 0 {
		if passwordLockTimeDays == "-1" {
			passwordLockTimeDays = " PASSWORD_LOCK_TIME UNBOUNDED"
		} else {
			passwordLockTimeDays = " PASSWORD_LOCK_TIME " + passwordLockTimeDays
		}
	}
	authData := rows[0].GetString(10)

	maxUserConnections := rows[0].GetInt64(11)
	maxUserConnectionsStr := ""
	if maxUserConnections > 0 {
		maxUserConnectionsStr = fmt.Sprintf(" WITH MAX_USER_CONNECTIONS %d", maxUserConnections)
	}

	rows, _, err = exec.ExecRestrictedSQL(ctx, nil, `SELECT Priv FROM %n.%n WHERE User=%? AND Host=%?`, mysql.SystemDB, mysql.GlobalPrivTable, userName, hostName)
	if err != nil {
		return errors.Trace(err)
	}

	require := "NONE"
	if len(rows) == 1 {
		privData := rows[0].GetString(0)
		var privValue privileges.GlobalPrivValue
		err = gjson.Unmarshal(hack.Slice(privData), &privValue)
		if err != nil {
			return errors.Trace(err)
		}
		require = privValue.RequireStr()
	}

	authStr := ""
	if !(authPlugin == mysql.AuthSocket && authData == "") {
		authStr = fmt.Sprintf(" AS '%s'", authData)
	}

	// FIXME: the returned string is not escaped safely
	showStr := fmt.Sprintf("CREATE USER '%s'@'%s' IDENTIFIED WITH '%s'%s REQUIRE %s%s%s %s ACCOUNT %s PASSWORD HISTORY %s PASSWORD REUSE INTERVAL %s%s%s%s",
		e.User.Username, e.User.Hostname, authPlugin, authStr, require, tokenIssuer, maxUserConnectionsStr, passwordExpiredStr, accountLocked, passwordHistory, passwordReuseInterval, failedLoginAttempts, passwordLockTimeDays, userAttributes)
	e.appendRow([]any{showStr})
	return nil
}

func (e *ShowExec) fetchShowGrants(ctx context.Context) error {
	vars := e.Ctx().GetSessionVars()
	checker := privilege.GetPrivilegeManager(e.Ctx())
	if checker == nil {
		return errors.New("miss privilege checker")
	}
	if e.User == nil || e.User.CurrentUser {
		// The input is a "SHOW GRANTS" statement with no users *or* SHOW GRANTS FOR CURRENT_USER()
		// In these cases we include the active roles for showing privileges.
		e.User = &auth.UserIdentity{Username: vars.User.AuthUsername, Hostname: vars.User.AuthHostname}
		if len(e.Roles) == 0 {
			e.Roles = vars.ActiveRoles
		}
	} else {
		userName := vars.User.AuthUsername
		hostName := vars.User.AuthHostname
		// Show grant user requires the SELECT privilege on mysql schema.
		// Ref https://dev.mysql.com/doc/refman/8.0/en/show-grants.html
		if userName != e.User.Username || hostName != e.User.Hostname {
			if !checker.RequestVerification(vars.ActiveRoles, mysql.SystemDB, "", "", mysql.SelectPriv) {
				return exeerrors.ErrDBaccessDenied.GenWithStackByArgs(userName, hostName, mysql.SystemDB)
			}
		}
	}
	// This is for the syntax SHOW GRANTS FOR x USING role
	for _, r := range e.Roles {
		if r.Hostname == "" {
			r.Hostname = "%"
		}
		if !checker.FindEdge(ctx, r, e.User) {
			return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(r.String(), e.User.String())
		}
	}
	gs, err := checker.ShowGrants(ctx, e.Ctx(), e.User, e.Roles)
	if err != nil {
		return errors.Trace(err)
	}
	for _, g := range gs {
		e.appendRow([]any{g})
	}
	return nil
}

func (e *ShowExec) fetchShowPrivileges() error {
	e.appendRow([]any{"Alter", "Tables", "To alter the table"})
	e.appendRow([]any{"Alter routine", "Functions,Procedures", "To alter or drop stored functions/procedures"})
	e.appendRow([]any{"Config", "Server Admin", "To use SHOW CONFIG and SET CONFIG statements"})
	e.appendRow([]any{"Create", "Databases,Tables,Indexes", "To create new databases and tables"})
	e.appendRow([]any{"Create routine", "Databases", "To use CREATE FUNCTION/PROCEDURE"})
	e.appendRow([]any{"Create role", "Server Admin", "To create new roles"})
	e.appendRow([]any{"Create temporary tables", "Databases", "To use CREATE TEMPORARY TABLE"})
	e.appendRow([]any{"Create view", "Tables", "To create new views"})
	e.appendRow([]any{"Create user", "Server Admin", "To create new users"})
	e.appendRow([]any{"Delete", "Tables", "To delete existing rows"})
	e.appendRow([]any{"Drop", "Databases,Tables", "To drop databases, tables, and views"})
	e.appendRow([]any{"Drop role", "Server Admin", "To drop roles"})
	e.appendRow([]any{"Event", "Server Admin", "To create, alter, drop and execute events"})
	e.appendRow([]any{"Execute", "Functions,Procedures", "To execute stored routines"})
	e.appendRow([]any{"File", "File access on server", "To read and write files on the server"})
	e.appendRow([]any{"Grant option", "Databases,Tables,Functions,Procedures", "To give to other users those privileges you possess"})
	e.appendRow([]any{"Index", "Tables", "To create or drop indexes"})
	e.appendRow([]any{"Insert", "Tables", "To insert data into tables"})
	e.appendRow([]any{"Lock tables", "Databases", "To use LOCK TABLES (together with SELECT privilege)"})
	e.appendRow([]any{"Process", "Server Admin", "To view the plain text of currently executing queries"})
	e.appendRow([]any{"Proxy", "Server Admin", "To make proxy user possible"})
	e.appendRow([]any{"References", "Databases,Tables", "To have references on tables"})
	e.appendRow([]any{"Reload", "Server Admin", "To reload or refresh tables, logs and privileges"})
	e.appendRow([]any{"Replication client", "Server Admin", "To ask where the slave or master servers are"})
	e.appendRow([]any{"Replication slave", "Server Admin", "To read binary log events from the master"})
	e.appendRow([]any{"Select", "Tables", "To retrieve rows from table"})
	e.appendRow([]any{"Show databases", "Server Admin", "To see all databases with SHOW DATABASES"})
	e.appendRow([]any{"Show view", "Tables", "To see views with SHOW CREATE VIEW"})
	e.appendRow([]any{"Shutdown", "Server Admin", "To shut down the server"})
	e.appendRow([]any{"Super", "Server Admin", "To use KILL thread, SET GLOBAL, CHANGE MASTER, etc."})
	e.appendRow([]any{"Trigger", "Tables", "To use triggers"})
	e.appendRow([]any{"Create tablespace", "Server Admin", "To create/alter/drop tablespaces"})
	e.appendRow([]any{"Update", "Tables", "To update existing rows"})
	e.appendRow([]any{"Usage", "Server Admin", "No privileges - allow connect only"})

	for _, priv := range privileges.GetDynamicPrivileges() {
		e.appendRow([]any{priv, "Server Admin", ""})
	}
	return nil
}

func (*ShowExec) fetchShowTriggers() error {
	return nil
}

func (*ShowExec) fetchShowProcedureStatus() error {
	return nil
}

func (e *ShowExec) fetchShowPlugins() error {
	tiPlugins := plugin.GetAll()
	for _, ps := range tiPlugins {
		for _, p := range ps {
			e.appendRow([]any{p.Name, p.StateValue(), p.Kind.String(), p.Path, p.License, strconv.Itoa(int(p.Version))})
		}
	}
	return nil
}

func (e *ShowExec) fetchShowWarnings(errOnly bool) error {
	stmtCtx := e.Ctx().GetSessionVars().StmtCtx
	if e.CountWarningsOrErrors {
		errCount, warnCount := stmtCtx.NumErrorWarnings()
		if errOnly {
			e.appendRow([]any{int64(errCount)})
		} else {
			e.appendRow([]any{int64(warnCount)})
		}
		return nil
	}
	for _, w := range stmtCtx.GetWarnings() {
		if errOnly && w.Level != contextutil.WarnLevelError {
			continue
		}
		warn := errors.Cause(w.Err)
		switch x := warn.(type) {
		case *terror.Error:
			sqlErr := terror.ToSQLError(x)
			e.appendRow([]any{w.Level, int64(sqlErr.Code), sqlErr.Message})
		default:
			var err string
			if warn != nil {
				err = warn.Error()
			}
			e.appendRow([]any{w.Level, int64(mysql.ErrUnknown), err})
		}
	}
	return nil
}

func (e *ShowExec) getTable() (table.Table, error) {
	if e.Table == nil {
		return nil, errors.New("table not found")
	}
	tb, ok := e.is.TableByID(context.Background(), e.Table.TableInfo.ID)
	if !ok {
		return nil, errors.Errorf("table %s not found", e.Table.Name)
	}
	return tb, nil
}

func (e *ShowExec) dbAccessDenied() error {
	user := e.Ctx().GetSessionVars().User
	u := user.Username
	h := user.Hostname
	if len(user.AuthUsername) > 0 && len(user.AuthHostname) > 0 {
		u = user.AuthUsername
		h = user.AuthHostname
	}
	return exeerrors.ErrDBaccessDenied.GenWithStackByArgs(u, h, e.DBName)
}

func (e *ShowExec) tableAccessDenied(access string, table string) error {
	user := e.Ctx().GetSessionVars().User
	u := user.Username
	h := user.Hostname
	if len(user.AuthUsername) > 0 && len(user.AuthHostname) > 0 {
		u = user.AuthUsername
		h = user.AuthHostname
	}
	return exeerrors.ErrTableaccessDenied.GenWithStackByArgs(access, u, h, table)
}

func (e *ShowExec) appendRow(row []any) {
	for i, col := range row {
		switch x := col.(type) {
		case nil:
			e.result.AppendNull(i)
		case int:
			e.result.AppendInt64(i, int64(x))
		case int64:
			e.result.AppendInt64(i, x)
		case uint64:
			e.result.AppendUint64(i, x)
		case float64:
			e.result.AppendFloat64(i, x)
		case float32:
			e.result.AppendFloat32(i, x)
		case string:
			e.result.AppendString(i, x)
		case []byte:
			e.result.AppendBytes(i, x)
		case types.BinaryLiteral:
			e.result.AppendBytes(i, x)
		case *types.MyDecimal:
			e.result.AppendMyDecimal(i, x)
		case types.Time:
			e.result.AppendTime(i, x)
		case types.BinaryJSON:
			e.result.AppendJSON(i, x)
		case types.VectorFloat32:
			e.result.AppendVectorFloat32(i, x)
		case types.Duration:
			e.result.AppendDuration(i, x)
		case types.Enum:
			e.result.AppendEnum(i, x)
		case types.Set:
			e.result.AppendSet(i, x)
		default:
			e.result.AppendNull(i)
		}
	}
}

func (e *ShowExec) fetchShowDistributions(ctx context.Context) error {
	tb, err := e.getTable()
	if err != nil {
		return errors.Trace(err)
	}
	physicalIDs := []int64{}
	partitionNames := make([]string, 0)
	if pi := tb.Meta().GetPartitionInfo(); pi != nil {
		for _, name := range e.Table.PartitionNames {
			pid, err := tables.FindPartitionByName(tb.Meta(), name.L)
			if err != nil {
				return err
			}
			physicalIDs = append(physicalIDs, pid)
			partitionNames = append(partitionNames, name.L)
		}
		if len(physicalIDs) == 0 {
			for _, p := range pi.Definitions {
				physicalIDs = append(physicalIDs, p.ID)
				partitionNames = append(partitionNames, p.Name.L)
			}
		}
	} else {
		if len(e.Table.PartitionNames) != 0 {
			return plannererrors.ErrPartitionClauseOnNonpartitioned
		}
		physicalIDs = append(physicalIDs, tb.Meta().ID)
		partitionNames = append(partitionNames, tb.Meta().Name.L)
	}
	distributions := make([]*pdHttp.RegionDistribution, 0)
	var resp *pdHttp.RegionDistributions
	for idx, pid := range physicalIDs {
		startKey := codec.EncodeBytes([]byte{}, tablecodec.GenTablePrefix(pid))
		endKey := codec.EncodeBytes([]byte{}, tablecodec.GenTablePrefix(pid+1))
		// todo： support engine type
		resp, err = infosync.GetRegionDistributionByKeyRange(ctx, startKey, endKey, "")
		if err != nil {
			return err
		}
		e.fillDistributionsToChunk(partitionNames[idx], resp.RegionDistributions)
		distributions = append(distributions, resp.RegionDistributions...)
	}
	return nil
}

func (e *ShowExec) fetchShowTableRegions(ctx context.Context) error {
	store := e.Ctx().GetStore()
	tikvStore, ok := store.(helper.Storage)
	if !ok {
		return nil
	}
	splitStore, ok := store.(kv.SplittableStore)
	if !ok {
		return nil
	}

	tb, err := e.getTable()
	if err != nil {
		return errors.Trace(err)
	}

	physicalIDs := []int64{}
	hasGlobalIndex := false
	if pi := tb.Meta().GetPartitionInfo(); pi != nil {
		for _, name := range e.Table.PartitionNames {
			pid, err := tables.FindPartitionByName(tb.Meta(), name.L)
			if err != nil {
				return err
			}
			physicalIDs = append(physicalIDs, pid)
		}
		if len(physicalIDs) == 0 {
			for _, p := range pi.Definitions {
				physicalIDs = append(physicalIDs, p.ID)
			}
		}
		// when table has global index, show the logical table region.
		for _, index := range tb.Meta().Indices {
			if index.Global {
				hasGlobalIndex = true
				break
			}
		}
	} else {
		if len(e.Table.PartitionNames) != 0 {
			return plannererrors.ErrPartitionClauseOnNonpartitioned
		}
		physicalIDs = append(physicalIDs, tb.Meta().ID)
	}

	// Get table regions from pd, not from regionCache, because the region cache maybe outdated.
	var regions []regionMeta
	if len(e.IndexName.L) != 0 {
		// show table * index * region
		indexInfo := tb.Meta().FindIndexByName(e.IndexName.L)
		if indexInfo == nil {
			return plannererrors.ErrKeyDoesNotExist.GenWithStackByArgs(e.IndexName, tb.Meta().Name)
		}
		if indexInfo.Global {
			regions, err = getTableIndexRegions(indexInfo, []int64{tb.Meta().ID}, tikvStore, splitStore)
		} else {
			regions, err = getTableIndexRegions(indexInfo, physicalIDs, tikvStore, splitStore)
		}
	} else {
		// show table * region
		if hasGlobalIndex {
			physicalIDs = append([]int64{tb.Meta().ID}, physicalIDs...)
		}
		regions, err = getTableRegions(tb, physicalIDs, tikvStore, splitStore)
	}
	if err != nil {
		return err
	}

	regionRowItem, err := e.fetchSchedulingInfo(ctx, regions, tb.Meta())
	if err != nil {
		return err
	}

	e.fillRegionsToChunk(regionRowItem)
	return nil
}

func (e *ShowExec) fetchSchedulingInfo(ctx context.Context, regions []regionMeta, tbInfo *model.TableInfo) ([]showTableRegionRowItem, error) {
	scheduleState := make(map[int64]infosync.PlacementScheduleState)
	schedulingConstraints := make(map[int64]*model.PlacementSettings)
	regionRowItem := make([]showTableRegionRowItem, 0)
	tblPlacement, err := e.getTablePlacement(tbInfo)
	if err != nil {
		return nil, err
	}

	if tbInfo.GetPartitionInfo() != nil {
		// partitioned table
		for _, part := range tbInfo.GetPartitionInfo().Definitions {
			_, err = fetchScheduleState(ctx, scheduleState, part.ID)
			if err != nil {
				return nil, err
			}
			placement, err := e.getPolicyPlacement(part.PlacementPolicyRef)
			if err != nil {
				return nil, err
			}
			if placement == nil {
				schedulingConstraints[part.ID] = tblPlacement
			} else {
				schedulingConstraints[part.ID] = placement
			}
		}
	} else {
		// un-partitioned table or index
		schedulingConstraints[tbInfo.ID] = tblPlacement
		_, err = fetchScheduleState(ctx, scheduleState, tbInfo.ID)
		if err != nil {
			return nil, err
		}
	}
	var constraintStr string
	var scheduleStateStr string
	for i := range regions {
		if constraint, ok := schedulingConstraints[regions[i].physicalID]; ok && constraint != nil {
			constraintStr = constraint.String()
			scheduleStateStr = scheduleState[regions[i].physicalID].String()
		} else {
			constraintStr = ""
			scheduleStateStr = ""
		}
		regionRowItem = append(regionRowItem, showTableRegionRowItem{
			regionMeta:            regions[i],
			schedulingConstraints: constraintStr,
			schedulingState:       scheduleStateStr,
		})
	}
	return regionRowItem, nil
}

func getTableRegions(tb table.Table, physicalIDs []int64, tikvStore helper.Storage, splitStore kv.SplittableStore) ([]regionMeta, error) {
	regions := make([]regionMeta, 0, len(physicalIDs))
	uniqueRegionMap := make(map[uint64]struct{})
	for _, id := range physicalIDs {
		rs, err := getPhysicalTableRegions(id, tb.Meta(), tikvStore, splitStore, uniqueRegionMap)
		if err != nil {
			return nil, err
		}
		regions = append(regions, rs...)
	}
	return regions, nil
}

func getTableIndexRegions(indexInfo *model.IndexInfo, physicalIDs []int64, tikvStore helper.Storage, splitStore kv.SplittableStore) ([]regionMeta, error) {
	regions := make([]regionMeta, 0, len(physicalIDs))
	uniqueRegionMap := make(map[uint64]struct{})
	for _, id := range physicalIDs {
		rs, err := getPhysicalIndexRegions(id, indexInfo, tikvStore, splitStore, uniqueRegionMap)
		if err != nil {
			return nil, err
		}
		regions = append(regions, rs...)
	}
	return regions, nil
}

func (e *ShowExec) fillDistributionsToChunk(partitionName string, distributions []*pdHttp.RegionDistribution) {
	for _, dis := range distributions {
		e.result.AppendString(0, partitionName)
		e.result.AppendUint64(1, dis.StoreID)
		e.result.AppendString(2, dis.EngineType)
		e.result.AppendInt64(3, int64(dis.RegionLeaderCount))
		e.result.AppendInt64(4, int64(dis.RegionPeerCount))
		e.result.AppendUint64(5, dis.RegionWriteBytes)
		e.result.AppendUint64(6, dis.RegionWriteKeys)
		e.result.AppendUint64(7, dis.RegionWriteQuery)
		e.result.AppendUint64(8, dis.RegionLeaderReadBytes)
		e.result.AppendUint64(9, dis.RegionLeaderReadKeys)
		e.result.AppendUint64(10, dis.RegionLeaderReadQuery)
		e.result.AppendUint64(11, dis.RegionPeerReadBytes)
		e.result.AppendUint64(12, dis.RegionPeerReadKeys)
		e.result.AppendUint64(13, dis.RegionPeerReadQuery)
	}
}

func (e *ShowExec) fillRegionsToChunk(regions []showTableRegionRowItem) {
	for i := range regions {
		e.result.AppendUint64(0, regions[i].region.Id)
		e.result.AppendString(1, regions[i].start)
		e.result.AppendString(2, regions[i].end)
		e.result.AppendUint64(3, regions[i].leaderID)
		e.result.AppendUint64(4, regions[i].storeID)

		peers := ""
		for i, peer := range regions[i].region.Peers {
			if i > 0 {
				peers += ", "
			}
			peers += strconv.FormatUint(peer.Id, 10)
		}
		e.result.AppendString(5, peers)
		if regions[i].scattering {
			e.result.AppendInt64(6, 1)
		} else {
			e.result.AppendInt64(6, 0)
		}

		e.result.AppendUint64(7, regions[i].writtenBytes)
		e.result.AppendUint64(8, regions[i].readBytes)
		e.result.AppendInt64(9, regions[i].approximateSize)
		e.result.AppendInt64(10, regions[i].approximateKeys)
		e.result.AppendString(11, regions[i].schedulingConstraints)
		e.result.AppendString(12, regions[i].schedulingState)
	}
}

func (e *ShowExec) fetchShowBuiltins() error {
	for _, f := range expression.GetBuiltinList() {
		e.appendRow([]any{f})
	}
	return nil
}

func (e *ShowExec) fetchShowSessionStates(ctx context.Context) error {
	sessionStates := &sessionstates.SessionStates{}
	err := e.Ctx().EncodeStates(ctx, sessionStates)
	if err != nil {
		return err
	}
	stateBytes, err := gjson.Marshal(sessionStates)
	if err != nil {
		return errors.Trace(err)
	}
	stateJSON := types.BinaryJSON{}
	if err = stateJSON.UnmarshalJSON(stateBytes); err != nil {
		return err
	}
	// session token
	var token *sessionstates.SessionToken
	// In testing, user may be nil.
	if user := e.Ctx().GetSessionVars().User; user != nil {
		// The token may be leaked without secure transport, but the cloud can ensure security in some situations,
		// so we don't enforce secure connections.
		if token, err = sessionstates.CreateSessionToken(user.Username); err != nil {
			// Some users deploy TiProxy after the cluster is running and configuring signing certs will restart TiDB.
			// The users may don't need connection migration, e.g. they only want traffic replay, which requires session states
			// but not session tokens. So we don't return errors, just log it.
			logutil.Logger(ctx).Warn("create session token failed", zap.Error(err))
		}
	}
	if token != nil {
		tokenBytes, err := gjson.Marshal(token)
		if err != nil {
			return errors.Trace(err)
		}
		tokenJSON := types.BinaryJSON{}
		if err = tokenJSON.UnmarshalJSON(tokenBytes); err != nil {
			return err
		}
		e.appendRow([]any{stateJSON, tokenJSON})
	} else {
		e.appendRow([]any{stateJSON, nil})
	}
	return nil
}

// FillOneImportJobInfo is exported for testing.
func FillOneImportJobInfo(result *chunk.Chunk, info *importer.JobInfo, runInfo *importinto.RuntimeInfo) {
	fullTableName := utils.EncloseDBAndTable(info.TableSchema, info.TableName)
	result.AppendInt64(0, info.ID)
	if info.GroupKey == "" {
		result.AppendNull(1)
	} else {
		result.AppendString(1, info.GroupKey)
	}

	result.AppendString(2, info.Parameters.FileLocation)
	result.AppendString(3, fullTableName)
	result.AppendInt64(4, info.TableID)
	result.AppendString(5, info.Step)
	result.AppendString(6, info.Status)
	result.AppendString(7, units.BytesSize(float64(info.SourceFileSize)))
	if runInfo != nil {
		// running import job
		result.AppendUint64(8, uint64(runInfo.ImportRows))
	} else if info.IsSuccess() {
		// successful import job
		result.AppendUint64(8, uint64(info.Summary.ImportedRows))
	} else {
		// failed import job
		result.AppendNull(8)
	}

	if info.IsSuccess() {
		msgItems := make([]string, 0, 1)
		if info.Summary.ConflictRowCnt > 0 {
			msgItems = append(msgItems, fmt.Sprintf("%d conflicted rows.", info.Summary.ConflictRowCnt))
		}
		if info.Summary.TooManyConflicts {
			msgItems = append(msgItems, "Too many conflicted rows, checksum skipped.")
		}
		result.AppendString(9, strings.Join(msgItems, " "))
	} else {
		result.AppendString(9, info.ErrorMessage)
	}

	result.AppendTime(10, info.CreateTime)
	if info.StartTime.IsZero() {
		result.AppendNull(11)
	} else {
		result.AppendTime(11, info.StartTime)
	}
	if info.EndTime.IsZero() {
		result.AppendNull(12)
	} else {
		result.AppendTime(12, info.EndTime)
	}
	result.AppendString(13, info.CreatedBy)

	// For finished job, only keep the update time same as end time
	// and fill other fields with null.
	if runInfo == nil {
		if info.EndTime.IsZero() {
			result.AppendNull(14)
		} else {
			result.AppendTime(14, info.EndTime)
		}
		for i := 15; i < 21; i++ {
			result.AppendNull(i)
		}
		return
	}

	// update time of run info comes from subtask summary, but checksum step don't
	// have period updated summary.
	updateTime := runInfo.UpdateTime
	if updateTime.IsZero() {
		updateTime = info.UpdateTime
	}
	result.AppendTime(14, updateTime)
	result.AppendString(15, proto.Step2Str(proto.ImportInto, runInfo.Step))
	result.AppendString(16, runInfo.ProcessedSize())
	result.AppendString(17, runInfo.TotalSize())
	result.AppendString(18, runInfo.Percent())
	result.AppendString(19, fmt.Sprintf("%s/s", units.BytesSize(float64(runInfo.Speed))))
	result.AppendString(20, runInfo.ETA())
}

func handleImportJobInfo(
	ctx context.Context, location *time.Location,
	info *importer.JobInfo, result *chunk.Chunk,
) error {
	var (
		runInfo *importinto.RuntimeInfo
		err     error
	)

	if info.Status == importer.JobStatusRunning {
		// need to get more info from distributed framework for running jobs
		runInfo, err = importinto.GetRuntimeInfoForJob(ctx, location, info.ID)
		if err != nil {
			return err
		}
		if runInfo.Status == proto.TaskStateAwaitingResolution {
			info.Status = string(runInfo.Status)
			info.ErrorMessage = runInfo.ErrorMsg
		}
	}
	FillOneImportJobInfo(result, info, runInfo)
	return nil
}

const balanceRangeScheduler = "balance-range-scheduler"

func (e *ShowExec) fetchShowDistributionJobs(ctx context.Context) error {
	config, err := infosync.GetSchedulerConfig(ctx, balanceRangeScheduler)
	if err != nil {
		return err
	}
	configs, ok := config.([]any)
	if !ok {
		// it means that no any jobs
		return nil
	}
	jobs := make([]map[string]any, 0, len(configs))
	for _, cfg := range configs {
		job, ok := cfg.(map[string]any)
		if !ok {
			return errs.ErrClientProtoUnmarshal.FastGenByArgs(cfg)
		}
		jobs = append(jobs, job)
	}
	if e.DistributionJobID != nil {
		for _, job := range jobs {
			jobID, ok := job["job-id"].(float64)
			if ok && *e.DistributionJobID == int64(jobID) {
				if err := fillDistributionJobToChunk(ctx, job, e.result); err != nil {
					return err
				}
				break
			}
		}
	} else {
		for _, job := range jobs {
			if err := fillDistributionJobToChunk(ctx, job, e.result); err != nil {
				return err
			}
		}
	}
	return nil
}

// fillDistributionJobToChunk fills the distribution job to the chunk
func fillDistributionJobToChunk(ctx context.Context, job map[string]any, result *chunk.Chunk) error {
	// alias is {db_name}.{table_name}.{partition_name}
	alias := strings.Split(job["alias"].(string), ".")
	logutil.Logger(ctx).Info("fillDistributionJobToChunk", zap.String("alias", job["alias"].(string)))
	if len(alias) != 3 {
		return errs.ErrClientProtoUnmarshal.FastGenByArgs(fmt.Sprintf("alias:%s is invalid", job["alias"].(string)))
	}
	result.AppendUint64(0, uint64(job["job-id"].(float64)))
	result.AppendString(1, alias[0])
	result.AppendString(2, alias[1])
	// partition name maybe empty when the table is not partitioned
	if alias[2] == "" {
		result.AppendNull(3)
	} else {
		result.AppendString(3, alias[2])
	}
	result.AppendString(4, job["engine"].(string))
	result.AppendString(5, job["rule"].(string))
	result.AppendString(6, job["status"].(string))
	timeout := uint64(job["timeout"].(float64))
	result.AppendString(7, time.Duration(timeout).String())
	if create, ok := job["create"]; ok {
		createTime := &time.Time{}
		err := createTime.UnmarshalText([]byte(create.(string)))
		if err != nil {
			return err
		}
		result.AppendTime(8, types.NewTime(types.FromGoTime(*createTime), mysql.TypeDatetime, types.DefaultFsp))
	} else {
		result.AppendNull(8)
	}
	if start, ok := job["start"]; ok {
		startTime := &time.Time{}
		err := startTime.UnmarshalText([]byte(start.(string)))
		if err != nil {
			return err
		}
		result.AppendTime(9, types.NewTime(types.FromGoTime(*startTime), mysql.TypeDatetime, types.DefaultFsp))
	} else {
		result.AppendNull(9)
	}
	if finish, ok := job["finish"]; ok {
		finishedTime := &time.Time{}
		err := finishedTime.UnmarshalText([]byte(finish.(string)))
		if err != nil {
			return err
		}
		result.AppendTime(10, types.NewTime(types.FromGoTime(*finishedTime), mysql.TypeDatetime, types.DefaultFsp))
	} else {
		result.AppendNull(10)
	}
	return nil
}

type groupInfo struct {
	groupKey   string
	jobCount   int64
	pending    int64
	running    int64
	completed  int64
	failed     int64
	canceled   int64
	createTime types.Time
	updateTime types.Time
}

func (e *ShowExec) fetchShowImportGroups(ctx context.Context) error {
	sctx := e.Ctx()

	var hasSuperPriv bool
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		hasSuperPriv = pm.RequestVerification(sctx.GetSessionVars().ActiveRoles, "", "", "", mysql.SuperPriv)
	}
	// we use sessionCtx from GetTaskManager, user ctx might not have system table privileges.
	taskManager, err := fstorage.GetTaskManager()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalDistTask)
	if err != nil {
		return err
	}

	var infos []*importer.JobInfo
	if err = taskManager.WithNewSession(func(se sessionctx.Context) error {
		exec := se.GetSQLExecutor()
		var err2 error
		infos, err2 = importer.GetJobsByGroupKey(ctx, exec, sctx.GetSessionVars().User.String(), e.ImportGroupKey, hasSuperPriv)
		return err2
	}); err != nil {
		return err
	}

	groupMap := make(map[string]*groupInfo)
	for _, info := range infos {
		if _, ok := groupMap[info.GroupKey]; !ok {
			groupMap[info.GroupKey] = &groupInfo{
				groupKey:   info.GroupKey,
				createTime: types.ZeroTime,
				updateTime: types.ZeroTime,
			}
		}

		updateTime := types.ZeroTime
		// See FillOneImportJobInfo, we make the update time calculation same with SHOW IMPORT JOBS.
		if info.Status == importer.JobStatusRunning {
			runInfo, err := importinto.GetRuntimeInfoForJob(ctx, sctx.GetSessionVars().Location(), info.ID)
			if err != nil {
				return err
			}
			updateTime = runInfo.UpdateTime
			if updateTime.IsZero() {
				updateTime = info.UpdateTime
			}
		}

		gInfo := groupMap[info.GroupKey]
		if !info.CreateTime.IsZero() && (gInfo.createTime.IsZero() || info.CreateTime.Compare(gInfo.createTime) < 0) {
			gInfo.createTime = info.CreateTime
		}
		if updateTime.Compare(gInfo.updateTime) > 0 {
			gInfo.updateTime = updateTime
		}

		// See pkg/executor/importer/job.go for job status
		gInfo.jobCount++
		switch info.Status {
		case "pending":
			gInfo.pending++
		case "running":
			gInfo.running++
		case "finished":
			gInfo.completed++
		case "failed":
			gInfo.failed++
		case "cancelled":
			gInfo.canceled++
		}
	}

	for _, gInfo := range groupMap {
		e.result.AppendString(0, gInfo.groupKey)
		e.result.AppendInt64(1, gInfo.jobCount)
		e.result.AppendInt64(2, gInfo.pending)
		e.result.AppendInt64(3, gInfo.running)
		e.result.AppendInt64(4, gInfo.completed)
		e.result.AppendInt64(5, gInfo.failed)
		e.result.AppendInt64(6, gInfo.canceled)
		if gInfo.createTime.IsZero() {
			e.result.AppendNull(7)
		} else {
			e.result.AppendTime(7, gInfo.createTime)
		}
		if gInfo.updateTime.IsZero() {
			e.result.AppendNull(8)
		} else {
			e.result.AppendTime(8, gInfo.updateTime)
		}
	}
	return nil
}

// fetchShowImportJobs fills the result with the schema:
// {"Job_ID", "Group_Key", "Data_Source", "Target_Table", "Table_ID",
// "Phase", "Status", "Source_File_Size", "Imported_Rows",
// "Result_Message", "Create_Time", "Start_Time", "End_Time", "Created_By"}
func (e *ShowExec) fetchShowImportJobs(ctx context.Context) error {
	sctx := e.Ctx()

	var hasSuperPriv bool
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		hasSuperPriv = pm.RequestVerification(sctx.GetSessionVars().ActiveRoles, "", "", "", mysql.SuperPriv)
	}
	// we use sessionCtx from GetTaskManager, user ctx might not have system table privileges.
	taskManager, err := fstorage.GetTaskManager()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalDistTask)
	if err != nil {
		return err
	}

	loc := sctx.GetSessionVars().Location()
	if e.ImportJobID != nil {
		var info *importer.JobInfo
		if err = taskManager.WithNewSession(func(se sessionctx.Context) error {
			exec := se.GetSQLExecutor()
			var err2 error
			info, err2 = importer.GetJob(ctx, exec, *e.ImportJobID, sctx.GetSessionVars().User.String(), hasSuperPriv)
			return err2
		}); err != nil {
			return err
		}
		return handleImportJobInfo(ctx, loc, info, e.result)
	}
	var infos []*importer.JobInfo
	if err = taskManager.WithNewSession(func(se sessionctx.Context) error {
		exec := se.GetSQLExecutor()
		var err2 error
		infos, err2 = importer.GetAllViewableJobs(ctx, exec, sctx.GetSessionVars().User.String(), hasSuperPriv)
		return err2
	}); err != nil {
		return err
	}
	for _, info := range infos {
		if err2 := handleImportJobInfo(ctx, loc, info, e.result); err2 != nil {
			return err2
		}
	}
	// TODO: does not support filtering for now
	return nil
}

// tryFillViewColumnType fill the columns type info of a view.
// Because view's underlying table's column could change or recreate, so view's column type may change over time.
// To avoid this situation we need to generate a logical plan and extract current column types from Schema.
func tryFillViewColumnType(ctx context.Context, sctx sessionctx.Context, is infoschema.InfoSchema, dbName ast.CIStr, tbl *model.TableInfo) error {
	if !tbl.IsView() {
		return nil
	}
	ctx = kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	// We need to run the build plan process in another session because there may be
	// multiple goroutines running at the same time while session is not goroutine-safe.
	// Take joining system table as an example, `fetchBuildSideRows` and `fetchProbeSideChunks` can be run concurrently.
	return runWithSystemSession(ctx, sctx, func(s sessionctx.Context) error {
		// Retrieve view columns info.
		planBuilder, _ := plannercore.NewPlanBuilder(
			plannercore.PlanBuilderOptNoExecution{}).Init(s.GetPlanCtx(), is, hint.NewQBHintHandler(nil))
		viewLogicalPlan, err := planBuilder.BuildDataSourceFromView(ctx, dbName, tbl, nil, nil)
		if err != nil {
			return err
		}
		viewSchema := viewLogicalPlan.Schema()
		viewOutputNames := viewLogicalPlan.OutputNames()
		for _, col := range tbl.Columns {
			idx := expression.FindFieldNameIdxByColName(viewOutputNames, col.Name.L)
			if idx >= 0 {
				col.FieldType = *viewSchema.Columns[idx].GetType(sctx.GetExprCtx().GetEvalCtx())
			}
			if col.GetType() == mysql.TypeVarString {
				col.SetType(mysql.TypeVarchar)
			}
		}
		return nil
	})
}

func runWithSystemSession(ctx context.Context, sctx sessionctx.Context, fn func(sessionctx.Context) error) error {
	b := exec.NewBaseExecutor(sctx, nil, 0)
	sysCtx, err := b.GetSysSession()
	if err != nil {
		return err
	}
	defer b.ReleaseSysSession(ctx, sysCtx)

	if err = loadSnapshotInfoSchemaIfNeeded(sysCtx, sctx.GetSessionVars().SnapshotTS); err != nil {
		return err
	}
	// `fn` may use KV transaction, so initialize the txn here
	if err = sessiontxn.NewTxn(ctx, sysCtx); err != nil {
		return err
	}
	defer sysCtx.RollbackTxn(ctx)
	if err = ResetContextOfStmt(sysCtx, &ast.SelectStmt{}); err != nil {
		return err
	}
	return fn(sysCtx)
}
