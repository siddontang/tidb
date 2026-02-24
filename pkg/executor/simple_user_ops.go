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
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/querywatch"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	pwdValidator "github.com/pingcap/tidb/pkg/util/password-validation"
	sem "github.com/pingcap/tidb/pkg/util/sem/compat"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

func (e *SimpleExec) executeAlterUser(ctx context.Context, s *ast.AlterUserStmt) error {
	disableSandBoxMode := false
	var err error
	if e.Ctx().InSandBoxMode() {
		if err = e.checkSandboxMode(s.Specs); err != nil {
			return err
		}
		disableSandBoxMode = true
	}
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnPrivilege)
	if s.CurrentAuth != nil {
		user := e.Ctx().GetSessionVars().User
		if user == nil {
			return errors.New("Session user is empty")
		}
		// Use AuthHostname to search the user record, set Hostname as AuthHostname.
		userCopy := *user
		userCopy.Hostname = userCopy.AuthHostname
		spec := &ast.UserSpec{
			User:    &userCopy,
			AuthOpt: s.CurrentAuth,
		}
		s.Specs = []*ast.UserSpec{spec}
	}

	userResource := &resourceOptionsInfo{
		maxQueriesPerHour:     0,
		maxUpdatesPerHour:     0,
		maxConnectionsPerHour: 0,
		// can't set 0 to maxUserConnections as default, because user could set 0 to this field.
		// so we use -1(invalid value) as default.
		maxUserConnections: -1,
	}

	err = userResource.loadResourceOptions(s.ResourceOptions)
	if err != nil {
		return err
	}

	plOptions := passwordOrLockOptionsInfo{
		lockAccount:                 "",
		passwordExpired:             "",
		passwordLifetime:            notSpecified,
		passwordHistory:             notSpecified,
		passwordReuseInterval:       notSpecified,
		failedLoginAttemptsChange:   false,
		passwordLockTimeChange:      false,
		passwordHistoryChange:       false,
		passwordReuseIntervalChange: false,
	}
	err = plOptions.loadOptions(s.PasswordOrLockOptions)
	if err != nil {
		return err
	}

	privData, err := tlsOption2GlobalPriv(s.AuthTokenOrTLSOptions)
	if err != nil {
		return err
	}

	failedUsers := make([]string, 0, len(s.Specs))
	needRollback := false
	checker := privilege.GetPrivilegeManager(e.Ctx())
	if checker == nil {
		return errors.New("could not load privilege checker")
	}
	activeRoles := e.Ctx().GetSessionVars().ActiveRoles
	hasCreateUserPriv := checker.RequestVerification(activeRoles, "", "", "", mysql.CreateUserPriv)
	hasSystemUserPriv := checker.RequestDynamicVerification(activeRoles, "SYSTEM_USER", false)
	hasRestrictedUserPriv := checker.RequestDynamicVerification(activeRoles, "RESTRICTED_USER_ADMIN", false)
	hasSystemSchemaPriv := checker.RequestVerification(activeRoles, mysql.SystemDB, mysql.UserTable, "", mysql.UpdatePriv)

	var authTokenOptions []*ast.AuthTokenOrTLSOption
	for _, authTokenOrTLSOption := range s.AuthTokenOrTLSOptions {
		if authTokenOrTLSOption.Type == ast.TokenIssuer {
			authTokenOptions = append(authTokenOptions, authTokenOrTLSOption)
		}
	}

	sysSession, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(ctx, sysSession)
	sqlExecutor := sysSession.GetSQLExecutor()
	// session isolation level changed to READ-COMMITTED.
	// When tidb is at the RR isolation level, executing `begin` will obtain a consistent state.
	// When operating the same user concurrently, it may happen that historical versions are read.
	// In order to avoid this risk, change the isolation level to RC.
	_, err = sqlExecutor.ExecuteInternal(ctx, "set tx_isolation = 'READ-COMMITTED'")
	if err != nil {
		return err
	}
	if _, err := sqlExecutor.ExecuteInternal(ctx, "BEGIN PESSIMISTIC"); err != nil {
		return err
	}

	for _, spec := range s.Specs {
		user := e.Ctx().GetSessionVars().User
		alterCurrentUser := spec.User.CurrentUser || ((user != nil) && (user.Username == spec.User.Username) && (user.AuthHostname == spec.User.Hostname))
		alterPassword := false
		if spec.AuthOpt != nil && spec.AuthOpt.AuthPlugin == "" {
			if len(s.AuthTokenOrTLSOptions) == 0 && len(s.ResourceOptions) == 0 && len(s.PasswordOrLockOptions) == 0 {
				alterPassword = true
			}
		}
		if alterCurrentUser && alterPassword {
			spec.User.Username = user.Username
			spec.User.Hostname = user.AuthHostname
		} else {
			// The user executing the query (user) does not match the user specified (spec.User)
			// The MySQL manual states:
			// "In most cases, ALTER USER requires the global CREATE USER privilege, or the UPDATE privilege for the mysql system schema"
			//
			// This is true unless the user being modified has the SYSTEM_USER dynamic privilege.
			// See: https://mysqlserverteam.com/the-system_user-dynamic-privilege/
			//
			// In the current implementation of DYNAMIC privileges, SUPER can be used as a substitute for any DYNAMIC privilege
			// (unless SEM is enabled; in which case RESTRICTED_* privileges will not use SUPER as a substitute). This is intentional
			// because visitInfo can not accept OR conditions for permissions and in many cases MySQL permits SUPER instead.

			// Thus, any user with SUPER can effectively ALTER/DROP a SYSTEM_USER, and
			// any user with only CREATE USER can not modify the properties of users with SUPER privilege.
			// We extend this in TiDB with SEM, where SUPER users can not modify users with RESTRICTED_USER_ADMIN.
			// For simplicity: RESTRICTED_USER_ADMIN also counts for SYSTEM_USER here.

			if !(hasCreateUserPriv || hasSystemSchemaPriv) {
				return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
			}
			if !(hasSystemUserPriv || hasRestrictedUserPriv) && checker.RequestDynamicVerificationWithUser(ctx, "SYSTEM_USER", false, spec.User) {
				return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SYSTEM_USER or SUPER")
			}
			if sem.IsEnabled() && !hasRestrictedUserPriv && checker.RequestDynamicVerificationWithUser(ctx, "RESTRICTED_USER_ADMIN", false, spec.User) {
				return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("RESTRICTED_USER_ADMIN")
			}
		}

		exists, currentAuthPlugin, err := userExistsInternal(ctx, sqlExecutor, spec.User.Username, spec.User.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			user := fmt.Sprintf(`'%s'@'%s'`, spec.User.Username, spec.User.Hostname)
			failedUsers = append(failedUsers, user)
			continue
		}

		type AuthTokenOptionHandler int
		const (
			// noNeedAuthTokenOptions means the final auth plugin is NOT tidb_auth_plugin
			noNeedAuthTokenOptions AuthTokenOptionHandler = iota
			// OptionalAuthTokenOptions means the final auth_plugin is tidb_auth_plugin,
			// and whether to declare AuthTokenOptions or not is ok.
			OptionalAuthTokenOptions
			// RequireAuthTokenOptions means the final auth_plugin is tidb_auth_plugin and need AuthTokenOptions here
			RequireAuthTokenOptions
		)
		authTokenOptionHandler := noNeedAuthTokenOptions
		if currentAuthPlugin == mysql.AuthTiDBAuthToken {
			authTokenOptionHandler = OptionalAuthTokenOptions
		}

		type alterField struct {
			expr  string
			value any
		}
		var fields []alterField
		if spec.AuthOpt != nil {
			fields = append(fields, alterField{"password_last_changed=current_timestamp()", nil})
			if spec.AuthOpt.AuthPlugin == "" {
				spec.AuthOpt.AuthPlugin = currentAuthPlugin
			}
			extensions, err := extension.GetExtensions()
			if err != nil {
				return exeerrors.ErrPluginIsNotLoaded.GenWithStackByArgs(err.Error())
			}
			authPlugins := extensions.GetAuthPlugins()
			var authPluginImpl *extension.AuthPlugin
			switch spec.AuthOpt.AuthPlugin {
			case mysql.AuthNativePassword, mysql.AuthCachingSha2Password, mysql.AuthTiDBSM3Password, mysql.AuthSocket, mysql.AuthLDAPSimple, mysql.AuthLDAPSASL, "":
				authTokenOptionHandler = noNeedAuthTokenOptions
			case mysql.AuthTiDBAuthToken:
				if authTokenOptionHandler != OptionalAuthTokenOptions {
					authTokenOptionHandler = RequireAuthTokenOptions
				}
			default:
				found := false
				if authPluginImpl, found = authPlugins[spec.AuthOpt.AuthPlugin]; !found {
					return exeerrors.ErrPluginIsNotLoaded.GenWithStackByArgs(spec.AuthOpt.AuthPlugin)
				}
			}
			// changing the auth method prunes history.
			if spec.AuthOpt.AuthPlugin != currentAuthPlugin {
				// delete password history from mysql.password_history.
				sql := new(strings.Builder)
				sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, mysql.PasswordHistoryTable, spec.User.Hostname, spec.User.Username)
				if _, err := sqlExecutor.ExecuteInternal(ctx, sql.String()); err != nil {
					failedUsers = append(failedUsers, spec.User.String())
					needRollback = true
					break
				}
			}
			if e.isValidatePasswordEnabled() && spec.AuthOpt.ByAuthString && mysql.IsAuthPluginClearText(spec.AuthOpt.AuthPlugin) {
				if err := pwdValidator.ValidatePassword(e.Ctx().GetSessionVars(), spec.AuthOpt.AuthString); err != nil {
					return err
				}
			}
			// we have assigned the currentAuthPlugin to spec.AuthOpt.AuthPlugin if the latter is empty, so keep the incomming argument defaultPlugin empty is ok.
			pwd, ok := encodePasswordWithPlugin(*spec, authPluginImpl, "")
			if !ok {
				return errors.Trace(exeerrors.ErrPasswordFormat)
			}
			// for Support Password Reuse Policy.
			// The empty password does not count in the password history and is subject to reuse at any time.
			// https://dev.mysql.com/doc/refman/8.0/en/password-management.html#password-reuse-policy
			if len(pwd) != 0 {
				userDetail := &userInfo{
					host:       spec.User.Hostname,
					user:       spec.User.Username,
					pLI:        &plOptions,
					pwd:        pwd,
					authString: spec.AuthOpt.AuthString,
				}
				err := checkPasswordReusePolicy(ctx, sqlExecutor, userDetail, e.Ctx(), spec.AuthOpt.AuthPlugin, authPlugins)
				if err != nil {
					return err
				}
			}
			fields = append(fields, alterField{"authentication_string=%?", pwd})
			if spec.AuthOpt.AuthPlugin != "" {
				fields = append(fields, alterField{"plugin=%?", spec.AuthOpt.AuthPlugin})
			}
			if spec.AuthOpt.ByAuthString || spec.AuthOpt.ByHashString {
				if plOptions.passwordExpired == "" {
					plOptions.passwordExpired = "N"
				}
			}
		}

		if len(plOptions.lockAccount) != 0 {
			fields = append(fields, alterField{"account_locked=%?", plOptions.lockAccount})
		}

		// support alter Password_reuse_history and Password_reuse_time.
		if plOptions.passwordHistoryChange {
			if plOptions.passwordHistory == notSpecified {
				fields = append(fields, alterField{"Password_reuse_history = NULL ", ""})
			} else {
				fields = append(fields, alterField{"Password_reuse_history = %? ", strconv.FormatInt(plOptions.passwordHistory, 10)})
			}
		}
		if plOptions.passwordReuseIntervalChange {
			if plOptions.passwordReuseInterval == notSpecified {
				fields = append(fields, alterField{"Password_reuse_time = NULL ", ""})
			} else {
				fields = append(fields, alterField{"Password_reuse_time = %? ", strconv.FormatInt(plOptions.passwordReuseInterval, 10)})
			}
		}

		passwordLockingInfo, err := readPasswordLockingInfo(ctx, sqlExecutor, spec.User.Username, spec.User.Hostname, &plOptions)
		if err != nil {
			return err
		}
		passwordLockingStr := alterUserFailedLoginJSON(passwordLockingInfo, plOptions.lockAccount)

		if len(plOptions.passwordExpired) != 0 {
			if len(spec.User.Username) == 0 && plOptions.passwordExpired == "Y" {
				return exeerrors.ErrPasswordExpireAnonymousUser.GenWithStackByArgs()
			}
			fields = append(fields, alterField{"password_expired=%?", plOptions.passwordExpired})
		}
		if plOptions.passwordLifetime != notSpecified {
			fields = append(fields, alterField{"password_lifetime=%?", plOptions.passwordLifetime})
		}

		if userResource.maxUserConnections >= 0 {
			// need `CREATE USER` privilege for the operation of modifying max_user_connections.
			if !hasCreateUserPriv {
				return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
			}
			fields = append(fields, alterField{"max_user_connections=%?", userResource.maxUserConnections})
		}

		var newAttributes []string
		if s.CommentOrAttributeOption != nil {
			if s.CommentOrAttributeOption.Type == ast.UserCommentType {
				newAttributes = append(newAttributes, fmt.Sprintf(`"metadata": {"comment": "%s"}`, s.CommentOrAttributeOption.Value))
			} else {
				newAttributes = append(newAttributes, fmt.Sprintf(`"metadata": %s`, s.CommentOrAttributeOption.Value))
			}
		}
		if s.ResourceGroupNameOption != nil {
			if !vardef.EnableResourceControl.Load() {
				return infoschema.ErrResourceGroupSupportDisabled
			}
			is, err := isRole(ctx, sqlExecutor, spec.User.Username, spec.User.Hostname)
			if err != nil {
				return err
			}
			if is {
				return infoschema.ErrResourceGroupInvalidForRole
			}

			// check if specified resource group exists
			resourceGroupName := strings.ToLower(s.ResourceGroupNameOption.Value)
			if resourceGroupName != resourcegroup.DefaultResourceGroupName && s.ResourceGroupNameOption.Value != "" {
				_, exists := e.is.ResourceGroupByName(ast.NewCIStr(resourceGroupName))
				if !exists {
					return infoschema.ErrResourceGroupNotExists.GenWithStackByArgs(resourceGroupName)
				}
			}

			newAttributes = append(newAttributes, fmt.Sprintf(`"resource_group": "%s"`, resourceGroupName))
		}
		if passwordLockingStr != "" {
			newAttributes = append(newAttributes, passwordLockingStr)
		}
		if length := len(newAttributes); length > 0 {
			if length > 1 || passwordLockingStr == "" {
				passwordLockingInfo.containsNoOthers = false
			}
			newAttributesStr := fmt.Sprintf("{%s}", strings.Join(newAttributes, ","))
			fields = append(fields, alterField{"user_attributes=json_merge_patch(coalesce(user_attributes, '{}'), %?)", newAttributesStr})
		}

		switch authTokenOptionHandler {
		case noNeedAuthTokenOptions:
			if len(authTokenOptions) > 0 {
				err := errors.NewNoStackError("TOKEN_ISSUER is not needed for the auth plugin")
				e.Ctx().GetSessionVars().StmtCtx.AppendWarning(err)
			}
		case OptionalAuthTokenOptions:
			if len(authTokenOptions) > 0 {
				for _, authTokenOption := range authTokenOptions {
					fields = append(fields, alterField{authTokenOption.Type.String() + "=%?", authTokenOption.Value})
				}
			}
		case RequireAuthTokenOptions:
			if len(authTokenOptions) > 0 {
				for _, authTokenOption := range authTokenOptions {
					fields = append(fields, alterField{authTokenOption.Type.String() + "=%?", authTokenOption.Value})
				}
			} else {
				err := errors.NewNoStackError("Auth plugin 'tidb_auth_plugin' needs TOKEN_ISSUER")
				e.Ctx().GetSessionVars().StmtCtx.AppendWarning(err)
			}
		}

		if len(fields) > 0 {
			sql := new(strings.Builder)
			sqlescape.MustFormatSQL(sql, "UPDATE %n.%n SET ", mysql.SystemDB, mysql.UserTable)
			for i, f := range fields {
				sqlescape.MustFormatSQL(sql, f.expr, f.value)
				if i < len(fields)-1 {
					sqlescape.MustFormatSQL(sql, ",")
				}
			}
			sqlescape.MustFormatSQL(sql, " WHERE Host=%? and User=%?;", spec.User.Hostname, spec.User.Username)
			_, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
			if err != nil {
				failedUsers = append(failedUsers, spec.User.String())
				needRollback = true
				continue
			}
		}

		// Remove useless Password_locking from User_attributes.
		err = deletePasswordLockingAttribute(ctx, sqlExecutor, spec.User.Username, spec.User.Hostname, passwordLockingInfo)
		if err != nil {
			failedUsers = append(failedUsers, spec.User.String())
			needRollback = true
			continue
		}

		if len(privData) > 0 {
			sql := new(strings.Builder)
			sqlescape.MustFormatSQL(sql, "INSERT INTO %n.%n (Host, User, Priv) VALUES (%?,%?,%?) ON DUPLICATE KEY UPDATE Priv = values(Priv)", mysql.SystemDB, mysql.GlobalPrivTable, spec.User.Hostname, spec.User.Username, string(hack.String(privData)))
			_, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
			if err != nil {
				failedUsers = append(failedUsers, spec.User.String())
				needRollback = true
			}
		}
	}
	if len(failedUsers) > 0 {
		// Compatible with MySQL 8.0, `ALTER USER` realizes atomic operation.
		if !s.IfExists || needRollback {
			return exeerrors.ErrCannotUser.GenWithStackByArgs("ALTER USER", strings.Join(failedUsers, ","))
		}
		for _, user := range failedUsers {
			err := infoschema.ErrUserDropExists.FastGenByArgs(user)
			e.Ctx().GetSessionVars().StmtCtx.AppendNote(err)
		}
	}
	if _, err := sqlExecutor.ExecuteInternal(ctx, "commit"); err != nil {
		return err
	}
	users := userSpecToUserList(s.Specs)
	if err = domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege(users); err != nil {
		return err
	}
	if disableSandBoxMode {
		e.Ctx().DisableSandBoxMode()
	}
	return nil
}

func (e *SimpleExec) checkSandboxMode(specs []*ast.UserSpec) error {
	for _, spec := range specs {
		if spec.AuthOpt == nil {
			continue
		}
		if spec.AuthOpt.ByAuthString || spec.AuthOpt.ByHashString {
			if spec.User.CurrentUser || e.Ctx().GetSessionVars().User.Username == spec.User.Username {
				return nil
			}
		}
	}
	return exeerrors.ErrMustChangePassword.GenWithStackByArgs()
}

func (e *SimpleExec) executeGrantRole(ctx context.Context, s *ast.GrantRoleStmt) error {
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)

	e.setCurrentUser(s.Users)

	for _, role := range s.Roles {
		exists, err := userExists(ctx, e.Ctx(), role.Username, role.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return exeerrors.ErrGrantRole.GenWithStackByArgs(role.String())
		}
	}
	for _, user := range s.Users {
		exists, err := userExists(ctx, e.Ctx(), user.Username, user.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return exeerrors.ErrCannotUser.GenWithStackByArgs("GRANT ROLE", user.String())
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
		return err
	}

	sql := new(strings.Builder)
	for _, user := range s.Users {
		for _, role := range s.Roles {
			sql.Reset()
			sqlescape.MustFormatSQL(sql, `INSERT IGNORE INTO %n.%n (FROM_HOST, FROM_USER, TO_HOST, TO_USER) VALUES (%?,%?,%?,%?)`, mysql.SystemDB, mysql.RoleEdgeTable, role.Hostname, role.Username, user.Hostname, user.Username)
			if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
				logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
				if _, err := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); err != nil {
					return err
				}
				return exeerrors.ErrCannotUser.GenWithStackByArgs("GRANT ROLE", user.String())
			}
		}
	}
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "commit"); err != nil {
		return err
	}
	userList := userIdentityToUserList(s.Users)
	return domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege(userList)
}

// Should cover same internal mysql.* tables as DROP USER, so this function is very similar
func (e *SimpleExec) executeRenameUser(s *ast.RenameUserStmt) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	var failedUser string
	sysSession, err := e.GetSysSession()
	defer e.ReleaseSysSession(ctx, sysSession)
	if err != nil {
		return err
	}
	sqlExecutor := sysSession.GetSQLExecutor()

	if _, err := sqlExecutor.ExecuteInternal(ctx, "BEGIN PESSIMISTIC"); err != nil {
		return err
	}
	for _, userToUser := range s.UserToUsers {
		oldUser, newUser := userToUser.OldUser, userToUser.NewUser
		if len(newUser.Username) > auth.UserNameMaxLength {
			return exeerrors.ErrWrongStringLength.GenWithStackByArgs(newUser.Username, "user name", auth.UserNameMaxLength)
		}
		if len(newUser.Hostname) > auth.HostNameMaxLength {
			return exeerrors.ErrWrongStringLength.GenWithStackByArgs(newUser.Hostname, "host name", auth.HostNameMaxLength)
		}
		exists, _, err := userExistsInternal(ctx, sqlExecutor, oldUser.Username, oldUser.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			failedUser = oldUser.String() + " TO " + newUser.String() + " old did not exist"
			break
		}

		exists, _, err = userExistsInternal(ctx, sqlExecutor, newUser.Username, newUser.Hostname)
		if err != nil {
			return err
		}
		if exists {
			// MySQL reports the old user, even when the issue is the new user.
			failedUser = oldUser.String() + " TO " + newUser.String() + " new did exist"
			break
		}

		if err = renameUserHostInSystemTable(sqlExecutor, mysql.UserTable, "User", "Host", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.UserTable + " error"
			break
		}

		// rename privileges from mysql.global_priv
		if err = renameUserHostInSystemTable(sqlExecutor, mysql.GlobalPrivTable, "User", "Host", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.GlobalPrivTable + " error"
			break
		}

		// rename privileges from mysql.db
		if err = renameUserHostInSystemTable(sqlExecutor, mysql.DBTable, "User", "Host", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.DBTable + " error"
			break
		}

		// rename privileges from mysql.tables_priv
		if err = renameUserHostInSystemTable(sqlExecutor, mysql.TablePrivTable, "User", "Host", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.TablePrivTable + " error"
			break
		}

		// rename relationship from mysql.role_edges
		if err = renameUserHostInSystemTable(sqlExecutor, mysql.RoleEdgeTable, "TO_USER", "TO_HOST", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.RoleEdgeTable + " (to) error"
			break
		}

		if err = renameUserHostInSystemTable(sqlExecutor, mysql.RoleEdgeTable, "FROM_USER", "FROM_HOST", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.RoleEdgeTable + " (from) error"
			break
		}

		// rename relationship from mysql.default_roles
		if err = renameUserHostInSystemTable(sqlExecutor, mysql.DefaultRoleTable, "DEFAULT_ROLE_USER", "DEFAULT_ROLE_HOST", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.DefaultRoleTable + " (default role user) error"
			break
		}

		if err = renameUserHostInSystemTable(sqlExecutor, mysql.DefaultRoleTable, "USER", "HOST", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.DefaultRoleTable + " error"
			break
		}

		// rename passwordhistory from  PasswordHistoryTable.
		if err = renameUserHostInSystemTable(sqlExecutor, mysql.PasswordHistoryTable, "USER", "HOST", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " " + mysql.PasswordHistoryTable + " error"
			break
		}

		// rename relationship from mysql.global_grants
		// TODO: add global_grants into the parser
		// TODO: need update columns_priv once we implement columns_priv functionality.
		// When that is added, please refactor both executeRenameUser and executeDropUser to use an array of tables
		// to loop over, so it is easier to maintain.
		if err = renameUserHostInSystemTable(sqlExecutor, "global_grants", "User", "Host", userToUser); err != nil {
			failedUser = oldUser.String() + " TO " + newUser.String() + " mysql.global_grants error"
			break
		}
	}

	if failedUser != "" {
		if _, err := sqlExecutor.ExecuteInternal(ctx, "rollback"); err != nil {
			return err
		}
		return exeerrors.ErrCannotUser.GenWithStackByArgs("RENAME USER", failedUser)
	}
	if _, err := sqlExecutor.ExecuteInternal(ctx, "commit"); err != nil {
		return err
	}

	userList := make([]string, 0, len(s.UserToUsers)*2)
	for _, users := range s.UserToUsers {
		userList = append(userList, users.OldUser.Username)
		userList = append(userList, users.NewUser.Username)
	}
	return domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege(userList)
}

func renameUserHostInSystemTable(sqlExecutor sqlexec.SQLExecutor, tableName, usernameColumn, hostColumn string, users *ast.UserToUser) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, `UPDATE %n.%n SET %n = %?, %n = %? WHERE %n = %? and %n = %?;`,
		mysql.SystemDB, tableName,
		usernameColumn, users.NewUser.Username, hostColumn, strings.ToLower(users.NewUser.Hostname),
		usernameColumn, users.OldUser.Username, hostColumn, strings.ToLower(users.OldUser.Hostname))
	_, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	return err
}

func (e *SimpleExec) executeDropQueryWatch(s *ast.DropQueryWatchStmt) error {
	return querywatch.ExecDropQueryWatch(e.Ctx(), s)
}

func (e *SimpleExec) executeDropUser(ctx context.Context, s *ast.DropUserStmt) error {
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	// Check privileges.
	// Check `CREATE USER` privilege.
	checker := privilege.GetPrivilegeManager(e.Ctx())
	if checker == nil {
		return errors.New("miss privilege checker")
	}
	activeRoles := e.Ctx().GetSessionVars().ActiveRoles
	if !checker.RequestVerification(activeRoles, mysql.SystemDB, mysql.UserTable, "", mysql.DeletePriv) {
		if s.IsDropRole {
			if !checker.RequestVerification(activeRoles, "", "", "", mysql.DropRolePriv) &&
				!checker.RequestVerification(activeRoles, "", "", "", mysql.CreateUserPriv) {
				return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("DROP ROLE or CREATE USER")
			}
		}
		if !s.IsDropRole && !checker.RequestVerification(activeRoles, "", "", "", mysql.CreateUserPriv) {
			return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
		}
	}
	hasSystemUserPriv := checker.RequestDynamicVerification(activeRoles, "SYSTEM_USER", false)
	hasRestrictedUserPriv := checker.RequestDynamicVerification(activeRoles, "RESTRICTED_USER_ADMIN", false)
	failedUsers := make([]string, 0, len(s.UserList))
	sysSession, err := e.GetSysSession()
	defer e.ReleaseSysSession(internalCtx, sysSession)
	if err != nil {
		return err
	}
	sqlExecutor := sysSession.GetSQLExecutor()

	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "begin"); err != nil {
		return err
	}

	sql := new(strings.Builder)
	for _, user := range s.UserList {
		exists, err := userExists(ctx, e.Ctx(), user.Username, user.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			if !s.IfExists {
				failedUsers = append(failedUsers, user.String())
				break
			}
			e.Ctx().GetSessionVars().StmtCtx.AppendNote(infoschema.ErrUserDropExists.FastGenByArgs(user))
		}

		// Certain users require additional privileges in order to be modified.
		// If this is the case, we need to rollback all changes and return a privilege error.
		// Because in TiDB SUPER can be used as a substitute for any dynamic privilege, this effectively means that
		// any user with SUPER requires a user with SUPER to be able to DROP the user.
		// We also allow RESTRICTED_USER_ADMIN to count for simplicity.
		if !(hasSystemUserPriv || hasRestrictedUserPriv) && checker.RequestDynamicVerificationWithUser(ctx, "SYSTEM_USER", false, user) {
			if _, err := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); err != nil {
				return err
			}
			return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SYSTEM_USER or SUPER")
		}

		// begin a transaction to delete a user.
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, mysql.UserTable, strings.ToLower(user.Hostname), user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete password history from mysql.password_history.
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, mysql.PasswordHistoryTable, strings.ToLower(user.Hostname), user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete privileges from mysql.global_priv
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, mysql.GlobalPrivTable, user.Hostname, user.Username)
		if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			if _, err := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); err != nil {
				return err
			}
			continue
		}

		// delete privileges from mysql.db
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, mysql.DBTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete privileges from mysql.tables_priv
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, mysql.TablePrivTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete privileges from mysql.columns_priv
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, mysql.ColumnPrivTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete relationship from mysql.role_edges
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE TO_HOST = %? and TO_USER = %?;`, mysql.SystemDB, mysql.RoleEdgeTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE FROM_HOST = %? and FROM_USER = %?;`, mysql.SystemDB, mysql.RoleEdgeTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete relationship from mysql.default_roles
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE DEFAULT_ROLE_HOST = %? and DEFAULT_ROLE_USER = %?;`, mysql.SystemDB, mysql.DefaultRoleTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE HOST = %? and USER = %?;`, mysql.SystemDB, mysql.DefaultRoleTable, user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete relationship from mysql.global_grants
		sql.Reset()
		sqlescape.MustFormatSQL(sql, `DELETE FROM %n.%n WHERE Host = %? and User = %?;`, mysql.SystemDB, "global_grants", user.Hostname, user.Username)
		if _, err = sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete from activeRoles
		if s.IsDropRole {
			for i := range activeRoles {
				if activeRoles[i].Username == user.Username && activeRoles[i].Hostname == user.Hostname {
					activeRoles = slices.Delete(activeRoles, i, i+1)
					break
				}
			}
		} // TODO: need delete columns_priv once we implement columns_priv functionality.
	}

	if len(failedUsers) != 0 {
		if _, err := sqlExecutor.ExecuteInternal(internalCtx, "rollback"); err != nil {
			return err
		}
		if s.IsDropRole {
			return exeerrors.ErrCannotUser.GenWithStackByArgs("DROP ROLE", strings.Join(failedUsers, ","))
		}
		return exeerrors.ErrCannotUser.GenWithStackByArgs("DROP USER", strings.Join(failedUsers, ","))
	}
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "commit"); err != nil {
		return err
	}
	if s.IsDropRole {
		// apply new activeRoles
		if ok, roleName := checker.ActiveRoles(ctx, e.Ctx(), activeRoles); !ok {
			u := e.Ctx().GetSessionVars().User
			return exeerrors.ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
		}
	}
	userList := userIdentityToUserList(s.UserList)
	return domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege(userList)
}

func userExists(ctx context.Context, sctx sessionctx.Context, name string, host string) (bool, error) {
	exec := sctx.GetRestrictedSQLExecutor()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnPrivilege)
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, `SELECT * FROM %n.%n WHERE User=%? AND Host=%?;`, mysql.SystemDB, mysql.UserTable, name, strings.ToLower(host))
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}

// use the same internal executor to read within the same transaction, otherwise same as userExists
func userExistsInternal(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, name string, host string) (bool, string, error) {
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, `SELECT * FROM %n.%n WHERE User=%? AND Host=%? FOR UPDATE;`, mysql.SystemDB, mysql.UserTable, name, strings.ToLower(host))
	recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	if err != nil {
		return false, "", err
	}
	req := recordSet.NewChunk(nil)
	err = recordSet.Next(ctx, req)
	var rows = 0
	if err == nil {
		rows = req.NumRows()
	}

	var authPlugin string
	colIdx := -1
	for i, f := range recordSet.Fields() {
		if f.ColumnAsName.L == "plugin" {
			colIdx = i
		}
	}
	if rows == 1 {
		// rows can only be 0 or 1
		// When user + host does not exist, the rows is 0
		// When user + host exists, the rows is 1 because user + host is primary key of the table.
		row := req.GetRow(0)
		authPlugin = row.GetString(colIdx)
	}

	errClose := recordSet.Close()
	if errClose != nil {
		return false, "", errClose
	}
	return rows > 0, authPlugin, err
}

func (e *SimpleExec) executeSetPwd(ctx context.Context, s *ast.SetPwdStmt) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnPrivilege)
	sysSession, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(ctx, sysSession)

	sqlExecutor := sysSession.GetSQLExecutor()
	// session isolation level changed to READ-COMMITTED.
	// When tidb is at the RR isolation level, executing `begin` will obtain a consistent state.
	// When operating the same user concurrently, it may happen that historical versions are read.
	// In order to avoid this risk, change the isolation level to RC.
	_, err = sqlExecutor.ExecuteInternal(ctx, "set tx_isolation = 'READ-COMMITTED'")
	if err != nil {
		return err
	}
	if _, err := sqlExecutor.ExecuteInternal(ctx, "BEGIN PESSIMISTIC"); err != nil {
		return err
	}

	var u, h string
	disableSandboxMode := false
	if s.User == nil || s.User.CurrentUser {
		if e.Ctx().GetSessionVars().User == nil {
			return errors.New("Session error is empty")
		}
		u = e.Ctx().GetSessionVars().User.AuthUsername
		h = e.Ctx().GetSessionVars().User.AuthHostname
	} else {
		u = s.User.Username
		h = s.User.Hostname

		checker := privilege.GetPrivilegeManager(e.Ctx())
		activeRoles := e.Ctx().GetSessionVars().ActiveRoles
		if checker != nil && !checker.RequestVerification(activeRoles, "", "", "", mysql.SuperPriv) {
			currUser := e.Ctx().GetSessionVars().User
			return exeerrors.ErrDBaccessDenied.GenWithStackByArgs(currUser.Username, currUser.Hostname, "mysql")
		}
	}
	exists, authplugin, err := userExistsInternal(ctx, sqlExecutor, u, h)
	if err != nil {
		return err
	}
	if !exists {
		return errors.Trace(exeerrors.ErrPasswordNoMatch)
	}
	if e.Ctx().InSandBoxMode() {
		if !(s.User == nil || s.User.CurrentUser ||
			e.Ctx().GetSessionVars().User.AuthUsername == u && e.Ctx().GetSessionVars().User.AuthHostname == strings.ToLower(h)) {
			return exeerrors.ErrMustChangePassword.GenWithStackByArgs()
		}
		disableSandboxMode = true
	}

	if e.isValidatePasswordEnabled() {
		if err := pwdValidator.ValidatePassword(e.Ctx().GetSessionVars(), s.Password); err != nil {
			return err
		}
	}
	extensions, err := extension.GetExtensions()
	if err != nil {
		return exeerrors.ErrPluginIsNotLoaded.GenWithStackByArgs(err.Error())
	}
	authPlugins := extensions.GetAuthPlugins()
	var pwd string
	switch authplugin {
	case mysql.AuthCachingSha2Password, mysql.AuthTiDBSM3Password:
		pwd = auth.NewHashPassword(s.Password, authplugin)
	case mysql.AuthSocket:
		e.Ctx().GetSessionVars().StmtCtx.AppendNote(exeerrors.ErrSetPasswordAuthPlugin.FastGenByArgs(u, h))
		pwd = ""
	default:
		if pluginImpl, ok := authPlugins[authplugin]; ok {
			if pwd, ok = pluginImpl.GenerateAuthString(s.Password); !ok {
				return exeerrors.ErrPasswordFormat.GenWithStackByArgs()
			}
		} else {
			pwd = auth.EncodePassword(s.Password)
		}
	}

	// for Support Password Reuse Policy.
	plOptions := &passwordOrLockOptionsInfo{
		lockAccount:                 "",
		passwordHistory:             notSpecified,
		passwordReuseInterval:       notSpecified,
		passwordHistoryChange:       false,
		passwordReuseIntervalChange: false,
	}
	// The empty password does not count in the password history and is subject to reuse at any time.
	// https://dev.mysql.com/doc/refman/8.0/en/password-management.html#password-reuse-policy
	if len(pwd) != 0 {
		userDetail := &userInfo{
			host:       h,
			user:       u,
			pLI:        plOptions,
			pwd:        pwd,
			authString: s.Password,
		}
		err := checkPasswordReusePolicy(ctx, sqlExecutor, userDetail, e.Ctx(), authplugin, authPlugins)
		if err != nil {
			return err
		}
	}
	// update mysql.user
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, `UPDATE %n.%n SET authentication_string=%?,password_expired='N',password_last_changed=current_timestamp() WHERE User=%? AND Host=%?;`, mysql.SystemDB, mysql.UserTable, pwd, u, strings.ToLower(h))
	_, err = sqlExecutor.ExecuteInternal(ctx, sql.String())
	if err != nil {
		return err
	}
	if _, err := sqlExecutor.ExecuteInternal(ctx, "commit"); err != nil {
		return err
	}
	err = domain.GetDomain(e.Ctx()).NotifyUpdatePrivilege([]string{u})
	if err != nil {
		return err
	}
	if disableSandboxMode {
		e.Ctx().DisableSandBoxMode()
	}
	return nil
}
