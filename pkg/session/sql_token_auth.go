// Copyright 2026 PingCAP, Inc.
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

package session

import (
	"context"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/privilege/conn"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionauth"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

type sqlTokenNativeVerifier interface {
	SQLTokenNativeCheck(user, host string) func() bool
	SQLTokenValidateAccount(user, host string, vars *variable.SessionVars) error
}

func (s *session) stageSQLTokenAuthentication(identity *sessionauth.Identity, authConn conn.AuthConn, nativeCheck func() bool) (sessionauth.Authorization, error) {
	if identity == nil {
		return nil, nil
	}
	connection, ok := authConn.(sessionauth.Connection)
	if !ok {
		return nil, privileges.ErrSQLTokenAccessDenied
	}
	if nativeCheck == nil {
		return nil, privileges.ErrSQLTokenAccessDenied
	}
	return connection.StageSQLTokenIdentity(*identity, nativeCheck)
}

// checkSQLTokenAccess also runs for restricted SQL using a token-bound session.
// The connection owns the authorization lifetime; Session.Close must not release
// it because RESET CONNECTION transfers the same guard to a fresh session.
func (s *session) isSQLTokenSession() bool {
	vars := s.sessionVars
	if vars.SQLTokenIdentity != nil || vars.SQLTokenAuthorization != nil {
		return true
	}
	if user := vars.User; user != nil {
		if user.AuthPlugin == mysql.AuthTiDBSQLToken {
			return true
		}
		if runtime := privileges.CurrentSQLTokenRuntime(); runtime != nil {
			return runtime.IsManagedUsername(user.Username) || runtime.IsManagedUsername(user.AuthUsername)
		}
	}
	return false
}

func (s *session) checkSQLTokenAccess() error {
	vars := s.sessionVars
	identity, authorization := vars.SQLTokenIdentity, vars.SQLTokenAuthorization
	if identity == nil {
		if authorization != nil {
			return privileges.ErrSQLTokenAccessDenied
		}
		if user := vars.User; user != nil {
			if user.AuthPlugin == mysql.AuthTiDBSQLToken {
				return privileges.ErrSQLTokenAccessDenied
			}
			if runtime := privileges.CurrentSQLTokenRuntime(); runtime != nil &&
				(runtime.IsManagedUsername(user.Username) || runtime.IsManagedUsername(user.AuthUsername)) {
				return privileges.ErrSQLTokenAccessDenied
			}
		}
		return nil
	}
	if authorization == nil || authorization.Identity() != *identity {
		return privileges.ErrSQLTokenAccessDenied
	}
	if err := authorization.Check(); err != nil {
		return privileges.ErrSQLTokenAccessDenied
	}
	return s.checkSQLTokenStore(s.store)
}

func (s *session) checkSQLTokenStore(store kv.Storage) error {
	identity := s.sessionVars.SQLTokenIdentity
	if identity == nil {
		return nil
	}
	target, err := privileges.SQLTokenTargetForStore(store, identity.Target.Audience)
	if err != nil || target != identity.Target {
		return privileges.ErrSQLTokenAccessDenied
	}
	return nil
}

// RestoreSQLToken preserves the original guard and deadlines across a connection
// reset. It deliberately does not match the client host again: a newly created
// competing host record must not change the native grant identity on reset.
func (s *session) RestoreSQLToken(ctx context.Context, user *auth.UserIdentity, identity sessionauth.Identity, authorization sessionauth.Authorization) error {
	if user == nil || authorization == nil || authorization.Identity() != identity ||
		identity.Profile != mysql.AuthTiDBSQLToken || user.AuthUsername != identity.NativeUsername ||
		user.AuthHostname != identity.NativeHost {
		return privileges.ErrSQLTokenAccessDenied
	}
	if err := authorization.Check(); err != nil {
		return privileges.ErrSQLTokenAccessDenied
	}
	runtime := privileges.CurrentSQLTokenRuntime()
	if runtime == nil || runtime.Target() != identity.Target {
		return privileges.ErrSQLTokenAccessDenied
	}
	target, err := privileges.SQLTokenTargetForStore(s.store, runtime.Target().Audience)
	if err != nil || target != identity.Target {
		return privileges.ErrSQLTokenAccessDenied
	}
	pm := privilege.GetPrivilegeManager(s)
	native, ok := pm.(sqlTokenNativeVerifier)
	if !ok || native.SQLTokenValidateAccount(identity.NativeUsername, identity.NativeHost, s.sessionVars) != nil {
		return privileges.ErrSQLTokenAccessDenied
	}
	roles := pm.GetDefaultRoles(ctx, identity.NativeUsername, identity.NativeHost)
	if !native.SQLTokenNativeCheck(identity.NativeUsername, identity.NativeHost)() || authorization.Check() != nil {
		return privileges.ErrSQLTokenAccessDenied
	}
	userCopy := *user
	pm.AuthSuccess(identity.NativeUsername, identity.NativeHost)
	s.sessionVars.User = &userCopy
	s.sessionVars.ActiveRoles = roles
	s.sessionVars.SQLTokenTarget = &target
	s.sessionVars.SQLTokenIdentity = &identity
	s.sessionVars.SQLTokenAuthorization = authorization
	return nil
}
