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
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionauth"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	kvstore "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit/testenv"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

type sqlTokenTestAuthorization struct {
	identity sessionauth.Identity
	err      error
}

func (a *sqlTokenTestAuthorization) Check() error                   { return a.err }
func (a *sqlTokenTestAuthorization) Identity() sessionauth.Identity { return a.identity }

type sqlTokenOtherClusterStore struct{ kv.Storage }

func (s sqlTokenOtherClusterStore) GetClusterID() uint64 { return s.Storage.GetClusterID() + 1 }

func TestSQLTokenSessionGuard(t *testing.T) {
	// A deliberately incomplete session proves rejection precedes statement,
	// transaction and prepared-statement state allocation, including restricted SQL.
	identity := sessionauth.Identity{Profile: mysql.AuthTiDBSQLToken}
	se := &session{sessionVars: variable.NewSessionVars(nil)}
	se.sessionVars.SQLTokenIdentity = &identity
	for _, restricted := range []bool{false, true} {
		se.sessionVars.InRestrictedSQL = restricted
		_, err := se.ExecuteStmt(context.Background(), &ast.SelectStmt{})
		require.ErrorIs(t, err, privileges.ErrSQLTokenAccessDenied)
		_, _, _, err = se.PrepareStmt("select 1")
		require.ErrorIs(t, err, privileges.ErrSQLTokenAccessDenied)
	}
	guard := &sqlTokenTestAuthorization{identity: identity, err: privileges.ErrSQLTokenAccessDenied}
	se.sessionVars.SQLTokenAuthorization = guard
	require.ErrorIs(t, se.checkSQLTokenAccess(), privileges.ErrSQLTokenAccessDenied)
	se.sessionVars.SQLTokenIdentity = nil
	require.ErrorIs(t, se.checkSQLTokenAccess(), privileges.ErrSQLTokenAccessDenied)
	se.sessionVars.SQLTokenAuthorization = nil
	se.sessionVars.User = &auth.UserIdentity{AuthPlugin: mysql.AuthTiDBSQLToken}
	require.ErrorIs(t, se.checkSQLTokenAccess(), privileges.ErrSQLTokenAccessDenied)
	require.False(t, se.AuthWithoutVerification(context.Background(), se.sessionVars.User))
	_, err := se.stageSQLTokenAuthentication(&identity, nil, nil)
	require.ErrorIs(t, err, privileges.ErrSQLTokenAccessDenied)
	require.ErrorIs(t, se.RestoreSQLToken(context.Background(), se.sessionVars.User, identity, nil), privileges.ErrSQLTokenAccessDenied)
}

func createSQLTokenSessionTestStore(t *testing.T) kv.Storage {
	t.Helper()
	if kerneltype.IsClassic() {
		store, dom := CreateStoreAndBootstrap(t)
		t.Cleanup(func() {
			dom.Close()
			require.NoError(t, store.Close())
		})
		return store
	}

	testenv.SetGOMAXPROCSForTest()
	testenv.UpdateConfigForNextgen(t)
	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	t.Cleanup(func() { cluster.Terminate(t) })
	newKeyspaceStore := func(id uint32, name string) kv.Storage {
		store, err := mockstore.NewMockStore(
			mockstore.WithStoreType(mockstore.EmbedUnistore),
			mockstore.WithCurrentKeyspaceMeta(&keyspacepb.KeyspaceMeta{Id: id, Name: name}),
		)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		dom, err := BootstrapSession(store)
		require.NoError(t, err)
		t.Cleanup(dom.Close)
		return store
	}

	// A customer keyspace requires a fully bootstrapped SYSTEM store. Keep its
	// storage and domain alive until all customer sessions and storage close.
	systemStore := newKeyspaceStore(0xFFFFFF-1, keyspace.System)
	originalSystemStore := kvstore.GetSystemStorage()
	kvstore.SetSystemStorage(systemStore)
	t.Cleanup(func() { kvstore.SetSystemStorage(originalSystemStore) })
	_, err := privileges.SQLTokenTargetForStore(systemStore, "urn:example:database:session-test")
	require.ErrorIs(t, err, privileges.ErrSQLTokenAccessDenied)

	// Customer-domain startup reads SYSTEM metadata through the cross-keyspace
	// manager. Share the owned store and give each manager a fresh namespaced
	// etcd client, which the manager closes before fixture cleanup.
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/beforeGetStore",
		func(fn *func(string) (kv.Storage, error)) {
			*fn = func(name string) (kv.Storage, error) {
				if name != keyspace.System {
					return nil, fmt.Errorf("unexpected cross-keyspace fixture target %q", name)
				}
				return systemStore, nil
			}
		})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/injectETCDCli",
		func(cli **clientv3.Client, name string) {
			require.Equal(t, keyspace.System, name)
			client, err := clientv3.New(clientv3.Config{
				Endpoints: cluster.Client(0).Endpoints(), DialTimeout: 5 * time.Second,
			})
			require.NoError(t, err)
			t.Cleanup(func() { _ = client.Close() })
			etcd.SetEtcdCliByNamespace(client, keyspace.MakeKeyspaceEtcdNamespace(systemStore.GetCodec()))
			*cli = client
		})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/skipCloseStore",
		func(shouldClose *bool) { *shouldClose = false })

	// The real mock codec supplies the customer keyspace ID and name even when
	// test setup's process-global keyspace configuration names SYSTEM.
	return newKeyspaceStore(0xFFFFFF-2, "sql_token_session_test")
}

// executeSQLTokenTestStatement also observes execution errors from lazy results.
func executeSQLTokenTestStatement(t *testing.T, se sessionapi.Session, sql string) error {
	t.Helper()
	ctx := context.Background()
	results, err := se.Execute(ctx, sql)
	for _, result := range results {
		_, readErr := GetRows4Test(ctx, se, result)
		closeErr := result.Close()
		require.NoError(t, closeErr)
		if err == nil {
			err = readErr
		}
	}
	return err
}

func TestSQLTokenNativeAccountBoundaries(t *testing.T) {
	store := createSQLTokenSessionTestStore(t)
	admin := CreateSessionAndSetID(t, store)
	t.Cleanup(admin.Close)
	MustExec(t, admin, "create user 'sql_token_user'@'%' identified with 'tidb_sql_token_v1'")
	MustExec(t, admin, "create user 'sql_token_alter'@'%'")
	MustExec(t, admin, "alter user 'sql_token_alter'@'%' identified with 'tidb_sql_token_v1' by ''")
	for _, sql := range []string{
		"create user 'sql_token_bad'@'%' identified with 'tidb_sql_token_v1' by 'not-a-token'",
		"create user 'sql_token_bad'@'%' identified with 'tidb_sql_token_v1' as 'not-a-token'",
		"alter user 'sql_token_alter'@'%' identified with 'tidb_sql_token_v1' by 'not-a-token'",
		"set password for 'sql_token_alter'@'%' = 'not-a-token'",
	} {
		err := executeSQLTokenTestStatement(t, admin, sql)
		require.Error(t, err, sql)
	}
	se := CreateSessionAndSetID(t, store).(*session)
	t.Cleanup(se.Close)
	user := &auth.UserIdentity{Username: "sql_token_user", Hostname: "localhost", AuthPlugin: mysql.AuthTiDBSQLToken}
	// Native records must fail closed when there is no configured runtime.
	require.Error(t, se.Auth(user, []byte("synthetic-token\x00"), nil, nil))
	require.False(t, se.AuthWithoutVerification(context.Background(), user))
	user.AuthPlugin = mysql.AuthTiDBSessionToken
	require.Error(t, se.Auth(user, []byte("synthetic-migration-token"), nil, nil))
	require.Nil(t, se.sessionVars.SQLTokenIdentity)
	require.Nil(t, se.sessionVars.SQLTokenAuthorization)

	pm := privilege.GetPrivilegeManager(se)
	native := pm.(sqlTokenNativeVerifier)
	nativeCheck := native.SQLTokenNativeCheck("sql_token_user", "%")
	require.True(t, nativeCheck())
	require.False(t, native.SQLTokenNativeCheck("sql_token_user", "localhost")())
	MustExec(t, admin, "alter user 'sql_token_user'@'%' account lock")
	require.False(t, nativeCheck())
	MustExec(t, admin, "alter user 'sql_token_user'@'%' account unlock")
	require.False(t, nativeCheck())
	nativeCheck = native.SQLTokenNativeCheck("sql_token_user", "%")
	require.True(t, nativeCheck())
	MustExec(t, admin, "alter user 'sql_token_user'@'%' identified with 'mysql_native_password' by ''")
	require.False(t, nativeCheck())
	MustExec(t, admin, "alter user 'sql_token_user'@'%' identified with 'tidb_sql_token_v1'")
	require.False(t, nativeCheck())
	nativeCheck = native.SQLTokenNativeCheck("sql_token_user", "%")
	require.True(t, nativeCheck())

	// The guard fixture isolates session/executor boundaries; runtime-backed
	// registration, revocation and wire reset are covered by server tests.
	target, err := privileges.SQLTokenTargetForStore(store, "urn:example:database:session-test")
	require.NoError(t, err)
	identity := sessionauth.Identity{Target: target, Profile: mysql.AuthTiDBSQLToken, NativeUsername: "sql_token_user", NativeHost: "%"}
	guard := &sqlTokenTestAuthorization{identity: identity}
	user.AuthUsername, user.AuthHostname, user.AuthPlugin = "sql_token_user", "%", mysql.AuthTiDBSQLToken
	pm.AuthSuccess(user.AuthUsername, user.AuthHostname)
	se.sessionVars.User = user
	se.sessionVars.SQLTokenTarget = &target
	se.sessionVars.SQLTokenIdentity = &identity
	se.sessionVars.SQLTokenAuthorization = guard
	MustExec(t, se, "select 1")
	for _, sql := range []string{
		"alter user user() identified by 'escape'",
		"set password = 'escape'",
		"show session_states",
	} {
		err := executeSQLTokenTestStatement(t, se, sql)
		require.Error(t, err, sql)
	}
	// Even an account with CREATE USER cannot change its own token auth mode.
	MustExec(t, admin, "grant create user on *.* to 'sql_token_user'@'%'")
	require.False(t, nativeCheck())
	nativeCheck = native.SQLTokenNativeCheck("sql_token_user", "%")
	err = executeSQLTokenTestStatement(t, se, "alter user 'sql_token_user'@'%' identified with 'mysql_native_password' by 'escape'")
	require.ErrorIs(t, err, privileges.ErrSQLTokenAccessDenied)
	require.True(t, nativeCheck())
	state := &sessionstates.SessionStates{}
	require.Error(t, se.EncodeStates(context.Background(), state))
	require.NoError(t, json.Unmarshal([]byte(`{"SQLTokenIdentity":{"NativeUsername":"root"},"SQLTokenAuthorization":{},"sql-token-identity":{"NativeUsername":"root"}}`), state))
	state.ResourceGroupName = se.sessionVars.ResourceGroupName
	require.NoError(t, se.DecodeStates(context.Background(), state))
	require.Same(t, &identity, se.sessionVars.SQLTokenIdentity)
	require.Same(t, guard, se.sessionVars.SQLTokenAuthorization)
	require.Equal(t, "sql_token_user", se.sessionVars.User.AuthUsername)
	require.ErrorIs(t, se.checkSQLTokenStore(sqlTokenOtherClusterStore{store}), privileges.ErrSQLTokenAccessDenied)
	se.store = sqlTokenOtherClusterStore{store}
	_, err = se.ExecuteStmt(context.Background(), &ast.SelectStmt{})
	require.ErrorIs(t, err, privileges.ErrSQLTokenAccessDenied)
	se.store = store
	internal, clean, err := se.getInternalSession(sqlexec.ExecOption{})
	require.NoError(t, err)
	require.Same(t, guard, internal.sessionVars.SQLTokenAuthorization)
	require.Same(t, &identity, internal.sessionVars.SQLTokenIdentity)
	guard.err = privileges.ErrSQLTokenAccessDenied
	_, err = internal.ExecuteStmt(context.Background(), &ast.SelectStmt{})
	require.ErrorIs(t, err, privileges.ErrSQLTokenAccessDenied)
	clean()
	require.Nil(t, internal.sessionVars.SQLTokenAuthorization)
	require.Nil(t, internal.sessionVars.SQLTokenIdentity)
	guard.err = privileges.ErrSQLTokenAccessDenied
	err = executeSQLTokenTestStatement(t, se, "select 1")
	require.ErrorIs(t, err, privileges.ErrSQLTokenAccessDenied)
}
