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

package privileges

import (
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/stretchr/testify/require"
)

func sqlTokenNativeTestBase(user, host string) baseRecord {
	chars, types := stringutil.CompilePatternBinary(host, '\\')
	return baseRecord{User: user, Host: host, patChars: chars, patTypes: types}
}

func sqlTokenNativeTestSnapshot() *MySQLPrivilege {
	p := newMySQLPrivilege()
	for _, name := range []string{"managed", "role1", "role2", "other"} {
		row := UserRecord{baseRecord: sqlTokenNativeTestBase(name, "%"), AuthPlugin: mysql.AuthTiDBSQLToken}
		p.user.ReplaceOrInsert(itemUser{username: name, data: []UserRecord{row}})
	}
	managed, _ := p.user.Get(itemUser{username: "managed"})
	otherHost := UserRecord{baseRecord: sqlTokenNativeTestBase("managed", "localhost"), AuthPlugin: mysql.AuthNativePassword}
	managed.data = append([]UserRecord{otherHost}, managed.data...)
	p.user.ReplaceOrInsert(managed)
	for _, name := range []string{"managed", "role2"} {
		base := sqlTokenNativeTestBase(name, "%")
		p.db.ReplaceOrInsert(itemDB{username: name, data: []dbRecord{{baseRecord: base, DB: "app", Privileges: mysql.SelectPriv}}})
		p.tablesPriv.ReplaceOrInsert(itemTablesPriv{username: name, data: []tablesPrivRecord{{baseRecord: base, DB: "app", TableName: "items", TablePriv: mysql.SelectPriv}}})
		p.columnsPriv.ReplaceOrInsert(itemColumnsPriv{username: name, data: []columnsPrivRecord{{baseRecord: base, DB: "app", TableName: "items", ColumnName: "id", ColumnPriv: mysql.SelectPriv}}})
		p.dynamicPriv.ReplaceOrInsert(itemDynamicPriv{username: name, data: []dynamicPrivRecord{{baseRecord: base, PrivilegeName: "BACKUP_ADMIN"}}})
		p.globalPriv.ReplaceOrInsert(itemGlobalPriv{username: name, data: []globalPrivRecord{{baseRecord: base, Priv: GlobalPrivValue{SSLType: SslTypeAny}}}})
	}
	key := auth.RoleIdentity{Username: "managed", Hostname: "%"}
	role1 := auth.RoleIdentity{Username: "role1", Hostname: "%"}
	role2 := auth.RoleIdentity{Username: "role2", Hostname: "%"}
	p.roleGraph = map[auth.RoleIdentity]roleGraphEdgesTable{
		key:   {roleList: map[auth.RoleIdentity]*auth.RoleIdentity{role1: &role1}},
		role1: {roleList: map[auth.RoleIdentity]*auth.RoleIdentity{role2: &role2}},
	}
	p.defaultRoles.ReplaceOrInsert(itemDefaultRole{username: "managed", data: []defaultRoleRecord{{baseRecord: sqlTokenNativeTestBase("managed", "%"), DefaultRoleUser: "role1", DefaultRoleHost: "%"}}})
	return p
}

func TestSQLTokenNativeEpoch(t *testing.T) {
	mutations := map[string]func(*MySQLPrivilege){
		"plugin": func(p *MySQLPrivilege) {
			p.connectionVerification("managed", "%").AuthPlugin = mysql.AuthNativePassword
		},
		"password": func(p *MySQLPrivilege) {
			p.connectionVerification("managed", "%").AuthenticationString = "synthetic-hash"
		},
		"lock":             func(p *MySQLPrivilege) { p.connectionVerification("managed", "%").AccountLocked = true },
		"auto lock":        func(p *MySQLPrivilege) { p.connectionVerification("managed", "%").AutoAccountLocked = true },
		"password expired": func(p *MySQLPrivilege) { p.connectionVerification("managed", "%").PasswordExpired = true },
		"user grants":      func(p *MySQLPrivilege) { p.connectionVerification("managed", "%").Privileges = mysql.InsertPriv },
		"removed account":  func(p *MySQLPrivilege) { p.user.Delete(itemUser{username: "managed"}) },
		"db grants": func(p *MySQLPrivilege) {
			item, _ := p.db.Get(itemDB{username: "managed"})
			item.data[0].Privileges = mysql.InsertPriv
		},
		"table grants": func(p *MySQLPrivilege) {
			item, _ := p.tablesPriv.Get(itemTablesPriv{username: "managed"})
			item.data[0].TablePriv = mysql.InsertPriv
		},
		"column grants": func(p *MySQLPrivilege) {
			item, _ := p.columnsPriv.Get(itemColumnsPriv{username: "managed"})
			item.data[0].ColumnPriv = mysql.InsertPriv
		},
		"dynamic grants": func(p *MySQLPrivilege) {
			item, _ := p.dynamicPriv.Get(itemDynamicPriv{username: "managed"})
			item.data[0].GrantOption = true
		},
		"TLS requirements": func(p *MySQLPrivilege) {
			item, _ := p.globalPriv.Get(itemGlobalPriv{username: "managed"})
			item.data[0].Priv.SSLType = SslTypeX509
		},
		"default role":           func(p *MySQLPrivilege) { p.defaultRoles.Delete(itemDefaultRole{username: "managed"}) },
		"role edge":              func(p *MySQLPrivilege) { delete(p.roleGraph, auth.RoleIdentity{Username: "managed", Hostname: "%"}) },
		"transitive role grants": func(p *MySQLPrivilege) { p.connectionVerification("role2", "%").Privileges = mysql.InsertPriv },
		"transitive role db grants": func(p *MySQLPrivilege) {
			item, _ := p.db.Get(itemDB{username: "role2"})
			item.data[0].Privileges = mysql.InsertPriv
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			h := NewHandle(nil, nil)
			h.publishSQLTokenNativeSnapshot(sqlTokenNativeTestSnapshot())
			check := h.sqlTokenNativeCheck("managed", "%")
			require.True(t, check())
			changed := sqlTokenNativeTestSnapshot()
			mutate(changed)
			h.publishSQLTokenNativeSnapshot(changed)
			// Revert before checking: publication itself must preserve the change.
			h.publishSQLTokenNativeSnapshot(sqlTokenNativeTestSnapshot())
			require.False(t, check())
			require.True(t, h.sqlTokenNativeCheck("managed", "%")())
		})
	}

	h := NewHandle(nil, nil)
	h.publishSQLTokenNativeSnapshot(sqlTokenNativeTestSnapshot())
	check := h.sqlTokenNativeCheck("managed", "%")
	h.publishSQLTokenNativeSnapshot(sqlTokenNativeTestSnapshot())
	require.True(t, check(), "an identical refresh is not native drift")
	unrelated := sqlTokenNativeTestSnapshot()
	unrelated.connectionVerification("other", "%").Privileges = mysql.AllPrivMask
	unrelated.connectionVerification("managed", "localhost").Privileges = mysql.AllPrivMask
	h.publishSQLTokenNativeSnapshot(unrelated)
	require.True(t, check(), "unrelated users and competing nonmatching hosts do not change the original grants")
	verified := h.Get()
	newTLS := sqlTokenNativeTestSnapshot()
	ssl, _ := newTLS.globalPriv.Get(itemGlobalPriv{username: "managed"})
	ssl.data[0].Priv.SSLType = SslTypeX509
	h.publishSQLTokenNativeSnapshot(newTLS)
	require.False(t, h.sqlTokenNativeCheckForSnapshot("managed", "%", verified)(), "epoch capture cannot replace TLS facts already checked against an older snapshot")
	require.True(t, h.sqlTokenNativeCheck("managed", "%")())
	h.publishSQLTokenNativeSnapshot(sqlTokenNativeTestSnapshot())
	check = h.sqlTokenNativeCheck("managed", "%")
	changed := sqlTokenNativeTestSnapshot()
	changed.connectionVerification("managed", "%").Privileges = mysql.SelectPriv
	h.merge(changed, map[string]struct{}{"managed": {}})
	h.merge(sqlTokenNativeTestSnapshot(), map[string]struct{}{"managed": {}})
	require.False(t, check(), "incremental cache publication also fences reverted changes")
}

func TestSQLTokenNativeEpochConcurrentPublication(t *testing.T) {
	h := NewHandle(nil, nil)
	h.publishSQLTokenNativeSnapshot(sqlTokenNativeTestSnapshot())
	check := h.sqlTokenNativeCheck("managed", "%")
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 25 {
			changed := sqlTokenNativeTestSnapshot()
			changed.connectionVerification("managed", "%").AccountLocked = true
			h.publishSQLTokenNativeSnapshot(changed)
			h.publishSQLTokenNativeSnapshot(sqlTokenNativeTestSnapshot())
		}
	}()
	go func() {
		defer wg.Done()
		for range 100 {
			check()
		}
	}()
	wg.Wait()
	require.False(t, check())
}
