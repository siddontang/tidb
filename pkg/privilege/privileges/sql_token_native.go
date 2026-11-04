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
	"crypto/sha256"
	"encoding/json"
	"slices"

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// An epoch object is replaced on every observed authorization change, including
// a change reverted before the next guard poll. Its digest is never logged.
type sqlTokenNativeEpoch struct {
	fingerprint [sha256.Size]byte
	valid       bool
}

func (h *Handle) publishSQLTokenNativeSnapshot(snapshot *MySQLPrivilege) {
	h.sqlTokenNativeMu.Lock()
	defer h.sqlTokenNativeMu.Unlock()
	h.priv.Store(snapshot)
	h.refreshSQLTokenNativeEpochsLocked(snapshot)
}

func (h *Handle) refreshSQLTokenNativeEpochsLocked(snapshot *MySQLPrivilege) {
	for key, epoch := range h.sqlTokenNativeEpochs {
		fingerprint, valid := sqlTokenNativeFingerprint(snapshot, key)
		if fingerprint != epoch.fingerprint || valid != epoch.valid {
			h.sqlTokenNativeEpochs[key] = &sqlTokenNativeEpoch{fingerprint: fingerprint, valid: valid}
		}
	}
}

func (h *Handle) sqlTokenNativeCheck(user, host string) func() bool {
	return h.sqlTokenNativeCheckForSnapshot(user, host, nil)
}

// The authentication path supplies the snapshot whose TLS/account facts it
// verified. A publication before epoch capture must not silently replace those
// already-checked facts with a new native authorization generation.
func (h *Handle) sqlTokenNativeCheckForSnapshot(user, host string, verified *MySQLPrivilege) func() bool {
	key := auth.RoleIdentity{Username: user, Hostname: host}
	h.sqlTokenNativeMu.Lock()
	if h.sqlTokenNativeEpochs == nil {
		h.sqlTokenNativeEpochs = make(map[auth.RoleIdentity]*sqlTokenNativeEpoch)
	}
	epoch := h.sqlTokenNativeEpochs[key]
	if epoch == nil {
		fingerprint, valid := sqlTokenNativeFingerprint(h.Get(), key)
		epoch = &sqlTokenNativeEpoch{fingerprint: fingerprint, valid: valid}
		h.sqlTokenNativeEpochs[key] = epoch
	}
	matchesVerified := true
	if verified != nil && verified != h.Get() {
		fingerprint, valid := sqlTokenNativeFingerprint(verified, key)
		matchesVerified = valid && epoch.valid && fingerprint == epoch.fingerprint
	}
	h.sqlTokenNativeMu.Unlock()
	return func() bool {
		if SkipWithGrant || !matchesVerified {
			return false
		}
		h.sqlTokenNativeMu.Lock()
		defer h.sqlTokenNativeMu.Unlock()
		if !epoch.valid || h.sqlTokenNativeEpochs[key] != epoch {
			return false
		}
		record := h.Get().connectionVerification(user, host)
		return record != nil && record.User == user && record.Host == host &&
			record.AuthPlugin == mysql.AuthTiDBSQLToken && record.AuthenticationString == "" &&
			!record.AccountLocked && !record.AutoAccountLocked && !record.PasswordExpired
	}
}

// Fingerprinting runs only on registration/cache publication, never SQL dispatch.
// It includes all roles reachable from this exact account, not just defaults:
// SET ROLE may activate any granted role without a new authentication exchange.
// Cache publication owns the epoch lock; no global variable accessor, SQL-token
// engine, session callback, SQL execution or network service is called here.
func sqlTokenNativeFingerprint(snapshot *MySQLPrivilege, account auth.RoleIdentity) ([sha256.Size]byte, bool) {
	if snapshot == nil {
		return [sha256.Size]byte{}, false
	}
	var records []string
	valid := true
	add := func(kind string, value any) {
		encoded, err := json.Marshal(struct {
			Kind  string
			Value any
		}{kind, value})
		if err != nil {
			valid = false
			return
		}
		records = append(records, string(encoded))
	}
	queue := []auth.RoleIdentity{account}
	visited := make(map[auth.RoleIdentity]struct{})
	for len(queue) > 0 {
		key := queue[0]
		queue = queue[1:]
		if _, seen := visited[key]; seen {
			continue
		}
		visited[key] = struct{}{}
		add("identity", key)
		// Include exact existence/security and the account selected for native
		// grant checks. Full account rows conservatively fence bookkeeping and
		// metadata changes too. Competing nonmatching host records are not relevant.
		add("exact-account", snapshot.connectionVerification(key.Username, key.Hostname))
		add("grant-account", snapshot.matchUser(key.Username, key.Hostname))
		matches := func(record baseRecord) bool {
			return record.fullyMatch(key.Username, key.Hostname) || record.match(key.Username, key.Hostname)
		}
		if item, ok := snapshot.db.Get(itemDB{username: key.Username}); ok {
			for _, record := range item.data {
				if matches(record.baseRecord) {
					add("db", record)
				}
			}
		}
		if item, ok := snapshot.tablesPriv.Get(itemTablesPriv{username: key.Username}); ok {
			for _, record := range item.data {
				if matches(record.baseRecord) {
					add("table", record)
				}
			}
		}
		if item, ok := snapshot.columnsPriv.Get(itemColumnsPriv{username: key.Username}); ok {
			for _, record := range item.data {
				if matches(record.baseRecord) {
					add("column", record)
				}
			}
		}
		if item, ok := snapshot.globalPriv.Get(itemGlobalPriv{username: key.Username}); ok {
			for _, record := range item.data {
				if matches(record.baseRecord) {
					add("global", record)
				}
			}
		}
		if item, ok := snapshot.dynamicPriv.Get(itemDynamicPriv{username: key.Username}); ok {
			for _, record := range item.data {
				if matches(record.baseRecord) {
					add("dynamic", record)
				}
			}
		}
		if item, ok := snapshot.defaultRoles.Get(itemDefaultRole{username: key.Username}); ok {
			for _, record := range item.data {
				if record.fullyMatch(key.Username, key.Hostname) {
					add("default-role", record)
				}
			}
		}
		for role := range snapshot.roleGraph[key].roleList {
			add("role-edge", struct{ From, To auth.RoleIdentity }{key, role})
			queue = append(queue, role)
		}
	}
	// SQL load order and map iteration order are not authorization changes.
	slices.Sort(records)
	encoded, err := json.Marshal(records)
	if err != nil || !valid {
		return [sha256.Size]byte{}, false
	}
	return sha256.Sum256(encoded), true
}
