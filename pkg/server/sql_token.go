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

package server

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionauth"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
)

var sqlTokenServerMu sync.Mutex

func (s *Server) initSQLTokenRuntime() error {
	cfg := s.cfg.Security.SQLToken
	if !cfg.Enabled {
		return nil
	}
	if s.GetTLSConfig() == nil || s.cfg.Security.SkipGrantTable || privileges.SkipWithGrant {
		return errors.New("SQL token authentication requires TLS and native privilege checks")
	}
	driver, ok := s.driver.(*TiDBDriver)
	if !ok {
		return errors.New("SQL token authentication requires an attested TiDB storage driver")
	}
	target, err := privileges.SQLTokenTargetForStore(driver.store, cfg.Audience)
	if err != nil {
		return errors.New("invalid SQL token storage target")
	}
	sqlTokenServerMu.Lock()
	defer sqlTokenServerMu.Unlock()
	if privileges.CurrentSQLTokenRuntime() != nil {
		return errors.New("only one SQL token receiver may run in a TiDB process")
	}
	runtime, err := privileges.NewSQLTokenRuntime(privileges.SQLTokenRuntimeOptions{
		Target: target, AuthorityKeyPath: cfg.AuthorityKey, PolicyPath: cfg.PolicyFile, StateDir: cfg.StateDir,
	})
	if err != nil {
		return err
	}
	s.sqlTokenRuntime = runtime
	privileges.SetSQLTokenRuntime(runtime)
	return nil
}

func (s *Server) closeSQLTokenRuntime() {
	// Close admission before removing global managed-account classification.
	// Accepted handshakes and existing CHANGE USER commands consult this barrier.
	if s.sqlTokenRuntime != nil {
		s.sqlTokenClosed.Store(true)
	}
	sqlTokenServerMu.Lock()
	runtime := s.sqlTokenRuntime
	if runtime != nil && privileges.CurrentSQLTokenRuntime() == runtime {
		privileges.SetSQLTokenRuntime(nil)
	}
	sqlTokenServerMu.Unlock()
	if runtime != nil {
		runtime.Close()
	}
}

// StageSQLTokenIdentity implements sessionauth.Connection. The candidate guard
// is not committed until native authentication and database selection succeed.
// The old guard remains active while CHANGE USER is in progress.
func (cc *clientConn) StageSQLTokenIdentity(identity sessionauth.Identity, nativeCheck func() bool) (sessionauth.Authorization, error) {
	runtime := privileges.CurrentSQLTokenRuntime()
	if runtime == nil || runtime != cc.server.sqlTokenRuntime || cc.server.sqlTokenClosed.Load() {
		return nil, privileges.ErrSQLTokenAccessDenied
	}
	guard, err := runtime.Register(identity, nativeCheck, cc.revokeSQLToken)
	if err != nil {
		return nil, err
	}
	cc.sqlToken.Lock()
	defer cc.sqlToken.Unlock()
	if cc.sqlToken.terminal || cc.sqlToken.pending != nil || guard.Check() != nil ||
		(cc.sqlToken.current != nil && cc.sqlToken.current.Check() != nil) {
		guard.Close()
		return nil, privileges.ErrSQLTokenAccessDenied
	}
	cc.sqlToken.pending = guard
	return guard, nil
}

func (cc *clientConn) discardSQLTokenCandidate() {
	cc.sqlToken.Lock()
	guard := cc.sqlToken.pending
	cc.sqlToken.pending = nil
	cc.sqlToken.Unlock()
	if guard != nil {
		guard.Close()
	}
}

func (cc *clientConn) commitSQLTokenAuthentication() error {
	cc.sqlToken.Lock()
	defer cc.sqlToken.Unlock()
	if cc.sqlToken.terminal || cc.server.sqlTokenClosed.Load() || (cc.sqlToken.current != nil && cc.sqlToken.current.Check() != nil) ||
		(cc.sqlToken.pending != nil && cc.sqlToken.pending.Check() != nil) {
		return privileges.ErrSQLTokenAccessDenied
	}
	old := cc.sqlToken.current
	if old != nil || cc.sqlToken.pending != nil {
		if err := cc.server.sqlTokenRuntime.Replace(old, cc.sqlToken.pending); err != nil {
			return err
		}
	}
	cc.sqlToken.current = cc.sqlToken.pending
	cc.sqlToken.pending = nil
	return nil
}

// The engine marks a guard terminal before scheduling this callback. Checking
// the guard pointer under the same lock as authentication replacement prevents
// a delayed callback for an old identity from terminating a replacement session.
func (cc *clientConn) revokeSQLToken(guard *privileges.SQLTokenGuard) {
	cc.sqlToken.Lock()
	if cc.sqlToken.terminal || (guard != cc.sqlToken.current && guard != cc.sqlToken.pending) {
		cc.sqlToken.Unlock()
		return
	}
	cc.sqlToken.terminal = true
	cc.sqlToken.Unlock()
	cc.interruptSQLTokenConnection()
}

// Do not close the TiDB session from the policy worker: dispatch owns executor
// and transaction cleanup. Closing the transport also wakes an idle connection.
func (cc *clientConn) interruptSQLTokenConnection() {
	cc.cancelDispatch()
	if ctx := cc.getCtx(); ctx != nil {
		ctx.GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)
	}
	if cc.bufReadConn != nil {
		_ = cc.bufReadConn.Close()
	}
}

func (cc *clientConn) checkSQLTokenConnection() error {
	cc.sqlToken.Lock()
	defer cc.sqlToken.Unlock()
	if cc.sqlToken.terminal || cc.server.sqlTokenClosed.Load() {
		return privileges.ErrSQLTokenAccessDenied
	}
	if cc.sqlToken.current != nil {
		return cc.sqlToken.current.Check()
	}
	return nil
}

func (cc *clientConn) closeSQLTokenGuards() {
	cc.sqlToken.Lock()
	cc.sqlToken.terminal = true
	current, pending := cc.sqlToken.current, cc.sqlToken.pending
	cc.sqlToken.current, cc.sqlToken.pending = nil, nil
	cc.sqlToken.Unlock()
	if current != nil {
		current.Close()
	}
	if pending != nil {
		pending.Close()
	}
}

type sqlTokenSessionRestorer interface {
	RestoreSQLToken(context.Context, *auth.UserIdentity, sessionauth.Identity, sessionauth.Authorization) error
}
