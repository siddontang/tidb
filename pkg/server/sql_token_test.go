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
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestSQLTokenConnectionGeneration(t *testing.T) {
	old, next := &privileges.SQLTokenGuard{}, &privileges.SQLTokenGuard{}
	cc := &clientConn{server: &Server{}}
	var cancelled atomic.Int64
	cc.mu.cancelFunc = func() { cancelled.Add(1) }
	// Runtime Replace has already closed old at its atomic ordering point.
	// A callback queued before the pointer replacement must now be obsolete.
	cc.sqlToken.current = old
	cc.sqlToken.Lock()
	started, finished := make(chan struct{}), make(chan struct{})
	go func() {
		close(started)
		cc.revokeSQLToken(old)
		close(finished)
	}()
	<-started
	cc.sqlToken.current = next
	cc.sqlToken.Unlock()
	<-finished
	require.False(t, cc.sqlToken.terminal)
	require.Zero(t, cancelled.Load())
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() { cc.revokeSQLToken(next) })
	}
	workers.Wait()
	require.True(t, cc.sqlToken.terminal)
	require.EqualValues(t, 1, cancelled.Load())
	require.ErrorIs(t, cc.checkSQLTokenConnection(), privileges.ErrSQLTokenAccessDenied)
	require.ErrorIs(t, cc.commitSQLTokenAuthentication(), privileges.ErrSQLTokenAccessDenied)

	// A pending identity can also terminalize the physical connection. It cannot
	// be discarded to escape a revocation that already claimed this connection.
	candidate := &clientConn{server: &Server{}}
	candidate.sqlToken.pending = next
	candidate.revokeSQLToken(next)
	require.ErrorIs(t, candidate.checkSQLTokenConnection(), privileges.ErrSQLTokenAccessDenied)
}

func TestSQLTokenShutdownAdmissionBarrier(t *testing.T) {
	s := &Server{}
	s.sqlTokenClosed.Store(true)
	cc := &clientConn{server: s}
	// These checks must precede session creation/native plugin lookup, even if
	// global runtime classification is already removed during shutdown.
	require.ErrorIs(t, cc.prepareSessionAndDoAuth(nil, "", 0), privileges.ErrSQLTokenAccessDenied)
	_, err := cc.checkAuthPlugin(context.Background(), nil)
	require.ErrorIs(t, err, privileges.ErrSQLTokenAccessDenied)
	require.ErrorIs(t, cc.dispatch(context.Background(), nil), privileges.ErrSQLTokenAccessDenied)
	require.ErrorIs(t, cc.commitSQLTokenAuthentication(), privileges.ErrSQLTokenAccessDenied)

	// Rejected bootstrap must not leave the automatic certificate renewal
	// worker running for a server that was never returned to its owner.
	t.Cleanup(config.RestoreFunc())
	config.UpdateGlobal(func(cfg *config.Config) { cfg.TempStoragePath = t.TempDir() })
	cfg := config.NewConfig()
	cfg.Security.AutoTLS, cfg.Security.SkipGrantTable = true, true
	cfg.Security.RSAKeySize = 2048
	cfg.Security.SQLToken.Enabled = true
	previousGoroutines := goleak.IgnoreCurrent()
	rejected, err := NewServer(cfg, nil)
	require.Error(t, err)
	require.Nil(t, rejected)
	require.NoError(t, goleak.Find(previousGoroutines))
}
