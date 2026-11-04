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
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionauth"
	"github.com/stretchr/testify/require"
)

type sqlRuntimeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *sqlRuntimeClock) read() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *sqlRuntimeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type sqlRuntimeFixture struct {
	token     sqlTokenFixture
	doc       SQLTokenPolicyDocument
	authority ed25519.PrivateKey
	opts      SQLTokenRuntimeOptions
	clock     *sqlRuntimeClock
	start     time.Time
	runtime   *SQLTokenRuntime
}

func newSQLRuntimeFixture(t *testing.T) *sqlRuntimeFixture {
	t.Helper()
	f := &sqlRuntimeFixture{token: newSQLTokenFixture()}
	root := t.TempDir()
	f.start = time.Now()
	f.clock = &sqlRuntimeClock{now: f.start}
	// Deliberately public synthetic test authority; never read an operator key.
	f.authority = ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	der, err := x509.MarshalPKIXPublicKey(f.authority.Public())
	require.NoError(t, err)
	authorityPath := filepath.Join(root, "authority.pem")
	require.NoError(t, os.WriteFile(authorityPath, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}), 0600))
	f.opts = SQLTokenRuntimeOptions{Target: f.token.request.Target, AuthorityKeyPath: authorityPath, PolicyPath: filepath.Join(root, "response.json"), StateDir: filepath.Join(root, "state")}
	key, err := x509.MarshalPKIXPublicKey(pubKeys[0])
	require.NoError(t, err)
	f.doc = SQLTokenPolicyDocument{
		Target: sqlTargetDocument(f.opts.Target), Issuer: f.token.policy.Issuer, Revision: "7", ValidFrom: f.token.now.Add(-time.Minute).Unix(), ValidUntil: f.token.now.Add(time.Hour).Unix(), MaxTokenLifetimeSeconds: 300, MaxSessionDurationSeconds: 3600,
		Keys:     map[string]string{"test-key": base64.RawURLEncoding.EncodeToString(key)},
		Bindings: map[string]SQLTokenBindingDocument{"binding-test": {PrincipalID: "principal-test", NativeUsername: "sql_user", NativeHost: "%", BindingRevision: "3", PolicyRevision: "9", Enabled: true, ExpiresAt: f.token.now.Add(2 * time.Hour).Unix(), DesiredGrantRevision: "4", AppliedGrantRevision: "4"}},
	}
	f.restart(t)
	return f
}

func (f *sqlRuntimeFixture) restart(t *testing.T) {
	t.Helper()
	if f.runtime != nil {
		f.runtime.Close()
	}
	r, err := newSQLTokenRuntime(f.opts, f.clock.read)
	require.NoError(t, err)
	r.workers.Add(1)
	go r.callbackLoop()
	f.runtime = r
	t.Cleanup(r.Close)
}

func (f *sqlRuntimeFixture) challenge(t *testing.T) SQLTokenPolicyChallenge {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(f.opts.StateDir, "challenge.json"))
	require.NoError(t, err)
	var c SQLTokenPolicyChallenge
	require.NoError(t, json.Unmarshal(raw, &c))
	return c
}

func (f *sqlRuntimeFixture) response(t *testing.T) SQLTokenPolicyResponse {
	t.Helper()
	c := f.challenge(t)
	issued := f.token.now.Add(f.clock.read().Sub(f.start)).Unix()
	return SQLTokenPolicyResponse{Version: 1, Incarnation: c.Incarnation, Nonce: c.Nonce, Sequence: c.Sequence, Target: c.Target, IssuedAt: issued, ValidUntil: issued + 30, Policy: f.doc}
}

func (f *sqlRuntimeFixture) writePayload(t *testing.T, payload []byte) {
	t.Helper()
	signature := ed25519.Sign(f.authority, append([]byte(sqlPolicyDomain), payload...))
	envelope := SQLTokenPolicyEnvelope{Payload: base64.RawURLEncoding.EncodeToString(payload), Signature: base64.RawURLEncoding.EncodeToString(signature)}
	require.NoError(t, os.WriteFile(f.opts.PolicyPath, sqlJSON(t, envelope), 0600))
}

func (f *sqlRuntimeFixture) publish(t *testing.T, response SQLTokenPolicyResponse) {
	t.Helper()
	f.writePayload(t, sqlJSON(t, response))
	f.runtime.poll()
}

func (f *sqlRuntimeFixture) login(t *testing.T, onRevoke func(*SQLTokenGuard)) *SQLTokenGuard {
	t.Helper()
	identity, err := f.runtime.Authenticate(f.token.token(t), f.token.request)
	require.NoError(t, err)
	guard, err := f.runtime.Register(identity, func() bool { return true }, onRevoke)
	require.NoError(t, err)
	return guard
}

func (f *sqlRuntimeFixture) status(t *testing.T) SQLTokenRuntimeStatus {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(f.opts.StateDir, "status.json"))
	require.NoError(t, err)
	var status SQLTokenRuntimeStatus
	require.NoError(t, json.Unmarshal(raw, &status))
	return status
}

func TestSQLTokenRuntimeAdmission(t *testing.T) {
	f := newSQLRuntimeFixture(t)
	identity, err := f.runtime.Authenticate(f.token.token(t), f.token.request)
	require.ErrorIs(t, err, ErrSQLTokenPolicyUnavailable)
	require.Equal(t, sessionauth.Identity{}, identity)
	require.False(t, f.status(t).Ready)
	response := f.response(t)
	f.publish(t, response)
	require.True(t, f.status(t).Ready)
	require.Equal(t, response.Nonce, f.status(t).AcceptedNonce)
	require.Equal(t, response.Sequence, f.status(t).AcceptedSequence)
	require.Equal(t, response.Incarnation, f.status(t).Incarnation)
	require.Equal(t, response.Target, f.status(t).Target)
	require.Equal(t, sqlPolicyHash(sqlJSON(t, f.doc)), f.status(t).PolicyDigest)
	require.NotEqual(t, response.Nonce, f.challenge(t).Nonce)
	require.True(t, f.runtime.IsManagedUsername("sql_user"))
	require.False(t, f.runtime.IsManagedUsername("root"))
	identity, err = f.runtime.Authenticate(f.token.token(t), f.token.request)
	require.NoError(t, err)
	altered := identity
	altered.SessionDeadline = altered.SessionDeadline.Add(time.Hour)
	_, err = f.runtime.Register(altered, func() bool { return true }, nil)
	require.ErrorIs(t, err, ErrSQLTokenAccessDenied)
	guard, err := f.runtime.Register(identity, func() bool { return true }, nil)
	require.NoError(t, err)
	require.Equal(t, identity, guard.Identity())
	require.NoError(t, guard.Check())
	_, err = f.runtime.Register(identity, func() bool { return true }, nil)
	require.ErrorIs(t, err, ErrSQLTokenAccessDenied)
	guard.Close()
	require.ErrorIs(t, guard.Check(), ErrSQLTokenAccessDenied)
	for _, name := range []string{"challenge.json", "status.json", "watermark.json", "runtime.lock"} {
		info, err := os.Stat(filepath.Join(f.opts.StateDir, name))
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	}
	info, err := os.Stat(f.opts.StateDir)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0700), info.Mode().Perm())
}

func TestSQLTokenRuntimeRejectsResponse(t *testing.T) {
	tests := []struct {
		name   string
		change func(*SQLTokenPolicyResponse)
	}{
		{"wrong incarnation", func(r *SQLTokenPolicyResponse) { r.Incarnation = "other" }},
		{"wrong nonce", func(r *SQLTokenPolicyResponse) { r.Nonce = "other" }},
		{"wrong sequence", func(r *SQLTokenPolicyResponse) { r.Sequence = "02" }},
		{"wrong target", func(r *SQLTokenPolicyResponse) { r.Target.Audience = "other" }},
		{"wrong policy target", func(r *SQLTokenPolicyResponse) { r.Policy.Target.KeyspaceID++ }},
		{"wrong version", func(r *SQLTokenPolicyResponse) { r.Version = 2 }},
		{"inverted times", func(r *SQLTokenPolicyResponse) { r.ValidUntil = r.IssuedAt }},
		{"response outlives policy", func(r *SQLTokenPolicyResponse) { r.ValidUntil = r.Policy.ValidUntil + 1 }},
		{"policy not yet valid", func(r *SQLTokenPolicyResponse) { r.Policy.ValidFrom = r.IssuedAt + 1 }},
		{"excess token lifetime", func(r *SQLTokenPolicyResponse) { r.Policy.MaxTokenLifetimeSeconds = 901 }},
		{"excess session duration", func(r *SQLTokenPolicyResponse) { r.Policy.MaxSessionDurationSeconds = 86401 }},
		{"RSA key malformed", func(r *SQLTokenPolicyResponse) { r.Policy.Keys = map[string]string{"test-key": "not-key"} }},
		{"noncanonical revision", func(r *SQLTokenPolicyResponse) { r.Policy.Revision = "07" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newSQLRuntimeFixture(t)
			before := f.challenge(t)
			r := f.response(t)
			tt.change(&r)
			f.publish(t, r)
			require.False(t, f.status(t).Ready)
			require.Equal(t, before, f.challenge(t))
			require.False(t, f.runtime.IsManagedUsername("sql_user"))
		})
	}
	t.Run("authority signature", func(t *testing.T) {
		f := newSQLRuntimeFixture(t)
		f.writePayload(t, sqlJSON(t, f.response(t)))
		raw, err := os.ReadFile(f.opts.PolicyPath)
		require.NoError(t, err)
		var envelope SQLTokenPolicyEnvelope
		require.NoError(t, json.Unmarshal(raw, &envelope))
		envelope.Signature = base64.RawURLEncoding.EncodeToString(make([]byte, 64))
		require.NoError(t, os.WriteFile(f.opts.PolicyPath, sqlJSON(t, envelope), 0600))
		f.runtime.poll()
		require.False(t, f.status(t).Ready)
	})
	t.Run("challenge expired", func(t *testing.T) {
		f := newSQLRuntimeFixture(t)
		r := f.response(t)
		f.clock.advance(10 * time.Second)
		f.publish(t, r)
		require.False(t, f.status(t).Ready)
		require.NotEqual(t, r.Nonce, f.challenge(t).Nonce)
	})
}

func TestSQLTokenRuntimeStrictJSON(t *testing.T) {
	cases := []struct {
		name   string
		change func(string) string
	}{
		{"duplicate", func(s string) string { return strings.Replace(s, `"version":1`, `"version":1,"version":1`, 1) }},
		{"escaped duplicate", func(s string) string { return strings.Replace(s, `"version":1`, `"version":1,"vers\u0069on":1`, 1) }},
		{"unknown", func(s string) string { return strings.Replace(s, `"version":1`, `"version":1,"unknown":0`, 1) }},
		{"missing", func(s string) string { return strings.Replace(s, `"version":1,`, "", 1) }},
		{"null", func(s string) string { return strings.Replace(s, `"version":1`, `"version":null`, 1) }},
		{"fractional", func(s string) string { return strings.Replace(s, `"version":1`, `"version":1.0`, 1) }},
		{"negative", func(s string) string { return strings.Replace(s, `"version":1`, `"version":-1`, 1) }},
		{"exponent", func(s string) string { return strings.Replace(s, `"version":1`, `"version":1e0`, 1) }},
		{"trailing JSON", func(s string) string { return s + "{}" }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			f := newSQLRuntimeFixture(t)
			f.writePayload(t, []byte(tt.change(string(sqlJSON(t, f.response(t))))))
			f.runtime.poll()
			require.False(t, f.status(t).Ready)
		})
	}
	t.Run("padded base64", func(t *testing.T) {
		f := newSQLRuntimeFixture(t)
		payload := sqlJSON(t, f.response(t))
		signature := ed25519.Sign(f.authority, append([]byte(sqlPolicyDomain), payload...))
		envelope := SQLTokenPolicyEnvelope{Payload: base64.RawURLEncoding.EncodeToString(payload) + "=", Signature: base64.RawURLEncoding.EncodeToString(signature)}
		require.NoError(t, os.WriteFile(f.opts.PolicyPath, sqlJSON(t, envelope), 0600))
		f.runtime.poll()
		require.False(t, f.status(t).Ready)
	})
	t.Run("bounded input", func(t *testing.T) {
		f := newSQLRuntimeFixture(t)
		require.NoError(t, os.WriteFile(f.opts.PolicyPath, []byte(strings.Repeat(" ", sqlPolicyMaxBytes+1)), 0600))
		f.runtime.poll()
		require.False(t, f.status(t).Ready)
	})
}

func TestSQLTokenRuntimeReplayAndExpiry(t *testing.T) {
	f := newSQLRuntimeFixture(t)
	r := f.response(t)
	f.publish(t, r)
	callbacks := make(chan *SQLTokenGuard, 2)
	g := f.login(t, func(g *SQLTokenGuard) { require.True(t, f.runtime.IsManagedUsername("sql_user")); callbacks <- g })
	f.clock.advance(20 * time.Second)
	f.publish(t, r)
	require.NoError(t, g.Check())
	// A signed, old response cannot renew a lease, even with a repeated file write.
	f.clock.advance(10 * time.Second)
	f.publish(t, r)
	require.ErrorIs(t, g.Check(), ErrSQLTokenAccessDenied)
	require.False(t, f.status(t).Ready)
	select {
	case received := <-callbacks:
		require.Same(t, g, received)
	case <-time.After(time.Second):
		t.Fatal("idle revocation callback missing")
	}
	f.runtime.poll()
	require.ErrorIs(t, g.Check(), ErrSQLTokenAccessDenied)
	select {
	case <-callbacks:
		t.Fatal("callback repeated")
	default:
	}
	require.True(t, f.runtime.IsManagedUsername("sql_user"))
	// A new response can admit new connections but can never revive a dead guard.
	f.publish(t, f.response(t))
	require.True(t, f.status(t).Ready)
	require.ErrorIs(t, g.Check(), ErrSQLTokenAccessDenied)
	require.NoError(t, f.login(t, nil).Check())
}

func TestSQLTokenRuntimeRefreshDoesNotExtendSession(t *testing.T) {
	f := newSQLRuntimeFixture(t)
	f.doc.MaxSessionDurationSeconds = 5
	f.publish(t, f.response(t))
	g := f.login(t, nil)
	original := g.Identity()
	for i := 0; i < 4; i++ {
		f.clock.advance(time.Second)
		f.publish(t, f.response(t))
		require.NoError(t, g.Check())
		require.Equal(t, original, g.Identity())
	}
	f.clock.advance(time.Second)
	f.publish(t, f.response(t))
	require.ErrorIs(t, g.Check(), ErrSQLTokenAccessDenied)
	require.NoError(t, f.login(t, nil).Check())
}

func TestSQLTokenRuntimeLateRefreshCannotReviveIdleGuard(t *testing.T) {
	f := newSQLRuntimeFixture(t)
	f.publish(t, f.response(t))
	g := f.login(t, nil)
	// Polling at 20s rotates the challenge while the original lease is live.
	f.clock.advance(20 * time.Second)
	f.runtime.poll()
	require.NoError(t, g.Check())
	// No Check or poll sees the old response expire at 29s before this refresh.
	f.clock.advance(9500 * time.Millisecond)
	f.publish(t, f.response(t))
	require.True(t, f.status(t).Ready)
	require.ErrorIs(t, g.Check(), ErrSQLTokenAccessDenied)
	require.NoError(t, f.login(t, nil).Check())
}

func TestSQLTokenRuntimePolicyRevocation(t *testing.T) {
	tests := []struct {
		name   string
		change func(*SQLTokenPolicyDocument)
	}{
		{"key removed", func(p *SQLTokenPolicyDocument) { delete(p.Keys, "test-key") }},
		{"binding removed", func(p *SQLTokenPolicyDocument) { delete(p.Bindings, "binding-test") }},
		{"binding disabled", func(p *SQLTokenPolicyDocument) {
			b := p.Bindings["binding-test"]
			b.Enabled = false
			b.BindingRevision = "4"
			b.PolicyRevision = "10"
			p.Bindings["binding-test"] = b
		}},
		{"policy revision changed", func(p *SQLTokenPolicyDocument) {
			b := p.Bindings["binding-test"]
			b.PolicyRevision = "10"
			p.Bindings["binding-test"] = b
		}},
		{"native grants not applied", func(p *SQLTokenPolicyDocument) {
			b := p.Bindings["binding-test"]
			b.PolicyRevision = "10"
			b.DesiredGrantRevision = "5"
			p.Bindings["binding-test"] = b
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newSQLRuntimeFixture(t)
			f.publish(t, f.response(t))
			g := f.login(t, nil)
			pending, err := f.runtime.Authenticate(f.token.token(t), f.token.request)
			require.NoError(t, err)
			f.doc.Revision = "8"
			tt.change(&f.doc)
			f.publish(t, f.response(t))
			require.Equal(t, "8", f.status(t).Revision)
			require.ErrorIs(t, g.Check(), ErrSQLTokenAccessDenied)
			_, err = f.runtime.Register(pending, func() bool { return true }, nil)
			require.ErrorIs(t, err, ErrSQLTokenAccessDenied)
			require.True(t, f.runtime.IsManagedUsername("sql_user"))
		})
	}
}

func TestSQLTokenRuntimeRollbackAndTombstones(t *testing.T) {
	t.Run("same revision change", func(t *testing.T) {
		f := newSQLRuntimeFixture(t)
		f.publish(t, f.response(t))
		digest := f.status(t).PolicyDigest
		f.doc.MaxSessionDurationSeconds++
		f.publish(t, f.response(t))
		require.Equal(t, digest, f.status(t).PolicyDigest)
	})
	t.Run("snapshot rollback", func(t *testing.T) {
		f := newSQLRuntimeFixture(t)
		f.publish(t, f.response(t))
		f.doc.Revision = "6"
		f.publish(t, f.response(t))
		require.Equal(t, "7", f.status(t).Revision)
	})
	t.Run("key material immutable", func(t *testing.T) {
		f := newSQLRuntimeFixture(t)
		f.publish(t, f.response(t))
		der, err := x509.MarshalPKIXPublicKey(pubKeys[1])
		require.NoError(t, err)
		f.doc.Revision = "8"
		f.doc.Keys["test-key"] = base64.RawURLEncoding.EncodeToString(der)
		f.publish(t, f.response(t))
		require.Equal(t, "7", f.status(t).Revision)
	})
	t.Run("removed key cannot resurrect", func(t *testing.T) {
		f := newSQLRuntimeFixture(t)
		f.publish(t, f.response(t))
		key := f.doc.Keys["test-key"]
		delete(f.doc.Keys, "test-key")
		f.doc.Revision = "8"
		f.publish(t, f.response(t))
		f.doc.Keys["test-key"] = key
		f.doc.Revision = "9"
		f.publish(t, f.response(t))
		require.Equal(t, "8", f.status(t).Revision)
	})
	t.Run("removed binding needs both newer fences", func(t *testing.T) {
		f := newSQLRuntimeFixture(t)
		f.publish(t, f.response(t))
		binding := f.doc.Bindings["binding-test"]
		delete(f.doc.Bindings, "binding-test")
		f.doc.Revision = "8"
		f.publish(t, f.response(t))
		f.doc.Revision = "9"
		f.doc.Bindings["binding-test"] = binding
		f.publish(t, f.response(t))
		require.Equal(t, "8", f.status(t).Revision)
		binding.BindingRevision = "4"
		f.doc.Bindings["binding-test"] = binding
		f.publish(t, f.response(t))
		require.Equal(t, "8", f.status(t).Revision)
		binding.PolicyRevision = "10"
		f.doc.Bindings["binding-test"] = binding
		f.publish(t, f.response(t))
		require.Equal(t, "9", f.status(t).Revision)
	})
	t.Run("same content different JSON ordering", func(t *testing.T) {
		f := newSQLRuntimeFixture(t)
		f.publish(t, f.response(t))
		f.clock.advance(time.Second)
		r := f.response(t)
		var object map[string]any
		require.NoError(t, json.Unmarshal(sqlJSON(t, r), &object))
		payload, err := json.MarshalIndent(object, "", " ")
		require.NoError(t, err)
		f.writePayload(t, payload)
		f.runtime.poll()
		require.Equal(t, r.Nonce, f.status(t).AcceptedNonce)
		require.True(t, f.status(t).Ready)
	})
}

func TestSQLTokenRuntimeRestart(t *testing.T) {
	f := newSQLRuntimeFixture(t)
	response := f.response(t)
	f.publish(t, response)
	f.clock.advance(3 * time.Second)
	f.runtime.poll()
	f.restart(t)
	require.True(t, f.runtime.IsManagedUsername("sql_user"))
	require.False(t, f.status(t).Ready)
	require.NotEqual(t, response.Incarnation, f.challenge(t).Incarnation)
	f.publish(t, response)
	require.False(t, f.status(t).Ready)
	require.NotEmpty(t, f.status(t).PolicyDigest)
	fresh := f.response(t)
	f.publish(t, fresh)
	require.True(t, f.status(t).Ready)
	f.doc.Revision = "6"
	f.publish(t, f.response(t))
	require.Equal(t, "7", f.status(t).Revision)

	t.Run("expired policy and rolled back local clock", func(t *testing.T) {
		f := newSQLRuntimeFixture(t)
		f.doc.ValidUntil = f.token.now.Unix() + 5
		cached := f.response(t)
		cached.ValidUntil = f.doc.ValidUntil
		f.publish(t, cached)
		require.True(t, f.status(t).Ready)
		f.clock.advance(6 * time.Second)
		f.runtime.Close()
		f.clock.advance(-time.Hour)
		f.restart(t)
		f.publish(t, cached)
		require.False(t, f.status(t).Ready)
		// Correct fresh nonce is insufficient when signed time regresses below
		// the durable lower/high-water fences, regardless of the local clock.
		f.doc.Revision = "8"
		f.doc.ValidUntil = f.token.now.Add(time.Hour).Unix()
		stale := f.response(t)
		stale.IssuedAt = f.token.now.Unix()
		stale.ValidUntil = stale.IssuedAt + 30
		f.publish(t, stale)
		require.False(t, f.status(t).Ready)
		require.True(t, f.runtime.IsManagedUsername("sql_user"))
		fresh := f.response(t)
		fresh.IssuedAt = f.token.now.Unix() + 6
		fresh.ValidUntil = fresh.IssuedAt + 30
		f.publish(t, fresh)
		require.True(t, f.status(t).Ready)
		require.NoError(t, f.login(t, nil).Check())
	})
	for _, kind := range []string{"key", "binding"} {
		t.Run("persisted "+kind+" tombstone", func(t *testing.T) {
			f := newSQLRuntimeFixture(t)
			f.publish(t, f.response(t))
			key, binding := f.doc.Keys["test-key"], f.doc.Bindings["binding-test"]
			if kind == "key" {
				delete(f.doc.Keys, "test-key")
			} else {
				delete(f.doc.Bindings, "binding-test")
			}
			f.doc.Revision = "8"
			f.publish(t, f.response(t))
			f.restart(t)
			require.True(t, f.runtime.IsManagedUsername("sql_user"))
			require.False(t, f.status(t).Ready)
			f.publish(t, f.response(t))
			require.True(t, f.status(t).Ready)
			if kind == "key" {
				f.doc.Keys["test-key"] = key
			} else {
				f.doc.Bindings["binding-test"] = binding
			}
			f.doc.Revision = "9"
			f.publish(t, f.response(t))
			require.Equal(t, "8", f.status(t).Revision)
		})
	}
}

func TestSQLTokenRuntimeRejectsStateCorruption(t *testing.T) {
	for _, name := range []string{"corrupt watermark", "missing watermark", "changed authority", "open permissions", "concurrent owner"} {
		t.Run(name, func(t *testing.T) {
			f := newSQLRuntimeFixture(t)
			f.publish(t, f.response(t))
			if name != "concurrent owner" {
				f.runtime.Close()
			}
			switch name {
			case "corrupt watermark":
				require.NoError(t, os.WriteFile(filepath.Join(f.opts.StateDir, "watermark.json"), []byte(`{}`), 0600))
			case "missing watermark":
				require.NoError(t, os.Remove(filepath.Join(f.opts.StateDir, "watermark.json")))
			case "changed authority":
				seed := make([]byte, ed25519.SeedSize)
				seed[0] = 1
				key := ed25519.NewKeyFromSeed(seed)
				der, err := x509.MarshalPKIXPublicKey(key.Public())
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(f.opts.AuthorityKeyPath, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}), 0600))
			case "open permissions":
				require.NoError(t, os.Chmod(f.opts.StateDir, 0755))
			}
			r, err := newSQLTokenRuntime(f.opts, f.clock.read)
			if r != nil {
				r.Close()
			}
			require.ErrorIs(t, err, ErrSQLTokenPolicyUnavailable)
		})
	}
}

func TestSQLTokenRuntimeNativeCheckAndReplace(t *testing.T) {
	f := newSQLRuntimeFixture(t)
	f.publish(t, f.response(t))
	var allowed atomic.Bool
	allowed.Store(true)
	identity, err := f.runtime.Authenticate(f.token.token(t), f.token.request)
	require.NoError(t, err)
	old, err := f.runtime.Register(identity, allowed.Load, nil)
	require.NoError(t, err)
	next := f.login(t, nil)
	allowed.Store(false)
	require.ErrorIs(t, f.runtime.Replace(old, next), ErrSQLTokenAccessDenied)
	require.NoError(t, next.Check())
	allowed.Store(true)
	require.ErrorIs(t, old.Check(), ErrSQLTokenAccessDenied) // observed failure is terminal
	old = f.login(t, nil)
	require.NoError(t, f.runtime.Replace(old, next))
	require.ErrorIs(t, old.Check(), ErrSQLTokenAccessDenied)
	require.NoError(t, next.Check())
	require.NoError(t, f.runtime.Replace(next, nil))
	require.ErrorIs(t, next.Check(), ErrSQLTokenAccessDenied)
	// Native-cache changes also revoke sessions without a policy publication.
	callback := make(chan struct{}, 1)
	identity, err = f.runtime.Authenticate(f.token.token(t), f.token.request)
	require.NoError(t, err)
	guard, err := f.runtime.Register(identity, allowed.Load, func(*SQLTokenGuard) { callback <- struct{}{} })
	require.NoError(t, err)
	allowed.Store(false)
	f.runtime.poll()
	require.ErrorIs(t, guard.Check(), ErrSQLTokenAccessDenied)
	select {
	case <-callback:
	case <-time.After(time.Second):
		t.Fatal("native revocation missing")
	}
}

func TestSQLTokenRuntimeTrustedTimeBounds(t *testing.T) {
	t.Run("future nbf remains denied under response latency", func(t *testing.T) {
		f := newSQLRuntimeFixture(t)
		response := f.response(t)
		f.clock.advance(4 * time.Second)
		f.publish(t, response)
		f.token.claims[jwt.NotBeforeKey] = f.token.now.Add(3 * time.Second).Unix()
		_, err := f.runtime.Authenticate(f.token.token(t), f.token.request)
		require.ErrorIs(t, err, ErrInvalidSQLToken)
	})
	t.Run("expiry uses upper bound", func(t *testing.T) {
		f := newSQLRuntimeFixture(t)
		response := f.response(t)
		f.clock.advance(4 * time.Second)
		f.publish(t, response)
		f.token.claims[jwt.ExpirationKey] = f.token.now.Add(5 * time.Second).Unix()
		_, err := f.runtime.Authenticate(f.token.token(t), f.token.request)
		require.ErrorIs(t, err, ErrInvalidSQLToken)
	})
	t.Run("response expiry uses upper bound", func(t *testing.T) {
		f := newSQLRuntimeFixture(t)
		response := f.response(t)
		response.ValidUntil = response.IssuedAt + 3
		f.publish(t, response)
		f.clock.advance(2 * time.Second)
		f.runtime.poll()
		require.False(t, f.status(t).Ready)
	})
	t.Run("same second responses cannot hold trusted clock", func(t *testing.T) {
		f := newSQLRuntimeFixture(t)
		f.doc.MaxSessionDurationSeconds = 2
		f.publish(t, f.response(t))
		g := f.login(t, nil)
		var before, upperBefore time.Time
		for i := 1; i <= 25; i++ {
			f.clock.advance(100 * time.Millisecond)
			r := f.response(t)
			r.IssuedAt = f.token.now.Unix()
			r.ValidUntil = r.IssuedAt + 30
			f.publish(t, r)
			f.runtime.mu.Lock()
			now := f.runtime.authorityNowLocked(f.clock.read())
			upper := f.runtime.authorityUpperNowLocked(f.clock.read())
			f.runtime.mu.Unlock()
			require.False(t, now.Before(before))
			require.False(t, upper.Before(upperBefore))
			before = now
			upperBefore = upper
		}
		require.True(t, before.After(f.token.now.Add(2*time.Second)))
		require.ErrorIs(t, g.Check(), ErrSQLTokenAccessDenied)
	})
	t.Run("signed time rollback", func(t *testing.T) {
		f := newSQLRuntimeFixture(t)
		f.publish(t, f.response(t))
		f.clock.advance(2 * time.Second)
		f.publish(t, f.response(t))
		previous := f.status(t).AcceptedNonce
		r := f.response(t)
		r.IssuedAt--
		f.publish(t, r)
		require.Equal(t, previous, f.status(t).AcceptedNonce)
	})
}

func TestSQLTokenRuntimePollAndGlobalInstall(t *testing.T) {
	f := newSQLRuntimeFixture(t)
	f.runtime.Close()
	runtime, err := NewSQLTokenRuntime(f.opts)
	require.NoError(t, err)
	t.Cleanup(runtime.Close)
	f.runtime = runtime
	response := f.response(t)
	f.writePayload(t, sqlJSON(t, response))
	require.Eventually(t, func() bool { return runtime.IsManagedUsername("sql_user") }, time.Second, 10*time.Millisecond)
	old := SetSQLTokenRuntime(runtime)
	t.Cleanup(func() { SetSQLTokenRuntime(old) })
	require.Same(t, runtime, CurrentSQLTokenRuntime())
	require.Equal(t, f.opts.Target, runtime.Target())
	runtime.Close()
	require.False(t, f.status(t).Ready)
	require.Equal(t, "closed", f.status(t).LastError)
}

func TestSQLTokenRuntimeDocumentBounds(t *testing.T) {
	f := newSQLRuntimeFixture(t)
	for _, test := range []struct {
		name string
		edit func(*SQLTokenPolicyDocument)
	}{
		{"too many keys", func(d *SQLTokenPolicyDocument) {
			key := d.Keys["test-key"]
			for i := 0; i < sqlPolicyMaxKeys; i++ {
				d.Keys[strconv.Itoa(i)] = key
			}
		}},
		{"ambiguous native username", func(d *SQLTokenPolicyDocument) { d.Bindings["other"] = d.Bindings["binding-test"] }},
		{"unknown storage scope", func(d *SQLTokenPolicyDocument) { d.Target.StorageScope = "unknown" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			var doc SQLTokenPolicyDocument
			require.NoError(t, json.Unmarshal(sqlJSON(t, f.doc), &doc))
			test.edit(&doc)
			_, err := doc.policy()
			require.ErrorIs(t, err, ErrSQLTokenPolicyUnavailable)
		})
	}
}
