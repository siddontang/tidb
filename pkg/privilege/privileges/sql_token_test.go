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
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jws"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionauth"
	"github.com/stretchr/testify/require"
)

type sqlTokenFixture struct {
	now     time.Time
	request SQLTokenRequest
	policy  SQLTokenPolicy
	header  map[string]any
	claims  map[string]any
}

func newSQLTokenFixture() sqlTokenFixture {
	now := time.Unix(1793682000, 0)
	target := sessionauth.Target{Audience: "urn:example:database:orders", StorageClusterID: 101, StorageScope: sessionauth.StorageScopeKeyspace, KeyspaceID: 42}
	return sqlTokenFixture{
		now:     now,
		request: SQLTokenRequest{Target: target, NativeUsername: "sql_user", NativeHost: "%", NativePlugin: SQLTokenPlugin, TLSEstablished: true},
		policy: SQLTokenPolicy{
			Target: target, Issuer: "https://issuer.invalid/provider-test", SnapshotRevision: 7,
			ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(30 * time.Second),
			MaxTokenLifetime: 5 * time.Minute, MaxSessionDuration: time.Hour,
			Keys: map[string]*rsa.PublicKey{"test-key": pubKeys[0]},
			Bindings: map[string]SQLTokenBinding{"binding-test": {
				PrincipalID: "principal-test", NativeUsername: "sql_user", NativeHost: "%",
				BindingRevision: 3, PolicyRevision: 9, Enabled: true, ExpiresAt: now.Add(2 * time.Hour),
				DesiredGrantRevision: 4, AppliedGrantRevision: 4,
			}},
		},
		header: map[string]any{jws.AlgorithmKey: string(jwa.RS256), jws.TypeKey: SQLTokenType, jws.KeyIDKey: "test-key"},
		claims: map[string]any{
			SQLTokenClaimVersion: SQLTokenVersion, jwt.IssuerKey: "https://issuer.invalid/provider-test", jwt.AudienceKey: "urn:example:database:orders", jwt.SubjectKey: "sql_user",
			SQLTokenClaimPrincipalID: "principal-test",
			SQLTokenClaimBindingID:   "binding-test", SQLTokenClaimBindingRevision: "3", SQLTokenClaimPolicyRevision: "9",
			jwt.IssuedAtKey: now.Unix(), jwt.NotBeforeKey: now.Unix(), jwt.ExpirationKey: now.Add(5 * time.Minute).Unix(), jwt.JwtIDKey: "token-test",
		},
	}
}

func (f *sqlTokenFixture) editBinding(change func(*SQLTokenBinding)) {
	binding := f.policy.Bindings["binding-test"]
	change(&binding)
	f.policy.Bindings["binding-test"] = binding
}

func sqlJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	require.NoError(t, err)
	return raw
}

// The existing package fixtures are synthetic keys. Sign raw JSON through jwx
// so malformed/duplicate-field cases still have a correct cryptographic signature.
func signSQLJSON(t *testing.T, header, payload []byte) []byte {
	t.Helper()
	input := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	signer, err := jws.NewSigner(jwa.RS256)
	require.NoError(t, err)
	signature, err := signer.Sign([]byte(input), priKeys[0])
	require.NoError(t, err)
	return []byte(input + "." + base64.RawURLEncoding.EncodeToString(signature))
}

func (f sqlTokenFixture) token(t *testing.T) []byte {
	t.Helper()
	return signSQLJSON(t, sqlJSON(t, f.header), sqlJSON(t, f.claims))
}

func requireSQLDenied(t *testing.T, raw []byte, f sqlTokenFixture, expected error) {
	t.Helper()
	identity, err := VerifySQLToken(raw, f.request, f.policy, f.now)
	require.Equal(t, expected, err)
	require.Equal(t, sessionauth.Identity{}, identity)
	// Compare the fixed sentinel as well as checking that neither raw credentials
	// nor a representative private claim value appears in the error surface.
	if len(raw) != 0 {
		require.NotContains(t, err.Error(), string(raw))
	}
	require.NotContains(t, err.Error(), "private-claim-canary")
}

// Keep this signed wire fixture literal and independent of production constants:
// changing a constant and every fixture together must not silently change v1.
func TestSQLTokenWireContract(t *testing.T) {
	f := newSQLTokenFixture()
	header := []byte(`{"alg":"RS256","typ":"tidb-sql+jwt","kid":"test-key"}`)
	payload := []byte(`{"ver":1,"iss":"https://issuer.invalid/provider-test","aud":"urn:example:database:orders","sub":"sql_user","principal_id":"principal-test","binding_id":"binding-test","binding_revision":"3","policy_revision":"9","iat":1793682000,"nbf":1793682000,"exp":1793682300,"jti":"token-test"}`)
	require.Equal(t, "tidb_sql_token_v1", SQLTokenPlugin)
	identity, err := VerifySQLToken(signSQLJSON(t, header, payload), f.request, f.policy, f.now)
	require.NoError(t, err)
	require.Equal(t, "urn:example:database:orders", identity.Target.Audience)
	require.Equal(t, "principal-test", identity.PrincipalID)
	require.Equal(t, "sql_user", identity.NativeUsername)
}

func TestSQLTokenDeploymentScopes(t *testing.T) {
	// These synthetic operator policies need no provider-specific account hierarchy or
	// human/workload taxonomy. Resource names remain opaque to the verifier.
	tests := []struct {
		name, issuer, audience, principal string
		scope                             sessionauth.StorageScope
		keyspace                          uint32
	}{
		{"self-managed cluster", "https://broker.invalid", "urn:example:database:orders", "employee:42", sessionauth.StorageScopeCluster, 0},
		{"API-v2 default", "https://broker.invalid", "database/default", "service:batch", sessionauth.StorageScopeKeyspace, 0},
		{"external issuer", "https://external-broker.invalid", "urn:example:database:tenant-42", "opaque-principal", sessionauth.StorageScopeKeyspace, 42},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newSQLTokenFixture()
			f.request.Target = sessionauth.Target{Audience: tt.audience, StorageClusterID: 101, StorageScope: tt.scope, KeyspaceID: tt.keyspace}
			f.policy.Target, f.policy.Issuer = f.request.Target, tt.issuer
			f.claims[jwt.AudienceKey], f.claims[jwt.IssuerKey], f.claims[SQLTokenClaimPrincipalID] = tt.audience, tt.issuer, tt.principal
			f.editBinding(func(b *SQLTokenBinding) { b.PrincipalID = tt.principal })
			raw := f.token(t)
			identity, err := VerifySQLToken(raw, f.request, f.policy, f.now)
			require.NoError(t, err)
			require.Equal(t, f.request.Target, identity.Target)
			require.Equal(t, tt.principal, identity.PrincipalID)
			// Equivalent native bindings and shared signing keys do not authorize
			// reuse in a different resource, even when both local descriptors agree.
			f.request.Target.Audience, f.policy.Target.Audience = "another-database", "another-database"
			requireSQLDenied(t, raw, f, ErrSQLTokenAccessDenied)
		})
	}
	for _, scope := range []sessionauth.StorageScope{sessionauth.StorageScopeCluster, sessionauth.StorageScopeKeyspace} {
		f := newSQLTokenFixture()
		f.request.Target.KeyspaceID, f.policy.Target.KeyspaceID = 0, 0
		f.request.Target.StorageScope, f.policy.Target.StorageScope = scope, scope
		if scope == sessionauth.StorageScopeCluster {
			f.request.Target.StorageScope = sessionauth.StorageScopeKeyspace
		} else {
			f.request.Target.StorageScope = sessionauth.StorageScopeCluster
		}
		// A cluster and API-v2 DEFAULT are distinct despite sharing numeric zero.
		requireSQLDenied(t, f.token(t), f, ErrSQLTokenAccessDenied)
	}
}

func TestSQLTokenIdentity(t *testing.T) {
	f := newSQLTokenFixture()
	raw := f.token(t)
	identity, err := VerifySQLToken(raw, f.request, f.policy, f.now)
	require.NoError(t, err)
	require.Equal(t, sessionauth.Identity{
		Target: f.policy.Target, PrincipalID: "principal-test",
		NativeUsername: "sql_user", NativeHost: "%", BindingID: "binding-test", BindingRevision: 3, PolicyRevision: 9,
		Issuer: f.policy.Issuer, KeyID: "test-key", Profile: SQLTokenPlugin, TokenID: "token-test",
		VerificationSnapshotRevision: 7, AuthenticatedAt: f.now,
		SessionDeadline: f.now.Add(time.Hour), PolicyLeaseDeadline: f.now.Add(30 * time.Second),
	}, identity)
	// Session lifetime is deliberately independent of the five-minute login token.
	require.True(t, identity.SessionDeadline.After(time.Unix(f.claims[jwt.ExpirationKey].(int64), 0)))
	saved := identity
	f.policy.Bindings["binding-test"] = SQLTokenBinding{}
	f.policy.Keys["test-key"] = pubKeys[1]
	for i := range raw {
		raw[i] = 0
	}
	require.Equal(t, saved, identity)

	tests := []struct {
		name    string
		change  func(*sqlTokenFixture)
		session time.Duration
		lease   time.Duration
	}{
		{"binding caps both deadlines", func(f *sqlTokenFixture) {
			f.editBinding(func(b *SQLTokenBinding) { b.ExpiresAt = f.now.Add(10 * time.Second) })
		}, 10 * time.Second, 10 * time.Second},
		{"session caps lease", func(f *sqlTokenFixture) { f.policy.MaxSessionDuration = time.Second }, time.Second, time.Second},
		{"policy valid-from is inclusive", func(f *sqlTokenFixture) { f.policy.ValidFrom = f.now }, time.Hour, 30 * time.Second},
		{"fractional receiver clock", func(f *sqlTokenFixture) { f.now = f.now.Add(time.Nanosecond) }, time.Hour, 30*time.Second - time.Nanosecond},
		{"not-before may follow issuance", func(f *sqlTokenFixture) {
			f.claims[jwt.IssuedAtKey] = f.now.Add(-time.Second).Unix()
			f.claims[jwt.ExpirationKey] = f.now.Add(time.Minute).Unix()
		}, time.Hour, 30 * time.Second},
		{"uint64 revisions preserve precision", func(f *sqlTokenFixture) {
			f.claims[SQLTokenClaimBindingRevision], f.claims[SQLTokenClaimPolicyRevision] = "18446744073709551615", "9007199254740993"
			f.editBinding(func(b *SQLTokenBinding) { b.BindingRevision = math.MaxUint64; b.PolicyRevision = 9007199254740993 })
		}, time.Hour, 30 * time.Second},
		{"maximum configured durations", func(f *sqlTokenFixture) {
			f.policy.MaxTokenLifetime, f.policy.MaxSessionDuration = 15*time.Minute, 24*time.Hour
			f.claims[jwt.ExpirationKey] = f.now.Add(15 * time.Minute).Unix()
			f.editBinding(func(b *SQLTokenBinding) { b.ExpiresAt = f.now.Add(48 * time.Hour) })
		}, 24 * time.Hour, 30 * time.Second},
		{"identifier maximum length", func(f *sqlTokenFixture) {
			f.request.Target.Audience = strings.Repeat("i", sqlTokenMaxIDBytes)
			f.policy.Target = f.request.Target
			f.claims[jwt.AudienceKey] = f.policy.Target.Audience
		}, time.Hour, 30 * time.Second},
		{"minimum non-default keyspace number", func(f *sqlTokenFixture) {
			f.request.Target.KeyspaceID = 1
			f.policy.Target = f.request.Target
		}, time.Hour, 30 * time.Second},
		{"maximum API-v2 keyspace number", func(f *sqlTokenFixture) {
			f.request.Target.KeyspaceID = 0xFFFFFF
			f.policy.Target = f.request.Target
		}, time.Hour, 30 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newSQLTokenFixture()
			tt.change(&f)
			identity, err := VerifySQLToken(f.token(t), f.request, f.policy, f.now)
			require.NoError(t, err)
			require.Equal(t, f.now.Add(tt.session), identity.SessionDeadline)
			require.Equal(t, f.now.Add(tt.lease), identity.PolicyLeaseDeadline)
			require.Equal(t, f.policy.Target, identity.Target)
			require.Equal(t, f.claims[SQLTokenClaimPrincipalID], identity.PrincipalID)
		})
	}
}

func TestSQLTokenRejectsUntrustedInputs(t *testing.T) {
	tests := []struct {
		name   string
		change func(*sqlTokenFixture)
		err    error
	}{
		{"different actual audience", func(f *sqlTokenFixture) { f.request.Target.Audience = "database-other" }, ErrSQLTokenAccessDenied},
		{"same keyspace in different storage cluster", func(f *sqlTokenFixture) { f.request.Target.StorageClusterID++ }, ErrSQLTokenAccessDenied},
		{"different actual keyspace", func(f *sqlTokenFixture) { f.request.Target.KeyspaceID++ }, ErrSQLTokenAccessDenied},
		{"missing actual descriptor", func(f *sqlTokenFixture) { f.request.Target = sessionauth.Target{} }, ErrSQLTokenAccessDenied},
		{"missing trusted descriptor", func(f *sqlTokenFixture) { f.policy.Target = sessionauth.Target{} }, ErrSQLTokenPolicyUnavailable},
		{"missing storage scope", func(f *sqlTokenFixture) { f.policy.Target.StorageScope = 0 }, ErrSQLTokenPolicyUnavailable},
		{"unknown storage scope", func(f *sqlTokenFixture) { f.policy.Target.StorageScope = sessionauth.StorageScope(255) }, ErrSQLTokenPolicyUnavailable},
		{"cluster with nonzero keyspace", func(f *sqlTokenFixture) { f.policy.Target.StorageScope = sessionauth.StorageScopeCluster }, ErrSQLTokenPolicyUnavailable},
		{"missing actual storage scope", func(f *sqlTokenFixture) { f.request.Target.StorageScope = 0 }, ErrSQLTokenAccessDenied},
		{"keyspace above API-v2 range", func(f *sqlTokenFixture) {
			f.request.Target.KeyspaceID = 0x1000000
			f.policy.Target = f.request.Target
		}, ErrSQLTokenPolicyUnavailable},
		{"null keyspace", func(f *sqlTokenFixture) {
			f.request.Target.KeyspaceID = 0xFFFFFFFF
			f.policy.Target = f.request.Target
		}, ErrSQLTokenPolicyUnavailable},
		{"unknown storage cluster", func(f *sqlTokenFixture) { f.request.Target.StorageClusterID = 0; f.policy.Target = f.request.Target }, ErrSQLTokenPolicyUnavailable},
		{"noncanonical target", func(f *sqlTokenFixture) { f.policy.Target.Audience = "database test" }, ErrSQLTokenPolicyUnavailable},
		{"oversized target", func(f *sqlTokenFixture) { f.policy.Target.Audience = strings.Repeat("i", 257) }, ErrSQLTokenPolicyUnavailable},
		{"wrong native username", func(f *sqlTokenFixture) { f.request.NativeUsername = "other" }, ErrSQLTokenAccessDenied},
		{"wrong matched host", func(f *sqlTokenFixture) { f.request.NativeHost = "localhost" }, ErrSQLTokenAccessDenied},
		{"missing native account", func(f *sqlTokenFixture) { f.request.NativeUsername = "" }, ErrSQLTokenAccessDenied},
		{"missing matched host", func(f *sqlTokenFixture) { f.request.NativeHost = "" }, ErrSQLTokenAccessDenied},
		{"legacy auth plugin", func(f *sqlTokenFixture) { f.request.NativePlugin = mysql.AuthTiDBAuthToken }, ErrSQLTokenAccessDenied},
		{"password auth plugin", func(f *sqlTokenFixture) { f.request.NativePlugin = mysql.AuthNativePassword }, ErrSQLTokenAccessDenied},
		{"locked native account", func(f *sqlTokenFixture) { f.request.AccountLocked = true }, ErrSQLTokenAccessDenied},
		{"plaintext transport", func(f *sqlTokenFixture) { f.request.TLSEstablished = false }, ErrSQLTokenAccessDenied},
		{"missing issuer", func(f *sqlTokenFixture) { f.policy.Issuer = "" }, ErrSQLTokenPolicyUnavailable},
		{"oversized issuer", func(f *sqlTokenFixture) { f.policy.Issuer = strings.Repeat("i", 1025) }, ErrSQLTokenPolicyUnavailable},
		{"missing snapshot revision", func(f *sqlTokenFixture) { f.policy.SnapshotRevision = 0 }, ErrSQLTokenPolicyUnavailable},
		{"unknown clock", func(f *sqlTokenFixture) { f.now = time.Time{} }, ErrSQLTokenPolicyUnavailable},
		{"clock before supported range", func(f *sqlTokenFixture) { f.now = time.Unix(-1, 0) }, ErrSQLTokenPolicyUnavailable},
		{"clock beyond supported range", func(f *sqlTokenFixture) {
			f.now = time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC)
		}, ErrSQLTokenPolicyUnavailable},
		{"missing policy start", func(f *sqlTokenFixture) { f.policy.ValidFrom = time.Time{} }, ErrSQLTokenPolicyUnavailable},
		{"missing policy expiry", func(f *sqlTokenFixture) { f.policy.ValidUntil = time.Time{} }, ErrSQLTokenPolicyUnavailable},
		{"policy start before UTC epoch despite local year", func(f *sqlTokenFixture) {
			f.policy.ValidFrom = time.Date(1970, time.January, 1, 0, 0, 0, 0, time.FixedZone("test-east", 3600))
		}, ErrSQLTokenPolicyUnavailable},
		{"policy expiry beyond UTC range despite local year", func(f *sqlTokenFixture) {
			f.policy.ValidUntil = time.Date(9999, time.December, 31, 23, 59, 59, 0, time.FixedZone("test-west", -3600))
		}, ErrSQLTokenPolicyUnavailable},
		{"future snapshot", func(f *sqlTokenFixture) { f.policy.ValidFrom = f.now.Add(time.Nanosecond) }, ErrSQLTokenPolicyUnavailable},
		{"expired snapshot boundary", func(f *sqlTokenFixture) { f.policy.ValidUntil = f.now }, ErrSQLTokenPolicyUnavailable},
		{"inverted snapshot interval", func(f *sqlTokenFixture) { f.policy.ValidFrom = f.policy.ValidUntil }, ErrSQLTokenPolicyUnavailable},
		{"zero token lifetime", func(f *sqlTokenFixture) { f.policy.MaxTokenLifetime = 0 }, ErrSQLTokenPolicyUnavailable},
		{"excessive token lifetime", func(f *sqlTokenFixture) { f.policy.MaxTokenLifetime = 15*time.Minute + time.Nanosecond }, ErrSQLTokenPolicyUnavailable},
		{"negative session duration", func(f *sqlTokenFixture) { f.policy.MaxSessionDuration = -1 }, ErrSQLTokenPolicyUnavailable},
		{"excessive session duration", func(f *sqlTokenFixture) { f.policy.MaxSessionDuration = 24*time.Hour + time.Nanosecond }, ErrSQLTokenPolicyUnavailable},
		{"no keys", func(f *sqlTokenFixture) { f.policy.Keys = nil }, ErrSQLTokenPolicyUnavailable},
		{"unknown signing key", func(f *sqlTokenFixture) { f.header[jws.KeyIDKey] = "different-key" }, ErrSQLTokenPolicyUnavailable},
		{"issuer scoped keys", func(f *sqlTokenFixture) { f.policy.Issuer = "other-issuer" }, ErrSQLTokenAccessDenied},
		{"wrong signature", func(f *sqlTokenFixture) { f.policy.Keys["test-key"] = pubKeys[1] }, ErrInvalidSQLToken},
		{"nil rsa key", func(f *sqlTokenFixture) { f.policy.Keys["test-key"] = nil }, ErrSQLTokenPolicyUnavailable},
		{"nil rsa modulus", func(f *sqlTokenFixture) { f.policy.Keys["test-key"] = &rsa.PublicKey{E: 65537} }, ErrSQLTokenPolicyUnavailable},
		{"weak rsa modulus", func(f *sqlTokenFixture) { f.policy.Keys["test-key"] = &rsa.PublicKey{N: big.NewInt(17), E: 65537} }, ErrSQLTokenPolicyUnavailable},
		{"negative rsa modulus", func(f *sqlTokenFixture) {
			f.policy.Keys["test-key"] = &rsa.PublicKey{N: new(big.Int).Neg(pubKeys[0].N), E: 65537}
		}, ErrSQLTokenPolicyUnavailable},
		{"oversized rsa modulus", func(f *sqlTokenFixture) {
			f.policy.Keys["test-key"] = &rsa.PublicKey{N: new(big.Int).Lsh(big.NewInt(1), 8192), E: 65537}
		}, ErrSQLTokenPolicyUnavailable},
		{"even rsa modulus", func(f *sqlTokenFixture) {
			f.policy.Keys["test-key"] = &rsa.PublicKey{N: new(big.Int).Sub(pubKeys[0].N, big.NewInt(1)), E: 65537}
		}, ErrSQLTokenPolicyUnavailable},
		{"small rsa exponent", func(f *sqlTokenFixture) { f.policy.Keys["test-key"] = &rsa.PublicKey{N: pubKeys[0].N, E: 1} }, ErrSQLTokenPolicyUnavailable},
		{"even rsa exponent", func(f *sqlTokenFixture) { f.policy.Keys["test-key"] = &rsa.PublicKey{N: pubKeys[0].N, E: 65536} }, ErrSQLTokenPolicyUnavailable},
		{"large rsa exponent", func(f *sqlTokenFixture) { f.policy.Keys["test-key"] = &rsa.PublicKey{N: pubKeys[0].N, E: 1 << 31} }, ErrSQLTokenPolicyUnavailable},
		{"no bindings", func(f *sqlTokenFixture) { f.policy.Bindings = nil }, ErrSQLTokenAccessDenied},
		{"missing binding", func(f *sqlTokenFixture) { delete(f.policy.Bindings, "binding-test") }, ErrSQLTokenAccessDenied},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newSQLTokenFixture()
			tt.change(&f)
			requireSQLDenied(t, f.token(t), f, tt.err)
		})
	}
	bindings := []struct {
		name   string
		change func(*SQLTokenBinding)
	}{
		{"disabled", func(b *SQLTokenBinding) { b.Enabled = false }},
		{"expiry missing", func(b *SQLTokenBinding) { b.ExpiresAt = time.Time{} }},
		{"expiry boundary", func(b *SQLTokenBinding) { b.ExpiresAt = newSQLTokenFixture().now }},
		{"expiry outside supported range", func(b *SQLTokenBinding) {
			b.ExpiresAt = time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC)
		}},
		{"wrong principal", func(b *SQLTokenBinding) { b.PrincipalID = "other" }},
		{"missing principal", func(b *SQLTokenBinding) { b.PrincipalID = "" }},
		{"wrong username", func(b *SQLTokenBinding) { b.NativeUsername = "other" }},
		{"wrong host", func(b *SQLTokenBinding) { b.NativeHost = "localhost" }},
		{"old binding revision", func(b *SQLTokenBinding) { b.BindingRevision++ }},
		{"missing binding revision", func(b *SQLTokenBinding) { b.BindingRevision = 0 }},
		{"old policy revision", func(b *SQLTokenBinding) { b.PolicyRevision++ }},
		{"missing policy revision", func(b *SQLTokenBinding) { b.PolicyRevision = 0 }},
		{"grant not applied", func(b *SQLTokenBinding) { b.AppliedGrantRevision = 0 }},
		{"grant application behind desired", func(b *SQLTokenBinding) { b.DesiredGrantRevision++ }},
		{"grant application ahead of desired", func(b *SQLTokenBinding) { b.AppliedGrantRevision++ }},
		{"missing grant revision", func(b *SQLTokenBinding) { b.DesiredGrantRevision = 0; b.AppliedGrantRevision = 0 }},
	}
	for _, tt := range bindings {
		t.Run("binding "+tt.name, func(t *testing.T) {
			f := newSQLTokenFixture()
			f.editBinding(tt.change)
			requireSQLDenied(t, f.token(t), f, ErrSQLTokenAccessDenied)
		})
	}
}

func TestSQLTokenRejectsClaimsAndEncoding(t *testing.T) {
	claims := []struct {
		name  string
		field string
		value any
		err   error
	}{
		{"wrong version", SQLTokenClaimVersion, 2, ErrInvalidSQLToken},
		{"string version", SQLTokenClaimVersion, "1", ErrInvalidSQLToken},
		{"decimal version", SQLTokenClaimVersion, json.Number("1.0"), ErrInvalidSQLToken},
		{"wrong issuer", jwt.IssuerKey, "private-claim-canary", ErrSQLTokenAccessDenied},
		{"wrong audience", jwt.AudienceKey, "urn:example:database:other", ErrSQLTokenAccessDenied},
		{"wildcard audience", jwt.AudienceKey, "urn:example:database:*", ErrSQLTokenAccessDenied},
		{"audience array", jwt.AudienceKey, []string{"urn:example:database:orders"}, ErrInvalidSQLToken},
		{"wrong native user", jwt.SubjectKey, "other", ErrSQLTokenAccessDenied},
		{"principal is not native username", jwt.SubjectKey, "principal-test", ErrSQLTokenAccessDenied},
		{"wrong principal", SQLTokenClaimPrincipalID, "other", ErrSQLTokenAccessDenied},
		{"unknown binding", SQLTokenClaimBindingID, "other", ErrSQLTokenAccessDenied},
		{"old binding revision", SQLTokenClaimBindingRevision, "2", ErrSQLTokenAccessDenied},
		{"old policy revision", SQLTokenClaimPolicyRevision, "8", ErrSQLTokenAccessDenied},
		{"numeric revision", SQLTokenClaimBindingRevision, 3, ErrInvalidSQLToken},
		{"zero revision", SQLTokenClaimBindingRevision, "0", ErrInvalidSQLToken},
		{"leading zero revision", SQLTokenClaimBindingRevision, "03", ErrInvalidSQLToken},
		{"positive sign revision", SQLTokenClaimBindingRevision, "+3", ErrInvalidSQLToken},
		{"negative revision", SQLTokenClaimBindingRevision, "-3", ErrInvalidSQLToken},
		{"exponent revision", SQLTokenClaimBindingRevision, "3e0", ErrInvalidSQLToken},
		{"overflow revision", SQLTokenClaimBindingRevision, "18446744073709551616", ErrInvalidSQLToken},
		{"fraction timestamp", jwt.IssuedAtKey, json.Number("1793682000.5"), ErrInvalidSQLToken},
		{"exponent timestamp", jwt.IssuedAtKey, json.Number("1793682e3"), ErrInvalidSQLToken},
		{"string timestamp", jwt.IssuedAtKey, "1793682000", ErrInvalidSQLToken},
		{"negative timestamp", jwt.IssuedAtKey, -1, ErrInvalidSQLToken},
		{"negative zero timestamp", jwt.IssuedAtKey, json.Number("-0"), ErrInvalidSQLToken},
		{"overflow timestamp", jwt.ExpirationKey, json.Number("9223372036854775808"), ErrInvalidSQLToken},
		{"out of range timestamp", jwt.ExpirationKey, sqlTokenMaxUnixSeconds + 1, ErrInvalidSQLToken},
		{"far future timestamp", jwt.ExpirationKey, sqlTokenMaxUnixSeconds, ErrInvalidSQLToken},
		{"empty token id", jwt.JwtIDKey, "", ErrInvalidSQLToken},
		{"oversized token id", jwt.JwtIDKey, strings.Repeat("j", 257), ErrInvalidSQLToken},
		{"whitespace token id", jwt.JwtIDKey, "token id", ErrInvalidSQLToken},
		{"unicode token id", jwt.JwtIDKey, "token-é", ErrInvalidSQLToken},
		{"control token id", jwt.JwtIDKey, "token\x00id", ErrInvalidSQLToken},
		{"future issued-at", jwt.IssuedAtKey, int64(1793682001), ErrInvalidSQLToken},
		{"future not-before", jwt.NotBeforeKey, int64(1793682001), ErrInvalidSQLToken},
		{"issued-at follows not-before", jwt.NotBeforeKey, int64(1793681999), ErrInvalidSQLToken},
		{"expiry exclusive", jwt.ExpirationKey, int64(1793682000), ErrInvalidSQLToken},
		{"token lifetime exceeded", jwt.ExpirationKey, int64(1793682301), ErrInvalidSQLToken},
		{"platform organization is not a kernel claim", "organization_id", "org-example", ErrInvalidSQLToken},
		{"platform project is not a kernel claim", "project_id", "project-example", ErrInvalidSQLToken},
		{"platform principal kind is not a kernel claim", "principal_kind", "human", ErrInvalidSQLToken},
		{"unknown authorization claim", "roles", "admin", ErrInvalidSQLToken},
	}
	for _, tt := range claims {
		t.Run(tt.name, func(t *testing.T) {
			f := newSQLTokenFixture()
			f.claims[tt.field] = tt.value
			requireSQLDenied(t, f.token(t), f, tt.err)
		})
	}
	for name := range newSQLTokenFixture().claims {
		t.Run("missing claim "+name, func(t *testing.T) {
			f := newSQLTokenFixture()
			delete(f.claims, name)
			requireSQLDenied(t, f.token(t), f, ErrInvalidSQLToken)
		})
	}
	for _, value := range []any{nil, true, []any{}, map[string]any{}, json.Number("3")} {
		t.Run("invalid string claim type "+string(sqlJSON(t, value)), func(t *testing.T) {
			f := newSQLTokenFixture()
			f.claims[SQLTokenClaimPrincipalID] = value
			requireSQLDenied(t, f.token(t), f, ErrInvalidSQLToken)
		})
	}
	headers := []struct {
		name  string
		value any
	}{
		{jws.AlgorithmKey, "none"}, {jws.AlgorithmKey, "HS256"}, {jws.AlgorithmKey, "RS512"}, {jws.TypeKey, "JWT"}, {jws.TypeKey, 1},
		{jws.KeyIDKey, ""}, {jws.KeyIDKey, strings.Repeat("k", 257)}, {jws.KeyIDKey, "key name"}, {jws.KeyIDKey, nil},
		{"jku", "https://private-claim-canary.invalid/keys"}, {"x5u", "https://private-claim-canary.invalid/key"},
		{"jwk", map[string]any{"kty": "RSA"}}, {"crit", []string{"b64"}}, {"b64", false},
	}
	for _, tt := range headers {
		t.Run("header "+tt.name+" "+string(sqlJSON(t, tt.value)), func(t *testing.T) {
			f := newSQLTokenFixture()
			f.header[tt.name] = tt.value
			requireSQLDenied(t, f.token(t), f, ErrInvalidSQLToken)
		})
	}
	for _, name := range []string{jws.AlgorithmKey, jws.TypeKey, jws.KeyIDKey} {
		t.Run("missing header "+name, func(t *testing.T) {
			f := newSQLTokenFixture()
			delete(f.header, name)
			requireSQLDenied(t, f.token(t), f, ErrInvalidSQLToken)
		})
	}
	jsonCases := []struct {
		name    string
		header  func([]byte) []byte
		payload func([]byte) []byte
	}{
		{"duplicate header", func(b []byte) []byte { return append([]byte(`{"kid":"test-key",`), b[1:]...) }, nil},
		{"escaped equivalent header", func(b []byte) []byte { return append([]byte(`{"\u006bid":"test-key",`), b[1:]...) }, nil},
		{"trailing header object", func(b []byte) []byte { return append(b, []byte(` {}`)...) }, nil},
		{"header array", func([]byte) []byte { return []byte(`[]`) }, nil},
		{"duplicate claim", nil, func(b []byte) []byte { return append([]byte(`{"sub":"sql_user",`), b[1:]...) }},
		{"escaped equivalent claim", nil, func(b []byte) []byte { return append([]byte(`{"\u0073ub":"sql_user",`), b[1:]...) }},
		{"trailing payload object", nil, func(b []byte) []byte { return append(b, []byte(` {}`)...) }},
		{"payload array", nil, func([]byte) []byte { return []byte(`[]`) }},
		{"payload null", nil, func([]byte) []byte { return []byte(`null`) }},
		{"unknown instead of known claim", nil, func(b []byte) []byte { return []byte(strings.Replace(string(b), `"jti":`, `"unknown":`, 1)) }},
		{"invalid utf8", nil, func(b []byte) []byte { return []byte(strings.Replace(string(b), "token-test", "token-\xff", 1)) }},
		{"unpaired surrogate", nil, func(b []byte) []byte { return []byte(strings.Replace(string(b), "token-test", `token-\ud800`, 1)) }},
	}
	for _, tt := range jsonCases {
		t.Run(tt.name, func(t *testing.T) {
			f := newSQLTokenFixture()
			header, payload := sqlJSON(t, f.header), sqlJSON(t, f.claims)
			if tt.header != nil {
				header = tt.header(header)
			}
			if tt.payload != nil {
				payload = tt.payload(payload)
			}
			requireSQLDenied(t, signSQLJSON(t, header, payload), f, ErrInvalidSQLToken)
		})
	}
	compact := []struct {
		name   string
		change func([]byte) []byte
	}{
		{"empty", func([]byte) []byte { return nil }},
		{"oversized", func([]byte) []byte { return []byte(strings.Repeat("a", sqlTokenMaxBytes+1)) }},
		{"missing segment", func(b []byte) []byte { return b[:strings.LastIndexByte(string(b), '.')] }},
		{"extra segment", func(b []byte) []byte { return append(b, []byte(".extra")...) }},
		{"empty signature", func(b []byte) []byte { return b[:strings.LastIndexByte(string(b), '.')+1] }},
		{"leading whitespace", func(b []byte) []byte { return append([]byte(" "), b...) }},
		{"trailing newline", func(b []byte) []byte { return append(b, '\n') }},
		{"padding", func(b []byte) []byte { return append(b, '=') }},
		{"embedded newline", func(b []byte) []byte { return append(append([]byte{}, b[:10]...), append([]byte{'\n'}, b[10:]...)...) }},
		{"nonzero unused base64 bits", func(b []byte) []byte {
			alphabet := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
			index := strings.IndexByte(alphabet, b[len(b)-1])
			b[len(b)-1] = alphabet[index|1]
			return b
		}},
		{"bad signature", func(b []byte) []byte {
			index := strings.LastIndexByte(string(b), '.') + 1
			if b[index] == 'A' {
				b[index] = 'B'
			} else {
				b[index] = 'A'
			}
			return b
		}},
		{"JWS JSON serialization", func([]byte) []byte { return []byte(`{"payload":"e30","signatures":[]}`) }},
	}
	for _, tt := range compact {
		t.Run(tt.name, func(t *testing.T) {
			f := newSQLTokenFixture()
			requireSQLDenied(t, tt.change(f.token(t)), f, ErrInvalidSQLToken)
		})
	}
}
