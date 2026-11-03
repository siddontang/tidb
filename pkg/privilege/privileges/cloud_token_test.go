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
	"github.com/pingcap/tidb/pkg/sessionctx/sessionauth"
	"github.com/stretchr/testify/require"
)

type cloudTokenFixture struct {
	now     time.Time
	request CloudTokenRequest
	policy  CloudTokenPolicy
	header  map[string]any
	claims  map[string]any
}

func newCloudTokenFixture() cloudTokenFixture {
	now := time.Unix(1793682000, 0)
	target := sessionauth.Target{InstanceID: "inst-owned-test", OrganizationID: "org-test", ProjectID: "project-test", StorageClusterID: 101, KeyspaceID: 42}
	return cloudTokenFixture{
		now:     now,
		request: CloudTokenRequest{Target: target, NativeUsername: "cloud_user", NativeHost: "%", NativePlugin: CloudTokenPlugin, TLSEstablished: true},
		policy: CloudTokenPolicy{
			Target: target, Issuer: "https://issuer.invalid/region-test", SnapshotRevision: 7,
			ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(30 * time.Second),
			MaxTokenLifetime: 5 * time.Minute, MaxSessionDuration: time.Hour,
			Keys: map[string]*rsa.PublicKey{"test-key": pubKeys[0]},
			Bindings: map[string]CloudTokenBinding{"binding-test": {
				PrincipalID: "principal-test", PrincipalKind: "human", NativeUsername: "cloud_user", NativeHost: "%",
				BindingRevision: 3, PolicyRevision: 9, Enabled: true, ExpiresAt: now.Add(2 * time.Hour),
				DesiredGrantRevision: 4, AppliedGrantRevision: 4,
			}},
		},
		header: map[string]any{"alg": "RS256", "typ": CloudTokenType, "kid": "test-key"},
		claims: map[string]any{
			"ver": 1, "iss": "https://issuer.invalid/region-test", "aud": "tidb-cloud:instance:inst-owned-test", "sub": "cloud_user",
			"principal_id": "principal-test", "principal_kind": "human", "organization_id": "org-test", "project_id": "project-test",
			"binding_id": "binding-test", "binding_revision": "3", "policy_revision": "9",
			"iat": now.Unix(), "nbf": now.Unix(), "exp": now.Add(5 * time.Minute).Unix(), "jti": "token-test",
		},
	}
}

func (f *cloudTokenFixture) editBinding(change func(*CloudTokenBinding)) {
	binding := f.policy.Bindings["binding-test"]
	change(&binding)
	f.policy.Bindings["binding-test"] = binding
}

func cloudJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	require.NoError(t, err)
	return raw
}

// The existing package fixtures are synthetic keys. Sign raw JSON through jwx
// so malformed/duplicate-field cases still have a correct cryptographic signature.
func signCloudJSON(t *testing.T, header, payload []byte) []byte {
	t.Helper()
	input := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	signer, err := jws.NewSigner(jwa.RS256)
	require.NoError(t, err)
	signature, err := signer.Sign([]byte(input), priKeys[0])
	require.NoError(t, err)
	return []byte(input + "." + base64.RawURLEncoding.EncodeToString(signature))
}

func (f cloudTokenFixture) token(t *testing.T) []byte {
	t.Helper()
	return signCloudJSON(t, cloudJSON(t, f.header), cloudJSON(t, f.claims))
}

func requireCloudDenied(t *testing.T, raw []byte, f cloudTokenFixture, expected error) {
	t.Helper()
	identity, err := VerifyCloudToken(raw, f.request, f.policy, f.now)
	require.Equal(t, expected, err)
	require.Equal(t, sessionauth.Identity{}, identity)
	// Compare the fixed sentinel as well as checking that neither raw credentials
	// nor a representative private claim value appears in the error surface.
	if len(raw) != 0 {
		require.NotContains(t, err.Error(), string(raw))
	}
	require.NotContains(t, err.Error(), "private-claim-canary")
}

func TestCloudTokenIdentity(t *testing.T) {
	f := newCloudTokenFixture()
	raw := f.token(t)
	identity, err := VerifyCloudToken(raw, f.request, f.policy, f.now)
	require.NoError(t, err)
	require.Equal(t, sessionauth.Identity{
		Target: f.policy.Target, PrincipalID: "principal-test", PrincipalKind: "human",
		NativeUsername: "cloud_user", NativeHost: "%", BindingID: "binding-test", BindingRevision: 3, PolicyRevision: 9,
		Issuer: f.policy.Issuer, KeyID: "test-key", Profile: CloudTokenPlugin, TokenID: "token-test",
		VerificationSnapshotRevision: 7, AuthenticatedAt: f.now,
		SessionDeadline: f.now.Add(time.Hour), PolicyLeaseDeadline: f.now.Add(30 * time.Second),
	}, identity)
	// Session lifetime is deliberately independent of the five-minute login token.
	require.True(t, identity.SessionDeadline.After(time.Unix(f.claims["exp"].(int64), 0)))
	saved := identity
	f.policy.Bindings["binding-test"] = CloudTokenBinding{}
	f.policy.Keys["test-key"] = pubKeys[1]
	for i := range raw {
		raw[i] = 0
	}
	require.Equal(t, saved, identity)

	tests := []struct {
		name    string
		change  func(*cloudTokenFixture)
		session time.Duration
		lease   time.Duration
	}{
		{"binding caps both deadlines", func(f *cloudTokenFixture) {
			f.editBinding(func(b *CloudTokenBinding) { b.ExpiresAt = f.now.Add(10 * time.Second) })
		}, 10 * time.Second, 10 * time.Second},
		{"session caps lease", func(f *cloudTokenFixture) { f.policy.MaxSessionDuration = time.Second }, time.Second, time.Second},
		{"policy valid-from is inclusive", func(f *cloudTokenFixture) { f.policy.ValidFrom = f.now }, time.Hour, 30 * time.Second},
		{"fractional receiver clock", func(f *cloudTokenFixture) { f.now = f.now.Add(time.Nanosecond) }, time.Hour, 30*time.Second - time.Nanosecond},
		{"not-before may follow issuance", func(f *cloudTokenFixture) {
			f.claims["iat"] = f.now.Add(-time.Second).Unix()
			f.claims["exp"] = f.now.Add(time.Minute).Unix()
		}, time.Hour, 30 * time.Second},
		{"workload principal", func(f *cloudTokenFixture) {
			f.claims["principal_kind"] = "workload"
			f.editBinding(func(b *CloudTokenBinding) { b.PrincipalKind = "workload" })
		}, time.Hour, 30 * time.Second},
		{"uint64 revisions preserve precision", func(f *cloudTokenFixture) {
			f.claims["binding_revision"], f.claims["policy_revision"] = "18446744073709551615", "9007199254740993"
			f.editBinding(func(b *CloudTokenBinding) { b.BindingRevision = math.MaxUint64; b.PolicyRevision = 9007199254740993 })
		}, time.Hour, 30 * time.Second},
		{"maximum configured durations", func(f *cloudTokenFixture) {
			f.policy.MaxTokenLifetime, f.policy.MaxSessionDuration = 15*time.Minute, 24*time.Hour
			f.claims["exp"] = f.now.Add(15 * time.Minute).Unix()
			f.editBinding(func(b *CloudTokenBinding) { b.ExpiresAt = f.now.Add(48 * time.Hour) })
		}, 24 * time.Hour, 30 * time.Second},
		{"identifier maximum length", func(f *cloudTokenFixture) {
			f.request.Target.InstanceID = strings.Repeat("i", cloudTokenMaxIDBytes)
			f.policy.Target = f.request.Target
			f.claims["aud"] = "tidb-cloud:instance:" + f.policy.Target.InstanceID
		}, time.Hour, 30 * time.Second},
		{"minimum non-default keyspace number", func(f *cloudTokenFixture) {
			f.request.Target.KeyspaceID = 1
			f.policy.Target = f.request.Target
		}, time.Hour, 30 * time.Second},
		{"maximum API-v2 keyspace number", func(f *cloudTokenFixture) {
			f.request.Target.KeyspaceID = 0xFFFFFF
			f.policy.Target = f.request.Target
		}, time.Hour, 30 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newCloudTokenFixture()
			tt.change(&f)
			identity, err := VerifyCloudToken(f.token(t), f.request, f.policy, f.now)
			require.NoError(t, err)
			require.Equal(t, f.now.Add(tt.session), identity.SessionDeadline)
			require.Equal(t, f.now.Add(tt.lease), identity.PolicyLeaseDeadline)
			require.Equal(t, f.policy.Target, identity.Target)
			require.Equal(t, f.claims["principal_kind"], identity.PrincipalKind)
		})
	}
}

func TestCloudTokenRejectsUntrustedInputs(t *testing.T) {
	tests := []struct {
		name   string
		change func(*cloudTokenFixture)
		err    error
	}{
		{"different actual instance", func(f *cloudTokenFixture) { f.request.Target.InstanceID = "inst-other" }, ErrCloudTokenAccessDenied},
		{"same keyspace in different storage cluster", func(f *cloudTokenFixture) { f.request.Target.StorageClusterID++ }, ErrCloudTokenAccessDenied},
		{"different actual keyspace", func(f *cloudTokenFixture) { f.request.Target.KeyspaceID++ }, ErrCloudTokenAccessDenied},
		{"different actual organization", func(f *cloudTokenFixture) { f.request.Target.OrganizationID = "org-other" }, ErrCloudTokenAccessDenied},
		{"different actual project", func(f *cloudTokenFixture) { f.request.Target.ProjectID = "project-other" }, ErrCloudTokenAccessDenied},
		{"missing actual descriptor", func(f *cloudTokenFixture) { f.request.Target = sessionauth.Target{} }, ErrCloudTokenAccessDenied},
		{"missing trusted descriptor", func(f *cloudTokenFixture) { f.policy.Target = sessionauth.Target{} }, ErrCloudTokenPolicyUnavailable},
		{"default keyspace", func(f *cloudTokenFixture) { f.request.Target.KeyspaceID = 0; f.policy.Target = f.request.Target }, ErrCloudTokenPolicyUnavailable},
		{"keyspace above API-v2 range", func(f *cloudTokenFixture) {
			f.request.Target.KeyspaceID = 0x1000000
			f.policy.Target = f.request.Target
		}, ErrCloudTokenPolicyUnavailable},
		{"null keyspace", func(f *cloudTokenFixture) {
			f.request.Target.KeyspaceID = 0xFFFFFFFF
			f.policy.Target = f.request.Target
		}, ErrCloudTokenPolicyUnavailable},
		{"unknown storage cluster", func(f *cloudTokenFixture) { f.request.Target.StorageClusterID = 0; f.policy.Target = f.request.Target }, ErrCloudTokenPolicyUnavailable},
		{"noncanonical target", func(f *cloudTokenFixture) { f.policy.Target.InstanceID = "inst test" }, ErrCloudTokenPolicyUnavailable},
		{"oversized target", func(f *cloudTokenFixture) { f.policy.Target.InstanceID = strings.Repeat("i", 257) }, ErrCloudTokenPolicyUnavailable},
		{"wrong native username", func(f *cloudTokenFixture) { f.request.NativeUsername = "other" }, ErrCloudTokenAccessDenied},
		{"wrong matched host", func(f *cloudTokenFixture) { f.request.NativeHost = "localhost" }, ErrCloudTokenAccessDenied},
		{"missing native account", func(f *cloudTokenFixture) { f.request.NativeUsername = "" }, ErrCloudTokenAccessDenied},
		{"missing matched host", func(f *cloudTokenFixture) { f.request.NativeHost = "" }, ErrCloudTokenAccessDenied},
		{"legacy auth plugin", func(f *cloudTokenFixture) { f.request.NativePlugin = "tidb_auth_token" }, ErrCloudTokenAccessDenied},
		{"password auth plugin", func(f *cloudTokenFixture) { f.request.NativePlugin = "mysql_native_password" }, ErrCloudTokenAccessDenied},
		{"locked native account", func(f *cloudTokenFixture) { f.request.AccountLocked = true }, ErrCloudTokenAccessDenied},
		{"plaintext transport", func(f *cloudTokenFixture) { f.request.TLSEstablished = false }, ErrCloudTokenAccessDenied},
		{"missing issuer", func(f *cloudTokenFixture) { f.policy.Issuer = "" }, ErrCloudTokenPolicyUnavailable},
		{"oversized issuer", func(f *cloudTokenFixture) { f.policy.Issuer = strings.Repeat("i", 1025) }, ErrCloudTokenPolicyUnavailable},
		{"missing snapshot revision", func(f *cloudTokenFixture) { f.policy.SnapshotRevision = 0 }, ErrCloudTokenPolicyUnavailable},
		{"unknown clock", func(f *cloudTokenFixture) { f.now = time.Time{} }, ErrCloudTokenPolicyUnavailable},
		{"clock before supported range", func(f *cloudTokenFixture) { f.now = time.Unix(-1, 0) }, ErrCloudTokenPolicyUnavailable},
		{"clock beyond supported range", func(f *cloudTokenFixture) {
			f.now = time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC)
		}, ErrCloudTokenPolicyUnavailable},
		{"missing policy start", func(f *cloudTokenFixture) { f.policy.ValidFrom = time.Time{} }, ErrCloudTokenPolicyUnavailable},
		{"missing policy expiry", func(f *cloudTokenFixture) { f.policy.ValidUntil = time.Time{} }, ErrCloudTokenPolicyUnavailable},
		{"policy start before UTC epoch despite local year", func(f *cloudTokenFixture) {
			f.policy.ValidFrom = time.Date(1970, time.January, 1, 0, 0, 0, 0, time.FixedZone("test-east", 3600))
		}, ErrCloudTokenPolicyUnavailable},
		{"policy expiry beyond UTC range despite local year", func(f *cloudTokenFixture) {
			f.policy.ValidUntil = time.Date(9999, time.December, 31, 23, 59, 59, 0, time.FixedZone("test-west", -3600))
		}, ErrCloudTokenPolicyUnavailable},
		{"future snapshot", func(f *cloudTokenFixture) { f.policy.ValidFrom = f.now.Add(time.Nanosecond) }, ErrCloudTokenPolicyUnavailable},
		{"expired snapshot boundary", func(f *cloudTokenFixture) { f.policy.ValidUntil = f.now }, ErrCloudTokenPolicyUnavailable},
		{"inverted snapshot interval", func(f *cloudTokenFixture) { f.policy.ValidFrom = f.policy.ValidUntil }, ErrCloudTokenPolicyUnavailable},
		{"zero token lifetime", func(f *cloudTokenFixture) { f.policy.MaxTokenLifetime = 0 }, ErrCloudTokenPolicyUnavailable},
		{"excessive token lifetime", func(f *cloudTokenFixture) { f.policy.MaxTokenLifetime = 15*time.Minute + time.Nanosecond }, ErrCloudTokenPolicyUnavailable},
		{"negative session duration", func(f *cloudTokenFixture) { f.policy.MaxSessionDuration = -1 }, ErrCloudTokenPolicyUnavailable},
		{"excessive session duration", func(f *cloudTokenFixture) { f.policy.MaxSessionDuration = 24*time.Hour + time.Nanosecond }, ErrCloudTokenPolicyUnavailable},
		{"no keys", func(f *cloudTokenFixture) { f.policy.Keys = nil }, ErrCloudTokenPolicyUnavailable},
		{"unknown signing key", func(f *cloudTokenFixture) { f.header["kid"] = "different-key" }, ErrCloudTokenPolicyUnavailable},
		{"issuer scoped keys", func(f *cloudTokenFixture) { f.policy.Issuer = "other-issuer" }, ErrCloudTokenAccessDenied},
		{"wrong signature", func(f *cloudTokenFixture) { f.policy.Keys["test-key"] = pubKeys[1] }, ErrInvalidCloudToken},
		{"nil rsa key", func(f *cloudTokenFixture) { f.policy.Keys["test-key"] = nil }, ErrCloudTokenPolicyUnavailable},
		{"nil rsa modulus", func(f *cloudTokenFixture) { f.policy.Keys["test-key"] = &rsa.PublicKey{E: 65537} }, ErrCloudTokenPolicyUnavailable},
		{"weak rsa modulus", func(f *cloudTokenFixture) { f.policy.Keys["test-key"] = &rsa.PublicKey{N: big.NewInt(17), E: 65537} }, ErrCloudTokenPolicyUnavailable},
		{"negative rsa modulus", func(f *cloudTokenFixture) {
			f.policy.Keys["test-key"] = &rsa.PublicKey{N: new(big.Int).Neg(pubKeys[0].N), E: 65537}
		}, ErrCloudTokenPolicyUnavailable},
		{"oversized rsa modulus", func(f *cloudTokenFixture) {
			f.policy.Keys["test-key"] = &rsa.PublicKey{N: new(big.Int).Lsh(big.NewInt(1), 8192), E: 65537}
		}, ErrCloudTokenPolicyUnavailable},
		{"even rsa modulus", func(f *cloudTokenFixture) {
			f.policy.Keys["test-key"] = &rsa.PublicKey{N: new(big.Int).Sub(pubKeys[0].N, big.NewInt(1)), E: 65537}
		}, ErrCloudTokenPolicyUnavailable},
		{"small rsa exponent", func(f *cloudTokenFixture) { f.policy.Keys["test-key"] = &rsa.PublicKey{N: pubKeys[0].N, E: 1} }, ErrCloudTokenPolicyUnavailable},
		{"even rsa exponent", func(f *cloudTokenFixture) { f.policy.Keys["test-key"] = &rsa.PublicKey{N: pubKeys[0].N, E: 65536} }, ErrCloudTokenPolicyUnavailable},
		{"large rsa exponent", func(f *cloudTokenFixture) { f.policy.Keys["test-key"] = &rsa.PublicKey{N: pubKeys[0].N, E: 1 << 31} }, ErrCloudTokenPolicyUnavailable},
		{"no bindings", func(f *cloudTokenFixture) { f.policy.Bindings = nil }, ErrCloudTokenAccessDenied},
		{"missing binding", func(f *cloudTokenFixture) { delete(f.policy.Bindings, "binding-test") }, ErrCloudTokenAccessDenied},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newCloudTokenFixture()
			tt.change(&f)
			requireCloudDenied(t, f.token(t), f, tt.err)
		})
	}
	bindings := []struct {
		name   string
		change func(*CloudTokenBinding)
	}{
		{"disabled", func(b *CloudTokenBinding) { b.Enabled = false }},
		{"expiry missing", func(b *CloudTokenBinding) { b.ExpiresAt = time.Time{} }},
		{"expiry boundary", func(b *CloudTokenBinding) { b.ExpiresAt = newCloudTokenFixture().now }},
		{"expiry outside supported range", func(b *CloudTokenBinding) {
			b.ExpiresAt = time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC)
		}},
		{"wrong principal", func(b *CloudTokenBinding) { b.PrincipalID = "other" }},
		{"missing principal", func(b *CloudTokenBinding) { b.PrincipalID = "" }},
		{"wrong kind", func(b *CloudTokenBinding) { b.PrincipalKind = "workload" }},
		{"unknown kind", func(b *CloudTokenBinding) { b.PrincipalKind = "robot" }},
		{"wrong username", func(b *CloudTokenBinding) { b.NativeUsername = "other" }},
		{"wrong host", func(b *CloudTokenBinding) { b.NativeHost = "localhost" }},
		{"old binding revision", func(b *CloudTokenBinding) { b.BindingRevision++ }},
		{"missing binding revision", func(b *CloudTokenBinding) { b.BindingRevision = 0 }},
		{"old policy revision", func(b *CloudTokenBinding) { b.PolicyRevision++ }},
		{"missing policy revision", func(b *CloudTokenBinding) { b.PolicyRevision = 0 }},
		{"grant not applied", func(b *CloudTokenBinding) { b.AppliedGrantRevision = 0 }},
		{"grant application behind desired", func(b *CloudTokenBinding) { b.DesiredGrantRevision++ }},
		{"grant application ahead of desired", func(b *CloudTokenBinding) { b.AppliedGrantRevision++ }},
		{"missing grant revision", func(b *CloudTokenBinding) { b.DesiredGrantRevision = 0; b.AppliedGrantRevision = 0 }},
	}
	for _, tt := range bindings {
		t.Run("binding "+tt.name, func(t *testing.T) {
			f := newCloudTokenFixture()
			f.editBinding(tt.change)
			requireCloudDenied(t, f.token(t), f, ErrCloudTokenAccessDenied)
		})
	}
}

func TestCloudTokenRejectsClaimsAndEncoding(t *testing.T) {
	claims := []struct {
		name  string
		field string
		value any
		err   error
	}{
		{"wrong version", "ver", 2, ErrInvalidCloudToken},
		{"string version", "ver", "1", ErrInvalidCloudToken},
		{"decimal version", "ver", json.Number("1.0"), ErrInvalidCloudToken},
		{"wrong issuer", "iss", "private-claim-canary", ErrCloudTokenAccessDenied},
		{"wrong instance", "aud", "tidb-cloud:instance:inst-other", ErrCloudTokenAccessDenied},
		{"wildcard instance", "aud", "tidb-cloud:instance:*", ErrCloudTokenAccessDenied},
		{"audience array", "aud", []string{"tidb-cloud:instance:inst-owned-test"}, ErrInvalidCloudToken},
		{"wrong native user", "sub", "other", ErrCloudTokenAccessDenied},
		{"principal is not native username", "sub", "principal-test", ErrCloudTokenAccessDenied},
		{"wrong principal", "principal_id", "other", ErrCloudTokenAccessDenied},
		{"wrong principal kind", "principal_kind", "workload", ErrCloudTokenAccessDenied},
		{"unknown principal kind", "principal_kind", "robot", ErrCloudTokenAccessDenied},
		{"wrong org", "organization_id", "other", ErrCloudTokenAccessDenied},
		{"wrong project", "project_id", "other", ErrCloudTokenAccessDenied},
		{"unknown binding", "binding_id", "other", ErrCloudTokenAccessDenied},
		{"old binding revision", "binding_revision", "2", ErrCloudTokenAccessDenied},
		{"old policy revision", "policy_revision", "8", ErrCloudTokenAccessDenied},
		{"numeric revision", "binding_revision", 3, ErrInvalidCloudToken},
		{"zero revision", "binding_revision", "0", ErrInvalidCloudToken},
		{"leading zero revision", "binding_revision", "03", ErrInvalidCloudToken},
		{"positive sign revision", "binding_revision", "+3", ErrInvalidCloudToken},
		{"negative revision", "binding_revision", "-3", ErrInvalidCloudToken},
		{"exponent revision", "binding_revision", "3e0", ErrInvalidCloudToken},
		{"overflow revision", "binding_revision", "18446744073709551616", ErrInvalidCloudToken},
		{"fraction timestamp", "iat", json.Number("1793682000.5"), ErrInvalidCloudToken},
		{"exponent timestamp", "iat", json.Number("1793682e3"), ErrInvalidCloudToken},
		{"string timestamp", "iat", "1793682000", ErrInvalidCloudToken},
		{"negative timestamp", "iat", -1, ErrInvalidCloudToken},
		{"negative zero timestamp", "iat", json.Number("-0"), ErrInvalidCloudToken},
		{"overflow timestamp", "exp", json.Number("9223372036854775808"), ErrInvalidCloudToken},
		{"out of range timestamp", "exp", cloudTokenMaxUnixSeconds + 1, ErrInvalidCloudToken},
		{"far future timestamp", "exp", cloudTokenMaxUnixSeconds, ErrInvalidCloudToken},
		{"empty token id", "jti", "", ErrInvalidCloudToken},
		{"oversized token id", "jti", strings.Repeat("j", 257), ErrInvalidCloudToken},
		{"whitespace token id", "jti", "token id", ErrInvalidCloudToken},
		{"unicode token id", "jti", "token-é", ErrInvalidCloudToken},
		{"control token id", "jti", "token\x00id", ErrInvalidCloudToken},
		{"future issued-at", "iat", int64(1793682001), ErrInvalidCloudToken},
		{"future not-before", "nbf", int64(1793682001), ErrInvalidCloudToken},
		{"issued-at follows not-before", "nbf", int64(1793681999), ErrInvalidCloudToken},
		{"expiry exclusive", "exp", int64(1793682000), ErrInvalidCloudToken},
		{"token lifetime exceeded", "exp", int64(1793682301), ErrInvalidCloudToken},
		{"unknown authorization claim", "roles", "admin", ErrInvalidCloudToken},
	}
	for _, tt := range claims {
		t.Run(tt.name, func(t *testing.T) {
			f := newCloudTokenFixture()
			f.claims[tt.field] = tt.value
			requireCloudDenied(t, f.token(t), f, tt.err)
		})
	}
	for name := range newCloudTokenFixture().claims {
		t.Run("missing claim "+name, func(t *testing.T) {
			f := newCloudTokenFixture()
			delete(f.claims, name)
			requireCloudDenied(t, f.token(t), f, ErrInvalidCloudToken)
		})
	}
	for _, value := range []any{nil, true, []any{}, map[string]any{}, json.Number("3")} {
		t.Run("invalid string claim type "+string(cloudJSON(t, value)), func(t *testing.T) {
			f := newCloudTokenFixture()
			f.claims["principal_id"] = value
			requireCloudDenied(t, f.token(t), f, ErrInvalidCloudToken)
		})
	}
	headers := []struct {
		name  string
		value any
	}{
		{"alg", "none"}, {"alg", "HS256"}, {"alg", "RS512"}, {"typ", "JWT"}, {"typ", 1},
		{"kid", ""}, {"kid", strings.Repeat("k", 257)}, {"kid", "key name"}, {"kid", nil},
		{"jku", "https://private-claim-canary.invalid/keys"}, {"x5u", "https://private-claim-canary.invalid/key"},
		{"jwk", map[string]any{"kty": "RSA"}}, {"crit", []string{"b64"}}, {"b64", false},
	}
	for _, tt := range headers {
		t.Run("header "+tt.name+" "+string(cloudJSON(t, tt.value)), func(t *testing.T) {
			f := newCloudTokenFixture()
			f.header[tt.name] = tt.value
			requireCloudDenied(t, f.token(t), f, ErrInvalidCloudToken)
		})
	}
	for _, name := range []string{"alg", "typ", "kid"} {
		t.Run("missing header "+name, func(t *testing.T) {
			f := newCloudTokenFixture()
			delete(f.header, name)
			requireCloudDenied(t, f.token(t), f, ErrInvalidCloudToken)
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
		{"duplicate claim", nil, func(b []byte) []byte { return append([]byte(`{"sub":"cloud_user",`), b[1:]...) }},
		{"escaped equivalent claim", nil, func(b []byte) []byte { return append([]byte(`{"\u0073ub":"cloud_user",`), b[1:]...) }},
		{"trailing payload object", nil, func(b []byte) []byte { return append(b, []byte(` {}`)...) }},
		{"payload array", nil, func([]byte) []byte { return []byte(`[]`) }},
		{"payload null", nil, func([]byte) []byte { return []byte(`null`) }},
		{"unknown instead of known claim", nil, func(b []byte) []byte { return []byte(strings.Replace(string(b), `"jti":`, `"unknown":`, 1)) }},
		{"invalid utf8", nil, func(b []byte) []byte { return []byte(strings.Replace(string(b), "token-test", "token-\xff", 1)) }},
		{"unpaired surrogate", nil, func(b []byte) []byte { return []byte(strings.Replace(string(b), "token-test", `token-\ud800`, 1)) }},
	}
	for _, tt := range jsonCases {
		t.Run(tt.name, func(t *testing.T) {
			f := newCloudTokenFixture()
			header, payload := cloudJSON(t, f.header), cloudJSON(t, f.claims)
			if tt.header != nil {
				header = tt.header(header)
			}
			if tt.payload != nil {
				payload = tt.payload(payload)
			}
			requireCloudDenied(t, signCloudJSON(t, header, payload), f, ErrInvalidCloudToken)
		})
	}
	compact := []struct {
		name   string
		change func([]byte) []byte
	}{
		{"empty", func([]byte) []byte { return nil }},
		{"oversized", func([]byte) []byte { return []byte(strings.Repeat("a", cloudTokenMaxBytes+1)) }},
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
			f := newCloudTokenFixture()
			requireCloudDenied(t, tt.change(f.token(t)), f, ErrInvalidCloudToken)
		})
	}
}
