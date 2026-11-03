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
	"bytes"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jws"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionauth"
)

const (
	// SQLTokenPlugin is a proposed, opt-in native authentication profile. This
	// verifier does not register it with the server or enable an authentication path.
	SQLTokenPlugin = "tidb_sql_token_v1"
	// SQLTokenType pins the protected JWT header to the native SQL profile.
	SQLTokenType = "tidb-sql+jwt"
	// SQLTokenVersion is the exact JSON number required by the v1 profile.
	SQLTokenVersion json.Number = "1"
	// SQLTokenClaimVersion identifies the fixed SQL profile version.
	SQLTokenClaimVersion = "ver"
	// SQLTokenClaimPrincipalID identifies the immutable issuer-scoped principal.
	SQLTokenClaimPrincipalID = "principal_id"
	// SQLTokenClaimBindingID selects an explicit native-account binding.
	SQLTokenClaimBindingID = "binding_id"
	// SQLTokenClaimBindingRevision requires an exact binding generation.
	SQLTokenClaimBindingRevision = "binding_revision"
	// SQLTokenClaimPolicyRevision requires an exact authorization generation.
	SQLTokenClaimPolicyRevision = "policy_revision"

	sqlTokenHeaderFields       = 3
	sqlTokenClaimFields        = 12
	sqlTokenMaxLifetime        = 15 * time.Minute
	sqlTokenMaxSessionDuration = 24 * time.Hour
	sqlTokenMaxBytes           = 16 * 1024
	sqlTokenMaxIDBytes         = 256
	sqlTokenMaxIssuerBytes     = 1024
	// Match the API-v2 codec's uint24 keyspace limit in the pinned TiKV client.
	sqlTokenMaxKeyspaceID = uint32(0xFFFFFF)
	// Keep conversion and duration comparisons within the supported civil range.
	sqlTokenMaxUnixSeconds = int64(253402300799)
)

var (
	// ErrInvalidSQLToken never includes incoming token or claim values.
	ErrInvalidSQLToken = errors.New("invalid SQL token")
	// ErrSQLTokenPolicyUnavailable covers absent, invalid, or expired trusted policy.
	ErrSQLTokenPolicyUnavailable = errors.New("SQL token policy unavailable")
	// ErrSQLTokenAccessDenied covers native-account, target, and binding mismatches.
	ErrSQLTokenAccessDenied = errors.New("SQL token access denied")
)

// SQLTokenRequest contains trusted storage and matched native-account facts.
// These fields must not come from token claims, connection attributes, or SQL SET.
// V1 audiences and native names are 1-256 printable ASCII bytes without whitespace.
// The caller attests the storage mode from the actual codec and normalizes cluster
// scope to KeyspaceID zero. Keyspace scope accepts API-v2 IDs [0, 0xFFFFFF]. The
// adapter must reject internal/reserved scopes by trusted name/classification and
// deployment policy; a numeric range alone cannot exclude SYSTEM. Neither the
// audience nor storage scope may be inferred from the token being verified.
type SQLTokenRequest struct {
	Target         sessionauth.Target
	NativeUsername string
	NativeHost     string
	NativePlugin   string
	AccountLocked  bool
	TLSEstablished bool
}

// SQLTokenBinding is an explicit authorization binding from an authenticated
// policy snapshot. Equal positive grant revisions mean native grants were applied;
// verification itself neither creates users nor grants SQL permissions.
// ExpiresAt is mandatory; a zero value does not represent an unlimited binding.
type SQLTokenBinding struct {
	PrincipalID          string
	NativeUsername       string
	NativeHost           string
	BindingRevision      uint64
	PolicyRevision       uint64
	Enabled              bool
	ExpiresAt            time.Time
	DesiredGrantRevision uint64
	AppliedGrantRevision uint64
}

// SQLTokenPolicy is one already-authenticated, immutable policy snapshot. Keys
// belong only to Issuer. The caller owns provenance, atomic installation, freshness,
// anti-rollback, and key-ID immutability, and must not mutate maps/keys during use.
// This value alone does not establish freshness after a restart.
// Issuer is 1-1024 printable ASCII bytes without whitespace. RSA keys require a
// positive odd 2048-8192-bit modulus and an odd exponent in [3, 2^31-1]. Maximum
// token lifetime is in (0, 15m], and maximum session duration is in (0, 24h].
// Trusted times and NumericDate claims must be within UTC years 1970-9999.
type SQLTokenPolicy struct {
	Target             sessionauth.Target
	Issuer             string
	SnapshotRevision   uint64
	ValidFrom          time.Time
	ValidUntil         time.Time
	MaxTokenLifetime   time.Duration
	MaxSessionDuration time.Duration
	Keys               map[string]*rsa.PublicKey
	Bindings           map[string]SQLTokenBinding
}

// VerifySQLToken checks the strict v1 signature, claims and trusted inputs. It
// has no network or mutable state and does not implement revocation, session guards,
// handshake integration, snapshot refresh, or SQL grants. All errors are fixed
// sentinels; parser/cryptography errors must not expose authentication material.
func VerifySQLToken(raw []byte, request SQLTokenRequest, policy SQLTokenPolicy, now time.Time) (sessionauth.Identity, error) {
	var zero sessionauth.Identity
	if !validSQLTarget(policy.Target) || !sqlTokenString(policy.Issuer, sqlTokenMaxIssuerBytes) ||
		policy.SnapshotRevision == 0 || !validSQLTime(now) || !validSQLTime(policy.ValidFrom) ||
		!validSQLTime(policy.ValidUntil) || !policy.ValidFrom.Before(policy.ValidUntil) ||
		now.Before(policy.ValidFrom) || !now.Before(policy.ValidUntil) ||
		policy.MaxTokenLifetime <= 0 || policy.MaxTokenLifetime > sqlTokenMaxLifetime ||
		policy.MaxSessionDuration <= 0 || policy.MaxSessionDuration > sqlTokenMaxSessionDuration {
		return zero, ErrSQLTokenPolicyUnavailable
	}
	if !validSQLTarget(request.Target) || request.Target != policy.Target ||
		!sqlTokenString(request.NativeUsername, sqlTokenMaxIDBytes) || !sqlTokenString(request.NativeHost, sqlTokenMaxIDBytes) ||
		request.NativePlugin != SQLTokenPlugin || request.AccountLocked || !request.TLSEstablished {
		return zero, ErrSQLTokenAccessDenied
	}
	header, payload, err := parseSQLCompact(raw)
	if err != nil {
		return zero, err
	}
	if len(header) != sqlTokenHeaderFields || header[jws.AlgorithmKey] != string(jwa.RS256) || header[jws.TypeKey] != SQLTokenType {
		return zero, ErrInvalidSQLToken
	}
	kid, ok := header[jws.KeyIDKey].(string)
	if !ok || !sqlTokenString(kid, sqlTokenMaxIDBytes) {
		return zero, ErrInvalidSQLToken
	}
	key := policy.Keys[kid]
	if !validSQLRSAKey(key) {
		return zero, ErrSQLTokenPolicyUnavailable
	}
	verified, err := jws.Verify(raw, jws.WithKey(jwa.RS256, key))
	if err != nil || !bytes.Equal(verified, payload) {
		return zero, ErrInvalidSQLToken
	}
	claims, err := decodeSQLObject(payload)
	if err != nil || len(claims) != sqlTokenClaimFields || claims[SQLTokenClaimVersion] != SQLTokenVersion {
		return zero, ErrInvalidSQLToken
	}
	stringLimits := map[string]int{
		jwt.IssuerKey: sqlTokenMaxIssuerBytes, jwt.AudienceKey: sqlTokenMaxIDBytes,
		jwt.SubjectKey: sqlTokenMaxIDBytes, SQLTokenClaimPrincipalID: sqlTokenMaxIDBytes,
		SQLTokenClaimBindingID: sqlTokenMaxIDBytes, jwt.JwtIDKey: sqlTokenMaxIDBytes,
	}
	for name, limit := range stringLimits {
		value, ok := claims[name].(string)
		if !ok || !sqlTokenString(value, limit) {
			return zero, ErrInvalidSQLToken
		}
	}
	bindingRevision, ok := sqlTokenRevision(claims[SQLTokenClaimBindingRevision])
	if !ok {
		return zero, ErrInvalidSQLToken
	}
	policyRevision, ok := sqlTokenRevision(claims[SQLTokenClaimPolicyRevision])
	if !ok {
		return zero, ErrInvalidSQLToken
	}
	iat, iatOK := sqlTokenNumericDate(claims[jwt.IssuedAtKey])
	nbf, nbfOK := sqlTokenNumericDate(claims[jwt.NotBeforeKey])
	exp, expOK := sqlTokenNumericDate(claims[jwt.ExpirationKey])
	if !iatOK || !nbfOK || !expOK || iat.After(nbf) || !nbf.Before(exp) ||
		iat.After(now) || nbf.After(now) || !now.Before(exp) || exp.Sub(iat) > policy.MaxTokenLifetime {
		return zero, ErrInvalidSQLToken
	}
	if claims[jwt.IssuerKey] != policy.Issuer || claims[jwt.AudienceKey] != policy.Target.Audience ||
		claims[jwt.SubjectKey] != request.NativeUsername {
		return zero, ErrSQLTokenAccessDenied
	}
	bindingID := claims[SQLTokenClaimBindingID].(string)
	binding, ok := policy.Bindings[bindingID]
	if !ok || !binding.Enabled || !validSQLTime(binding.ExpiresAt) || !now.Before(binding.ExpiresAt) ||
		!sqlTokenString(binding.PrincipalID, sqlTokenMaxIDBytes) ||
		binding.NativeUsername != request.NativeUsername || binding.NativeHost != request.NativeHost ||
		binding.BindingRevision == 0 || binding.BindingRevision != bindingRevision ||
		binding.PolicyRevision == 0 || binding.PolicyRevision != policyRevision ||
		binding.DesiredGrantRevision == 0 || binding.DesiredGrantRevision != binding.AppliedGrantRevision ||
		claims[SQLTokenClaimPrincipalID] != binding.PrincipalID {
		return zero, ErrSQLTokenAccessDenied
	}
	// Token expiry limits login, not session duration. These initial deadlines do
	// not authorize a caller to skip guard registration or later policy checks.
	sessionLifetime := min(policy.MaxSessionDuration, binding.ExpiresAt.Sub(now))
	return sessionauth.Identity{
		Target:         policy.Target,
		PrincipalID:    binding.PrincipalID,
		NativeUsername: request.NativeUsername, NativeHost: request.NativeHost,
		BindingID: bindingID, BindingRevision: bindingRevision, PolicyRevision: policyRevision,
		Issuer: policy.Issuer, KeyID: kid, Profile: SQLTokenPlugin, TokenID: claims[jwt.JwtIDKey].(string),
		VerificationSnapshotRevision: policy.SnapshotRevision, AuthenticatedAt: now,
		SessionDeadline:     now.Add(sessionLifetime),
		PolicyLeaseDeadline: now.Add(min(sessionLifetime, policy.ValidUntil.Sub(now))),
	}, nil
}

func validSQLTarget(target sessionauth.Target) bool {
	if !sqlTokenString(target.Audience, sqlTokenMaxIDBytes) || target.StorageClusterID == 0 {
		return false
	}
	switch target.StorageScope {
	case sessionauth.StorageScopeCluster:
		return target.KeyspaceID == 0
	case sessionauth.StorageScopeKeyspace:
		return target.KeyspaceID <= sqlTokenMaxKeyspaceID
	default:
		return false
	}
}

// V1 identifiers are byte-exact printable ASCII, with no whitespace or normalization.
func sqlTokenString(value string, limit int) bool {
	if len(value) == 0 || len(value) > limit {
		return false
	}
	for i := range len(value) {
		if value[i] < '!' || value[i] > '~' {
			return false
		}
	}
	return true
}

func validSQLTime(value time.Time) bool {
	year := value.UTC().Year()
	return year >= 1970 && year <= 9999
}

func validSQLRSAKey(key *rsa.PublicKey) bool {
	return key != nil && key.N != nil && key.N.Sign() > 0 && key.N.BitLen() >= 2048 && key.N.BitLen() <= 8192 &&
		key.N.Bit(0) == 1 && key.E >= 3 && key.E <= 1<<31-1 && key.E%2 == 1
}

func sqlTokenRevision(value any) (uint64, bool) {
	text, ok := value.(string)
	if !ok || len(text) == 0 || len(text) > 20 || text[0] == '0' {
		return 0, false
	}
	revision, err := strconv.ParseUint(text, 10, 64)
	return revision, err == nil && revision > 0 && strconv.FormatUint(revision, 10) == text
}

func sqlTokenNumericDate(value any) (time.Time, bool) {
	number, ok := value.(json.Number)
	if !ok {
		return time.Time{}, false
	}
	seconds, err := strconv.ParseInt(string(number), 10, 64)
	if err != nil || seconds < 0 || seconds > sqlTokenMaxUnixSeconds || strconv.FormatInt(seconds, 10) != string(number) {
		return time.Time{}, false
	}
	return time.Unix(seconds, 0), true
}

func parseSQLCompact(raw []byte) (map[string]any, []byte, error) {
	if len(raw) == 0 || len(raw) > sqlTokenMaxBytes {
		return nil, nil, ErrInvalidSQLToken
	}
	parts := strings.Split(string(raw), ".")
	if len(parts) != 3 {
		return nil, nil, ErrInvalidSQLToken
	}
	decoded := make([][]byte, 3)
	for i, part := range parts {
		value, err := base64.RawURLEncoding.Strict().DecodeString(part)
		if err != nil || len(value) == 0 || base64.RawURLEncoding.EncodeToString(value) != part {
			return nil, nil, ErrInvalidSQLToken
		}
		decoded[i] = value
	}
	header, err := decodeSQLObject(decoded[0])
	return header, decoded[1], err
}

// Decode primitives one field at a time so duplicate keys (including escaped
// equivalents) and nesting cannot be hidden by encoding/json's struct/map decoding.
func decodeSQLObject(raw []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	open, err := decoder.Token()
	if err != nil || open != json.Delim('{') {
		return nil, ErrInvalidSQLToken
	}
	result := make(map[string]any)
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return nil, ErrInvalidSQLToken
		}
		name, ok := key.(string)
		if !ok {
			return nil, ErrInvalidSQLToken
		}
		if _, exists := result[name]; exists {
			return nil, ErrInvalidSQLToken
		}
		value, err := decoder.Token()
		if err != nil {
			return nil, ErrInvalidSQLToken
		}
		switch value.(type) {
		case string, json.Number:
			result[name] = value
		default:
			return nil, ErrInvalidSQLToken
		}
	}
	closeToken, err := decoder.Token()
	if err != nil || closeToken != json.Delim('}') {
		return nil, ErrInvalidSQLToken
	}
	if _, err = decoder.Token(); err != io.EOF {
		return nil, ErrInvalidSQLToken
	}
	return result, nil
}
