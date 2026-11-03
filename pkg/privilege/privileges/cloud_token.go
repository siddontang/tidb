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
	"github.com/pingcap/tidb/pkg/sessionctx/sessionauth"
)

const (
	// CloudTokenPlugin is a proposed, opt-in native authentication profile. This
	// verifier does not register it with the server or enable an authentication path.
	CloudTokenPlugin = "tidb_cloud_token_v1"
	// CloudTokenType pins the protected JWT header to the Cloud SQL profile.
	CloudTokenType           = "tidb-cloud-sql+jwt"
	cloudTokenMaxBytes       = 16 * 1024
	cloudTokenMaxIDBytes     = 256
	cloudTokenMaxIssuerBytes = 1024
	// Match the API-v2 codec's uint24 keyspace limit in the pinned TiKV client.
	cloudTokenMaxKeyspaceID = uint32(0xFFFFFF)
	// Keep conversion and duration comparisons within the supported civil range.
	cloudTokenMaxUnixSeconds = int64(253402300799)
)

var (
	// ErrInvalidCloudToken never includes incoming token or claim values.
	ErrInvalidCloudToken = errors.New("invalid cloud SQL token")
	// ErrCloudTokenPolicyUnavailable covers absent, invalid, or expired trusted policy.
	ErrCloudTokenPolicyUnavailable = errors.New("cloud SQL token policy unavailable")
	// ErrCloudTokenAccessDenied covers native-account, target, and binding mismatches.
	ErrCloudTokenAccessDenied = errors.New("cloud SQL token access denied")
)

// CloudTokenRequest contains trusted storage and matched native-account facts.
// These fields must not come from token claims, connection attributes, or SQL SET.
// V1 IDs and native names are 1-256 printable ASCII bytes without whitespace;
// storage-cluster ID must be nonzero and keyspace ID must be in [1, 0xFFFFFF].
// This rejects DEFAULT (zero) and invalid/null IDs. The trusted adapter must also
// reject SYSTEM by its actual name/classification before constructing this input:
// SYSTEM can have a positive ID, so a numeric range check cannot exclude it.
type CloudTokenRequest struct {
	Target         sessionauth.Target
	NativeUsername string
	NativeHost     string
	NativePlugin   string
	AccountLocked  bool
	TLSEstablished bool
}

// CloudTokenBinding is an explicit authorization binding from an authenticated
// policy snapshot. Equal positive grant revisions mean native grants were applied;
// verification itself neither creates users nor grants SQL permissions.
// ExpiresAt is mandatory; a zero value does not represent an unlimited binding.
type CloudTokenBinding struct {
	PrincipalID          string
	PrincipalKind        string
	NativeUsername       string
	NativeHost           string
	BindingRevision      uint64
	PolicyRevision       uint64
	Enabled              bool
	ExpiresAt            time.Time
	DesiredGrantRevision uint64
	AppliedGrantRevision uint64
}

// CloudTokenPolicy is one already-authenticated, immutable policy snapshot. Keys
// belong only to Issuer. The caller owns provenance, atomic installation, freshness,
// anti-rollback, and key-ID immutability, and must not mutate maps/keys during use.
// This value alone does not establish freshness after a restart.
// Issuer is 1-1024 printable ASCII bytes without whitespace. RSA keys require a
// positive odd 2048-8192-bit modulus and an odd exponent in [3, 2^31-1]. Maximum
// token lifetime is in (0, 15m], and maximum session duration is in (0, 24h].
// Trusted times and NumericDate claims must be within UTC years 1970-9999.
type CloudTokenPolicy struct {
	Target             sessionauth.Target
	Issuer             string
	SnapshotRevision   uint64
	ValidFrom          time.Time
	ValidUntil         time.Time
	MaxTokenLifetime   time.Duration
	MaxSessionDuration time.Duration
	Keys               map[string]*rsa.PublicKey
	Bindings           map[string]CloudTokenBinding
}

// VerifyCloudToken checks the strict v1 signature, claims and trusted inputs. It
// has no network or mutable state and does not implement revocation, session guards,
// handshake integration, snapshot refresh, or SQL grants. All errors are fixed
// sentinels; parser/cryptography errors must not expose authentication material.
func VerifyCloudToken(raw []byte, request CloudTokenRequest, policy CloudTokenPolicy, now time.Time) (sessionauth.Identity, error) {
	var zero sessionauth.Identity
	if !validCloudTarget(policy.Target) || !cloudString(policy.Issuer, cloudTokenMaxIssuerBytes) ||
		policy.SnapshotRevision == 0 || !validCloudTime(now) || !validCloudTime(policy.ValidFrom) ||
		!validCloudTime(policy.ValidUntil) || !policy.ValidFrom.Before(policy.ValidUntil) ||
		now.Before(policy.ValidFrom) || !now.Before(policy.ValidUntil) ||
		policy.MaxTokenLifetime <= 0 || policy.MaxTokenLifetime > 15*time.Minute ||
		policy.MaxSessionDuration <= 0 || policy.MaxSessionDuration > 24*time.Hour {
		return zero, ErrCloudTokenPolicyUnavailable
	}
	if !validCloudTarget(request.Target) || request.Target != policy.Target ||
		!cloudString(request.NativeUsername, cloudTokenMaxIDBytes) || !cloudString(request.NativeHost, cloudTokenMaxIDBytes) ||
		request.NativePlugin != CloudTokenPlugin || request.AccountLocked || !request.TLSEstablished {
		return zero, ErrCloudTokenAccessDenied
	}
	header, payload, err := parseCloudCompact(raw)
	if err != nil {
		return zero, err
	}
	if len(header) != 3 || header["alg"] != "RS256" || header["typ"] != CloudTokenType {
		return zero, ErrInvalidCloudToken
	}
	kid, ok := header["kid"].(string)
	if !ok || !cloudString(kid, cloudTokenMaxIDBytes) {
		return zero, ErrInvalidCloudToken
	}
	key := policy.Keys[kid]
	if !validCloudRSAKey(key) {
		return zero, ErrCloudTokenPolicyUnavailable
	}
	verified, err := jws.Verify(raw, jws.WithKey(jwa.RS256, key))
	if err != nil || !bytes.Equal(verified, payload) {
		return zero, ErrInvalidCloudToken
	}
	claims, err := decodeCloudObject(payload)
	if err != nil || len(claims) != 15 || claims["ver"] != json.Number("1") {
		return zero, ErrInvalidCloudToken
	}
	stringLimits := map[string]int{
		"iss": cloudTokenMaxIssuerBytes, "aud": cloudTokenMaxIDBytes + len("tidb-cloud:instance:"),
		"sub": cloudTokenMaxIDBytes, "principal_id": cloudTokenMaxIDBytes, "principal_kind": cloudTokenMaxIDBytes,
		"organization_id": cloudTokenMaxIDBytes, "project_id": cloudTokenMaxIDBytes,
		"binding_id": cloudTokenMaxIDBytes, "jti": cloudTokenMaxIDBytes,
	}
	for name, limit := range stringLimits {
		value, ok := claims[name].(string)
		if !ok || !cloudString(value, limit) {
			return zero, ErrInvalidCloudToken
		}
	}
	bindingRevision, ok := cloudRevision(claims["binding_revision"])
	if !ok {
		return zero, ErrInvalidCloudToken
	}
	policyRevision, ok := cloudRevision(claims["policy_revision"])
	if !ok {
		return zero, ErrInvalidCloudToken
	}
	iat, iatOK := cloudNumericDate(claims["iat"])
	nbf, nbfOK := cloudNumericDate(claims["nbf"])
	exp, expOK := cloudNumericDate(claims["exp"])
	if !iatOK || !nbfOK || !expOK || iat.After(nbf) || !nbf.Before(exp) ||
		iat.After(now) || nbf.After(now) || !now.Before(exp) || exp.Sub(iat) > policy.MaxTokenLifetime {
		return zero, ErrInvalidCloudToken
	}
	if claims["iss"] != policy.Issuer || claims["aud"] != "tidb-cloud:instance:"+policy.Target.InstanceID ||
		claims["organization_id"] != policy.Target.OrganizationID || claims["project_id"] != policy.Target.ProjectID ||
		claims["sub"] != request.NativeUsername {
		return zero, ErrCloudTokenAccessDenied
	}
	bindingID := claims["binding_id"].(string)
	binding, ok := policy.Bindings[bindingID]
	if !ok || !binding.Enabled || !validCloudTime(binding.ExpiresAt) || !now.Before(binding.ExpiresAt) ||
		!cloudString(binding.PrincipalID, cloudTokenMaxIDBytes) ||
		(binding.PrincipalKind != "human" && binding.PrincipalKind != "workload") ||
		binding.NativeUsername != request.NativeUsername || binding.NativeHost != request.NativeHost ||
		binding.BindingRevision == 0 || binding.BindingRevision != bindingRevision ||
		binding.PolicyRevision == 0 || binding.PolicyRevision != policyRevision ||
		binding.DesiredGrantRevision == 0 || binding.DesiredGrantRevision != binding.AppliedGrantRevision ||
		claims["principal_id"] != binding.PrincipalID || claims["principal_kind"] != binding.PrincipalKind {
		return zero, ErrCloudTokenAccessDenied
	}
	// Token expiry limits login, not session duration. These initial deadlines do
	// not authorize a caller to skip guard registration or later policy checks.
	sessionLifetime := min(policy.MaxSessionDuration, binding.ExpiresAt.Sub(now))
	return sessionauth.Identity{
		Target:      policy.Target,
		PrincipalID: binding.PrincipalID, PrincipalKind: binding.PrincipalKind,
		NativeUsername: request.NativeUsername, NativeHost: request.NativeHost,
		BindingID: bindingID, BindingRevision: bindingRevision, PolicyRevision: policyRevision,
		Issuer: policy.Issuer, KeyID: kid, Profile: CloudTokenPlugin, TokenID: claims["jti"].(string),
		VerificationSnapshotRevision: policy.SnapshotRevision, AuthenticatedAt: now,
		SessionDeadline:     now.Add(sessionLifetime),
		PolicyLeaseDeadline: now.Add(min(sessionLifetime, policy.ValidUntil.Sub(now))),
	}, nil
}

func validCloudTarget(target sessionauth.Target) bool {
	return cloudString(target.InstanceID, cloudTokenMaxIDBytes) && cloudString(target.OrganizationID, cloudTokenMaxIDBytes) &&
		cloudString(target.ProjectID, cloudTokenMaxIDBytes) && target.StorageClusterID != 0 &&
		target.KeyspaceID != 0 && target.KeyspaceID <= cloudTokenMaxKeyspaceID
}

// V1 identifiers are byte-exact printable ASCII, with no whitespace or normalization.
func cloudString(value string, limit int) bool {
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

func validCloudTime(value time.Time) bool {
	year := value.UTC().Year()
	return year >= 1970 && year <= 9999
}

func validCloudRSAKey(key *rsa.PublicKey) bool {
	return key != nil && key.N != nil && key.N.Sign() > 0 && key.N.BitLen() >= 2048 && key.N.BitLen() <= 8192 &&
		key.N.Bit(0) == 1 && key.E >= 3 && key.E <= 1<<31-1 && key.E%2 == 1
}

func cloudRevision(value any) (uint64, bool) {
	text, ok := value.(string)
	if !ok || len(text) == 0 || len(text) > 20 || text[0] == '0' {
		return 0, false
	}
	revision, err := strconv.ParseUint(text, 10, 64)
	return revision, err == nil && revision > 0 && strconv.FormatUint(revision, 10) == text
}

func cloudNumericDate(value any) (time.Time, bool) {
	number, ok := value.(json.Number)
	if !ok {
		return time.Time{}, false
	}
	seconds, err := strconv.ParseInt(string(number), 10, 64)
	if err != nil || seconds < 0 || seconds > cloudTokenMaxUnixSeconds || strconv.FormatInt(seconds, 10) != string(number) {
		return time.Time{}, false
	}
	return time.Unix(seconds, 0), true
}

func parseCloudCompact(raw []byte) (map[string]any, []byte, error) {
	if len(raw) == 0 || len(raw) > cloudTokenMaxBytes {
		return nil, nil, ErrInvalidCloudToken
	}
	parts := strings.Split(string(raw), ".")
	if len(parts) != 3 {
		return nil, nil, ErrInvalidCloudToken
	}
	decoded := make([][]byte, 3)
	for i, part := range parts {
		value, err := base64.RawURLEncoding.Strict().DecodeString(part)
		if err != nil || len(value) == 0 || base64.RawURLEncoding.EncodeToString(value) != part {
			return nil, nil, ErrInvalidCloudToken
		}
		decoded[i] = value
	}
	header, err := decodeCloudObject(decoded[0])
	return header, decoded[1], err
}

// Decode primitives one field at a time so duplicate keys (including escaped
// equivalents) and nesting cannot be hidden by encoding/json's struct/map decoding.
func decodeCloudObject(raw []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	open, err := decoder.Token()
	if err != nil || open != json.Delim('{') {
		return nil, ErrInvalidCloudToken
	}
	result := make(map[string]any)
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return nil, ErrInvalidCloudToken
		}
		name, ok := key.(string)
		if !ok {
			return nil, ErrInvalidCloudToken
		}
		if _, exists := result[name]; exists {
			return nil, ErrInvalidCloudToken
		}
		value, err := decoder.Token()
		if err != nil {
			return nil, ErrInvalidCloudToken
		}
		switch value.(type) {
		case string, json.Number:
			result[name] = value
		default:
			return nil, ErrInvalidCloudToken
		}
	}
	closeToken, err := decoder.Token()
	if err != nil || closeToken != json.Delim('}') {
		return nil, ErrInvalidCloudToken
	}
	if _, err = decoder.Token(); err != io.EOF {
		return nil, ErrInvalidCloudToken
	}
	return result, nil
}
