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

// These values define the policy transport and its durable v1 representation.
// Keep independent wire fixtures literal so a constant change cannot silently
// redefine the protocol expected by the tests.
const (
	sqlPolicyWireVersion = 1
	sqlPolicyDomain      = "tidb-sql-policy-v1\n"

	sqlPolicyScopeCluster  = "cluster"
	sqlPolicyScopeKeyspace = "keyspace"

	sqlPolicyChallengeFile = "challenge.json"
	sqlPolicyStatusFile    = "status.json"
	sqlPolicyWatermarkFile = "watermark.json"
	sqlPolicyLockFile      = "runtime.lock"

	sqlPolicyStatusAwaiting    = "awaiting_policy"
	sqlPolicyStatusUnavailable = "policy_unavailable"
	sqlPolicyStatusClosed      = "closed"
)

// SQLTokenTargetDocument is the transport form of an operator-attested target.
// StorageScope is exactly "cluster" or "keyspace"; IDs are never inferred from a token.
type SQLTokenTargetDocument struct {
	Audience         string `json:"audience"`
	StorageClusterID string `json:"storage_cluster_id"`
	StorageScope     string `json:"storage_scope"`
	KeyspaceID       uint32 `json:"keyspace_id"`
}

// SQLTokenPolicyChallenge is written to challenge.json with a fresh random nonce.
// MinimumIssuedAt is a lower bound, not an assertion of current trusted time.
type SQLTokenPolicyChallenge struct {
	Version         int                    `json:"version"`
	Incarnation     string                 `json:"incarnation"`
	Nonce           string                 `json:"nonce"`
	Sequence        string                 `json:"sequence"`
	Target          SQLTokenTargetDocument `json:"target"`
	MinimumIssuedAt int64                  `json:"minimum_issued_at"`
}

// SQLTokenPolicyEnvelope carries a strict unpadded base64url payload and signature.
// The pinned Ed25519 authority signs "tidb-sql-policy-v1\n" followed by the exact
// decoded payload bytes. Its public key file contains a single PEM PKIX public key.
type SQLTokenPolicyEnvelope struct {
	Payload   string `json:"payload"`
	Signature string `json:"signature"`
}

// SQLTokenPolicyResponse attests fresh authority time for one challenge only.
// IssuedAt/ValidUntil are integer Unix seconds. Freshness cannot outlive Policy.
type SQLTokenPolicyResponse struct {
	Version     int                    `json:"version"`
	Incarnation string                 `json:"incarnation"`
	Nonce       string                 `json:"nonce"`
	Sequence    string                 `json:"sequence"`
	Target      SQLTokenTargetDocument `json:"target"`
	IssuedAt    int64                  `json:"issued_at"`
	ValidUntil  int64                  `json:"valid_until"`
	Policy      SQLTokenPolicyDocument `json:"policy"`
}

// SQLTokenPolicyDocument is a complete policy, not a patch. Revision fields are
// canonical positive uint64 strings. Keys map kid to base64url DER PKIX RSA keys.
// Removal of a key revokes its established sessions in this first runtime version.
type SQLTokenPolicyDocument struct {
	Target                    SQLTokenTargetDocument             `json:"target"`
	Issuer                    string                             `json:"issuer"`
	Revision                  string                             `json:"revision"`
	ValidFrom                 int64                              `json:"valid_from"`
	ValidUntil                int64                              `json:"valid_until"`
	MaxTokenLifetimeSeconds   int64                              `json:"max_token_lifetime_seconds"`
	MaxSessionDurationSeconds int64                              `json:"max_session_duration_seconds"`
	Keys                      map[string]string                  `json:"keys"`
	Bindings                  map[string]SQLTokenBindingDocument `json:"bindings"`
}

// SQLTokenBindingDocument records explicit native grants already reconciled by
// the policy authority. Neither the signed response nor authentication grants SQL privileges.
type SQLTokenBindingDocument struct {
	PrincipalID          string `json:"principal_id"`
	NativeUsername       string `json:"native_username"`
	NativeHost           string `json:"native_host"`
	BindingRevision      string `json:"binding_revision"`
	PolicyRevision       string `json:"policy_revision"`
	Enabled              bool   `json:"enabled"`
	ExpiresAt            int64  `json:"expires_at"`
	DesiredGrantRevision string `json:"desired_grant_revision"`
	AppliedGrantRevision string `json:"applied_grant_revision"`
}

// SQLTokenRuntimeStatus is a secret-free, advisory status.json projection. It is
// not an authorization source. LastError is empty or one of awaiting_policy,
// policy_unavailable, and closed. Times are authority Unix seconds, zero if absent.
type SQLTokenRuntimeStatus struct {
	Version          int                    `json:"version"`
	Target           SQLTokenTargetDocument `json:"target"`
	Incarnation      string                 `json:"incarnation"`
	AcceptedNonce    string                 `json:"accepted_nonce"`
	AcceptedSequence string                 `json:"accepted_sequence"`
	PolicyDigest     string                 `json:"policy_digest"`
	Ready            bool                   `json:"ready"`
	Revision         string                 `json:"revision"`
	PolicyValidUntil int64                  `json:"policy_valid_until"`
	LeaseValidUntil  int64                  `json:"lease_valid_until"`
	LastError        string                 `json:"last_error"`
}
