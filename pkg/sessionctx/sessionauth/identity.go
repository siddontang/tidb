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

// Package sessionauth defines secret-free values describing verified SQL identities.
package sessionauth

import "time"

// StorageScope distinguishes an unkeyspaced cluster from an API-v2 keyspace.
// The zero value is invalid so omitted scope cannot silently select a deployment.
type StorageScope uint8

const (
	// StorageScopeCluster denotes an unkeyspaced cluster (API-v1). Its canonical
	// KeyspaceID is zero; the adapter normalizes the codec's null sentinel.
	StorageScopeCluster StorageScope = iota + 1
	// StorageScopeKeyspace denotes an API-v2 keyspace, including DEFAULT (ID 0).
	StorageScopeKeyspace
)

// Target binds an operator-defined database audience to its actual storage scope.
// Audience is opaque: TiDB imposes no provider-specific resource hierarchy or ID prefix.
// Keyspace IDs are not globally unique, so StorageClusterID is always required.
// A trusted adapter must attest scope from the actual codec and reject reserved
// keyspaces such as SYSTEM by name/classification and deployment policy. This
// value alone performs neither discovery nor classification.
type Target struct {
	Audience         string
	StorageClusterID uint64
	StorageScope     StorageScope
	KeyspaceID       uint32
}

// Identity uses (Issuer, PrincipalID) as its immutable principal namespace.
// Identity is the result of authentication, not a grant or a live authorization
// guard. It contains no token, key, or mutable policy reference. A caller must
// revalidate policy and register a connection guard before activating a session.
type Identity struct {
	Target                       Target
	PrincipalID                  string
	NativeUsername               string
	NativeHost                   string
	BindingID                    string
	BindingRevision              uint64
	PolicyRevision               uint64
	Issuer                       string
	KeyID                        string
	Profile                      string
	TokenID                      string
	VerificationSnapshotRevision uint64
	AuthenticatedAt              time.Time
	SessionDeadline              time.Time
	PolicyLeaseDeadline          time.Time
}

// Authorization is a live, terminal-on-revocation guard owned by the physical
// connection. RESET CONNECTION may reuse it but must never renew its deadline.
// Implementations must be safe for concurrent policy updates and SQL execution.
type Authorization interface {
	Check() error
	Identity() Identity
}

// Connection stages a verified identity until the complete native login succeeds.
// NativeCheck reads the exact native account from the privilege cache; it must
// not access mutable session state, perform I/O, or acquire connection locks.
// A failed change-user attempt discards the candidate and retains the old guard.
type Connection interface {
	StageSQLTokenIdentity(identity Identity, nativeCheck func() bool) (Authorization, error)
}
