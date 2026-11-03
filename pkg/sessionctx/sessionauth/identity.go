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

// Target binds a stable Cloud Instance to its actual storage scope. Keyspace IDs
// are not globally unique, so the storage-cluster ID is part of the identity.
// The trusted storage adapter must reject internal keyspaces such as SYSTEM by
// their actual name/classification; a positive numeric ID alone cannot identify
// a user keyspace. This value does not perform that classification.
type Target struct {
	InstanceID       string
	OrganizationID   string
	ProjectID        string
	StorageClusterID uint64
	KeyspaceID       uint32
}

// Identity is the result of authentication, not a grant or a live authorization
// guard. It contains no token, key, or mutable policy reference. A caller must
// revalidate policy and register a connection guard before activating a session.
type Identity struct {
	Target                       Target
	PrincipalID                  string
	PrincipalKind                string
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
