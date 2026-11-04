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
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionauth"
)

// SQLTokenTargetForStore derives isolation facts from the actual session store.
// Audience is operator configuration, never a JWT claim or connection attribute.
// SYSTEM is an internal deployment and cannot accept external SQL identities.
func SQLTokenTargetForStore(store kv.Storage, audience string) (sessionauth.Target, error) {
	if store == nil || store.GetCodec() == nil || store.GetKeyspace() == keyspace.System {
		return sessionauth.Target{}, ErrSQLTokenAccessDenied
	}
	target := sessionauth.Target{Audience: audience, StorageClusterID: store.GetClusterID()}
	switch store.GetCodec().GetAPIVersion() {
	case kvrpcpb.APIVersion_V1:
		target.StorageScope = sessionauth.StorageScopeCluster
	case kvrpcpb.APIVersion_V2:
		target.StorageScope = sessionauth.StorageScopeKeyspace
		target.KeyspaceID = uint32(store.GetCodec().GetKeyspaceID())
	default:
		return sessionauth.Target{}, ErrSQLTokenAccessDenied
	}
	if !validSQLTarget(target) {
		return sessionauth.Target{}, ErrSQLTokenAccessDenied
	}
	return target, nil
}
