// Copyright 2025 PingCAP, Inc.
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

package ddl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGRPCServer_NilService(t *testing.T) {
	server := NewGRPCServer(nil, "127.0.0.1:4001")
	require.NotNil(t, server)

	// IsOwner should return error for nil service
	_, _, err := server.IsOwner(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "service not initialized")

	// SubmitJob should return error for nil service
	_, err = server.SubmitJob(context.Background(), nil, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "service not initialized")

	// GetJobStatus should return error for nil service
	_, err = server.GetJobStatus(context.Background(), 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "service not initialized")

	// CancelJob should return error for nil service
	err = server.CancelJob(context.Background(), 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "service not initialized")
}

func TestGRPCServer_WithService(t *testing.T) {
	svc := New()
	server := NewGRPCServer(svc, "127.0.0.1:4001")
	require.NotNil(t, server)

	// IsOwner should return false when no owner manager
	isOwner, ownerID, err := server.IsOwner(context.Background())
	require.NoError(t, err)
	require.False(t, isOwner)
	require.Empty(t, ownerID)

	// SubmitJob should return "not DDL owner" error
	_, err = server.SubmitJob(context.Background(), []byte(`{}`), 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not DDL owner")

	// GetJobStatus should return "not DDL owner" error
	_, err = server.GetJobStatus(context.Background(), 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not DDL owner")

	// CancelJob should return "not DDL owner" error
	err = server.CancelJob(context.Background(), 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not DDL owner")
}

func TestGRPCServer_Close(t *testing.T) {
	server := NewGRPCServer(nil, "127.0.0.1:4001")
	err := server.Close()
	require.NoError(t, err)
}

func TestOwnerListener(t *testing.T) {
	server := NewGRPCServer(nil, "127.0.0.1:4001")
	listener := NewOwnerListener(server, "127.0.0.1:4001")
	require.NotNil(t, listener)

	// These should not panic
	listener.OnBecomeOwner()
	listener.OnRetireOwner()
}

func TestClientDiscover_NilEtcd(t *testing.T) {
	discover := NewClientDiscover(nil)
	require.NotNil(t, discover)

	// GetClient should fail without etcd
	_, _, err := discover.GetClient(context.Background(), 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "etcd client not initialized")

	// Close should succeed
	err = discover.Close()
	require.NoError(t, err)
}

func TestClientDiscover_ResetConn(t *testing.T) {
	discover := NewClientDiscover(nil)
	require.NotNil(t, discover)

	// ResetConn should not panic even with no connection
	discover.ResetConn(0, nil)

	// Version should have incremented
	require.Equal(t, uint64(1), discover.version.Load())
}
