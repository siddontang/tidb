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

package stats

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/service"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	svc := New()
	require.NotNil(t, svc)
	require.Equal(t, service.ServiceStatistics, svc.Name())
	require.Contains(t, svc.Dependencies(), service.ServiceStorage)
	require.Contains(t, svc.Dependencies(), service.ServiceSchema)
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	require.True(t, cfg.EnableAutoAnalyze)
}

func TestService_Start(t *testing.T) {
	svc := New()
	err := svc.Start(context.Background())
	require.NoError(t, err)
}

func TestService_Stop(t *testing.T) {
	svc := New()
	err := svc.Stop(context.Background())
	require.NoError(t, err)
}

func TestGRPCServer_NilService(t *testing.T) {
	server := NewGRPCServer(nil, "127.0.0.1:4001")
	require.NotNil(t, server)

	// GetTableStats should return error for nil service
	_, err := server.GetTableStats(context.Background(), 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "service not initialized")

	// LoadStats should return error for nil service
	err = server.LoadStats(context.Background(), 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "service not initialized")

	// UpdateStats should return error for nil service
	err = server.UpdateStats(context.Background(), 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "service not initialized")

	// GetRowCount should return error for nil service
	_, err = server.GetRowCount(context.Background(), 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "service not initialized")
}

func TestGRPCServer_Close(t *testing.T) {
	server := NewGRPCServer(nil, "127.0.0.1:4001")
	err := server.Close()
	require.NoError(t, err)
}

func TestClientDiscover_NilEtcd(t *testing.T) {
	discover := NewClientDiscover(nil)
	require.NotNil(t, discover)

	// GetClient should fail without etcd
	_, _, err := discover.GetClient(context.Background())
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
