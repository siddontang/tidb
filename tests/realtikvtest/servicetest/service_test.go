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

package servicetest

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/service"
	"github.com/pingcap/tidb/pkg/service/storage"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	realtikvtest.RunTestMain(m)
}

// TestServiceManagerWithTiKV tests the service manager with a real TiKV cluster.
func TestServiceManagerWithTiKV(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	defer func() {
		require.NoError(t, store.Close())
	}()

	// Create service manager in monolithic mode
	cfg := service.DefaultConfig()
	cfg.Mode = service.ModeMonolithic

	mgr, err := service.NewManager(cfg)
	require.NoError(t, err)

	// Create and register storage service
	storageSvc := storage.New()
	storageSvc.SetStorage(store)
	require.NoError(t, mgr.Register(storageSvc))

	// Start services
	ctx := context.Background()
	require.NoError(t, mgr.Start(ctx))

	// Verify service is healthy
	health := mgr.Health()
	require.Equal(t, service.StateHealthy, health.State)

	// Get storage service and verify it works
	svc, err := mgr.Get(service.ServiceStorage)
	require.NoError(t, err)
	require.NotNil(t, svc)

	storageSvcTyped, ok := svc.(*storage.Service)
	require.True(t, ok)
	require.Equal(t, store, storageSvcTyped.Storage())

	// Stop services
	require.NoError(t, mgr.Stop(ctx))
}

// TestServiceHealthAggregation tests health aggregation with real TiKV.
func TestServiceHealthAggregation(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	defer func() {
		require.NoError(t, store.Close())
	}()

	cfg := service.DefaultConfig()
	cfg.Mode = service.ModeMonolithic

	mgr, err := service.NewManager(cfg)
	require.NoError(t, err)

	// Register storage service
	storageSvc := storage.New()
	storageSvc.SetStorage(store)
	require.NoError(t, mgr.Register(storageSvc))

	// Start
	ctx := context.Background()
	require.NoError(t, mgr.Start(ctx))

	// Check individual service health
	storageHealth, ok := mgr.ServiceHealth(service.ServiceStorage)
	require.True(t, ok)
	require.Equal(t, service.StateHealthy, storageHealth.State)

	// Overall health should be healthy
	overall := mgr.Health()
	require.Equal(t, service.StateHealthy, overall.State)

	// Stop
	require.NoError(t, mgr.Stop(ctx))
}

// TestServiceRegistry tests the service registry with real TiKV.
func TestServiceRegistry(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	defer func() {
		require.NoError(t, store.Close())
	}()

	cfg := service.DefaultConfig()
	cfg.Mode = service.ModeMonolithic

	mgr, err := service.NewManager(cfg)
	require.NoError(t, err)

	// Register storage service
	storageSvc := storage.New()
	storageSvc.SetStorage(store)
	require.NoError(t, mgr.Register(storageSvc))

	// Start
	ctx := context.Background()
	require.NoError(t, mgr.Start(ctx))

	// List services from registry
	registry := mgr.Registry()
	require.NotNil(t, registry)

	services, err := registry.List(ctx)
	require.NoError(t, err)
	require.Contains(t, services, service.ServiceStorage)

	// Stop
	require.NoError(t, mgr.Stop(ctx))
}
