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

package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// mockService is a test implementation of Service.
type mockService struct {
	*BaseService
	initCalled  bool
	startCalled bool
	stopCalled  bool
}

func newMockService(name string, deps ...string) *mockService {
	return &mockService{
		BaseService: NewBaseService(name, deps...),
	}
}

func (s *mockService) Init(_ context.Context, opts Options) error {
	s.InitBase(opts)
	s.initCalled = true
	return nil
}

func (s *mockService) Start(_ context.Context) error {
	s.startCalled = true
	s.SetHealth(HealthStatus{State: StateHealthy})
	return nil
}

func (s *mockService) Stop(_ context.Context) error {
	s.stopCalled = true
	s.SetHealth(HealthStatus{State: StateStopped})
	return nil
}

func TestLocalRegistry(t *testing.T) {
	ctx := context.Background()
	registry := NewLocalRegistry()

	// Test Register
	svc := newMockService("test-service")
	err := registry.Register(ctx, svc)
	require.NoError(t, err)

	// Test Get
	got, err := registry.Get(ctx, "test-service")
	require.NoError(t, err)
	require.Equal(t, svc, got)

	// Test List
	names, err := registry.List(ctx)
	require.NoError(t, err)
	require.Contains(t, names, "test-service")

	// Test Unregister
	err = registry.Unregister(ctx, "test-service")
	require.NoError(t, err)

	_, err = registry.Get(ctx, "test-service")
	require.Error(t, err)

	// Test Close
	err = registry.Close()
	require.NoError(t, err)
}

func TestServiceManager(t *testing.T) {
	ctx := context.Background()

	cfg := DefaultConfig()
	manager, err := NewManager(cfg)
	require.NoError(t, err)

	// Create services with dependencies
	svcA := newMockService("service-a")
	svcB := newMockService("service-b", "service-a")
	svcC := newMockService("service-c", "service-b")

	// Register in random order
	err = manager.Register(svcC)
	require.NoError(t, err)
	err = manager.Register(svcA)
	require.NoError(t, err)
	err = manager.Register(svcB)
	require.NoError(t, err)

	// Start should resolve dependencies
	err = manager.Start(ctx)
	require.NoError(t, err)

	// All services should be initialized and started
	require.True(t, svcA.initCalled)
	require.True(t, svcA.startCalled)
	require.True(t, svcB.initCalled)
	require.True(t, svcB.startCalled)
	require.True(t, svcC.initCalled)
	require.True(t, svcC.startCalled)

	// Health should be healthy
	health := manager.Health()
	require.True(t, health.IsHealthy())

	// Stop should work
	err = manager.Stop(ctx)
	require.NoError(t, err)

	require.True(t, svcA.stopCalled)
	require.True(t, svcB.stopCalled)
	require.True(t, svcC.stopCalled)
}

func TestCircularDependencyDetection(t *testing.T) {
	ctx := context.Background()

	cfg := DefaultConfig()
	manager, err := NewManager(cfg)
	require.NoError(t, err)

	// Create circular dependency: A -> B -> C -> A
	svcA := newMockService("service-a", "service-c")
	svcB := newMockService("service-b", "service-a")
	svcC := newMockService("service-c", "service-b")

	err = manager.Register(svcA)
	require.NoError(t, err)
	err = manager.Register(svcB)
	require.NoError(t, err)
	err = manager.Register(svcC)
	require.NoError(t, err)

	// Start should fail with circular dependency error
	err = manager.Start(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "circular dependency")
}

func TestHealthAggregation(t *testing.T) {
	agg := NewHealthAggregator()

	svc1 := newMockService("svc1")
	svc1.SetHealth(HealthStatus{State: StateHealthy})

	svc2 := newMockService("svc2")
	svc2.SetHealth(HealthStatus{State: StateHealthy})

	agg.Register(svc1)
	agg.Register(svc2)

	// All healthy
	overall := agg.GetOverallHealth()
	require.Equal(t, StateHealthy, overall.State)

	// One unhealthy
	svc2.SetHealth(HealthStatus{State: StateUnhealthy})
	overall = agg.GetOverallHealth()
	require.Equal(t, StateUnhealthy, overall.State)
}

func TestConfig(t *testing.T) {
	cfg := DefaultConfig()
	require.Equal(t, ModeMonolithic, cfg.Mode)
	require.True(t, cfg.IsMonolithic())
	require.False(t, cfg.IsDistributed())

	cfg.Mode = ModeDistributed
	cfg.EnabledServices = []string{ServiceDDL, ServiceStatistics}
	require.True(t, cfg.IsServiceEnabled(ServiceDDL))
	require.True(t, cfg.IsServiceEnabled(ServiceStatistics))
	require.False(t, cfg.IsServiceEnabled(ServiceGateway))
}
