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

func TestDomainServiceProvider_NilManager(t *testing.T) {
	provider := NewDomainServiceProvider(nil)

	_, err := provider.GetService("test")
	require.Error(t, err)
	require.Contains(t, err.Error(), "service manager is nil")
}

func TestDomainServiceProvider_Manager(t *testing.T) {
	cfg := DefaultConfig()
	mgr, err := NewManager(cfg)
	require.NoError(t, err)
	defer mgr.Stop(context.Background())

	provider := NewDomainServiceProvider(mgr)
	require.NotNil(t, provider.Manager())
	require.Equal(t, mgr, provider.Manager())
}

func TestDomainServiceProvider_GetService(t *testing.T) {
	cfg := DefaultConfig()
	mgr, err := NewManager(cfg)
	require.NoError(t, err)
	defer mgr.Stop(context.Background())

	// Register a mock service
	mockSvc := &mockService{
		BaseService: NewBaseService("mock-service"),
	}
	err = mgr.Register(mockSvc)
	require.NoError(t, err)

	// Start the manager
	ctx := context.Background()
	err = mgr.Start(ctx)
	require.NoError(t, err)

	// Create provider and get service
	provider := NewDomainServiceProvider(mgr)

	// Get existing service
	svc, err := provider.GetService("mock-service")
	require.NoError(t, err)
	require.NotNil(t, svc)

	// Get non-existing service
	_, err = provider.GetService("non-existing")
	require.Error(t, err)
}

func TestDomainServiceProvider_GetStorage_NotConfigured(t *testing.T) {
	cfg := DefaultConfig()
	mgr, err := NewManager(cfg)
	require.NoError(t, err)
	defer mgr.Stop(context.Background())

	provider := NewDomainServiceProvider(mgr)

	// Storage service not registered
	_, err = provider.GetStorage()
	require.Error(t, err)
}

func TestDomainServiceProvider_GetEtcdClient_NotConfigured(t *testing.T) {
	cfg := DefaultConfig()
	mgr, err := NewManager(cfg)
	require.NoError(t, err)
	defer mgr.Stop(context.Background())

	provider := NewDomainServiceProvider(mgr)

	// Metadata service not registered
	_, err = provider.GetEtcdClient()
	require.Error(t, err)
}

func TestGetServiceClient_Generic(t *testing.T) {
	cfg := DefaultConfig()
	mgr, err := NewManager(cfg)
	require.NoError(t, err)
	defer mgr.Stop(context.Background())

	// Register a mock service with a known interface
	mockSvc := &mockServiceWithClient{
		BaseService: NewBaseService("test-client-service"),
	}
	err = mgr.Register(mockSvc)
	require.NoError(t, err)

	err = mgr.Start(context.Background())
	require.NoError(t, err)

	provider := NewDomainServiceProvider(mgr)

	// Get service with correct type
	client, err := GetServiceClient[*mockServiceWithClient](provider, "test-client-service")
	require.NoError(t, err)
	require.NotNil(t, client)

	// Get service with wrong type
	_, err = GetServiceClient[*mockService](provider, "test-client-service")
	require.Error(t, err)
}

// mockServiceWithClient is a mock service that can be type-asserted.
type mockServiceWithClient struct {
	*BaseService
}

func (s *mockServiceWithClient) Init(_ context.Context, opts Options) error {
	s.InitBase(opts)
	return nil
}

func (s *mockServiceWithClient) Start(_ context.Context) error {
	s.SetHealth(HealthStatus{State: StateHealthy})
	return nil
}

func (s *mockServiceWithClient) Stop(_ context.Context) error {
	s.SetHealth(HealthStatus{State: StateStopped})
	return nil
}
