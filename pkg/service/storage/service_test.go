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

package storage

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/service"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	svc := New()
	require.NotNil(t, svc)
	require.Equal(t, service.ServiceStorage, svc.Name())
	require.Empty(t, svc.Dependencies())
}

func TestService_SetConfig(t *testing.T) {
	svc := New()
	cfg := Config{
		Path:      "tikv://127.0.0.1:2379",
		StoreType: "tikv",
	}
	svc.SetConfig(cfg)

	svc.mu.RLock()
	defer svc.mu.RUnlock()
	require.Equal(t, cfg.Path, svc.config.Path)
	require.Equal(t, cfg.StoreType, svc.config.StoreType)
}

func TestService_SetStorage(t *testing.T) {
	svc := New()

	// Create a mock storage
	mockStore := &mockStorage{}
	svc.SetStorage(mockStore)

	// Verify storage is set
	require.Equal(t, mockStore, svc.Storage())
}

func TestService_Init_NoPath(t *testing.T) {
	svc := New()
	err := svc.Init(context.Background(), service.Options{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "storage path is required")
}

func TestService_Init_WithStorage(t *testing.T) {
	svc := New()
	mockStore := &mockStorage{}
	svc.SetStorage(mockStore)

	// Init should succeed when storage is already set
	err := svc.Init(context.Background(), service.Options{})
	require.NoError(t, err)

	// Health should be healthy
	health := svc.Health()
	require.Equal(t, service.StateHealthy, health.State)
}

func TestService_Start(t *testing.T) {
	svc := New()
	mockStore := &mockStorage{}
	svc.SetStorage(mockStore)

	err := svc.Init(context.Background(), service.Options{})
	require.NoError(t, err)

	err = svc.Start(context.Background())
	require.NoError(t, err)
}

func TestService_Stop(t *testing.T) {
	svc := New()
	mockStore := &mockStorage{closeCalled: new(bool)}
	svc.SetStorage(mockStore)

	err := svc.Init(context.Background(), service.Options{})
	require.NoError(t, err)

	err = svc.Stop(context.Background())
	require.NoError(t, err)

	// Verify storage was closed
	require.True(t, *mockStore.closeCalled)
	require.Nil(t, svc.Storage())
}

func TestIsAlreadyRegisteredError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "already registered error",
			err:      &mockError{msg: "tikv is already registered"},
			expected: true,
		},
		{
			name:     "other error",
			err:      &mockError{msg: "some other error"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isAlreadyRegisteredError(tt.err)
			require.Equal(t, tt.expected, result)
		})
	}
}

// mockStorage implements kv.Storage for testing.
type mockStorage struct {
	kv.Storage
	closeCalled *bool
}

func (m *mockStorage) Close() error {
	if m.closeCalled != nil {
		*m.closeCalled = true
	}
	return nil
}

type mockError struct {
	msg string
}

func (e *mockError) Error() string {
	return e.msg
}
