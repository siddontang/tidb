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
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/service"
	kvstore "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/store/driver"
	"github.com/pingcap/tidb/pkg/store/mockstore"
)

// Config contains configuration for the storage service.
type Config struct {
	// Path is the storage path (e.g., "tikv://pd:2379").
	Path string `toml:"path" json:"path"`

	// StoreType is the type of storage (tikv, unistore, mocktikv).
	StoreType config.StoreType `toml:"store" json:"store"`

	// KeyspaceName is the keyspace name for the storage.
	KeyspaceName string `toml:"keyspace-name" json:"keyspace-name"`
}

// Service wraps kv.Storage as a service component.
// This is a local-only service that provides access to the underlying
// key-value storage. It does not require gRPC as storage access is
// always local to the process.
type Service struct {
	*service.BaseService

	mu      sync.RWMutex
	config  Config
	storage kv.Storage
}

// New creates a new storage service.
func New() *Service {
	return &Service{
		BaseService: service.NewBaseService(service.ServiceStorage),
	}
}

// SetConfig sets the service configuration.
func (s *Service) SetConfig(cfg Config) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.config = cfg
}

// Init initializes the storage service.
func (s *Service) Init(ctx context.Context, opts service.Options) error {
	s.InitBase(opts)

	// Extract configuration
	if cfg, ok := opts.Config.(*Config); ok {
		s.config = *cfg
	} else if cfg, ok := opts.Config.(Config); ok {
		s.config = cfg
	}

	// If storage is already set (e.g., for testing), skip initialization
	s.mu.RLock()
	if s.storage != nil {
		s.mu.RUnlock()
		s.SetHealth(service.HealthStatus{State: service.StateHealthy})
		return nil
	}
	s.mu.RUnlock()

	if s.config.Path == "" {
		return errors.New("storage path is required")
	}

	// Register store drivers
	if err := s.registerStoreDrivers(); err != nil {
		return errors.Wrap(err, "failed to register store drivers")
	}

	// Open storage using the kvstore package
	store, err := s.openStorage()
	if err != nil {
		return errors.Wrap(err, "failed to open storage")
	}

	s.mu.Lock()
	s.storage = store
	s.mu.Unlock()

	s.SetHealth(service.HealthStatus{State: service.StateHealthy})
	return nil
}

// registerStoreDrivers registers all store drivers.
// Ignores "already registered" errors since drivers may be registered by main.go
// before the storage service initializes.
func (s *Service) registerStoreDrivers() error {
	// Register TiKV driver
	if err := kvstore.Register(config.StoreTypeTiKV, &driver.TiKVDriver{}); err != nil {
		// Ignore already registered error
		if !isAlreadyRegisteredError(err) {
			return err
		}
	}

	// Register MockTiKV driver
	if err := kvstore.Register(config.StoreTypeMockTiKV, mockstore.MockTiKVDriver{}); err != nil {
		if !isAlreadyRegisteredError(err) {
			return err
		}
	}

	// Register UniStore driver
	if err := kvstore.Register(config.StoreTypeUniStore, mockstore.EmbedUnistoreDriver{}); err != nil {
		if !isAlreadyRegisteredError(err) {
			return err
		}
	}

	return nil
}

// isAlreadyRegisteredError checks if the error is a "driver already registered" error.
func isAlreadyRegisteredError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "already registered")
}

// openStorage opens the storage based on configuration.
func (s *Service) openStorage() (kv.Storage, error) {
	// If keyspace name is provided, use kvstore.MustInitStorage
	if s.config.KeyspaceName != "" {
		return kvstore.New(s.config.Path)
	}

	// Otherwise, use the direct path-based opening
	storeType := s.config.StoreType
	if storeType == "" {
		// Default to TiKV if path looks like tikv:// or unistore otherwise
		if len(s.config.Path) > 7 && s.config.Path[:7] == "tikv://" {
			storeType = config.StoreTypeTiKV
		} else {
			storeType = config.StoreTypeUniStore
		}
	}

	return kvstore.New(s.config.Path)
}

// Start starts the storage service.
func (s *Service) Start(_ context.Context) error {
	s.SetHealth(service.HealthStatus{State: service.StateHealthy})
	return nil
}

// Stop stops the storage service.
func (s *Service) Stop(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.SetHealth(service.HealthStatus{State: service.StateStopping})

	if s.storage != nil {
		if err := s.storage.Close(); err != nil {
			return errors.Wrap(err, "failed to close storage")
		}
		s.storage = nil
	}

	s.SetHealth(service.HealthStatus{State: service.StateStopped})
	return nil
}

// Storage returns the underlying kv.Storage.
func (s *Service) Storage() kv.Storage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.storage
}

// SetStorage sets the storage instance.
// This is useful for testing or when the storage is created externally.
func (s *Service) SetStorage(store kv.Storage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.storage = store
}

// Ensure Service implements service.Service.
var _ service.Service = (*Service)(nil)
