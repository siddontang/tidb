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
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/service"
	"github.com/pingcap/tidb/pkg/store/driver"
)

// Config contains configuration for the storage service.
type Config struct {
	// Path is the storage path (e.g., "tikv://pd:2379").
	Path string `toml:"path" json:"path"`
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

	if s.config.Path == "" {
		return errors.New("storage path is required")
	}

	// Open storage
	d := &driver.TiKVDriver{}
	store, err := d.Open(s.config.Path)
	if err != nil {
		return errors.Wrap(err, "failed to open storage")
	}

	s.mu.Lock()
	s.storage = store
	s.mu.Unlock()

	s.SetHealth(service.HealthStatus{State: service.StateHealthy})
	return nil
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
