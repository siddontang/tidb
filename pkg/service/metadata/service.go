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

package metadata

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/service"
	"github.com/pingcap/tidb/pkg/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// Config contains configuration for the metadata service.
type Config struct {
	// Endpoints is the list of etcd endpoints.
	Endpoints []string `toml:"endpoints" json:"endpoints"`

	// DialTimeout is the timeout for connecting to etcd.
	DialTimeout time.Duration `toml:"dial-timeout" json:"dial-timeout"`
}

// DefaultConfig returns the default metadata service configuration.
func DefaultConfig() Config {
	return Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	}
}

// Service provides etcd client and owner management functionality.
// It handles coordination between TiDB nodes for leader election and
// distributed state management.
type Service struct {
	*service.BaseService

	mu     sync.RWMutex
	config Config
	client *clientv3.Client

	// ownerManagers holds owner managers for different components.
	ownerManagers map[string]owner.Manager
}

// New creates a new metadata service.
func New() *Service {
	return &Service{
		BaseService:   service.NewBaseService(service.ServiceMetadata, service.ServiceStorage),
		config:        DefaultConfig(),
		ownerManagers: make(map[string]owner.Manager),
	}
}

// Init initializes the metadata service.
func (s *Service) Init(ctx context.Context, opts service.Options) error {
	s.InitBase(opts)

	// Extract configuration
	if cfg, ok := opts.Config.(*Config); ok {
		s.config = *cfg
	} else if cfg, ok := opts.Config.(Config); ok {
		s.config = cfg
	}

	if len(s.config.Endpoints) == 0 {
		s.config = DefaultConfig()
	}
	if s.config.DialTimeout == 0 {
		s.config.DialTimeout = 5 * time.Second
	}

	// Create etcd client
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   s.config.Endpoints,
		DialTimeout: s.config.DialTimeout,
	})
	if err != nil {
		return errors.Wrap(err, "failed to create etcd client")
	}

	s.mu.Lock()
	s.client = client
	s.mu.Unlock()

	s.SetHealth(service.HealthStatus{State: service.StateHealthy})
	return nil
}

// Start starts the metadata service.
func (s *Service) Start(_ context.Context) error {
	s.SetHealth(service.HealthStatus{State: service.StateHealthy})
	return nil
}

// Stop stops the metadata service.
func (s *Service) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.SetHealth(service.HealthStatus{State: service.StateStopping})

	// Close all owner managers
	for name, mgr := range s.ownerManagers {
		logutil.BgLogger().Info("closing owner manager", zap.String("name", name))
		mgr.Close()
	}
	s.ownerManagers = make(map[string]owner.Manager)

	// Close etcd client
	if s.client != nil {
		if err := s.client.Close(); err != nil {
			logutil.BgLogger().Warn("failed to close etcd client", zap.Error(err))
		}
		s.client = nil
	}

	s.SetHealth(service.HealthStatus{State: service.StateStopped})
	return nil
}

// EtcdClient returns the etcd client.
func (s *Service) EtcdClient() *clientv3.Client {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.client
}

// SetEtcdClient sets the etcd client.
// This is useful for testing or when the client is created externally.
func (s *Service) SetEtcdClient(client *clientv3.Client) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.client = client
}

// CreateOwnerManager creates a new owner manager for the given component.
func (s *Service) CreateOwnerManager(ctx context.Context, prompt, id, key string) (owner.Manager, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.client == nil {
		return nil, errors.New("etcd client not initialized")
	}

	mgr := owner.NewOwnerManager(ctx, s.client, prompt, id, key)
	s.ownerManagers[key] = mgr
	return mgr, nil
}

// GetOwnerManager returns the owner manager for the given key.
func (s *Service) GetOwnerManager(key string) (owner.Manager, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	mgr, ok := s.ownerManagers[key]
	return mgr, ok
}

// RemoveOwnerManager removes an owner manager.
func (s *Service) RemoveOwnerManager(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if mgr, ok := s.ownerManagers[key]; ok {
		mgr.Close()
		delete(s.ownerManagers, key)
	}
}

// Ensure Service implements service.Service.
var _ service.Service = (*Service)(nil)
