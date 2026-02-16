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

package gateway

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/service"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// Config contains configuration for the gateway service.
type Config struct {
	// Host is the host to listen on.
	Host string `toml:"host" json:"host"`

	// Port is the port to listen on.
	Port uint `toml:"port" json:"port"`

	// Socket is the Unix socket path (optional).
	Socket string `toml:"socket" json:"socket"`

	// MaxConnections is the maximum number of concurrent connections.
	MaxConnections uint32 `toml:"max-connections" json:"max-connections"`

	// StoragePath is the TiKV path for local backend mode.
	StoragePath string `toml:"storage-path" json:"storage-path"`

	// ServiceEndpoints for distributed gRPC mode (optional).
	Endpoints ServiceEndpoints `toml:"endpoints" json:"endpoints"`

	// UseLocalBackend uses local TiDB backend instead of gRPC.
	UseLocalBackend bool `toml:"use-local-backend" json:"use-local-backend"`
}

// DefaultConfig returns the default gateway service configuration.
func DefaultConfig() Config {
	return Config{
		Host:            "0.0.0.0",
		Port:            4000,
		MaxConnections:  0, // 0 means no limit
		UseLocalBackend: true,
	}
}

// Service provides MySQL protocol gateway functionality.
// It handles client connections and routes queries to backend services.
type Service struct {
	*service.BaseService

	mu            sync.RWMutex
	config        Config
	mysqlServer   *MySQLServer
	clientManager *GRPCClientManager
	clients       *ServiceClients
	backend       *TiDBBackend
	store         kv.Storage
	dom           *domain.Domain
}

// New creates a new gateway service.
func New() *Service {
	return &Service{
		BaseService: service.NewBaseService(
			service.ServiceGateway,
			// In distributed mode, gateway has no local dependencies
			// It connects to other services via gRPC or local backend
		),
		config: DefaultConfig(),
	}
}

// Init initializes the gateway service.
func (s *Service) Init(_ context.Context, opts service.Options) error {
	s.InitBase(opts)

	// Extract configuration
	if cfg, ok := opts.Config.(*Config); ok {
		s.config = *cfg
	} else if cfg, ok := opts.Config.(Config); ok {
		s.config = cfg
	}

	s.SetHealth(service.HealthStatus{State: service.StateHealthy})
	return nil
}

// Start starts the gateway service.
func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if clients were externally set (e.g., from main.go)
	if s.clients != nil {
		logutil.BgLogger().Info("using externally configured clients")
	} else if s.config.UseLocalBackend && s.config.StoragePath != "" {
		// Use local TiDB backend
		if err := s.initLocalBackend(ctx); err != nil {
			return errors.Wrap(err, "failed to initialize local backend")
		}
		s.clients = NewLocalBackendClient(s.backend)
		logutil.BgLogger().Info("using local TiDB backend",
			zap.String("storage", s.config.StoragePath))
	} else if s.config.Endpoints.Query != "" {
		// Use remote gRPC services
		s.clientManager = NewGRPCClientManager(s.config.Endpoints)
		clients, err := s.clientManager.Connect(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to connect to remote services")
		}
		s.clients = clients
		logutil.BgLogger().Info("connected to remote services via gRPC")
	}

	// Start MySQL server
	s.mysqlServer = NewMySQLServer(s.config, s.clients)
	if err := s.mysqlServer.Start(); err != nil {
		return errors.Wrap(err, "failed to start MySQL server")
	}

	s.SetHealth(service.HealthStatus{State: service.StateHealthy})
	logutil.BgLogger().Info("Gateway service started",
		zap.String("host", s.config.Host),
		zap.Uint("port", s.config.Port))

	return nil
}

func (s *Service) initLocalBackend(ctx context.Context) error {
	// Open storage
	store, err := openStorage(s.config.StoragePath)
	if err != nil {
		return errors.Wrap(err, "failed to open storage")
	}
	s.store = store

	// Bootstrap domain
	dom, err := session.BootstrapSession(store)
	if err != nil {
		store.Close()
		return errors.Wrap(err, "failed to bootstrap session")
	}
	s.dom = dom

	// Create backend
	s.backend = NewTiDBBackend(store, dom)
	return nil
}

func openStorage(path string) (kv.Storage, error) {
	// Use store.New which handles driver registration
	return store.New(path)
}

// Stop stops the gateway service.
func (s *Service) Stop(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.SetHealth(service.HealthStatus{State: service.StateStopping})

	if s.mysqlServer != nil {
		s.mysqlServer.Stop()
		s.mysqlServer = nil
	}

	if s.clientManager != nil {
		s.clientManager.Close()
		s.clientManager = nil
	}

	if s.backend != nil {
		s.backend.Close()
		s.backend = nil
	}

	s.SetHealth(service.HealthStatus{State: service.StateStopped})
	return nil
}

// Config returns the gateway configuration.
func (s *Service) Config() Config {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.config
}

// SetConfig sets the service configuration.
func (s *Service) SetConfig(cfg Config) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.config = cfg
}

// SetClients sets the service clients (for testing or external injection).
func (s *Service) SetClients(clients *ServiceClients) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clients = clients
}

// ConnectionCount returns the current connection count.
func (s *Service) ConnectionCount() uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mysqlServer == nil {
		return 0
	}
	return uint32(len(s.mysqlServer.conns))
}

// Ensure Service implements service.Service.
var _ service.Service = (*Service)(nil)
