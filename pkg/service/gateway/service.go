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

	"github.com/pingcap/tidb/pkg/service"
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
}

// DefaultConfig returns the default gateway service configuration.
func DefaultConfig() Config {
	return Config{
		Host:           "0.0.0.0",
		Port:           4000,
		MaxConnections: 0, // 0 means no limit
	}
}

// Service provides MySQL protocol gateway functionality.
// It handles client connections and routes queries to the query service.
type Service struct {
	*service.BaseService

	mu       sync.RWMutex
	config   Config
	listener any // Would be net.Listener in full implementation
}

// New creates a new gateway service.
func New() *Service {
	return &Service{
		BaseService: service.NewBaseService(
			service.ServiceGateway,
			service.ServiceSession,
			service.ServiceQuery,
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
func (s *Service) Start(_ context.Context) error {
	// In a full implementation, this would start the MySQL protocol listener.
	// The existing server.Server code would be refactored here.
	s.SetHealth(service.HealthStatus{State: service.StateHealthy})
	return nil
}

// Stop stops the gateway service.
func (s *Service) Stop(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.SetHealth(service.HealthStatus{State: service.StateStopping})

	// Close listener
	// if s.listener != nil {
	//     s.listener.Close()
	// }

	s.SetHealth(service.HealthStatus{State: service.StateStopped})
	return nil
}

// Config returns the gateway configuration.
func (s *Service) Config() Config {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.config
}

// ConnectionCount returns the current connection count.
func (s *Service) ConnectionCount() uint32 {
	// In a full implementation, this would return the actual count.
	return 0
}

// Ensure Service implements service.Service.
var _ service.Service = (*Service)(nil)
