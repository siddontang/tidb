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

package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/pingcap/tidb/pkg/service"
)

// Config contains configuration for the admin service.
type Config struct {
	// Host is the host to listen on.
	Host string `toml:"host" json:"host"`

	// Port is the status port to listen on.
	Port uint `toml:"port" json:"port"`

	// EnablePProf enables pprof endpoints.
	EnablePProf bool `toml:"enable-pprof" json:"enable-pprof"`
}

// DefaultConfig returns the default admin service configuration.
func DefaultConfig() Config {
	return Config{
		Host:        "0.0.0.0",
		Port:        10080,
		EnablePProf: true,
	}
}

// Service provides HTTP admin and status endpoints.
// It handles health checks, metrics, and administrative operations.
type Service struct {
	*service.BaseService

	mu      sync.RWMutex
	config  Config
	server  *http.Server
	manager *service.Manager
}

// New creates a new admin service.
func New() *Service {
	return &Service{
		BaseService: service.NewBaseService(service.ServiceAdmin),
		config:      DefaultConfig(),
	}
}

// Init initializes the admin service.
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

// Start starts the admin service.
func (s *Service) Start(_ context.Context) error {
	// In a full implementation, this would start the HTTP server.
	// The existing status server code would be refactored here.
	s.SetHealth(service.HealthStatus{State: service.StateHealthy})
	return nil
}

// Stop stops the admin service.
func (s *Service) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.SetHealth(service.HealthStatus{State: service.StateStopping})

	if s.server != nil {
		if err := s.server.Shutdown(ctx); err != nil {
			return err
		}
		s.server = nil
	}

	s.SetHealth(service.HealthStatus{State: service.StateStopped})
	return nil
}

// SetManager sets the service manager for health aggregation.
func (s *Service) SetManager(mgr *service.Manager) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.manager = mgr
}

// Config returns the admin configuration.
func (s *Service) Config() Config {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.config
}

// HealthHandler returns an HTTP handler for health checks.
func (s *Service) HealthHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.mu.RLock()
		mgr := s.manager
		s.mu.RUnlock()

		var status service.HealthStatus
		if mgr != nil {
			status = mgr.Health()
		} else {
			status = s.Health()
		}

		if status.IsHealthy() {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	}
}

// ReadinessHandler returns an HTTP handler for readiness checks.
func (s *Service) ReadinessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.mu.RLock()
		mgr := s.manager
		s.mu.RUnlock()

		var status service.HealthStatus
		if mgr != nil {
			status = mgr.Health()
		} else {
			status = s.Health()
		}

		if status.IsReady() {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	}
}

// Ensure Service implements service.Service.
var _ service.Service = (*Service)(nil)
