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

package session

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/service"
)

// Config contains configuration for the session service.
type Config struct {
	// MaxConnections is the maximum number of concurrent connections.
	MaxConnections uint32 `toml:"max-connections" json:"max-connections"`

	// PoolSize is the session pool size.
	PoolSize int `toml:"pool-size" json:"pool-size"`
}

// DefaultConfig returns the default session service configuration.
func DefaultConfig() Config {
	return Config{
		MaxConnections: 0, // 0 means no limit
		PoolSize:       200,
	}
}

// Service provides session management functionality.
// It handles session lifecycle, pooling, and variable management.
type Service struct {
	*service.BaseService

	mu             sync.RWMutex
	config         Config
	nextSessionID  atomic.Uint64
	activeSessions map[uint64]*SessionInfo
}

// New creates a new session service.
func New() *Service {
	return &Service{
		BaseService: service.NewBaseService(
			service.ServiceSession,
			service.ServiceStorage,
			service.ServiceSchema,
			service.ServiceStatistics,
			service.ServiceTransaction,
		),
		config:         DefaultConfig(),
		activeSessions: make(map[uint64]*SessionInfo),
	}
}

// Init initializes the session service.
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

// Start starts the session service.
func (s *Service) Start(_ context.Context) error {
	s.SetHealth(service.HealthStatus{State: service.StateHealthy})
	return nil
}

// Stop stops the session service.
func (s *Service) Stop(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.SetHealth(service.HealthStatus{State: service.StateStopping})

	// Close all active sessions
	for id := range s.activeSessions {
		delete(s.activeSessions, id)
	}

	s.SetHealth(service.HealthStatus{State: service.StateStopped})
	return nil
}

// CreateSession creates a new session.
func (s *Service) CreateSession() (*SessionInfo, error) {
	id := s.nextSessionID.Add(1)

	info := &SessionInfo{
		ID:        id,
		Variables: make(map[string]string),
	}

	s.mu.Lock()
	s.activeSessions[id] = info
	s.mu.Unlock()

	return info, nil
}

// GetSession returns a session by ID.
func (s *Service) GetSession(id uint64) (*SessionInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, ok := s.activeSessions[id]
	return info, ok
}

// CloseSession closes a session.
func (s *Service) CloseSession(id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.activeSessions, id)
	return nil
}

// ActiveSessionCount returns the number of active sessions.
func (s *Service) ActiveSessionCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.activeSessions)
}

// SessionInfo contains information about a session.
type SessionInfo struct {
	// ID is the unique session identifier.
	ID uint64

	// User is the authenticated user.
	User string

	// Database is the current database.
	Database string

	// Variables contains session variables.
	Variables map[string]string
}

// SessionClient defines the client interface for remote session access.
type SessionClient interface {
	// CreateSession creates a new session.
	CreateSession(ctx context.Context) (*SessionInfo, error)

	// GetSession returns session info by ID.
	GetSession(ctx context.Context, id uint64) (*SessionInfo, error)

	// CloseSession closes a session.
	CloseSession(ctx context.Context, id uint64) error

	// SetVariable sets a session variable.
	SetVariable(ctx context.Context, sessionID uint64, name, value string) error

	// GetVariable gets a session variable.
	GetVariable(ctx context.Context, sessionID uint64, name string) (string, error)
}

// Ensure Service implements service.Service.
var _ service.Service = (*Service)(nil)

// AsClient returns the service as a SessionClient.
// This allows the service to be used directly as a client in monolithic mode.
func (s *Service) AsClient() SessionClient {
	return &localClient{service: s}
}

// localClient implements SessionClient using the local service.
type localClient struct {
	service *Service
}

// CreateSession creates a new session.
func (c *localClient) CreateSession(_ context.Context) (*SessionInfo, error) {
	return c.service.CreateSession()
}

// GetSession returns session info by ID.
func (c *localClient) GetSession(_ context.Context, id uint64) (*SessionInfo, error) {
	info, ok := c.service.GetSession(id)
	if !ok {
		return nil, errors.Errorf("session %d not found", id)
	}
	return info, nil
}

// CloseSession closes a session.
func (c *localClient) CloseSession(_ context.Context, id uint64) error {
	return c.service.CloseSession(id)
}

// SetVariable sets a session variable.
func (c *localClient) SetVariable(_ context.Context, sessionID uint64, name, value string) error {
	info, ok := c.service.GetSession(sessionID)
	if !ok {
		return errors.Errorf("session %d not found", sessionID)
	}
	info.Variables[name] = value
	return nil
}

// GetVariable gets a session variable.
func (c *localClient) GetVariable(_ context.Context, sessionID uint64, name string) (string, error) {
	info, ok := c.service.GetSession(sessionID)
	if !ok {
		return "", errors.Errorf("session %d not found", sessionID)
	}
	return info.Variables[name], nil
}
