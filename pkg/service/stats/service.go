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

package stats

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/service"
	storageSvc "github.com/pingcap/tidb/pkg/service/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle"
)

// Config contains configuration for the statistics service.
type Config struct {
	// StatsLease is the interval for statistics update.
	StatsLease string `toml:"stats-lease" json:"stats-lease"`

	// EnableAutoAnalyze enables automatic analyze.
	EnableAutoAnalyze bool `toml:"enable-auto-analyze" json:"enable-auto-analyze"`
}

// DefaultConfig returns the default statistics service configuration.
func DefaultConfig() Config {
	return Config{
		StatsLease:        "3s",
		EnableAutoAnalyze: true,
	}
}

// Service provides statistics management functionality.
// It maintains table statistics for query optimization.
type Service struct {
	*service.BaseService

	mu          sync.RWMutex
	config      Config
	store       kv.Storage
	statsHandle *handle.Handle
}

// New creates a new statistics service.
func New() *Service {
	return &Service{
		BaseService: service.NewBaseService(
			service.ServiceStatistics,
			service.ServiceStorage,
			service.ServiceSchema,
		),
		config: DefaultConfig(),
	}
}

// Init initializes the statistics service.
func (s *Service) Init(ctx context.Context, opts service.Options) error {
	s.InitBase(opts)

	// Extract configuration
	if cfg, ok := opts.Config.(*Config); ok {
		s.config = *cfg
	} else if cfg, ok := opts.Config.(Config); ok {
		s.config = cfg
	}

	// Get storage from registry
	storageSvcAny, err := opts.Registry.GetClient(ctx, service.ServiceStorage)
	if err != nil {
		return errors.Wrap(err, "failed to get storage service")
	}
	storageService, ok := storageSvcAny.(*storageSvc.Service)
	if !ok {
		return errors.New("invalid storage service type")
	}
	s.store = storageService.Storage()

	s.SetHealth(service.HealthStatus{State: service.StateHealthy})
	return nil
}

// Start starts the statistics service.
func (s *Service) Start(_ context.Context) error {
	s.SetHealth(service.HealthStatus{State: service.StateHealthy})
	return nil
}

// Stop stops the statistics service.
func (s *Service) Stop(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.SetHealth(service.HealthStatus{State: service.StateStopping})

	if s.statsHandle != nil {
		s.statsHandle.Close()
		s.statsHandle = nil
	}

	s.SetHealth(service.HealthStatus{State: service.StateStopped})
	return nil
}

// StatsHandle returns the statistics handle.
func (s *Service) StatsHandle() *handle.Handle {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.statsHandle
}

// SetStatsHandle sets the statistics handle.
// This is useful for testing or when the handle is managed externally.
func (s *Service) SetStatsHandle(h *handle.Handle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statsHandle = h
}

// StatsClient defines the client interface for remote statistics access.
// This interface is used by other services to access statistics,
// whether locally or remotely.
type StatsClient interface {
	// GetTableStats returns statistics for a table.
	GetTableStats(ctx context.Context, tableID int64) (*TableStats, error)

	// LoadStats loads statistics for a table.
	LoadStats(ctx context.Context, tableID int64) error

	// UpdateStats updates statistics for a table.
	UpdateStats(ctx context.Context, tableID int64) error

	// GetRowCount returns the estimated row count for a table.
	GetRowCount(ctx context.Context, tableID int64) (int64, error)
}

// TableStats contains statistics for a table.
type TableStats struct {
	TableID     int64
	RowCount    int64
	ModifyCount int64
	Version     uint64
}

// Ensure Service implements service.Service.
var _ service.Service = (*Service)(nil)

// AsClient returns the service as a StatsClient.
// This allows the service to be used directly as a client in monolithic mode.
func (s *Service) AsClient() StatsClient {
	return &localClient{service: s}
}

// localClient implements StatsClient using the local service.
type localClient struct {
	service *Service
}

// GetTableStats returns statistics for a table.
func (c *localClient) GetTableStats(_ context.Context, tableID int64) (*TableStats, error) {
	h := c.service.StatsHandle()
	if h == nil {
		return nil, errors.New("statistics handle not initialized")
	}

	tbl, ok := h.Get(tableID)
	if !ok {
		return nil, errors.Errorf("statistics for table %d not found", tableID)
	}

	return &TableStats{
		TableID:     tableID,
		RowCount:    tbl.RealtimeCount,
		ModifyCount: tbl.ModifyCount,
		Version:     tbl.Version,
	}, nil
}

// LoadStats loads statistics for a table.
func (c *localClient) LoadStats(_ context.Context, _ int64) error {
	// In a full implementation, this would trigger stats loading
	return nil
}

// UpdateStats updates statistics for a table.
func (c *localClient) UpdateStats(_ context.Context, _ int64) error {
	// In a full implementation, this would trigger stats update
	return nil
}

// GetRowCount returns the estimated row count for a table.
func (c *localClient) GetRowCount(_ context.Context, tableID int64) (int64, error) {
	h := c.service.StatsHandle()
	if h == nil {
		return 0, errors.New("statistics handle not initialized")
	}

	tbl, ok := h.Get(tableID)
	if !ok {
		return 0, nil // Return 0 if no stats available
	}

	return tbl.RealtimeCount, nil
}
