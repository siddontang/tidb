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

package query

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/service"
	storageSvc "github.com/pingcap/tidb/pkg/service/storage"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// Config contains configuration for the query service.
type Config struct {
	// MaxExecutionTime is the maximum query execution time in seconds.
	MaxExecutionTime uint64 `toml:"max-execution-time" json:"max-execution-time"`

	// MemQuotaQuery is the memory quota for a single query.
	MemQuotaQuery int64 `toml:"mem-quota-query" json:"mem-quota-query"`

	// GRPCAddr is the address for the gRPC server.
	GRPCAddr string `toml:"grpc-addr" json:"grpc-addr"`
}

// DefaultConfig returns the default query service configuration.
func DefaultConfig() Config {
	return Config{
		MaxExecutionTime: 0,       // 0 means no limit
		MemQuotaQuery:    1 << 30, // 1GB
		GRPCAddr:         "0.0.0.0:4001",
	}
}

// Service provides query planning and execution functionality.
// It combines the planner and executor components.
type Service struct {
	*service.BaseService

	mu          sync.RWMutex
	config      Config
	store       kv.Storage
	dom         *domain.Domain
	grpcHandler *GRPCHandler
}

// New creates a new query service.
func New() *Service {
	return &Service{
		BaseService: service.NewBaseService(
			service.ServiceQuery,
			service.ServiceStorage,
		),
		config: DefaultConfig(),
	}
}

// Init initializes the query service.
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

// Start starts the query service.
func (s *Service) Start(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Bootstrap domain if we have storage
	if s.store != nil && s.dom == nil {
		dom, err := session.BootstrapSession(s.store)
		if err != nil {
			return errors.Wrap(err, "failed to bootstrap domain")
		}
		s.dom = dom
	}

	// Start gRPC server if configured
	if s.config.GRPCAddr != "" && s.store != nil {
		s.grpcHandler = NewGRPCHandler(s.store, s.dom, s.config.GRPCAddr)
		if err := s.grpcHandler.Start(); err != nil {
			return errors.Wrap(err, "failed to start gRPC server")
		}
		logutil.BgLogger().Info("Query service gRPC server started",
			zap.String("addr", s.config.GRPCAddr))
	}

	s.SetHealth(service.HealthStatus{State: service.StateHealthy})
	return nil
}

// Stop stops the query service.
func (s *Service) Stop(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.SetHealth(service.HealthStatus{State: service.StateStopping})

	if s.grpcHandler != nil {
		s.grpcHandler.Stop()
		s.grpcHandler = nil
	}

	if s.dom != nil {
		s.dom.Close()
		s.dom = nil
	}

	s.SetHealth(service.HealthStatus{State: service.StateStopped})
	return nil
}

// SetStore sets the storage (for testing).
func (s *Service) SetStore(store kv.Storage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store = store
}

// SetDomain sets the domain (for testing).
func (s *Service) SetDomain(dom *domain.Domain) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dom = dom
}

// GRPCAddr returns the gRPC server address.
func (s *Service) GRPCAddr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.grpcHandler != nil {
		return s.grpcHandler.Addr()
	}
	return ""
}

// QueryClient defines the client interface for remote query execution.
type QueryClient interface {
	// Execute executes a SQL query.
	Execute(ctx context.Context, req *QueryExecRequest) (*QueryExecResponse, error)

	// Prepare prepares a SQL statement.
	Prepare(ctx context.Context, sql string) (*PreparedStatement, error)

	// ExecutePrepared executes a prepared statement.
	ExecutePrepared(ctx context.Context, stmtID uint32, params []any) (*QueryExecResponse, error)
}

// QueryExecRequest represents a query execution request.
type QueryExecRequest struct {
	// SessionID is the session identifier.
	SessionID uint64

	// SQL is the SQL query to execute.
	SQL string

	// Database is the current database.
	Database string

	// Variables contains session variables.
	Variables map[string]string
}

// QueryExecResponse represents a query execution response.
type QueryExecResponse struct {
	// Columns contains column metadata.
	Columns []*ColumnMeta

	// Rows contains the result rows.
	Rows [][]any

	// AffectedRows is the number of affected rows for DML statements.
	AffectedRows uint64

	// LastInsertID is the last insert ID for INSERT statements.
	LastInsertID uint64

	// Warnings contains any warnings generated.
	Warnings []string
}

// ColumnMeta contains column metadata.
type ColumnMeta struct {
	// Name is the column name.
	Name string

	// Type is the column type.
	Type string

	// Nullable indicates whether the column can be NULL.
	Nullable bool
}

// PreparedStatement represents a prepared statement.
type PreparedStatement struct {
	// ID is the statement ID.
	ID uint32

	// ParamCount is the number of parameters.
	ParamCount int

	// Columns contains column metadata for the result set.
	Columns []*ColumnMeta
}

// Ensure Service implements service.Service.
var _ service.Service = (*Service)(nil)

// AsClient returns the service as a QueryClient.
// This allows the service to be used directly as a client in monolithic mode.
func (s *Service) AsClient() QueryClient {
	return &localClient{service: s}
}

// localClient implements QueryClient using the local service.
type localClient struct {
	service *Service
}

// Execute executes a SQL query.
func (c *localClient) Execute(_ context.Context, _ *QueryExecRequest) (*QueryExecResponse, error) {
	// In a full implementation, this would parse, plan, and execute the query
	// using the session, planner, and executor components.
	// For now, this is a placeholder that indicates the architecture.
	return &QueryExecResponse{}, nil
}

// Prepare prepares a SQL statement.
func (c *localClient) Prepare(_ context.Context, _ string) (*PreparedStatement, error) {
	return &PreparedStatement{}, nil
}

// ExecutePrepared executes a prepared statement.
func (c *localClient) ExecutePrepared(_ context.Context, _ uint32, _ []any) (*QueryExecResponse, error) {
	return &QueryExecResponse{}, nil
}
