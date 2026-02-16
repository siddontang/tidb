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

package copr

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/service"
	storageSvc "github.com/pingcap/tidb/pkg/service/storage"
)

// Config contains configuration for the coprocessor service.
type Config struct {
	// ConcurrencyLimit is the maximum concurrent coprocessor requests.
	ConcurrencyLimit int `toml:"concurrency-limit" json:"concurrency-limit"`

	// MaxBatchSize is the maximum batch size for coprocessor requests.
	MaxBatchSize int `toml:"max-batch-size" json:"max-batch-size"`
}

// DefaultConfig returns the default coprocessor service configuration.
func DefaultConfig() Config {
	return Config{
		ConcurrencyLimit: 0, // 0 means no limit
		MaxBatchSize:     128,
	}
}

// Service provides coprocessor functionality for distributed computation.
// It dispatches computation requests to TiKV and handles MPP (Massively
// Parallel Processing) coordination.
type Service struct {
	*service.BaseService

	mu     sync.RWMutex
	config Config
	store  kv.Storage
}

// New creates a new coprocessor service.
func New() *Service {
	return &Service{
		BaseService: service.NewBaseService(
			service.ServiceCoprocessor,
			service.ServiceStorage,
			service.ServiceSchema,
		),
		config: DefaultConfig(),
	}
}

// Init initializes the coprocessor service.
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

// Start starts the coprocessor service.
func (s *Service) Start(_ context.Context) error {
	s.SetHealth(service.HealthStatus{State: service.StateHealthy})
	return nil
}

// Stop stops the coprocessor service.
func (s *Service) Stop(_ context.Context) error {
	s.SetHealth(service.HealthStatus{State: service.StateStopping})
	s.SetHealth(service.HealthStatus{State: service.StateStopped})
	return nil
}

// Store returns the underlying storage.
func (s *Service) Store() kv.Storage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.store
}

// CoprClient defines the client interface for remote coprocessor access.
// This interface is used by the query service to execute distributed
// computations on TiKV.
type CoprClient interface {
	// ExecuteCopr executes a coprocessor request.
	ExecuteCopr(ctx context.Context, req *CoprRequest) (*CoprResponse, error)

	// StreamCopr executes a streaming coprocessor request.
	StreamCopr(ctx context.Context, req *CoprRequest) (CoprStream, error)

	// DispatchMPP dispatches an MPP task.
	DispatchMPP(ctx context.Context, req *MPPRequest) (*MPPResponse, error)
}

// CoprRequest represents a coprocessor request.
type CoprRequest struct {
	// StartTS is the transaction start timestamp.
	StartTS uint64

	// KeyRanges specifies the key ranges to process.
	KeyRanges []kv.KeyRange

	// Data contains the serialized coprocessor request.
	Data []byte

	// SchemaVersion is the schema version.
	SchemaVersion int64
}

// CoprResponse represents a coprocessor response.
type CoprResponse struct {
	// Data contains the serialized response data.
	Data []byte

	// OtherError contains any error message.
	OtherError string
}

// CoprStream represents a streaming coprocessor response.
type CoprStream interface {
	// Recv receives the next response chunk.
	Recv() (*CoprResponse, error)

	// Close closes the stream.
	Close() error
}

// MPPRequest represents an MPP task request.
type MPPRequest struct {
	// TaskID is the unique task identifier.
	TaskID int64

	// StartTS is the transaction start timestamp.
	StartTS uint64

	// TableID is the target table ID.
	TableID int64

	// Data contains the serialized MPP task.
	Data []byte
}

// MPPResponse represents an MPP task response.
type MPPResponse struct {
	// Data contains the serialized response data.
	Data []byte

	// Error contains any error message.
	Error string
}

// Ensure Service implements service.Service.
var _ service.Service = (*Service)(nil)

// AsClient returns the service as a CoprClient.
// This allows the service to be used directly as a client in monolithic mode.
func (s *Service) AsClient() CoprClient {
	return &localClient{service: s}
}

// localClient implements CoprClient using the local service.
type localClient struct {
	service *Service
}

// ExecuteCopr executes a coprocessor request.
func (c *localClient) ExecuteCopr(_ context.Context, _ *CoprRequest) (*CoprResponse, error) {
	// In a full implementation, this would delegate to distsql
	return nil, errors.New("not implemented: use distsql directly")
}

// StreamCopr executes a streaming coprocessor request.
func (c *localClient) StreamCopr(_ context.Context, _ *CoprRequest) (CoprStream, error) {
	return nil, errors.New("not implemented: use distsql directly")
}

// DispatchMPP dispatches an MPP task.
func (c *localClient) DispatchMPP(_ context.Context, _ *MPPRequest) (*MPPResponse, error) {
	return nil, errors.New("not implemented: use mpp directly")
}
