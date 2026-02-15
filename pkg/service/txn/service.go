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

package txn

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/service"
	storageSvc "github.com/pingcap/tidb/pkg/service/storage"
)

// Config contains configuration for the transaction service.
type Config struct {
	// MaxTxnTTL is the maximum TTL for a transaction in milliseconds.
	MaxTxnTTL uint64 `toml:"max-txn-ttl" json:"max-txn-ttl"`
}

// DefaultConfig returns the default transaction service configuration.
func DefaultConfig() Config {
	return Config{
		MaxTxnTTL: 60 * 60 * 1000, // 1 hour
	}
}

// Service provides transaction management functionality.
// It coordinates transaction lifecycle including begin, commit, and rollback.
type Service struct {
	*service.BaseService

	mu     sync.RWMutex
	config Config
	store  kv.Storage
}

// New creates a new transaction service.
func New() *Service {
	return &Service{
		BaseService: service.NewBaseService(
			service.ServiceTransaction,
			service.ServiceStorage,
			service.ServiceMetadata,
		),
		config: DefaultConfig(),
	}
}

// Init initializes the transaction service.
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

// Start starts the transaction service.
func (s *Service) Start(_ context.Context) error {
	s.SetHealth(service.HealthStatus{State: service.StateHealthy})
	return nil
}

// Stop stops the transaction service.
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

// TxnClient defines the client interface for remote transaction access.
// This interface is used by other services to manage transactions,
// whether locally or remotely.
type TxnClient interface {
	// BeginTxn starts a new transaction.
	BeginTxn(ctx context.Context, opts TxnOptions) (*TxnHandle, error)

	// CommitTxn commits a transaction.
	CommitTxn(ctx context.Context, handle *TxnHandle) error

	// RollbackTxn rolls back a transaction.
	RollbackTxn(ctx context.Context, handle *TxnHandle) error

	// GetTimestamp gets a timestamp from PD.
	GetTimestamp(ctx context.Context) (uint64, error)
}

// TxnOptions contains options for beginning a transaction.
type TxnOptions struct {
	// StartTS is the start timestamp (0 for auto-generated).
	StartTS uint64

	// Pessimistic indicates whether this is a pessimistic transaction.
	Pessimistic bool

	// CausalConsistency indicates whether to enable causal consistency.
	CausalConsistency bool
}

// TxnHandle represents a transaction handle.
type TxnHandle struct {
	// TxnID is the unique transaction identifier.
	TxnID uint64

	// StartTS is the transaction start timestamp.
	StartTS uint64

	// Status is the current transaction status.
	Status TxnStatus
}

// TxnStatus represents the status of a transaction.
type TxnStatus int

const (
	// TxnStatusActive indicates the transaction is active.
	TxnStatusActive TxnStatus = iota
	// TxnStatusCommitted indicates the transaction is committed.
	TxnStatusCommitted
	// TxnStatusRolledBack indicates the transaction is rolled back.
	TxnStatusRolledBack
)

// Ensure Service implements service.Service.
var _ service.Service = (*Service)(nil)

// AsClient returns the service as a TxnClient.
// This allows the service to be used directly as a client in monolithic mode.
func (s *Service) AsClient() TxnClient {
	return &localClient{service: s}
}

// localClient implements TxnClient using the local service.
type localClient struct {
	service *Service
}

// BeginTxn starts a new transaction.
func (c *localClient) BeginTxn(_ context.Context, opts TxnOptions) (*TxnHandle, error) {
	store := c.service.Store()
	if store == nil {
		return nil, errors.New("storage not initialized")
	}

	var startTS uint64
	if opts.StartTS != 0 {
		startTS = opts.StartTS
	} else {
		// Use global scope by default
		ts, err := store.CurrentVersion(kv.GlobalTxnScope)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get current version")
		}
		startTS = ts.Ver
	}

	return &TxnHandle{
		TxnID:   startTS, // Use startTS as TxnID for simplicity
		StartTS: startTS,
		Status:  TxnStatusActive,
	}, nil
}

// CommitTxn commits a transaction.
func (c *localClient) CommitTxn(_ context.Context, handle *TxnHandle) error {
	if handle == nil {
		return errors.New("nil transaction handle")
	}
	handle.Status = TxnStatusCommitted
	return nil
}

// RollbackTxn rolls back a transaction.
func (c *localClient) RollbackTxn(_ context.Context, handle *TxnHandle) error {
	if handle == nil {
		return errors.New("nil transaction handle")
	}
	handle.Status = TxnStatusRolledBack
	return nil
}

// GetTimestamp gets a timestamp from PD.
func (c *localClient) GetTimestamp(_ context.Context) (uint64, error) {
	store := c.service.Store()
	if store == nil {
		return 0, errors.New("storage not initialized")
	}

	ver, err := store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get current version")
	}
	return ver.Ver, nil
}
