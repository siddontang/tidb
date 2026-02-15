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

package ddl

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/service"
	metadataSvc "github.com/pingcap/tidb/pkg/service/metadata"
	storageSvc "github.com/pingcap/tidb/pkg/service/storage"
)

const (
	// DDLOwnerKey is the etcd key for DDL owner election.
	DDLOwnerKey = "/tidb/ddl/fg/owner"
)

// Config contains configuration for the DDL service.
type Config struct {
	// Lease is the schema lease duration.
	Lease string `toml:"lease" json:"lease"`
}

// Service provides DDL (Data Definition Language) functionality.
// It manages schema changes such as CREATE TABLE, ALTER TABLE, etc.
// In distributed mode, DDL operations are coordinated through etcd
// leader election.
type Service struct {
	*service.BaseService

	mu           sync.RWMutex
	config       Config
	store        kv.Storage
	ddlInstance  ddl.DDL
	ownerManager owner.Manager
}

// New creates a new DDL service.
func New() *Service {
	return &Service{
		BaseService: service.NewBaseService(
			service.ServiceDDL,
			service.ServiceStorage,
			service.ServiceMetadata,
			service.ServiceSchema,
		),
	}
}

// Init initializes the DDL service.
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

	// Get metadata service for owner management
	metadataSvcAny, err := opts.Registry.GetClient(ctx, service.ServiceMetadata)
	if err != nil {
		return errors.Wrap(err, "failed to get metadata service")
	}
	metadataService, ok := metadataSvcAny.(*metadataSvc.Service)
	if !ok {
		return errors.New("invalid metadata service type")
	}

	// Create owner manager for DDL leader election
	ownerMgr, err := metadataService.CreateOwnerManager(ctx, ddl.Prompt, s.store.UUID(), DDLOwnerKey)
	if err != nil {
		return errors.Wrap(err, "failed to create DDL owner manager")
	}
	s.ownerManager = ownerMgr

	s.SetHealth(service.HealthStatus{State: service.StateHealthy})
	return nil
}

// Start starts the DDL service.
func (s *Service) Start(ctx context.Context) error {
	s.SetHealth(service.HealthStatus{State: service.StateHealthy})
	return nil
}

// Stop stops the DDL service.
func (s *Service) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.SetHealth(service.HealthStatus{State: service.StateStopping})

	if s.ddlInstance != nil {
		if err := s.ddlInstance.Stop(); err != nil {
			return errors.Wrap(err, "failed to stop DDL")
		}
		s.ddlInstance = nil
	}

	s.SetHealth(service.HealthStatus{State: service.StateStopped})
	return nil
}

// DDL returns the underlying DDL instance.
func (s *Service) DDL() ddl.DDL {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ddlInstance
}

// SetDDL sets the DDL instance.
// This is useful for testing or when DDL is managed externally.
func (s *Service) SetDDL(d ddl.DDL) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ddlInstance = d
}

// OwnerManager returns the owner manager.
func (s *Service) OwnerManager() owner.Manager {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ownerManager
}

// IsOwner returns whether this instance is the DDL owner.
func (s *Service) IsOwner() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.ownerManager == nil {
		return false
	}
	return s.ownerManager.IsOwner()
}

// DDLClient defines the client interface for remote DDL access.
// This interface is used by other services to submit DDL jobs,
// whether locally or remotely.
type DDLClient interface {
	// SubmitJob submits a DDL job for execution.
	SubmitJob(ctx context.Context, job *model.Job) error

	// GetJobStatus returns the status of a DDL job.
	GetJobStatus(ctx context.Context, jobID int64) (*model.Job, error)

	// CancelJob cancels a running DDL job.
	CancelJob(ctx context.Context, jobID int64) error

	// IsOwner returns whether the connected DDL node is the owner.
	IsOwner(ctx context.Context) (bool, error)
}

// Ensure Service implements service.Service.
var _ service.Service = (*Service)(nil)

// AsClient returns the service as a DDLClient.
// This allows the service to be used directly as a client in monolithic mode.
func (s *Service) AsClient() DDLClient {
	return &localClient{service: s}
}

// localClient implements DDLClient using the local service.
type localClient struct {
	service *Service
}

// SubmitJob submits a DDL job for execution.
func (c *localClient) SubmitJob(_ context.Context, _ *model.Job) error {
	// In a full implementation, this would delegate to the DDL instance
	return errors.New("not implemented: use session context for DDL operations")
}

// GetJobStatus returns the status of a DDL job.
func (c *localClient) GetJobStatus(_ context.Context, _ int64) (*model.Job, error) {
	// In a full implementation, this would query the DDL job table
	return nil, errors.New("not implemented")
}

// CancelJob cancels a running DDL job.
func (c *localClient) CancelJob(_ context.Context, _ int64) error {
	// In a full implementation, this would cancel via DDL instance
	return errors.New("not implemented")
}

// IsOwner returns whether the connected DDL node is the owner.
func (c *localClient) IsOwner(_ context.Context) (bool, error) {
	return c.service.IsOwner(), nil
}
