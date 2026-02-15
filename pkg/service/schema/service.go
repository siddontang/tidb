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

package schema

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/service"
	storageSvc "github.com/pingcap/tidb/pkg/service/storage"
)

// Config contains configuration for the schema service.
type Config struct {
	// CacheSize is the maximum number of schema versions to cache.
	CacheSize int `toml:"cache-size" json:"cache-size"`
}

// DefaultConfig returns the default schema service configuration.
func DefaultConfig() Config {
	return Config{
		CacheSize: 16,
	}
}

// Service provides schema (InfoSchema) management functionality.
// It maintains a cache of schema versions and provides both local
// and remote access to schema information.
type Service struct {
	*service.BaseService

	mu        sync.RWMutex
	config    Config
	infoCache *infoschema.InfoCache
	store     kv.Storage
}

// New creates a new schema service.
func New() *Service {
	return &Service{
		BaseService: service.NewBaseService(
			service.ServiceSchema,
			service.ServiceStorage,
			service.ServiceMetadata,
		),
		config: DefaultConfig(),
	}
}

// Init initializes the schema service.
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

	// Create info cache
	s.mu.Lock()
	s.infoCache = infoschema.NewCache(nil, s.config.CacheSize)
	s.mu.Unlock()

	s.SetHealth(service.HealthStatus{State: service.StateHealthy})
	return nil
}

// Start starts the schema service.
func (s *Service) Start(_ context.Context) error {
	s.SetHealth(service.HealthStatus{State: service.StateHealthy})
	return nil
}

// Stop stops the schema service.
func (s *Service) Stop(_ context.Context) error {
	s.SetHealth(service.HealthStatus{State: service.StateStopping})
	// InfoCache doesn't need explicit cleanup
	s.SetHealth(service.HealthStatus{State: service.StateStopped})
	return nil
}

// InfoCache returns the schema info cache.
func (s *Service) InfoCache() *infoschema.InfoCache {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.infoCache
}

// SetInfoCache sets the schema info cache.
// This is useful for testing or when the cache is managed externally.
func (s *Service) SetInfoCache(cache *infoschema.InfoCache) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.infoCache = cache
}

// GetLatest returns the latest schema version.
func (s *Service) GetLatest() infoschema.InfoSchema {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.infoCache == nil {
		return nil
	}
	return s.infoCache.GetLatest()
}

// GetByVersion returns the schema for a specific version.
func (s *Service) GetByVersion(version int64) infoschema.InfoSchema {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.infoCache == nil {
		return nil
	}
	return s.infoCache.GetByVersion(version)
}

// GetBySnapshotTS returns the schema for a specific timestamp.
func (s *Service) GetBySnapshotTS(ts uint64) infoschema.InfoSchema {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.infoCache == nil {
		return nil
	}
	return s.infoCache.GetBySnapshotTS(ts)
}

// Insert inserts a new schema version into the cache.
func (s *Service) Insert(is infoschema.InfoSchema, schemaTS uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.infoCache == nil {
		return false
	}
	return s.infoCache.Insert(is, schemaTS)
}

// SchemaClient defines the client interface for remote schema access.
// This interface is used by other services to access schema information,
// whether locally or remotely.
type SchemaClient interface {
	// GetLatestVersion returns the latest schema version number.
	GetLatestVersion(ctx context.Context) (int64, error)

	// GetTableByName returns table info by database and table name.
	GetTableByName(ctx context.Context, schema, table string) (*model.TableInfo, error)

	// GetTableByID returns table info by table ID.
	GetTableByID(ctx context.Context, tableID int64) (*model.TableInfo, error)

	// GetDatabaseByName returns database info by name.
	GetDatabaseByName(ctx context.Context, name string) (*model.DBInfo, error)

	// GetAllDatabases returns all database names.
	GetAllDatabases(ctx context.Context) ([]string, error)
}

// Ensure Service implements service.Service.
var _ service.Service = (*Service)(nil)

// AsClient returns the service as a SchemaClient.
// This allows the service to be used directly as a client in monolithic mode.
func (s *Service) AsClient() SchemaClient {
	return &localClient{service: s}
}

// localClient implements SchemaClient using the local service.
type localClient struct {
	service *Service
}

// GetLatestVersion returns the latest schema version number.
func (c *localClient) GetLatestVersion(_ context.Context) (int64, error) {
	is := c.service.GetLatest()
	if is == nil {
		return 0, errors.New("no schema available")
	}
	return is.SchemaMetaVersion(), nil
}

// GetTableByName returns table info by database and table name.
func (c *localClient) GetTableByName(_ context.Context, schema, tableName string) (*model.TableInfo, error) {
	is := c.service.GetLatest()
	if is == nil {
		return nil, errors.New("no schema available")
	}
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr(schema), ast.NewCIStr(tableName))
	if err != nil {
		return nil, err
	}
	return tbl.Meta(), nil
}

// GetTableByID returns table info by table ID.
func (c *localClient) GetTableByID(_ context.Context, tableID int64) (*model.TableInfo, error) {
	is := c.service.GetLatest()
	if is == nil {
		return nil, errors.New("no schema available")
	}
	tbl, ok := is.TableByID(context.Background(), tableID)
	if !ok {
		return nil, errors.Errorf("table %d not found", tableID)
	}
	return tbl.Meta(), nil
}

// GetDatabaseByName returns database info by name.
func (c *localClient) GetDatabaseByName(_ context.Context, name string) (*model.DBInfo, error) {
	is := c.service.GetLatest()
	if is == nil {
		return nil, errors.New("no schema available")
	}
	db, ok := is.SchemaByName(ast.NewCIStr(name))
	if !ok {
		return nil, errors.Errorf("database %q not found", name)
	}
	return db, nil
}

// GetAllDatabases returns all database names.
func (c *localClient) GetAllDatabases(_ context.Context) ([]string, error) {
	is := c.service.GetLatest()
	if is == nil {
		return nil, errors.New("no schema available")
	}
	dbs := is.AllSchemaNames()
	names := make([]string, len(dbs))
	for i, db := range dbs {
		names[i] = db.L
	}
	return names, nil
}
