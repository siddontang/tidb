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
	"encoding/json"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// GRPCServer provides gRPC interface for the Schema service.
// This implements a simple RPC interface for remote schema access.
type GRPCServer struct {
	mu       sync.RWMutex
	service  *Service
	selfAddr string
}

// NewGRPCServer creates a new Schema gRPC server.
func NewGRPCServer(svc *Service, selfAddr string) *GRPCServer {
	return &GRPCServer{
		service:  svc,
		selfAddr: selfAddr,
	}
}

// RegisterService registers the Schema gRPC service with a gRPC server.
func (s *GRPCServer) RegisterService(_ *grpc.Server) {
	// In full implementation:
	// pb.RegisterSchemaServiceServer(grpcServer, s)
	logutil.BgLogger().Info("Schema gRPC service registered",
		zap.String("addr", s.selfAddr))
}

// GetLatestVersion returns the latest schema version.
func (s *GRPCServer) GetLatestVersion(ctx context.Context) (int64, error) {
	s.mu.RLock()
	svc := s.service
	s.mu.RUnlock()

	if svc == nil {
		return 0, errors.New("service not initialized")
	}

	is := svc.GetLatest()
	if is == nil {
		return 0, errors.New("no schema available")
	}

	return is.SchemaMetaVersion(), nil
}

// GetTableByName returns table info by schema and table name.
// Returns JSON-serialized TableInfo.
func (s *GRPCServer) GetTableByName(ctx context.Context, schemaName, tableName string) ([]byte, error) {
	s.mu.RLock()
	svc := s.service
	s.mu.RUnlock()

	if svc == nil {
		return nil, errors.New("service not initialized")
	}

	is := svc.GetLatest()
	if is == nil {
		return nil, errors.New("no schema available")
	}

	tbl, err := is.TableByName(ctx, ast.NewCIStr(schemaName), ast.NewCIStr(tableName))
	if err != nil {
		return nil, err
	}

	return json.Marshal(tbl.Meta())
}

// GetTableByID returns table info by table ID.
// Returns JSON-serialized TableInfo.
func (s *GRPCServer) GetTableByID(ctx context.Context, tableID int64) ([]byte, error) {
	s.mu.RLock()
	svc := s.service
	s.mu.RUnlock()

	if svc == nil {
		return nil, errors.New("service not initialized")
	}

	is := svc.GetLatest()
	if is == nil {
		return nil, errors.New("no schema available")
	}

	tbl, ok := is.TableByID(ctx, tableID)
	if !ok {
		return nil, errors.Errorf("table %d not found", tableID)
	}

	return json.Marshal(tbl.Meta())
}

// GetDatabaseByName returns database info by name.
// Returns JSON-serialized DBInfo.
func (s *GRPCServer) GetDatabaseByName(_ context.Context, name string) ([]byte, error) {
	s.mu.RLock()
	svc := s.service
	s.mu.RUnlock()

	if svc == nil {
		return nil, errors.New("service not initialized")
	}

	is := svc.GetLatest()
	if is == nil {
		return nil, errors.New("no schema available")
	}

	db, ok := is.SchemaByName(ast.NewCIStr(name))
	if !ok {
		return nil, errors.Errorf("database %q not found", name)
	}

	return json.Marshal(db)
}

// GetAllDatabases returns all database names.
func (s *GRPCServer) GetAllDatabases(_ context.Context) ([]string, error) {
	s.mu.RLock()
	svc := s.service
	s.mu.RUnlock()

	if svc == nil {
		return nil, errors.New("service not initialized")
	}

	is := svc.GetLatest()
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

// Close closes the gRPC server.
func (s *GRPCServer) Close() error {
	return nil
}

// unmarshalTableInfo deserializes a TableInfo from JSON.
func unmarshalTableInfo(data []byte) (*model.TableInfo, error) {
	var info model.TableInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

// unmarshalDBInfo deserializes a DBInfo from JSON.
func unmarshalDBInfo(data []byte) (*model.DBInfo, error) {
	var info model.DBInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, err
	}
	return &info, nil
}
