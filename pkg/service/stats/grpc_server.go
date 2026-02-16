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
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// GRPCServer provides gRPC interface for the Statistics service.
type GRPCServer struct {
	mu       sync.RWMutex
	service  *Service
	selfAddr string
}

// NewGRPCServer creates a new Statistics gRPC server.
func NewGRPCServer(svc *Service, selfAddr string) *GRPCServer {
	return &GRPCServer{
		service:  svc,
		selfAddr: selfAddr,
	}
}

// RegisterService registers the Statistics gRPC service with a gRPC server.
func (s *GRPCServer) RegisterService(_ *grpc.Server) {
	// In full implementation:
	// pb.RegisterStatsServiceServer(grpcServer, s)
	logutil.BgLogger().Info("Statistics gRPC service registered",
		zap.String("addr", s.selfAddr))
}

// GetTableStats returns statistics for a table.
func (s *GRPCServer) GetTableStats(ctx context.Context, tableID int64) (*TableStats, error) {
	s.mu.RLock()
	svc := s.service
	s.mu.RUnlock()

	if svc == nil {
		return nil, errors.New("service not initialized")
	}

	h := svc.StatsHandle()
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
func (s *GRPCServer) LoadStats(_ context.Context, _ int64) error {
	s.mu.RLock()
	svc := s.service
	s.mu.RUnlock()

	if svc == nil {
		return errors.New("service not initialized")
	}

	// In full implementation, this would trigger stats loading
	return nil
}

// UpdateStats updates statistics for a table.
func (s *GRPCServer) UpdateStats(_ context.Context, _ int64) error {
	s.mu.RLock()
	svc := s.service
	s.mu.RUnlock()

	if svc == nil {
		return errors.New("service not initialized")
	}

	// In full implementation, this would trigger stats update
	return nil
}

// GetRowCount returns the estimated row count for a table.
func (s *GRPCServer) GetRowCount(_ context.Context, tableID int64) (int64, error) {
	s.mu.RLock()
	svc := s.service
	s.mu.RUnlock()

	if svc == nil {
		return 0, errors.New("service not initialized")
	}

	h := svc.StatsHandle()
	if h == nil {
		return 0, errors.New("statistics handle not initialized")
	}

	tbl, ok := h.Get(tableID)
	if !ok {
		return 0, nil
	}

	return tbl.RealtimeCount, nil
}

// Close closes the gRPC server.
func (s *GRPCServer) Close() error {
	return nil
}
