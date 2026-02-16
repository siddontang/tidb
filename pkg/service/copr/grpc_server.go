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
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// GRPCServer provides gRPC interface for the Coprocessor service.
type GRPCServer struct {
	mu       sync.RWMutex
	service  *Service
	selfAddr string
}

// NewGRPCServer creates a new Coprocessor gRPC server.
func NewGRPCServer(svc *Service, selfAddr string) *GRPCServer {
	return &GRPCServer{
		service:  svc,
		selfAddr: selfAddr,
	}
}

// RegisterService registers the Coprocessor gRPC service with a gRPC server.
func (s *GRPCServer) RegisterService(_ *grpc.Server) {
	// In full implementation:
	// pb.RegisterCoprServiceServer(grpcServer, s)
	logutil.BgLogger().Info("Coprocessor gRPC service registered",
		zap.String("addr", s.selfAddr))
}

// ExecuteCopr executes a coprocessor request.
func (s *GRPCServer) ExecuteCopr(ctx context.Context, req *CoprRequest) (*CoprResponse, error) {
	s.mu.RLock()
	svc := s.service
	s.mu.RUnlock()

	if svc == nil {
		return nil, errors.New("service not initialized")
	}

	store := svc.Store()
	if store == nil {
		return nil, errors.New("storage not initialized")
	}

	// In a full implementation, this would delegate to distsql
	// For now, return a placeholder response
	return &CoprResponse{
		Data:       nil,
		OtherError: "coprocessor execution not fully implemented",
	}, nil
}

// StreamCopr executes a streaming coprocessor request.
// Returns a stream handle for receiving chunks.
func (s *GRPCServer) StreamCopr(ctx context.Context, req *CoprRequest) (CoprStream, error) {
	s.mu.RLock()
	svc := s.service
	s.mu.RUnlock()

	if svc == nil {
		return nil, errors.New("service not initialized")
	}

	// In a full implementation, this would return a streaming interface
	return nil, errors.New("streaming coprocessor not fully implemented")
}

// DispatchMPP dispatches an MPP task.
func (s *GRPCServer) DispatchMPP(ctx context.Context, req *MPPRequest) (*MPPResponse, error) {
	s.mu.RLock()
	svc := s.service
	s.mu.RUnlock()

	if svc == nil {
		return nil, errors.New("service not initialized")
	}

	// In a full implementation, this would dispatch MPP tasks to TiKV/TiFlash
	return &MPPResponse{
		Error: "MPP dispatch not fully implemented",
	}, nil
}

// GetStats returns coprocessor statistics.
func (s *GRPCServer) GetStats(ctx context.Context) (*CoprStats, error) {
	s.mu.RLock()
	svc := s.service
	s.mu.RUnlock()

	if svc == nil {
		return nil, errors.New("service not initialized")
	}

	// Return basic stats
	return &CoprStats{
		RequestCount:  0,
		ErrorCount:    0,
		ActiveStreams: 0,
	}, nil
}

// Close closes the gRPC server.
func (s *GRPCServer) Close() error {
	return nil
}

// CoprStats contains coprocessor statistics.
type CoprStats struct {
	// RequestCount is the total number of requests processed.
	RequestCount int64

	// ErrorCount is the number of failed requests.
	ErrorCount int64

	// ActiveStreams is the number of active streaming requests.
	ActiveStreams int32
}
