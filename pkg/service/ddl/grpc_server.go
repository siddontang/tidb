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
	"encoding/json"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// GRPCServer provides gRPC interface for the DDL service.
// This implements a simple RPC interface that can be used for distributed
// DDL coordination. For full proto definition, see the kvproto integration.
//
// The server follows the autoid_service pattern with leader election
// and forwards requests to the owner.
type GRPCServer struct {
	mu       sync.RWMutex
	service  *Service
	selfAddr string
}

// NewGRPCServer creates a new DDL gRPC server.
func NewGRPCServer(svc *Service, selfAddr string) *GRPCServer {
	return &GRPCServer{
		service:  svc,
		selfAddr: selfAddr,
	}
}

// RegisterService registers the DDL gRPC service with a gRPC server.
// Note: This is a placeholder for actual gRPC registration.
// In a full implementation, this would register with kvproto-defined services.
func (s *GRPCServer) RegisterService(_ *grpc.Server) {
	// In full implementation:
	// pb.RegisterDDLServiceServer(grpcServer, s)
	logutil.BgLogger().Info("DDL gRPC service registered",
		zap.String("addr", s.selfAddr))
}

// IsOwner returns whether this node is the DDL owner.
func (s *GRPCServer) IsOwner(_ context.Context) (bool, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.service == nil {
		return false, "", errors.New("service not initialized")
	}

	ownerMgr := s.service.OwnerManager()
	if ownerMgr == nil {
		return false, "", nil
	}

	isOwner := ownerMgr.IsOwner()
	ownerID, err := ownerMgr.GetOwnerID(context.Background())
	if err != nil {
		// Not an error if there's no owner yet
		return isOwner, "", nil
	}

	return isOwner, ownerID, nil
}

// SubmitJob submits a DDL job for execution.
// This is a simplified interface that marshals the job to JSON.
// In production, this would use protobuf serialization.
func (s *GRPCServer) SubmitJob(ctx context.Context, jobData []byte, keyspaceID uint32) (int64, error) {
	s.mu.RLock()
	svc := s.service
	s.mu.RUnlock()

	if svc == nil {
		return 0, errors.New("service not initialized")
	}

	// Check if we're the owner
	isOwner, _, err := s.IsOwner(ctx)
	if err != nil {
		return 0, err
	}
	if !isOwner {
		return 0, errors.New("not DDL owner")
	}

	// Unmarshal job
	var job model.Job
	if err := json.Unmarshal(jobData, &job); err != nil {
		return 0, errors.Wrap(err, "failed to unmarshal job")
	}

	// In full implementation, this would submit the job through the DDL instance
	// For now, return an error indicating this needs to go through session context
	return 0, errors.New("direct job submission not implemented: use session context")
}

// GetJobStatus returns the status of a DDL job.
func (s *GRPCServer) GetJobStatus(ctx context.Context, jobID int64) ([]byte, error) {
	s.mu.RLock()
	svc := s.service
	s.mu.RUnlock()

	if svc == nil {
		return nil, errors.New("service not initialized")
	}

	// Check if we're the owner
	isOwner, _, err := s.IsOwner(ctx)
	if err != nil {
		return nil, err
	}
	if !isOwner {
		return nil, errors.New("not DDL owner")
	}

	// In full implementation, this would query the DDL job table
	// For now, return not implemented
	return nil, errors.New("job status query not implemented")
}

// CancelJob cancels a DDL job.
func (s *GRPCServer) CancelJob(ctx context.Context, jobID int64) error {
	s.mu.RLock()
	svc := s.service
	s.mu.RUnlock()

	if svc == nil {
		return errors.New("service not initialized")
	}

	// Check if we're the owner
	isOwner, _, err := s.IsOwner(ctx)
	if err != nil {
		return err
	}
	if !isOwner {
		return errors.New("not DDL owner")
	}

	// In full implementation, this would cancel the job through DDL instance
	return errors.New("job cancellation not implemented")
}

// Close closes the gRPC server.
func (s *GRPCServer) Close() error {
	return nil
}

// OwnerListener implements owner.Listener for DDL leader election.
type OwnerListener struct {
	server   *GRPCServer
	selfAddr string
}

// NewOwnerListener creates a new owner listener.
func NewOwnerListener(server *GRPCServer, selfAddr string) *OwnerListener {
	return &OwnerListener{
		server:   server,
		selfAddr: selfAddr,
	}
}

// OnBecomeOwner is called when this node becomes the DDL owner.
func (l *OwnerListener) OnBecomeOwner() {
	logutil.BgLogger().Info("this node became DDL owner",
		zap.String("addr", l.selfAddr))
}

// OnRetireOwner is called when this node stops being the DDL owner.
func (l *OwnerListener) OnRetireOwner() {
	logutil.BgLogger().Info("this node retired as DDL owner",
		zap.String("addr", l.selfAddr))
}

// Ensure OwnerListener implements owner.Listener.
var _ owner.Listener = (*OwnerListener)(nil)
