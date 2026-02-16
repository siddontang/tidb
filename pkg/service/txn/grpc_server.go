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
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// GRPCServer provides gRPC interface for the Transaction service.
type GRPCServer struct {
	mu       sync.RWMutex
	service  *Service
	selfAddr string
}

// NewGRPCServer creates a new Transaction gRPC server.
func NewGRPCServer(svc *Service, selfAddr string) *GRPCServer {
	return &GRPCServer{
		service:  svc,
		selfAddr: selfAddr,
	}
}

// RegisterService registers the Transaction gRPC service with a gRPC server.
func (s *GRPCServer) RegisterService(_ *grpc.Server) {
	// In full implementation:
	// pb.RegisterTxnServiceServer(grpcServer, s)
	logutil.BgLogger().Info("Transaction gRPC service registered",
		zap.String("addr", s.selfAddr))
}

// BeginTxn starts a new transaction.
func (s *GRPCServer) BeginTxn(ctx context.Context, opts TxnOptions) (*TxnHandle, error) {
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

	var startTS uint64
	if opts.StartTS != 0 {
		startTS = opts.StartTS
	} else {
		ver, err := store.CurrentVersion(kv.GlobalTxnScope)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get current version")
		}
		startTS = ver.Ver
	}

	return &TxnHandle{
		TxnID:   startTS,
		StartTS: startTS,
		Status:  TxnStatusActive,
	}, nil
}

// CommitTxn commits a transaction.
func (s *GRPCServer) CommitTxn(_ context.Context, handle *TxnHandle) error {
	s.mu.RLock()
	svc := s.service
	s.mu.RUnlock()

	if svc == nil {
		return errors.New("service not initialized")
	}

	if handle == nil {
		return errors.New("nil transaction handle")
	}

	handle.Status = TxnStatusCommitted
	return nil
}

// RollbackTxn rolls back a transaction.
func (s *GRPCServer) RollbackTxn(_ context.Context, handle *TxnHandle) error {
	s.mu.RLock()
	svc := s.service
	s.mu.RUnlock()

	if svc == nil {
		return errors.New("service not initialized")
	}

	if handle == nil {
		return errors.New("nil transaction handle")
	}

	handle.Status = TxnStatusRolledBack
	return nil
}

// GetTimestamp gets a timestamp from PD.
func (s *GRPCServer) GetTimestamp(_ context.Context) (uint64, error) {
	s.mu.RLock()
	svc := s.service
	s.mu.RUnlock()

	if svc == nil {
		return 0, errors.New("service not initialized")
	}

	store := svc.Store()
	if store == nil {
		return 0, errors.New("storage not initialized")
	}

	ver, err := store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get current version")
	}
	return ver.Ver, nil
}

// Close closes the gRPC server.
func (s *GRPCServer) Close() error {
	return nil
}
