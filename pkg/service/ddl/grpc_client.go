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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// DDLLeaderPath is the etcd path for DDL leader.
	DDLLeaderPath = "/tidb/ddl/fg/owner"

	// dialTimeout is the timeout for gRPC dial.
	dialTimeout = 5 * time.Second

	// reconnectDelay is the delay before reconnecting.
	reconnectDelay = 200 * time.Millisecond
)

// ClientDiscover discovers and maintains connection to the DDL leader.
// It follows the autoid service ClientDiscover pattern.
type ClientDiscover struct {
	etcdCli *clientv3.Client

	mu struct {
		sync.RWMutex
		conn    *grpc.ClientConn
		addr    string
		version uint64
	}

	// version is used for safe reconnection
	version atomic.Uint64
}

// NewClientDiscover creates a new DDL client discover.
func NewClientDiscover(etcdCli *clientv3.Client) *ClientDiscover {
	return &ClientDiscover{
		etcdCli: etcdCli,
	}
}

// GetClient returns a DDL client connected to the current leader.
// It discovers the leader from etcd and establishes a gRPC connection.
func (d *ClientDiscover) GetClient(ctx context.Context, keyspaceID uint32) (DDLClient, uint64, error) {
	d.mu.RLock()
	if d.mu.conn != nil {
		client := &grpcClient{
			conn:       d.mu.conn,
			keyspaceID: keyspaceID,
		}
		version := d.mu.version
		d.mu.RUnlock()
		return client, version, nil
	}
	d.mu.RUnlock()

	// Need to discover leader
	addr, err := d.discoverLeader(ctx)
	if err != nil {
		return nil, 0, err
	}

	// Connect to leader
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to connect to DDL leader")
	}

	d.mu.Lock()
	// Check if another goroutine already connected
	if d.mu.conn != nil {
		d.mu.Unlock()
		conn.Close()
		return d.GetClient(ctx, keyspaceID)
	}
	d.mu.conn = conn
	d.mu.addr = addr
	d.version.Add(1)
	version := d.version.Load()
	d.mu.version = version
	d.mu.Unlock()

	logutil.BgLogger().Info("connected to DDL leader",
		zap.String("addr", addr))

	return &grpcClient{
		conn:       conn,
		keyspaceID: keyspaceID,
	}, version, nil
}

// ResetConn resets the connection if the version matches.
// This is used for safe reconnection after RPC errors.
func (d *ClientDiscover) ResetConn(version uint64, err error) {
	if !d.version.CompareAndSwap(version, version+1) {
		// Another goroutine already reset
		return
	}

	d.mu.Lock()
	oldConn := d.mu.conn
	d.mu.conn = nil
	d.mu.addr = ""
	d.mu.Unlock()

	if oldConn != nil {
		// Delay close to allow in-flight requests to complete
		go func() {
			time.Sleep(reconnectDelay)
			oldConn.Close()
		}()
	}

	logutil.BgLogger().Warn("reset DDL client connection",
		zap.Error(err))
}

// discoverLeader discovers the DDL leader from etcd.
func (d *ClientDiscover) discoverLeader(ctx context.Context) (string, error) {
	if d.etcdCli == nil {
		return "", errors.New("etcd client not initialized")
	}

	resp, err := d.etcdCli.Get(ctx, DDLLeaderPath, clientv3.WithFirstCreate()...)
	if err != nil {
		return "", errors.Wrap(err, "failed to get DDL leader from etcd")
	}

	if len(resp.Kvs) == 0 {
		return "", errors.New("no DDL leader found")
	}

	addr := string(resp.Kvs[0].Value)
	return addr, nil
}

// Close closes the client discover and releases resources.
func (d *ClientDiscover) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.mu.conn != nil {
		err := d.mu.conn.Close()
		d.mu.conn = nil
		return err
	}
	return nil
}

// grpcClient implements DDLClient using gRPC.
type grpcClient struct {
	conn       *grpc.ClientConn
	keyspaceID uint32
}

// SubmitJob submits a DDL job for execution.
func (c *grpcClient) SubmitJob(_ context.Context, _ *model.Job) error {
	// In full implementation, this would call the gRPC method
	// pb.NewDDLServiceClient(c.conn).SubmitJob(ctx, req)
	return errors.New("gRPC DDL client not fully implemented")
}

// GetJobStatus returns the status of a DDL job.
func (c *grpcClient) GetJobStatus(_ context.Context, _ int64) (*model.Job, error) {
	// In full implementation, this would call the gRPC method
	return nil, errors.New("gRPC DDL client not fully implemented")
}

// CancelJob cancels a running DDL job.
func (c *grpcClient) CancelJob(_ context.Context, _ int64) error {
	// In full implementation, this would call the gRPC method
	return errors.New("gRPC DDL client not fully implemented")
}

// IsOwner returns whether the connected DDL node is the owner.
func (c *grpcClient) IsOwner(_ context.Context) (bool, error) {
	// In full implementation, this would call the gRPC method
	return false, errors.New("gRPC DDL client not fully implemented")
}

// Ensure grpcClient implements DDLClient.
var _ DDLClient = (*grpcClient)(nil)
