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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// StatsServicePath is the etcd path for stats service discovery.
	StatsServicePath = "/tidb/service/statistics"

	// dialTimeout is the timeout for gRPC dial.
	dialTimeout = 5 * time.Second

	// reconnectDelay is the delay before reconnecting.
	reconnectDelay = 200 * time.Millisecond
)

// ClientDiscover discovers and maintains connection to a statistics service.
type ClientDiscover struct {
	etcdCli *clientv3.Client

	mu struct {
		sync.RWMutex
		conn    *grpc.ClientConn
		addr    string
		version uint64
	}

	version atomic.Uint64
}

// NewClientDiscover creates a new Statistics client discover.
func NewClientDiscover(etcdCli *clientv3.Client) *ClientDiscover {
	return &ClientDiscover{
		etcdCli: etcdCli,
	}
}

// GetClient returns a Stats client connected to a statistics service.
func (d *ClientDiscover) GetClient(ctx context.Context) (StatsClient, uint64, error) {
	d.mu.RLock()
	if d.mu.conn != nil {
		client := &grpcClient{conn: d.mu.conn}
		version := d.mu.version
		d.mu.RUnlock()
		return client, version, nil
	}
	d.mu.RUnlock()

	// Need to discover service
	addr, err := d.discoverService(ctx)
	if err != nil {
		return nil, 0, err
	}

	// Connect to service
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to connect to statistics service")
	}

	d.mu.Lock()
	if d.mu.conn != nil {
		d.mu.Unlock()
		conn.Close()
		return d.GetClient(ctx)
	}
	d.mu.conn = conn
	d.mu.addr = addr
	d.version.Add(1)
	version := d.version.Load()
	d.mu.version = version
	d.mu.Unlock()

	logutil.BgLogger().Info("connected to statistics service",
		zap.String("addr", addr))

	return &grpcClient{conn: conn}, version, nil
}

// ResetConn resets the connection if the version matches.
func (d *ClientDiscover) ResetConn(version uint64, err error) {
	if !d.version.CompareAndSwap(version, version+1) {
		return
	}

	d.mu.Lock()
	oldConn := d.mu.conn
	d.mu.conn = nil
	d.mu.addr = ""
	d.mu.Unlock()

	if oldConn != nil {
		go func() {
			time.Sleep(reconnectDelay)
			oldConn.Close()
		}()
	}

	logutil.BgLogger().Warn("reset statistics client connection",
		zap.Error(err))
}

// discoverService discovers a statistics service from etcd.
func (d *ClientDiscover) discoverService(ctx context.Context) (string, error) {
	if d.etcdCli == nil {
		return "", errors.New("etcd client not initialized")
	}

	resp, err := d.etcdCli.Get(ctx, StatsServicePath, clientv3.WithPrefix(), clientv3.WithLimit(1))
	if err != nil {
		return "", errors.Wrap(err, "failed to get statistics service from etcd")
	}

	if len(resp.Kvs) == 0 {
		return "", errors.New("no statistics service found")
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

// grpcClient implements StatsClient using gRPC.
type grpcClient struct {
	conn *grpc.ClientConn
}

// GetTableStats returns statistics for a table.
func (c *grpcClient) GetTableStats(_ context.Context, _ int64) (*TableStats, error) {
	// In full implementation, this would call the gRPC method
	return nil, errors.New("gRPC stats client not fully implemented")
}

// LoadStats loads statistics for a table.
func (c *grpcClient) LoadStats(_ context.Context, _ int64) error {
	// In full implementation, this would call the gRPC method
	return errors.New("gRPC stats client not fully implemented")
}

// UpdateStats updates statistics for a table.
func (c *grpcClient) UpdateStats(_ context.Context, _ int64) error {
	// In full implementation, this would call the gRPC method
	return errors.New("gRPC stats client not fully implemented")
}

// GetRowCount returns the estimated row count for a table.
func (c *grpcClient) GetRowCount(_ context.Context, _ int64) (int64, error) {
	// In full implementation, this would call the gRPC method
	return 0, errors.New("gRPC stats client not fully implemented")
}

// Ensure grpcClient implements StatsClient.
var _ StatsClient = (*grpcClient)(nil)
