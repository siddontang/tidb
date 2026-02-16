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

package gateway

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ServiceEndpoints holds the addresses of remote services.
type ServiceEndpoints struct {
	DDL   string
	Query string
	Txn   string
	Stats string
}

// GRPCClientManager manages gRPC connections to all services.
type GRPCClientManager struct {
	endpoints ServiceEndpoints
	mu        sync.RWMutex
	clients   *ServiceClients
	conns     []*grpc.ClientConn
}

// NewGRPCClientManager creates a new client manager.
func NewGRPCClientManager(endpoints ServiceEndpoints) *GRPCClientManager {
	return &GRPCClientManager{
		endpoints: endpoints,
	}
}

// Connect establishes connections to all services.
func (m *GRPCClientManager) Connect(ctx context.Context) (*ServiceClients, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.clients != nil {
		return m.clients, nil
	}

	clients := &ServiceClients{}
	var conns []*grpc.ClientConn

	// Connect to DDL service
	if m.endpoints.DDL != "" {
		conn, err := m.dial(ctx, m.endpoints.DDL)
		if err != nil {
			m.closeConns(conns)
			return nil, errors.Wrapf(err, "failed to connect to DDL service at %s", m.endpoints.DDL)
		}
		conns = append(conns, conn)
		clients.DDL = &grpcDDLClient{conn: conn}
		logutil.BgLogger().Info("connected to DDL service", zap.String("addr", m.endpoints.DDL))
	}

	// Connect to Query service
	if m.endpoints.Query != "" {
		conn, err := m.dial(ctx, m.endpoints.Query)
		if err != nil {
			m.closeConns(conns)
			return nil, errors.Wrapf(err, "failed to connect to Query service at %s", m.endpoints.Query)
		}
		conns = append(conns, conn)
		clients.Query = &grpcQueryClient{conn: conn}
		logutil.BgLogger().Info("connected to Query service", zap.String("addr", m.endpoints.Query))
	}

	// Connect to Transaction service
	if m.endpoints.Txn != "" {
		conn, err := m.dial(ctx, m.endpoints.Txn)
		if err != nil {
			m.closeConns(conns)
			return nil, errors.Wrapf(err, "failed to connect to Txn service at %s", m.endpoints.Txn)
		}
		conns = append(conns, conn)
		clients.Txn = &grpcTxnClient{conn: conn}
		logutil.BgLogger().Info("connected to Transaction service", zap.String("addr", m.endpoints.Txn))
	}

	// Connect to Stats service
	if m.endpoints.Stats != "" {
		conn, err := m.dial(ctx, m.endpoints.Stats)
		if err != nil {
			m.closeConns(conns)
			return nil, errors.Wrapf(err, "failed to connect to Stats service at %s", m.endpoints.Stats)
		}
		conns = append(conns, conn)
		clients.Stats = &grpcStatsClient{conn: conn}
		logutil.BgLogger().Info("connected to Stats service", zap.String("addr", m.endpoints.Stats))
	}

	m.clients = clients
	m.conns = conns
	return clients, nil
}

func (m *GRPCClientManager) dial(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	return grpc.DialContext(dialCtx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
}

func (m *GRPCClientManager) closeConns(conns []*grpc.ClientConn) {
	for _, conn := range conns {
		conn.Close()
	}
}

// Close closes all connections.
func (m *GRPCClientManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closeConns(m.conns)
	m.conns = nil
	m.clients = nil
	return nil
}

// grpcDDLClient implements DDLClient using gRPC.
type grpcDDLClient struct {
	conn *grpc.ClientConn
}

func (c *grpcDDLClient) ExecuteDDL(ctx context.Context, dbName, query string) (*DDLResult, error) {
	// Use the raw gRPC connection to call the DDL service
	// The service exposes a simple JSON-based protocol over gRPC streaming
	req := &ddlRequest{
		Database: dbName,
		Query:    query,
	}

	resp, err := callService(ctx, c.conn, "DDL", "ExecuteDDL", req)
	if err != nil {
		return nil, err
	}

	result := &DDLResult{}
	if err := json.Unmarshal(resp, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *grpcDDLClient) Close() error {
	return c.conn.Close()
}

// grpcQueryClient implements QueryClient using gRPC.
type grpcQueryClient struct {
	conn *grpc.ClientConn
}

func (c *grpcQueryClient) ExecuteQuery(ctx context.Context, dbName, query string) (*QueryResult, error) {
	req := &queryRequest{
		Database: dbName,
		Query:    query,
	}

	resp, err := callService(ctx, c.conn, "Query", "ExecuteQuery", req)
	if err != nil {
		return nil, err
	}

	result := &QueryResult{}
	if err := json.Unmarshal(resp, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *grpcQueryClient) Close() error {
	return c.conn.Close()
}

// grpcTxnClient implements TxnClient using gRPC.
type grpcTxnClient struct {
	conn  *grpc.ClientConn
	txnID uint64
}

func (c *grpcTxnClient) BeginTxn(ctx context.Context) (uint64, error) {
	req := &txnRequest{Action: "begin"}

	resp, err := callService(ctx, c.conn, "Txn", "BeginTxn", req)
	if err != nil {
		return 0, err
	}

	result := &txnResponse{}
	if err := json.Unmarshal(resp, result); err != nil {
		return 0, err
	}
	c.txnID = result.TxnID
	return result.TxnID, nil
}

func (c *grpcTxnClient) CommitTxn(ctx context.Context) error {
	req := &txnRequest{Action: "commit", TxnID: c.txnID}

	_, err := callService(ctx, c.conn, "Txn", "CommitTxn", req)
	if err != nil {
		return err
	}
	c.txnID = 0
	return nil
}

func (c *grpcTxnClient) RollbackTxn(ctx context.Context) error {
	req := &txnRequest{Action: "rollback", TxnID: c.txnID}

	_, err := callService(ctx, c.conn, "Txn", "RollbackTxn", req)
	if err != nil {
		return err
	}
	c.txnID = 0
	return nil
}

func (c *grpcTxnClient) Close() error {
	return c.conn.Close()
}

// grpcStatsClient implements StatsClient using gRPC.
type grpcStatsClient struct {
	conn *grpc.ClientConn
}

func (c *grpcStatsClient) Analyze(ctx context.Context, dbName, query string) error {
	req := &statsRequest{
		Database: dbName,
		Query:    query,
	}

	_, err := callService(ctx, c.conn, "Stats", "Analyze", req)
	return err
}

func (c *grpcStatsClient) GetTableStats(ctx context.Context, tableID int64) (*TableStats, error) {
	req := &statsRequest{TableID: tableID}

	resp, err := callService(ctx, c.conn, "Stats", "GetTableStats", req)
	if err != nil {
		return nil, err
	}

	result := &TableStats{}
	if err := json.Unmarshal(resp, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *grpcStatsClient) Close() error {
	return c.conn.Close()
}

// Request/response types for JSON encoding
type ddlRequest struct {
	Database string `json:"database"`
	Query    string `json:"query"`
}

type queryRequest struct {
	Database string `json:"database"`
	Query    string `json:"query"`
}

type txnRequest struct {
	Action string `json:"action"`
	TxnID  uint64 `json:"txn_id"`
}

type txnResponse struct {
	TxnID uint64 `json:"txn_id"`
	Error string `json:"error"`
}

type statsRequest struct {
	Database string `json:"database"`
	Query    string `json:"query"`
	TableID  int64  `json:"table_id"`
}

// callService makes a gRPC call to a service.
// This is a simplified implementation using a generic service protocol.
func callService(ctx context.Context, conn *grpc.ClientConn, service, method string, req any) ([]byte, error) {
	reqData, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	// Use the generic service invoker
	var resp []byte
	err = conn.Invoke(ctx, "/tidb.service."+service+"/"+method, reqData, &resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}
