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

import "context"

// ServiceClients holds gRPC clients to other services.
type ServiceClients struct {
	DDL   DDLClient
	Query QueryClient
	Txn   TxnClient
	Stats StatsClient
}

// DDLClient interface for DDL service communication.
type DDLClient interface {
	ExecuteDDL(ctx context.Context, dbName, query string) (*DDLResult, error)
	Close() error
}

// QueryClient interface for Query service communication.
type QueryClient interface {
	ExecuteQuery(ctx context.Context, dbName, query string) (*QueryResult, error)
	Close() error
}

// TxnClient interface for Transaction service communication.
type TxnClient interface {
	BeginTxn(ctx context.Context) (uint64, error)
	CommitTxn(ctx context.Context) error
	RollbackTxn(ctx context.Context) error
	Close() error
}

// StatsClient interface for Statistics service communication.
type StatsClient interface {
	Analyze(ctx context.Context, dbName, query string) error
	GetTableStats(ctx context.Context, tableID int64) (*TableStats, error)
	Close() error
}

// DDLResult represents the result of a DDL operation.
type DDLResult struct {
	Success bool
	Error   string
}

// QueryResult represents the result of a query.
type QueryResult struct {
	Columns      []ColumnInfo
	Rows         [][]string
	AffectedRows uint64
	LastInsertID uint64
	Error        string
}

// ColumnInfo describes a result column.
type ColumnInfo struct {
	Schema       string
	Table        string
	OrgTable     string
	Name         string
	OrgName      string
	ColumnLength uint32
	Type         byte
	Flag         uint16
	Decimal      byte
}

// TableStats represents table statistics.
type TableStats struct {
	TableID     int64
	RowCount    int64
	ModifyCount int64
}
