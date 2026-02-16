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
	"fmt"
	"sync"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// TiDBBackend provides query execution against TiKV.
type TiDBBackend struct {
	store    kv.Storage
	dom      *domain.Domain
	sessions sync.Pool
}

// NewTiDBBackend creates a new TiDB backend.
func NewTiDBBackend(store kv.Storage, dom *domain.Domain) *TiDBBackend {
	b := &TiDBBackend{
		store: store,
		dom:   dom,
	}
	b.sessions = sync.Pool{
		New: func() any {
			sess, err := session.CreateSession(store)
			if err != nil {
				logutil.BgLogger().Error("failed to create session", zap.Error(err))
				return nil
			}
			return sess
		},
	}
	return b
}

// ExecuteQuery executes a SQL query.
func (b *TiDBBackend) ExecuteQuery(ctx context.Context, dbName, query string) (*QueryResult, error) {
	sessAny := b.sessions.Get()
	if sessAny == nil {
		return nil, fmt.Errorf("failed to get session")
	}
	sess := sessAny.(sessionapi.Session)
	defer b.sessions.Put(sess)

	// Set database if specified
	if dbName != "" {
		_, err := sess.Execute(ctx, fmt.Sprintf("USE `%s`", dbName))
		if err != nil {
			return &QueryResult{Error: err.Error()}, nil
		}
	}

	// Execute the query
	recordSets, err := sess.Execute(ctx, query)
	if err != nil {
		return &QueryResult{Error: err.Error()}, nil
	}

	if len(recordSets) == 0 {
		// No result set (DDL or DML)
		return &QueryResult{
			AffectedRows: sess.AffectedRows(),
			LastInsertID: sess.LastInsertID(),
		}, nil
	}

	// Process the first result set
	rs := recordSets[0]
	defer func() {
		for _, r := range recordSets {
			r.Close()
		}
	}()

	return b.processResultSet(rs), nil
}

func (b *TiDBBackend) processResultSet(rs sqlexec.RecordSet) *QueryResult {
	result := &QueryResult{}

	// Get columns
	fields := rs.Fields()
	for _, field := range fields {
		tableName := ""
		if field.Table != nil {
			tableName = field.Table.Name.O
		}
		col := ColumnInfo{
			Name:         field.Column.Name.O,
			OrgName:      field.Column.Name.O,
			Table:        tableName,
			OrgTable:     tableName,
			Schema:       field.DBName.O,
			Type:         byte(field.Column.GetType()),
			ColumnLength: uint32(field.Column.GetFlen()),
			Decimal:      byte(field.Column.GetDecimal()),
			Flag:         uint16(field.Column.GetFlag()),
		}
		result.Columns = append(result.Columns, col)
	}

	// Get rows
	ctx := context.Background()
	chk := rs.NewChunk(nil)
	for {
		err := rs.Next(ctx, chk)
		if err != nil {
			result.Error = err.Error()
			return result
		}
		if chk.NumRows() == 0 {
			break
		}

		for i := 0; i < chk.NumRows(); i++ {
			row := make([]string, len(fields))
			for j := 0; j < len(fields); j++ {
				if chk.GetRow(i).IsNull(j) {
					row[j] = ""
				} else {
					row[j] = formatValue(chk.GetRow(i), j, fields[j].Column.GetType())
				}
			}
			result.Rows = append(result.Rows, row)
		}
		chk.Reset()
	}

	return result
}

func formatValue(row chunk.Row, col int, tp byte) string {
	switch tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong,
		mysql.TypeInt24, mysql.TypeYear:
		return fmt.Sprintf("%d", row.GetInt64(col))
	case mysql.TypeFloat, mysql.TypeDouble:
		return fmt.Sprintf("%v", row.GetFloat64(col))
	case mysql.TypeNewDecimal:
		return row.GetMyDecimal(col).String()
	case mysql.TypeTimestamp, mysql.TypeDatetime, mysql.TypeDate:
		return row.GetTime(col).String()
	case mysql.TypeDuration:
		return row.GetDuration(col, 0).String()
	case mysql.TypeJSON:
		return row.GetJSON(col).String()
	default:
		return row.GetString(col)
	}
}

// Close closes the backend.
func (b *TiDBBackend) Close() {
	if b.dom != nil {
		b.dom.Close()
	}
	if b.store != nil {
		b.store.Close()
	}
}

// LocalBackendClient implements the service clients using the local TiDB backend.
type LocalBackendClient struct {
	backend *TiDBBackend
}

// NewLocalBackendClient creates clients that use the local backend.
func NewLocalBackendClient(backend *TiDBBackend) *ServiceClients {
	client := &LocalBackendClient{backend: backend}
	return &ServiceClients{
		DDL:   client,
		Query: client,
		Txn:   client,
		Stats: client,
	}
}

// ExecuteDDL implements DDLClient.
func (c *LocalBackendClient) ExecuteDDL(ctx context.Context, dbName, query string) (*DDLResult, error) {
	result, err := c.backend.ExecuteQuery(ctx, dbName, query)
	if err != nil {
		return &DDLResult{Error: err.Error()}, nil
	}
	if result.Error != "" {
		return &DDLResult{Error: result.Error}, nil
	}
	return &DDLResult{Success: true}, nil
}

// ExecuteQuery implements QueryClient.
func (c *LocalBackendClient) ExecuteQuery(ctx context.Context, dbName, query string) (*QueryResult, error) {
	return c.backend.ExecuteQuery(ctx, dbName, query)
}

// BeginTxn implements TxnClient.
func (c *LocalBackendClient) BeginTxn(ctx context.Context) (uint64, error) {
	_, err := c.backend.ExecuteQuery(ctx, "", "BEGIN")
	return 0, err
}

// CommitTxn implements TxnClient.
func (c *LocalBackendClient) CommitTxn(ctx context.Context) error {
	_, err := c.backend.ExecuteQuery(ctx, "", "COMMIT")
	return err
}

// RollbackTxn implements TxnClient.
func (c *LocalBackendClient) RollbackTxn(ctx context.Context) error {
	_, err := c.backend.ExecuteQuery(ctx, "", "ROLLBACK")
	return err
}

// Analyze implements StatsClient.
func (c *LocalBackendClient) Analyze(ctx context.Context, dbName, query string) error {
	_, err := c.backend.ExecuteQuery(ctx, dbName, query)
	return err
}

// GetTableStats implements StatsClient.
func (c *LocalBackendClient) GetTableStats(_ context.Context, _ int64) (*TableStats, error) {
	return &TableStats{}, nil
}

// Close implements all client interfaces.
func (c *LocalBackendClient) Close() error {
	return nil
}
