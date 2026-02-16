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

package query

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// QueryRequest is the request for query execution.
type QueryRequest struct {
	Database string `json:"database"`
	Query    string `json:"query"`
}

// QueryResponse is the response for query execution.
type QueryResponse struct {
	Columns      []ColumnInfo `json:"columns"`
	Rows         [][]string   `json:"rows"`
	AffectedRows uint64       `json:"affected_rows"`
	LastInsertID uint64       `json:"last_insert_id"`
	Error        string       `json:"error"`
}

// ColumnInfo describes a result column.
type ColumnInfo struct {
	Schema       string `json:"schema"`
	Table        string `json:"table"`
	OrgTable     string `json:"org_table"`
	Name         string `json:"name"`
	OrgName      string `json:"org_name"`
	ColumnLength uint32 `json:"column_length"`
	Type         byte   `json:"type"`
	Flag         uint16 `json:"flag"`
	Decimal      byte   `json:"decimal"`
}

// GRPCHandler handles gRPC requests for the query service.
type GRPCHandler struct {
	store    kv.Storage
	dom      *domain.Domain
	addr     string
	server   *grpc.Server
	listener net.Listener
	mu       sync.RWMutex
	sessions sync.Pool
}

// NewGRPCHandler creates a new gRPC handler.
func NewGRPCHandler(store kv.Storage, dom *domain.Domain, addr string) *GRPCHandler {
	h := &GRPCHandler{
		store: store,
		dom:   dom,
		addr:  addr,
	}
	h.sessions = sync.Pool{
		New: func() any {
			sess, err := session.CreateSession(store)
			if err != nil {
				logutil.BgLogger().Error("failed to create session", zap.Error(err))
				return nil
			}
			return sess
		},
	}
	return h
}

// Start starts the gRPC server.
func (h *GRPCHandler) Start() error {
	listener, err := net.Listen("tcp", h.addr)
	if err != nil {
		return errors.Wrapf(err, "failed to listen on %s", h.addr)
	}
	h.listener = listener

	h.server = grpc.NewServer(
		grpc.UnknownServiceHandler(h.handleRequest),
	)

	go func() {
		logutil.BgLogger().Info("Query gRPC server started", zap.String("addr", h.addr))
		if err := h.server.Serve(listener); err != nil {
			logutil.BgLogger().Error("Query gRPC server error", zap.Error(err))
		}
	}()

	return nil
}

// Stop stops the gRPC server.
func (h *GRPCHandler) Stop() {
	if h.server != nil {
		h.server.GracefulStop()
	}
}

// Addr returns the server address.
func (h *GRPCHandler) Addr() string {
	if h.listener != nil {
		return h.listener.Addr().String()
	}
	return h.addr
}

func (h *GRPCHandler) handleRequest(srv any, stream grpc.ServerStream) error {
	// Receive request
	var reqData []byte
	if err := stream.RecvMsg(&reqData); err != nil {
		return err
	}

	var req QueryRequest
	if err := json.Unmarshal(reqData, &req); err != nil {
		return err
	}

	// Execute query
	resp := h.executeQuery(stream.Context(), &req)

	// Send response
	respData, err := json.Marshal(resp)
	if err != nil {
		return err
	}

	return stream.SendMsg(respData)
}

func (h *GRPCHandler) executeQuery(ctx context.Context, req *QueryRequest) *QueryResponse {
	// Get a session from the pool
	sessAny := h.sessions.Get()
	if sessAny == nil {
		return &QueryResponse{Error: "failed to get session"}
	}
	sess := sessAny.(sessionapi.Session)
	defer h.sessions.Put(sess)

	// Set database if specified
	if req.Database != "" {
		_, err := sess.Execute(ctx, fmt.Sprintf("USE `%s`", req.Database))
		if err != nil {
			return &QueryResponse{Error: err.Error()}
		}
	}

	// Execute the query
	recordSets, err := sess.Execute(ctx, req.Query)
	if err != nil {
		return &QueryResponse{Error: err.Error()}
	}

	if len(recordSets) == 0 {
		// No result set (DDL or DML)
		return &QueryResponse{
			AffectedRows: sess.AffectedRows(),
			LastInsertID: sess.LastInsertID(),
		}
	}

	// Process the first result set
	rs := recordSets[0]
	defer func() {
		for _, r := range recordSets {
			r.Close()
		}
	}()

	return h.processResultSet(rs)
}

func (h *GRPCHandler) processResultSet(rs sqlexec.RecordSet) *QueryResponse {
	resp := &QueryResponse{}

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
		resp.Columns = append(resp.Columns, col)
	}

	// Get rows
	ctx := context.Background()
	chk := rs.NewChunk(nil)
	for {
		err := rs.Next(ctx, chk)
		if err != nil {
			resp.Error = err.Error()
			return resp
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
			resp.Rows = append(resp.Rows, row)
		}
		chk.Reset()
	}

	return resp
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
