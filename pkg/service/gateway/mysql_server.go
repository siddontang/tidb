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
	"io"
	"net"
	"sync"
	"sync/atomic"

	"crypto/rand"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// MySQLServer handles MySQL protocol connections for the gateway.
type MySQLServer struct {
	cfg      Config
	listener net.Listener
	clients  *ServiceClients

	mu       sync.RWMutex
	connID   atomic.Uint64
	conns    map[uint64]*clientConn
	closed   atomic.Bool
	wg       sync.WaitGroup
}

// clientConn represents a client connection.
type clientConn struct {
	id       uint64
	conn     net.Conn
	server   *MySQLServer
	salt     []byte
	user     string
	dbname   string
	parser   *parser.Parser
	ctx      context.Context
	cancel   context.CancelFunc
	sequence byte // MySQL packet sequence number
}

// NewMySQLServer creates a new MySQL protocol server.
func NewMySQLServer(cfg Config, clients *ServiceClients) *MySQLServer {
	return &MySQLServer{
		cfg:     cfg,
		clients: clients,
		conns:   make(map[uint64]*clientConn),
	}
}

// Start starts the MySQL server.
func (s *MySQLServer) Start() error {
	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.Wrapf(err, "failed to listen on %s", addr)
	}
	s.listener = listener

	logutil.BgLogger().Info("MySQL server started",
		zap.String("addr", addr))

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// Stop stops the MySQL server.
func (s *MySQLServer) Stop() error {
	if s.closed.Swap(true) {
		return nil
	}

	if s.listener != nil {
		s.listener.Close()
	}

	// Close all connections
	s.mu.Lock()
	for _, conn := range s.conns {
		conn.close()
	}
	s.mu.Unlock()

	s.wg.Wait()
	return nil
}

func (s *MySQLServer) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.closed.Load() {
				return
			}
			logutil.BgLogger().Error("accept error", zap.Error(err))
			continue
		}

		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *MySQLServer) handleConn(conn net.Conn) {
	defer s.wg.Done()

	connID := s.connID.Add(1)
	ctx, cancel := context.WithCancel(context.Background())

	cc := &clientConn{
		id:     connID,
		conn:   conn,
		server: s,
		salt:   randomBuf(20),
		parser: parser.New(),
		ctx:    ctx,
		cancel: cancel,
	}

	s.mu.Lock()
	s.conns[connID] = cc
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.conns, connID)
		s.mu.Unlock()
		cc.close()
	}()

	if err := cc.handshake(); err != nil {
		logutil.BgLogger().Debug("handshake failed", zap.Error(err))
		return
	}

	cc.run()
}

func (cc *clientConn) close() {
	cc.cancel()
	cc.conn.Close()
}

func (cc *clientConn) handshake() error {
	// Send initial handshake packet
	if err := cc.writeInitialHandshake(); err != nil {
		logutil.BgLogger().Error("failed to write initial handshake", zap.Error(err))
		return err
	}

	// Read client response
	if err := cc.readHandshakeResponse(); err != nil {
		logutil.BgLogger().Error("failed to read handshake response", zap.Error(err))
		return err
	}

	// Send OK packet
	if err := cc.writeOK(); err != nil {
		logutil.BgLogger().Error("failed to write OK", zap.Error(err))
		return err
	}

	logutil.BgLogger().Info("client connected", zap.Uint64("connID", cc.id), zap.String("user", cc.user))
	return nil
}

func (cc *clientConn) writeInitialHandshake() error {
	// MySQL protocol version 10
	data := make([]byte, 0, 128)

	// Protocol version
	data = append(data, 10)

	// Server version
	serverVersion := "8.0.11-TiDB-Gateway"
	data = append(data, serverVersion...)
	data = append(data, 0)

	// Connection ID (4 bytes)
	data = append(data, byte(cc.id), byte(cc.id>>8), byte(cc.id>>16), byte(cc.id>>24))

	// Auth plugin data part 1 (8 bytes)
	data = append(data, cc.salt[:8]...)

	// Filler
	data = append(data, 0)

	// Capability flags lower 2 bytes
	capability := mysql.ClientLongPassword | mysql.ClientLongFlag |
		mysql.ClientConnectWithDB | mysql.ClientProtocol41 |
		mysql.ClientTransactions | mysql.ClientSecureConnection |
		mysql.ClientPluginAuth
	data = append(data, byte(capability), byte(capability>>8))

	// Character set
	data = append(data, mysql.DefaultCollationID)

	// Status flags (2 bytes)
	data = append(data, 0, 0)

	// Capability flags upper 2 bytes
	data = append(data, byte(capability>>16), byte(capability>>24))

	// Length of auth plugin data
	data = append(data, 21)

	// Reserved (10 bytes)
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

	// Auth plugin data part 2 (12 bytes + null terminator)
	data = append(data, cc.salt[8:]...)
	data = append(data, 0)

	// Auth plugin name
	data = append(data, "mysql_native_password"...)
	data = append(data, 0)

	return cc.writePacket(data)
}

func (cc *clientConn) readHandshakeResponse() error {
	data, err := cc.readPacket()
	if err != nil {
		return err
	}

	// Parse capability flags (4 bytes)
	pos := 0
	if len(data) < 4 {
		return errors.New("invalid handshake response")
	}
	capability := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
	pos += 4

	// Max packet size (4 bytes)
	if len(data) < pos+4 {
		return errors.New("invalid handshake response")
	}
	pos += 4

	// Character set (1 byte)
	if len(data) < pos+1 {
		return errors.New("invalid handshake response")
	}
	pos++

	// Reserved (23 bytes)
	pos += 23

	// Username (null-terminated)
	if pos >= len(data) {
		return errors.New("invalid handshake response")
	}
	end := pos
	for end < len(data) && data[end] != 0 {
		end++
	}
	cc.user = string(data[pos:end])
	pos = end + 1

	// Skip auth response (length-encoded string or fixed)
	if pos < len(data) {
		// Check if using length-encoded auth response
		if capability&mysql.ClientPluginAuthLenencClientData > 0 {
			// Length-encoded
			authLen := int(data[pos])
			pos++
			pos += authLen
		} else if capability&mysql.ClientSecureConnection > 0 {
			// Fixed length (1 byte length + data)
			if pos < len(data) {
				authLen := int(data[pos])
				pos++
				pos += authLen
			}
		} else {
			// Null-terminated
			for pos < len(data) && data[pos] != 0 {
				pos++
			}
			pos++
		}
	}

	// Database name (null-terminated) if CLIENT_CONNECT_WITH_DB flag is set
	if capability&mysql.ClientConnectWithDB > 0 && pos < len(data) {
		end = pos
		for end < len(data) && data[end] != 0 {
			end++
		}
		cc.dbname = string(data[pos:end])
		logutil.BgLogger().Debug("client connected with database",
			zap.String("database", cc.dbname))
	}

	return nil
}

func (cc *clientConn) run() {
	for {
		// Reset sequence number for each command
		cc.sequence = 0

		data, err := cc.readPacket()
		if err != nil {
			if err != io.EOF {
				logutil.BgLogger().Debug("read packet error", zap.Error(err))
			}
			return
		}

		if len(data) == 0 {
			continue
		}

		cmd := data[0]
		data = data[1:]

		switch cmd {
		case mysql.ComQuit:
			return
		case mysql.ComInitDB:
			cc.dbname = string(data)
			if err := cc.writeOK(); err != nil {
				return
			}
		case mysql.ComQuery:
			if err := cc.handleQuery(string(data)); err != nil {
				logutil.BgLogger().Debug("query error", zap.Error(err))
				if err := cc.writeError(err); err != nil {
					return
				}
			}
		case mysql.ComFieldList:
			if err := cc.writeOK(); err != nil {
				return
			}
		case mysql.ComPing:
			if err := cc.writeOK(); err != nil {
				return
			}
		default:
			if err := cc.writeError(errors.Errorf("command %d not supported", cmd)); err != nil {
				return
			}
		}
	}
}

func (cc *clientConn) handleQuery(query string) error {
	// Parse the query
	stmts, _, err := cc.parser.Parse(query, "", "")
	if err != nil {
		return err
	}

	if len(stmts) == 0 {
		return cc.writeOK()
	}

	// Route to appropriate service based on statement type
	for _, stmt := range stmts {
		if err := cc.executeStmt(stmt, query); err != nil {
			return err
		}
	}

	return nil
}

func (cc *clientConn) executeStmt(stmt ast.StmtNode, query string) error {
	ctx := cc.ctx

	switch stmt.(type) {
	// DDL statements -> DDL service
	case *ast.CreateDatabaseStmt, *ast.DropDatabaseStmt,
		*ast.CreateTableStmt, *ast.DropTableStmt, *ast.AlterTableStmt,
		*ast.CreateIndexStmt, *ast.DropIndexStmt,
		*ast.TruncateTableStmt, *ast.RenameTableStmt:
		return cc.executeDDL(ctx, query)

	// DML statements -> Query service (which uses Txn service)
	case *ast.SelectStmt:
		return cc.executeSelect(ctx, query)

	case *ast.InsertStmt, *ast.UpdateStmt, *ast.DeleteStmt:
		return cc.executeDML(ctx, query)

	// Transaction control
	case *ast.BeginStmt:
		return cc.executeBegin(ctx)

	case *ast.CommitStmt:
		return cc.executeCommit(ctx)

	case *ast.RollbackStmt:
		return cc.executeRollback(ctx)

	// Statistics
	case *ast.AnalyzeTableStmt:
		return cc.executeAnalyze(ctx, query)

	// Show statements
	case *ast.ShowStmt:
		return cc.executeShow(ctx, query, stmt.(*ast.ShowStmt))

	// Use statement
	case *ast.UseStmt:
		cc.dbname = stmt.(*ast.UseStmt).DBName
		return cc.writeOK()

	// Set statements
	case *ast.SetStmt:
		return cc.writeOK()

	default:
		// For other statements, try to execute via query service
		return cc.executeSelect(ctx, query)
	}
}

func (cc *clientConn) executeDDL(ctx context.Context, query string) error {
	if cc.server.clients == nil || cc.server.clients.DDL == nil {
		return errors.New("DDL service not available")
	}

	result, err := cc.server.clients.DDL.ExecuteDDL(ctx, cc.dbname, query)
	if err != nil {
		return err
	}

	if result.Error != "" {
		return errors.New(result.Error)
	}

	return cc.writeOK()
}

func (cc *clientConn) executeSelect(ctx context.Context, query string) error {
	if cc.server.clients == nil || cc.server.clients.Query == nil {
		return errors.New("Query service not available")
	}

	result, err := cc.server.clients.Query.ExecuteQuery(ctx, cc.dbname, query)
	if err != nil {
		return err
	}

	if result.Error != "" {
		return errors.New(result.Error)
	}

	return cc.writeResultSet(result)
}

func (cc *clientConn) executeDML(ctx context.Context, query string) error {
	if cc.server.clients == nil || cc.server.clients.Query == nil {
		return errors.New("Query service not available")
	}

	result, err := cc.server.clients.Query.ExecuteQuery(ctx, cc.dbname, query)
	if err != nil {
		return err
	}

	if result.Error != "" {
		return errors.New(result.Error)
	}

	return cc.writeOKWithAffected(result.AffectedRows, result.LastInsertID)
}

func (cc *clientConn) executeBegin(ctx context.Context) error {
	if cc.server.clients == nil || cc.server.clients.Txn == nil {
		return errors.New("Transaction service not available")
	}

	_, err := cc.server.clients.Txn.BeginTxn(ctx)
	if err != nil {
		return err
	}

	return cc.writeOK()
}

func (cc *clientConn) executeCommit(ctx context.Context) error {
	if cc.server.clients == nil || cc.server.clients.Txn == nil {
		return errors.New("Transaction service not available")
	}

	err := cc.server.clients.Txn.CommitTxn(ctx)
	if err != nil {
		return err
	}

	return cc.writeOK()
}

func (cc *clientConn) executeRollback(ctx context.Context) error {
	if cc.server.clients == nil || cc.server.clients.Txn == nil {
		return errors.New("Transaction service not available")
	}

	err := cc.server.clients.Txn.RollbackTxn(ctx)
	if err != nil {
		return err
	}

	return cc.writeOK()
}

func (cc *clientConn) executeAnalyze(ctx context.Context, query string) error {
	if cc.server.clients == nil || cc.server.clients.Stats == nil {
		return errors.New("Statistics service not available")
	}

	err := cc.server.clients.Stats.Analyze(ctx, cc.dbname, query)
	if err != nil {
		return err
	}

	return cc.writeOK()
}

func (cc *clientConn) executeShow(ctx context.Context, query string, stmt *ast.ShowStmt) error {
	// Handle SHOW statements via query service
	return cc.executeSelect(ctx, query)
}

func (cc *clientConn) writePacket(data []byte) error {
	// MySQL packet format: 3-byte length + 1-byte sequence + payload
	length := len(data)
	buf := make([]byte, 4+length)
	buf[0] = byte(length)
	buf[1] = byte(length >> 8)
	buf[2] = byte(length >> 16)
	buf[3] = cc.sequence
	cc.sequence++
	copy(buf[4:], data)

	_, err := cc.conn.Write(buf)
	return err
}

func (cc *clientConn) readPacket() ([]byte, error) {
	// Read header (4 bytes)
	header := make([]byte, 4)
	if _, err := io.ReadFull(cc.conn, header); err != nil {
		return nil, err
	}

	// Parse length
	length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	// Update sequence from packet
	cc.sequence = header[3] + 1

	if length == 0 {
		return nil, nil
	}

	// Read payload
	data := make([]byte, length)
	if _, err := io.ReadFull(cc.conn, data); err != nil {
		return nil, err
	}

	return data, nil
}

func (cc *clientConn) writeOK() error {
	return cc.writeOKWithAffected(0, 0)
}

func (cc *clientConn) writeOKWithAffected(affected, lastInsertID uint64) error {
	data := make([]byte, 0, 32)
	data = append(data, 0x00) // OK packet header

	// Affected rows (length-encoded int)
	data = appendLengthEncodedInt(data, affected)

	// Last insert ID (length-encoded int)
	data = appendLengthEncodedInt(data, lastInsertID)

	// Status flags (2 bytes)
	data = append(data, 0, 0)

	// Warnings (2 bytes)
	data = append(data, 0, 0)

	return cc.writePacket(data)
}

func (cc *clientConn) writeError(err error) error {
	data := make([]byte, 0, 64)
	data = append(data, 0xff) // ERR packet header

	// Error code (2 bytes)
	data = append(data, 0x00, 0x04) // 1024 - unknown error

	// SQL state marker
	data = append(data, '#')

	// SQL state (5 bytes)
	data = append(data, "HY000"...)

	// Error message
	data = append(data, err.Error()...)

	return cc.writePacket(data)
}

func (cc *clientConn) writeResultSet(result *QueryResult) error {
	if len(result.Columns) == 0 {
		return cc.writeOK()
	}

	// Column count packet
	data := make([]byte, 0, 16)
	data = appendLengthEncodedInt(data, uint64(len(result.Columns)))
	if err := cc.writePacket(data); err != nil {
		return err
	}

	// Column definition packets
	for _, col := range result.Columns {
		if err := cc.writeColumnDef(col); err != nil {
			return err
		}
	}

	// EOF packet (before rows)
	if err := cc.writeEOF(); err != nil {
		return err
	}

	// Row packets
	for _, row := range result.Rows {
		if err := cc.writeRow(row); err != nil {
			return err
		}
	}

	// EOF packet (after rows)
	return cc.writeEOF()
}

func (cc *clientConn) writeColumnDef(col ColumnInfo) error {
	data := make([]byte, 0, 128)

	// Catalog (always "def")
	data = appendLengthEncodedString(data, "def")

	// Schema
	data = appendLengthEncodedString(data, col.Schema)

	// Table
	data = appendLengthEncodedString(data, col.Table)

	// Org table
	data = appendLengthEncodedString(data, col.OrgTable)

	// Name
	data = appendLengthEncodedString(data, col.Name)

	// Org name
	data = appendLengthEncodedString(data, col.OrgName)

	// Length of fixed-length fields [0c]
	data = append(data, 0x0c)

	// Character set (2 bytes)
	data = append(data, mysql.DefaultCollationID, 0)

	// Column length (4 bytes)
	data = append(data, byte(col.ColumnLength), byte(col.ColumnLength>>8),
		byte(col.ColumnLength>>16), byte(col.ColumnLength>>24))

	// Column type (1 byte)
	data = append(data, col.Type)

	// Flags (2 bytes)
	data = append(data, byte(col.Flag), byte(col.Flag>>8))

	// Decimals (1 byte)
	data = append(data, col.Decimal)

	// Filler (2 bytes)
	data = append(data, 0, 0)

	return cc.writePacket(data)
}

func (cc *clientConn) writeRow(row []string) error {
	data := make([]byte, 0, 256)

	for _, val := range row {
		if val == "" {
			data = append(data, 0xfb) // NULL
		} else {
			data = appendLengthEncodedString(data, val)
		}
	}

	return cc.writePacket(data)
}

func (cc *clientConn) writeEOF() error {
	data := []byte{0xfe, 0, 0, 0, 0}
	return cc.writePacket(data)
}

func appendLengthEncodedInt(data []byte, n uint64) []byte {
	switch {
	case n < 251:
		return append(data, byte(n))
	case n < 1<<16:
		return append(data, 0xfc, byte(n), byte(n>>8))
	case n < 1<<24:
		return append(data, 0xfd, byte(n), byte(n>>8), byte(n>>16))
	default:
		return append(data, 0xfe, byte(n), byte(n>>8), byte(n>>16), byte(n>>24),
			byte(n>>32), byte(n>>40), byte(n>>48), byte(n>>56))
	}
}

func appendLengthEncodedString(data []byte, s string) []byte {
	data = appendLengthEncodedInt(data, uint64(len(s)))
	return append(data, s...)
}

// randomBuf generates random bytes.
func randomBuf(size int) []byte {
	buf := make([]byte, size)
	rand.Read(buf)
	// Ensure no zeros in the salt (MySQL protocol requirement)
	for i := range buf {
		if buf[i] == 0 {
			buf[i] = byte(i%254 + 1)
		}
	}
	return buf
}
