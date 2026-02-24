// Copyright 2015 PingCAP, Inc.
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

// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// The MIT License (MIT)
//
// Copyright (c) 2014 wandoulabs
// Copyright (c) 2014 siddontang
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os/user"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/plugin"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/privilege/privileges/ldap"
	"github.com/pingcap/tidb/pkg/resourcegroup"
	servererr "github.com/pingcap/tidb/pkg/server/err"
	"github.com/pingcap/tidb/pkg/server/internal"
	"github.com/pingcap/tidb/pkg/server/internal/column"
	"github.com/pingcap/tidb/pkg/server/internal/dump"
	"github.com/pingcap/tidb/pkg/server/internal/handshake"
	"github.com/pingcap/tidb/pkg/server/internal/parse"
	util2 "github.com/pingcap/tidb/pkg/server/internal/util"
	server_metrics "github.com/pingcap/tidb/pkg/server/metrics"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/arena"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	tlsutil "github.com/pingcap/tidb/pkg/util/tls"
	"github.com/pingcap/tidb/pkg/util/traceevent"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	connStatusDispatching int32 = iota
	connStatusReading
	connStatusShutdown     = variable.ConnStatusShutdown // Closed by server.
	connStatusWaitShutdown = 3                           // Notified by server to close.

	tidbGatewayAttrsConnKey = "TiDB-Gateway-ConnID"
)

var (
	statusCompression          = "Compression"
	statusCompressionAlgorithm = "Compression_algorithm"
	statusCompressionLevel     = "Compression_level"
)

var (
	// ConnectionInMemCounterForTest is a variable to count live connection object
	ConnectionInMemCounterForTest = atomic.Int64{}
)

// newClientConn creates a *clientConn object.
func newClientConn(s *Server) *clientConn {
	cc := &clientConn{
		server:       s,
		connectionID: s.dom.NextConnID(),
		collation:    mysql.DefaultCollationID,
		alloc:        arena.NewAllocator(32 * 1024),
		chunkAlloc:   chunk.NewAllocator(),
		status:       connStatusDispatching,
		lastActive:   time.Now(),
		authPlugin:   mysql.AuthNativePassword,
		quit:         make(chan struct{}),
		ppEnabled:    s.cfg.ProxyProtocol.Networks != "",
	}

	if intest.InTest {
		ConnectionInMemCounterForTest.Add(1)
		runtime.SetFinalizer(cc, func(*clientConn) {
			ConnectionInMemCounterForTest.Add(-1)
		})
	}
	return cc
}

// clientConn represents a connection between server and client, it maintains connection specific state,
// handles client query.
type clientConn struct {
	pkt          *internal.PacketIO      // a helper to read and write data in packet format.
	bufReadConn  *util2.BufferedReadConn // a buffered-read net.Conn or buffered-read tls.Conn.
	tlsConn      *tls.Conn               // TLS connection, nil if not TLS.
	server       *Server                 // a reference of server instance.
	capability   uint32                  // client capability affects the way server handles client request.
	connectionID uint64                  // atomically allocated by a global variable, unique in process scope.
	user         string                  // user of the client.
	dbname       string                  // default database name.
	salt         []byte                  // random bytes used for authentication.
	alloc        arena.Allocator         // an memory allocator for reducing memory allocation.
	chunkAlloc   chunk.Allocator
	lastPacket   []byte // latest sql query string, currently used for logging error.
	// ShowProcess() and mysql.ComChangeUser both visit this field, ShowProcess() read information through
	// the TiDBContext and mysql.ComChangeUser re-create it, so a lock is required here.
	ctx struct {
		sync.RWMutex
		*TiDBContext // an interface to execute sql statements.
	}
	attrs         map[string]string     // attributes parsed from client handshake response.
	serverHost    string                // server host
	peerHost      string                // peer host
	peerPort      string                // peer port
	status        int32                 // dispatching/reading/shutdown/waitshutdown
	lastCode      uint16                // last error code
	collation     uint8                 // collation used by client, may be different from the collation used by database.
	lastActive    time.Time             // last active time
	authPlugin    string                // default authentication plugin
	isUnixSocket  bool                  // connection is Unix Socket file
	closeOnce     sync.Once             // closeOnce is used to make sure clientConn closes only once
	rsEncoder     *column.ResultEncoder // rsEncoder is used to encode the string result to different charsets
	inputDecoder  *util2.InputDecoder   // inputDecoder is used to decode the different charsets of incoming strings to utf-8
	socketCredUID uint32                // UID from the other end of the Unix Socket
	// mu is used for cancelling the execution of current transaction.
	mu struct {
		sync.RWMutex
		cancelFunc context.CancelFunc
	}
	// quit is close once clientConn quit Run().
	quit       chan struct{}
	extensions *extension.SessionExtensions

	// Proxy Protocol Enabled
	ppEnabled bool
}

type userResourceLimits struct {
	connections int
}

func (cc *clientConn) getCtx() *TiDBContext {
	cc.ctx.RLock()
	defer cc.ctx.RUnlock()
	return cc.ctx.TiDBContext
}

func (cc *clientConn) SetCtx(ctx *TiDBContext) {
	cc.ctx.Lock()
	cc.ctx.TiDBContext = ctx
	cc.ctx.Unlock()
}

func (cc *clientConn) String() string {
	// MySQL converts a collation from u32 to char in the protocol, so the value could be wrong. It works fine for the
	// default parameters (and libmysql seems not to provide any way to specify the collation other than the default
	// one), so it's not a big problem.
	collationStr := mysql.Collations[uint16(cc.collation)]
	return fmt.Sprintf("id:%d, addr:%s status:%b, collation:%s, user:%s",
		cc.connectionID, cc.bufReadConn.RemoteAddr(), cc.ctx.Status(), collationStr, cc.user,
	)
}

func (cc *clientConn) setStatus(status int32) {
	atomic.StoreInt32(&cc.status, status)
	if ctx := cc.getCtx(); ctx != nil {
		atomic.StoreInt32(&ctx.GetSessionVars().ConnectionStatus, status)
	}
}

func (cc *clientConn) getStatus() int32 {
	return atomic.LoadInt32(&cc.status)
}

func (cc *clientConn) CompareAndSwapStatus(oldStatus, newStatus int32) bool {
	return atomic.CompareAndSwapInt32(&cc.status, oldStatus, newStatus)
}

// authSwitchRequest is used by the server to ask the client to switch to a different authentication
// plugin. MySQL 8.0 libmysqlclient based clients by default always try `caching_sha2_password`, even
// when the server advertises the its default to be `mysql_native_password`. In addition to this switching
// may be needed on a per user basis as the authentication method is set per user.
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_request.html
// https://bugs.mysql.com/bug.php?id=93044
func (cc *clientConn) authSwitchRequest(ctx context.Context, plugin string) ([]byte, error) {
	clientPlugin := plugin
	if plugin == mysql.AuthLDAPSASL {
		clientPlugin += "_client"
	} else if plugin == mysql.AuthLDAPSimple {
		clientPlugin = mysql.AuthMySQLClearPassword
	} else if authPluginImpl, ok := cc.extensions.GetAuthPlugin(plugin); ok {
		if authPluginImpl.RequiredClientSidePlugin != "" {
			clientPlugin = authPluginImpl.RequiredClientSidePlugin
		} else {
			// If RequiredClientSidePlugin is empty, use the plugin name as the client plugin.
			clientPlugin = authPluginImpl.Name
		}
	}
	failpoint.Inject("FakeAuthSwitch", func() {
		failpoint.Return([]byte(clientPlugin), nil)
	})
	enclen := 1 + len(clientPlugin) + 1 + len(cc.salt) + 1
	data := cc.alloc.AllocWithLen(4, enclen)
	data = append(data, mysql.AuthSwitchRequest) // switch request
	data = append(data, []byte(clientPlugin)...)
	data = append(data, byte(0x00)) // requires null
	if plugin == mysql.AuthLDAPSASL {
		// append sasl auth method name
		data = append(data, []byte(ldap.LDAPSASLAuthImpl.GetSASLAuthMethod())...)
		data = append(data, byte(0x00))
	} else {
		data = append(data, cc.salt...)
		data = append(data, 0)
	}
	err := cc.writePacket(data)
	if err != nil {
		logutil.Logger(ctx).Debug("write response to client failed", zap.Error(err))
		return nil, err
	}
	err = cc.flush(ctx)
	if err != nil {
		logutil.Logger(ctx).Debug("flush response to client failed", zap.Error(err))
		return nil, err
	}
	resp, err := cc.readPacket()
	if err != nil {
		err = errors.SuspendStack(err)
		if errors.Cause(err) == io.EOF {
			logutil.Logger(ctx).Warn("authSwitchRequest response fail due to connection has be closed by client-side")
		} else {
			logutil.Logger(ctx).Warn("authSwitchRequest response fail", zap.Error(err))
		}
		return nil, err
	}
	cc.authPlugin = plugin
	return resp, nil
}

// handshake works like TCP handshake, but in a higher level, it first writes initial packet to client,
// during handshake, client and server negotiate compatible features and do authentication.
// After handshake, client can send sql query to server.
func (cc *clientConn) handshake(ctx context.Context) error {
	if err := cc.writeInitialHandshake(ctx); err != nil {
		if errors.Cause(err) == io.EOF {
			logutil.Logger(ctx).Debug("Could not send handshake due to connection has be closed by client-side")
		} else {
			logutil.Logger(ctx).Debug("Write init handshake to client fail", zap.Error(errors.SuspendStack(err)))
		}
		return err
	}
	if err := cc.readOptionalSSLRequestAndHandshakeResponse(ctx); err != nil {
		err1 := cc.writeError(ctx, err)
		if err1 != nil {
			logutil.Logger(ctx).Debug("writeError failed", zap.Error(err1))
		}
		return err
	}

	// MySQL supports an "init_connect" query, which can be run on initial connection.
	// The query must return a non-error or the client is disconnected.
	if err := cc.initConnect(ctx); err != nil {
		logutil.Logger(ctx).Warn("init_connect failed", zap.Error(err))
		initErr := servererr.ErrNewAbortingConnection.FastGenByArgs(cc.connectionID, "unconnected", cc.user, cc.peerHost, "init_connect command failed")
		if err1 := cc.writeError(ctx, initErr); err1 != nil {
			terror.Log(err1)
		}
		return initErr
	}

	data := cc.alloc.AllocWithLen(4, 32)
	data = append(data, mysql.OKHeader)
	data = append(data, 0, 0)
	if cc.capability&mysql.ClientProtocol41 > 0 {
		data = dump.Uint16(data, mysql.ServerStatusAutocommit)
		data = append(data, 0, 0)
	}

	err := cc.writePacket(data)
	cc.pkt.SetSequence(0)
	if err != nil {
		err = errors.SuspendStack(err)
		logutil.Logger(ctx).Debug("write response to client failed", zap.Error(err))
		return err
	}

	err = cc.flush(ctx)
	if err != nil {
		err = errors.SuspendStack(err)
		logutil.Logger(ctx).Debug("flush response to client failed", zap.Error(err))
		return err
	}

	// With mysql --compression-algorithms=zlib,zstd both flags are set, the result is Zlib
	if cc.capability&mysql.ClientCompress > 0 {
		cc.pkt.SetCompressionAlgorithm(mysql.CompressionZlib)
		cc.ctx.SetCompressionAlgorithm(mysql.CompressionZlib)
	} else if cc.capability&mysql.ClientZstdCompressionAlgorithm > 0 {
		cc.pkt.SetCompressionAlgorithm(mysql.CompressionZstd)
		cc.ctx.SetCompressionAlgorithm(mysql.CompressionZstd)
	}

	return err
}

func (cc *clientConn) Close() error {
	// Be careful, this function should be re-entrant. It might be called more than once for a single connection.
	// Any logic which is not idempotent should be in closeConn() and wrapped with `cc.closeOnce.Do`, like decresing
	// metrics, releasing resources, etc.
	//
	// TODO: avoid calling this function multiple times. It's not intuitive that a connection can be closed multiple
	// times.

	cc.server.rwlock.Lock()
	delete(cc.server.clients, cc.connectionID)
	cc.server.rwlock.Unlock()
	metrics.DDLClearTempIndexWrite(cc.connectionID)
	return closeConn(cc)
}

// closeConn is idempotent and thread-safe.
// It will be called on the same `clientConn` more than once to avoid connection leak.
func closeConn(cc *clientConn) error {
	var err error
	cc.closeOnce.Do(func() {
		if cc.connectionID > 0 {
			cc.server.dom.ReleaseConnID(cc.connectionID)
			cc.connectionID = 0
		}
		if cc.bufReadConn != nil {
			err := cc.bufReadConn.Close()
			if err != nil {
				// We need to expect connection might have already disconnected.
				// This is because closeConn() might be called after a connection read-timeout.
				logutil.Logger(context.Background()).Debug("could not close connection", zap.Error(err))
			}
		}

		// Close statements and session
		// At first, it'll decrese the count of connections in the resource group, update the corresponding gauge.
		// Then it'll close the statements and session, which release advisory locks, row locks, etc.
		if ctx := cc.getCtx(); ctx != nil {
			resourceGroupName := ctx.GetSessionVars().ResourceGroupName
			metrics.ConnGauge.WithLabelValues(resourceGroupName).Dec()

			err = ctx.Close()
		} else {
			metrics.ConnGauge.WithLabelValues(resourcegroup.DefaultResourceGroupName).Dec()
		}
	})
	return err
}

func (cc *clientConn) closeWithoutLock() error {
	delete(cc.server.clients, cc.connectionID)
	return closeConn(cc)
}

func (cc *clientConn) currentResourceGroupName() string {
	if ctx := cc.getCtx(); ctx != nil {
		if name := ctx.GetSessionVars().ResourceGroupName; name != "" {
			return name
		}
	}
	return resourcegroup.DefaultResourceGroupName
}

func (cc *clientConn) moveResourceGroupCounter(oldGroup string) {
	if oldGroup == "" {
		oldGroup = resourcegroup.DefaultResourceGroupName
	}
	newGroup := cc.currentResourceGroupName()
	if oldGroup != newGroup {
		metrics.ConnGauge.WithLabelValues(oldGroup).Dec()
		metrics.ConnGauge.WithLabelValues(newGroup).Inc()
	}
}

// writeInitialHandshake sends server version, connection ID, server capability, collation, server status
// and auth salt to the client.
func (cc *clientConn) writeInitialHandshake(ctx context.Context) error {
	data := make([]byte, 4, 128)

	// min version 10
	data = append(data, 10)
	// server version[00]
	data = append(data, mysql.ServerVersion...)
	data = append(data, 0)
	// connection id
	data = append(data, byte(cc.connectionID), byte(cc.connectionID>>8), byte(cc.connectionID>>16), byte(cc.connectionID>>24))
	// auth-plugin-data-part-1
	data = append(data, cc.salt[0:8]...)
	// filler [00]
	data = append(data, 0)
	// capability flag lower 2 bytes, using default capability here
	data = append(data, byte(cc.server.capability), byte(cc.server.capability>>8))
	// charset
	if cc.collation == 0 {
		cc.collation = uint8(mysql.DefaultCollationID)
	}
	data = append(data, cc.collation)
	// status
	data = dump.Uint16(data, mysql.ServerStatusAutocommit)
	// below 13 byte may not be used
	// capability flag upper 2 bytes, using default capability here
	data = append(data, byte(cc.server.capability>>16), byte(cc.server.capability>>24))
	// length of auth-plugin-data
	data = append(data, byte(len(cc.salt)+1))
	// reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	// auth-plugin-data-part-2
	data = append(data, cc.salt[8:]...)
	data = append(data, 0)
	// auth-plugin name
	if ctx := cc.getCtx(); ctx == nil {
		if err := cc.openSession(); err != nil {
			return err
		}
	}
	defAuthPlugin, err := cc.ctx.GetSessionVars().GetGlobalSystemVar(context.Background(), vardef.DefaultAuthPlugin)
	if err != nil {
		return err
	}
	cc.authPlugin = defAuthPlugin
	data = append(data, []byte(defAuthPlugin)...)

	// Close the session to force this to be re-opened after we parse the response. This is needed
	// to ensure we use the collation and client flags from the response for the session.
	if err = cc.ctx.Close(); err != nil {
		return err
	}
	cc.SetCtx(nil)

	data = append(data, 0)
	if err = cc.writePacket(data); err != nil {
		return err
	}
	return cc.flush(ctx)
}

func (cc *clientConn) readPacket() ([]byte, error) {
	if cc.getCtx() != nil {
		cc.pkt.SetMaxAllowedPacket(cc.ctx.GetSessionVars().MaxAllowedPacket)
	}
	data, err := cc.pkt.ReadPacket()
	if err == nil && cc.getCtx() != nil {
		cc.ctx.GetSessionVars().InPacketBytes.Add(uint64(len(data)))
	}
	return data, err
}

func (cc *clientConn) writePacket(data []byte) error {
	failpoint.Inject("FakeClientConn", func() {
		if cc.pkt == nil {
			failpoint.Return(nil)
		}
	})
	if cc.getCtx() != nil {
		cc.ctx.GetSessionVars().OutPacketBytes.Add(uint64(len(data)))
	}
	return cc.pkt.WritePacket(data)
}

func (cc *clientConn) getWaitTimeout(ctx context.Context) uint64 {
	sessVars := cc.ctx.GetSessionVars()
	if sessVars.InTxn() && sessVars.IdleTransactionTimeout > 0 {
		return uint64(sessVars.IdleTransactionTimeout)
	}
	return cc.getSessionVarsWaitTimeout(ctx)
}

// getSessionVarsWaitTimeout get session variable wait_timeout
func (cc *clientConn) getSessionVarsWaitTimeout(ctx context.Context) uint64 {
	valStr, exists := cc.ctx.GetSessionVars().GetSystemVar(vardef.WaitTimeout)
	if !exists {
		return vardef.DefWaitTimeout
	}
	waitTimeout, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		logutil.Logger(ctx).Warn("get sysval wait_timeout failed, use default value", zap.Error(err))
		// if get waitTimeout error, use default value
		return vardef.DefWaitTimeout
	}
	return waitTimeout
}

func (cc *clientConn) readOptionalSSLRequestAndHandshakeResponse(ctx context.Context) error {
	// Read a packet. It may be a SSLRequest or HandshakeResponse.
	data, err := cc.readPacket()
	if err != nil {
		err = errors.SuspendStack(err)
		if errors.Cause(err) == io.EOF {
			logutil.Logger(ctx).Debug("wait handshake response fail due to connection has be closed by client-side")
		} else {
			logutil.Logger(ctx).Debug("wait handshake response fail", zap.Error(err))
		}
		return err
	}

	var resp handshake.Response41
	var pos int

	if len(data) < 2 {
		logutil.Logger(ctx).Warn("got malformed handshake response", zap.ByteString("packetData", data))
		return mysql.ErrMalformPacket
	}

	capability := uint32(binary.LittleEndian.Uint16(data[:2]))
	if capability&mysql.ClientProtocol41 <= 0 {
		logutil.Logger(ctx).Warn("ClientProtocol41 flag is not set, please upgrade client")
		return servererr.ErrNotSupportedAuthMode
	}
	pos, err = parse.HandshakeResponseHeader(ctx, &resp, data)
	if err != nil {
		terror.Log(err)
		return err
	}

	// After read packets we should update the client's host and port to grab
	// real client's IP and port from PROXY Protocol header if PROXY Protocol is enabled.
	_, _, err = cc.PeerHost("", true)
	if err != nil {
		terror.Log(err)
		return err
	}
	// If enable proxy protocol check audit plugins after update real IP
	if cc.ppEnabled {
		err = cc.server.checkAuditPlugin(cc)
		if err != nil {
			return err
		}
	}

	if resp.Capability&mysql.ClientSSL > 0 {
		tlsConfig := (*tls.Config)(atomic.LoadPointer(&cc.server.tlsConfig))
		if tlsConfig != nil {
			// The packet is a SSLRequest, let's switch to TLS.
			if err = cc.upgradeToTLS(tlsConfig); err != nil {
				return err
			}
			// Read the following HandshakeResponse packet.
			data, err = cc.readPacket()
			if err != nil {
				logutil.Logger(ctx).Warn("read handshake response failure after upgrade to TLS", zap.Error(err))
				return err
			}
			pos, err = parse.HandshakeResponseHeader(ctx, &resp, data)
			if err != nil {
				terror.Log(err)
				return err
			}
		}
	} else if tlsutil.RequireSecureTransport.Load() && !cc.isUnixSocket {
		// If it's not a socket connection, we should reject the connection
		// because TLS is required.
		err := servererr.ErrSecureTransportRequired.FastGenByArgs()
		terror.Log(err)
		return err
	}

	// Read the remaining part of the packet.
	err = parse.HandshakeResponseBody(ctx, &resp, data, pos)
	if err != nil {
		terror.Log(err)
		return err
	}

	cc.capability = resp.Capability & cc.server.capability
	cc.user = resp.User
	cc.dbname = resp.DBName
	cc.collation = resp.Collation
	cc.attrs = resp.Attrs
	cc.pkt.SetZstdLevel(zstd.EncoderLevelFromZstd(resp.ZstdLevel))

	err = cc.handleAuthPlugin(ctx, &resp)
	if err != nil {
		return err
	}

	switch resp.AuthPlugin {
	case mysql.AuthCachingSha2Password:
		resp.Auth, err = cc.authSha(ctx, resp)
		if err != nil {
			return err
		}
	case mysql.AuthTiDBSM3Password:
		resp.Auth, err = cc.authSM3(ctx, resp)
		if err != nil {
			return err
		}
	case mysql.AuthNativePassword:
	case mysql.AuthSocket:
	case mysql.AuthTiDBSessionToken:
	case mysql.AuthTiDBAuthToken:
	case mysql.AuthMySQLClearPassword:
	case mysql.AuthLDAPSASL:
	case mysql.AuthLDAPSimple:
	default:
		if _, ok := cc.extensions.GetAuthPlugin(resp.AuthPlugin); !ok {
			return errors.New("Unknown auth plugin")
		}
	}

	err = cc.openSessionAndDoAuth(resp.Auth, resp.AuthPlugin, resp.ZstdLevel)
	if err != nil {
		logutil.Logger(ctx).Warn("open new session or authentication failure", zap.Error(err))
	}
	return err
}

func (cc *clientConn) handleAuthPlugin(ctx context.Context, resp *handshake.Response41) error {
	if resp.Capability&mysql.ClientPluginAuth > 0 {
		newAuth, err := cc.checkAuthPlugin(ctx, resp)
		if err != nil {
			logutil.Logger(ctx).Warn("failed to check the user authplugin", zap.Error(err))
			return err
		}
		if len(newAuth) > 0 {
			resp.Auth = newAuth
		}

		if _, ok := cc.extensions.GetAuthPlugin(resp.AuthPlugin); ok {
			// The auth plugin has been registered, skip other checks.
			return nil
		}
		switch resp.AuthPlugin {
		case mysql.AuthCachingSha2Password:
		case mysql.AuthTiDBSM3Password:
		case mysql.AuthNativePassword:
		case mysql.AuthSocket:
		case mysql.AuthTiDBSessionToken:
		case mysql.AuthMySQLClearPassword:
		case mysql.AuthLDAPSASL:
		case mysql.AuthLDAPSimple:
		default:
			logutil.Logger(ctx).Warn("Unknown Auth Plugin", zap.String("plugin", resp.AuthPlugin))
		}
	} else {
		// MySQL 5.1 and older clients don't support authentication plugins.
		logutil.Logger(ctx).Warn("Client without Auth Plugin support; Please upgrade client")
		_, err := cc.checkAuthPlugin(ctx, resp)
		if err != nil {
			return err
		}
		resp.AuthPlugin = mysql.AuthNativePassword
	}
	return nil
}

// authSha implements the caching_sha2_password specific part of the protocol.
func (cc *clientConn) authSha(ctx context.Context, resp handshake.Response41) ([]byte, error) {
	const (
		shaCommand       = 1
		requestRsaPubKey = 2 // Not supported yet, only TLS is supported as secure channel.
		fastAuthOk       = 3
		fastAuthFail     = 4
	)

	// If no password is specified, we don't send the FastAuthFail to do the full authentication
	// as that doesn't make sense without a password and confuses the client.
	// https://github.com/pingcap/tidb/issues/40831
	if len(resp.Auth) == 0 {
		return []byte{}, nil
	}

	// Currently we always send a "FastAuthFail" as the cached part of the protocol isn't implemented yet.
	// This triggers the client to send the full response.
	err := cc.writePacket([]byte{0, 0, 0, 0, shaCommand, fastAuthFail})
	if err != nil {
		logutil.Logger(ctx).Warn("authSha packet write failed", zap.Error(err))
		return nil, err
	}
	err = cc.flush(ctx)
	if err != nil {
		logutil.Logger(ctx).Warn("authSha packet flush failed", zap.Error(err))
		return nil, err
	}

	data, err := cc.readPacket()
	if err != nil {
		logutil.Logger(ctx).Warn("authSha packet read failed", zap.Error(err))
		return nil, err
	}
	return bytes.Trim(data, "\x00"), nil
}

// authSM3 implements the tidb_sm3_password specific part of the protocol.
// tidb_sm3_password is very similar to caching_sha2_password.
func (cc *clientConn) authSM3(ctx context.Context, resp handshake.Response41) ([]byte, error) {
	// If no password is specified, we don't send the FastAuthFail to do the full authentication
	// as that doesn't make sense without a password and confuses the client.
	// https://github.com/pingcap/tidb/issues/40831
	if len(resp.Auth) == 0 {
		return []byte{}, nil
	}

	err := cc.writePacket([]byte{0, 0, 0, 0, 1, 4}) // fastAuthFail
	if err != nil {
		logutil.Logger(ctx).Error("authSM3 packet write failed", zap.Error(err))
		return nil, err
	}
	err = cc.flush(ctx)
	if err != nil {
		logutil.Logger(ctx).Error("authSM3 packet flush failed", zap.Error(err))
		return nil, err
	}

	data, err := cc.readPacket()
	if err != nil {
		logutil.Logger(ctx).Error("authSM3 packet read failed", zap.Error(err))
		return nil, err
	}
	return bytes.Trim(data, "\x00"), nil
}

func (cc *clientConn) SessionStatusToString() string {
	status := cc.ctx.Status()
	inTxn, autoCommit := 0, 0
	if status&mysql.ServerStatusInTrans > 0 {
		inTxn = 1
	}
	if status&mysql.ServerStatusAutocommit > 0 {
		autoCommit = 1
	}
	return fmt.Sprintf("inTxn:%d, autocommit:%d",
		inTxn, autoCommit,
	)
}

func (cc *clientConn) openSession() error {
	var tlsStatePtr *tls.ConnectionState
	if cc.tlsConn != nil {
		tlsState := cc.tlsConn.ConnectionState()
		tlsStatePtr = &tlsState
	}
	ctx, err := cc.server.driver.OpenCtx(cc.connectionID, cc.capability, cc.collation, cc.dbname, tlsStatePtr, cc.extensions)
	if err != nil {
		return err
	}
	cc.SetCtx(ctx)

	err = cc.server.checkConnectionCount()
	if err != nil {
		return err
	}
	return nil
}

func (cc *clientConn) openSessionAndDoAuth(authData []byte, authPlugin string, zstdLevel int) error {
	// Open a context unless this was done before.
	if ctx := cc.getCtx(); ctx == nil {
		err := cc.openSession()
		if err != nil {
			return err
		}
	}

	hasPassword := "YES"
	if len(authData) == 0 {
		hasPassword = "NO"
	}

	host, port, err := cc.PeerHost(hasPassword, false)
	if err != nil {
		return err
	}

	err = cc.checkUserConnectionCount(host)
	if err != nil {
		return err
	}

	if !cc.isUnixSocket && authPlugin == mysql.AuthSocket {
		return servererr.ErrAccessDeniedNoPassword.FastGenByArgs(cc.user, host)
	}

	userIdentity := &auth.UserIdentity{Username: cc.user, Hostname: host, AuthPlugin: authPlugin}
	if err = cc.ctx.Auth(userIdentity, authData, cc.salt, cc); err != nil {
		return err
	}
	cc.ctx.SetPort(port)
	cc.ctx.SetCompressionLevel(zstdLevel)
	if cc.dbname != "" {
		_, err = cc.useDB(context.Background(), cc.dbname)
		if err != nil {
			return err
		}
	}
	cc.ctx.SetSessionManager(cc.server)
	return nil
}

// mockOSUserForAuthSocketTest should only be used in test
var mockOSUserForAuthSocketTest atomic.Pointer[string]

// Check if the Authentication Plugin of the server, client and user configuration matches
func (cc *clientConn) checkAuthPlugin(ctx context.Context, resp *handshake.Response41) ([]byte, error) {
	// Open a context unless this was done before.
	if ctx := cc.getCtx(); ctx == nil {
		err := cc.openSession()
		if err != nil {
			return nil, err
		}
	}

	authData := resp.Auth
	// tidb_session_token is always permitted and skips stored user plugin.
	if resp.AuthPlugin == mysql.AuthTiDBSessionToken {
		return authData, nil
	}
	hasPassword := "YES"
	if len(authData) == 0 {
		hasPassword = "NO"
	}

	host, _, err := cc.PeerHost(hasPassword, false)
	if err != nil {
		return nil, err
	}
	// Find the identity of the user based on username and peer host.
	identity, err := cc.ctx.MatchIdentity(ctx, cc.user, host)
	if err != nil {
		return nil, servererr.ErrAccessDenied.FastGenByArgs(cc.user, host, hasPassword)
	}
	// Get the plugin for the identity.
	userplugin, err := cc.ctx.AuthPluginForUser(ctx, identity)
	if err != nil {
		logutil.Logger(ctx).Warn("Failed to get authentication method for user",
			zap.String("user", cc.user), zap.String("host", host))
	}
	failpoint.Inject("FakeUser", func(val failpoint.Value) {
		//nolint:forcetypeassert
		userplugin = val.(string)
	})
	if userplugin == mysql.AuthSocket {
		if !cc.isUnixSocket {
			return nil, servererr.ErrAccessDenied.FastGenByArgs(cc.user, host, hasPassword)
		}
		resp.AuthPlugin = mysql.AuthSocket
		user, err := user.LookupId(fmt.Sprint(cc.socketCredUID))
		if err != nil {
			return nil, err
		}
		uname := user.Username

		if intest.InTest {
			if p := mockOSUserForAuthSocketTest.Load(); p != nil {
				uname = *p
			}
		}

		return []byte(uname), nil
	}
	if len(userplugin) == 0 {
		// No user plugin set, assuming MySQL Native Password
		// This happens if the account doesn't exist or if the account doesn't have
		// a password set.
		if resp.AuthPlugin != mysql.AuthNativePassword && resp.Capability&mysql.ClientPluginAuth > 0 {
			resp.AuthPlugin = mysql.AuthNativePassword
			authData, err := cc.authSwitchRequest(ctx, mysql.AuthNativePassword)
			if err != nil {
				return nil, err
			}
			return authData, nil
		}
		return nil, nil
	}

	// If the authentication method send by the server (cc.authPlugin) doesn't match
	// the plugin configured for the user account in the mysql.user.plugin column
	// or if the authentication method send by the server doesn't match the authentication
	// method send by the client (*authPlugin) then we need to switch the authentication
	// method to match the one configured for that specific user.
	if (cc.authPlugin != userplugin) || (cc.authPlugin != resp.AuthPlugin) {
		if userplugin == mysql.AuthTiDBAuthToken {
			userplugin = mysql.AuthMySQLClearPassword
		}
		if resp.Capability&mysql.ClientPluginAuth > 0 {
			authData, err := cc.authSwitchRequest(ctx, userplugin)
			if err != nil {
				return nil, err
			}
			resp.AuthPlugin = userplugin
			return authData, nil
		} else if userplugin != mysql.AuthNativePassword {
			// MySQL 5.1 and older don't support authentication plugins yet
			return nil, servererr.ErrNotSupportedAuthMode
		}
	}

	return nil, nil
}

func (cc *clientConn) PeerHost(hasPassword string, update bool) (host, port string, err error) {
	// already get peer host
	if len(cc.peerHost) > 0 {
		// Proxy protocol enabled and not update
		if cc.ppEnabled && !update {
			return cc.peerHost, cc.peerPort, nil
		}
		// Proxy protocol not enabled
		if !cc.ppEnabled {
			return cc.peerHost, cc.peerPort, nil
		}
	}
	host = vardef.DefHostname
	if cc.isUnixSocket {
		cc.peerHost = host
		cc.serverHost = host
		return
	}
	addr := cc.bufReadConn.RemoteAddr().String()
	host, port, err = net.SplitHostPort(addr)
	if err != nil {
		err = servererr.ErrAccessDenied.GenWithStackByArgs(cc.user, addr, hasPassword)
		return
	}
	cc.peerHost = host
	cc.peerPort = port

	serverAddr := cc.bufReadConn.LocalAddr().String()
	serverHost, _, err := net.SplitHostPort(serverAddr)
	if err != nil {
		err = servererr.ErrAccessDenied.GenWithStackByArgs(cc.user, addr, hasPassword)
		return
	}
	cc.serverHost = serverHost

	return
}

// skipInitConnect follows MySQL's rules of when init-connect should be skipped.
// In 5.7 it is any user with SUPER privilege, but in 8.0 it is:
// - SUPER or the CONNECTION_ADMIN dynamic privilege.
// - (additional exception) users with expired passwords (not yet supported)
// In TiDB CONNECTION_ADMIN is satisfied by SUPER, so we only need to check once.
func (cc *clientConn) skipInitConnect() bool {
	checker := privilege.GetPrivilegeManager(cc.ctx.Session)
	activeRoles := cc.ctx.GetSessionVars().ActiveRoles
	return checker != nil && checker.RequestDynamicVerification(activeRoles, "CONNECTION_ADMIN", false)
}

// initResultEncoder initialize the result encoder for current connection.
func (cc *clientConn) initResultEncoder(ctx context.Context) {
	chs, err := cc.ctx.GetSessionVars().GetSessionOrGlobalSystemVar(context.Background(), vardef.CharacterSetResults)
	if err != nil {
		chs = ""
		logutil.Logger(ctx).Warn("get character_set_results system variable failed", zap.Error(err))
	}
	cc.rsEncoder = column.NewResultEncoder(chs)
}

func (cc *clientConn) initInputEncoder(ctx context.Context) {
	chs, err := cc.ctx.GetSessionVars().GetSessionOrGlobalSystemVar(context.Background(), vardef.CharacterSetClient)
	if err != nil {
		chs = ""
		logutil.Logger(ctx).Warn("get character_set_client system variable failed", zap.Error(err))
	}
	cc.inputDecoder = util2.NewInputDecoder(chs)
}

// initConnect runs the initConnect SQL statement if it has been specified.
// The semantics are MySQL compatible.
func (cc *clientConn) initConnect(ctx context.Context) error {
	val, err := cc.ctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(vardef.InitConnect)
	if err != nil {
		return err
	}
	if val == "" || cc.skipInitConnect() {
		return nil
	}
	logutil.Logger(ctx).Debug("init_connect starting")
	stmts, err := cc.ctx.Parse(ctx, val)
	if err != nil {
		return err
	}
	for _, stmt := range stmts {
		rs, err := cc.ctx.ExecuteStmt(ctx, stmt)
		if err != nil {
			return err
		}
		// init_connect does not care about the results,
		// but they need to be drained because of lazy loading.
		if rs != nil {
			req := rs.NewChunk(nil)
			for {
				if err = rs.Next(ctx, req); err != nil {
					return err
				}
				if req.NumRows() == 0 {
					break
				}
			}
			rs.Close()
		}
	}
	logutil.Logger(ctx).Debug("init_connect complete")
	return nil
}

// Run reads client query and writes query result to client in for loop, if there is a panic during query handling,
// it will be recovered and log the panic error.
// This function returns and the connection is closed if there is an IO error or there is a panic.
func (cc *clientConn) Run(ctx context.Context) {
	defer func() {
		r := recover()
		if r != nil {
			logutil.Logger(ctx).Error("connection running loop panic",
				zap.Stringer("lastSQL", getLastStmtInConn{cc}),
				zap.String("err", fmt.Sprintf("%v", r)),
				zap.Stack("stack"),
			)
			err := cc.writeError(ctx, fmt.Errorf("%v", r))
			terror.Log(err)
			metrics.PanicCounter.WithLabelValues(metrics.LabelSession).Inc()
		}
		util.WithRecovery(
			func() {
				if cc.getStatus() != connStatusShutdown {
					err := cc.Close()
					terror.Log(err)
				}
			}, nil)

		close(cc.quit)
	}()

	cc.addConnMetrics()

	var traceInfo *tracing.TraceInfo
	trace := traceevent.NewTrace()
	ctx = tracing.WithFlightRecorder(ctx, trace)

	// Usually, client connection status changes between [dispatching] <=> [reading].
	// When some event happens, server may notify this client connection by setting
	// the status to special values, for example: kill or graceful shutdown.
	// The client connection would detect the events when it fails to change status
	// by CAS operation, it would then take some actions accordingly.
	parentCtx := ctx
	for {
		sessVars := cc.ctx.GetSessionVars()
		if alias := sessVars.SessionAlias; traceInfo == nil || traceInfo.SessionAlias != alias {
			// We should reset the context trace info when traceInfo not inited or session alias changed.
			traceInfo = &tracing.TraceInfo{
				ConnectionID: cc.connectionID,
				SessionAlias: alias,
			}
			ctx = logutil.WithSessionAlias(parentCtx, sessVars.SessionAlias)
			ctx = tracing.ContextWithTraceInfo(ctx, traceInfo)
		}

		// Close connection between txn when we are going to shutdown server.
		// Note the current implementation when shutting down, for an idle connection, the connection may block at readPacket()
		// consider provider a way to close the connection directly after sometime if we can not read any data.
		if cc.server.inShutdownMode.Load() {
			if !sessVars.InTxn() {
				return
			}
		}

		if !cc.CompareAndSwapStatus(connStatusDispatching, connStatusReading) ||
			// The judge below will not be hit by all means,
			// But keep it stayed as a reminder and for the code reference for connStatusWaitShutdown.
			cc.getStatus() == connStatusWaitShutdown {
			return
		}

		cc.alloc.Reset()
		// close connection when idle time is more than wait_timeout
		// default 28800(8h), FIXME: should not block at here when we kill the connection.
		waitTimeout := cc.getWaitTimeout(ctx)
		cc.pkt.SetReadTimeout(time.Duration(waitTimeout) * time.Second)
		start := time.Now()
		data, err := cc.readPacket()
		if err != nil {
			if terror.ErrorNotEqual(err, io.EOF) {
				if netErr, isNetErr := errors.Cause(err).(net.Error); isNetErr && netErr.Timeout() {
					if cc.getStatus() == connStatusWaitShutdown {
						logutil.Logger(ctx).Info("read packet timeout because of killed connection")
					} else {
						idleTime := time.Since(start)
						tidbGatewayConnID := cc.attrs[tidbGatewayAttrsConnKey]
						cc.server.SetNormalClosedConn(keyspace.GetKeyspaceNameBySettings(), tidbGatewayConnID, "read packet timeout")
						logutil.Logger(ctx).Info("read packet timeout, close this connection",
							zap.Duration("idle", idleTime),
							zap.Uint64("waitTimeout", waitTimeout),
							zap.Error(err),
						)
					}
				} else if errors.ErrorEqual(err, servererr.ErrNetPacketTooLarge) {
					err := cc.writeError(ctx, err)
					if err != nil {
						terror.Log(err)
					}
				} else {
					errStack := errors.ErrorStack(err)
					if !strings.Contains(errStack, "use of closed network connection") {
						logutil.Logger(ctx).Warn("read packet failed, close this connection",
							zap.Error(errors.SuspendStack(err)))
					}
				}
			}
			server_metrics.DisconnectByClientWithError.Inc()
			return
		}

		// It should be CAS before checking the `inShutdownMode` to avoid the following scenario:
		// 1. The connection checks the `inShutdownMode` and it's false.
		// 2. The server sets the `inShutdownMode` to true. The `DrainClients` process ignores this connection
		//   because the connection is in the `connStatusReading` status.
		// 3. The connection changes its status to `connStatusDispatching` and starts to execute the command.
		if !cc.CompareAndSwapStatus(connStatusReading, connStatusDispatching) {
			return
		}

		// Should check InTxn() to avoid execute `begin` stmt and allow executing statements in the not committed txn.
		if cc.server.inShutdownMode.Load() {
			if !cc.ctx.GetSessionVars().InTxn() {
				return
			}
		}

		startTime := time.Now()
		err = cc.dispatch(ctx, data)
		cc.ctx.GetSessionVars().ClearAlloc(&cc.chunkAlloc, err != nil)
		cc.chunkAlloc.Reset()
		trace.DiscardOrFlush(ctx)

		if err != nil {
			cc.audit(context.Background(), plugin.Error) // tell the plugin API there was a dispatch error
			if terror.ErrorEqual(err, io.EOF) {
				cc.addQueryMetrics(data[0], startTime, nil)
				server_metrics.DisconnectNormal.Inc()
				return
			} else if terror.ErrResultUndetermined.Equal(err) {
				logutil.Logger(ctx).Warn("result undetermined, close this connection", zap.Error(err))
				server_metrics.DisconnectErrorUndetermined.Inc()
				return
			} else if terror.ErrCritical.Equal(err) {
				metrics.CriticalErrorCounter.Add(1)
				logutil.Logger(ctx).Fatal("critical error, stop the server", zap.Error(err))
			}
			var txnMode string
			if ctx := cc.getCtx(); ctx != nil {
				txnMode = ctx.GetSessionVars().GetReadableTxnMode()
			}
			vars := cc.getCtx().GetSessionVars()
			for _, dbName := range session.GetDBNames(vars) {
				metrics.ExecuteErrorCounter.WithLabelValues(metrics.ExecuteErrorToLabel(err), dbName, vars.ResourceGroupName).Inc()
			}

			if storeerr.ErrLockAcquireFailAndNoWaitSet.Equal(err) {
				logutil.Logger(ctx).Debug("Expected error for FOR UPDATE NOWAIT", zap.Error(err))
			} else {
				var timestamp uint64
				if ctx := cc.getCtx(); ctx != nil && ctx.GetSessionVars() != nil && ctx.GetSessionVars().TxnCtx != nil {
					timestamp = ctx.GetSessionVars().TxnCtx.StartTS
					if timestamp == 0 && ctx.GetSessionVars().TxnCtx.StaleReadTs > 0 {
						// for state-read query.
						timestamp = ctx.GetSessionVars().TxnCtx.StaleReadTs
					}
				}
				logutil.Logger(ctx).Warn("command dispatched failed",
					zap.String("connInfo", cc.String()),
					zap.String("command", mysql.Command2Str[data[0]]),
					zap.String("status", cc.SessionStatusToString()),
					zap.Stringer("sql", getLastStmtInConn{cc}),
					zap.String("txn_mode", txnMode),
					zap.Uint64("timestamp", timestamp),
					zap.String("err", errStrForLog(err, cc.ctx.GetSessionVars().EnableRedactLog)),
				)
			}
			err1 := cc.writeError(ctx, err)
			terror.Log(err1)
		}
		cc.addQueryMetrics(data[0], startTime, err)
		cc.pkt.SetSequence(0)
		cc.pkt.SetCompressedSequence(0)
	}
}

func errStrForLog(err error, redactMode string) string {
	if redactMode != errors.RedactLogDisable {
		// currently, only ErrParse is considered when enableRedactLog because it may contain sensitive information like
		// password or accesskey
		if parser.ErrParse.Equal(err) {
			return "fail to parse SQL, and must redact the whole error when enable log redaction"
		}
	}
	var ret string
	if kv.ErrKeyExists.Equal(err) || parser.ErrParse.Equal(err) || infoschema.ErrTableNotExists.Equal(err) {
		// Do not log stack for duplicated entry error.
		ret = err.Error()
	} else {
		ret = errors.ErrorStack(err)
	}
	return ret
}

// Per connection metrics
func (cc *clientConn) addConnMetrics() {
	if cc.tlsConn != nil {
		connState := cc.tlsConn.ConnectionState()
		metrics.TLSVersion.WithLabelValues(
			tlsutil.VersionName(connState.Version),
		).Inc()
		metrics.TLSCipher.WithLabelValues(
			tlsutil.CipherSuiteName(connState.CipherSuite),
		).Inc()
	}
}

// Per query metrics
func (cc *clientConn) addQueryMetrics(cmd byte, startTime time.Time, err error) {
	if cmd == mysql.ComQuery && cc.ctx.Value(sessionctx.LastExecuteDDL) != nil {
		// Don't take DDL execute time into account.
		// It's already recorded by other metrics in ddl package.
		return
	}

	vars := cc.getCtx().GetSessionVars()
	resourceGroupName := vars.ResourceGroupName
	var counter prometheus.Counter
	if len(resourceGroupName) == 0 || resourceGroupName == resourcegroup.DefaultResourceGroupName {
		if err != nil && int(cmd) < len(server_metrics.QueryTotalCountErr) {
			counter = server_metrics.QueryTotalCountErr[cmd]
		} else if err == nil && int(cmd) < len(server_metrics.QueryTotalCountOk) {
			counter = server_metrics.QueryTotalCountOk[cmd]
		}
	}

	if counter != nil {
		counter.Inc()
	} else {
		label := server_metrics.CmdToString(cmd)
		if err != nil {
			metrics.QueryTotalCounter.WithLabelValues(label, "Error", resourceGroupName).Inc()
		} else {
			metrics.QueryTotalCounter.WithLabelValues(label, "OK", resourceGroupName).Inc()
		}
	}

	cost := time.Since(startTime)
	sessionVar := cc.ctx.GetSessionVars()
	affectedRows := cc.ctx.AffectedRows()
	cc.ctx.GetTxnWriteThroughputSLI().FinishExecuteStmt(cost, affectedRows, sessionVar.InTxn())

	stmtType := sessionVar.StmtCtx.StmtType
	sqlType := metrics.LblGeneral
	if stmtType != "" {
		sqlType = stmtType
	}

	for _, dbName := range session.GetDBNames(vars) {
		metrics.QueryDurationHistogram.WithLabelValues(sqlType, dbName, vars.StmtCtx.ResourceGroupName).Observe(cost.Seconds())
		metrics.QueryRPCHistogram.WithLabelValues(sqlType, dbName).Observe(float64(vars.StmtCtx.GetExecDetails().RequestCount))
		if vars.StmtCtx.GetExecDetails().ScanDetail != nil {
			metrics.QueryProcessedKeyHistogram.WithLabelValues(sqlType, dbName).Observe(float64(vars.StmtCtx.GetExecDetails().ScanDetail.ProcessedKeys))
		}
	}
}

// dispatch handles client request based on command which is the first byte of the data.
// It also gets a token from server which is used to limit the concurrently handling clients.
// The most frequently used command is ComQuery.
