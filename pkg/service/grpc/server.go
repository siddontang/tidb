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

package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ServiceHandler handles requests for a service.
type ServiceHandler interface {
	// HandleRequest processes a request and returns a response.
	HandleRequest(ctx context.Context, method string, request []byte) ([]byte, error)
}

// Server is a gRPC server that hosts service handlers.
type Server struct {
	addr       string
	grpcServer *grpc.Server
	listener   net.Listener
	handlers   map[string]ServiceHandler
	mu         sync.RWMutex
	started    bool
}

// NewServer creates a new gRPC server.
func NewServer(addr string) *Server {
	return &Server{
		addr:     addr,
		handlers: make(map[string]ServiceHandler),
	}
}

// RegisterHandler registers a service handler.
func (s *Server) RegisterHandler(serviceName string, handler ServiceHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[serviceName] = handler
}

// Start starts the gRPC server.
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return nil
	}

	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return errors.Wrapf(err, "failed to listen on %s", s.addr)
	}
	s.listener = listener

	s.grpcServer = grpc.NewServer(
		grpc.UnknownServiceHandler(s.handleUnknown),
	)

	s.started = true

	go func() {
		logutil.BgLogger().Info("gRPC server started", zap.String("addr", s.addr))
		if err := s.grpcServer.Serve(listener); err != nil {
			logutil.BgLogger().Error("gRPC server error", zap.Error(err))
		}
	}()

	return nil
}

// Stop stops the gRPC server.
func (s *Server) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return
	}

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	s.started = false
}

// Addr returns the server address.
func (s *Server) Addr() string {
	return s.addr
}

// handleUnknown handles unknown service calls using our generic protocol.
func (s *Server) handleUnknown(srv any, stream grpc.ServerStream) error {
	// Get the full method name from the stream context
	fullMethod, ok := grpc.MethodFromOutgoingContext(stream.Context())
	if !ok {
		// Try to get it from the stream info
		info := grpc.ServerTransportStreamFromContext(stream.Context())
		if info != nil {
			fullMethod = info.Method()
		}
	}

	// Parse service and method from full method name
	// Format: /tidb.service.{Service}/{Method}
	serviceName, methodName := parseMethod(fullMethod)

	s.mu.RLock()
	handler, ok := s.handlers[serviceName]
	s.mu.RUnlock()

	if !ok {
		return status.Errorf(codes.Unimplemented, "service %s not found", serviceName)
	}

	// Receive request
	var reqData []byte
	if err := stream.RecvMsg(&reqData); err != nil {
		return err
	}

	// Handle request
	respData, err := handler.HandleRequest(stream.Context(), methodName, reqData)
	if err != nil {
		return status.Errorf(codes.Internal, "handler error: %v", err)
	}

	// Send response
	return stream.SendMsg(respData)
}

// parseMethod parses a full method name into service and method.
func parseMethod(fullMethod string) (service, method string) {
	// Format: /tidb.service.{Service}/{Method}
	if len(fullMethod) < 2 {
		return "", ""
	}

	// Remove leading /
	fullMethod = fullMethod[1:]

	// Find the last /
	for i := len(fullMethod) - 1; i >= 0; i-- {
		if fullMethod[i] == '/' {
			return fullMethod[:i], fullMethod[i+1:]
		}
	}

	return fullMethod, ""
}

// JSONHandler is a helper that wraps a typed handler function.
type JSONHandler[Req, Resp any] struct {
	handler func(ctx context.Context, req *Req) (*Resp, error)
}

// NewJSONHandler creates a new JSON handler.
func NewJSONHandler[Req, Resp any](handler func(ctx context.Context, req *Req) (*Resp, error)) *JSONHandler[Req, Resp] {
	return &JSONHandler[Req, Resp]{handler: handler}
}

// Handle processes a JSON request.
func (h *JSONHandler[Req, Resp]) Handle(ctx context.Context, data []byte) ([]byte, error) {
	var req Req
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("failed to unmarshal request: %w", err)
	}

	resp, err := h.handler(ctx, &req)
	if err != nil {
		return nil, err
	}

	return json.Marshal(resp)
}

// MultiMethodHandler handles multiple methods for a service.
type MultiMethodHandler struct {
	methods map[string]func(ctx context.Context, data []byte) ([]byte, error)
}

// NewMultiMethodHandler creates a new multi-method handler.
func NewMultiMethodHandler() *MultiMethodHandler {
	return &MultiMethodHandler{
		methods: make(map[string]func(ctx context.Context, data []byte) ([]byte, error)),
	}
}

// Register registers a method handler.
func (h *MultiMethodHandler) Register(method string, handler func(ctx context.Context, data []byte) ([]byte, error)) {
	h.methods[method] = handler
}

// HandleRequest implements ServiceHandler.
func (h *MultiMethodHandler) HandleRequest(ctx context.Context, method string, request []byte) ([]byte, error) {
	handler, ok := h.methods[method]
	if !ok {
		return nil, fmt.Errorf("method %s not found", method)
	}
	return handler(ctx, request)
}
