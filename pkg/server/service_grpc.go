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

package server

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/service"
	"github.com/pingcap/tidb/pkg/service/copr"
	"github.com/pingcap/tidb/pkg/service/ddl"
	"github.com/pingcap/tidb/pkg/service/schema"
	"github.com/pingcap/tidb/pkg/service/stats"
	"github.com/pingcap/tidb/pkg/service/txn"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// svcManager holds a reference to the service manager for HTTP health endpoints.
var svcManager atomic.Pointer[service.Manager]

// serviceGRPCRegistrar holds gRPC servers for service framework services.
type serviceGRPCRegistrar struct {
	ddlServer    *ddl.GRPCServer
	schemaServer *schema.GRPCServer
	statsServer  *stats.GRPCServer
	txnServer    *txn.GRPCServer
	coprServer   *copr.GRPCServer
}

// SetupServiceGRPC registers gRPC services from the service framework.
// This method is called from main.go after the service manager is initialized.
// If the service manager is nil or services are not available, this is a no-op.
func (s *Server) SetupServiceGRPC(svcMgr *service.Manager, grpcServer *grpc.Server) {
	if svcMgr == nil || grpcServer == nil {
		return
	}

	selfAddr := net.JoinHostPort(s.cfg.AdvertiseAddress, strconv.Itoa(int(s.cfg.Status.StatusPort)))
	registrar := &serviceGRPCRegistrar{}

	// Register DDL service
	if svc, err := svcMgr.Get(service.ServiceDDL); err == nil {
		if ddlSvc, ok := svc.(*ddl.Service); ok {
			registrar.ddlServer = ddl.NewGRPCServer(ddlSvc, selfAddr)
			registrar.ddlServer.RegisterService(grpcServer)
			logutil.BgLogger().Info("DDL gRPC service registered",
				zap.String("addr", selfAddr))
		}
	}

	// Register Schema service
	if svc, err := svcMgr.Get(service.ServiceSchema); err == nil {
		if schemaSvc, ok := svc.(*schema.Service); ok {
			registrar.schemaServer = schema.NewGRPCServer(schemaSvc, selfAddr)
			registrar.schemaServer.RegisterService(grpcServer)
			logutil.BgLogger().Info("Schema gRPC service registered",
				zap.String("addr", selfAddr))
		}
	}

	// Register Stats service
	if svc, err := svcMgr.Get(service.ServiceStatistics); err == nil {
		if statsSvc, ok := svc.(*stats.Service); ok {
			registrar.statsServer = stats.NewGRPCServer(statsSvc, selfAddr)
			registrar.statsServer.RegisterService(grpcServer)
			logutil.BgLogger().Info("Stats gRPC service registered",
				zap.String("addr", selfAddr))
		}
	}

	// Register Transaction service
	if svc, err := svcMgr.Get(service.ServiceTransaction); err == nil {
		if txnSvc, ok := svc.(*txn.Service); ok {
			registrar.txnServer = txn.NewGRPCServer(txnSvc, selfAddr)
			registrar.txnServer.RegisterService(grpcServer)
			logutil.BgLogger().Info("Transaction gRPC service registered",
				zap.String("addr", selfAddr))
		}
	}

	// Register Coprocessor service
	if svc, err := svcMgr.Get(service.ServiceCoprocessor); err == nil {
		if coprSvc, ok := svc.(*copr.Service); ok {
			registrar.coprServer = copr.NewGRPCServer(coprSvc, selfAddr)
			registrar.coprServer.RegisterService(grpcServer)
			logutil.BgLogger().Info("Coprocessor gRPC service registered",
				zap.String("addr", selfAddr))
		}
	}
}

// GetGRPCServer returns the gRPC server if available.
func (s *Server) GetGRPCServer() *grpc.Server {
	return s.grpcServer
}

// SetServiceManager sets the service manager for HTTP health endpoints.
func SetServiceManager(mgr *service.Manager) {
	svcManager.Store(mgr)
}

// ServiceHealthStatus represents the health status of all services.
type ServiceHealthStatus struct {
	// Overall indicates if the overall system is healthy.
	Overall bool `json:"overall"`

	// OverallState is the aggregated state string.
	OverallState string `json:"overall_state"`

	// Services contains health status for each service.
	Services map[string]ServiceHealthInfo `json:"services"`
}

// ServiceHealthInfo represents health info for a single service.
type ServiceHealthInfo struct {
	// State is the service state (healthy, degraded, unhealthy, etc.).
	State string `json:"state"`

	// Message contains additional information about the health status.
	Message string `json:"message,omitempty"`
}

// handleServiceHealth handles the /services/health HTTP endpoint.
func handleServiceHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	mgr := svcManager.Load()
	if mgr == nil {
		// Service framework not initialized, return basic healthy status
		status := ServiceHealthStatus{
			Overall:      true,
			OverallState: "healthy",
			Services:     make(map[string]ServiceHealthInfo),
		}
		writeJSON(w, status)
		return
	}

	// Get overall health
	overallHealth := mgr.Health()
	status := ServiceHealthStatus{
		Overall:      overallHealth.State == service.StateHealthy,
		OverallState: stateToString(overallHealth.State),
		Services:     make(map[string]ServiceHealthInfo),
	}

	// Get health for each known service
	serviceNames := []string{
		service.ServiceStorage,
		service.ServiceMetadata,
		service.ServiceSchema,
		service.ServiceDDL,
		service.ServiceStatistics,
		service.ServiceAutoID,
		service.ServiceTransaction,
		service.ServiceCoprocessor,
		service.ServiceSession,
		service.ServiceQuery,
		service.ServiceGateway,
		service.ServiceAdmin,
	}

	for _, name := range serviceNames {
		if health, ok := mgr.ServiceHealth(name); ok {
			status.Services[name] = ServiceHealthInfo{
				State:   stateToString(health.State),
				Message: health.Message,
			}
		}
	}

	// Set HTTP status code based on health
	if !status.Overall {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	writeJSON(w, status)
}

// handleServiceList handles the /services HTTP endpoint.
func handleServiceList(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	mgr := svcManager.Load()
	if mgr == nil {
		writeJSON(w, map[string]any{
			"mode":     "traditional",
			"services": []string{},
		})
		return
	}

	// Get service list from registry
	registry := mgr.Registry()
	if registry == nil {
		writeJSON(w, map[string]any{
			"mode":     "unknown",
			"services": []string{},
		})
		return
	}

	ctx := context.Background()
	services, err := registry.List(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]any{
		"mode":     "service-framework",
		"services": services,
	})
}

// stateToString converts a service state to string.
func stateToString(state service.HealthState) string {
	switch state {
	case service.StateHealthy:
		return "healthy"
	case service.StateDegraded:
		return "degraded"
	case service.StateUnhealthy:
		return "unhealthy"
	case service.StateStopping:
		return "stopping"
	case service.StateStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

// writeJSON writes a JSON response.
func writeJSON(w http.ResponseWriter, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(data)
}

// RegisterServiceHTTPHandlers registers HTTP handlers for service endpoints.
// This should be called when setting up the HTTP router.
func RegisterServiceHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/services", handleServiceList)
	mux.HandleFunc("/services/health", handleServiceHealth)
}
