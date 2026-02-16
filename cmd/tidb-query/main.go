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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/service"
	"github.com/pingcap/tidb/pkg/service/admin"
	"github.com/pingcap/tidb/pkg/service/metadata"
	"github.com/pingcap/tidb/pkg/service/query"
	"github.com/pingcap/tidb/pkg/service/storage"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/printer"
	"go.uber.org/zap"
)

var (
	version     = flag.Bool("V", false, "print version information and exit")
	configPath  = flag.String("config", "", "config file path")
	storagePath = flag.String("path", "tikv://127.0.0.1:2379", "storage path")
	grpcAddr    = flag.String("grpc-addr", "0.0.0.0:4001", "gRPC server address")
	statusPort  = flag.Int("status", 10084, "tidb status port")
)

func main() {
	flag.Parse()

	if *version {
		fmt.Println(printer.GetTiDBInfo())
		os.Exit(0)
	}

	// Initialize logging
	cfg := logutil.NewLogConfig("info", "text", "", "", logutil.EmptyFileLogConfig, false)
	if err := logutil.InitLogger(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	logger := logutil.BgLogger()
	logger.Info("Starting TiDB Query Service (Distributed Mode)",
		zap.String("version", mysql.TiDBReleaseVersion))

	// Create service configuration for distributed mode
	svcCfg := &service.Config{
		Mode: service.ModeDistributed,
		EnabledServices: []string{
			service.ServiceStorage,
			service.ServiceMetadata,
			service.ServiceQuery,
			service.ServiceAdmin,
		},
		Registry: service.RegistryConfig{
			Type:      "etcd",
			Endpoints: []string{"127.0.0.1:2379"},
		},
		ServiceConfigs: map[string]any{
			service.ServiceStorage: storage.Config{
				Path: *storagePath,
			},
			service.ServiceQuery: query.Config{
				GRPCAddr: *grpcAddr,
			},
		},
	}

	// Create service manager
	manager, err := service.NewManager(svcCfg)
	if err != nil {
		logger.Fatal("Failed to create service manager", zap.Error(err))
	}

	// Register services in dependency order
	storageSvc := storage.New()
	metadataSvc := metadata.New()
	querySvc := query.New()
	adminSvc := admin.New()

	if err := manager.Register(storageSvc); err != nil {
		logger.Fatal("Failed to register storage service", zap.Error(err))
	}
	if err := manager.Register(metadataSvc); err != nil {
		logger.Fatal("Failed to register metadata service", zap.Error(err))
	}
	if err := manager.Register(querySvc); err != nil {
		logger.Fatal("Failed to register query service", zap.Error(err))
	}
	if err := manager.Register(adminSvc); err != nil {
		logger.Fatal("Failed to register admin service", zap.Error(err))
	}

	// Start services
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := manager.Start(ctx); err != nil {
		logger.Fatal("Failed to start services", zap.Error(err))
	}

	logger.Info("TiDB Query Service started (Distributed Mode)",
		zap.String("storage", *storagePath),
		zap.String("grpc_addr", *grpcAddr),
		zap.Int("status_port", *statusPort))

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("Shutting down TiDB Query Service")

	// Stop services
	if err := manager.Stop(ctx); err != nil {
		logger.Error("Error during shutdown", zap.Error(err))
	}

	logger.Info("TiDB Query Service stopped")
}
