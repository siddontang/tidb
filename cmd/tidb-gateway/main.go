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
	"strings"
	"syscall"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session"
	kvstore "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/store/driver"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/printer"
	"go.uber.org/zap"

	"github.com/pingcap/tidb/pkg/service"
	"github.com/pingcap/tidb/pkg/service/admin"
	"github.com/pingcap/tidb/pkg/service/gateway"
)

var (
	version     = flag.Bool("V", false, "print version information and exit")
	configPath  = flag.String("config", "", "config file path")
	host        = flag.String("host", "0.0.0.0", "tidb gateway host")
	port        = flag.Int("P", 4000, "tidb gateway port")
	statusPort  = flag.Int("status", 10080, "tidb status port")
	storagePath = flag.String("path", "tikv://127.0.0.1:2379", "TiKV storage path")
)

func main() {
	flag.Parse()

	if *version {
		fmt.Println(printer.GetTiDBInfo())
		os.Exit(0)
	}

	// Initialize logging
	logCfg := logutil.NewLogConfig("info", "text", "", "", logutil.EmptyFileLogConfig, false)
	if err := logutil.InitLogger(logCfg); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	logger := logutil.BgLogger()
	logger.Info("Starting TiDB Gateway Service",
		zap.String("version", mysql.TiDBReleaseVersion))

	// Initialize configuration
	cfg := config.GetGlobalConfig()
	cfg.Store = config.StoreTypeTiKV
	cfg.Path = *storagePath
	config.StoreGlobalConfig(cfg)

	// Register drivers
	registerDrivers(logger)

	// Initialize storage and domain (like tidb-server does)
	store, dom, err := initStorageAndDomain()
	if err != nil {
		logger.Fatal("Failed to initialize storage and domain", zap.Error(err))
	}
	defer func() {
		dom.Close()
		store.Close()
	}()

	logger.Info("Storage and domain initialized successfully")

	// Create service manager
	svcCfg := &service.Config{
		Mode: service.ModeDistributed,
		EnabledServices: []string{
			service.ServiceGateway,
			service.ServiceAdmin,
		},
		Registry: service.RegistryConfig{
			Type: "local",
		},
	}

	manager, err := service.NewManager(svcCfg)
	if err != nil {
		logger.Fatal("Failed to create service manager", zap.Error(err))
	}

	// Create gateway with pre-initialized backend
	gatewaySvc := gateway.New()
	backend := gateway.NewTiDBBackend(store, dom)
	clients := gateway.NewLocalBackendClient(backend)

	gatewayCfg := gateway.Config{
		Host:            *host,
		Port:            uint(*port),
		UseLocalBackend: false, // We're setting clients directly
	}
	gatewaySvc.SetConfig(gatewayCfg)
	gatewaySvc.SetClients(clients)

	adminSvc := admin.New()

	if err := manager.Register(gatewaySvc); err != nil {
		logger.Fatal("Failed to register gateway service", zap.Error(err))
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

	logger.Info("TiDB Gateway Service started",
		zap.String("host", *host),
		zap.Int("port", *port),
		zap.Int("status_port", *statusPort),
		zap.String("storage", *storagePath))

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("Shutting down TiDB Gateway Service")

	// Stop services
	if err := manager.Stop(ctx); err != nil {
		logger.Error("Error during shutdown", zap.Error(err))
	}

	logger.Info("TiDB Gateway Service stopped")
}

func registerDrivers(logger *zap.Logger) {
	if err := kvstore.Register(config.StoreTypeTiKV, &driver.TiKVDriver{}); err != nil {
		if !isAlreadyRegisteredError(err) {
			logger.Fatal("Failed to register TiKV driver", zap.Error(err))
		}
	}
	if err := kvstore.Register(config.StoreTypeMockTiKV, mockstore.MockTiKVDriver{}); err != nil {
		if !isAlreadyRegisteredError(err) {
			logger.Fatal("Failed to register MockTiKV driver", zap.Error(err))
		}
	}
}

func initStorageAndDomain() (kv.Storage, *domain.Domain, error) {
	cfg := config.GetGlobalConfig()

	// Initialize storage using the same pattern as tidb-server
	store, err := kvstore.New(cfg.Path)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create store: %w", err)
	}

	// Start DDL owner manager before bootstrapping session (required for DDL)
	err = ddl.StartOwnerManager(context.Background(), store)
	if err != nil {
		store.Close()
		return nil, nil, fmt.Errorf("failed to start DDL owner manager: %w", err)
	}

	// Bootstrap session and domain
	dom, err := session.BootstrapSession(store)
	if err != nil {
		store.Close()
		return nil, nil, fmt.Errorf("failed to bootstrap session: %w", err)
	}

	return store, dom, nil
}

func isAlreadyRegisteredError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "already registered")
}
