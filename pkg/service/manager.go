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

package service

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// Manager manages the lifecycle of all services.
// It handles service registration, dependency resolution, and coordinated
// startup/shutdown.
type Manager struct {
	mu             sync.RWMutex
	services       map[string]Service
	startOrder     []string // Services in dependency-sorted start order
	registry       Registry
	config         *Config
	ctx            context.Context
	cancel         context.CancelFunc
	healthAgg      *HealthAggregator
	logger         *zap.Logger
	started        bool
	shutdownHooks  []func()
}

// NewManager creates a new service manager.
func NewManager(cfg *Config) (*Manager, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	var registry Registry
	if cfg.Mode == ModeDistributed {
		etcdCfg := EtcdRegistryConfig{
			Endpoints: cfg.Registry.Endpoints,
			Prefix:    cfg.Registry.Prefix,
		}
		var err error
		registry, err = NewEtcdRegistry(etcdCfg)
		if err != nil {
			cancel()
			return nil, errors.Wrap(err, "failed to create etcd registry")
		}
	} else {
		registry = NewLocalRegistry()
	}

	return &Manager{
		services:  make(map[string]Service),
		registry:  registry,
		config:    cfg,
		ctx:       ctx,
		cancel:    cancel,
		healthAgg: NewHealthAggregator(),
		logger:    logutil.BgLogger().With(zap.String("component", "service-manager")),
	}, nil
}

// Register adds a service to the manager.
// The service is not started until Start() is called.
func (m *Manager) Register(svc Service) error {
	if svc == nil {
		return errors.New("service cannot be nil")
	}
	name := svc.Name()
	if name == "" {
		return errors.New("service name cannot be empty")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.started {
		return errors.New("cannot register service after manager has started")
	}

	if _, exists := m.services[name]; exists {
		return errors.Errorf("service %q already registered", name)
	}

	m.services[name] = svc
	m.healthAgg.Register(svc)
	m.logger.Info("registered service", zap.String("service", name))
	return nil
}

// Start initializes and starts all registered services in dependency order.
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return errors.New("manager already started")
	}
	m.mu.Unlock()

	// Resolve dependencies and determine start order
	order, err := m.resolveDependencies()
	if err != nil {
		return errors.Wrap(err, "failed to resolve dependencies")
	}

	m.mu.Lock()
	m.startOrder = order
	m.mu.Unlock()

	// Initialize and start services in order
	for _, name := range order {
		svc := m.services[name]

		// Check if this service should be enabled
		if !m.isServiceEnabled(name) {
			m.logger.Info("skipping disabled service", zap.String("service", name))
			continue
		}

		m.logger.Info("initializing service", zap.String("service", name))

		opts := Options{
			Config:   m.config.ServiceConfigs[name],
			Registry: m.registry,
			Logger:   &zapLogger{logger: m.logger.With(zap.String("service", name))},
		}

		if err := svc.Init(ctx, opts); err != nil {
			return errors.Wrapf(err, "failed to initialize service %q", name)
		}

		// Register with the registry
		if err := m.registry.Register(ctx, svc); err != nil {
			return errors.Wrapf(err, "failed to register service %q", name)
		}

		m.logger.Info("starting service", zap.String("service", name))
		if err := svc.Start(ctx); err != nil {
			return errors.Wrapf(err, "failed to start service %q", name)
		}

		m.logger.Info("service started", zap.String("service", name))
	}

	m.mu.Lock()
	m.started = true
	m.mu.Unlock()

	return nil
}

// Stop gracefully shuts down all services in reverse dependency order.
func (m *Manager) Stop(ctx context.Context) error {
	m.mu.Lock()
	if !m.started {
		m.mu.Unlock()
		return nil
	}
	order := m.startOrder
	m.mu.Unlock()

	// Run shutdown hooks first
	for _, hook := range m.shutdownHooks {
		hook()
	}

	// Stop services in reverse order
	var errs []error
	for i := len(order) - 1; i >= 0; i-- {
		name := order[i]
		svc, ok := m.services[name]
		if !ok {
			continue
		}

		m.logger.Info("stopping service", zap.String("service", name))
		if err := svc.Stop(ctx); err != nil {
			m.logger.Error("failed to stop service",
				zap.String("service", name),
				zap.Error(err))
			errs = append(errs, errors.Wrapf(err, "failed to stop service %q", name))
		} else {
			m.logger.Info("service stopped", zap.String("service", name))
		}

		// Unregister from registry
		if err := m.registry.Unregister(ctx, name); err != nil {
			m.logger.Warn("failed to unregister service",
				zap.String("service", name),
				zap.Error(err))
		}
	}

	// Close registry
	if err := m.registry.Close(); err != nil {
		errs = append(errs, errors.Wrap(err, "failed to close registry"))
	}

	m.cancel()

	m.mu.Lock()
	m.started = false
	m.mu.Unlock()

	if len(errs) > 0 {
		return errors.Errorf("errors during shutdown: %v", errs)
	}
	return nil
}

// Get returns a service by name.
func (m *Manager) Get(name string) (Service, error) {
	return m.registry.Get(m.ctx, name)
}

// GetClient returns a typed client for a service.
func (m *Manager) GetClient(name string) (any, error) {
	return m.registry.GetClient(m.ctx, name)
}

// Health returns the overall health status.
func (m *Manager) Health() HealthStatus {
	return m.healthAgg.GetOverallHealth()
}

// ServiceHealth returns the health of a specific service.
func (m *Manager) ServiceHealth(name string) (HealthStatus, bool) {
	return m.healthAgg.GetHealth(name)
}

// Registry returns the service registry.
func (m *Manager) Registry() Registry {
	return m.registry
}

// AddShutdownHook adds a function to be called during shutdown.
func (m *Manager) AddShutdownHook(hook func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shutdownHooks = append(m.shutdownHooks, hook)
}

// isServiceEnabled checks if a service should be started based on configuration.
func (m *Manager) isServiceEnabled(name string) bool {
	if m.config.Mode == ModeMonolithic {
		// In monolithic mode, all services are enabled
		return true
	}
	// In distributed mode, check if service is in enabled list
	for _, enabled := range m.config.EnabledServices {
		if enabled == name {
			return true
		}
	}
	return false
}

// resolveDependencies performs a topological sort to determine start order.
func (m *Manager) resolveDependencies() ([]string, error) {
	// Build dependency graph
	visited := make(map[string]bool)
	visiting := make(map[string]bool)
	order := make([]string, 0, len(m.services))

	var visit func(name string) error
	visit = func(name string) error {
		if visited[name] {
			return nil
		}
		if visiting[name] {
			return errors.Errorf("circular dependency detected involving %q", name)
		}

		svc, ok := m.services[name]
		if !ok {
			return errors.Errorf("dependency %q not found", name)
		}

		visiting[name] = true
		for _, dep := range svc.Dependencies() {
			if err := visit(dep); err != nil {
				return err
			}
		}
		visiting[name] = false
		visited[name] = true
		order = append(order, name)
		return nil
	}

	for name := range m.services {
		if err := visit(name); err != nil {
			return nil, err
		}
	}

	return order, nil
}

// zapLogger wraps zap.Logger to implement the Logger interface.
type zapLogger struct {
	logger *zap.Logger
}

func (l *zapLogger) Info(msg string, fields ...any) {
	l.logger.Info(msg, toZapFields(fields)...)
}

func (l *zapLogger) Warn(msg string, fields ...any) {
	l.logger.Warn(msg, toZapFields(fields)...)
}

func (l *zapLogger) Error(msg string, fields ...any) {
	l.logger.Error(msg, toZapFields(fields)...)
}

func (l *zapLogger) Debug(msg string, fields ...any) {
	l.logger.Debug(msg, toZapFields(fields)...)
}

func toZapFields(fields []any) []zap.Field {
	zapFields := make([]zap.Field, 0, len(fields)/2)
	for i := 0; i+1 < len(fields); i += 2 {
		key, ok := fields[i].(string)
		if !ok {
			continue
		}
		zapFields = append(zapFields, zap.Any(key, fields[i+1]))
	}
	return zapFields
}
