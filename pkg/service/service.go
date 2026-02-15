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
)

// Service defines the interface that all TiDB services must implement.
// Services are modular components that can run independently or combined
// into a single TiDB binary.
type Service interface {
	// Name returns the unique identifier for this service.
	Name() string

	// Dependencies returns the list of service names that this service
	// depends on. These services must be started before this one.
	Dependencies() []string

	// Init initializes the service with the given options.
	// This is called before Start and should set up any required resources.
	Init(ctx context.Context, opts Options) error

	// Start begins the service operation.
	// This should be non-blocking; long-running operations should be
	// started in goroutines.
	Start(ctx context.Context) error

	// Stop gracefully shuts down the service.
	// This should wait for any in-flight operations to complete.
	Stop(ctx context.Context) error

	// Health returns the current health status of the service.
	Health() HealthStatus
}

// Options contains configuration options for initializing a service.
type Options struct {
	// Config holds service-specific configuration.
	Config any

	// Registry provides access to other services.
	Registry Registry

	// Logger provides structured logging.
	Logger Logger
}

// Logger defines the logging interface for services.
type Logger interface {
	Info(msg string, fields ...any)
	Warn(msg string, fields ...any)
	Error(msg string, fields ...any)
	Debug(msg string, fields ...any)
}

// BaseService provides a default implementation of the Service interface
// that can be embedded in concrete service implementations.
type BaseService struct {
	name         string
	dependencies []string
	health       HealthStatus
	registry     Registry
	logger       Logger
}

// NewBaseService creates a new BaseService with the given name and dependencies.
func NewBaseService(name string, dependencies ...string) *BaseService {
	return &BaseService{
		name:         name,
		dependencies: dependencies,
		health:       HealthStatus{State: StateUnknown},
	}
}

// Name returns the service name.
func (s *BaseService) Name() string {
	return s.name
}

// Dependencies returns the service dependencies.
func (s *BaseService) Dependencies() []string {
	return s.dependencies
}

// Health returns the current health status.
func (s *BaseService) Health() HealthStatus {
	return s.health
}

// SetHealth updates the health status.
func (s *BaseService) SetHealth(status HealthStatus) {
	s.health = status
}

// Registry returns the service registry.
func (s *BaseService) Registry() Registry {
	return s.registry
}

// Logger returns the logger.
func (s *BaseService) Logger() Logger {
	return s.logger
}

// InitBase initializes the base service fields from options.
func (s *BaseService) InitBase(opts Options) {
	s.registry = opts.Registry
	s.logger = opts.Logger
	s.health = HealthStatus{State: StateStarting}
}
