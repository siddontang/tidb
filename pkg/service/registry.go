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

// Registry provides service discovery and lookup functionality.
// It abstracts the underlying service discovery mechanism, allowing
// the same code to work in both monolithic (in-process) and
// distributed (gRPC) modes.
type Registry interface {
	// Register adds a service to the registry.
	Register(ctx context.Context, svc Service) error

	// Unregister removes a service from the registry.
	Unregister(ctx context.Context, name string) error

	// Get returns a service by name.
	// In monolithic mode, this returns the actual service implementation.
	// In distributed mode, this may return a gRPC client wrapper.
	Get(ctx context.Context, name string) (Service, error)

	// GetClient returns a typed client for the specified service.
	// The caller should type-assert to the expected client interface.
	GetClient(ctx context.Context, name string) (any, error)

	// List returns all registered service names.
	List(ctx context.Context) ([]string, error)

	// Watch returns a channel that receives service change events.
	Watch(ctx context.Context) (<-chan ServiceEvent, error)

	// Close closes the registry and releases resources.
	Close() error
}

// ServiceEvent represents a change in service registration.
type ServiceEvent struct {
	// Type is the type of event.
	Type ServiceEventType

	// Name is the name of the affected service.
	Name string

	// Service is the service instance (may be nil for unregister events).
	Service Service
}

// ServiceEventType indicates the type of service event.
type ServiceEventType int

const (
	// ServiceEventRegister indicates a new service was registered.
	ServiceEventRegister ServiceEventType = iota
	// ServiceEventUnregister indicates a service was unregistered.
	ServiceEventUnregister
	// ServiceEventUpdate indicates a service was updated.
	ServiceEventUpdate
)

// ServiceEndpoint contains connection information for a remote service.
type ServiceEndpoint struct {
	// Name is the service name.
	Name string

	// Address is the network address (host:port).
	Address string

	// Metadata contains additional endpoint information.
	Metadata map[string]string
}

// ServiceResolver resolves service names to endpoints.
type ServiceResolver interface {
	// Resolve returns the endpoints for a service.
	Resolve(ctx context.Context, name string) ([]ServiceEndpoint, error)

	// Watch watches for endpoint changes.
	Watch(ctx context.Context, name string) (<-chan []ServiceEndpoint, error)
}

// ClientFactory creates clients for remote services.
type ClientFactory interface {
	// CreateClient creates a client for the given service endpoint.
	// The returned client should implement the service's client interface.
	CreateClient(ctx context.Context, endpoint ServiceEndpoint) (any, error)

	// CloseClient closes a client connection.
	CloseClient(client any) error
}
