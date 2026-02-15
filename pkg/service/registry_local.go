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
)

// LocalRegistry implements Registry for monolithic (in-process) mode.
// It directly stores and returns service implementations without any
// network communication.
type LocalRegistry struct {
	mu       sync.RWMutex
	services map[string]Service
	clients  map[string]any
	watchers []chan ServiceEvent
}

// NewLocalRegistry creates a new local registry.
func NewLocalRegistry() *LocalRegistry {
	return &LocalRegistry{
		services: make(map[string]Service),
		clients:  make(map[string]any),
	}
}

// Register adds a service to the registry.
func (r *LocalRegistry) Register(ctx context.Context, svc Service) error {
	if svc == nil {
		return errors.New("service cannot be nil")
	}
	name := svc.Name()
	if name == "" {
		return errors.New("service name cannot be empty")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.services[name]; exists {
		return errors.Errorf("service %q already registered", name)
	}

	r.services[name] = svc
	r.notifyWatchers(ServiceEvent{
		Type:    ServiceEventRegister,
		Name:    name,
		Service: svc,
	})
	return nil
}

// Unregister removes a service from the registry.
func (r *LocalRegistry) Unregister(_ context.Context, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.services[name]; !exists {
		return errors.Errorf("service %q not found", name)
	}

	delete(r.services, name)
	delete(r.clients, name)
	r.notifyWatchers(ServiceEvent{
		Type: ServiceEventUnregister,
		Name: name,
	})
	return nil
}

// Get returns a service by name.
func (r *LocalRegistry) Get(_ context.Context, name string) (Service, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	svc, ok := r.services[name]
	if !ok {
		return nil, errors.Errorf("service %q not found", name)
	}
	return svc, nil
}

// GetClient returns a typed client for the specified service.
// In local mode, this returns the service itself if it implements
// the expected client interface, or a registered client wrapper.
func (r *LocalRegistry) GetClient(_ context.Context, name string) (any, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// First check if there's a specific client registered
	if client, ok := r.clients[name]; ok {
		return client, nil
	}

	// Otherwise return the service itself
	svc, ok := r.services[name]
	if !ok {
		return nil, errors.Errorf("service %q not found", name)
	}
	return svc, nil
}

// RegisterClient registers a client implementation for a service.
// This is useful when the service implementation doesn't directly
// implement the client interface.
func (r *LocalRegistry) RegisterClient(name string, client any) error {
	if client == nil {
		return errors.New("client cannot be nil")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.clients[name] = client
	return nil
}

// List returns all registered service names.
func (r *LocalRegistry) List(_ context.Context) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.services))
	for name := range r.services {
		names = append(names, name)
	}
	return names, nil
}

// Watch returns a channel that receives service change events.
func (r *LocalRegistry) Watch(_ context.Context) (<-chan ServiceEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ch := make(chan ServiceEvent, 16)
	r.watchers = append(r.watchers, ch)
	return ch, nil
}

// Close closes the registry and releases resources.
func (r *LocalRegistry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, ch := range r.watchers {
		close(ch)
	}
	r.watchers = nil
	r.services = make(map[string]Service)
	r.clients = make(map[string]any)
	return nil
}

// notifyWatchers sends an event to all watchers.
// Must be called with mu held.
func (r *LocalRegistry) notifyWatchers(event ServiceEvent) {
	for _, ch := range r.watchers {
		select {
		case ch <- event:
		default:
			// Channel full, skip
		}
	}
}

// Ensure LocalRegistry implements Registry.
var _ Registry = (*LocalRegistry)(nil)
