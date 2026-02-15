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
	"encoding/json"
	"path"
	"sync"
	"time"

	"github.com/pingcap/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	// DefaultServicePrefix is the default etcd key prefix for service registration.
	DefaultServicePrefix = "/tidb/services/"

	// DefaultServiceTTL is the default TTL for service registration in seconds.
	DefaultServiceTTL = 30

	// DefaultDialTimeout is the default timeout for etcd connection.
	DefaultDialTimeout = 5 * time.Second
)

// EtcdRegistryConfig contains configuration for the etcd registry.
type EtcdRegistryConfig struct {
	// Endpoints is the list of etcd endpoints.
	Endpoints []string

	// Prefix is the key prefix for service registration.
	Prefix string

	// TTL is the lease TTL in seconds.
	TTL int

	// DialTimeout is the timeout for connecting to etcd.
	DialTimeout time.Duration

	// ClientFactory creates clients for remote services.
	ClientFactory ClientFactory
}

// EtcdRegistry implements Registry using etcd for service discovery.
// This is used in distributed mode where services communicate via gRPC.
type EtcdRegistry struct {
	config        EtcdRegistryConfig
	client        *clientv3.Client
	leaseID       clientv3.LeaseID
	mu            sync.RWMutex
	localServices map[string]Service // Services registered locally
	remoteClients map[string]any     // Cached remote clients
	watchers      []chan ServiceEvent
	ctx           context.Context
	cancel        context.CancelFunc
}

// NewEtcdRegistry creates a new etcd-based service registry.
func NewEtcdRegistry(cfg EtcdRegistryConfig) (*EtcdRegistry, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, errors.New("etcd endpoints cannot be empty")
	}
	if cfg.Prefix == "" {
		cfg.Prefix = DefaultServicePrefix
	}
	if cfg.TTL == 0 {
		cfg.TTL = DefaultServiceTTL
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = DefaultDialTimeout
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: cfg.DialTimeout,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create etcd client")
	}

	ctx, cancel := context.WithCancel(context.Background())

	r := &EtcdRegistry{
		config:        cfg,
		client:        client,
		localServices: make(map[string]Service),
		remoteClients: make(map[string]any),
		ctx:           ctx,
		cancel:        cancel,
	}

	// Create a lease for service registration
	if err := r.createLease(ctx); err != nil {
		cancel()
		client.Close()
		return nil, err
	}

	// Start lease keepalive
	go r.keepalive()

	return r, nil
}

func (r *EtcdRegistry) createLease(ctx context.Context) error {
	lease, err := r.client.Grant(ctx, int64(r.config.TTL))
	if err != nil {
		return errors.Wrap(err, "failed to create etcd lease")
	}
	r.leaseID = lease.ID
	return nil
}

func (r *EtcdRegistry) keepalive() {
	ch, err := r.client.KeepAlive(r.ctx, r.leaseID)
	if err != nil {
		return
	}

	for {
		select {
		case <-r.ctx.Done():
			return
		case resp, ok := <-ch:
			if !ok {
				// Lease expired, try to recreate
				if err := r.createLease(r.ctx); err != nil {
					continue
				}
				// Re-register all local services
				r.reregisterServices()
				ch, _ = r.client.KeepAlive(r.ctx, r.leaseID)
			}
			_ = resp // Keepalive response
		}
	}
}

func (r *EtcdRegistry) reregisterServices() {
	r.mu.RLock()
	services := make([]Service, 0, len(r.localServices))
	for _, svc := range r.localServices {
		services = append(services, svc)
	}
	r.mu.RUnlock()

	for _, svc := range services {
		_ = r.registerToEtcd(r.ctx, svc)
	}
}

// serviceInfo contains the information stored in etcd for a service.
type serviceInfo struct {
	Name     string            `json:"name"`
	Address  string            `json:"address"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Register adds a service to the registry.
func (r *EtcdRegistry) Register(ctx context.Context, svc Service) error {
	if svc == nil {
		return errors.New("service cannot be nil")
	}
	name := svc.Name()
	if name == "" {
		return errors.New("service name cannot be empty")
	}

	r.mu.Lock()
	r.localServices[name] = svc
	r.mu.Unlock()

	return r.registerToEtcd(ctx, svc)
}

func (r *EtcdRegistry) registerToEtcd(ctx context.Context, svc Service) error {
	info := serviceInfo{
		Name: svc.Name(),
		// Address would be set from service configuration
		Metadata: make(map[string]string),
	}

	data, err := json.Marshal(info)
	if err != nil {
		return errors.Wrap(err, "failed to marshal service info")
	}

	key := path.Join(r.config.Prefix, svc.Name())
	_, err = r.client.Put(ctx, key, string(data), clientv3.WithLease(r.leaseID))
	if err != nil {
		return errors.Wrap(err, "failed to register service to etcd")
	}

	r.notifyWatchers(ServiceEvent{
		Type:    ServiceEventRegister,
		Name:    svc.Name(),
		Service: svc,
	})
	return nil
}

// Unregister removes a service from the registry.
func (r *EtcdRegistry) Unregister(ctx context.Context, name string) error {
	r.mu.Lock()
	delete(r.localServices, name)
	r.mu.Unlock()

	key := path.Join(r.config.Prefix, name)
	_, err := r.client.Delete(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to unregister service from etcd")
	}

	r.notifyWatchers(ServiceEvent{
		Type: ServiceEventUnregister,
		Name: name,
	})
	return nil
}

// Get returns a service by name.
// For local services, returns the service directly.
// For remote services, returns a wrapper that uses gRPC.
func (r *EtcdRegistry) Get(_ context.Context, name string) (Service, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Check local services first
	if svc, ok := r.localServices[name]; ok {
		return svc, nil
	}

	// Remote services don't return Service interface in distributed mode
	return nil, errors.Errorf("service %q not found locally", name)
}

// GetClient returns a typed client for the specified service.
func (r *EtcdRegistry) GetClient(ctx context.Context, name string) (any, error) {
	r.mu.RLock()
	// Check local services first
	if svc, ok := r.localServices[name]; ok {
		r.mu.RUnlock()
		return svc, nil
	}

	// Check cached remote clients
	if client, ok := r.remoteClients[name]; ok {
		r.mu.RUnlock()
		return client, nil
	}
	r.mu.RUnlock()

	// Discover and connect to remote service
	endpoint, err := r.discoverService(ctx, name)
	if err != nil {
		return nil, err
	}

	if r.config.ClientFactory == nil {
		return nil, errors.New("client factory not configured")
	}

	client, err := r.config.ClientFactory.CreateClient(ctx, endpoint)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create client")
	}

	r.mu.Lock()
	r.remoteClients[name] = client
	r.mu.Unlock()

	return client, nil
}

func (r *EtcdRegistry) discoverService(ctx context.Context, name string) (ServiceEndpoint, error) {
	key := path.Join(r.config.Prefix, name)
	resp, err := r.client.Get(ctx, key)
	if err != nil {
		return ServiceEndpoint{}, errors.Wrap(err, "failed to get service from etcd")
	}

	if len(resp.Kvs) == 0 {
		return ServiceEndpoint{}, errors.Errorf("service %q not found", name)
	}

	var info serviceInfo
	if err := json.Unmarshal(resp.Kvs[0].Value, &info); err != nil {
		return ServiceEndpoint{}, errors.Wrap(err, "failed to unmarshal service info")
	}

	return ServiceEndpoint{
		Name:     info.Name,
		Address:  info.Address,
		Metadata: info.Metadata,
	}, nil
}

// List returns all registered service names.
func (r *EtcdRegistry) List(ctx context.Context) ([]string, error) {
	resp, err := r.client.Get(ctx, r.config.Prefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return nil, errors.Wrap(err, "failed to list services from etcd")
	}

	names := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		name := path.Base(string(kv.Key))
		names = append(names, name)
	}
	return names, nil
}

// Watch returns a channel that receives service change events.
func (r *EtcdRegistry) Watch(ctx context.Context) (<-chan ServiceEvent, error) {
	ch := make(chan ServiceEvent, 16)

	r.mu.Lock()
	r.watchers = append(r.watchers, ch)
	r.mu.Unlock()

	// Start watching etcd
	go r.watchEtcd(ctx, ch)

	return ch, nil
}

func (r *EtcdRegistry) watchEtcd(ctx context.Context, ch chan ServiceEvent) {
	watchCh := r.client.Watch(ctx, r.config.Prefix, clientv3.WithPrefix())
	for {
		select {
		case <-ctx.Done():
			return
		case resp, ok := <-watchCh:
			if !ok {
				return
			}
			for _, event := range resp.Events {
				name := path.Base(string(event.Kv.Key))
				var eventType ServiceEventType
				switch event.Type {
				case clientv3.EventTypePut:
					eventType = ServiceEventRegister
				case clientv3.EventTypeDelete:
					eventType = ServiceEventUnregister
				}
				select {
				case ch <- ServiceEvent{Type: eventType, Name: name}:
				default:
				}
			}
		}
	}
}

// Close closes the registry and releases resources.
func (r *EtcdRegistry) Close() error {
	r.cancel()

	r.mu.Lock()
	for _, ch := range r.watchers {
		close(ch)
	}
	r.watchers = nil

	// Close remote clients
	if r.config.ClientFactory != nil {
		for _, client := range r.remoteClients {
			r.config.ClientFactory.CloseClient(client)
		}
	}
	r.remoteClients = make(map[string]any)
	r.localServices = make(map[string]Service)
	r.mu.Unlock()

	// Revoke lease
	if r.leaseID != 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		r.client.Revoke(ctx, r.leaseID)
		cancel()
	}

	return r.client.Close()
}

// notifyWatchers sends an event to all watchers.
func (r *EtcdRegistry) notifyWatchers(event ServiceEvent) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, ch := range r.watchers {
		select {
		case ch <- event:
		default:
		}
	}
}

// Ensure EtcdRegistry implements Registry.
var _ Registry = (*EtcdRegistry)(nil)
