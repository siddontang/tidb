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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// DomainServiceProvider adapts ServiceManager to work with the domain package.
// It implements the ServiceProvider interface expected by domain.Domain.
type DomainServiceProvider struct {
	manager *Manager
	ctx     context.Context
}

// NewDomainServiceProvider creates a new DomainServiceProvider that wraps
// a ServiceManager to provide service access to the domain package.
func NewDomainServiceProvider(mgr *Manager) *DomainServiceProvider {
	return &DomainServiceProvider{
		manager: mgr,
		ctx:     context.Background(),
	}
}

// GetService returns a service by name.
// The caller should type-assert to the expected service type.
func (p *DomainServiceProvider) GetService(name string) (any, error) {
	if p.manager == nil {
		return nil, errors.New("service manager is nil")
	}
	return p.manager.GetClient(name)
}

// GetStorage returns the kv.Storage from the storage service.
func (p *DomainServiceProvider) GetStorage() (kv.Storage, error) {
	svc, err := p.GetService(ServiceStorage)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get storage service")
	}

	// The storage service should provide a Storage() method
	type storageProvider interface {
		Storage() kv.Storage
	}

	provider, ok := svc.(storageProvider)
	if !ok {
		return nil, errors.New("storage service does not implement Storage() method")
	}

	return provider.Storage(), nil
}

// GetEtcdClient returns the etcd client from the metadata service.
func (p *DomainServiceProvider) GetEtcdClient() (*clientv3.Client, error) {
	svc, err := p.GetService(ServiceMetadata)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get metadata service")
	}

	// The metadata service should provide an EtcdClient() method
	type etcdClientProvider interface {
		EtcdClient() *clientv3.Client
	}

	provider, ok := svc.(etcdClientProvider)
	if !ok {
		return nil, errors.New("metadata service does not implement EtcdClient() method")
	}

	return provider.EtcdClient(), nil
}

// Manager returns the underlying service manager.
func (p *DomainServiceProvider) Manager() *Manager {
	return p.manager
}

// GetServiceClient is a generic helper to get a typed service client.
// Usage: client, err := GetServiceClient[ddl.DDLClient](provider, ServiceDDL)
func GetServiceClient[T any](p *DomainServiceProvider, name string) (T, error) {
	var zero T
	svc, err := p.GetService(name)
	if err != nil {
		return zero, err
	}
	client, ok := svc.(T)
	if !ok {
		return zero, errors.Errorf("service %q does not implement expected interface", name)
	}
	return client, nil
}
