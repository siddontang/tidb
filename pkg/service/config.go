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

// Mode defines the operational mode for the service architecture.
type Mode string

const (
	// ModeMonolithic runs all services in a single process (default, backward compatible).
	ModeMonolithic Mode = "monolithic"

	// ModeDistributed runs services across multiple processes with gRPC communication.
	ModeDistributed Mode = "distributed"
)

// Config contains the configuration for the service framework.
type Config struct {
	// Mode determines whether services run in-process or distributed.
	Mode Mode `toml:"mode" json:"mode"`

	// EnabledServices lists which services to run in this instance.
	// Only used in distributed mode. If empty in monolithic mode, all services run.
	EnabledServices []string `toml:"enabled" json:"enabled"`

	// Registry contains service registry configuration.
	Registry RegistryConfig `toml:"registry" json:"registry"`

	// Remote contains addresses of remote services (distributed mode).
	Remote RemoteServicesConfig `toml:"remote" json:"remote"`

	// ServiceConfigs contains service-specific configuration.
	ServiceConfigs map[string]any `toml:"service_configs" json:"service_configs"`
}

// RegistryConfig contains configuration for the service registry.
type RegistryConfig struct {
	// Type is the registry type: "local" or "etcd".
	Type string `toml:"type" json:"type"`

	// Endpoints is the list of registry endpoints (for etcd).
	Endpoints []string `toml:"endpoints" json:"endpoints"`

	// Prefix is the key prefix for service registration.
	Prefix string `toml:"prefix" json:"prefix"`
}

// RemoteServicesConfig contains addresses for remote services.
type RemoteServicesConfig struct {
	// DDL is the address of the DDL service.
	DDL string `toml:"ddl" json:"ddl"`

	// Statistics is the address of the statistics service.
	Statistics string `toml:"statistics" json:"statistics"`

	// Schema is the address of the schema service.
	Schema string `toml:"schema" json:"schema"`

	// Transaction is the address of the transaction service.
	Transaction string `toml:"transaction" json:"transaction"`

	// Coprocessor is the address of the coprocessor service.
	Coprocessor string `toml:"coprocessor" json:"coprocessor"`
}

// DefaultConfig returns the default service configuration (monolithic mode).
func DefaultConfig() *Config {
	return &Config{
		Mode:            ModeMonolithic,
		EnabledServices: nil, // All services in monolithic mode
		Registry: RegistryConfig{
			Type: "local",
		},
		ServiceConfigs: make(map[string]any),
	}
}

// ServiceNames defines the standard service names.
const (
	// ServiceStorage is the storage service name.
	ServiceStorage = "storage"

	// ServiceMetadata is the metadata/coordination service name.
	ServiceMetadata = "metadata"

	// ServiceSchema is the schema/infoschema service name.
	ServiceSchema = "schema"

	// ServiceDDL is the DDL service name.
	ServiceDDL = "ddl"

	// ServiceStatistics is the statistics service name.
	ServiceStatistics = "statistics"

	// ServiceAutoID is the auto ID service name.
	ServiceAutoID = "autoid"

	// ServiceTransaction is the transaction service name.
	ServiceTransaction = "transaction"

	// ServiceCoprocessor is the coprocessor service name.
	ServiceCoprocessor = "coprocessor"

	// ServiceSession is the session service name.
	ServiceSession = "session"

	// ServiceQuery is the query (planner + executor) service name.
	ServiceQuery = "query"

	// ServiceGateway is the MySQL protocol gateway service name.
	ServiceGateway = "gateway"

	// ServiceAdmin is the HTTP admin service name.
	ServiceAdmin = "admin"
)

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c.Mode != ModeMonolithic && c.Mode != ModeDistributed {
		c.Mode = ModeMonolithic
	}

	if c.Mode == ModeDistributed {
		if c.Registry.Type == "etcd" && len(c.Registry.Endpoints) == 0 {
			// Use default PD endpoints
			c.Registry.Endpoints = []string{"127.0.0.1:2379"}
		}
	}

	return nil
}

// IsMonolithic returns true if running in monolithic mode.
func (c *Config) IsMonolithic() bool {
	return c.Mode == ModeMonolithic
}

// IsDistributed returns true if running in distributed mode.
func (c *Config) IsDistributed() bool {
	return c.Mode == ModeDistributed
}

// IsServiceEnabled checks if a service should be started.
func (c *Config) IsServiceEnabled(name string) bool {
	if c.IsMonolithic() {
		return true
	}
	for _, s := range c.EnabledServices {
		if s == name {
			return true
		}
	}
	return false
}
