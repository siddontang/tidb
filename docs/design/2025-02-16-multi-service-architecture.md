# TiDB Multi-Service Architecture

## Overview

This document describes the multi-service architecture for TiDB, which decomposes the monolithic TiDB server into modular, independently deployable services while maintaining full backward compatibility.

## Table of Contents

1. [Design Goals](#design-goals)
2. [Architecture Overview](#architecture-overview)
3. [Service Components](#service-components)
4. [Communication Patterns](#communication-patterns)
5. [Deployment Modes](#deployment-modes)
6. [Configuration](#configuration)
7. [Deployment Guide](#deployment-guide)
8. [Testing](#testing)
9. [Troubleshooting](#troubleshooting)

---

## Design Goals

### Primary Goals

1. **Modularity**: Decompose TiDB into independent services that can be developed, tested, and deployed separately.

2. **Backward Compatibility**: The default monolithic mode works exactly as before with no configuration changes required.

3. **Flexible Deployment**: Support both monolithic (all-in-one) and distributed (microservices) deployment models.

4. **Zero Overhead in Monolithic Mode**: In-process communication with no serialization overhead when running as a single binary.

5. **Gradual Migration**: Teams can adopt distributed mode incrementally, service by service.

### Non-Goals

- Complete rewrite of existing TiDB internals
- Breaking changes to existing APIs or protocols
- Mandatory distributed deployment

---

## Architecture Overview

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Client Applications                         │
│                      (MySQL Protocol / JDBC / etc.)                  │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          Gateway Service                             │
│                    (MySQL Protocol Handler)                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │
│  │  Handshake  │  │   Parser    │  │   Router    │  │  Results    │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
         ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
         │ DDL Service  │  │Query Service │  │Stats Service │
         │              │  │              │  │              │
         │ - Schema Ops │  │ - Planning   │  │ - Analyze    │
         │ - Migrations │  │ - Execution  │  │ - Histograms │
         └──────────────┘  └──────────────┘  └──────────────┘
                    │               │               │
                    └───────────────┼───────────────┘
                                    ▼
         ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
         │  Txn Service │  │Session Svc   │  │ Copr Service │
         │              │  │              │  │              │
         │ - Begin/End  │  │ - Variables  │  │ - DistSQL    │
         │ - 2PC Coord  │  │ - Pool Mgmt  │  │ - MPP        │
         └──────────────┘  └──────────────┘  └──────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          Storage Layer                               │
│                    (TiKV / PD / TiFlash)                            │
└─────────────────────────────────────────────────────────────────────┘
```

### Component Interaction

```
┌────────────────────────────────────────────────────────────────┐
│                      Service Manager                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │   Registry   │  │    Health    │  │  Lifecycle Manager   │  │
│  │  (Local/Etcd)│  │  Aggregator  │  │  (Start/Stop Order)  │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│   Service A  │      │   Service B  │      │   Service C  │
│ ┌──────────┐ │      │ ┌──────────┐ │      │ ┌──────────┐ │
│ │Go Iface  │◄├──────┤►│Go Iface  │◄├──────┤►│Go Iface  │ │
│ └──────────┘ │      │ └──────────┘ │      │ └──────────┘ │
│ ┌──────────┐ │      │ ┌──────────┐ │      │ ┌──────────┐ │
│ │gRPC Svc  │ │      │ │gRPC Svc  │ │      │ │gRPC Svc  │ │
│ └──────────┘ │      │ └──────────┘ │      │ └──────────┘ │
└──────────────┘      └──────────────┘      └──────────────┘
```

---

## Service Components

### Core Services

| Service | Package | Description | Dependencies |
|---------|---------|-------------|--------------|
| **Gateway** | `/pkg/service/gateway` | MySQL protocol frontend, connection management | Query, DDL, Txn, Stats |
| **Query** | `/pkg/service/query` | SQL planning and execution | Session, Copr |
| **DDL** | `/pkg/service/ddl` | Schema operations, migrations | Storage, Metadata |
| **Statistics** | `/pkg/service/stats` | Query statistics, histograms | Storage, Schema |
| **Transaction** | `/pkg/service/txn` | Transaction coordination, 2PC | Storage |
| **Session** | `/pkg/service/session` | Session management, variables | Storage |
| **Coprocessor** | `/pkg/service/copr` | Distributed computation | Storage |
| **Admin** | `/pkg/service/admin` | HTTP status, metrics | All services |

### Service Interface

All services implement the common `Service` interface:

```go
type Service interface {
    // Name returns the service name
    Name() string

    // Dependencies returns services this service depends on
    Dependencies() []string

    // Init initializes the service with options
    Init(ctx context.Context, opts Options) error

    // Start starts the service
    Start(ctx context.Context) error

    // Stop gracefully stops the service
    Stop(ctx context.Context) error

    // Health returns current health status
    Health() HealthStatus
}
```

### Gateway Service

The Gateway service is the entry point for MySQL client connections:

```go
// Gateway configuration
type Config struct {
    Host            string `toml:"host" json:"host"`
    Port            uint   `toml:"port" json:"port"`
    Socket          string `toml:"socket" json:"socket"`
    MaxConnections  uint32 `toml:"max-connections" json:"max-connections"`
    StoragePath     string `toml:"storage-path" json:"storage-path"`
    UseLocalBackend bool   `toml:"use-local-backend" json:"use-local-backend"`
}
```

**Features:**
- Full MySQL protocol support (handshake, authentication, commands)
- Connection pooling and management
- Query routing to backend services
- Result set encoding

### Query Service

Handles SQL parsing, planning, and execution:

**Supported Operations:**
- SELECT queries with joins, aggregations, subqueries
- INSERT, UPDATE, DELETE statements
- SHOW and EXPLAIN commands

### DDL Service

Manages schema operations with distributed coordination:

**Supported Operations:**
- CREATE/DROP DATABASE
- CREATE/DROP/ALTER TABLE
- CREATE/DROP INDEX
- ADD/DROP COLUMN

---

## Communication Patterns

### Local Mode (In-Process)

```
┌─────────────────────────────────────────┐
│              Single Process              │
│  ┌─────────┐    Direct Call   ┌───────┐ │
│  │ Gateway │ ───────────────► │ Query │ │
│  └─────────┘                  └───────┘ │
│       │                           │     │
│       │         Go Interface      │     │
│       ▼                           ▼     │
│  ┌─────────┐                 ┌───────┐  │
│  │   DDL   │                 │ Stats │  │
│  └─────────┘                 └───────┘  │
└─────────────────────────────────────────┘
```

- Zero serialization overhead
- Direct function calls
- Shared memory space
- Used by default in monolithic mode

### Distributed Mode (gRPC)

```
┌──────────────┐         ┌──────────────┐
│   Gateway    │  gRPC   │    Query     │
│   Process    │ ◄─────► │   Process    │
└──────────────┘         └──────────────┘
       │                        │
       │ gRPC                   │ gRPC
       ▼                        ▼
┌──────────────┐         ┌──────────────┐
│     DDL      │         │    Stats     │
│   Process    │         │   Process    │
└──────────────┘         └──────────────┘
```

- Protocol Buffers serialization
- Network transport
- Service discovery via etcd
- Independent scaling

---

## Deployment Modes

### Mode 1: Monolithic (Default)

All services run in a single process, exactly like traditional TiDB.

```
┌─────────────────────────────────────────┐
│              tidb-server                 │
│  ┌─────────┬─────────┬─────────────────┐│
│  │ Gateway │ Query   │ DDL │Stats│...  ││
│  └─────────┴─────────┴─────────────────┘│
└─────────────────────────────────────────┘
                    │
                    ▼
              ┌──────────┐
              │   TiKV   │
              └──────────┘
```

**Characteristics:**
- Single binary deployment
- No configuration changes required
- Zero communication overhead
- Full backward compatibility

### Mode 2: Gateway + Backend

Separate gateway from query processing.

```
┌──────────────┐      ┌──────────────────────┐
│ tidb-gateway │      │     tidb-server      │
│  (Frontend)  │─────►│  (Backend Services)  │
└──────────────┘      └──────────────────────┘
                                │
                                ▼
                          ┌──────────┐
                          │   TiKV   │
                          └──────────┘
```

**Use Cases:**
- Edge deployment with centralized processing
- Connection pooling at scale
- Protocol translation layer

### Mode 3: Full Distributed

Each service runs as an independent process.

```
┌────────────┐   ┌────────────┐   ┌────────────┐
│  Gateway   │   │   Query    │   │    DDL     │
│  (x3)      │   │   (x5)     │   │   (x2)     │
└────────────┘   └────────────┘   └────────────┘
      │                │                │
      └────────────────┼────────────────┘
                       ▼
                 ┌──────────┐
                 │   etcd   │ (Service Discovery)
                 └──────────┘
                       │
                       ▼
                 ┌──────────┐
                 │   TiKV   │
                 └──────────┘
```

**Use Cases:**
- Independent scaling of services
- Fault isolation
- Resource optimization

---

## Configuration

### Service Configuration (config.toml)

```toml
# Service mode: "monolithic" (default) or "distributed"
[services]
mode = "monolithic"

# Services to enable in this instance (distributed mode)
enabled = ["gateway", "query", "ddl", "stats", "txn", "session", "admin"]

# Service registry configuration
[services.registry]
type = "local"  # "local" or "etcd"
endpoints = ["127.0.0.1:2379"]
prefix = "/tidb/services"

# Remote service endpoints (distributed mode)
[services.remote]
ddl = "tidb-ddl:4001"
statistics = "tidb-stats:4002"
query = "tidb-query:4003"
transaction = "tidb-txn:4004"
```

### Gateway Configuration

```toml
[gateway]
host = "0.0.0.0"
port = 4000
max-connections = 4096
use-local-backend = true
storage-path = "tikv://127.0.0.1:2379"
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `TIDB_SERVICE_MODE` | Service mode | `monolithic` |
| `TIDB_REGISTRY_TYPE` | Registry type | `local` |
| `TIDB_REGISTRY_ENDPOINTS` | Etcd endpoints | `127.0.0.1:2379` |

---

## Deployment Guide

### Prerequisites

1. **TiKV Cluster**: Running TiKV with PD
2. **Go 1.21+**: For building from source
3. **etcd** (optional): For distributed service discovery

### Building

```bash
# Build all binaries
make build

# Or build individual services
go build -o bin/tidb-server ./cmd/tidb-server/
go build -o bin/tidb-gateway ./cmd/tidb-gateway/
go build -o bin/tidb-ddl ./cmd/tidb-ddl/
go build -o bin/tidb-stats ./cmd/tidb-stats/
go build -o bin/tidb-query ./cmd/tidb-query/
go build -o bin/tidb-txn ./cmd/tidb-txn/
```

### Deployment Option 1: Monolithic Mode

This is the default mode and requires no special configuration.

```bash
# Start TiKV cluster first
tiup playground --mode tikv-slim &

# Wait for cluster to be ready
sleep 10

# Start TiDB server (monolithic mode)
./bin/tidb-server \
    --store tikv \
    --path "127.0.0.1:2379" \
    --status 10080 \
    -P 4000

# Connect with MySQL client
mysql -h 127.0.0.1 -P 4000 -u root
```

### Deployment Option 2: Gateway Mode

Run the gateway as a separate frontend.

```bash
# Start TiKV cluster
tiup playground --mode tikv-slim &
sleep 10

# Start TiDB Gateway
./bin/tidb-gateway \
    -path "tikv://127.0.0.1:2379" \
    -P 4000 \
    -host "0.0.0.0"

# Connect with MySQL client
mysql -h 127.0.0.1 -P 4000 -u root
```

**Gateway Command-Line Options:**

| Option | Description | Default |
|--------|-------------|---------|
| `-P` | MySQL protocol port | 4000 |
| `-host` | Listen host | 0.0.0.0 |
| `-path` | TiKV storage path | tikv://127.0.0.1:2379 |
| `-status` | Status port | 10080 |
| `-config` | Config file path | "" |
| `-V` | Print version | false |

### Deployment Option 3: Multi-Service Mode

For testing multiple services together:

```bash
#!/bin/bash
# multi-service-start.sh

# Create log directory
mkdir -p /tmp/tidb-multi-services/logs

# Start TiKV cluster
tiup playground --mode tikv-slim &
sleep 15

# Start services
./bin/tidb-gateway \
    -path "tikv://127.0.0.1:2379" \
    -P 4000 \
    > /tmp/tidb-multi-services/logs/gateway.log 2>&1 &

echo "Gateway started on port 4000"
echo "Logs: /tmp/tidb-multi-services/logs/gateway.log"

# Wait for startup
sleep 20

# Verify
mysql -h 127.0.0.1 -P 4000 -u root -e "SELECT 1;"
```

### Docker Deployment

```dockerfile
# Dockerfile.gateway
FROM golang:1.21 AS builder
WORKDIR /app
COPY . .
RUN go build -o tidb-gateway ./cmd/tidb-gateway/

FROM debian:bookworm-slim
COPY --from=builder /app/tidb-gateway /usr/local/bin/
EXPOSE 4000 10080
ENTRYPOINT ["tidb-gateway"]
CMD ["-path", "tikv://pd:2379", "-P", "4000"]
```

```yaml
# docker-compose.yml
version: '3.8'
services:
  gateway:
    build:
      context: .
      dockerfile: Dockerfile.gateway
    ports:
      - "4000:4000"
      - "10080:10080"
    command: ["-path", "tikv://pd:2379", "-P", "4000"]
    depends_on:
      - pd
      - tikv
```

### Kubernetes Deployment

```yaml
# tidb-gateway-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tidb-gateway
spec:
  replicas: 3
  selector:
    matchLabels:
      app: tidb-gateway
  template:
    metadata:
      labels:
        app: tidb-gateway
    spec:
      containers:
      - name: gateway
        image: pingcap/tidb-gateway:latest
        ports:
        - containerPort: 4000
          name: mysql
        - containerPort: 10080
          name: status
        args:
        - "-path"
        - "tikv://pd-service:2379"
        - "-P"
        - "4000"
        readinessProbe:
          tcpSocket:
            port: 4000
          initialDelaySeconds: 10
          periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: tidb-gateway
spec:
  type: LoadBalancer
  ports:
  - port: 4000
    targetPort: 4000
    name: mysql
  selector:
    app: tidb-gateway
```

---

## Testing

### Unit Tests

```bash
# Run service framework tests
go test -v --tags=intest ./pkg/service/...

# Run gateway tests
go test -v --tags=intest ./pkg/service/gateway/...
```

### Integration Tests

```bash
# Start TiKV
tiup playground --mode tikv-slim &
sleep 15

# Run gateway and test
./bin/tidb-gateway -path "tikv://127.0.0.1:2379" -P 4000 &
sleep 20

# Basic connectivity test
mysql -h 127.0.0.1 -P 4000 -u root -e "SELECT 1;"

# Comprehensive test
mysql -h 127.0.0.1 -P 4000 -u root -e "
CREATE DATABASE test_db;
USE test_db;
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100));
INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
SELECT * FROM users;
DROP TABLE users;
DROP DATABASE test_db;
"
```

### Performance Testing

```bash
# Using sysbench
sysbench oltp_read_write \
    --mysql-host=127.0.0.1 \
    --mysql-port=4000 \
    --mysql-user=root \
    --tables=4 \
    --table-size=100000 \
    prepare

sysbench oltp_read_write \
    --mysql-host=127.0.0.1 \
    --mysql-port=4000 \
    --mysql-user=root \
    --tables=4 \
    --table-size=100000 \
    --threads=16 \
    --time=60 \
    run
```

---

## Troubleshooting

### Common Issues

#### 1. "storage tikv is not registered"

**Cause**: TiKV driver not registered before storage initialization.

**Solution**: Ensure driver registration happens before store creation:
```go
import "github.com/pingcap/tidb/pkg/store/driver"

kvstore.Register("tikv", driver.TiKVDriver{})
```

#### 2. "nil pointer dereference in ddl.Start"

**Cause**: DDL Owner Manager not initialized.

**Solution**: Call `ddl.StartOwnerManager` before `session.BootstrapSession`:
```go
err := ddl.StartOwnerManager(ctx, store)
if err != nil {
    return err
}
dom, err := session.BootstrapSession(store)
```

#### 3. "Lost connection to MySQL server at 'reading authorization packet'"

**Cause**: MySQL protocol sequence number mismatch.

**Solution**: Ensure proper sequence number tracking in packet read/write.

#### 4. Service fails to connect to TiKV

**Cause**: PD/TiKV not ready or wrong endpoint.

**Solution**:
```bash
# Verify PD is accessible
curl http://127.0.0.1:2379/pd/api/v1/version

# Check TiKV stores
curl http://127.0.0.1:2379/pd/api/v1/stores
```

### Logs

Gateway logs location:
- Console output by default
- Redirect to file: `./bin/tidb-gateway ... > gateway.log 2>&1`

Log levels:
```bash
# Set log level via config
[log]
level = "info"  # debug, info, warn, error
```

### Health Checks

```bash
# Check service health
curl http://127.0.0.1:10080/status

# Check metrics
curl http://127.0.0.1:10080/metrics
```

---

## File Structure

```
/pkg/service/
├── service.go           # Service interface definition
├── manager.go           # Service lifecycle manager
├── registry.go          # Registry interface
├── registry_local.go    # In-process registry
├── registry_etcd.go     # Etcd-based registry
├── health.go            # Health aggregation
├── config.go            # Service configuration
│
├── gateway/             # Gateway service
│   ├── service.go       # Service implementation
│   ├── mysql_server.go  # MySQL protocol handler
│   ├── clients.go       # Service client interfaces
│   ├── grpc_clients.go  # gRPC client implementations
│   └── tidb_backend.go  # Local TiDB backend
│
├── ddl/                 # DDL service
│   └── service.go
│
├── stats/               # Statistics service
│   └── service.go
│
├── query/               # Query service
│   ├── service.go
│   └── grpc_handler.go
│
├── txn/                 # Transaction service
│   └── service.go
│
├── session/             # Session service
│   └── service.go
│
├── copr/                # Coprocessor service
│   └── service.go
│
└── admin/               # Admin service
    └── service.go

/cmd/
├── tidb-server/         # Monolithic server
│   └── main.go
├── tidb-gateway/        # Gateway binary
│   └── main.go
├── tidb-ddl/            # DDL service binary
│   └── main.go
├── tidb-stats/          # Stats service binary
│   └── main.go
├── tidb-query/          # Query service binary
│   └── main.go
└── tidb-txn/            # Transaction service binary
    └── main.go
```

---

## Future Work

1. **gRPC Protocol Implementation**: Full gRPC interfaces for distributed mode
2. **Service Mesh Integration**: Istio/Linkerd support
3. **Dynamic Scaling**: Auto-scaling based on load
4. **Observability**: Distributed tracing with OpenTelemetry
5. **Multi-tenancy**: Namespace isolation between services

---

## References

- [TiDB Architecture](https://docs.pingcap.com/tidb/stable/tidb-architecture)
- [MySQL Protocol](https://dev.mysql.com/doc/internals/en/client-server-protocol.html)
- [gRPC Documentation](https://grpc.io/docs/)
- [etcd Documentation](https://etcd.io/docs/)
