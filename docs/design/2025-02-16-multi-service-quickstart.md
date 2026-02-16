# TiDB Multi-Service Quick Start Guide

This guide helps you quickly deploy and test the TiDB multi-service architecture.

## Prerequisites

- Go 1.21+
- TiUP (for TiKV cluster)
- MySQL client

## Quick Start (5 minutes)

### Step 1: Build the Gateway

```bash
cd /path/to/tidb

# Build the gateway binary
go build -o bin/tidb-gateway ./cmd/tidb-gateway/
```

### Step 2: Start TiKV Cluster

```bash
# Start a minimal TiKV cluster using TiUP
tiup playground --mode tikv-slim &

# Wait for cluster initialization
sleep 15

# Verify cluster is ready
curl -s http://127.0.0.1:2379/pd/api/v1/version
```

### Step 3: Start the Gateway

```bash
# Start the gateway
./bin/tidb-gateway \
    -path "tikv://127.0.0.1:2379" \
    -P 4000 \
    -host "0.0.0.0"
```

You should see output like:
```
[INFO] ["Starting TiDB Gateway Service"]
[INFO] ["Storage and domain initialized successfully"]
[INFO] ["MySQL server started"] [addr=0.0.0.0:4000]
[INFO] ["Gateway service started"] [host=0.0.0.0] [port=4000]
[INFO] ["TiDB Gateway Service started"]
```

### Step 4: Connect and Test

```bash
# Connect with MySQL client
mysql -h 127.0.0.1 -P 4000 -u root

# Run test queries
mysql> SELECT 1;
mysql> CREATE DATABASE test;
mysql> USE test;
mysql> CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100));
mysql> INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
mysql> SELECT * FROM users;
mysql> DROP DATABASE test;
```

## Running as Background Service

```bash
# Create log directory
mkdir -p /var/log/tidb

# Start gateway in background
./bin/tidb-gateway \
    -path "tikv://127.0.0.1:2379" \
    -P 4000 \
    > /var/log/tidb/gateway.log 2>&1 &

# Check logs
tail -f /var/log/tidb/gateway.log
```

## Command-Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `-P` | MySQL port | 4000 |
| `-host` | Listen address | 0.0.0.0 |
| `-path` | TiKV path | tikv://127.0.0.1:2379 |
| `-status` | Status port | 10080 |
| `-config` | Config file | "" |
| `-V` | Print version | false |

## Verifying the Deployment

### 1. Check Process

```bash
ps aux | grep tidb-gateway
```

### 2. Check Port

```bash
lsof -i :4000
```

### 3. Test Connection

```bash
mysql -h 127.0.0.1 -P 4000 -u root -e "SELECT 1 as test;"
```

### 4. Run Comprehensive Test

```bash
mysql -h 127.0.0.1 -P 4000 -u root -e "
-- Basic operations
SELECT VERSION();
SHOW DATABASES;

-- DDL
CREATE DATABASE quicktest;
USE quicktest;
CREATE TABLE items (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10,2)
);

-- DML
INSERT INTO items (name, price) VALUES
    ('Widget', 9.99),
    ('Gadget', 19.99),
    ('Gizmo', 29.99);

-- Queries
SELECT * FROM items;
SELECT COUNT(*), AVG(price) FROM items;

-- Cleanup
DROP DATABASE quicktest;
"
```

## Stopping the Service

```bash
# Find and kill the gateway process
pkill -f tidb-gateway

# Or if you know the PID
kill <PID>

# Stop TiKV cluster
pkill -f "tiup playground"
```

## Troubleshooting

### Gateway won't start

1. Check if TiKV is running:
```bash
curl http://127.0.0.1:2379/pd/api/v1/version
```

2. Check if port is in use:
```bash
lsof -i :4000
```

3. Check logs for errors:
```bash
tail -100 /var/log/tidb/gateway.log | grep -i error
```

### Connection refused

1. Verify gateway is listening:
```bash
netstat -an | grep 4000
```

2. Check firewall rules:
```bash
sudo iptables -L -n | grep 4000
```

### Query errors

Check gateway logs for detailed error messages:
```bash
grep -i "error\|failed" /var/log/tidb/gateway.log
```

## Next Steps

- Read the [full design document](./2025-02-16-multi-service-architecture.md)
- Configure for production with proper logging and monitoring
- Set up multiple gateway instances for high availability
- Integrate with your application's connection pooling

## Architecture Overview

```
┌──────────────────┐
│   MySQL Client   │
└────────┬─────────┘
         │ MySQL Protocol
         ▼
┌──────────────────┐
│   TiDB Gateway   │ ← You are here
│   (Port 4000)    │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│   TiDB Backend   │
│   (Embedded)     │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│   TiKV Cluster   │
│   (Port 2379)    │
└──────────────────┘
```

The gateway provides:
- MySQL protocol compatibility
- Query routing and execution
- DDL operations (CREATE, DROP, ALTER)
- Transaction support (BEGIN, COMMIT, ROLLBACK)
- Full SQL support via embedded TiDB backend
