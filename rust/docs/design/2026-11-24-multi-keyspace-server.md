# Multi-keyspace tidb-server: one process, many tenants

- Status: **Draft for review**. No code has been written yet.
- Scope: the Rust SQL node (`rust/crates/tidb-server`), Next Generation kernel (`--features nextgen`) only.
- Branch base: `hparser-integration` @ `73a0893eff`.

## 1. Summary

Today one `tidb-server` process serves exactly one keyspace, chosen by the
`keyspace-name` config. This design lets one process serve many user
keyspaces. The client picks its keyspace per connection through the MySQL user
name, in the same `<prefix>.<account>` form that TiDB Cloud Starter uses:

```sh
mysql -h 127.0.0.1 -P 4000 -u 'keyspace1.root' -p   # tenant keyspace1
mysql -h 127.0.0.1 -P 4000 -u 'keyspace2.app'  -p   # tenant keyspace2, same server
```

The server splits the user name at the first `.`, opens or reuses a
per-keyspace runtime (store, domain, accounts, background loops), and then runs
the existing authentication and session path against that runtime unchanged.

Most of the work is not in routing. It is in turning about 30 process-wide
singletons into per-keyspace state, so that one tenant's `SET GLOBAL`,
statistics, statement summary, `KILL` or `SHOW PROCESSLIST` cannot affect or
observe another tenant.

## 2. Background

### 2.1 How TiDB Cloud Starter does it today

- Each Starter instance has a unique user-name prefix. Users connect as
  `'<prefix>.root'` on port 4000, and TLS is required.
- Users are created with the prefix: `CREATE USER '<prefix>.jeffrey'`.
  `mysql.user` stores the full prefixed name.
- Go TiDB defines the prefix as the keyspace name:
  `pkg/keyspace/username_policy.go`, `prefixPolicy{userPrefix: GetKeyspaceNameBySettings()}`,
  active when `deploymode.IsStarter()`.
- Go TiDB has no in-process multi-tenancy. Starter runs **one TiDB process
  per keyspace**:
  - a pool of standby processes waits under `pkg/standby`;
  - a process is activated for a keyspace over HTTP;
  - an external gateway routes connections to the right process using the
    prefix. Go only sees the gateway through connection attributes
    (`TiDB-Gateway-ConnID`, `GATEWAY_SECURECONN_ATTR_KEY`) and the PROXY
    protocol.

This design moves that routing step into the SQL node itself.

### 2.2 What the Rust node already has

| Piece | State | Where |
| --- | --- | --- |
| Keyspace-bound store | `TikvStore::connect` with `with_keyspace(name)`; PD `LoadKeyspace`; API v2 codec; per-keyspace TSO routing | `tidb-txnkv/src/driver/tikv_store.rs:154`, `third_party/tikv-client-rs/src/pd/client.rs:2087` |
| Several stores in one process | Yes. `STORE_CACHE` keyed `tikv-{cluster}/{keyspace}`, shared store runtime | `tikv_store.rs:93,129,168` |
| Second keyspace in-process | Yes, as a cross-keyspace "lite domain": no DDL owner, no privileges, empty sysvars. Used for SYSTEM | `tidb-domain-crossks`, `tidb-session/src/domain/cross_ks.rs:87,424` |
| Per-keyspace DDL owner managers | `GLOBAL_OWNER_MANAGERS: HashMap<keyspace, ..>` | `tidb-ddl/src/owner_mgr.rs:16` |
| Per-instance domain state | `Domain` owns the catalog, privileges, process registry, stats, DDL and plan cache. `ConfiguredUserStore` owns accounts and `GlobalSysvars` | `tidb-domain-core/src/domain/mod.rs:638`, `tidb-session/src/domain/configured_user_store.rs:194` |
| Username prefix policy | Ported, **never called** | `tidb-keyspace/src/lib.rs:170` |
| Connection path | One `factory` and one `users` per `ConcurrentSqlNode` | `tidb-server/src/sql_node.rs:564,667` |

In short, the data path (store, codec, region cache, TSO) and the session
objects already work per keyspace. The process wiring and many globals do not.

## 3. Goals and non-goals

### Goals

1. One nextgen `tidb-server` serves N user keyspaces at once. The keyspace is
   chosen per connection by `<keyspace>.<account>`.
2. Tenant isolation equal to one-process-per-keyspace for:
   - data and users;
   - `SET GLOBAL` and bindings;
   - statistics;
   - observability (processlist, `KILL`, statement summary, slow log, TopSQL,
     `information_schema` cluster tables).
3. Keep Starter's user model unchanged: `mysql.user` stores
   `<keyspace>.<account>`, and `CREATE USER` requires the prefix.
4. **Off by default.** With multi-keyspace mode off, behavior must stay exactly
   as it is today, so Go parity and differential tests are unaffected.
5. Lazy open and idle close of tenant runtimes, so that many mostly idle
   tenants are cheap.

### Non-goals (v1)

- The classic kernel.
- Queries or transactions across tenants.
- TLS/SNI-based routing. A gateway in front may still exist, but it is not
  required.
- Strict per-tenant CPU or memory isolation. v1 uses process-wide limits plus
  per-tenant connection caps. Resource groups and RU come later.
- Moving a live tenant between processes.

## 4. Connection-string contract

| Input user | Result |
| --- | --- |
| `ks.account` with `ks` an ENABLED keyspace that this node may serve | Routed to `ks`. Authenticates as the full name `ks.account` against `ks`'s `mysql.user`. |
| `ks.account` with `ks` unknown, disabled or not allowed | `ER_ACCESS_DENIED_ERROR` (1045). This is the same error as a wrong password, so that keyspace existence is not leaked. |
| `account` without a `.` | Rejected with 1045, unless `default-keyspace` is set. In that case it is routed there, with Go's `GetUsernameVariants` behavior: `root` → `ks.root`. |

Parsing rules:

- Split at the **first** `.`.
- Keyspace names are `[A-Za-z0-9_-]` (`tidb-naming::check_keyspace_name`), so
  they never contain `.`.
- The account part may contain further dots. Go's
  `ValidateUsernameFormat` requires exactly one `.` for created users, and
  that rule is kept for `CREATE USER`.
- The same rule covers `COM_CHANGE_USER`. In v1, changing to a different
  keyspace on an existing connection is rejected; the client must reconnect.

The mapping from prefix to keyspace is the identity function in v1, as in Go.
An opaque alias, like Starter's random 15-character prefix, can be added later
via a `tidb_user_prefix` entry in the PD keyspace `config` map. See open
question Q1.

## 5. Architecture

```
                 ┌──────────────────── tidb-server process ────────────────────┐
 client ──TCP──▶ │ Listener / TLS / conn tracker / process conn-id allocator    │
                 │        │ handshake: user = "ks.account"                       │
                 │        ▼                                                      │
                 │ KeyspaceRegistry ── get_or_open("ks") ──┐                     │
                 │   ks1 → KeyspaceRuntime (Ready)         │ single-flight open  │
                 │   ks2 → KeyspaceRuntime (Ready)         │ idle close          │
                 │   SYSTEM → SystemRuntime (always)       │ PD state watch      │
                 │        │                                                      │
                 │        ▼ (users, factory) of that runtime                     │
                 │ existing auth → open_session → query loop                     │
                 │                                                               │
                 │ ProcessServices: runtimes, MemArbitrator, CPU manager, log,   │
                 │ kernel type, PD cluster id, status HTTP (multiplexed)         │
                 └───────────────────────────────────────────────────────────────┘
```

### 5.1 `ProcessServices` (one per process)

ProcessServices holds what is genuinely process-wide today and stays that way:

- the Tokio and store runtimes;
- the MemArbitrator and the CPU `ResourceManager`;
- logging, JWKS/LDAP and the session-token certificate;
- the TLS manager, the connection tracker and the process-level connection-ID
  allocator (§6.6);
- the kernel type and the PD cluster ID;
- a handle to the SYSTEM keyspace runtime.

### 5.2 `KeyspaceRuntime` (one per open tenant)

This is today's per-node stack, extracted unchanged into a constructor:

```rust
pub struct KeyspaceRuntime {
    keyspace: KeyspaceContext,          // name, id, name bytes for resource tags
    store: Arc<TikvStore>,              // with_keyspace(name)
    etcd: KeyspaceEtcd,                 // /keyspaces/tidb/<id> namespace
    domain: Arc<Domain>,                // catalog, privileges, stats, DDL, plan cache
    users: Arc<ConfiguredUserStore>,    // accounts + GlobalSysvars (+ per-ks vardef, §6.3)
    factory: Arc<dyn QuerySessionFactory>,
    services: RuntimeServices,          // infosync managers, autoid service, GC, TTL, MV,
                                        // binding/stats/privilege/sysvar loops, serverinfo
    state: RuntimeState,                // Opening | Ready | Draining | Closed | Failed
    connections: AtomicUsize,
}

impl KeyspaceRuntime {
    pub async fn open(process: &ProcessServices, keyspace: &str) -> Result<Arc<Self>>;
    pub async fn close(self: Arc<Self>);   // resign owners, stop loops, unregister serverinfo
}
```

The body of `open` is what `cluster_stack::cluster_session_stack`
(`cluster_stack.rs:103`), `new_cluster_domain` (`:748`) and the per-keyspace part
of `cluster_boot.rs` (GC worker, AutoID service, DXF, TTL) do today. Today's
single-keyspace node becomes "a registry with exactly one preloaded runtime and
no routing". That is the main lever for keeping default behavior unchanged.

Each runtime is a full TiDB member of its keyspace. It registers server info,
campaigns for the DDL owner and other owners, runs stats, TTL and GC duties,
and bootstraps the keyspace if needed. See open question Q4 for a later
"serve-only" variant.

### 5.3 `KeyspaceRegistry`

- `get_or_open(name)`: single-flight. Concurrent first connections wait on one
  open, bounded by `keyspace-open-timeout` (default 30s). A failed open is
  cached briefly as `Failed` with backoff.
- Admission:
  - `keyspace-allow-list`, a list of names or `*`;
  - `max-keyspaces`, the cap on open runtimes;
  - the PD keyspace state must be `ENABLED`.
- Idle close: when a runtime has had no connections for `keyspace-idle-timeout`
  (default 10m), it is closed. Owner roles are resigned first, so another node
  takes over. `preload-keyspaces` are never idle-closed.
- PD watch: a keyspace that becomes `DISABLED` or `ARCHIVED` is drained:
  - new connections are refused;
  - existing connections get a configurable grace period, then are killed.
- The SYSTEM runtime is always open. Go requires SYSTEM to be bootstrapped
  before any user keyspace (`session.go:4318`), and user runtimes reach it
  through the existing cross-keyspace path.

### 5.4 Configuration

```toml
[multi-keyspace]
enable = false                # off: exactly today's behavior; keyspace-name required
allow-list = ["*"]            # or explicit names
preload = ["keyspace1"]
default-keyspace = ""         # route prefix-less users here; empty = reject
max-keyspaces = 64
idle-timeout = "10m"
open-timeout = "30s"
default-authentication-plugin = "mysql_native_password"  # greeting before tenant is known (§6.7)
```

With `enable = true`, `keyspace-name` must be empty. The nextgen kernel check
in `standby_node::check_kernel_config` is relaxed to accept "multi-keyspace
enabled" in place of "keyspace name or standby mode". Standby mode and
multi-keyspace mode are mutually exclusive.

## 6. Removing single-keyspace globals

This is the bulk of the work. Each row names the current global and its
replacement. Line numbers are on `73a0893eff`.

### 6.1 Keyspace identity reads

| Today | Replacement |
| --- | --- |
| `get_global_keyspace_name()` / `get_keyspace_name_by_settings()`, read in about 20 places: slow log and statement summary (`exec_stmt_observation.rs:428,727`), IMPORT INTO and extstore, DDL ingest, serverinfo, standby, HTTP status | A `KeyspaceContext` reachable from the session (`SessionContext` → domain) and from each runtime's background loops. In multi-keyspace mode the global stays **empty**. A debug assertion catches any remaining reader that returns empty while multi-keyspace mode is on. |
| `KEYSPACE_NAME_BYTES: OnceLock` (`tidb-keyspace/src/lib.rs:94`), built into resource-group tags sent to TiKV (`observation.rs:579`, `prefetch.rs:137`, `job_ctx.rs:120`) | A field on `KeyspaceContext`; the taggers take it from the session or job. |
| `get_username_policy()`, unused | A `UsernamePolicy` built per runtime (`prefix = keyspace name`). It is called by `CREATE`/`ALTER`/`GRANT USER` and by `match_identity`, as Go does in Starter mode. |
| Process-wide log field `keyspaceName` (`lib.rs:225`) | Dropped in multi-keyspace mode. Per-connection and per-runtime spans add `keyspaceName` instead. |

### 6.2 PD/infosync singletons

The following are `Mutex<Option<Arc<..>>>` statics set from one store in
`new_cluster_domain`:

- `resource_manager_client.rs:147`
- `label_manager.rs:114`
- `placement_manager.rs:154`
- `schedule_manager.rs:91`
- `tiflash_manager.rs:455`
- `region.rs:83` (`PD_HTTP_CLI`)
- `prometheus_addr.rs:86`
- `tidb-domain-affinity/src/lib.rs:427`
- `tidb-variable/src/domain_slots.rs:31,32,75`

They move into `RuntimeServices` on the runtime, and the global accessors take
`&Domain`. Some of them are cluster-level HTTP clients that carry no keyspace,
for example region and Prometheus. Those may stay shared once each call site has
been checked for keyspace-scoped requests. Resource groups, placement and label
rules are keyspace-scoped and must be per runtime.

### 6.3 `SET GLOBAL` and process atomics

This is the largest semantic issue. `GlobalSysvars` is already per instance,
but its SetGlobal hooks write **83 process atomics** in `tidb-vardef/src/lib.rs`
(for example `ENABLE_FOREIGN_KEY` at :190 and `INSTANCE_PLAN_CACHE_*`). So one
tenant's `SET GLOBAL` would change behavior for every tenant.

The plan:

1. Classify all 83 atomics into an appendix generated from the source:
   - **instance-scoped**, truly per process (for example instance plan-cache
     memory limits or log settings). These stay process-wide. In
     multi-keyspace mode, setting them requires a process-level administrator,
     not a tenant `root`. See Q6.
   - **global-scoped**, where Go semantics are "for this cluster". These move
     into a per-runtime `VardefSnapshot` owned by `GlobalSysvars`. Readers take
     it from the session's domain instead of the static.
2. The change is mechanical but wide. Reader call sites switch from
   `vardef::X.load()` to `session.vardef().x()` (or `domain.vardef()` for
   background loops). In single-keyspace mode both paths read the same
   snapshot, so behavior is unchanged.

### 6.4 Caches keyed by table or job ID

Table and job IDs are per keyspace, so these caches collide across tenants:

- `TABLE_ROW_STATS_CACHE` (`stats_table_row_cache.rs:61`)
- the stats async-load and sync-load queues (`tidb-stats-asyncload/src/lib.rs:26`,
  `sync_load.rs:75`)
- the auto-analyze process ID generator (`auto_analyze_proc_id_generator.rs:101`)
- `JOB_CTX_MAP` (`tidb-ddl/src/job_ctx.rs:129`)

Each moves into the runtime's `Domain`, or is re-keyed by `(keyspace_id, id)`.
Moving it into the `Domain` is preferred, because it closes with the tenant.

### 6.5 Transactions and GC

- `GLOBAL_INNER_TXN_START_TS` and `ACTIVE_START_TS` (`tidb-txnkv/src/inner_txn.rs:28,153`)
  feed the min-start-ts report used for GC safe points. TSO can differ per
  keyspace group, so these become a map keyed by keyspace ID, and each
  runtime's reporter reads only its own entry.
- `DEFAULT_RESOURCE_GROUP` (`tidb-meta/src/transaction.rs:59`) moves per runtime.
- The GC worker already takes the domain and store (`tidb-gcworker/src/node.rs:49`),
  so it runs once per runtime.

### 6.6 Connection IDs, server IDs and `KILL`

The connection ID is allocated before the handshake, from one keyspace's
`ServerIdAuthority` (global kill embeds the server ID). The replacement:

- The process takes **one server ID from the SYSTEM keyspace's etcd** and
  allocates every connection ID from it, so IDs are unique within the process
  for all tenants.
- Each runtime's serverinfo record carries that same server ID, so global kill
  routing still finds the process.
- `KILL <id>` and `SHOW PROCESSLIST` resolve only connections of the caller's
  keyspace. A connection ID from another tenant behaves exactly like a
  nonexistent one (`ER_NO_SUCH_THREAD`).

### 6.7 Decisions taken before the tenant is known

- **Greeting auth plugin.** Today it comes from the keyspace sysvar
  `default_authentication_plugin`. In multi-keyspace mode it comes from
  process config. If the tenant's account uses another plugin, the existing
  AuthSwitch path handles it.
- **`require_secure_transport`.** It is checked after routing, against the
  tenant's sysvar. `admit_transport` already reads it from `users`, which is now
  the tenant's.
- **`max_connections`.** A process-wide cap plus the tenant's own
  `max_connections`, both enforced. Today only the process cap is checked
  (`mysql_connection.rs:1634`).

### 6.8 Observability isolation

| Surface | Today | Change |
| --- | --- | --- |
| Statement summary v1 and v2 | process global (`statement_summary.rs:46`, `v2/stmtsummary.rs:138`) | One instance per runtime |
| Top-N slow query, deadlock history, txn info | process global | Per runtime |
| Slow log file | one file | Each entry already carries `keyspace_name` from the session (§6.1). `information_schema.slow_query` filters by the caller's keyspace. |
| TopSQL reporter | binds one keyspace name (`tidb-topsql/src/lib.rs:68`) | Reports carry the keyspace per record. Before merging, check that the reporter protocol supports this; otherwise use one reporter per runtime. |
| Metrics | the `keyspace_name` const label is fixed at registration | A variable `keyspace` label on per-tenant series, bounded by `max-keyspaces` |
| `CLUSTER_*` tables, `SHOW PROCESSLIST`, `KILL` | | Filtered to the caller's keyspace |

### 6.9 Status port and AutoID service

- The status HTTP server is wired to one factory (`cluster_boot.rs:192-249`).
  Per-tenant endpoints take `?keyspace=` or a `/ks/<name>/…` prefix. Process
  endpoints (`/status`, `/metrics`) stay global.
- The AutoID `Service` is bound to one keyspace ID and rejects others
  (`tidb-autoid-service/src/lib.rs:491`). It becomes a map from keyspace ID to
  service, populated by runtime open and close.

### 6.10 Other per-store globals

The following become per runtime:

- `BRIE_NODE` (`tidb-executor/src/brie.rs:675`)
- the IMPORT INTO / DXF import globals (`tidb-dxf-importinto/src/lib.rs:107,141`)
- `tidb-extworkload/src/lib.rs:173`
- `tidb-planner-extstore/src/lib.rs:33`

## 7. Isolation and security

- **Data.** The API v2 keyspace prefix in each runtime's store codec is the same
  isolation Starter relies on. Each tenant's session can only reach its own
  store.
- **Authentication.** It is resolved against the tenant's own `mysql.user`.
  Prefix routing means `keyspace1.root` can never authenticate against
  `keyspace2`.
- **Error shape.** Routing failures look exactly like failed authentication
  (§4).
- **Information leakage.** §6.6 and §6.8 are release blockers. Multi-keyspace
  mode must not be documented as tenant-isolated until the isolation test suite
  (§9) passes.
- **Noisy neighbors.** v1 caps connections per tenant and keeps process-wide
  memory and CPU limits. Per-tenant memory quotas and resource-group RU
  enforcement are follow-ups (§10, Phase 5).

## 8. Cost per open tenant

Each runtime has its own:

- PD gRPC connection and TSO stream;
- region cache;
- TiKV channels;
- etcd client;
- schema-lease, privilege, sysvar, binding and stats loops;
- owner campaigns, GC, TTL and MV loops.

The cross-keyspace "lite domain" shows that the floor is modest. A full
runtime costs more. Phase 1 measures resident memory, threads, file
descriptors and idle CPU for N = 1, 10 and 50 tenants on the local TiDB X
playground. Later mitigations, only if the measurements call for them:

- share TiKV connections across stores, keyed by address;
- share the PD client, so only TSO streams are per keyspace group;
- lengthen idle loop intervals for runtimes with no sessions.

## 9. Testing

- **Unit.** Username parsing and routing errors, registry single-flight, open
  failure backoff, idle close, drain on PD state change.
- **Default-mode regression.** With multi-keyspace mode off, the existing gates
  run unchanged and must stay green:
  - `cargo nextest` for the touched crates;
  - the `run-realtikv-*` runners;
  - `integration_corpus_matches_recordings`.
- **Multi-tenant integration**, on the local TiDB X playground (CSE + RustFS,
  already working on the Singapore Mac), with SYSTEM, `keyspace1` and
  `keyspace2` created through the PD keyspace API:
  - data isolation: same database and table names in both tenants, different
    contents;
  - auth isolation: `keyspace1.root` cannot log into `keyspace2`, with the
    1045 error shape;
  - `SET GLOBAL` isolation for a sample from each class in §6.3;
  - concurrent DDL in both tenants, with each tenant's DDL owner on this node;
  - `KILL` and processlist across tenants;
  - statement summary and slow query filtering;
  - idle close and reopen;
  - keyspace disabled while connected;
  - a restart with preload.
- **Mixed fleet.** A multi-keyspace node and a classic single-keyspace node
  serve the same keyspace at the same time: DDL ownership handoff and schema
  version sync must interoperate.

## 10. Plan

Each phase is shippable. Phases 3a–3f are independent of each other and can run
in parallel once Phase 1 lands.

| Phase | Deliverable | Gate |
| --- | --- | --- |
| **0** | This design; decisions on §11 | Owner sign-off |
| **1. Extract the runtime** | `ProcessServices` + `KeyspaceRuntime::open/close` + `KeyspaceRegistry` with one preloaded runtime. Pure refactor; no routing, no behavior change | All default-mode gates green; baseline footprint numbers for N=1 |
| **2. Routing MVP** (behind `multi-keyspace.enable`) | Handshake split and registry lookup; process-level connection and server IDs (§6.6); greeting plugin from config; allow-list, preload, max-keyspaces; open timeout | Two tenants on one node with data and auth isolation passing. Known leaks listed in release notes; flag documented as experimental |
| **3a** | Keyspace identity reads + resource tags + username policy (§6.1) | Unit and integration tests; no empty-keyspace assertions firing |
| **3b** | PD/infosync singletons into `RuntimeServices` (§6.2) | Resource-group, placement and label-rule SQL per tenant |
| **3c** | `SET GLOBAL` / vardef split (§6.3), with the generated classification appendix | `SET GLOBAL` isolation tests for every global-scoped atomic |
| **3d** | ID-keyed caches into the runtime (§6.4) and transaction/GC maps (§6.5) | Stats and DDL concurrency in two tenants; GC safe point per tenant |
| **3e** | Observability isolation (§6.8) | Processlist, KILL, statement summary and slow-query filtering tests |
| **3f** | Status port multiplexing + AutoID service map + per-store globals (§6.9, §6.10) | AUTO_ID_CACHE=1 tables in two tenants; IMPORT INTO per tenant |
| **4. Lifecycle** | Idle close, PD state watch and drain, per-tenant `max_connections`, optional prefix alias (Q1) | Lifecycle tests; leak check that repeated open/close does not grow memory or file descriptors |
| **5. Hardening** | Full isolation suite; footprint benchmark N=1/10/50; per-tenant memory quota; docs | The isolation suite is the bar for calling the mode tenant-isolated |

Phase 2 may be developed alongside Phase 3. The flag stays experimental until
3a, 3c and 3e are done, because those are the cross-tenant correctness and
leakage items.

## 11. Open questions

- **Q1. Prefix = keyspace name?** Go and Starter-on-TiDB use the keyspace name.
  The public Starter service shows an opaque 15-character prefix.
  *Recommendation:* the keyspace name in v1. Later add an optional alias from
  PD keyspace config (`tidb_user_prefix`), resolved through a cached PD lookup.
- **Q2. Users without a prefix.** *Recommendation:* reject by default, with
  optional `default-keyspace`.
- **Q3. Which keyspaces may a node serve?** *Recommendation:* an explicit
  allow-list, with `*` permitted for development.
- **Q4. Full member or serve-only?** Should every runtime take owner duties
  (DDL owner, GC, TTL, auto-analyze) for its tenant? *Recommendation:* full
  member in v1, because it matches a normal TiDB node and needs no new
  coordination. A serve-only runtime, with owners on dedicated nodes, is a
  later cost optimization.
- **Q5. Where does this live?** The Rust workspace's rule is that Go `pkg/` is
  authoritative, and Go has no in-process multi-tenancy. Is this a fork-only
  experiment (`siddontang/tidb`), or should it be proposed to
  `ngaut/tidb@hparser-integration` as a deliberate, flag-gated extension
  beyond Go parity?
- **Q6. Instance-scoped variables.** In multi-keyspace mode, who may set
  process-wide variables? *Recommendation:* only the SYSTEM keyspace's `root`,
  or process config. Tenant `root` gets `ER_SPECIFIC_ACCESS_DENIED_ERROR`.
