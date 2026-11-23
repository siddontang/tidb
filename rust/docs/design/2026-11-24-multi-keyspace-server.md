# Multi-keyspace tidb-server: one process, many tenants

- Status: **Draft for review, revision 2**: addresses the review of `42b666f`. No code has been written yet.
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
  - an external gateway sits in front. Go only sees the gateway through
    connection attributes (`TiDB-Gateway-ConnID`,
    `GATEWAY_SECURECONN_ATTR_KEY`) and the PROXY protocol.
- **Inference, not documented:** the public docs establish only the prefixed
  username and the verified-TLS requirement. They do not say how the hosted
  gateway routes connections. That it routes on the prefix is inferred from
  the Essential docs, where an exclusive endpoint removes the need for the
  prefix.

This design does that routing step inside the SQL node itself.

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
| `ks.account` with `ks` an ENABLED, **provisioned** (§4.1) keyspace that this node may serve | Routed to `ks`. Authenticates as the full name `ks.account` against `ks`'s `mysql.user`. There is no fallback to the unprefixed `account`. |
| `ks.account` with `ks` unknown, disabled, unprovisioned, not allowed, or `SYSTEM` | `ER_ACCESS_DENIED_ERROR` (1045). This is the same error as a wrong password, so that keyspace existence is not leaked. `SYSTEM` is never routable on the shared listener (§5.5). |
| `account` without a `.` | Rejected with 1045, unless `default-keyspace` is set. In that case it is routed there, with Go's `GetUsernameVariants` behavior: `root` → `ks.root`. |

Parsing and validation rules, matching Go's two separate checks:

- Routing splits at the **first** `.`. Keyspace names are `[A-Za-z0-9_-]`
  (`tidb-naming::check_keyspace_name`), so they never contain `.`.
- **`CREATE`/`ALTER`/`GRANT USER`** apply Go's `ValidateUsername`: the name
  must start with `<the session's keyspace>.` (`pkg/executor/simple.go:1181`).
  Further dots in the account part are allowed there.
- **Identity lookup** applies Go's one-dot predicate `ValidateUsernameFormat`
  through `checkUserVariantMismatch` (`pkg/server/conn.go:898`). A name of the
  `x.y` shape whose prefix is not the session's keyspace fails with
  `ErrUserPrefixMismatch`. In multi-keyspace mode, routing has already chosen
  the keyspace from the prefix, so this check passes for every routed
  connection, and it is kept for parity.
- `COM_CHANGE_USER` uses the same routing. In v1, changing to a different
  keyspace on an existing connection is rejected; the client must reconnect.

### 4.1 Accounts, first login and migration

Bootstrap creates a plain, **passwordless** `root`, in Go and in Rust
(`rust/crates/tidb-session/src/bootstrap.rs:232`). Lookup only ever uses
`ks.root`, so a freshly bootstrapped keyspace has no account that can log in
through the shared listener. That is deliberate: a passwordless account must
never be reachable there.

- **Provisioning is a separate operator step**, and a keyspace is admitted
  only once it is done. It extends Go's `--keyspace-activate` mode, which
  bootstraps a keyspace and exits:

  ```sh
  tidb-server --keyspace-activate --keyspace-name ks1 --path <pd> \
      --initial-admin-password-file /secure/ks1-root.pw   # new flag
  ```

  This bootstraps `ks1` if needed. In one transaction it then creates
  `'ks1.root'@'%'` with that password, `ALL PRIVILEGES ... WITH GRANT OPTION`,
  and writes a `tidb_multi_keyspace_provisioned` row to `mysql.tidb`. The same
  operation is available on the operator listener (§5.5).
- **At runtime open**, the registry refuses (1045) a keyspace that has no
  provisioned row.
- **Existing keyspaces** whose accounts are unprefixed (`app`) are migrated by
  the same command with `--migrate-accounts`. It issues
  `RENAME USER 'app'@'h' TO 'ks1.app'@'h'` for every non-internal account.
  `RENAME USER` carries table, column and role grants, and the bootstrap
  `root` is renamed only if it has a password.
- **Rolling upgrade.** Switch the keyspace's existing single-keyspace nodes to
  `deploy-mode = starter` first, so they apply Go's prefix policy. Then migrate
  the accounts. Then add multi-keyspace nodes. Starter-mode single-keyspace
  nodes accept both `app` (via `GetUsernameVariants`, `app` → `ks1.app`) and
  `ks1.app`, so clients keep working throughout.
- **Rollback.** Remove the multi-keyspace nodes. The prefixed accounts keep
  working on Starter-mode single-keyspace nodes. Undoing the rename is optional
  and uses the same tool in reverse.
- **Test.** On a fresh keyspace:
  - provision it, then log in as `ks1.root` with the provisioned password;
  - `CREATE USER 'ks1.app'` and log in as it;
  - check that `CREATE USER 'app'` and `CREATE USER 'ks2.app'` both fail
    with Go's error;
  - check that `ks1.root` cannot log in before provisioning;
  - on an existing keyspace, run `--migrate-accounts` and check that its grants
    survive.

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
  open, bounded by `open-timeout` (default 30s). A failed open is cached briefly
  as `Failed` with backoff.
- Admission:
  - `allow-list`, a list of names. `*` is accepted only with `dev-mode = true`,
    and it never includes `SYSTEM`;
  - `max-keyspaces` caps **Opening + Ready + Draining** runtimes together, so
    concurrent opens count. A connection that would exceed the cap gets
    `ER_CON_COUNT_ERROR` (1040) after routing;
  - the PD keyspace state must be `ENABLED`, and the keyspace must be
    provisioned (§4.1).
- The SYSTEM runtime is always open but **internal**. User runtimes reach it
  through the existing cross-keyspace path, and operators reach it through the
  operator listener (§5.5). Go requires SYSTEM to be bootstrapped before any
  user keyspace (`session.go:4318`).

**What keeps a runtime alive.** A runtime holds a **pin count**. Every pin is
taken and released under the registry lock.

| Pin | Taken | Released |
| --- | --- | --- |
| Connection | When routing resolves the keyspace, **before** authentication and before waiting on an Opening runtime | On connection close, including a failed authentication |
| Background job | When this node becomes owner of, or starts executing, tenant work: DDL jobs in the tenant's queue, DXF/IMPORT subtasks, TTL jobs, a running auto-analyze, an MV refresh | When the job finishes or ownership moves away |
| Preload | At startup, for `preload` keyspaces | Never |

**Idle close.** A Ready runtime with zero pins for `idle-timeout` moves to
Draining, under the same lock that `get_or_open` uses to add a pin, so a
reconnect can never race with eviction:

- a connection that arrives once Draining has started waits for Closed and
  then triggers a fresh open;
- before closing, the runtime checks the tenant's own queues (`mysql.tidb_ddl_job`,
  DXF task tables, TTL jobs). If any work is unfinished and this node may be
  the **last serving node**, the close is cancelled and the runtime returns to
  Ready. "Last serving node" means no other live server-info record in the
  tenant's namespace.

Closing resigns owner roles, so another node can take over, then stops the
loops and unregisters server info.

**PD disable or archive.** A keyspace that becomes `DISABLED` or `ARCHIVED` is
drained:

- new connections are refused;
- existing connections are killed after `drain-grace` (default 30s);
- unfinished jobs are abandoned exactly as when a node stops, and the next
  owner resumes them once the keyspace is enabled again.

**Tests.**

- A reconnect during eviction, with concurrent open and close.
- A DDL job submitted by a client that then disconnects, with only one serving
  node. The runtime must stay open until the job is done.
- A burst of first connections to more keyspaces than `max-keyspaces`.

### 5.4 Configuration

```toml
[multi-keyspace]
enable = false                # off: exactly today's behavior; keyspace-name required
allow-list = ["keyspace1", "keyspace2"]  # "*" only with dev-mode; never SYSTEM
preload = ["keyspace1"]
default-keyspace = ""         # route prefix-less users here; empty = reject
max-keyspaces = 64            # Opening + Ready + Draining
idle-timeout = "10m"
open-timeout = "30s"
drain-grace = "30s"
default-authentication-plugin = "caching_sha2_password"  # greeting before tenant is known (§6.7)
dev-mode = false              # allows "*", plaintext; never for shared deployments
operator-addr = "127.0.0.1:4100"  # §5.5; mTLS required unless loopback + dev-mode
```

With `enable = true`, the shared SQL listener **requires TLS before the
handshake response**: the `[security]` certificate must be configured, and a
plaintext client is rejected. This is a process-level rule, because the
password exchange happens before the tenant, and so its
`require_secure_transport`, is known. Clients connect with verified TLS:

```sh
mysql -h tidb.example.com -P 4000 -u 'keyspace1.root' -p \
      --ssl-mode=VERIFY_IDENTITY --ssl-ca=/path/to/ca.pem
```

With `enable = true`, `keyspace-name` must be empty. The nextgen kernel check
in `standby_node::check_kernel_config` is relaxed to accept "multi-keyspace
enabled" in place of "keyspace name or standby mode". Standby mode and
multi-keyspace mode are mutually exclusive.

### 5.5 Operator path: SYSTEM, process administration and status APIs

Tenants and operators get separate doors:

- **Shared SQL listener (tenants).** Only allow-listed user keyspaces are
  routable. `SYSTEM.x` is rejected with 1045. A tenant's `root` can change only
  its own keyspace's state.
- **Operator listener** (`operator-addr`). It requires mTLS with an operator
  client certificate (CA from `[security] operator-ca`); plaintext is allowed
  only on loopback with `dev-mode`. It serves:
  - SQL to SYSTEM;
  - process-wide administration: instance-scoped variables (§6.3),
    provisioning and account migration (§4.1), forced drain or close of a
    runtime;
  - every tenant-scoped HTTP status endpoint (§6.9), selected by
    `/ks/<name>/…`.

  The operator certificate is the authority, so the path selector is honored
  only there.
- **The public status port** keeps only process endpoints that expose no tenant
  data: `/status` and the health checks. `/metrics` carries per-tenant labels,
  so it moves to the operator listener in multi-keyspace mode.
- There is **no tenant self-service HTTP API in v1**. If one is added later,
  the keyspace is derived from the authenticated tenant identity, never from a
  path or query parameter.

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
     multi-keyspace mode they can be set only through process config or the
     operator listener (§5.5). A tenant `root` gets
     `ER_SPECIFIC_ACCESS_DENIED_ERROR`.
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

With global kill, a connection ID embeds the node's server ID.
`ServerIdAuthority` leases that server ID in the **tenant's** namespaced etcd
(`cluster_stack.rs:831`). Remote `KILL` finds the owning node by looking up the
server ID among the tenant's server-info records.

The connection ID must be fixed **before** the tenant is known: it is in the
handshake greeting, and drivers such as Connector/J use it for
`KILL QUERY`. So a multi-keyspace process needs one server ID that is valid
in every tenant it serves. A server ID leased only in SYSTEM is not enough,
because a single-keyspace nextgen node in that tenant may already lease the
same ID. Publishing it there would make remote `KILL` ambiguous.

**Rule: a node publishes a server ID in a tenant only after reserving it in
that tenant's own namespace.**

1. At startup the multi-keyspace process picks a candidate server ID and
   reserves it **in SYSTEM's namespace**, with the existing lease and CAS. This
   keeps IDs unique among multi-keyspace nodes.
2. At each runtime open, the process reserves the **same ID in the tenant's
   namespace**, using the exact key, lease and CAS that single-keyspace nodes
   use today. Only after the reservation succeeds does it register server info
   and start admitting connections for that tenant. In the tenant's namespace,
   the ID is now owned by exactly one node, whatever its kind.
3. **Collision.** If the ID is already leased in the tenant's namespace (by a
   single-keyspace node, or by a multi-keyspace node whose SYSTEM lease
   expired), the open fails. The runtime goes to `Failed` with backoff, logs a
   warning with both node identities, and that tenant's connections get 1045
   on this node. Other nodes keep serving it. The process does not switch IDs,
   because connections already greeted would keep the old one.
4. **Lease renewal** runs for every reservation. Losing the tenant's lease
   drains only that runtime, mirroring Go's lost-server-ID handling for that
   tenant. Losing the SYSTEM lease stops new handshakes on the whole process
   until it re-acquires a fresh candidate ID. Runtimes whose tenant leases are
   still held keep serving their existing connections.
5. **ID space.** With `enable-32bits-connection-id`, the server ID has 11 bits,
   so at most 2,047 IDs. Multi-keyspace mode requires 64-bit connection IDs, so
   that reserving one ID across many namespaces stays easy.

`KILL <id>` and `SHOW PROCESSLIST` resolve only connections of the caller's
keyspace. A connection ID from another tenant behaves exactly like a
nonexistent one (`ER_NO_SUCH_THREAD`).

**Mixed-fleet test.** One multi-keyspace node and one single-keyspace
**nextgen** node serve the same tenant, with a failpoint that forces
overlapping candidate IDs. Check:

- the second node to open the tenant fails with the collision error, and never
  publishes the ID;
- remote `KILL` and `KILL QUERY` work in both directions;
- lease expiry and renewal on both nodes, including a tenant-lease loss on the
  multi-keyspace node that drains only that tenant.

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
  In multi-keyspace mode, tenant-scoped endpoints are served only on the
  operator listener under `/ks/<name>/…`. Examples are schema, regions, MVCC,
  DDL history and the stats dump. See §5.5 for who may call them.
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
- **Transport.** The shared listener requires TLS before credentials are sent
  (§5.4). Operators use a separate mTLS listener (§5.5). SYSTEM, process-wide
  settings and tenant status APIs are never reachable with a tenant identity.
- **No passwordless entry.** Only provisioned prefixed accounts can log in
  (§4.1).
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
  - data isolation: same database and table names in both tenants, with
    **overlapping table IDs** and different contents;
  - concurrent AutoID allocations in both tenants, including
    `AUTO_ID_CACHE=1` tables once Phase 3f lands;
  - GC safety: a long transaction in `keyspace1` while GC runs in `keyspace2`
    and in `keyspace1`, with every row the long transaction reads still intact;
  - provisioning and first login (§4.1);
  - routing and the operator path: TLS required, `SYSTEM.root` rejected on the
    shared listener, operator listener mTLS;
  - auth isolation: `keyspace1.root` cannot log into `keyspace2`, with the
    1045 error shape;
  - `SET GLOBAL` isolation for a sample from each class in §6.3;
  - concurrent DDL in both tenants, with each tenant's DDL owner on this node;
  - `KILL` and processlist across tenants;
  - statement summary and slow query filtering;
  - idle close and reopen;
  - keyspace disabled while connected;
  - a restart with preload.
- **Mixed fleet.** A multi-keyspace node and a single-keyspace **nextgen** node
  serve the same keyspace at the same time. Check:
  - DDL ownership handoff and schema version sync;
  - server-ID reservation and remote `KILL` (§6.6);
  - registry lifecycle (§5.3).

## 10. Plan

Each phase is shippable. Phases 3a–3f are independent of each other and can run
in parallel once Phase 1 lands.

| Phase | Deliverable | Gate |
| --- | --- | --- |
| **0** | This design; decisions on §11 | Owner sign-off |
| **1. Extract the runtime** | `ProcessServices` + `KeyspaceRuntime::open/close` + `KeyspaceRegistry` with one preloaded runtime. Pure refactor; no routing, no behavior change | All default-mode gates green; baseline footprint numbers for N=1 |
| **2. Controlled demo** (behind `multi-keyspace.enable` **and** `dev-mode`) | Handshake split and registry lookup; TLS rule and operator listener (§5.4, §5.5); provisioning (§4.1); server-ID reservation (§6.6); pin-count lifecycle without idle close (§5.3); greeting plugin from config; allow-list, preload, max-keyspaces. **Pulled forward from 3d:** per-keyspace min-start-ts / GC tracking (§6.5), because GC is a data-loss risk even in a demo. **Unsupported operations are rejected** with `ER_NOT_SUPPORTED_YET` until their phase lands: placement and resource-group DDL (3b), `AUTO_ID_CACHE=1` tables (3f), IMPORT INTO / BR / external storage (3f), tenant HTTP status APIs | Two tenants on one node: data, auth, GC-safety and AutoID tests (§9) pass; every rejected operation has a test asserting the rejection. **Not for shared deployment.** |
| **3a** | Keyspace identity reads + resource tags + username policy (§6.1) | Unit and integration tests; no empty-keyspace assertions firing |
| **3b** | PD/infosync singletons into `RuntimeServices` (§6.2) | Resource-group, placement and label-rule SQL per tenant |
| **3c** | `SET GLOBAL` / vardef split (§6.3), with the generated classification appendix | `SET GLOBAL` isolation tests for every global-scoped atomic |
| **3d** | ID-keyed caches into the runtime (§6.4) and transaction/GC maps (§6.5) | Stats and DDL concurrency in two tenants; GC safe point per tenant |
| **3e** | Observability isolation (§6.8) | Processlist, KILL, statement summary and slow-query filtering tests |
| **3f** | Status port multiplexing + AutoID service map + per-store globals (§6.9, §6.10) | AUTO_ID_CACHE=1 tables in two tenants; IMPORT INTO per tenant |
| **4. Lifecycle** | Idle close, PD state watch and drain, per-tenant `max_connections`, optional prefix alias (Q1) | Lifecycle tests; leak check that repeated open/close does not grow memory or file descriptors |
| **5. Hardening** | Full isolation suite; footprint benchmark N=1/10/50; per-tenant memory quota; docs | The isolation suite is the bar for calling the mode tenant-isolated |

Phase 2 may be developed alongside Phase 3.

**Gate for the first shared deployment**, meaning `dev-mode` off with real
tenants:

- **all** of 3a–3f and Phase 4 are done;
- every supported operation uses the correct tenant's service: GC tracking,
  placement and label clients, AutoID dispatch, resource tags,
  `SET GLOBAL`, observability;
- every operation without a tenant-correct implementation is rejected, not
  approximated;
- the §9 isolation suite, including the mixed-fleet test, passes.

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
- **Q6. Operator identity.** *Resolved in review:* the operator listener uses
  mTLS operator certificates (§5.5). SYSTEM is internal, and no SQL identity
  on the shared listener has process-wide powers. *Remaining question:* should
  operator certificates map to SQL users inside SYSTEM for auditing, or is the
  certificate subject enough?
- **Q7. Provisioning owner.** Is `tidb-server --keyspace-activate
  --initial-admin-password-file` the right provisioning path, or is there an
  existing control-plane component that should own it?
