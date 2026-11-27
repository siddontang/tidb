# Multi-keyspace tidb-server: one process, many tenants

- Status: **Draft for review, revision 8**: tenant hibernation (§5.8), revised after review; session migration through the Rust TiProxy (§5.6, §5.7); addresses the reviews of `42b666f`, `7cbdef8ad3` and `08d9476bfc`. Implementation in progress on `multi-keyspace`; §5.9 records the per-tenant data model of the code.
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

- **One admission tool, two modes.** Both modes extend Go's
  `--keyspace-activate` mode, which bootstraps a keyspace and exits. The same
  operations are available on the operator listener (§5.5). A keyspace is
  admitted only after the tool has written a `tidb_multi_keyspace_provisioned`
  row to `mysql.tidb`. At runtime open, the registry refuses (1045) a keyspace
  without that row, or with a migration still in progress.

  ```sh
  # Fresh keyspace: no user accounts besides the bootstrap root.
  tidb-server --keyspace-activate --keyspace-name ks1 --path <pd> \
      --initial-admin-password-file /secure/ks1-root.pw
  # Existing keyspace with unprefixed accounts.
  tidb-server --keyspace-activate --keyspace-name ks1 --path <pd> --migrate-accounts
  ```

- **Provision mode (fresh keyspace).** It bootstraps `ks1` if needed and
  creates `'ks1.root'@'%'` with the given password and
  `ALL PRIVILEGES ON *.* WITH GRANT OPTION`. It refuses to run if any
  non-internal account other than a passwordless bootstrap `root` exists;
  those keyspaces must use migrate mode.

- **Migrate mode (existing keyspace).** This is the only path for a keyspace
  that already has accounts, so an existing administrator has exactly one
  route:
  1. **Plan.** Compute the rename set: every non-internal `'u'@'h'` becomes
     `'ks1.u'@'h'`. For `root`:
     - a **password-protected** `'root'@'h'` is renamed and keeps its own
       credential and grants. `--initial-admin-password-file` is rejected in
       this mode, so there is never a second, competing `ks1.root`;
     - a **passwordless** `root` is not renamed. If it is the only
       administrator, the tool stops and asks the operator to give it a
       password first, then rerun.
  2. **Preflight, before any write.** Every destination name must be free. If
     any `'ks1.u'@'h'` already exists, the tool aborts with the full collision
     list and changes nothing. It never merges accounts.
  3. **Record the plan.** Store the plan as a
     `tidb_multi_keyspace_migration` row in `mysql.tidb` with state
     `in_progress`.
  4. **Rename** each account with `RENAME USER`. That statement carries the
     credential and table, column and role grants.
     - **Account changes are frozen** for the duration. While the migration
       row is `in_progress`, every serving node of the keyspace rejects
       `CREATE`/`ALTER`/`DROP`/`RENAME USER`, `GRANT`/`REVOKE` and
       `SET PASSWORD` with a clear error. Nodes read the row through the
       privilege-reload path they already run, and the tool waits until every
       live node reports the new privilege version before the first rename.
       Without the Phase 1b release, nodes cannot enforce the freeze, so the
       tool requires a coordinated cutover (no other account writers) and
       says so.
     - **Completion is recorded, not inferred.** After each rename, in the
       same transaction where possible and otherwise immediately after, the
       tool marks that entry `done` in the stored plan. On retry, an entry not
       marked `done` whose source is gone and whose destination exists is
       **not** assumed to be the migration's work. The tool stops and reports
       it for the operator to resolve.
  5. **Finish.** Only after every rename succeeds, set the migration row to
     `done` and write the provisioned marker.

  A rerun after an interruption resumes from the stored plan.

- **Rolling migration needs a compatibility release first.** Go's handshake
  maps a plain `app` to `ks1.app` in Starter mode
  (`matchIdentityWithVariants`, `pkg/server/conn.go:910`), and its user DDL
  enforces the prefix. Rust has the policy ported, but
  `get_username_variants`, `validate_username` and `get_username_policy` have
  **no production callers** (`rust/crates/tidb-keyspace/src/lib.rs:214`). So
  switching today's Rust nodes to `deploy-mode = starter` would not keep `app`
  logins working. The order is:
  1. **Compatibility release (Phase 1b).** Port Go's Starter username handling
     into the Rust handshake: `matchIdentityWithVariants` and
     `checkUserVariantMismatch`. Also port it into `CREATE`/`ALTER`/`GRANT
     USER`. This is Go parity work and has no multi-keyspace dependency.
     Deploy it to every single-keyspace node of the keyspace, with
     `deploy-mode = starter`.
  2. Run migrate mode. Clients using `app` and `ks1.app` both keep working.
  3. Add multi-keyspace nodes. These accept only `ks1.app`.
     - **Acceptance criterion:** a shared node joins a client endpoint only
       after every client of that endpoint connects as `ks1.app`. Otherwise a
       connection pool using `app` succeeds on one backend and gets 1045 on
       the next. Legacy clients stay on an endpoint backed only by
       single-keyspace nodes until they switch. Verify this from the
       single-keyspace nodes' login records: no successful `app`-form logins
       on that endpoint over a full client reconnect window.
- **Coordinated cutover**, for keyspaces whose nodes cannot take the
  compatibility release: stop client traffic, migrate, and switch every client
  to `ks1.app` before reopening. This is documented, but it is not the default
  path.
- **Rollback.** Remove the multi-keyspace nodes. Single-keyspace nodes on the
  compatibility release keep accepting both `app` and `ks1.app`. Undoing the
  rename (migrate mode with `--reverse`, same plan and preflight rules) is
  needed only to roll the single-keyspace nodes back before the compatibility
  release.
- **Tests.**
  - Fresh keyspace:
    - provision it, then log in as `ks1.root` with the provisioned password;
    - `CREATE USER 'ks1.app'` and log in as it;
    - check that `CREATE USER 'app'` and `CREATE USER 'ks2.app'` both fail
      with Go's error;
    - check that nothing can log in before provisioning.
  - Migration:
    - a password-protected `root` keeps its password and grants as
      `ks1.root`;
    - a pre-existing destination account aborts the run with no writes;
    - a run interrupted after some renames completes on retry, with the marker
      written once;
    - a passwordless-only `root` stops the run;
    - an interrupted migration while another node attempts an account change:
      the change is rejected by the freeze, and the retry stops on any rename
      it did not record.
  - Rolling path:
    - on a single-keyspace Rust node with the compatibility release, a fresh
      connection as plain `app` succeeds after migration;
    - it succeeds again after rollback of the multi-keyspace nodes;
    - it succeeds after a `--reverse` rollback.

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
  - process-wide administration: process variables (§6.3; tenants set only
    their keyspace variables),
    provisioning and account migration (§4.1), forced drain or close of a
    runtime;
  - every tenant-scoped HTTP status endpoint (§6.9), selected by
    `/ks/<name>/…`.

  The operator certificate is the authority, so the path selector is honored
  only there.
- **The public status port** keeps only process endpoints that expose no tenant
  data:
  - `/status` and the health checks;
  - a **process-level `/metrics`**. It carries only the unlabeled series
    TiProxy scrapes for balancing (§5.7): `process_cpu_seconds_total`,
    `process_resident_memory_bytes`, `tidb_server_maxprocs`,
    `tidb_server_memory_quota_bytes`, `tidb_tikvclient_backoff_seconds_count`
    and `tidb_tikvclient_request_seconds_count`. These are process totals.

  The full `/metrics`, with per-tenant labels, is served only on the operator
  listener.
- There is **no tenant self-service HTTP API in v1**. If one is added later,
  the keyspace is derived from the authenticated tenant identity, never from a
  path or query parameter.

### 5.6 Session migration through TiProxy

**Decision (siddontang, 2026-11-24):** the proxy is the **Rust TiProxy**,
`pingcap/tiproxy` branch `rust` (`be937b9d`). There is no new proxy and no
direct TiDB-to-TiDB handoff. Paths below are in that repository.

TiProxy moves a client's session from one tidb-server to another without
breaking the client's connection. It does this for rebalancing, scale-in,
draining (§5.3) and rolling restarts.

**TiProxy terminates the client handshake itself**
(`rust/crates/dataplane/src/session_engine.rs:5070-5113`):

- It sends its own greeting and handles client TLS and inbound PROXY v2.
- It forwards the client's response to TiDB with the **full username
  unchanged** (`ks.app`), the original auth bytes, database, collation and
  connection attributes. The plugin is replaced by `auth_unknown_plugin`, so
  TiDB must answer with an AuthSwitch carrying its own salt; TiProxy relays the
  rest of the exchange (`:2387-2535`).
- Backend TLS follows `security.sql-tls` / `require-backend-tls`.
- Outbound PROXY v2 carries the client address.

**The migration flow** (`session_engine.rs:4385-4719`;
`session-core/src/fsm.rs:539-680`):

1. **Wait for a safe point.** A redirect starts only when the session is not
   in a transaction and has no pending prepared statements, cursor or long
   data. TiProxy learns this from the `IN_TRANS` and `CURSOR_EXISTS` status
   flags on every final OK/EOF, including multi-result responses. A `BEGIN`
   inside a transaction while a redirect is pending makes TiProxy commit on the
   old backend, migrate, then replay the `BEGIN`.
2. **Capture state.** It runs `SHOW SESSION_STATES` on the old backend and
   expects exactly one row with columns `Session_states` (a JSON object) and
   `Session_token`. Any ERR, such as `ErrCannotMigrateSession`, fails that
   redirect and keeps the session on the old backend; the router may try again
   later.
3. **Connect to the new backend** with a handshake response containing:
   - the **same username**;
   - plugin `tidb_session_token`, with the token as auth data;
   - database = the state's `current-db`.

   **TiDB must reply OK directly, with no AuthSwitch** (`:4630-4636`).
4. **Restore state.** It sends `SET SESSION_STATES '<json>'`, which must return
   OK. TiDB rebuilds the session: user and system variables, prepared
   statements re-prepared under the same IDs, current database, and so on.
   Then TiProxy swaps backends.

What TiDB must provide, all of which exists in Go and is ported in Rust:

- `tidb-sessionstates` and `tidb-session/src/session_states.rs`;
- token login, `mysql_connection.rs:1131`;
- the same `session-token-signing-cert`/`-key` on every node;
- correct `IN_TRANS`, `CURSOR_EXISTS` and `MORE_RESULTS` flags;
- `graceful-wait-before-shutdown`, during which `/status` returns non-200 while
  SQL is still served.

**TiDB-side requirements in multi-keyspace mode:**

- **Routing happens before any auth decision.** Both handshakes TiProxy sends
  carry `ks.app`:
  - normal login with `auth_unknown_plugin`. The AuthSwitch must name the
    routed tenant account's plugin, so routing comes first (§4);
  - token login. It must be accepted **without** an AuthSwitch, even though
    the greeting advertised the process-level default plugin (§6.7).

  Both are covered by routing on the username in the first response.
- **Bind the token to the keyspace.**
  - The signing cert is fleet-wide. In multi-keyspace mode every username is
    prefixed and the signature covers it, so a `ks1.app` token cannot become
    `ks2.app`.
  - But a single-keyspace node outside Starter mode signs **unprefixed**
    usernames (`app`). Such a token would be accepted by any keyspace that has
    an `app` account, which is a cross-tenant login.
  - Fix: add a `keyspace` field, covered by the signature, to tokens signed in
    nextgen. The verifier rejects a token whose keyspace differs from the
    routed one, and multi-keyspace nodes reject nextgen tokens without the
    field.
  - TiProxy treats the token as opaque, so no TiProxy change is needed for
    this.
- **Restored state is tenant-scoped.** Global-dependent values are checked
  against the target tenant's sysvars (§6.3), and prepared statements are
  rebuilt against the tenant's catalog. The connection ID changes with the
  backend; TiProxy keeps the client-visible ID stable and handles the client's
  `KILL`.
- **`COM_CHANGE_USER`.** TiProxy updates its stored identity but never
  re-routes. Rejecting a cross-keyspace change-user on the TiDB side (§4)
  keeps TiProxy's stored identity and the backend's tenant consistent.
- **TLS behind TiProxy.** The client-facing TLS rule (§5.4) is enforced by
  TiProxy. TiDB then requires TLS on the TiProxy→TiDB hop
  (`require-backend-tls = true` in TiProxy). The only exception is plaintext
  from addresses in TiDB's `proxy-protocol.networks`, if the operator accepts
  that network as trusted.
- **Lifetime.** Go's token lifetimes are kept: 60s by default, 8h in Starter
  mode. The target node takes the migrated session's pin (§5.3) at routing
  time.

**Tests (Phase 3g)**, run on the local TiDB X playground behind a Rust TiProxy
(`tiproxy-rs --standalone`) that has the §5.7 changes:

- A session with user variables, session sysvars, prepared statements and a
  current database migrates A→B. The client keeps executing its prepared
  statements under the same IDs.
- Migration between a multi-keyspace node and a single-keyspace node of the
  same keyspace, in both directions.
- Migration is refused mid-transaction, with a cursor open, and with a
  temporary table. The session stays on the old node, and the client sees no
  error.
- Rejected with 1045:
  - a `ks1` token presented as `ks2`;
  - an unprefixed nextgen token presented to a multi-keyspace node;
  - an expired token.
- Signing-cert rotation during migration.
- Whole-node graceful shutdown and per-tenant drain (§5.7, K5): no client sees
  an error.

### 5.7 Changes needed in the Rust TiProxy

The Rust TiProxy assumes **one keyspace per TiDB address**:

- **Discovery.** It reads `/topology/tidb/<addr>` and
  `/keyspaces/tidb/<ks>/topology/tidb/<addr>` and keys backends **by address
  only**. When an address appears under several keyspaces, the last one
  parsed wins (`control-topology/src/model.rs:322-372`).
- **New connections** are routed without regard to keyspace. A namespace
  matches the exact username and falls back to `default`
  (`control-router/src/namespace.rs:106-180`).
- **Migration.** It requires the old and new backend to have the same
  keyspace (`control-router/src/selector.rs:1197-1201`). A balancing round is
  skipped when the busiest and idlest backends' keyspaces differ (`:952-978`).
- **Metering** is keyed by the backend's keyspace
  (`dataplane/src/session_engine.rs:2201-2203`).

A multi-keyspace TiDB therefore needs these TiProxy changes. They are
independent of the TiDB phases except where noted.

| # | Change | Why |
| --- | --- | --- |
| K1 | **Backends serve a set of keyspaces.** Read a multi-keyspace node's process record (below) and keep `serves: {keyspaces} or Any(allow-list)` per address, instead of one keyspace string | Otherwise the address collapses to one tenant |
| K2 | **Route new connections by username prefix.** Split `ks.app` at the first `.` (TiProxy already has the username at `admit_within`) and choose among backends that serve `ks`. A user without a prefix follows today's rules | Today a tenant can land on a node that does not serve it |
| K3 | **Migration guard by the session's keyspace.** The target must serve the session's keyspace (from its username), instead of "same backend keyspace" | Allows multi↔single migration within a tenant and blocks cross-tenant moves |
| K4 | **Meter by the session's keyspace** when the backend serves more than one | Otherwise all traffic is billed to one tenant or not metered |
| K5 | **Per-keyspace drain signal.** Read `draining_keyspaces` from TiDB's `/status` JSON. Migrate those keyspaces' sessions away while the node stays healthy for others | Needed for per-tenant drain (§5.3); whole-node shutdown already works via non-200 `/status` |
| K6 | **Balance within keyspace-compatible backends**, instead of skipping the round when keyspaces differ | Otherwise rebalancing stalls in mixed fleets |

**Registration on the TiDB side, matching K1:**

- **Process record.** A multi-keyspace node writes
  `/topology/tidb/<addr>/{info,ttl}` in the **unprefixed** PD etcd, the path
  TiProxy already reads for keyspace-less backends. Its `info` JSON adds
  `"multi_keyspace": true` and `"keyspaces": [allow-list]` (or `"*"` in
  dev-mode), next to the existing fields TiProxy uses (`ip`, `status_port`,
  `version`, `labels`).
- **Per-tenant records.** The node does **not** write
  `/keyspaces/tidb/<ks>/topology/tidb/<addr>` for TiProxy discovery. Each open
  runtime still registers its server info inside the tenant's own namespace,
  for DDL, schema sync and remote `KILL` (§6.6). Today's TiProxy keeps seeing
  the node as a single keyspace-less backend, which is safe as long as K2 is
  not deployed: Phase 2 is dev-mode only.
- **`/status`.** It returns 200 with
  `{"connections": n, "version": …, "draining_keyspaces": [..]}`. It returns
  non-200 only for a whole-process graceful shutdown.

The TiProxy changes are tracked as **Phase 3h**, with K1–K3 needed before any
shared deployment behind TiProxy. **Decision (siddontang, 2026-11-24):** they live on a
branch of a `siddontang/tiproxy` fork, based on `pingcap/tiproxy` `rust`. They
do not block TiDB Phases 1–3g.

### 5.8 Tenant hibernation (freeze and thaw)

**Decision (siddontang, 2026-11-24):** add hibernation as Phase 4b, after
Phase 4.

Everything a tenant uses inside the process should belong to that tenant and
be accountable to it. Then a cold tenant can be **frozen**: its warm state is
written out, and its memory goes back to the process. A frozen tenant is
**thawed** on its next connection, without the slow rebuild a fresh open does
today. This is scale-to-zero per tenant inside a shared process. Starter does
it today with one process per tenant and a standby pool.

**What a tenant owns.** After Phase 3, all per-tenant state lives in its
`KeyspaceRuntime` (§5.2). It falls into three kinds:

| Kind | Examples | On freeze |
| --- | --- | --- |
| Rebuildable caches | infoschema, stats, plan cache, privileges, bindings, global sysvars, region and TSO caches | Snapshot the expensive ones; drop the rest |
| Connections to the cluster | PD client, TiKV channels, etcd client, owner campaigns, background loops | Close, resigning owner roles first (§5.3) |
| Live sessions | client connections, transactions, prepared statements | Migrate through TiProxy (§5.6), or wait for them to end |

Storage (CSE) is the source of truth, so nothing in the first two rows needs a
snapshot for correctness. The snapshot only makes thaw fast.

**Memory isolation.**
- Each runtime gets its own memory-tracker root under the process
  `MemArbitrator`, with a per-tenant quota. Quota pressure on one tenant then
  cancels or spills that tenant's statements, not another tenant's.
- **v1 frees memory by ownership, not by arena.** Freeze joins the tenant's
  background tasks and drops the runtime normally. The tracker root must reach
  zero tracked bytes; anything left is a leak the tests report.
- **Per-tenant jemalloc arenas are not in v1.** On the shared Tokio runtime,
  tenant A's task can suspend and tenant B's run on the same worker thread, so
  a thread-level arena would put B's allocations in A's arena. jemalloc also
  needs every participating thread cache flushed before
  [`arena.<i>.destroy`](https://jemalloc.net/jemalloc.3.html#arena.i.destroy).
  Arenas stay a later option, only if a test can show that no worker thread,
  thread cache or surviving reference can still touch the arena.

**One eviction decision.** The registry has a single idle path, replacing
§5.3's separate idle close and freeze timers. When a Ready runtime has had no
pins for `idle-timeout`, or the process is under memory pressure, the registry
decides once: **freeze** when hibernation is enabled and a snapshot is
allowed, otherwise **close** as in §5.3.

**Freeze:**
1. Move the runtime to Draining. New connections wait for thaw.
2. Migrate or wait out live sessions (§5.6). Skip the freeze while the tenant
   owns unfinished DDL, DXF or TTL work and this node may be its last serving
   node (§5.3).
3. **Bounded drain.** If sessions cannot be migrated or a job keeps its pin
   past `freeze-drain-timeout` (default 30s), cancel the freeze: return to
   Ready and wake the waiting connections. A pressure-triggered freeze then
   tries the next tenant.
4. Write the snapshot (below) to a temporary object, then publish it
   atomically by rename (local disk) or by writing the manifest last (object
   storage). A tenant that hits a snapshot I/O error after a successful drain
   is closed normally; its next open is a cold open.
5. Retire and release the runtime (§5.2) and check that its tracker root is
   at zero.

**What the snapshot holds, and what it does not.** v1 snapshots only the
schema cache (the infoschema at schema version N) and the stats already
loaded. Accounts, grants, global sysvars and bindings are **not** taken from
the snapshot: a user or grant can be dropped while the tenant is frozen, and a
read timestamp alone cannot tell thaw which rows disappeared. They are always
loaded from the live keyspace on thaw, before the runtime becomes Ready. The
full privilege reload already does this
(`rust/crates/tidb-privilege/src/handle.rs:118`).

**Manifest binding.** The snapshot's manifest records the PD cluster ID, the
immutable keyspace ID, the keyspace's storage generation (a value that changes
when the keyspace is restored, for example from BR), the binary version, the
schema version N and a checksum. A valid checksum alone would also accept
another tenant's perfectly valid image. Thaw checks every field against the
live store before seeding any cache. Any mismatch, a missing manifest or an
interrupted upload gives a normal cold open.

**Thaw**, on the first connection or an explicit preload:
1. Verify the manifest (above).
2. Load accounts, grants, global sysvars and bindings from the keyspace.
3. Seed the schema cache from the snapshot only if every schema diff from N to
   the current version is still available; apply them through the normal
   schema-sync path. Otherwise reload the schema in full. Seed stats, then
   refresh those changed after the snapshot as the stats loop normally does.
4. Mark the runtime Ready and wake waiting connections.

Correctness never depends on the snapshot: every failure path is a cold open.

**Policy.** Tenants move between hot (open), warm (frozen to local disk) and
cold (frozen to object storage). Eviction is by idle time and by memory
pressure, most idle first; `preload` tenants are never frozen.

**Tests.**
- **Can tenant B use A's memory?** Under a fixed process memory limit: warm
  tenant A, freeze it, then have tenant B allocate the released budget and
  keep serving queries. Repeat with A and B alternating over many cycles.
  Record the peak memory while writing the snapshot (it must not need a second
  copy of the state), the tenant-tracked bytes left after each freeze, process
  RSS, and B's query latency. RSS alone can stay flat when the allocator
  reuses pages. Both outcomes are required in every cycle: A's tracker root
  reaches zero, **and** B allocates the released budget and completes its
  queries under the same process limit. The second catches memory the tracker
  missed.
- **Thaw matches a fresh node.** While A is frozen, revoke a grant, drop a
  user and run DDL on A from another node. After thaw, the first login and the
  first query must behave exactly as on a freshly started node. The test
  also asserts that thaw **replayed** the schema diffs from the snapshot's
  version N (a metric or log counter), since a cold open would return the same
  query result.
- **Wrong or broken snapshots fall back.** Another tenant's snapshot, one from
  before a restore, a corrupt one and an interrupted upload each give a cold
  open with correct results.
- **Bounded drain.** A pressure-triggered freeze with an unmigratable session
  is cancelled after `freeze-drain-timeout` and the tenant keeps serving.
- First-query latency after thaw versus a cold open, for 10, 100 and 1000
  tables.
- A session migrated away during freeze keeps working (§9 harness).

### 5.9 Per-tenant data model

This section lists what each tenant's state is, where it lives, who changes
it, and whether it is isolated today. It was checked against the code at
`3905611647` (paths relative to `rust/crates/`).

**Admin model.** Tenants change only keyspace state: their data, accounts and
keyspace-scoped variables. Process state (instance variables, the process
statics behind them, PD-wide settings) is changed only by an operator, through
SYSTEM on the mTLS operator listener (§5.5). A tenant's `SET GLOBAL` of a
process variable is refused with 1227 (§6.3).

Abbreviations. **Lives in:** TS = tenant storage (inside the keyspace prefix),
RT = per-runtime memory, P = process memory, PD = the PD cluster. **Changed
by:** SQL = tenant SQL, Op = operator, Cfg = node config only. Until the
operator listener exists, Op means node config.

| Item | Lives in | Changed by | Status | Evidence | Fix phase |
| --- | --- | --- | --- | --- | --- |
| Schema meta (meta keys, schema version, global IDs) | TS | SQL (DDL) | Isolated: the store is opened with the keyspace's API v2 codec. `GLOBAL_ID_MUTEX` is a process lock (contention only). | `tidb-server/src/cluster_stack.rs:114`; `tidb-txnkv/src/driver/tikv_store.rs:158`; `tidb-meta/src/transaction.rs:52` | — |
| Infoschema cache, schema lease, schema-version sync | RT | SQL (DDL) | Isolated: one loader per runtime; schema sync on the tenant's etcd; the validator keeps its keyspace's `tidb_max_delta_schema_count` of deltas. | `cluster_stack.rs:799,970` | — |
| DDL job queue and history, DDL owner | TS + tenant etcd | SQL | Isolated: owner managers are keyed by keyspace. A job's reorganization reads its keyspace's vardef values on every thread it spawns. | `tidb-ddl/src/owner_mgr.rs:16`; `tidb-ddl/src/real_tikv_ddl.rs` (job scope) | — |
| `JOB_CTX_MAP` (reorg contexts by job ID) | P | — | Isolated: keyed by keyspace and job ID (`JobKey`). | `tidb-ddl/src/job_ctx.rs` | — |
| `JOB_VER_IN_USE`, `GLOBAL_INDEX_V1_SUPPORTED` | P | Classic DDL's version check only | Not per tenant: only the classic kernel's DDL writes them; on next-generation (every multi-keyspace node) they are one constant process value (§6.4). | `tidb-model/src/job_enums.rs:210`; `tidb-model/src/index.rs:84`; `tidb-ddl/src/worker.rs:1664` | — |
| MDL (`mysql.tidb_mdl_info`, schema pins) | TS + RT | — | Isolated: pins per domain. Next-generation kernels always lock, so `ENABLE_MDL` is an instance setting. | vardef appendix | — |
| Accounts (`mysql.user`, `db`, `tables_priv`, roles, `global_priv`, password history) | TS | SQL | Isolated: privileges per domain, privilege watch on the tenant's etcd, username policy per runtime. Users without a prefix route to the configured keyspace. | `tidb-server/src/keyspace_registry.rs` (`route`) | Prefix-less users: Phase 2 (Q2) |
| `mysql.global_variables` and the sysvar cache | TS + RT | SQL (keyspace variables); Op (process variables) | 54 of 55 global statics are kept per keyspace (the cloud storage URI is a process variable); a tenant's `SET GLOBAL` of a process variable is refused. | `tidb-server/src/keyspace_runtime.rs`; §6.3 appendices | — |
| `tidb_external_ts` | PD (whole cluster) | Op | Shared by design: PD keeps one external timestamp per cluster. A process variable; tenants read it. | commit `a18dec47b5` | — |
| Bindings (`mysql.bind_info`, cache) | TS + RT | SQL | Isolated: binding handle per domain. | `cluster_stack.rs:256` | — |
| Statistics (`mysql.stats_*`, cache, owner, auto-analyze queue) | TS + RT | SQL (ANALYZE) | Isolated per domain; auto-analyze reads its keyspace's settings. | `cluster_stack.rs:810,944` | — |
| Stats globals keyed by table ID (the table row-count cache, async and sync load queues, `GLOBAL_AUTO_ANALYZE_PROCESS_LIST`) | RT + P | — | Isolated: the row-count cache behind `INFORMATION_SCHEMA.TABLES` sizes is one per domain (`Domain::table_row_stats`); the sync-load flights and the async-load demand are the domain pool's (`StatisticsLoadWorkers`). Process IDs do not collide: they come from the one process allocator (§6.6). | `tidb-domain-core/src/domain/mod.rs` (`table_row_stats`, `load_workers`); `tidb-stats-handle-syncload/src/sync_load.rs`; `tidb-stats-handle-util/src/auto_analyze_proc_id_generator.rs:101` | — |
| GC worker, delete ranges, GC settings, safe points | TS + PD (per keyspace) | SQL (GC settings) | Isolated: one GC worker per runtime. | `keyspace_runtime.rs` | — |
| min-start-ts report | Tenant etcd | — | Isolated: `ACTIVE_START_TS` and `GLOBAL_INNER_TXN_START_TS` keep one set per keyspace ID of the store a transaction reads, and each runtime's report reads its own set, its own process list and internal sessions. Statement results and cursors kept past their transaction cannot name their keyspace: they hold a shared set every report reads (conservative: another tenant's GC waits for them). The auto-analyze list only names process IDs, unique in the process (§6.6). | `cluster_stack.rs` (`start_min_start_ts_reporter`); `tidb-txnkv/src/inner_txn.rs` (`PerKeyspace`) | — |
| TSO, PD client, region cache, TiKV channels | RT | — | Isolated: one store per keyspace. The tokio pool is shared by design. | `tikv_store.rs:93,127,158` | — |
| AutoID allocators (default caching) | RT | — | Isolated per domain. | `cluster_stack.rs:793` | — |
| AutoID service (`AUTO_ID_CACHE=1`) | P | — | Shared: only the configured runtime serves it, bound to one keyspace. Refused in multi-keyspace mode. | `keyspace_registry.rs`; `tidb-autoid-service/src/lib.rs:354` | 3f |
| Sequences | TS + RT | SQL | Isolated. | — | — |
| Resource groups | PD (per keyspace) | SQL | Isolated: each runtime's info syncer holds its own store's resource-manager client, whose requests name the store's keyspace, and PD keeps groups per keyspace; resource-group DDL runs per tenant (and a keyspace bootstraps on a multi-keyspace node, whose bootstrap alters `default`). `DEFAULT_RESOURCE_GROUP` is an immutable default, the same in every keyspace (§6.5). | `tidb-domain-infosync/src/info_syncer.rs`; `cluster_stack.rs` (`PdResourceManagerClient`); serial test `two_keyspaces_keep_their_own_resource_groups` | — |
| Placement, label rules, TiFlash replicas, PD HTTP | PD | SQL | Each runtime keeps its own PD managers (Go's `InfoSyncer`, §6.2). **Isolated:** label rules, whose IDs and ranges carry the job's keyspace (Go `UseKeyspaceAwareRules`, `keyspace/<id>/schema/<db>/<table>`) in DDL job steps, their delivery and compensation, `ATTRIBUTES` (the session keyspace's rules) and GC (its own keyspace's rules, found by their keyspace-encoded ranges: a departure from Go, whose GC compares keyspace-less ranges and so never removes a next-generation rule); a job inherits only the rules over its table's own physical IDs (a dropped table's rule under the reused name stays for GC; Go inherits it); placement, whose bundles are keyed `keyspace-<id>-<group>` and ranged by the keyspace's codec where they meet PD (DDL, `ALTER RANGE`, `SHOW PLACEMENT`, the policy-in-use check, GC). Both hold on single-keyspace next-generation nodes too. A Go or older node writes keyspace-less IDs and ranges: its label rules are not listed by a keyspace-aware node, its table bundles not read back, and a keyspace's GC leaves them alone (they cover no keyspace's keys; nothing can tell which keyspace wrote them, so there is no migration; the operator removes them). The exception is the global range's unqualified `TiDB_GLOBAL` bundle: its empty bounds cover every keyspace, so it is detected (a warning at domain start, a `RANGE TiDB_GLOBAL (legacy, every keyspace)` row in `SHOW PLACEMENT`, its policy not droppable) until the operator removes it (§6.2). A step's PD writes are undone on a definite abort to their previous values by the owner (§6.2). **Refused (1235), named by bare IDs:** TiFlash replicas -- the statements, a DDL step whose resolved metadata would configure TiFlash rules (an inherited replica on a partition change or TRUNCATE), the replication check of a resumed ADD PARTITION and the build-progress poll of a resumed columnar index job; the TiFlash poller and GC's TiFlash rule cleanup are off; the progress-cache delete of `SET TIFLASH REPLICA 0` and a materialized view cutover's TiFlash progress and PD affinity cleanup are skipped. Next-generation TiDB has TiFlash (Go's TiFlash manager keys its rules `keyspace-<id>-table-<id>-r`), but this port's manager serves the null keyspace (keyspace-less rule IDs, ranges and status URLs) and the poller and progress cache are per table ID: making them per keyspace is later work. Affinity groups (`_tidb_t_<id>`; next-generation PD requires `_tidb_ks<id>_...`), `SHOW AFFINITY`, DISTRIBUTE TABLE and its job statements (PD's cluster-wide schedulers over keyspace-less ranges) and `SHOW TABLE DISTRIBUTIONS` (keyspace-less ranges, as in Go) are refused too. FLASHBACK CLUSTER is refused: it closes and restores PD's schedule limits for the whole cluster. On any node a finishing FLASHBACK CLUSTER job restores the limits only if it closed them: the job persists its intent before it asks PD to close them, and the close after PD answers (`pd_schedule_state`). A job that stopped between the two, or a job with no record still in `DELETE_ONLY`, may or may not have closed them; it leaves them as they are and hands off to the operator, with the saved value in its job warning and an error log. A job with no record resumed in `DELETE_ONLY` is cancelled there with the same handoff (the saved and the current values), writing nothing: it cannot tell PD's current limits from its own close or an operator's, and PD's schedule configuration has no compare-and-set to close them only while they still hold the saved value. So a later state without a record is still a Go node's real close. | `tidb-domain-infosync/src/info_syncer.rs`, `placement_manager.rs` (`encode_bundle`); `tidb-ddl-build/src/label_rules.rs`; `tidb-executor/src/multi_keyspace.rs`; `tidb-ddl/src/cluster_ddl.rs` (`pd_backed_write_feature`); serial tests `two_keyspaces_keep_their_own_label_rules`, `two_keyspaces_keep_their_own_placement` | TiFlash and affinity: later |
| DXF tables and task managers | SYSTEM storage (nextgen) | — | User runtimes skip the DXF loop, but `TASK_MANAGER_INSTANCE` and `DXF_SVC_TASK_MGR` belong to the last-opened runtime. | `tidb-domain-core/src/domain/dist_task.rs:145`; `tidb-dxf-storage/src/lib.rs:300,304` | 3f (§6.10) |
| TTL jobs and timers | TS + RT | SQL | Isolated: job manager per domain; delete rate per keyspace. | `tidb-ttl-ttlworker/src/del.rs:118` | — |
| MV service, workload repository, runaway, RU writer, telemetry | RT | — | Isolated per domain. | `keyspace_runtime.rs` | — |
| IMPORT INTO, BR, PLAN REPLAYER, external storage | P | — | Shared; refused in multi-keyspace mode. Each runtime open overwrites the import store, BRIE node and cluster ID. | `keyspace_runtime.rs:100`; `tidb-executor/src/builder.rs`; `tidb-executor/src/admin.rs` | 3f |
| INFORMATION_SCHEMA, PERFORMANCE_SCHEMA, SYS | RT + P | — | Isolated (§6.8): table definitions, catalog rows, processlist, `TIDB_TRX` and index usage are per runtime; the process-wide diagnostics are keyed by keyspace or filtered to the session's; `CLUSTER_*` tables read this node only; `ATTRIBUTES` lists the session keyspace's label rules. Refused (1235): `CLUSTER_LOG`, `TIDB_HOT_REGIONS(_HISTORY)` (not done in 3b), the profile tables. Empty: `CLIENT_ERRORS_SUMMARY_*`, `MEMORY_USAGE_OPS_HISTORY`. | `tidb-executor/src/memtable_reader.rs` (`ViewerState::keyspace`) | Rest: §6.8 |
| METRICS_SCHEMA | P | — | Refused (1235) in multi-keyspace mode, and with it the metric-backed summaries and inspections: `PROMETHEUS_STATE` belongs to the last-opened runtime, and metrics carry no keyspace label. | `tidb-executor/src/driver/physical_builder.rs` (metric scan) | Metrics labels (§5.5); not done in 3b |
| Instance plan cache, stats, binding and schema caches | RT | Cfg (limits) | Isolated, but each runtime gets the full process quota, so memory grows with the number of tenants. | `tidb-session/src/domain/cluster_session_node/node_services.rs:37` | Phase 5 |
| Statement summary v1 and v2 | P | — | v1 isolated: one process map keyed by keyspace, read for the session's keyspace; the evicted rollup (every keyspace's) reads as empty. Capacity is shared (noisy neighbour). v2 (persistent) is refused (1235): its records and files have no keyspace key. | `tidb-stmtsummary/src/statement_summary.rs` (`append_keyspace`); `tidb-executor/src/memtable_reader.rs` (`statement_summary_rows`) | v2: later |
| Slow log, `slow_query`, top-N slow | P | — | Isolated: one slow log; `slow_query` reads the entries whose `Keyspace_name` is the session's. `ADMIN SHOW SLOW` keeps one memory per keyspace. | `tidb-executor/src/slow_query.rs` (`SlowLogChecker::keyspace`); `tidb-domain/src/topn_slow_query.rs` | — |
| Deadlock history, transaction summary, lock waits | P | — | Isolated: deadlocks carry the detecting store's keyspace and are read for the session's (capacity shared); each keyspace's process list records into its own fork of the transaction-summary recorder; `DATA_LOCK_WAITS` keeps the store's keyspace's waits (TiKV answers with every keyspace's). | `tidb-deadlockhistory/src/lib.rs` (`push_in_keyspace`); `tidb-txninfo/src/lib.rs` (`fork`); `tidb-txnkv/src/driver/tikv_store.rs` (`own_lock_waits`) | — |
| Top SQL | P | — | Shared: the reporter binds one keyspace. Not readable through SQL; reports go to the collector. | `tidb-topsql/src/lib.rs:45` | Later (§6.8) |
| `SHOW STATUS` providers | P | — | Isolated: one process list whose providers are keyed by keyspace (each domain's and GC worker's); a session reads the process providers and its keyspace's. | `tidb-variable/src/statusvar.rs` (`get_keyspace_status_vars`) | — |
| Processlist, `KILL`, connection IDs | RT + P | — | Isolated: processlist per domain; one process server ID, reserved in SYSTEM and in each tenant before it is published, and one process connection-ID allocator; `KILL` of another tenant's ID finds nothing. | `tidb-server/src/keyspace_runtime.rs` (`ProcessServerId`); `tidb-domain/src/server_id.rs` | — |
| Expensive-query watchdog | RT + P | — | Isolated: each runtime runs its own over its own processes and settings. It recognizes auto-analyze sessions through the process-wide `GLOBAL_AUTO_ANALYZE_PROCESS_LIST`, whose connection IDs are unique in the process (§6.6). | `keyspace_registry.rs` (`start_expensive_query_watchdog`); `tidb-stats-handle-util/src/auto_analyze_proc_id_generator.rs:101` | — |
| `max_prepared_stmt_count` counter | P | — | Shared: one process counter checked against each tenant's own limit. | `tidb-variable/src/vars.rs:1024` | Phase 4 |
| Server info, topology | Tenant etcd | — | Isolated per runtime; the process record for TiProxy is missing. | `cluster_stack.rs:840` | 3h |
| Keyspace identity, resource tags | RT | — | Mostly isolated. A few readers still use the configured keyspace name. | `tidb-ddl-ingest/src/util.rs:45`; `tidb-planner/src/plan_builder/stmt_build.rs:51`; `tidb-planner-extstore/src/lib.rs:56`; `tidb-server/src/http_status.rs` | 3a |
| Logging | P (one file) | Cfg | Shared by design; each record carries the tenant. | `tidb-logutil/src/tenant.rs` | — |
| Metrics | P | — | Shared: no keyspace label. Not readable by a tenant: METRICS_SCHEMA is refused, and the status port is operator-only (§5.5). | `tidb-metrics` | §5.5 |
| Password validation, history, lockout | TS + RT | SQL | Isolated (vardef appendix: kept per keyspace). | §6.3 appendix | — |
| Signing cert, JWKS, LDAP, SEM, extensions | P | Cfg | Shared by design. Session tokens are not bound to a keyspace. | `tidb-sessionstates/src/session_token.rs:189` | 3g |
| `require_secure_transport` | P | Op | A process static, while §6.7 checks it per tenant. | `tidb-tls/src/lib.rs:28` | Phase 2 (§6.7) |
| Memory arbitrator, server memory limit, top-1 kill | P | Cfg | Shared by design (noisy neighbour). | `tidb-memory/src/process.rs:13,55` | Phase 5 |
| `max_connections` | P | Cfg | Process cap only; no per-tenant cap, no `max-keyspaces`, no idle close. | `sql_node.rs:161` | Phase 4 |
| `DDL_HISTORY_SEQUENCE` | P | — | Shared; harmless (a sequence of history writes). | `tidb-ddl/src/cluster_ddl.rs:90` | — |

**Cross-tenant risks today**, all in development mode only:

- GC: a statement result or cursor kept past its transaction holds back
  every tenant's reported min-start-ts (conservative, §6.5).
- Diagnostics (§6.8): what has no keyspace-correct source is refused or
  empty; Top SQL reports still name one keyspace (not SQL-readable).
- Exhaustion: one tenant's prepared statements count against another tenant's
  limit; one process connection cap; the memory top-1 kill can pick a tenant
  that is not the cause (Phases 4 and 5).
- PD state keyed by bare object IDs or keyspace-less ranges (TiFlash
  replicas, affinity groups, PD's schedulers) and `AUTO_ID_CACHE=1`:
  refused in multi-keyspace mode (3f for the AutoID service). Label rules
  and placement bundles that a Go or older node wrote are keyspace-less and
  stay until an operator removes them (§6.2).

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

The following were `Mutex<Option<Arc<..>>>` statics set from one store in
`new_cluster_domain`, so the last-opened runtime's clients served every
tenant:

- `resource_manager_client.rs:147`
- `label_manager.rs:114`
- `placement_manager.rs:154`
- `schedule_manager.rs:91`
- `tiflash_manager.rs:455`
- `region.rs:83` (`PD_HTTP_CLI`)
- `prometheus_addr.rs:86`
- `tidb-domain-affinity/src/lib.rs:427`
- `tidb-variable/src/domain_slots.rs:31,32,75`

**Done (3b, development mode).** The first six are the fields of an
`InfoSyncer` (Go's struct, `tidb-domain-infosync/src/info_syncer.rs`), one
per runtime, which also holds the codec of the runtime's store (Go
`InfoSyncer.tikvCodec`). Each runtime's domains keep its own; the
configured keyspace's is also the process's (Go `globalInfoSyncer`), which
a thread that entered none reads (the status port, BR). A session enters
its domain's while it runs a statement, a DDL job its DDL's, as they enter
the vardef snapshot (§6.3); the package-level accessors answer from the
entered one, so their call sites are unchanged. What runs on a thread of
its own holds its domain's explicitly: the `ATTRIBUTES` reader (executor
workers pull it), the unistore resource-group provider and the GC worker's
rule client. A tenant's PD effects then go through its own clients:

- **Resource groups:** the resource-manager client is the store's PD
  client, whose requests name its keyspace; PD keeps groups per keyspace.
- **Label rules:** a DDL step builds rule IDs and ranges with its job's
  keyspace codec (Go `jobCtx.store.GetCodec()`), so on a next-generation
  kernel they are `keyspace/<id>/schema/<db>/<table>` over the keyspace's
  keys (Go `UseKeyspaceAwareRules`), on single-keyspace nodes too.
  `ATTRIBUTES` lists the session keyspace's rules (Go
  `filterRulesByKeyspace`), and a keyspace's GC reads only its own and
  finds a dropped table's rule by its keyspace-encoded ranges (Go's
  `getGCRules` compares keyspace-less ranges and never finds it). A job
  inherits a rule found by name only if it ranges over one of the table's
  own physical IDs: a dropped table's rule keeps its name until GC, and a
  new table that takes the name must not get its attributes (Go inherits
  it).
- **Placement:** rule group IDs are `TiDB_DDL_<object id>`
  (`tidb-placement/src/bundle.rs:49`), one namespace in PD for the whole
  cluster, and the bundles' ranges are keyspace-less. Go has no keyspace in
  either, so its bundles never covered a keyspace's keys on a
  next-generation kernel. Where a bundle meets PD, a keyspace's bundle is
  keyed `keyspace-<keyspace id>-<group id>` -- the prefix Go's TiFlash
  manager gives its placement rule IDs (`encodeRuleID`) -- and each rule's
  range is encoded by the keyspace's codec (`EncodeRegionRange`), as label
  and TiFlash rules are; reads decode them back
  (`placement_manager::encode_bundle`, `decode_bundle`). The rest of TiDB
  keeps keyspace-less bundles. There is no migration for table bundles
  that a Go or older node wrote: they cover no keyspace's keys, and nothing
  records which keyspace wrote them, so they are left for the operator.
- **Legacy global bundle (upgrade check and cutover):** Go's (and a pre-3b
  node's) `ALTER RANGE global` bundle is `TiDB_GLOBAL` with empty bounds,
  which on a next-generation kernel places every keyspace's keys, on a
  single-keyspace node as on a multi-keyspace one. A next-generation node
  serving a keyspace detects it (`info_syncer::get_legacy_range_bundle`):
  it logs it at domain start, `SHOW PLACEMENT` lists it as `RANGE
  TiDB_GLOBAL (legacy, every keyspace)`, and a policy of its name cannot be
  dropped (the check is by name in every keyspace, conservatively: the
  bundle does not say which keyspace's policy it is). No keyspace rewrites
  it: its `ALTER RANGE global` (to a policy or back to default) writes its
  own qualified bundle. The cutover is the operator's: once each keyspace
  that wants global placement has set its own, remove the unqualified group
  from PD (`pd-ctl config placement-rules rule-bundle delete TiDB_GLOBAL`).
  Go next-generation nodes in the same fleet keep writing it.
- **PD journal and reconciler (eventual repair):** PD's bundle, label-rule
  and resource-group APIs have no conditional write, so a stale owner whose
  forward write lands late (paused after its last lease check while a
  successor committed) could leave PD differing from the committed catalog.
  Every physical PD object a keyspace writes now has a journal record in
  the keyspace's own meta (`tidb-ddl-pdjournal`; plain string meta keys
  `PDJrnl/...`): the authoritative record (desired value as PD keeps it, a
  tombstone for a delete, and a generation) and the prepared intents of
  attempts about to write it (stable op ID, value, the generation it would
  commit, and the pre-image, which is the last authoritative value and
  never a fresh PD read). A write is (1) intent committed, (2) PD written,
  (3) record committed: a DDL job step writes the records in its catalog
  transaction (`DdlWrite.mutations`), so they commit with the catalog
  change or not at all, and racing attempts conflict on the record key;
  ALTER RANGE (global and meta, a policy or `DEFAULT`) and GC's deletes
  commit them in a small meta transaction that commits only while the
  attempt's own intents (op ID and generation) are still outstanding and
  over the generation they were prepared against, so a delayed session
  cannot commit over its successor, nor after an abort resolved it (even
  one of its objects: a refused attempt then aborts all its intents and
  puts every object it wrote back to its current record, a newer winner's
  included), and SQL succeeds only after that commit; the bootstrap commits the default
  group's record (and the keyspace's cutover) with its catalog. A definite
  abort (a job step's, a session's compensation, or the reconciler's)
  puts every object's pre-image back (a failed or unanswered PD write
  counts as applied) and removes the intents only in the transaction that
  moves the record above the attempt's generation, holding the baseline (a
  tombstone for an object that did not exist), so the attempt's write that
  PD applies later is a divergence the reconciler reverts; an undetermined
  commit leaves the intents. Only the owner undoes; a lost owner leaves PD
  and its intents to the successor.

  The current DDL owner reconciles at takeover and every 30 s
  (`reconcile::SCAN_INTERVAL`; a node without PD, whose objects live in the
  process's mocks, runs no loop): it resolves each intent no live attempt
  holds from the records (its op committed: dropped; otherwise aborted, and
  the record's generation moves past it so a delayed commit of the attempt
  conflicts or fails its compare), then compares each record with PD over
  the fields TiDB sets (`normalize`: both sides read into TiDB's own types,
  which keeps every range bound and the keyspace-qualified IDs; PD-only
  fields and defaults compare equal, a cleared field or another tenant's
  range never does) and rewrites and reads back what differs. It never acts
  on an object without a record. Records are never deleted: a request PD
  accepted late can complete at any time, which no lease or timeout
  bounds, so a deleted object's tombstone (key, generation, op ID, no
  value; about a hundred bytes of the keyspace's meta for every object
  ever deleted or aborted while absent) stays until a newer record of the
  same object replaces it. A late write stays in PD for at most the interval
  plus one pass (a journal scan, one PD read per record, and per differing
  object a re-read, a write and a read back, each bounded by the PD
  client's timeout); a failed repair adds an interval per retry. An
  object PD cannot be read for (or whose value does not decode) fails
  alone: the pass falls back to one read per object, repairs the others
  and reports it, and the next pass retries it. Within the
  affected tenant PD can be stale for that long; other tenants' objects are
  never read or written. A repair re-reads the record and checks for an
  open intent under the object's lock, the one a writer of the same
  process takes before its prepare, so it never writes back a value such a
  writer has just replaced. The writers of an object in one process (job
  steps, ALTER RANGE, GC) take turns from their first PD write until their
  records committed or their compensation put PD back, and a compensation
  reads the records it restores within its turn, so it never puts an older
  value over a newer committed one; across nodes the reconciler repairs
  such an overwrite at its next pass.

  Baselines: resource groups and table or partition bundles without a
  record take the catalog's value as of the step's snapshot. Label rules
  and range bundles exist only in PD, and a PD read can return an old
  owner's delayed write, so they need a seeded baseline before their first
  mutation: a keyspace bootstrapped with the journal records its cutover at
  bootstrap; an older keyspace needs the operator's cutover, which seeds a
  record of each such object from PD at DDL owner takeover (only TiDB's
  own, and the range bundles under their own group IDs), once
  `TIDB_PD_JOURNAL_CUTOVER` (`*` or keyspace IDs) says the writers that
  bypass the journal (nodes of an older build) are drained; no writer of
  this build touches such an object before. Until then such a mutation
  fails before PD with an error naming the cutover: a DDL step fails and
  its job is cancelled (Go cancels a job whose PD call failed), ALTER RANGE
  and GC's delete return the error.

  Which label rules the cutover adopts: `Rule::reset` keeps a rule's
  database, table, partition and keyspace in its labels and sets every
  other field TiDB writes, so a rule is TiDB's only when `new_rule_id`
  rebuilt from those labels for the keyspace's codec is its exact ID and
  the whole rule equals the one `Rule::reset` builds from its labels over
  the catalog's physical IDs of that table or partition (table and
  partitions for a table rule): index 2 or 3, type, reserved labels (the
  tenant's keyspace label on a keyspace-aware codec) and key ranges. The
  catalog is listed once per database per cutover run; names with `/` are
  fine, the labels carry them whole. A rule outside TiDB's IDs (`[keyspace/<id>/]schema/...`) is the
  operator's or PD's: no record, never written or reverted. A rule under
  TiDB's IDs that fails any check (missing or mismatched labels, another
  range, a table the catalog does not hold, a catalog that cannot be read)
  is held back: logged with the reason, returned in the cutover report and
  recorded durably in the journal (`PDJrnl/H/...`), and every write of it
  -- a job step, ALTER RANGE, GC -- fails before PD with an error naming
  it, across restarts and owner changes, so an abort can never take it for
  absent and delete it; GC's delete of a dropped table's rule fails too
  (GC reads the records and the holds in one snapshot, and a resolution
  after it is caught by the delete's prepare), so its delete-range task
  stays pending until the rule is resolved. The operator resolves it by inspecting the rule in
  PD and either deleting it (`pd-ctl region-label delete <id>`; re-apply
  the table's attributes by DDL afterwards if they were TiDB's) or making
  it the table's own (its labels and ranges), then restarting the DDL
  owner with `TIDB_PD_JOURNAL_CUTOVER` naming the keyspace again: that run
  re-checks every held-back object, adopts one that is now TiDB's, records
  a tombstone for one PD no longer holds, and keeps the rest held back.
  With the variable left set, a takeover after a complete cutover reads
  the marker and the held-back objects first and, with none held back,
  reads neither PD nor the catalog.

  Residual: a repair racing a non-job writer on another node can revert that writer's
  just-written value until the next pass; BR's restore writes label rules
  without the journal, so where a record already holds a restored rule's ID
  (a tombstone of a dropped table of that name) the reconciler puts the
  record's value back (re-apply the attributes by DDL after a restore); the
  unqualified legacy `TiDB_GLOBAL` bundle is never journaled; a non-job
  intent (ALTER RANGE, GC) is left to its session for 60 s before the
  reconciler resolves it; a table bundle's first catalog baseline scans
  the keyspace's tables once; and a pass reads PD once per bundle and
  group, so its duration grows with the number of journaled objects.

Still shared, and every multi-keyspace use is refused (1235) or off:

- `schedule_manager`: FLASHBACK CLUSTER, whose schedule limits are the
  whole cluster's.
- The TiFlash replica manager is per runtime, but it is a port of the null
  keyspace's (keyspace-less rule IDs `table-<id>-r`, ranges and status
  URLs), and its poller, progress cache and GC cleanup work by bare table
  ID. Next-generation TiDB has TiFlash (Go keys its rules
  `keyspace-<id>-table-<id>-r`), so TiFlash replicas are refused until
  that port lands, not by design.
- The PD HTTP client's scheduler calls (DISTRIBUTE TABLE and its jobs:
  cluster-wide schedulers over keyspace-less ranges) and region
  distributions (`SHOW TABLE DISTRIBUTIONS`, keyspace-less ranges as in
  Go).
- `prometheus_addr`: METRICS_SCHEMA (metrics carry no keyspace label, §5.5).
- `tidb-domain-affinity`: affinity groups are named by bare table IDs
  (`_tidb_t_<id>`; next-generation PD requires keyspace-aware
  `_tidb_ks<id>_...` IDs): the job steps that write them, `SHOW AFFINITY`
  and a materialized view cutover's cleanup.
- `domain_slots` holds process variables (§6.3): the configured keyspace's
  domain fills them, and a tenant's `SET GLOBAL` of a process variable is
  refused.

### 6.3 `SET GLOBAL` and process atomics

This is the largest semantic issue. `GlobalSysvars` is already per instance,
but its SetGlobal hooks write **85 process statics** in `tidb-vardef/src/lib.rs`
(the appendix below classifies all 92 statics it declares)
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

#### Appendix to §6.3: the process statics of `tidb-vardef`

Generated from `tidb_vardef::classification::PROCESS_STATICS` and the
system-variable registry by
`rust/crates/tidb-variable/tests/vardef_classification_appendix.rs`, which
fails when the table and this appendix drift (rerun it with
`UPDATE_MULTI_KEYSPACE_APPENDIX=1` to regenerate). The classification test in
`tidb-vardef` fails when `lib.rs` declares a static the table does not
classify. *Global* statics move into each keyspace runtime's snapshot;
*instance* statics stay process-wide and a tenant cannot set them in
multi-keyspace mode; *internal* statics are not written by a `SetGlobal`
hook. The registry scope is the Rust registry's, which follows Go's: Go keeps
several process-resource variables at `GLOBAL` scope (instance plan cache,
memory alarm, PD client), and the class is what the value governs.

How the global statics are kept per keyspace (`tidb_vardef::snapshot`):

- Each keyspace runtime's `GlobalSysvars` owns a `VardefSnapshot`. Its
  `SetGlobal` hooks write the snapshot (`SessionVars::vardef`) and its
  `GetGlobal` hooks read it. A single-keyspace node, and the configured
  keyspace of a multi-keyspace node, use the *process-backed* snapshot,
  whose accessors are the statics themselves, so they read exactly what they
  read before. Another keyspace of a multi-keyspace node owns its values.
- Readers take the snapshot from the session (`SessionVars::vardef`) or the
  domain (`Domain::vardef`, which background workers such as the TTL job
  manager are handed). Code below the session tiers -- the DDL steps and the
  DDL a statement submits -- reads `tidb_vardef::snapshot::current()`: the
  snapshot its thread entered. A session enters its own while its store half
  is lent (`Session::node_conn`), so every caller -- a connection, a pooled or
  internal session -- runs its statements under its keyspace's values and
  gets its own scope back afterwards; a DDL job enters the snapshot its
  domain gave the DDL's session pool for all of its steps and its
  reorganization. A thread spawned below a session or a DDL job does not
  inherit the scope by itself: it is handed the snapshot explicitly (the
  reorg scan workers' `ScanContext::vardef`, the job-config update and DXF
  modify loops) or runs under `snapshot::inherit` (the reorganization's own
  thread). On a multi-keyspace node, debug builds assert that `current()`
  runs on a thread that entered a snapshot. There is no process-wide
  registry of snapshots.
- *Per keyspace: not yet* marks a global static that other code still
  writes directly; only the cloud storage URI remains, which is a process
  variable (§6.10, phase 3f). The DDL error limit, the analyze default
  sizes and the schema validator's delta count are kept per keyspace;
  metadata locking is an instance setting, since next-generation kernels
  (every multi-keyspace node) always lock.
- A tenant's `SET GLOBAL` / `SET INSTANCE` of an *instance* variable (and of
  a variable Go scopes to the instance alone) is refused with
  `ER_SPECIFIC_ACCESS_DENIED_ERROR` (`RESTRICTED_VARIABLES_ADMIN`) in
  multi-keyspace mode.

<!-- BEGIN generated: vardef classification -->

92 statics: 55 global (54 kept per keyspace so far), 30 instance, 7 internal.

| Static | System variable | Registry scope | Class | Per keyspace | Governs |
| --- | --- | --- | --- | --- | --- |
| `ENABLE_MDL` | `tidb_enable_metadata_lock` | GLOBAL | instance | -- | metadata locking on a classic node (next-generation kernels, every multi-keyspace node among them, always lock) |
| `DDL_FLASHBACK_CONCURRENCY` | `tidb_ddl_flashback_concurrency` | GLOBAL | global | yes | the keyspace's DDL jobs |
| `DDL_REORG_WORKER_COUNTER` | `tidb_ddl_reorg_worker_cnt` | GLOBAL \| SESSION | global | yes | the keyspace's DDL jobs |
| `DDL_REORG_BATCH_SIZE` | `tidb_ddl_reorg_batch_size` | GLOBAL \| SESSION | global | yes | the keyspace's DDL jobs |
| `DDL_ERROR_COUNT_LIMIT` | `tidb_ddl_error_count_limit` | GLOBAL | global | yes | the keyspace's DDL jobs |
| `MAX_AUTO_ANALYZE_TIME` | `tidb_max_auto_analyze_time` | GLOBAL | global | yes | the keyspace's auto-analyze |
| `ENABLE_WORKLOAD_BASED_LEARNING` | -- | -- | internal | -- | no system variable |
| `WORKLOAD_BASED_LEARNING_INTERVAL` | -- | -- | internal | -- | no system variable |
| `ENABLE_FAST_CREATE_TABLE` | `tidb_enable_fast_create_table` | GLOBAL | global | yes | the keyspace's DDL submission |
| `IS_SAND_BOX_MODE_ENABLED` | -- | -- | internal | -- | process config (`security.disconnect-on-expired-password`) |
| `TABLE_CACHE_LEASE` | `tidb_table_cache_lease` | GLOBAL | global | yes | the keyspace's cached tables |
| `ANALYZE_DEFAULT_NUM_BUCKETS` | `tidb_analyze_default_num_buckets` | GLOBAL | global | yes | the keyspace's statistics |
| `ANALYZE_DEFAULT_NUM_TOP_N` | `tidb_analyze_default_num_topn` | GLOBAL | global | yes | the keyspace's statistics |
| `STATS_CACHE_MEM_QUOTA` | `tidb_stats_cache_mem_quota` | GLOBAL \| INSTANCE | instance | -- | process memory |
| `MEMORY_USAGE_ALARM_RATIO` | `tidb_memory_usage_alarm_ratio` | GLOBAL | instance | -- | process memory |
| `FORCE_PRIORITY` | `tidb_force_priority` | INSTANCE | instance | -- | Go `ScopeInstance` |
| `MEMORY_USAGE_ALARM_KEEP_RECORD_NUM` | `tidb_memory_usage_alarm_keep_record_num` | GLOBAL | instance | -- | process memory |
| `PLAN_REPLAYER_FILE_RETENTION_TIME` | -- | -- | internal | -- | no SetGlobal writer |
| `CONNECT_ATTRS_SIZE` | `performance_schema_session_connect_attrs_size` | GLOBAL | global | yes | the keyspace's connections |
| `CONNECT_ATTRS_LONGEST_SEEN` | -- | -- | internal | -- | status counter |
| `CONNECT_ATTRS_LOST` | -- | -- | internal | -- | status counter |
| `MAX_DELTA_SCHEMA_COUNT` | `tidb_max_delta_schema_count` | GLOBAL | global | yes | the keyspace's schema validator |
| `ENABLE_ROW_LEVEL_CHECKSUM` | `tidb_enable_row_level_checksum` | GLOBAL | global | yes | the keyspace's row encoding |
| `ENABLE_FOREIGN_KEY` | `tidb_enable_foreign_key` | GLOBAL | global | yes | the keyspace's DDL policy |
| `ENABLE_CHECK_CONSTRAINT` | `tidb_enable_check_constraint` | GLOBAL | global | yes | the keyspace's DDL policy |
| `ENABLE_FAST_REORG` | `tidb_ddl_enable_fast_reorg` | GLOBAL | global | yes | the keyspace's DDL jobs |
| `SERVICE_SCOPE` | `tidb_service_scope` | INSTANCE | instance | -- | Go `ScopeInstance`: this node's DXF scope |
| `CLOUD_STORAGE_URI` | `tidb_cloud_storage_uri` | GLOBAL | global | not yet | the keyspace's import and DDL storage |
| `DDL_DISK_QUOTA` | `tidb_ddl_disk_quota` | GLOBAL | instance | -- | the node's local disk, which every keyspace's ingest shares |
| `ENABLE_TTL_JOB` | `tidb_ttl_job_enable` | GLOBAL | global | yes | the keyspace's TTL jobs |
| `TTL_SCAN_BATCH_SIZE` | `tidb_ttl_scan_batch_size` | GLOBAL | global | yes | the keyspace's TTL jobs |
| `TTL_DELETE_BATCH_SIZE` | `tidb_ttl_delete_batch_size` | GLOBAL | global | yes | the keyspace's TTL jobs |
| `TTL_DELETE_RATE_LIMIT` | `tidb_ttl_delete_rate_limit` | GLOBAL | global | yes | the keyspace's TTL jobs |
| `TTL_JOB_SCHEDULE_WINDOW_START_TIME` | `tidb_ttl_job_schedule_window_start_time` | GLOBAL | global | yes | the keyspace's TTL jobs |
| `TTL_JOB_SCHEDULE_WINDOW_END_TIME` | `tidb_ttl_job_schedule_window_end_time` | GLOBAL | global | yes | the keyspace's TTL jobs |
| `TTL_SCAN_WORKER_COUNT` | `tidb_ttl_scan_worker_count` | GLOBAL | global | yes | the keyspace's TTL jobs |
| `TTL_DELETE_WORKER_COUNT` | `tidb_ttl_delete_worker_count` | GLOBAL | global | yes | the keyspace's TTL jobs |
| `TTL_RUNNING_TASKS` | `tidb_ttl_running_tasks` | GLOBAL | global | yes | the keyspace's TTL jobs |
| `ENABLE_INSTANCE_PLAN_CACHE` | `tidb_enable_instance_plan_cache` | GLOBAL | instance | -- | the process's plan cache |
| `INSTANCE_PLAN_CACHE_MAX_MEM_SIZE` | `tidb_instance_plan_cache_max_size` | GLOBAL | instance | -- | process memory |
| `INSTANCE_PLAN_CACHE_RESERVED_PERCENTAGE` | `tidb_instance_plan_cache_reserved_percentage` | GLOBAL | instance | -- | process memory |
| `TXN_ENTRY_SIZE_LIMIT` | `tidb_txn_entry_size_limit` | GLOBAL \| SESSION | global | yes | the keyspace's transactions |
| `SCHEMA_CACHE_SIZE` | `tidb_schema_cache_size` | GLOBAL \| INSTANCE | instance | -- | process memory |
| `ENABLE_TSO_FOLLOWER_PROXY` | `tidb_enable_tso_follower_proxy` | GLOBAL | instance | -- | the PD client |
| `ENABLE_PD_FOLLOWER_HANDLE_REGION` | `pd_enable_follower_handle_region` | GLOBAL | instance | -- | the PD client |
| `ENABLE_BATCH_QUERY_REGION` | `tidb_enable_batch_query_region` | GLOBAL | instance | -- | the PD client |
| `MAX_TSO_BATCH_WAIT_INTERVAL` | `tidb_tso_client_batch_max_wait_time` | GLOBAL | instance | -- | the PD client |
| `LOW_RESOLUTION_TSO_UPDATE_INTERVAL` | `tidb_low_resolution_tso_update_interval` | GLOBAL | instance | -- | the PD client |
| `SCHEMA_CACHE_SIZE_ORIGIN_TEXT` | `tidb_schema_cache_size` | GLOBAL \| INSTANCE | instance | -- | process memory (the text `SCHEMA_CACHE_SIZE` was set with) |
| `CIRCUIT_BREAKER_PD_METADATA_ERROR_RATE_THRESHOLD_RATIO` | `tidb_cb_pd_metadata_error_rate_threshold_ratio` | GLOBAL | instance | -- | the PD client |
| `RUN_AUTO_ANALYZE` | `tidb_enable_auto_analyze` | GLOBAL | global | yes | the keyspace's auto-analyze |
| `ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE` | `tidb_enable_auto_analyze_priority_queue` | GLOBAL | global | yes | the keyspace's auto-analyze |
| `AUTO_ANALYZE_CONCURRENCY` | `tidb_auto_analyze_concurrency` | GLOBAL | global | yes | the keyspace's auto-analyze |
| `ENABLE_RESOURCE_CONTROL` | `tidb_enable_resource_control` | GLOBAL | instance | -- | the process TiKV client's resource-control switch |
| `MAX_USER_CONNECTIONS_VALUE` | `max_user_connections` | GLOBAL | global | yes | the keyspace's accounts |
| `ACCELERATE_USER_CREATION_UPDATE` | `tidb_accelerate_user_creation_update` | GLOBAL | global | yes | the keyspace's privilege cache |
| `ENABLE_RESOURCE_CONTROL_STRICT_MODE` | `tidb_resource_control_strict_mode` | GLOBAL | global | yes | the keyspace's resource groups |
| `OOM_ACTION` | `tidb_mem_oom_action` | GLOBAL | global | yes | the keyspace's statements |
| `GOGC_TUNER_THRESHOLD` | `tidb_gogc_tuner_threshold` | GLOBAL | instance | -- | process memory |
| `PLAN_REPLAYER_GC_LEASE` | -- | -- | internal | -- | process internal |
| `TI_FLASH_ENABLE_PIPELINE_MODE` | `tidb_enable_tiflash_pipeline_model` | GLOBAL | global | yes | the keyspace's TiFlash requests |
| `PROCESS_GENERAL_LOG` | `tidb_general_log` | INSTANCE | instance | -- | Go `ScopeInstance`: the process's general log |
| `ENABLE_RC_READ_CHECK_TS` | `tidb_rc_read_check_ts` | INSTANCE | instance | -- | Go `ScopeInstance` |
| `PERSIST_ANALYZE_OPTIONS` | `tidb_persist_analyze_options` | GLOBAL | global | yes | the keyspace's statistics |
| `STATS_LOAD_PSEUDO_TIMEOUT` | `tidb_stats_load_pseudo_timeout` | GLOBAL | global | yes | the keyspace's statistics |
| `ENABLE_BATCH_DML` | `tidb_enable_batch_dml` | GLOBAL | global | yes | the keyspace's statements |
| `ENABLE_NOOP_VARIABLES` | `tidb_enable_noop_variables` | GLOBAL | global | yes | the keyspace's sessions |
| `ENABLE_TMP_STORAGE_ON_OOM` | `tidb_enable_tmp_storage_on_oom` | GLOBAL | global | yes | the keyspace's statements |
| `ENABLE_BINDING_USAGE` | `tidb_enable_binding_usage` | GLOBAL | global | yes | the keyspace's bindings |
| `M_LOG_LOG_SLOW_PURGE` | `tidb_mlog_log_slow_purge` | GLOBAL | global | yes | the keyspace's materialized views |
| `IGNORE_INLIST_PLAN_DIGEST` | `tidb_ignore_inlist_plan_digest` | GLOBAL | global | yes | the keyspace's statement digests |
| `ENABLE_CONNECTION_EVENT_LOG` | `tidb_enable_connection_event_log` | GLOBAL | instance | -- | the process's log |
| `ENABLE_DIST_TASK` | `tidb_enable_dist_task` | GLOBAL | global | yes | the keyspace's DXF tasks |
| `PASSWORD_VALIDATION_LENGTH` | `validate_password.length` | GLOBAL | global | yes | the keyspace's accounts |
| `PASSWORD_VALIDATION_MIXED_CASE_COUNT` | `validate_password.mixed_case_count` | GLOBAL | global | yes | the keyspace's accounts |
| `PASSWORD_VALIDTAION_NUMBER_COUNT` | `validate_password.number_count` | GLOBAL | global | yes | the keyspace's accounts |
| `PASSWORD_VALIDATION_SPECIAL_CHAR_COUNT` | `validate_password.special_char_count` | GLOBAL | global | yes | the keyspace's accounts |
| `PASSWORD_HISTORY` | `password_history` | GLOBAL | global | yes | the keyspace's accounts |
| `PASSWORD_REUSE_INTERVAL` | `password_reuse_interval` | GLOBAL | global | yes | the keyspace's accounts |
| `MEM_QUOTA_BINDING_CACHE` | `tidb_mem_quota_binding_cache` | GLOBAL \| INSTANCE | instance | -- | process memory |
| `STATS_LOAD_SYNC_WAIT` | `tidb_stats_load_sync_wait` | GLOBAL \| SESSION | global | yes | the keyspace's statistics |
| `QUERY_LOG_MAX_LEN` | `tidb_query_log_max_len` | GLOBAL | instance | -- | the process's log |
| `PAGING_SIZE_BYTES` | `tidb_paging_size_bytes` | GLOBAL \| SESSION | global | yes | the keyspace's coprocessor requests |
| `EXPENSIVE_QUERY_TIME_THRESHOLD` | `tidb_expensive_query_time_threshold` | INSTANCE | instance | -- | Go `ScopeInstance`: the process's watchdog |
| `EXPENSIVE_TXN_TIME_THRESHOLD` | `tidb_expensive_txn_time_threshold` | INSTANCE | instance | -- | Go `ScopeInstance`: the process's watchdog |
| `ENABLE_P_PROF_SQLCPU` | `tidb_pprof_sql_cpu` | INSTANCE | instance | -- | Go `ScopeInstance`: the process's profiler |
| `PREPARED_PLAN_CACHE_MEMORY_GUARD_RATIO` | `tidb_prepared_plan_cache_memory_guard_ratio` | GLOBAL | instance | -- | process memory |
| `ANALYZE_COLUMN_OPTIONS` | `tidb_analyze_column_options` | GLOBAL | global | yes | the keyspace's statistics |
| `ADVANCER_CHECK_POINT_LAG_LIMIT` | `tidb_advancer_check_point_lag_limit` | GLOBAL | global | yes | the keyspace's log backup |
| `ENABLE_LOCAL_TXN` | `tidb_enable_local_txn` | GLOBAL | global | yes | the keyspace's transactions |
| `DDL_REORG_MAX_WRITE_SPEED` | `tidb_ddl_reorg_max_write_speed` | GLOBAL | global | yes | the keyspace's DDL jobs |
| `DDL_SLOW_OPR_THRESHOLD` | `ddl_slow_threshold` | INSTANCE | instance | -- | Go `ScopeInstance`: the process's log |

<!-- END generated: vardef classification -->

#### Appendix to §6.3: where the `SetGlobal` hooks write

Generated from `tidb_variable::process_scope::HOOK_DESTINATIONS` by the same
test; a `tidb-variable` unit test fails when a registry variable gains a
`SetGlobal` hook the table does not classify. A *keyspace* hook writes only
the keyspace's vardef snapshot (or nothing). A *process* hook writes state the
whole process shares (another crate's static, collector or client, the
process configuration, a vardef static not kept per keyspace): its variable is
a process variable. In multi-keyspace mode a tenant's `SET GLOBAL` / `SET
INSTANCE` of it is refused (`ER_SPECIFIC_ACCESS_DENIED_ERROR`), and a keyspace
that owns its vardef values never runs its hook, neither for a write nor for a
value its cache rebuild loads, so a tenant's stored value cannot reach the
process. Hooks other packages register on a domain (GC, materialized views,
the workload repository) act on that domain's keyspace and are not listed.
The process settings `setGlobalVars` seeds directly are seeded once per node,
by the configured keyspace's runtime; a later keyspace's open seeds only its
own registry. Likewise only the table over the process statics binds the
process's slots to its store -- the PD client option callbacks and the
external-timestamp slot -- and applies `tidb_cloud_storage_uri`. PD keeps
one external timestamp per cluster, not per keyspace (the client sends only
the cluster ID), so `tidb_external_ts` is a process variable: a tenant reads
the cluster's value and cannot set it, and its cache rebuild never writes it.

<!-- BEGIN generated: set-global hook destinations -->

145 variables with a `SetGlobal` hook: 55 keyspace, 90 process.

| System variable | Registry scope | Hook writes | Class |
| --- | --- | --- | --- |
| `authentication_ldap_sasl_auth_method_name` | GLOBAL | the process LDAP authentication | process |
| `authentication_ldap_sasl_bind_base_dn` | GLOBAL | the process LDAP authentication | process |
| `authentication_ldap_sasl_bind_root_dn` | GLOBAL | the process LDAP authentication | process |
| `authentication_ldap_sasl_bind_root_pwd` | GLOBAL | the process LDAP authentication | process |
| `authentication_ldap_sasl_ca_path` | GLOBAL | the process LDAP authentication | process |
| `authentication_ldap_sasl_init_pool_size` | GLOBAL | the process LDAP authentication | process |
| `authentication_ldap_sasl_max_pool_size` | GLOBAL | the process LDAP authentication | process |
| `authentication_ldap_sasl_server_host` | GLOBAL | the process LDAP authentication | process |
| `authentication_ldap_sasl_server_port` | GLOBAL | the process LDAP authentication | process |
| `authentication_ldap_sasl_tls` | GLOBAL | the process LDAP authentication | process |
| `authentication_ldap_sasl_user_search_attr` | GLOBAL | the process LDAP authentication | process |
| `authentication_ldap_simple_auth_method_name` | GLOBAL | the process LDAP authentication | process |
| `authentication_ldap_simple_bind_base_dn` | GLOBAL | the process LDAP authentication | process |
| `authentication_ldap_simple_bind_root_dn` | GLOBAL | the process LDAP authentication | process |
| `authentication_ldap_simple_bind_root_pwd` | GLOBAL | the process LDAP authentication | process |
| `authentication_ldap_simple_ca_path` | GLOBAL | the process LDAP authentication | process |
| `authentication_ldap_simple_init_pool_size` | GLOBAL | the process LDAP authentication | process |
| `authentication_ldap_simple_max_pool_size` | GLOBAL | the process LDAP authentication | process |
| `authentication_ldap_simple_server_host` | GLOBAL | the process LDAP authentication | process |
| `authentication_ldap_simple_server_port` | GLOBAL | the process LDAP authentication | process |
| `authentication_ldap_simple_tls` | GLOBAL | the process LDAP authentication | process |
| `authentication_ldap_simple_user_search_attr` | GLOBAL | the process LDAP authentication | process |
| `ddl_slow_threshold` | INSTANCE | the process vardef static `DDL_SLOW_OPR_THRESHOLD` | process |
| `max_connections` | INSTANCE | the process configuration | process |
| `max_user_connections` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `password_history` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `password_reuse_interval` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `pd_enable_follower_handle_region` | GLOBAL | the process vardef static `ENABLE_PD_FOLLOWER_HANDLE_REGION` | process |
| `performance_schema_session_connect_attrs_size` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `require_secure_transport` | GLOBAL | the process TLS transport policy | process |
| `tidb_accelerate_user_creation_update` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_advancer_check_point_lag_limit` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_analyze_column_options` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_analyze_default_num_buckets` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_analyze_default_num_topn` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_auto_analyze_concurrency` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_cb_pd_metadata_error_rate_threshold_ratio` | GLOBAL | the process vardef static `CIRCUIT_BREAKER_PD_METADATA_ERROR_RATE_THRESHOLD_RATIO` | process |
| `tidb_check_mb4_value_in_utf8` | INSTANCE | the process configuration | process |
| `tidb_committer_concurrency` | GLOBAL | the process TiKV client | process |
| `tidb_ddl_disk_quota` | GLOBAL | the process vardef static `DDL_DISK_QUOTA` | process |
| `tidb_ddl_enable_fast_reorg` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_ddl_error_count_limit` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_ddl_flashback_concurrency` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_ddl_reorg_batch_size` | GLOBAL \| SESSION | the keyspace vardef snapshot | keyspace |
| `tidb_ddl_reorg_max_write_speed` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_ddl_reorg_worker_cnt` | GLOBAL \| SESSION | the keyspace vardef snapshot | keyspace |
| `tidb_enable_auto_analyze` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_enable_auto_analyze_priority_queue` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_enable_batch_dml` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_enable_batch_query_region` | GLOBAL | the process vardef static `ENABLE_BATCH_QUERY_REGION` | process |
| `tidb_enable_binding_usage` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_enable_check_constraint` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_enable_collect_execution_info` | INSTANCE | the process configuration | process |
| `tidb_enable_connection_event_log` | GLOBAL | the process vardef static `ENABLE_CONNECTION_EVENT_LOG` | process |
| `tidb_enable_ddl` | INSTANCE | the process configuration | process |
| `tidb_enable_dist_task` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_enable_fast_create_table` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_enable_foreign_key` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_enable_gc_aware_memory_track` | GLOBAL | the process memory tuner and arbitrator | process |
| `tidb_enable_instance_plan_cache` | GLOBAL | the process vardef static `ENABLE_INSTANCE_PLAN_CACHE` | process |
| `tidb_enable_local_txn` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_enable_noop_variables` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_enable_resource_control` | GLOBAL | the process TiKV client resource-control switch | process |
| `tidb_enable_row_level_checksum` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_enable_slow_log` | INSTANCE | the process configuration | process |
| `tidb_enable_stats_owner` | INSTANCE | the process configuration | process |
| `tidb_enable_stmt_summary` | GLOBAL | the process statement summary | process |
| `tidb_enable_tiflash_pipeline_model` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_enable_tmp_storage_on_oom` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_enable_ts_validation` | GLOBAL | the process TiKV client | process |
| `tidb_enable_tso_follower_proxy` | GLOBAL | the process vardef static `ENABLE_TSO_FOLLOWER_PROXY` | process |
| `tidb_expensive_query_time_threshold` | INSTANCE | the process vardef static `EXPENSIVE_QUERY_TIME_THRESHOLD` | process |
| `tidb_expensive_txn_time_threshold` | INSTANCE | the process vardef static `EXPENSIVE_TXN_TIME_THRESHOLD` | process |
| `tidb_external_ts` | GLOBAL | the PD external timestamp, which is cluster-scoped, not keyspace-scoped | process |
| `tidb_force_priority` | INSTANCE | the process vardef static `FORCE_PRIORITY` | process |
| `tidb_general_log` | INSTANCE | the process vardef static `PROCESS_GENERAL_LOG` | process |
| `tidb_generate_binary_plan` | GLOBAL | the process log and trace | process |
| `tidb_gogc_tuner_max_value` | GLOBAL | the process memory tuner and arbitrator | process |
| `tidb_gogc_tuner_min_value` | GLOBAL | the process memory tuner and arbitrator | process |
| `tidb_gogc_tuner_threshold` | GLOBAL | the process vardef static `GOGC_TUNER_THRESHOLD` | process |
| `tidb_ignore_inlist_plan_digest` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_instance_plan_cache_max_size` | GLOBAL | the process vardef static `INSTANCE_PLAN_CACHE_MAX_MEM_SIZE` | process |
| `tidb_instance_plan_cache_reserved_percentage` | GLOBAL | the process vardef static `INSTANCE_PLAN_CACHE_RESERVED_PERCENTAGE` | process |
| `tidb_log_file_max_days` | INSTANCE | the process configuration | process |
| `tidb_low_resolution_tso_update_interval` | GLOBAL | the process vardef static `LOW_RESOLUTION_TSO_UPDATE_INTERVAL` | process |
| `tidb_max_auto_analyze_time` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_max_delta_schema_count` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_mem_arbitrator_mode` | GLOBAL | the process memory tuner and arbitrator | process |
| `tidb_mem_arbitrator_soft_limit` | GLOBAL | the process memory tuner and arbitrator | process |
| `tidb_mem_oom_action` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_mem_quota_analyze` | GLOBAL | the process memory tuner and arbitrator | process |
| `tidb_mem_quota_binding_cache` | GLOBAL \| INSTANCE | the process vardef static `MEM_QUOTA_BINDING_CACHE` | process |
| `tidb_memory_usage_alarm_keep_record_num` | GLOBAL | the process vardef static `MEMORY_USAGE_ALARM_KEEP_RECORD_NUM` | process |
| `tidb_memory_usage_alarm_ratio` | GLOBAL | the process vardef static `MEMORY_USAGE_ALARM_RATIO` | process |
| `tidb_mlog_log_slow_purge` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_paging_size_bytes` | GLOBAL \| SESSION | the keyspace vardef snapshot | keyspace |
| `tidb_partition_prune_mode` | GLOBAL \| SESSION | none: a statement warning | keyspace |
| `tidb_persist_analyze_options` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_pprof_sql_cpu` | INSTANCE | the process vardef static `ENABLE_P_PROF_SQLCPU` | process |
| `tidb_prepared_plan_cache_memory_guard_ratio` | GLOBAL | the process vardef static `PREPARED_PLAN_CACHE_MEMORY_GUARD_RATIO` | process |
| `tidb_query_log_max_len` | GLOBAL | the process vardef static `QUERY_LOG_MAX_LEN` | process |
| `tidb_rc_read_check_ts` | INSTANCE | the process vardef static `ENABLE_RC_READ_CHECK_TS` | process |
| `tidb_record_plan_in_slow_log` | INSTANCE | the process configuration | process |
| `tidb_redact_log` | GLOBAL \| SESSION | the process log and trace | process |
| `tidb_resource_control_strict_mode` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_schema_cache_size` | GLOBAL \| INSTANCE | the process vardef static `SCHEMA_CACHE_SIZE_ORIGIN_TEXT` | process |
| `tidb_server_memory_limit` | GLOBAL \| INSTANCE | the process memory tuner and arbitrator | process |
| `tidb_server_memory_limit_gc_trigger` | GLOBAL \| INSTANCE | the process memory tuner and arbitrator | process |
| `tidb_server_memory_limit_sess_min_size` | GLOBAL | the process memory tuner and arbitrator | process |
| `tidb_service_scope` | INSTANCE | the process vardef static `SERVICE_SCOPE` | process |
| `tidb_slow_log_max_per_sec` | GLOBAL | the process log and trace | process |
| `tidb_slow_log_rules` | GLOBAL \| SESSION | the process log and trace | process |
| `tidb_slow_log_threshold` | INSTANCE | the process configuration | process |
| `tidb_stats_cache_mem_quota` | GLOBAL \| INSTANCE | the process vardef static `STATS_CACHE_MEM_QUOTA` | process |
| `tidb_stats_load_pseudo_timeout` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_stats_load_sync_wait` | GLOBAL \| SESSION | the keyspace vardef snapshot | keyspace |
| `tidb_stmt_summary_group_by_user` | GLOBAL | the process statement summary | process |
| `tidb_stmt_summary_history_size` | GLOBAL | the process statement summary | process |
| `tidb_stmt_summary_internal_query` | GLOBAL | the process statement summary | process |
| `tidb_stmt_summary_max_sql_length` | GLOBAL | the process statement summary | process |
| `tidb_stmt_summary_max_stmt_count` | GLOBAL \| INSTANCE | the process statement summary | process |
| `tidb_stmt_summary_persist_evicted` | GLOBAL | the process statement summary | process |
| `tidb_stmt_summary_refresh_interval` | GLOBAL | the process statement summary | process |
| `tidb_store_limit` | GLOBAL | the process TiKV client | process |
| `tidb_table_cache_lease` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_top_sql_max_meta_count` | GLOBAL | the process Top SQL collector | process |
| `tidb_top_sql_max_time_series_count` | GLOBAL | the process Top SQL collector | process |
| `tidb_trace_event` | INSTANCE | the process log and trace | process |
| `tidb_tso_client_batch_max_wait_time` | GLOBAL | the process vardef static `MAX_TSO_BATCH_WAIT_INTERVAL` | process |
| `tidb_tso_client_rpc_mode` | GLOBAL | the process PD client | process |
| `tidb_ttl_delete_batch_size` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_ttl_delete_rate_limit` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_ttl_delete_worker_count` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_ttl_job_enable` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_ttl_job_schedule_window_end_time` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_ttl_job_schedule_window_start_time` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_ttl_running_tasks` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_ttl_scan_batch_size` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_ttl_scan_worker_count` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_txn_commit_batch_size` | GLOBAL | the process TiKV client | process |
| `tidb_txn_entry_size_limit` | GLOBAL \| SESSION | the keyspace vardef snapshot | keyspace |
| `validate_password.length` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `validate_password.mixed_case_count` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `validate_password.number_count` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `validate_password.special_char_count` | GLOBAL | the keyspace vardef snapshot | keyspace |

<!-- END generated: set-global hook destinations -->

### 6.4 Caches keyed by table or job ID

Table and job IDs are per keyspace, so these caches collide across tenants:

- `TABLE_ROW_STATS_CACHE` (`stats_table_row_cache.rs:61`). **Done:** one
  per domain (`Domain::table_row_stats`); the process static is gone.
- the stats async-load and sync-load queues (`tidb-stats-asyncload/src/lib.rs:26`,
  `sync_load.rs:75`). **Done for sync load:** its single-flight map is the
  domain pool's (`StatisticsLoadWorkers`), so one keyspace's request never
  joins another's load of the same ID. **Done for async load:** the demand map is
  the domain pool's too (`StatisticsLoadWorkers::queue_async_load`,
  `async_load_items`, `finish_async_load`): the planner's statistics
  collection and the sync-load fallback reach it through their statistics
  handle's loaders (`StatsHandle::sync_load_service`), and the domain's
  asynchronous loader drains its own (`Domain::load_workers`). The process
  static is gone.
- the auto-analyze process ID generator (`auto_analyze_proc_id_generator.rs:101`).
  **Done in Phase 2** (§6.6, step 6): IDs come from the one process
  allocator, so they are unique in the process.
- `JOB_CTX_MAP` (`tidb-ddl/src/job_ctx.rs:129`). **Done:** keyed by
  `JobKey` (the keyspace ID of the store the DDL runs on, and the job ID).
  Go's map is the DDL's own; its callers here reach no DDL handle, so each
  names its keyspace from what it has: the step's session pool
  (`DdlSessionPool::keyspace_id`, through `DdlStepWorker::job_key` and
  `JobTxn::job_key`) or the reorganization's transaction opener.
- `JOB_VER_IN_USE` and `GLOBAL_INDEX_V1_SUPPORTED` (`tidb-model/src/job_enums.rs:210`,
  `tidb-model/src/index.rs:84`). **Not per tenant:** only the classic
  kernel's DDL detects them from its peers' server-info versions
  (`worker.rs`, `detect_and_update_job_version`, Go's `ddl.Start` behind
  `kerneltype.IsClassic()`). A multi-keyspace node is next-generation,
  where no DDL writes them, so every tenant reads the process's one value
  (a test opens two keyspaces and runs their DDL and finds it unchanged).
  Go's next-generation kernel fixes them at version 2 and V1 supported in
  its packages' `init`; this port's stay at the classic initial values
  (version 1, not supported) on next-generation, a Go-parity gap to close
  outside this phase.

Each moves into the runtime's `Domain`, or is re-keyed by `(keyspace_id, id)`.
Moving it into the `Domain` is preferred, because it closes with the tenant.

### 6.5 Transactions and GC

- `GLOBAL_INNER_TXN_START_TS` and `ACTIVE_START_TS` (`tidb-txnkv/src/inner_txn.rs:28,153`)
  feed the min-start-ts report used for GC safe points. TSO can differ per
  keyspace group, so these become a map keyed by keyspace ID, and each
  runtime's reporter reads only its own entry. **Done:** a transaction, a
  snapshot read and an inner transaction (`NewTxnStorage::keyspace_id`) are
  held under the keyspace ID of the store they read. A statement result or
  cursor kept past its transaction does not know its store, so it holds a
  shared entry that every report reads (as a single-keyspace node does).
- **Live GC gate** (`rust/scripts/multi-tenant-gc-gate.py`, passed on the
  development playground). Its evidence is the GC safe point PD stores for
  each keyspace (keyspace-level GC: `/pd/<cluster>/keyspaces/gc_safe_point/
  <keyspace id>`, what a completed GC job's `AdvanceGCSafePoint`
  publishes), against the reader's full start TSO; the `mysql.tidb` GC rows
  are written before a job runs and prove nothing. Times below are labelled:
  a TSO's *physical time* is its value, decoded; an *observed* time is when
  the script saw the event (local clock). Both tenants' rows were
  overwritten and deleted after the reader began, and each tenant ran a DDL
  every two minutes (so the playground's older node, which also serves both
  keyspaces and reports its keyspace's last DDL, holds neither keyspace back
  before the reader). In event order:
  1. `keyspace1`'s reader began at TSO 470704065376157703 (physical time
     15:58:31.109).
  2. `keyspace2`'s PD safe point moved to 470704097214857216 (physical time
     16:00:32.564), past the reader; observed in a poll before step 3's
     (that run did not log the observed time).
  3. `keyspace1`'s PD safe point moved to 470704065376157703 (physical time
     15:58:31.109), the reader's TSO exactly; observed in the poll that
     began at `keyspace1` TSO 470704273834901505 (physical time
     16:11:46.316), when that TSO minus the 10m life time was already 195s
     past the reader. Only the reader held it back, and it never passed the
     reader while the reader was open.
  4. The reader still read its original rows; it then rolled back, right
     after step 3's poll (not logged; estimated at about 16:12:16, the
     poll's 30s sleep after it began).
  5. `keyspace1`'s PD safe point moved to 470704252523642880 (physical time
     16:10:25.020, consistent with a job at about 16:20:25 minus the 10m
     life time), past the reader; observed (not logged) before the run's
     log file was last written, at 16:22:32.
  The script now logs every observed time, so a future run records each
  step's. Its assertions are stricter than when it ran: B's PD safe point
  must pass both the reader and its own point at the reader's start, and A's
  must reach the reader's TSO exactly while the reader is open. The run
  satisfies them: `--replay` of its saved output passes, and `--self-test`
  replays it beside a trace that must fail (B above the reader from the
  start and never moving, A rising only to 20s below the reader).
- `DEFAULT_RESOURCE_GROUP` (`tidb-meta/src/transaction.rs:59`) moves per runtime.
  **Not needed:** it only memoizes Go's `DefaultGroupMeta4Test`, an
  immutable value that is the same in every keyspace; each keyspace's own
  `default` group is read from its meta.
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
   still held keep serving their existing connections while the process has
   no ID. Once it holds the fresh one, each runtime drains its connections,
   which carry the old ID, and reserves the new ID in its tenant's namespace.
   A runtime's connections and its published ID therefore always agree.
5. **ID space.** With `enable-32bits-connection-id`, the server ID has 11 bits,
   so at most 2,047 IDs. Multi-keyspace mode requires 64-bit connection IDs, so
   that reserving one ID across many namespaces stays easy. A node with
   `enable-32bits-connection-id = true` (the default) does not start in
   multi-keyspace mode.
6. **One allocator.** Every runtime's connections and internal processes
   (auto-analyze among them) draw their connection IDs from one process
   allocator. With the same server ID, one allocator per runtime would hand
   out the same IDs in different runtimes. The shared allocator keeps a
   connection ID unique in the process, so the process-wide
   `GLOBAL_AUTO_ANALYZE_PROCESS_LIST` cannot take one tenant's session for
   another tenant's auto-analyze, in the expensive-query watchdog or in the
   auto-analyze window check.

`KILL <id>` and `SHOW PROCESSLIST` resolve only connections of the caller's
keyspace. A connection ID from another tenant has this node's server ID, so
`KILL` looks it up locally, in the caller's runtime only. It behaves exactly
like a nonexistent one: `KILL` answers OK and does nothing (Go ignores an
unknown ID), and `EXPLAIN FOR CONNECTION` answers `ER_NO_SUCH_THREAD`.

**Status (development mode).** Steps 1 to 6 are implemented:

- The process reservation is `ProcessServerId` (`tidb-server/src/keyspace_runtime.rs`).
  It reserves a candidate under SYSTEM's `/keyspaces/tidb/<id>/tidb/server_id/`
  before the configured keyspace opens.
- Each runtime open, in its bootstrap domain and in its serving domain,
  reserves the ID with `ServerIdAuthority::new_tenant_reservation` and
  `ServerIdKeeper::start_reserved` (`tidb-domain/src/server_id.rs`). This runs
  before the server-info syncer starts, so a collision fails the open before
  anything is published. The error names the node whose server-info record
  holds the ID and this node. The registry logs it, caches `Failed` for 10s,
  and answers 1045.
- A lost tenant lease kills the runtime's registered connections, and routing
  refuses the keyspace with 1045 until its keeper reserves the ID again.
  Routing also refuses a runtime that still holds an ID the process no longer
  greets with. A tenant keeper wakes every 10s (the retry interval) to follow
  its process's ID, and refreshes its lease every 5 minutes as Go does.
- A connection is admitted against the ID it was greeted with. The greeting
  records the SYSTEM reservation's loss count as the connection ID is
  allocated. Routing takes an `AdmissionTicket` (that greeting, and the
  tenant reservation's ID and loss count), and the connection's process
  registration (opening its session) runs under it, holding both
  reservations' admission locks: a SYSTEM loss since the greeting (even one
  recovered onto the same number), or a drain or a SYSTEM rotation since
  routing, fails the login with 1045, and nothing is registered. A loss
  waits for a registration in progress; a tenant's then retires it with
  the rest.
- A fresh tenant claim counts as a successful refresh, so one transient
  refresh error loses nothing. A keeper that lost admission while its lease
  lived recognizes its own claim (the key's lease is its own) and keeps it.
  Recovery reopens admission only once the server info is published; while
  publication fails it keeps the claim, refreshed, with admission closed.
- The mixed-fleet checks are unit tests over a mock etcd with forced
  candidates: both opening orders, and tenant and SYSTEM lease loss. The
  failpoint `github.com/pingcap/tidb/pkg/domain/forceServerIDCandidate`
  (`return(<id>)`, set at startup through `GO_FAILPOINTS` in a `failpoints`
  build) forces a real node's first candidate. A remote `KILL` into a
  multi-keyspace node is refused for now: the request names no keyspace
  (§6.8).
- The two-node run is `rust/scripts/mixed-fleet-server-id.py`, over a
  `failpoints` + `nextgen` build and an existing TiDB X cluster. It passed
  (32 checks) on the development playground, with `keyspace2` as the shared
  tenant: whichever node opens the tenant second, the multi-keyspace node
  refuses the tenant's logins with 1045 and publishes nothing there, and a
  single-keyspace node moves off the ID (Go's collision retry); `KILL` and
  `KILL QUERY` from the multi-keyspace node reach the single-keyspace node,
  and from another tenant of the multi-keyspace node do not; the other
  direction answers OK with the warning `KILL remote connection failed: ...
  answers peer requests of its configured keyspace's users only` and kills
  nothing (3f); every lease (SYSTEM, both tenants', the single-keyspace
  node's) is renewed by its keeper's refresh; and revoking the
  single-keyspace node's lease and the multi-keyspace node's tenant lease
  drains both within one refresh interval (about 260s), while the
  multi-keyspace node's other tenant keeps its connections, and both reserve
  again within the 10s retry (the multi-keyspace node on the same ID). The
  SYSTEM lease loss has not been run live (unit-tested only).

**Mixed-fleet test.** One multi-keyspace node and one single-keyspace
**nextgen** node serve the same tenant, with a failpoint that forces
overlapping candidate IDs. Check:

- the second node to open the tenant fails with the collision error, and never
  publishes the ID;
- remote `KILL` and `KILL QUERY` work in both directions (into a
  multi-keyspace node: refused until 3f);
- lease expiry and renewal on both nodes, including a tenant-lease loss on the
  multi-keyspace node that drains only that tenant.

### 6.7 Decisions taken before the tenant is known

- **Greeting auth plugin.** Today it comes from the keyspace sysvar
  `default_authentication_plugin`. In multi-keyspace mode it comes from
  process config. If the tenant's account uses another plugin, the existing
  AuthSwitch path handles it.
- **`require_secure_transport`.** It is checked after routing, against the
  tenant's sysvar. `admit_transport` already reads it from `users`, which is now
  the tenant's. *Gap:* the policy itself is still the process static
  `REQUIRE_SECURE_TRANSPORT` (`tidb-tls/src/lib.rs:28`), so today it is a
  process variable that tenants cannot set (§6.3). Phase 2 makes the check
  read the tenant's value.
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
| `SHOW STATUS` | one process provider list (`tidb-variable/src/statusvar.rs:50`); every domain and GC worker registers into it, a later provider wins on name | One provider list per runtime |

**Status (development mode, phase 3e).** The rule: a process-wide owner is
keyed by keyspace or filtered at read time by the session's keyspace, never by
a numeric ID alone (table and connection IDs collide across tenants). The
least invasive tenant-correct choice was taken per surface; the process-wide
owners stay, so a single-keyspace node is unchanged. A session reads its
keyspace through `ViewerState::keyspace`, which is `None` on a
single-keyspace node.

- **Statement summary v1:** one process map, keyed by keyspace
  (`StmtDigestKey::append_keyspace`); each record keeps its keyspace and the
  reader reads the session's only, in the current, history, `CLUSTER_` and
  cumulative (`TIDB_STATEMENTS_STATS`) tables. The evicted rollup sums every
  keyspace's evicted statements, so `STATEMENTS_SUMMARY_EVICTED` and the
  rollup row read as empty. The capacity (`tidb_stmt_summary_max_stmt_count`)
  is shared: one tenant can evict another's (noisy neighbour, not a leak).
- **Statement summary v2 (persistent):** refused (1235); its memory and files
  have no keyspace key.
- **Slow log:** one file, each entry with `Keyspace_name`; `slow_query` reads
  the session's keyspace's entries, so an entry with no keyspace reads for
  none (this also covers a session that points `tidb_slow_query_file`
  elsewhere). `ADMIN SHOW SLOW` keeps one top-N memory per keyspace.
- **Deadlocks:** each record carries the keyspace of the store whose
  transaction detected it (a wait cycle never crosses keyspaces: their keys
  are disjoint), and `DEADLOCKS` reads the session's. The capacity is shared.
- **`DATA_LOCK_WAITS`:** TiKV answers `GetLockWaitInfo` with every
  keyspace's waits; an API-v2 store keeps its keyspace's, with the prefix
  decoded, and drops the rest (this also fixes the raw keys a single-keyspace
  nextgen node showed).
- **`TRX_SUMMARY`:** each domain's process list records into its own fork of
  the process recorder, which follows the process's capacity and minimum
  duration. `TIDB_TRX` and the processlist were per runtime already (§6.6).
- **`SHOW STATUS`:** the provider list is keyed by keyspace (each domain's
  and GC worker's providers carry their keyspace); a session reads the
  process providers and its keyspace's. A domain registers each provider
  under its keyspace from the first registration (its store's keyspace
  until its runtime binds its identity), so an opening runtime's DDL
  statuses are never briefly every keyspace's.
- **`CLUSTER_*` tables:** read this node only. Every keyspace's server info
  names the one status port, whose peer service answers for the configured
  keyspace and cannot tell which keyspace asks. That service, on a
  multi-keyspace node, answers only requests whose user carries the
  configured keyspace's prefix; a request with no user (a remote `KILL`) or
  another keyspace's user is refused, so a remote `KILL` into a
  multi-keyspace node does not work yet (3f, status port multiplexing).
- **Refused (1235):** METRICS_SCHEMA (and the metric-backed summaries and
  inspections), `CLUSTER_LOG` (the node's log holds every keyspace's records),
  `TIDB_HOT_REGIONS` and `TIDB_HOT_REGIONS_HISTORY` (PD's regions matched to
  the catalog by table ID; 3b), the `PERFORMANCE_SCHEMA` profile tables (the
  process's and the cluster's), `FLUSH CLIENT_ERRORS_SUMMARY`.
- **Empty:** `CLIENT_ERRORS_SUMMARY_GLOBAL`, `_BY_USER` and `_BY_HOST` (the
  process's error counters), `MEMORY_USAGE_OPS_HISTORY` (the process's memory
  kills, with other tenants' statements).
- **Left as they are:** `MEMORY_USAGE`, `CLUSTER_INFO`, `CLUSTER_CONFIG`,
  `CLUSTER_LOAD`, `CLUSTER_HARDWARE`, `CLUSTER_SYSTEMINFO`,
  `TIKV_STORE_STATUS`: infrastructure the tenants share, not another tenant's
  data (Starter hides them with SEM). `TIDB_INDEX_USAGE`, the processlist and
  `TIDB_TRX` are per domain.
- **Deferred:** Top SQL (not SQL-readable; its reports name one keyspace),
  metrics keyspace labels (§5.5), the v2 statement summary, peer requests
  that name their keyspace (3f), and the TiFlash system tables (not checked
  yet; with TiFlash, §6.2).

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
- the DXF task managers `TASK_MANAGER_INSTANCE` and `DXF_SVC_TASK_MGR`
  (`tidb-dxf-storage/src/lib.rs:300,304`), which every runtime open overwrites

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
- **End-to-end harness: "move tenant A while tenant B stays put"** (proposed
  by Sig-Codex; it is the release gate for Phases 3g and 3h). It extends Rust
  TiProxy's live migration runner,
  `tests/dataplane/integration/standalone-mig01.sh` @ `be937b9dcd`.
  1. `A.app` and `B.app` connect through the same proxy port to the same
     multi-keyspace process. Both tenants have the same table and primary key
     with different row values. Each client prepares a binary-protocol
     statement, with auto-reconnect and re-prepare disabled.
  2. Drain only A. A's backend must change and B's must not. Both clients run
     their original statement handles on their original sockets and read
     their own tenant's row. This catches a no-op drain, a hidden reconnect
     and a wrong-tenant route.
  3. Repeat with A in a transaction:
     - A stays on the source until commit, then migrates;
     - its write lands exactly once in A and never in B, verified
       independently;
     - also cover the case where the only available destination serves B
       only. The migration must be refused, and A keeps its source.

  Topology, inputs, expected results, and per-PR versus nightly coverage for
  both repositories are in Sig-Codex's harness plan
  (Raft artifact in #Rust-TiDB-Multi-Tenants).
- **Mixed fleet.** A multi-keyspace node and a single-keyspace **nextgen** node
  serve the same keyspace at the same time. Check:
  - DDL ownership handoff and schema version sync;
  - server-ID reservation and remote `KILL` (§6.6);
  - registry lifecycle (§5.3).

## 10. Plan

Each phase is shippable. Phase 1b is independent of Phase 1 and can run in
parallel with it. Phases 3a–3h are independent of each other and can run
in parallel once Phase 1 lands.

| Phase | Deliverable | Gate |
| --- | --- | --- |
| **0** | This design; decisions on §11 | Owner sign-off |
| **1. Extract the runtime** | `ProcessServices` + `KeyspaceRuntime::open/close` + `KeyspaceRegistry` with one preloaded runtime. Pure refactor; no routing, no behavior change | All default-mode gates green; baseline footprint numbers for N=1 |
| **1b. Username-policy parity** | Port Go's Starter username handling into the Rust handshake and user DDL (§4.1): the prerequisite compatibility release for migrating existing keyspaces | Go-parity tests for variants, prefix mismatch and `CREATE USER` prefix; plain `app` login after migration on a single-keyspace node |
| **2. Controlled demo** (behind `multi-keyspace.enable` **and** `dev-mode`) | Handshake split and registry lookup; TLS rule and operator listener (§5.4, §5.5); provisioning (§4.1); server-ID reservation (§6.6); pin-count lifecycle without idle close (§5.3); greeting plugin from config; allow-list, preload, max-keyspaces. **Pulled forward from 3d:** per-keyspace min-start-ts / GC tracking (§6.5), because GC is a data-loss risk even in a demo. **Unsupported operations are rejected** with `ER_NOT_SUPPORTED_YET` until their phase lands: placement, resource-group, label-rule and TiFlash-replica DDL (3b), `AUTO_ID_CACHE=1` tables (3f), IMPORT INTO / BR / PLAN REPLAYER / external storage (3f), tenant HTTP status APIs. **Also in Phase 2:** prefix-less users rejected by default (Q2); `require_secure_transport` read per tenant (§6.7) | Two tenants on one node: data, auth, GC-safety and AutoID tests (§9) pass; every rejected operation has a test asserting the rejection. **Not for shared deployment.** |
| **3a** | Keyspace identity reads + resource tags + username policy (§6.1) | Unit and integration tests; no empty-keyspace assertions firing |
| **3b** | PD/infosync singletons into `RuntimeServices` (§6.2), including a keyspace-qualified placement group ID. **Done in development mode:** one `InfoSyncer` per runtime; resource groups, label rules (`keyspace/<id>/...`) and placement bundles (`keyspace-<id>-<group>`, keyspace-encoded ranges) per tenant. The serial tests `two_keyspaces_keep_their_own_label_rules`, `_placement` and `_resource_groups` pass with colliding table IDs. Live (`rust/scripts/multi-tenant-sql-client.py`, the `test_*_are_tenant_local` cases): PD holds each tenant's resource groups and label rules under its keyspace, its table bundles as `keyspace-<id>-TiDB_DDL_<table id>`, and each tenant's `ALTER RANGE global` bundle as `keyspace-<id>-TiDB_GLOBAL`. The PD cases run their DDL in both tenants and fail unless each tenant's DDL owner made its PD write (`require_owner_coverage`), unless every statement answered success (an 1105 is retried, never accepted), and unless PD's label-rule and bundle start/end bytes equal the tenant's keyspace-encoded table range (or whole keyspace for `TiDB_GLOBAL`); each tenant's previous global policy is restored. `--self-test` checks those guards offline. Live run on 2026-11-26 (both tenants' DDL owners on the 3b node): all 29 cases pass; e.g. `keyspace/1/schema/<db>/t` and `keyspace-1-TiDB_DDL_<id>` both range over `7800000174800000ff…fc`, keyspace 101's over `7800006574800000ff…fc`. The run found PD answering a missing resource group with `ErrGroupNotExists`, which the step's lookup now reads as no group. Earlier runs met an `invalid marker byte` 1105 right after PD split a tenant's regions at a new rule's bounds (the playground's TiKV answering a stale read with raw region bounds, from `decode_region_error`); not seen in this run, and not fixed: a statement that meets it now fails the case. Still refused: TiFlash replicas, affinity, PD's schedulers, FLASHBACK CLUSTER, METRICS_SCHEMA, `TIDB_HOT_REGIONS` (§6.2) | Resource-group, placement and label-rule SQL per tenant |
| **3c** | `SET GLOBAL` / vardef split (§6.3), with the generated classification appendix | `SET GLOBAL` isolation tests for every global-scoped atomic |
| **3d** | ID-keyed caches and DDL version state into the runtime (§6.4) and transaction/GC maps (§6.5). **Done in development mode:** the min-start-ts inputs are per keyspace (§6.5); the table row-count cache, the sync-load flights and the async-load demand are per domain; DDL job contexts are keyed by keyspace and job; the job version state is no tenant's (only the classic kernel writes it). Both gates passed: the live GC gate (`rust/scripts/multi-tenant-gc-gate.py`) and DDL concurrency with colliding job and table IDs (serial test `two_keyspaces_run_ddl_with_colliding_ids_at_once`, live case `test_concurrent_ddl_keeps_each_tenants_schema`, concurrent ADD COLUMN and asynchronous statistics loads: the playground's older node, owner of both tenants' DDL, runs neither this node's ADD INDEX reorganization nor its ANALYZE broadcast) | Stats and DDL concurrency in two tenants; GC safe point per tenant |
| **3e** | Observability isolation (§6.8), including the `SHOW STATUS` providers. **Done in development mode**; the deferred items are listed in §6.8 | Processlist, KILL, statement summary and slow-query filtering tests |
| **3f** | Status port multiplexing + AutoID service map + per-store globals, including the DXF task managers (§6.9, §6.10) | AUTO_ID_CACHE=1 tables in two tenants; IMPORT INTO per tenant |
| **3g** | Session migration (§5.6): keyspace-bound session tokens, token logins through routing, tenant-scoped `SET SESSION_STATES`, parity check of the ported session-states code in multi-keyspace mode | §5.6 migration tests, including cross-tenant token replay rejection |
| **3h. TiProxy** (pingcap/tiproxy `rust`) | K1–K6 (§5.7): multi-keyspace backends, username-prefix routing, migration guard and metering by session keyspace, per-keyspace drain, keyspace-compatible balancing | TiProxy unit tests plus the §5.6 migration tests through `tiproxy-rs --standalone` |
| **4. Lifecycle** | Idle close, PD state watch and drain (using proxy session migration when present, §5.6), per-tenant `max_connections` and prepared-statement count (`PREPARED_STMT_COUNT`, §5.9), optional prefix alias (Q1) | Lifecycle tests; leak check that repeated open/close does not grow memory or file descriptors |
| **4b. Hibernation** | Per-tenant memory roots and quotas (no arenas in v1); one eviction decision (freeze or close) with a bounded drain; freeze with an atomically published, tenant-bound snapshot of schema and stats; thaw that always reloads accounts, grants, sysvars and bindings; hot/warm/cold policy (§5.8) | §5.8 tests: tenant B reuses A's released budget over repeated cycles; thaw matches a fresh node after a remote revoke and DDL; every bad snapshot falls back to a cold open |
| **5. Hardening** | Full isolation suite; footprint benchmark N=1/10/50; per-tenant memory quota; a per-runtime share of the process cache quotas (plan, stats, binding, schema caches, §5.9); docs | The isolation suite is the bar for calling the mode tenant-isolated |

Phase 2 may be developed alongside Phase 3.

**Gate for the first shared deployment**, meaning `dev-mode` off with real
tenants:

- **all** of 3a–3h and Phase 4 are done;
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
  optional `default-keyspace`. *Today (development mode):* they route to the
  node's configured keyspace, which acts as `default-keyspace`; Phase 2 makes
  rejection the default.
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
