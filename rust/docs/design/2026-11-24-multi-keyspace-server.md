# Multi-keyspace tidb-server: one process, many tenants

- Status: **Draft for review, revision 6**: session migration through the Rust TiProxy (§5.6, §5.7); addresses the reviews of `42b666f`, `7cbdef8ad3` and `08d9476bfc`. No code has been written yet.
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
  - process-wide administration: instance-scoped variables (§6.3),
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
  gets its own scope back afterwards; a DDL step enters the snapshot its
  domain gave the DDL's session pool. There is no process-wide registry of
  snapshots.
- *Per keyspace: not yet* marks a global static that other code still
  writes directly (MDL, the DDL error limit, the analyze defaults, the
  schema validator's delta count, the cloud storage URI); it stays
  process-wide until its writers move.
- A tenant's `SET GLOBAL` / `SET INSTANCE` of an *instance* variable (and of
  a variable Go scopes to the instance alone) is refused with
  `ER_SPECIFIC_ACCESS_DENIED_ERROR` (`RESTRICTED_VARIABLES_ADMIN`) in
  multi-keyspace mode.

<!-- BEGIN generated: vardef classification -->

92 statics: 57 global (51 kept per keyspace so far), 28 instance, 7 internal.

| Static | System variable | Registry scope | Class | Per keyspace | Governs |
| --- | --- | --- | --- | --- | --- |
| `ENABLE_MDL` | `tidb_enable_metadata_lock` | GLOBAL | global | not yet | metadata locking of the keyspace's DDL (its meta key) |
| `DDL_FLASHBACK_CONCURRENCY` | `tidb_ddl_flashback_concurrency` | GLOBAL | global | yes | the keyspace's DDL jobs |
| `DDL_REORG_WORKER_COUNTER` | `tidb_ddl_reorg_worker_cnt` | GLOBAL \| SESSION | global | yes | the keyspace's DDL jobs |
| `DDL_REORG_BATCH_SIZE` | `tidb_ddl_reorg_batch_size` | GLOBAL \| SESSION | global | yes | the keyspace's DDL jobs |
| `DDL_ERROR_COUNT_LIMIT` | `tidb_ddl_error_count_limit` | GLOBAL | global | not yet | the keyspace's DDL jobs |
| `MAX_AUTO_ANALYZE_TIME` | `tidb_max_auto_analyze_time` | GLOBAL | global | yes | the keyspace's auto-analyze |
| `ENABLE_WORKLOAD_BASED_LEARNING` | -- | -- | internal | -- | no system variable |
| `WORKLOAD_BASED_LEARNING_INTERVAL` | -- | -- | internal | -- | no system variable |
| `ENABLE_FAST_CREATE_TABLE` | `tidb_enable_fast_create_table` | GLOBAL | global | yes | the keyspace's DDL submission |
| `IS_SAND_BOX_MODE_ENABLED` | -- | -- | internal | -- | process config (`security.disconnect-on-expired-password`) |
| `TABLE_CACHE_LEASE` | `tidb_table_cache_lease` | GLOBAL | global | yes | the keyspace's cached tables |
| `ANALYZE_DEFAULT_NUM_BUCKETS` | `tidb_analyze_default_num_buckets` | GLOBAL | global | not yet | the keyspace's statistics |
| `ANALYZE_DEFAULT_NUM_TOP_N` | `tidb_analyze_default_num_topn` | GLOBAL | global | not yet | the keyspace's statistics |
| `STATS_CACHE_MEM_QUOTA` | `tidb_stats_cache_mem_quota` | GLOBAL \| INSTANCE | instance | -- | process memory |
| `MEMORY_USAGE_ALARM_RATIO` | `tidb_memory_usage_alarm_ratio` | GLOBAL | instance | -- | process memory |
| `FORCE_PRIORITY` | `tidb_force_priority` | INSTANCE | instance | -- | Go `ScopeInstance` |
| `MEMORY_USAGE_ALARM_KEEP_RECORD_NUM` | `tidb_memory_usage_alarm_keep_record_num` | GLOBAL | instance | -- | process memory |
| `PLAN_REPLAYER_FILE_RETENTION_TIME` | -- | -- | internal | -- | no SetGlobal writer |
| `CONNECT_ATTRS_SIZE` | `performance_schema_session_connect_attrs_size` | GLOBAL | global | yes | the keyspace's connections |
| `CONNECT_ATTRS_LONGEST_SEEN` | -- | -- | internal | -- | status counter |
| `CONNECT_ATTRS_LOST` | -- | -- | internal | -- | status counter |
| `MAX_DELTA_SCHEMA_COUNT` | `tidb_max_delta_schema_count` | GLOBAL | global | not yet | the keyspace's schema validator |
| `ENABLE_ROW_LEVEL_CHECKSUM` | `tidb_enable_row_level_checksum` | GLOBAL | global | yes | the keyspace's row encoding |
| `ENABLE_FOREIGN_KEY` | `tidb_enable_foreign_key` | GLOBAL | global | yes | the keyspace's DDL policy |
| `ENABLE_CHECK_CONSTRAINT` | `tidb_enable_check_constraint` | GLOBAL | global | yes | the keyspace's DDL policy |
| `ENABLE_FAST_REORG` | `tidb_ddl_enable_fast_reorg` | GLOBAL | global | yes | the keyspace's DDL jobs |
| `SERVICE_SCOPE` | `tidb_service_scope` | INSTANCE | instance | -- | Go `ScopeInstance`: this node's DXF scope |
| `CLOUD_STORAGE_URI` | `tidb_cloud_storage_uri` | GLOBAL | global | not yet | the keyspace's import and DDL storage |
| `DDL_DISK_QUOTA` | `tidb_ddl_disk_quota` | GLOBAL | global | yes | the keyspace's DDL jobs |
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
external-timestamp slot -- and applies `tidb_cloud_storage_uri`; another
keyspace's domain answers `tidb_external_ts` from its own store.

<!-- BEGIN generated: set-global hook destinations -->

145 variables with a `SetGlobal` hook: 53 keyspace, 92 process.

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
| `tidb_analyze_default_num_buckets` | GLOBAL | the process vardef static `ANALYZE_DEFAULT_NUM_BUCKETS` (not yet per keyspace) | process |
| `tidb_analyze_default_num_topn` | GLOBAL | the process vardef static `ANALYZE_DEFAULT_NUM_TOP_N` (not yet per keyspace) | process |
| `tidb_auto_analyze_concurrency` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_cb_pd_metadata_error_rate_threshold_ratio` | GLOBAL | the process vardef static `CIRCUIT_BREAKER_PD_METADATA_ERROR_RATE_THRESHOLD_RATIO` | process |
| `tidb_check_mb4_value_in_utf8` | INSTANCE | the process configuration | process |
| `tidb_committer_concurrency` | GLOBAL | the process TiKV client | process |
| `tidb_ddl_disk_quota` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_ddl_enable_fast_reorg` | GLOBAL | the keyspace vardef snapshot | keyspace |
| `tidb_ddl_error_count_limit` | GLOBAL | the process vardef static `DDL_ERROR_COUNT_LIMIT` (not yet per keyspace) | process |
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
| `tidb_external_ts` | GLOBAL | the keyspace store's external timestamp (its own provider; the process slot is the configured keyspace's) | keyspace |
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
| `tidb_max_delta_schema_count` | GLOBAL | the process vardef static `MAX_DELTA_SCHEMA_COUNT` (not yet per keyspace) | process |
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
| **2. Controlled demo** (behind `multi-keyspace.enable` **and** `dev-mode`) | Handshake split and registry lookup; TLS rule and operator listener (§5.4, §5.5); provisioning (§4.1); server-ID reservation (§6.6); pin-count lifecycle without idle close (§5.3); greeting plugin from config; allow-list, preload, max-keyspaces. **Pulled forward from 3d:** per-keyspace min-start-ts / GC tracking (§6.5), because GC is a data-loss risk even in a demo. **Unsupported operations are rejected** with `ER_NOT_SUPPORTED_YET` until their phase lands: placement and resource-group DDL (3b), `AUTO_ID_CACHE=1` tables (3f), IMPORT INTO / BR / external storage (3f), tenant HTTP status APIs | Two tenants on one node: data, auth, GC-safety and AutoID tests (§9) pass; every rejected operation has a test asserting the rejection. **Not for shared deployment.** |
| **3a** | Keyspace identity reads + resource tags + username policy (§6.1) | Unit and integration tests; no empty-keyspace assertions firing |
| **3b** | PD/infosync singletons into `RuntimeServices` (§6.2) | Resource-group, placement and label-rule SQL per tenant |
| **3c** | `SET GLOBAL` / vardef split (§6.3), with the generated classification appendix | `SET GLOBAL` isolation tests for every global-scoped atomic |
| **3d** | ID-keyed caches into the runtime (§6.4) and transaction/GC maps (§6.5) | Stats and DDL concurrency in two tenants; GC safe point per tenant |
| **3e** | Observability isolation (§6.8) | Processlist, KILL, statement summary and slow-query filtering tests |
| **3f** | Status port multiplexing + AutoID service map + per-store globals (§6.9, §6.10) | AUTO_ID_CACHE=1 tables in two tenants; IMPORT INTO per tenant |
| **3g** | Session migration (§5.6): keyspace-bound session tokens, token logins through routing, tenant-scoped `SET SESSION_STATES`, parity check of the ported session-states code in multi-keyspace mode | §5.6 migration tests, including cross-tenant token replay rejection |
| **3h. TiProxy** (pingcap/tiproxy `rust`) | K1–K6 (§5.7): multi-keyspace backends, username-prefix routing, migration guard and metering by session keyspace, per-keyspace drain, keyspace-compatible balancing | TiProxy unit tests plus the §5.6 migration tests through `tiproxy-rs --standalone` |
| **4. Lifecycle** | Idle close, PD state watch and drain (using proxy session migration when present, §5.6), per-tenant `max_connections`, optional prefix alias (Q1) | Lifecycle tests; leak check that repeated open/close does not grow memory or file descriptors |
| **5. Hardening** | Full isolation suite; footprint benchmark N=1/10/50; per-tenant memory quota; docs | The isolation suite is the bar for calling the mode tenant-isolated |

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
