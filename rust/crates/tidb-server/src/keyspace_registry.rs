// Copyright 2026 PingCAP, Inc.
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

//! Serving more than one keyspace from one node, selected per connection by
//! the `<keyspace>.<account>` user name (TiDB Cloud Starter's form).
//!
//! **Development mode only** (Phase 2 of
//! `rust/docs/design/2026-11-24-multi-keyspace-server.md`). It is enabled by
//! the `TIDB_MULTI_KEYSPACE_DEV` environment variable, a comma-separated list
//! of the extra keyspaces this node may serve beside its configured one. Not
//! every process-wide service is per keyspace yet (design §6): the infosync
//! PD clients, IMPORT INTO / BRIE, the AutoID service, process-wide `SET
//! GLOBAL` atomics and observability still belong to the node's configured
//! keyspace. Do not serve real tenants with it.
//!
//! Connection IDs are the process's (design §6.6): every keyspace serves
//! under the server ID the process reserved in SYSTEM, which each keyspace's
//! open reserves again in that keyspace before publishing it. A keyspace
//! whose reservation fails or is lost is refused like a failed login.

use std::collections::HashMap;
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tidb_domain_core::domain::Domain;
use tidb_domain_core::node_config::NodeConfig;
use tidb_session::domain::cluster_session_node::DomainSessions as _;
use tidb_session::domain::configured_user_store::ConfiguredUserStore;
use tidb_session::domain::real_tikv_node::RunConfiguredNodeError;
use tidb_store_driver::real_tikv_read::ProductionReadProcessAuthority;

use crate::keyspace_runtime::{KeyspaceRuntime, ProcessServerId};
use tidb_domain::server_id::{AdmissionTicket, Greeting};
use crate::process_shutdown::ProcessReadAuthority as _;

/// The environment variable naming the extra keyspaces a node may serve.
pub(crate) const MULTI_KEYSPACE_DEV_ENV: &str = "TIDB_MULTI_KEYSPACE_DEV";
/// The environment variable holding the password the development
/// provisioning gives each served keyspace's `<keyspace>.root`.
pub(crate) const MULTI_KEYSPACE_DEV_ROOT_PASSWORD_ENV: &str = "TIDB_MULTI_KEYSPACE_DEV_ROOT_PASSWORD";

/// How long a failed open is remembered before a new connection retries it.
const FAILED_OPEN_BACKOFF: Duration = Duration::from_secs(10);
/// How long a connection waits for another connection's open of its keyspace.
const OPEN_TIMEOUT: Duration = Duration::from_secs(120);

/// Development multi-keyspace settings, read from the environment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MultiKeyspaceDev {
    /// The extra keyspaces this node may serve.
    allow: Vec<String>,
    /// The development provisioning password for `<keyspace>.root`.
    root_password: Option<String>,
}

impl MultiKeyspaceDev {
    /// Reads the settings; `None` when multi-keyspace mode is off. A node
    /// whose configured keyspace is SYSTEM never serves other keyspaces:
    /// SYSTEM is internal (design §5.5).
    pub(crate) fn from_env(configured_keyspace: &str) -> Option<Self> {
        if configured_keyspace.is_empty() || configured_keyspace == tidb_keyspace::SYSTEM {
            return None;
        }
        let allow = std::env::var(MULTI_KEYSPACE_DEV_ENV).ok()?;
        Some(Self::new(&allow, std::env::var(MULTI_KEYSPACE_DEV_ROOT_PASSWORD_ENV).ok()))
    }

    /// [`Self::from_env`] for `config`'s node: only a TiKV node serves
    /// keyspaces through a registry.
    pub(crate) fn for_node(config: &NodeConfig) -> Option<Self> {
        if config.store_kind != tidb_domain_core::node_config::StoreKind::TiKv {
            return None;
        }
        Self::from_env(&config.global_config.keyspace_name)
    }

    fn new(allow: &str, root_password: Option<String>) -> Self {
        let allow = allow
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty() && *name != tidb_keyspace::SYSTEM)
            .map(str::to_owned)
            .collect();
        Self {
            allow,
            root_password: root_password.filter(|password| !password.is_empty()),
        }
    }
}

/// What a connection is served by once its keyspace is known: the
/// keyspace's session factory and accounts.
pub(crate) type Routed<F> = (Arc<F>, Arc<ConfiguredUserStore>);
type RoutedKeyspace = Routed<Domain>;

/// Chooses the keyspace a connection is served by from its user name, before
/// any authentication decision (design §4).
pub(crate) trait ConnectionRouter<F>: Send + Sync {
    /// The keyspace's session factory and accounts for connection
    /// `connection_id`, greeted under `greeting`, with the ticket its
    /// process registration must pass (design §6.6; `None` for a keyspace
    /// with no server-ID reservation); `None` refuses the connection with
    /// the same error as a failed login.
    fn route(
        &self,
        user: &str,
        connection_id: u64,
        greeting: Option<Greeting>,
    ) -> Option<(Routed<F>, Option<AdmissionTicket>)>;
}

enum EntryState {
    Opening,
    Ready(RoutedKeyspace),
    Failed(Instant),
}

struct Entry {
    state: Mutex<EntryState>,
    changed: Condvar,
}

type TlsInstall = (Arc<dyn tidb_sessmgr::TlsManager>, Vec<(&'static str, String)>);

/// Work for the opener thread, which owns every extra keyspace's runtime: a
/// runtime's workers are not `Send`, so they stay on the thread that opened
/// them, and the connections only receive the keyspace's factory and
/// accounts.
enum OpenerRequest {
    Open(String, mpsc::Sender<Option<RoutedKeyspace>>),
    Close,
}

/// What the opener thread needs to open a keyspace.
struct Opener {
    base_config: NodeConfig,
    spill_storage: Arc<tidb_spill_storage::SpillStorage>,
    memory_arbitrator: Option<Arc<tidb_memory::MemArbitrator>>,
    process_id: Option<Arc<ProcessServerId>>,
    root_password: Option<String>,
    tls: Arc<Mutex<Option<TlsInstall>>>,
}

/// The keyspaces this node serves: its configured keyspace, opened at boot,
/// and the allowed extra keyspaces, each opened by its first connection.
pub(crate) struct KeyspaceRegistry {
    primary_keyspace: String,
    primary: RoutedKeyspace,
    allow: Vec<String>,
    tls: Arc<Mutex<Option<TlsInstall>>>,
    entries: Mutex<HashMap<String, Arc<Entry>>>,
    requests: Mutex<mpsc::Sender<OpenerRequest>>,
    opener: Mutex<Option<JoinHandle<()>>>,
}

impl KeyspaceRegistry {
    pub(crate) fn new(
        base_config: NodeConfig,
        primary: RoutedKeyspace,
        spill_storage: Arc<tidb_spill_storage::SpillStorage>,
        memory_arbitrator: Option<Arc<tidb_memory::MemArbitrator>>,
        process_id: Option<Arc<ProcessServerId>>,
        dev: MultiKeyspaceDev,
    ) -> Self {
        let primary_keyspace = base_config.global_config.keyspace_name.clone();
        provision(&primary_keyspace, &primary.0, dev.root_password.as_deref());
        retire_connections_on_lost_reservation(&primary.0);
        let tls = Arc::new(Mutex::new(None));
        let opener = Opener {
            base_config,
            spill_storage,
            memory_arbitrator,
            process_id,
            root_password: dev.root_password,
            tls: Arc::clone(&tls),
        };
        let (requests, receiver) = mpsc::channel();
        let thread = std::thread::Builder::new()
            .name("tidb-keyspace-opener".to_owned())
            .spawn(move || opener.serve(&receiver))
            .expect("spawn keyspace opener thread");
        Self {
            primary_keyspace,
            primary,
            allow: dev.allow,
            tls,
            entries: Mutex::new(HashMap::new()),
            requests: Mutex::new(requests),
            opener: Mutex::new(Some(thread)),
        }
    }

    /// The TLS owner the listener bound, installed into every keyspace's
    /// domain as `ConcurrentSqlNode::bind` installs it into the first.
    pub(crate) fn set_tls(&self, manager: Arc<dyn tidb_sessmgr::TlsManager>, variables: Vec<(&'static str, String)>) {
        *self.tls.lock().expect("registry tls") = Some((manager, variables));
    }

    fn allowed(&self, keyspace: &str) -> bool {
        self.allow.iter().any(|name| name == keyspace)
    }

    fn get_or_open(&self, keyspace: &str) -> Option<RoutedKeyspace> {
        let entry = {
            let mut entries = self.entries.lock().expect("registry entries");
            if let Some(entry) = entries.get(keyspace) {
                Arc::clone(entry)
            } else {
                let entry = Arc::new(Entry {
                    state: Mutex::new(EntryState::Opening),
                    changed: Condvar::new(),
                });
                entries.insert(keyspace.to_owned(), Arc::clone(&entry));
                drop(entries);
                return self.open_entry(keyspace, &entry);
            }
        };
        let deadline = Instant::now() + OPEN_TIMEOUT;
        let mut state = entry.state.lock().expect("registry entry");
        loop {
            match &*state {
                EntryState::Ready(routed) => return Some((Arc::clone(&routed.0), Arc::clone(&routed.1))),
                EntryState::Failed(at) if at.elapsed() < FAILED_OPEN_BACKOFF => return None,
                EntryState::Failed(_) => {
                    *state = EntryState::Opening;
                    drop(state);
                    return self.open_entry(keyspace, &entry);
                }
                EntryState::Opening => {
                    let now = Instant::now();
                    if now >= deadline {
                        return None;
                    }
                    state = entry
                        .changed
                        .wait_timeout(state, deadline - now)
                        .expect("registry entry")
                        .0;
                }
            }
        }
    }

    /// Has the opener thread open `keyspace` (the entry is `Opening` and
    /// owned by this connection) and publishes the outcome to the
    /// connections waiting on it.
    fn open_entry(&self, keyspace: &str, entry: &Entry) -> Option<RoutedKeyspace> {
        let (reply, outcome) = mpsc::channel();
        let sent = self
            .requests
            .lock()
            .expect("registry requests")
            .send(OpenerRequest::Open(keyspace.to_owned(), reply))
            .is_ok();
        let opened = if sent { outcome.recv().ok().flatten() } else { None };
        let mut state = entry.state.lock().expect("registry entry");
        *state = match &opened {
            Some(routed) => EntryState::Ready((Arc::clone(&routed.0), Arc::clone(&routed.1))),
            None => EntryState::Failed(Instant::now()),
        };
        entry.changed.notify_all();
        opened
    }

    /// Retires and releases every extra keyspace, then shuts its store down.
    /// The configured keyspace is the caller's, closed after this.
    pub(crate) fn close_extra_keyspaces(&self) {
        self.entries.lock().expect("registry entries").clear();
        let _ = self.requests.lock().expect("registry requests").send(OpenerRequest::Close);
        if let Some(thread) = self.opener.lock().expect("registry opener").take() {
            let _ = thread.join();
        }
    }
}

impl Opener {
    /// The opener thread: opens keyspaces on request and, on `Close` (or when
    /// the registry is gone), retires, releases and shuts down each of them.
    fn serve(self, requests: &mpsc::Receiver<OpenerRequest>) {
        let mut runtimes: Vec<(KeyspaceRuntime, ProductionReadProcessAuthority)> = Vec::new();
        while let Ok(OpenerRequest::Open(keyspace, reply)) = requests.recv() {
            let routed = match self.open_keyspace(&keyspace) {
                Ok((runtime, authority)) => {
                    let routed = (Arc::clone(&runtime.stack.factory), Arc::clone(&runtime.stack.users));
                    runtimes.push((runtime, authority));
                    Some(routed)
                }
                Err(error) => {
                    tidb_logutil::bg_logger().warn(
                        "open keyspace for multi-keyspace serving failed",
                        &[
                            tidb_log::Field::new("keyspace", tidb_log::Value::Str(keyspace.clone())),
                            tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string())),
                        ],
                    );
                    None
                }
            };
            let _ = reply.send(routed);
        }
        for (mut runtime, mut authority) in runtimes {
            runtime.retire();
            runtime.release();
            if let Err(error) = authority.shutdown_process() {
                tidb_logutil::bg_logger().warn(
                    "close keyspace store failed",
                    &[tidb_log::Field::new("error", tidb_log::Value::Str(format!("{error:?}")))],
                );
            }
        }
    }

    fn open_keyspace(
        &self,
        keyspace: &str,
    ) -> Result<(KeyspaceRuntime, ProductionReadProcessAuthority), RunConfiguredNodeError> {
        // The open's records (bootstrap, loaders, provisioning) are the
        // tenant's (design §6.1).
        let _tenant = tidb_logutil::enter_tenant(keyspace);
        let mut config = self.base_config.clone();
        config.global_config.keyspace_name = keyspace.to_owned();
        // The node's status port serves its configured keyspace's AutoID
        // service only, so extra keyspaces do not campaign for it.
        config.report_status = false;
        let (runtime, authority) = KeyspaceRuntime::open(
            &config,
            Arc::clone(&self.spill_storage),
            self.memory_arbitrator.clone(),
            true,
            self.process_id.as_deref(),
        )?;
        retire_connections_on_lost_reservation(&runtime.stack.factory);
        if let Some((manager, variables)) = self.tls.lock().expect("registry tls").clone() {
            runtime.stack.factory.install_tls_manager(manager, variables);
        }
        provision(keyspace, &runtime.stack.factory, self.root_password.as_deref());
        start_expensive_query_watchdog(keyspace, &runtime.stack.factory);
        tidb_logutil::bg_logger().info(
            "keyspace opened for multi-keyspace serving",
            &[tidb_log::Field::new("keyspace", tidb_log::Value::Str(keyspace.to_owned()))],
        );
        Ok((runtime, authority))
    }
}

/// Design §6.6: losing a keyspace's server-ID reservation retires that
/// keyspace's connections only, as Go retires its node's, and the keyspace
/// is refused until its keeper reserves the ID again.
fn retire_connections_on_lost_reservation(factory: &Domain) {
    let Some(identity) = factory.server_identity() else {
        return;
    };
    let processes = factory.processes();
    identity.set_connection_killer(Some(Arc::new(move || {
        for process in processes.snapshot() {
            processes.kill_with_normal_close_msg(process.id, false, false, false, "");
        }
    })));
}

/// Whether `factory`'s keyspace may admit connection `connection_id`,
/// greeted under `greeting`: its server-ID reservation is held, on the ID
/// the connection was greeted with, and the process has held its own lease
/// on that ID since the greeting (design §6.6). The ticket then guards the
/// connection's registration against a drain, a SYSTEM loss or a rotation
/// since (`Err` refuses the connection).
fn admission(factory: &Domain, connection_id: u64, greeting: Option<Greeting>) -> Result<Option<AdmissionTicket>, ()> {
    let Some(identity) = factory.server_identity() else {
        return Ok(None);
    };
    let greeting = greeting.ok_or(())?;
    let greeted = tidb_globalconn::parse_conn_id(connection_id).map_err(drop)?.0.server_id;
    identity.admission_ticket(greeted, greeting).map(Some).ok_or(())
}

/// Go `go dom.ExpensiveQueryHandle().SetSessionManager(svr).Run()` for an
/// extra keyspace: `ConcurrentSqlNode::bind` starts only the configured
/// keyspace's. The watchdog checks this keyspace's processes (its client
/// sessions and its auto-analyze workers) against this keyspace's
/// `tidb_max_auto_analyze_time` (design §6.3), and stops when the runtime
/// retires (`Domain::close` drops its exit).
fn start_expensive_query_watchdog(keyspace: &str, factory: &Domain) {
    let handle = factory.expensive_query_handle();
    handle.set_session_manager(factory.session_manager());
    let keyspace = keyspace.to_owned();
    let spawned = std::thread::Builder::new()
        .name("tidb-expensive-query".to_owned())
        .spawn(move || {
            let _tenant = tidb_logutil::enter_tenant(&keyspace);
            handle.run();
        });
    if let Err(error) = spawned {
        tidb_logutil::bg_logger().warn(
            "start expensive query watchdog failed",
            &[tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string()))],
        );
    }
}

/// Development stand-in for the provisioning tool (design §4.1): gives
/// `<keyspace>.root` the configured password and every privilege, so the
/// prefixed administrator can log in through the shared listener.
fn provision(keyspace: &str, factory: &Arc<Domain>, password: Option<&str>) {
    let Some(password) = password else {
        return;
    };
    let user = format!("{keyspace}.root");
    let quoted = |value: &str| value.replace('\\', "\\\\").replace('\'', "''");
    let statements = [
        format!(
            "CREATE USER IF NOT EXISTS '{}'@'%' IDENTIFIED BY '{}'",
            quoted(&user),
            quoted(password)
        ),
        format!("GRANT ALL PRIVILEGES ON *.* TO '{}'@'%' WITH GRANT OPTION", quoted(&user)),
    ];
    let result = factory.open_system_session().and_then(|mut session| {
        for sql in &statements {
            session.node_conn().execute_internal(sql)?;
        }
        Ok(())
    });
    if let Err(error) = result {
        tidb_logutil::bg_logger().warn(
            "provision keyspace administrator failed",
            &[
                tidb_log::Field::new("keyspace", tidb_log::Value::Str(keyspace.to_owned())),
                tidb_log::Field::new("error", tidb_log::Value::Str(error.message)),
            ],
        );
    }
}

impl ConnectionRouter<Domain> for KeyspaceRegistry {
    fn route(
        &self,
        user: &str,
        connection_id: u64,
        greeting: Option<Greeting>,
    ) -> Option<(RoutedKeyspace, Option<AdmissionTicket>)> {
        // A user without a prefix is served by the node's configured
        // keyspace: the design's `default-keyspace` (§4), which keeps plain
        // `root` and TiUP's readiness probe working in development mode.
        let routed = match tidb_keyspace::keyspace_of_user(user) {
            Some(keyspace) if keyspace != self.primary_keyspace => {
                if !self.allowed(keyspace) {
                    return None;
                }
                self.get_or_open(keyspace)?
            }
            _ => (Arc::clone(&self.primary.0), Arc::clone(&self.primary.1)),
        };
        let ticket = admission(&routed.0, connection_id, greeting).ok()?;
        Some((routed, ticket))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Records the kills an expensive-query watchdog sends a process.
    #[derive(Default)]
    struct RecordingKill(std::sync::atomic::AtomicUsize);

    impl tidb_sessmgr::ProcessKillTarget for RecordingKill {
        fn cancel_query(&self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        fn kill_connection(&self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// An extra keyspace runs its own expensive-query watchdog over its own
    /// processes and its own `tidb_max_auto_analyze_time` (design §6.3):
    /// the tenant's 1-second limit cancels the tenant's auto-analyze, and
    /// the configured keyspace's auto-analyze of the same age runs on under
    /// the configured keyspace's (process) limit.
    #[test]
    fn an_extra_keyspace_runs_its_own_expensive_query_watchdog() {
        use std::sync::atomic::Ordering::SeqCst;
        use tidb_session::domain::configured_user_store::ConfiguredUserStore;
        use tidb_session::domain::mock_store::MockStoreDomain;
        use tidb_stats_handle_util::GLOBAL_AUTO_ANALYZE_PROCESS_LIST;

        let configured = MockStoreDomain::create().unwrap();
        let users = ConfiguredUserStore::parse("root\t%\tmysql_native_password\t\n")
            .unwrap()
            .with_global_vars(tidb_domain::global_sysvars::GlobalSysvars::new().with_keyspace_vardef());
        let tenant = MockStoreDomain::create_with_users(Arc::new(users)).unwrap();
        tenant.new_session().unwrap().run("set global tidb_max_auto_analyze_time = 1").unwrap();
        assert_eq!(tenant.factory().vardef().max_auto_analyze_time().load(SeqCst), 1);
        let configured_limit = tidb_vardef::MAX_AUTO_ANALYZE_TIME.load(SeqCst);
        assert!(configured_limit == 0 || configured_limit > 60, "{configured_limit}");

        // One auto-analyze process in each keyspace, both running for longer
        // than the tenant's limit.
        let auto_analyze = |store: &MockStoreDomain, id: u64| {
            let target = Arc::new(RecordingKill::default());
            let guard = store.factory().processes().register(
                id,
                String::new(),
                "localhost".to_owned(),
                "test".to_owned(),
                Some(target.clone()),
            );
            store.factory().processes().statement_started(id, "analyze table test.t", "executing");
            GLOBAL_AUTO_ANALYZE_PROCESS_LIST.tracker(id);
            (guard, target)
        };
        let (_tenant_guard, tenant_target) = auto_analyze(&tenant, 7_700_001);
        let (_configured_guard, configured_target) = auto_analyze(&configured, 7_700_002);
        std::thread::sleep(Duration::from_millis(1_300));

        // The configured keyspace's watchdog, as `ConcurrentSqlNode::bind`
        // starts it, and the extra keyspace's, as its open starts it.
        crate::sql_node::run_expensive_query_handle(
            configured.factory().expensive_query_handle(),
            configured.factory().session_manager(),
        );
        start_expensive_query_watchdog("tenant", tenant.factory());

        let deadline = Instant::now() + Duration::from_secs(10);
        while tenant_target.0.load(SeqCst) == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
        }
        // Several ticks more, for the configured watchdog's chance.
        std::thread::sleep(Duration::from_millis(500));
        GLOBAL_AUTO_ANALYZE_PROCESS_LIST.untracker(7_700_001);
        GLOBAL_AUTO_ANALYZE_PROCESS_LIST.untracker(7_700_002);
        assert!(tenant_target.0.load(SeqCst) > 0, "the tenant's watchdog cancels its auto-analyze");
        assert_eq!(configured_target.0.load(SeqCst), 0, "the configured keyspace's limit is its own");
        configured.factory().close();
        tenant.factory().close();
    }

    /// The text cells of a statement's rows.
    fn cells(result: Result<tidb_session::StmtResult, tidb_driver_error::DriverError>) -> Vec<Vec<String>> {
        match result.unwrap() {
            tidb_session::StmtResult::Rows(rows) => rows
                .into_iter()
                .map(|row| {
                    row.iter()
                        .map(|cell| String::from_utf8_lossy(&cell.to_bytes().unwrap_or_default()).into_owned())
                        .collect()
                })
                .collect(),
            other => panic!("expected rows, got {other:?}"),
        }
    }

    /// Design §6.6: on a multi-keyspace node every keyspace serves under the
    /// process's one server ID, so `KILL` takes another keyspace's connection
    /// ID as a local one and looks it up in the caller's keyspace only. It
    /// behaves exactly like an ID no connection holds: `KILL` (Go's) answers
    /// OK and kills nothing, `EXPLAIN FOR CONNECTION` answers
    /// ER_NO_SUCH_THREAD, and neither processlist shows it. The keyspace's
    /// own connection is killed.
    #[test]
    fn kill_and_processlist_resolve_only_the_callers_keyspace() {
        use tidb_session::domain::mock_store::MockStoreDomain;

        let configured = MockStoreDomain::create().unwrap();
        let tenant = MockStoreDomain::create().unwrap();
        let mut session = tenant.new_session().unwrap();
        // The process's one allocator (here the tenant domain's, whose
        // standalone server ID both mock domains share).
        let allocator = tenant.factory().next_conn_id();
        let (foreign, missing, sibling) = (allocator.next_id(), allocator.next_id(), allocator.next_id());
        let server_id = |id: u64| tidb_globalconn::parse_conn_id(id).unwrap().0.server_id;
        assert_eq!(server_id(foreign), server_id(sibling));
        let register = |store: &MockStoreDomain, id: u64| {
            let target = Arc::new(RecordingKill::default());
            let guard = store.factory().processes().register(
                id,
                "root".to_owned(),
                "localhost".to_owned(),
                "test".to_owned(),
                Some(target.clone()),
            );
            (guard, target)
        };
        let (_foreign_guard, foreign_target) = register(&configured, foreign);
        let (_sibling_guard, sibling_target) = register(&tenant, sibling);

        let outcome = |session: &mut tidb_session::Session, sql: String| {
            session.run(&sql).map(|_| ()).map_err(|error| {
                let error = error.to_mysql_error();
                (error.code, error.message.replace(&foreign.to_string(), "<id>").replace(&missing.to_string(), "<id>"))
            })
        };
        for statement in ["kill {}", "kill query {}", "kill connection {}", "kill tidb {}"] {
            let kill = |id: u64| statement.replace("{}", &id.to_string());
            assert_eq!(outcome(&mut session, kill(foreign)), outcome(&mut session, kill(missing)), "{statement}");
            assert_eq!(outcome(&mut session, kill(foreign)), Ok(()), "{statement}");
        }
        assert_eq!(foreign_target.0.load(std::sync::atomic::Ordering::SeqCst), 0, "another keyspace's connection lives");
        let explain = |id: u64| format!("explain for connection {id}");
        let refused = outcome(&mut session, explain(foreign));
        assert_eq!(refused, outcome(&mut session, explain(missing)));
        assert_eq!(refused.unwrap_err().0, 1094);
        for list in ["show processlist", "select id from information_schema.processlist"] {
            let ids: Vec<String> = cells(session.run(list)).into_iter().map(|row| row[0].clone()).collect();
            assert!(ids.contains(&sibling.to_string()), "{list}: {ids:?}");
            assert!(!ids.contains(&foreign.to_string()), "{list}: {ids:?}");
        }
        // The keyspace's own connection.
        session.run(&format!("kill {sibling}")).unwrap();
        assert_eq!(cells(session.run("show warnings")), Vec::<Vec<String>>::new());
        assert_eq!(sibling_target.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        configured.factory().close();
        tenant.factory().close();
    }

    /// Design §6.6: one ID reserved in every served keyspace needs the
    /// 64-bit layout's 22-bit server IDs; the 32-bit layout is refused at
    /// startup, before anything is reserved.
    #[test]
    fn a_multi_keyspace_node_requires_64_bit_connection_ids() {
        let mut config = crate::node_flags::parse_node_config(std::iter::empty::<&str>()).unwrap();
        config.global_config.enable_32bits_connection_id = true;
        let Err(error) = crate::keyspace_runtime::ProcessServerId::reserve(&config) else {
            panic!("a 32-bit node reserved a process server ID");
        };
        assert!(error.to_string().contains("set enable-32bits-connection-id = false"), "{error}");
    }

    #[test]
    fn dev_settings_never_allow_system() {
        let dev = MultiKeyspaceDev::new(" keyspace2, SYSTEM ,,keyspace3", Some(String::new()));
        assert_eq!(dev.allow, vec!["keyspace2".to_owned(), "keyspace3".to_owned()]);
        assert_eq!(dev.root_password, None);
    }
}
