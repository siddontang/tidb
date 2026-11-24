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
//! GLOBAL` atomics, connection IDs and observability still belong to the
//! node's configured keyspace. Do not serve real tenants with it.

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

use crate::keyspace_runtime::KeyspaceRuntime;
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

/// Splits `<keyspace>.<account>` at the first dot. Keyspace names never
/// contain a dot (`tidb_naming::check_keyspace_name`), so the prefix is
/// unambiguous; `None` when the name has no prefix.
pub(crate) fn keyspace_of_user(user: &str) -> Option<&str> {
    let (keyspace, account) = user.split_once('.')?;
    (!keyspace.is_empty() && !account.is_empty()).then_some(keyspace)
}

/// What a connection is served by once its keyspace is known: the
/// keyspace's session factory and accounts.
pub(crate) type Routed<F> = (Arc<F>, Arc<ConfiguredUserStore>);
type RoutedKeyspace = Routed<Domain>;

/// Chooses the keyspace a connection is served by from its user name, before
/// any authentication decision (design §4).
pub(crate) trait ConnectionRouter<F>: Send + Sync {
    /// The keyspace's session factory and accounts; `None` refuses the
    /// connection with the same error as a failed login.
    fn route(&self, user: &str) -> Option<Routed<F>>;
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
        dev: MultiKeyspaceDev,
    ) -> Self {
        let primary_keyspace = base_config.global_config.keyspace_name.clone();
        provision(&primary_keyspace, &primary.0, dev.root_password.as_deref());
        let tls = Arc::new(Mutex::new(None));
        let opener = Opener {
            base_config,
            spill_storage,
            memory_arbitrator,
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
        let (runtime, authority) =
            KeyspaceRuntime::open(&config, Arc::clone(&self.spill_storage), self.memory_arbitrator.clone(), true)?;
        if let Some((manager, variables)) = self.tls.lock().expect("registry tls").clone() {
            runtime.stack.factory.install_tls_manager(manager, variables);
        }
        provision(keyspace, &runtime.stack.factory, self.root_password.as_deref());
        tidb_logutil::bg_logger().info(
            "keyspace opened for multi-keyspace serving",
            &[tidb_log::Field::new("keyspace", tidb_log::Value::Str(keyspace.to_owned()))],
        );
        Ok((runtime, authority))
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
    fn route(&self, user: &str) -> Option<RoutedKeyspace> {
        // A user without a prefix is served by the node's configured
        // keyspace: the design's `default-keyspace` (§4), which keeps plain
        // `root` and TiUP's readiness probe working in development mode.
        let Some(keyspace) = keyspace_of_user(user) else {
            return Some((Arc::clone(&self.primary.0), Arc::clone(&self.primary.1)));
        };
        if keyspace == self.primary_keyspace {
            return Some((Arc::clone(&self.primary.0), Arc::clone(&self.primary.1)));
        }
        if !self.allowed(keyspace) {
            return None;
        }
        self.get_or_open(keyspace)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyspace_of_user_splits_at_the_first_dot() {
        assert_eq!(keyspace_of_user("keyspace1.root"), Some("keyspace1"));
        assert_eq!(keyspace_of_user("ks.app.reader"), Some("ks"));
        assert_eq!(keyspace_of_user("root"), None);
        assert_eq!(keyspace_of_user(".root"), None);
        assert_eq!(keyspace_of_user("ks."), None);
    }

    #[test]
    fn dev_settings_never_allow_system() {
        let dev = MultiKeyspaceDev::new(" keyspace2, SYSTEM ,,keyspace3", Some(String::new()));
        assert_eq!(dev.allow, vec!["keyspace2".to_owned(), "keyspace3".to_owned()]);
        assert_eq!(dev.root_password, None);
    }
}
