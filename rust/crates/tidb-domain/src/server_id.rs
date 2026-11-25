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

//! Domain's leased numeric server identity and connection-admission lifetime.
//! Go `domain.go`: acquireServerID, proposeServerID, refreshServerIDTTL,
//! serverIDKeeper and releaseServerID. Connection IDs consume this live owner.
//!
//! A multi-keyspace node (`rust/docs/design/2026-11-24-multi-keyspace-server.md`,
//! §6.6) holds one ID for the whole process: it reserves a candidate in
//! SYSTEM's namespace ([`ServerIdAuthority::new_process_reservation`]), then
//! the same ID in each tenant's namespace with the key, lease and CAS above
//! ([`ServerIdAuthority::new_tenant_reservation`]). A tenant reservation
//! never proposes another ID: a collision fails it.

use tidb_domain_serverinfo::serverinfo_syncer::{EtcdOps, Syncer};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};
use tidb_globalconn::{MAX_SERVER_ID32, MAX_SERVER_ID64};

const SERVER_ID_PATH: &str = "/tidb/server_id";

/// Source timing policy; tests can shorten intervals without changing production.
#[derive(Clone, Copy)]
pub struct ServerIdIntervals {
    /// Lease TTL (Go: twelve hours).
    pub ttl: Duration,
    /// Refresh the leased claim (Go: five minutes).
    pub refresh: Duration,
    /// Retry after connectivity loss (Go: ten seconds).
    pub retry: Duration,
    /// Maximum interval without a successful refresh (Go: six hours).
    pub lost_timeout: Duration,
    /// Wait between candidate collisions (Go: 300 milliseconds).
    pub collision_retry: Duration,
}
impl Default for ServerIdIntervals {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(12 * 3600),
            refresh: Duration::from_secs(300),
            retry: Duration::from_secs(10),
            lost_timeout: Duration::from_secs(6 * 3600),
            collision_retry: Duration::from_millis(300),
        }
    }
}

type KillConnections = Arc<dyn Fn() + Send + Sync>;

/// One process identity shared by publication, connection allocation and admission.
pub struct ServerIdAuthority {
    id: AtomicU64,
    lost: AtomicBool,
    enable_32bits: bool,
    etcd: Option<Arc<dyn EtcdOps>>,
    kill_connections: Mutex<Option<KillConnections>>,
    /// A tenant reservation's process reservation, whose ID it reserves.
    process: Option<Arc<ServerIdAuthority>>,
    /// Whether a lost lease retires the connections (Go). A multi-keyspace
    /// process reservation only closes admission: each tenant's own
    /// reservation retires that tenant's connections.
    retire_on_loss: bool,
}
impl ServerIdAuthority {
    /// A nil etcd client is Go's standalone mode, with numeric identity one.
    #[must_use]
    pub fn new(etcd: Option<Arc<dyn EtcdOps>>, enable_32bits: bool) -> Arc<Self> {
        let clustered = etcd.is_some();
        Arc::new(Self {
            id: AtomicU64::new(if clustered { 0 } else { 1 }),
            lost: AtomicBool::new(clustered),
            enable_32bits,
            etcd,
            kill_connections: Mutex::new(None),
            process: None,
            retire_on_loss: true,
        })
    }
    /// A multi-keyspace process's candidate ID, reserved in SYSTEM's
    /// namespace `etcd` like [`Self::new`]'s (64-bit IDs only). Losing it
    /// closes new handshakes on the whole process but retires no connection;
    /// the keeper then acquires a fresh candidate.
    #[must_use]
    pub fn new_process_reservation(etcd: Arc<dyn EtcdOps>) -> Arc<Self> {
        Arc::new(Self {
            id: AtomicU64::new(0),
            lost: AtomicBool::new(true),
            enable_32bits: false,
            etcd: Some(etcd),
            kill_connections: Mutex::new(None),
            process: None,
            retire_on_loss: false,
        })
    }
    /// `process`'s ID, reserved in a tenant's namespace `etcd` with the key,
    /// lease and CAS of [`Self::new`]. It never proposes an ID of its own:
    /// an ID already leased there fails the reservation. Losing the lease
    /// retires the tenant's connections, as Go does for its node's.
    #[must_use]
    pub fn new_tenant_reservation(etcd: Arc<dyn EtcdOps>, process: Arc<Self>) -> Arc<Self> {
        Arc::new(Self {
            id: AtomicU64::new(0),
            lost: AtomicBool::new(true),
            enable_32bits: false,
            etcd: Some(etcd),
            kill_connections: Mutex::new(None),
            process: Some(process),
            retire_on_loss: true,
        })
    }
    /// Whether this identity may admit a connection: its lease is held and,
    /// for a tenant reservation, it holds the ID its process greets with.
    #[must_use]
    pub fn admits(&self) -> bool {
        !self.is_lost() && self.process.as_ref().is_none_or(|process| process.id() == self.id())
    }
    /// The identity whose loss stops new handshakes: a tenant reservation's
    /// process reservation, otherwise this one.
    #[must_use]
    pub fn admission(self: &Arc<Self>) -> Arc<Self> {
        self.process.clone().unwrap_or_else(|| Arc::clone(self))
    }
    /// Numeric ID read by both globalconn and server-info serialization.
    #[must_use]
    pub fn id(&self) -> u64 {
        self.id.load(Ordering::Acquire)
    }
    /// Go IsLostConnectionToPD: new sockets must be closed before authentication.
    #[must_use]
    pub fn is_lost(&self) -> bool {
        self.lost.load(Ordering::Acquire)
    }
    /// Installs the server's actual socket/query retirement callback.
    pub fn set_connection_killer(&self, kill: Option<KillConnections>) {
        *self
            .kill_connections
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = kill;
    }
    fn mark_lost(&self) {
        if !self.lost.swap(true, Ordering::AcqRel) && self.retire_on_loss {
            let kill = self
                .kill_connections
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if let Some(kill) = kill {
                kill();
            }
        }
    }
}

struct LeaseState {
    authority: Arc<ServerIdAuthority>,
    lease: Option<i64>,
    last_success: Option<Instant>,
    policy: ServerIdIntervals,
}
impl LeaseState {
    fn acquire(&mut self, syncer: &Syncer, stop: &mpsc::Receiver<()>) -> Result<(), String> {
        if !matches!(stop.try_recv(), Err(mpsc::TryRecvError::Empty)) {
            return Err("server-ID keeper stopped".into());
        }
        self.authority.id.store(0, Ordering::Release);
        let etcd = self
            .authority
            .etcd
            .as_ref()
            .expect("cluster keeper has etcd");
        // Go's concurrency.Session independently signals expiration. During
        // recovery, recheck it before reusing a claim lease after an outage.
        if let Some(lease) = self.lease {
            if etcd.lease_keep_alive_ttl(lease)? <= 0 {
                self.lease = None;
            }
        }
        let lease = match self.lease {
            Some(lease) => lease,
            None => {
                let lease = etcd.lease_grant(self.policy.ttl.as_secs() as i64)?;
                self.lease = Some(lease);
                lease
            }
        };
        let mut conflicts = 0;
        loop {
            if !matches!(stop.try_recv(), Err(mpsc::TryRecvError::Empty)) {
                return Err("server-ID keeper stopped".into());
            }
            let id = match &self.authority.process {
                Some(process) if process.is_lost() || process.id() == 0 => {
                    return Err("the process holds no server ID to reserve".into());
                }
                Some(process) => process.id(),
                None => self.propose(syncer, conflicts)?,
            };
            if etcd.create_if_absent_with_lease(&format!("{SERVER_ID_PATH}/{id}"), b"0", lease)? {
                self.authority.id.store(id, Ordering::Release);
                return Ok(());
            }
            if self.authority.process.is_some() {
                // Switching IDs would orphan the process's greeted connections.
                return Err(collision(syncer, id));
            }
            conflicts += 1;
            if !matches!(
                stop.recv_timeout(self.policy.collision_retry),
                Err(mpsc::RecvTimeoutError::Timeout)
            ) {
                return Err("server-ID keeper stopped".into());
            }
        }
    }
    fn propose(&self, syncer: &Syncer, conflicts: usize) -> Result<u64, String> {
        #[cfg(test)]
        if let Some(id) = tests::FORCED_CANDIDATES.with(|forced| forced.borrow_mut().pop()) {
            return Ok(id);
        }
        if !self.authority.enable_32bits {
            return random_id(1, MAX_SERVER_ID64);
        }
        if conflicts < 3 {
            let peers = syncer.get_all_server_info()?;
            if peers.len() as f32 <= 0.9 * MAX_SERVER_ID32 as f32 {
                let ids = peers
                    .values()
                    .map(|info| {
                        info.static_info
                            .server_id_getter
                            .as_ref()
                            .map_or(info.static_info.json_server_id, |get| get())
                    })
                    .collect::<std::collections::HashSet<_>>();
                for _ in 0..15 {
                    let id = random_id(1, MAX_SERVER_ID32)?;
                    if !ids.contains(&id) {
                        return Ok(id);
                    }
                }
            }
        }
        random_id(MAX_SERVER_ID32 + 1, MAX_SERVER_ID64)
    }
    fn refresh(&mut self) -> Result<(), String> {
        let etcd = self
            .authority
            .etcd
            .as_ref()
            .expect("cluster keeper has etcd");
        let lease = match self.lease {
            Some(lease) => lease,
            None => {
                let lease = etcd.lease_grant(self.policy.ttl.as_secs() as i64)?;
                self.lease = Some(lease);
                lease
            }
        };
        // Go concurrency.Session owns lease keepalive independently of the
        // claim refresh. Both operations run under this retained Rust worker.
        if etcd.lease_keep_alive_ttl(lease)? <= 0 {
            self.lease = None;
            self.authority.mark_lost();
            return Err("server-ID lease expired".into());
        }
        let key = format!("{SERVER_ID_PATH}/{}", self.authority.id());
        etcd.put_with_lease_retry(&key, b"0", lease, 3)
    }
    /// Whether a tenant reservation's process now holds another ID: a
    /// fresh candidate after its SYSTEM lease was lost.
    fn process_moved(&self) -> bool {
        self.authority.process.as_ref().is_some_and(|process| {
            let id = process.id();
            id != 0 && !process.is_lost() && id != self.authority.id()
        })
    }
    fn tick(&mut self, now: Instant, syncer: &Syncer, stop: &mpsc::Receiver<()>) {
        if self.authority.process.is_some()
            && !self.authority.is_lost()
            && !self.process_moved()
            && self
                .last_success
                .is_some_and(|last| now.saturating_duration_since(last) < self.policy.refresh)
        {
            return;
        }
        if !self.authority.is_lost() && self.process_moved() {
            // The tenant's connections carry the old ID: drain them, then
            // reserve the process's new ID in their place.
            tracing::warn!(
                old = self.authority.id(),
                "process server ID changed; draining the keyspace and reserving the new ID"
            );
            self.release();
        }
        if self.authority.is_lost() {
            if self.acquire(syncer, stop).is_ok() {
                // Publish the new identity before reopening admission.
                if let Err(error) = syncer.store_server_info() {
                    tracing::warn!(%error, "StoreServerInfo after server-ID recovery");
                }
                self.last_success = Some(now);
                self.authority.lost.store(false, Ordering::Release);
            }
        } else {
            match self.refresh() {
                Ok(()) => self.last_success = Some(now),
                Err(error) => {
                    tracing::warn!(%error, "refreshServerIDTTL failed");
                    // Go initializes lastSucceedTimestamp to the zero time.
                    if !self.policy.lost_timeout.is_zero()
                        && self.last_success.is_none_or(|last| {
                            now.saturating_duration_since(last) > self.policy.lost_timeout
                        })
                    {
                        self.authority.mark_lost();
                    }
                }
            }
        }
    }
    fn release(&mut self) {
        self.authority.mark_lost();
        self.authority.id.store(0, Ordering::Release);
        if let Some(lease) = self.lease.take() {
            if let Some(etcd) = &self.authority.etcd {
                if let Err(error) = etcd.lease_revoke(lease) {
                    tracing::warn!(%error, "releaseServerID failed");
                }
            }
        }
    }
}
impl Drop for LeaseState {
    fn drop(&mut self) {
        self.release();
    }
}

/// A tenant reservation's collision, naming this node and the server-info
/// record that publishes `id` in the tenant's namespace, if any.
fn collision(syncer: &Syncer, id: u64) -> String {
    let node = |info: &tidb_domain_serverinfo::serverinfo::ServerInfo| {
        format!("{} ({}:{})", info.static_info.id, info.static_info.ip, info.static_info.port)
    };
    let holder = syncer
        .get_all_server_info()
        .ok()
        .and_then(|peers| {
            peers.into_values().find(|info| {
                info.static_info
                    .server_id_getter
                    .as_ref()
                    .map_or(info.static_info.json_server_id, |get| get())
                    == id
            })
        })
        .map_or_else(|| "a node without a server-info record".to_owned(), |info| node(&info));
    format!(
        "server ID {id} is already leased in this keyspace by {holder}; this node {} does not publish it",
        node(&syncer.get_local_server_info())
    )
}

fn random_id(min: u64, max: u64) -> Result<u64, String> {
    let range = max - min + 1;
    // Rejection sampling, preserving Go's uniform random candidate distribution.
    let bound = u64::MAX - u64::MAX % range;
    loop {
        let value = getrandom::u64().map_err(|error| error.to_string())?;
        if value < bound {
            return Ok(min + value % range);
        }
    }
}

/// Retained and joined Domain server-ID keeper. Drop precedes etcd retirement.
pub struct ServerIdKeeper {
    stop: Option<mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl ServerIdKeeper {
    /// Acquire once at startup, then own refresh/loss/recovery until joined close.
    pub fn start(
        authority: Arc<ServerIdAuthority>,
        syncer: Arc<Syncer>,
        policy: ServerIdIntervals,
    ) -> Result<Self, String> {
        if authority.etcd.is_none() {
            return Ok(Self {
                stop: None,
                thread: None,
            });
        }
        let (send, receive) = mpsc::channel();
        let mut state = LeaseState {
            authority,
            lease: None,
            last_success: None,
            policy,
        };
        match state.acquire(&syncer, &receive) {
            Ok(()) => {
                syncer.store_server_info()?;
                state.authority.lost.store(false, Ordering::Release);
            }
            Err(error) => tracing::warn!(%error, "acquire serverID failed; keeper will retry"),
        }
        Self::run(state, syncer, send, receive)
    }
    /// [`Self::start`] for a tenant reservation
    /// ([`ServerIdAuthority::new_tenant_reservation`]), before `syncer`
    /// publishes anything: the reservation must succeed now, and the
    /// caller's first server-info publication then carries the ID. A
    /// collision or an unreachable etcd is the error, and nothing is left
    /// leased.
    pub fn start_reserved(
        authority: Arc<ServerIdAuthority>,
        syncer: Arc<Syncer>,
        policy: ServerIdIntervals,
    ) -> Result<Self, String> {
        if authority.process.is_none() || authority.etcd.is_none() {
            return Err("a tenant reservation needs its process reservation and etcd".into());
        }
        let (send, receive) = mpsc::channel();
        let mut state = LeaseState {
            authority,
            lease: None,
            last_success: None,
            policy,
        };
        // A failure drops the state, which revokes the lease.
        state.acquire(&syncer, &receive)?;
        state.authority.lost.store(false, Ordering::Release);
        Self::run(state, syncer, send, receive)
    }
    fn run(
        mut state: LeaseState,
        syncer: Arc<Syncer>,
        send: mpsc::Sender<()>,
        receive: mpsc::Receiver<()>,
    ) -> Result<Self, String> {
        let policy = state.policy;
        let thread = std::thread::Builder::new()
            .name("server-id-keeper".into())
            .spawn(move || loop {
                // A tenant reservation wakes at the retry pace to follow its
                // process's ID; it still refreshes at the refresh pace.
                let interval = if state.authority.is_lost() || state.authority.process.is_some() {
                    policy.retry
                } else {
                    policy.refresh
                };
                if !matches!(
                    receive.recv_timeout(interval),
                    Err(mpsc::RecvTimeoutError::Timeout)
                ) {
                    break;
                }
                if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    state.tick(Instant::now(), &syncer, &receive)
                }))
                .is_err()
                {
                    state.authority.mark_lost();
                    tracing::error!(
                        "server-ID keeper recovered panic; admission closed until reacquisition"
                    );
                }
            })
            .map_err(|error| error.to_string())?;
        Ok(Self {
            stop: Some(send),
            thread: Some(thread),
        })
    }
}
impl Drop for ServerIdKeeper {
    fn drop(&mut self) {
        self.stop.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_domain_serverinfo::serverinfo::ServerInfo;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicI64, AtomicUsize};

    thread_local! {
        /// Candidates [`LeaseState::propose`] returns first, last pushed
        /// first: a test's way of forcing two nodes onto one ID.
        pub(super) static FORCED_CANDIDATES: std::cell::RefCell<Vec<u64>> = const { std::cell::RefCell::new(Vec::new()) };
    }
    fn force_candidates(ids: &[u64]) {
        FORCED_CANDIDATES.with(|forced| *forced.borrow_mut() = ids.iter().rev().copied().collect());
    }

    #[derive(Default)]
    struct Etcd {
        keys: Mutex<BTreeMap<String, (Vec<u8>, i64)>>,
        next: AtomicI64,
        fail: AtomicBool,
        expired: AtomicBool,
        conflicts: AtomicUsize,
    }
    impl EtcdOps for Etcd {
        fn lease_grant(&self, _: i64) -> Result<i64, String> {
            Ok(self.next.fetch_add(1, Ordering::SeqCst) + 1)
        }
        fn lease_keep_alive_once(&self, _: i64) -> Result<(), String> {
            if self.fail.load(Ordering::SeqCst) {
                Err("unavailable".into())
            } else {
                Ok(())
            }
        }
        fn lease_keep_alive_ttl(&self, lease: i64) -> Result<i64, String> {
            self.lease_keep_alive_once(lease)?;
            Ok(if self.expired.load(Ordering::SeqCst) {
                0
            } else {
                43200
            })
        }
        fn lease_revoke(&self, lease: i64) -> Result<(), String> {
            self.keys
                .lock()
                .unwrap()
                .retain(|_, (_, owner)| *owner != lease);
            Ok(())
        }
        fn put_with_lease(&self, key: &str, value: &[u8], lease: i64) -> Result<(), String> {
            self.keys
                .lock()
                .unwrap()
                .insert(key.into(), (value.to_vec(), lease));
            Ok(())
        }
        fn create_if_absent_with_lease(
            &self,
            key: &str,
            value: &[u8],
            lease: i64,
        ) -> Result<bool, String> {
            if self
                .conflicts
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Ok(false);
            }
            let mut keys = self.keys.lock().unwrap();
            if keys.contains_key(key) {
                return Ok(false);
            }
            keys.insert(key.into(), (value.to_vec(), lease));
            Ok(true)
        }
        fn get_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, String> {
            Ok(self
                .keys
                .lock()
                .unwrap()
                .iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .map(|(key, (value, _))| (key.clone(), value.clone()))
                .collect())
        }
        fn delete(&self, key: &str) -> Result<(), String> {
            self.keys.lock().unwrap().remove(key);
            Ok(())
        }
        fn put(&self, key: &str, value: &[u8]) -> Result<(), String> {
            self.put_with_lease(key, value, 0)
        }
        fn delete_prefix(&self, prefix: &str) -> Result<(), String> {
            self.keys
                .lock()
                .unwrap()
                .retain(|key, _| !key.starts_with(prefix));
            Ok(())
        }
    }
    fn fixture(etcd: &Arc<Etcd>, name: &str) -> (Arc<ServerIdAuthority>, Arc<Syncer>) {
        let owner = ServerIdAuthority::new(Some(etcd.clone()), true);
        let mut info = ServerInfo::default();
        info.static_info.id = name.into();
        let getter = owner.clone();
        info.static_info.server_id_getter = Some(Arc::new(move || getter.id()));
        let syncer = Arc::new(Syncer::new_with_status_endpoint_claim(
            info,
            Some(etcd.clone()),
            false,
        ));
        syncer.new_session_and_store_server_info().unwrap();
        (owner, syncer)
    }
    fn policy() -> ServerIdIntervals {
        ServerIdIntervals {
            collision_retry: Duration::ZERO,
            ..Default::default()
        }
    }

    #[test]
    fn cluster_lifecycle_batch_claim_collision_fallback_and_release() {
        let etcd = Arc::new(Etcd::default());
        let (first, first_info) = fixture(&etcd, "first");
        let first_keeper = ServerIdKeeper::start(first.clone(), first_info, policy()).unwrap();
        assert!((1..=MAX_SERVER_ID32).contains(&first.id()));
        etcd.conflicts.store(3, Ordering::SeqCst);
        let (second, second_info) = fixture(&etcd, "second");
        let second_keeper =
            ServerIdKeeper::start(second.clone(), second_info.clone(), policy()).unwrap();
        assert!(second.id() > MAX_SERVER_ID32 && second.id() <= MAX_SERVER_ID64);
        assert_ne!(first.id(), second.id());
        let published: serde_json::Value =
            serde_json::from_slice(&etcd.keys.lock().unwrap()[second_info.server_info_path()].0)
                .unwrap();
        assert_eq!(published["server_id"], second.id());
        let key = format!("{SERVER_ID_PATH}/{}", second.id());
        drop(second_keeper);
        assert!(second.is_lost());
        assert_eq!(second.id(), 0);
        assert!(!etcd.keys.lock().unwrap().contains_key(&key));
        assert!(!first.is_lost());
        drop(first_keeper);
    }

    #[test]
    fn cluster_lifecycle_batch_loss_kills_once_then_recovers_and_republishes() {
        let etcd = Arc::new(Etcd::default());
        let (owner, syncer) = fixture(&etcd, "recover");
        let kills = Arc::new(AtomicUsize::new(0));
        let observed = kills.clone();
        owner.set_connection_killer(Some(Arc::new(move || {
            observed.fetch_add(1, Ordering::SeqCst);
        })));
        let (_send, receive) = mpsc::channel();
        let mut state = LeaseState {
            authority: owner.clone(),
            lease: None,
            last_success: None,
            policy: policy(),
        };
        state.acquire(&syncer, &receive).unwrap();
        owner.lost.store(false, Ordering::Release);
        etcd.fail.store(true, Ordering::SeqCst);
        state.tick(Instant::now(), &syncer, &receive);
        assert!(owner.is_lost());
        assert_eq!(kills.load(Ordering::SeqCst), 1);
        owner.mark_lost();
        assert_eq!(kills.load(Ordering::SeqCst), 1);
        etcd.fail.store(false, Ordering::SeqCst);
        state.tick(Instant::now(), &syncer, &receive);
        assert!(!owner.is_lost());
        let published: serde_json::Value =
            serde_json::from_slice(&etcd.keys.lock().unwrap()[syncer.server_info_path()].0)
                .unwrap();
        assert_eq!(published["server_id"], owner.id());
        // Even shortly after a successful refresh, a confirmed expired lease
        // must close admission before a new owner can reuse its ID.
        etcd.expired.store(true, Ordering::SeqCst);
        state.tick(Instant::now(), &syncer, &receive);
        assert!(owner.is_lost());
        assert!(state.lease.is_none());
        assert_eq!(kills.load(Ordering::SeqCst), 2);
        etcd.expired.store(false, Ordering::SeqCst);
        state.tick(Instant::now(), &syncer, &receive);
        assert!(!owner.is_lost());
    }

    #[test]
    fn cluster_lifecycle_batch_recent_refresh_tolerates_transient_failure_and_stop_cancels_acquire()
    {
        let etcd = Arc::new(Etcd::default());
        let (owner, syncer) = fixture(&etcd, "transient");
        let (send, receive) = mpsc::channel();
        let now = Instant::now();
        let mut state = LeaseState {
            authority: owner.clone(),
            lease: None,
            last_success: Some(now),
            policy: policy(),
        };
        state.acquire(&syncer, &receive).unwrap();
        owner.lost.store(false, Ordering::Release);
        etcd.fail.store(true, Ordering::SeqCst);
        state.tick(now + Duration::from_secs(1), &syncer, &receive);
        assert!(!owner.is_lost());
        state.tick(
            now + policy().lost_timeout + Duration::from_secs(1),
            &syncer,
            &receive,
        );
        assert!(owner.is_lost());
        drop(send);
        assert!(state
            .acquire(&syncer, &receive)
            .unwrap_err()
            .contains("stopped"));
    }

    /// A node's server info in `etcd`, publishing `authority`'s ID, not yet
    /// stored: a tenant reservation comes before the first publication.
    fn unpublished(etcd: &Arc<Etcd>, name: &str, authority: &Arc<ServerIdAuthority>) -> Arc<Syncer> {
        let mut info = ServerInfo::default();
        info.static_info.id = name.into();
        info.static_info.ip = "127.0.0.1".into();
        // Distinct addresses: a node's registration removes stale records of its own.
        info.static_info.port = if name == "single" { 4000 } else { 4001 };
        let getter = authority.clone();
        info.static_info.server_id_getter = Some(Arc::new(move || getter.id()));
        Arc::new(Syncer::new_with_status_endpoint_claim(info, Some(etcd.clone()), false))
    }
    /// A multi-keyspace process's reservation in SYSTEM's namespace.
    fn process_reservation(system: &Arc<Etcd>) -> (Arc<ServerIdAuthority>, LeaseState, Arc<Syncer>) {
        let process = ServerIdAuthority::new_process_reservation(system.clone());
        let syncer = Arc::new(Syncer::new_with_status_endpoint_claim(ServerInfo::default(), None, false));
        let mut state = LeaseState {
            authority: process.clone(),
            lease: None,
            last_success: None,
            policy: policy(),
        };
        let (_send, receive) = mpsc::channel();
        state.acquire(&syncer, &receive).unwrap();
        process.lost.store(false, Ordering::Release);
        (process, state, syncer)
    }
    /// The leases in `etcd` expire: their server-ID keys go with them.
    fn expire(etcd: &Etcd) {
        etcd.expired.store(true, Ordering::SeqCst);
        etcd.keys.lock().unwrap().retain(|key, _| !key.starts_with(SERVER_ID_PATH));
    }
    fn server_id_keys(etcd: &Etcd) -> Vec<String> {
        etcd.keys.lock().unwrap().keys().filter(|key| key.starts_with(SERVER_ID_PATH)).cloned().collect()
    }
    fn published(etcd: &Etcd, syncer: &Syncer) -> Option<u64> {
        let keys = etcd.keys.lock().unwrap();
        let (bytes, _) = keys.get(syncer.server_info_path())?;
        serde_json::from_slice::<serde_json::Value>(bytes).unwrap()["server_id"].as_u64()
    }

    /// Design §6.6 steps 1 and 2: the process reserves a 64-bit candidate in
    /// SYSTEM, and each tenant reserves the same ID with the single-keyspace
    /// key before its server info publishes it.
    #[test]
    fn a_tenant_reserves_the_process_id_before_publishing_it() {
        let system = Arc::new(Etcd::default());
        let (process, _process_state, _) = process_reservation(&system);
        let id = process.id();
        assert!(id > MAX_SERVER_ID32 && id <= MAX_SERVER_ID64);
        assert_eq!(server_id_keys(&system), [format!("{SERVER_ID_PATH}/{id}")]);

        let tenant_etcd = Arc::new(Etcd::default());
        let tenant = ServerIdAuthority::new_tenant_reservation(tenant_etcd.clone(), process.clone());
        let syncer = unpublished(&tenant_etcd, "multi", &tenant);
        let keeper = ServerIdKeeper::start_reserved(tenant.clone(), syncer.clone(), policy()).unwrap();
        assert_eq!(tenant.id(), id);
        assert!(!tenant.is_lost());
        assert_eq!(server_id_keys(&tenant_etcd), [format!("{SERVER_ID_PATH}/{id}")]);
        assert_eq!(published(&tenant_etcd, &syncer), None, "nothing is published by the reservation");
        syncer.new_session_and_store_server_info().unwrap();
        assert_eq!(published(&tenant_etcd, &syncer), Some(id));
        // Admission follows the process's reservation.
        assert!(Arc::ptr_eq(&tenant.admission(), &process));
        drop(keeper);
        assert!(server_id_keys(&tenant_etcd).is_empty(), "closing releases the tenant's lease");
        assert!(!process.is_lost());
    }

    /// Mixed fleet, design §6.6 step 3: a single-keyspace node already leases
    /// the ID the multi-keyspace process holds. The multi-keyspace node's
    /// reservation fails with both node identities, leaves nothing leased
    /// and publishes nothing; it never switches IDs.
    #[test]
    fn a_tenant_reservation_fails_on_an_id_a_single_keyspace_node_holds() {
        let system = Arc::new(Etcd::default());
        let tenant_etcd = Arc::new(Etcd::default());
        let overlap = MAX_SERVER_ID32 + 777;
        // The single-keyspace node of the tenant, with its own Go keeper.
        force_candidates(&[overlap]);
        let single = ServerIdAuthority::new(Some(tenant_etcd.clone()), false);
        let single_info = unpublished(&tenant_etcd, "single", &single);
        single_info.new_session_and_store_server_info().unwrap();
        let _single_keeper = ServerIdKeeper::start(single.clone(), single_info.clone(), policy()).unwrap();
        assert_eq!(single.id(), overlap);
        // The multi-keyspace process, forced onto the same candidate.
        force_candidates(&[overlap]);
        let (process, _process_state, _) = process_reservation(&system);
        assert_eq!(process.id(), overlap);

        let tenant = ServerIdAuthority::new_tenant_reservation(tenant_etcd.clone(), process.clone());
        let syncer = unpublished(&tenant_etcd, "multi", &tenant);
        let error = ServerIdKeeper::start_reserved(tenant.clone(), syncer.clone(), policy()).err().unwrap();
        assert!(error.contains(&format!("server ID {overlap} is already leased")), "{error}");
        assert!(error.contains("single (127.0.0.1:4000)") && error.contains("multi (127.0.0.1:4001)"), "{error}");
        assert!(tenant.is_lost());
        assert_eq!(tenant.id(), 0);
        assert_eq!(process.id(), overlap, "the process keeps its ID");
        assert_eq!(server_id_keys(&tenant_etcd), [format!("{SERVER_ID_PATH}/{overlap}")]);
        assert_eq!(published(&tenant_etcd, &syncer), None);
        assert_eq!(published(&tenant_etcd, &single_info), Some(overlap), "the single-keyspace node keeps serving");
    }

    /// Mixed fleet, the other order: the multi-keyspace node reserved the ID
    /// first, and a single-keyspace node proposing it collides and, as Go
    /// does, takes another; it never publishes the reserved ID.
    #[test]
    fn a_single_keyspace_node_moves_off_an_id_a_tenant_reservation_holds() {
        let system = Arc::new(Etcd::default());
        let tenant_etcd = Arc::new(Etcd::default());
        let overlap = MAX_SERVER_ID32 + 888;
        force_candidates(&[overlap]);
        let (process, _process_state, _) = process_reservation(&system);
        let tenant = ServerIdAuthority::new_tenant_reservation(tenant_etcd.clone(), process.clone());
        let syncer = unpublished(&tenant_etcd, "multi", &tenant);
        let _tenant_keeper = ServerIdKeeper::start_reserved(tenant.clone(), syncer.clone(), policy()).unwrap();
        syncer.new_session_and_store_server_info().unwrap();

        let other = overlap + 1;
        force_candidates(&[overlap, other]);
        let single = ServerIdAuthority::new(Some(tenant_etcd.clone()), false);
        let single_info = unpublished(&tenant_etcd, "single", &single);
        single_info.new_session_and_store_server_info().unwrap();
        let _single_keeper = ServerIdKeeper::start(single.clone(), single_info.clone(), policy()).unwrap();
        assert_eq!(single.id(), other);
        assert_eq!(published(&tenant_etcd, &single_info), Some(other));
        assert_eq!(published(&tenant_etcd, &syncer), Some(overlap));
        assert_eq!(tenant.id(), overlap);
    }

    /// Design §6.6 step 4: losing one tenant's lease retires that tenant's
    /// connections only, and its keeper reserves the same ID again. Losing
    /// the SYSTEM lease closes admission (the tenants' admission identity)
    /// without retiring connections; once the process holds a fresh ID,
    /// each tenant drains and reserves it.
    #[test]
    fn tenant_and_process_lease_loss() {
        let system = Arc::new(Etcd::default());
        let (process, mut process_state, process_syncer) = process_reservation(&system);
        let process_kills = Arc::new(AtomicUsize::new(0));
        let observed = process_kills.clone();
        process.set_connection_killer(Some(Arc::new(move || {
            observed.fetch_add(1, Ordering::SeqCst);
        })));
        let (_send, receive) = mpsc::channel();
        let tenant = |name: &str| {
            let etcd = Arc::new(Etcd::default());
            let authority = ServerIdAuthority::new_tenant_reservation(etcd.clone(), process.clone());
            let syncer = unpublished(&etcd, name, &authority);
            let kills = Arc::new(AtomicUsize::new(0));
            let observed = kills.clone();
            authority.set_connection_killer(Some(Arc::new(move || {
                observed.fetch_add(1, Ordering::SeqCst);
            })));
            let mut state = LeaseState {
                authority: authority.clone(),
                lease: None,
                last_success: None,
                policy: policy(),
            };
            state.acquire(&syncer, &receive).unwrap();
            authority.lost.store(false, Ordering::Release);
            syncer.new_session_and_store_server_info().unwrap();
            (etcd, authority, syncer, state, kills)
        };
        let (a_etcd, a, a_syncer, mut a_state, a_kills) = tenant("a");
        let (b_etcd, b, b_syncer, mut b_state, b_kills) = tenant("b");
        let id = process.id();
        assert_eq!((a.id(), b.id()), (id, id));

        // Tenant A's lease expires: only A drains, then A reserves the same ID.
        expire(&a_etcd);
        a_state.tick(Instant::now(), &a_syncer, &receive);
        assert!(a.is_lost() && !a.admits());
        assert_eq!(a_kills.load(Ordering::SeqCst), 1);
        b_state.tick(Instant::now(), &b_syncer, &receive);
        assert!(!b.is_lost() && !process.is_lost());
        assert_eq!(b_kills.load(Ordering::SeqCst), 0);
        a_etcd.expired.store(false, Ordering::SeqCst);
        a_state.tick(Instant::now(), &a_syncer, &receive);
        assert!(!a.is_lost());
        assert_eq!(a.id(), id);
        assert_eq!(published(&a_etcd, &a_syncer), Some(id));

        // The SYSTEM lease expires: admission closes, nothing is retired,
        // and the tenants keep their reservations while the process has none.
        expire(&system);
        process_state.tick(Instant::now(), &process_syncer, &receive);
        assert!(process.is_lost() && a.admission().is_lost() && b.admission().is_lost());
        assert!(a.admits() && b.admits(), "the tenants still hold the ID their connections carry");
        assert_eq!(process_kills.load(Ordering::SeqCst), 0);
        system.expired.store(false, Ordering::SeqCst);
        let fresh = id + 1;
        force_candidates(&[fresh]);
        process_state.tick(Instant::now(), &process_syncer, &receive);
        assert!(!process.is_lost());
        assert_eq!(process.id(), fresh);
        assert_eq!(server_id_keys(&system), [format!("{SERVER_ID_PATH}/{fresh}")]);
        assert!(!a.admits() && !b.admits(), "a tenant on the old ID admits no connection greeted with the fresh one");
        // Each tenant drains its connections (greeted with the old ID) and
        // reserves the fresh one.
        for (etcd, authority, syncer, state, kills) in
            [(&a_etcd, &a, &a_syncer, &mut a_state, &a_kills), (&b_etcd, &b, &b_syncer, &mut b_state, &b_kills)]
        {
            state.tick(Instant::now(), syncer, &receive);
            assert!(!authority.is_lost() && authority.admits());
            assert_eq!(authority.id(), fresh);
            assert_eq!(published(etcd, syncer), Some(fresh));
            assert_eq!(server_id_keys(etcd), [format!("{SERVER_ID_PATH}/{fresh}")]);
            assert_eq!(kills.load(Ordering::SeqCst), if Arc::ptr_eq(authority, &a) { 2 } else { 1 });
        }
    }
}
