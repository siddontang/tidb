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

//! The DDL owner's PD journal duties: the operator's cutover at takeover,
//! and the reconciler's loop for as long as it owns the DDL
//! ([`crate::reconcile`]).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tidb_domain_infosync::info_syncer::InfoSyncer;

use crate::pd::{bundle_entry, label_rule_value, InfoSyncerPd};
use crate::{ObjectKey, PdValue};

/// The variable that carries the operator's cutover: `*`, or the
/// comma-separated IDs of the keyspaces whose writers that bypass the journal
/// (nodes of an older build) the operator has drained.
pub const CUTOVER_VARIABLE: &str = "TIDB_PD_JOURNAL_CUTOVER";

fn cutover_asked(keyspace_id: Option<u32>) -> bool {
    std::env::var(CUTOVER_VARIABLE).is_ok_and(|ids| {
        ids.split(',')
            .map(str::trim)
            .any(|id| id == "*" || keyspace_id.is_some_and(|keyspace_id| id == keyspace_id.to_string()))
    })
}

/// The keyspace's PD-only objects PD holds now: its label rules and its
/// range bundles.
fn pd_only_objects(syncer: &InfoSyncer) -> Result<BTreeMap<ObjectKey, PdValue>, String> {
    let mut objects = BTreeMap::new();
    let rules = syncer.label_rule_manager().get_all_label_rules(syncer.label_codec())?;
    for rule in tidb_domain_infosync::label_manager::filter_rules_by_keyspace(rules, syncer.label_codec()) {
        objects.insert(ObjectKey::label_rule(&rule.id), label_rule_value(&rule));
    }
    for range in [tidb_placement::KEY_RANGE_GLOBAL, tidb_placement::KEY_RANGE_META] {
        let mut bundle = tidb_placement::Bundle::default();
        bundle.rebuild_for_range(range, "");
        let (key, _) = bundle_entry(&syncer.pd_bundle(&bundle)?);
        if let (_, Some(value)) = bundle_entry(&syncer.placement_manager().get_rule_bundle(&key.id)?) {
            objects.insert(key, value);
        }
    }
    Ok(objects)
}

/// The operator's cutover of the keyspace `syncer` serves, when
/// [`CUTOVER_VARIABLE`] names it and the keyspace had none: seeds a record
/// for each of its PD-only objects from PD. No attempt of this build writes
/// them before (they are refused until the cutover); the variable is the
/// operator's word that no older node does either.
fn cutover_if_asked(store: &dyn tidb_meta::MetaStore, syncer: &InfoSyncer) {
    if !cutover_asked(syncer.keyspace_id()) {
        return;
    }
    let seeded = pd_only_objects(syncer).and_then(|objects| crate::journal::seed_cutover(store, &objects));
    let keyspace = tidb_log::Field::new("keyspace", tidb_log::Value::Str(format!("{:?}", syncer.keyspace_id())));
    let logger = tidb_logutil::bg_logger();
    match seeded {
        Ok(Some(count)) => logger.info(
            "PD journal cutover: seeded the keyspace's PD-only objects",
            &[keyspace, tidb_log::Field::new("seeded", tidb_log::Value::U64(count as u64))],
        ),
        Ok(None) => {}
        Err(error) => logger.warn(
            "PD journal cutover failed; label rules and range bundles stay refused",
            &[keyspace, tidb_log::Field::new("error", tidb_log::Value::Str(error))],
        ),
    }
}

/// The DDL owner's PD journal duties over the keyspace's meta `store`, as
/// a listener of its owner manager beside the job scheduler (`scheduler`):
/// at a takeover, the operator's cutover and then the reconciler of the
/// keyspace `info_syncer` answers, which stops at the retirement or once the
/// node no longer owns the DDL.
pub fn with_scheduler(
    store: Arc<dyn tidb_meta::MetaStore>,
    info_syncer: SyncerOf,
    owner: &Arc<dyn tidb_owner::Manager>,
    scheduler: Arc<dyn tidb_owner::Listener>,
) -> Arc<dyn tidb_owner::Listener> {
    let duties = PdJournalDuties {
        store,
        info_syncer,
        owner: Arc::downgrade(owner),
        stop: std::sync::Mutex::default(),
    };
    Arc::new(tidb_owner::ListenersWrapper::new(vec![Arc::new(duties), scheduler]))
}

/// The info syncer of the DDL's keyspace, once its domain gave it one.
pub type SyncerOf = Arc<dyn Fn() -> Option<Arc<InfoSyncer>> + Send + Sync>;

struct PdJournalDuties {
    store: Arc<dyn tidb_meta::MetaStore>,
    info_syncer: SyncerOf,
    /// Weak: the owner manager holds this listener.
    owner: std::sync::Weak<dyn tidb_owner::Manager>,
    /// The running reconciler's stop flag.
    stop: std::sync::Mutex<Option<Arc<AtomicBool>>>,
}

impl tidb_owner::Listener for PdJournalDuties {
    /// Runs on a thread of its own: the DDL can become the owner before its
    /// domain gives the session pool an info syncer. The job scheduler may
    /// start meanwhile; until the cutover its steps refuse PD-only objects.
    fn on_become_owner(&self) {
        let stop = Arc::new(AtomicBool::new(false));
        if let Some(previous) = self.stop.lock().unwrap_or_else(std::sync::PoisonError::into_inner).replace(Arc::clone(&stop)) {
            previous.store(true, Ordering::Release);
        }
        let (store, owner, info_syncer) = (Arc::clone(&self.store), self.owner.clone(), Arc::clone(&self.info_syncer));
        let spawned = std::thread::Builder::new().name("ddl-pd-reconciler".to_owned()).spawn(move || {
            let still_owner =
                || !stop.load(Ordering::Acquire) && owner.upgrade().is_some_and(|owner| owner.is_owner());
            let wait = |interval: Duration| {
                let deadline = Instant::now() + interval;
                while Instant::now() < deadline && !stop.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(100).min(interval));
                }
                stop.load(Ordering::Acquire)
            };
            let syncer = loop {
                match info_syncer() {
                    Some(syncer) => break syncer,
                    None if still_owner() && !wait(Duration::from_millis(200)) => {}
                    None => return,
                }
            };
            let _entered = tidb_domain_infosync::info_syncer::enter(Arc::clone(&syncer));
            cutover_if_asked(store.as_ref(), &syncer);
            // A node without PD keeps its objects in this process's mocks,
            // which no other process writes late: nothing to repair.
            let Some(id) = owner.upgrade().filter(|_| syncer.pd_http_cli().is_some()).map(|owner| owner.id().to_owned()) else {
                return;
            };
            let options = crate::reconcile::Options::new(
                &id,
                crate::pd::scope(&syncer),
                syncer.keyspace_id(),
            );
            let pd = InfoSyncerPd(syncer);
            crate::reconcile::run_until_stopped(
                store.as_ref(),
                &pd,
                &options,
                crate::reconcile::SCAN_INTERVAL,
                &still_owner,
                &wait,
            );
        });
        if let Err(error) = spawned {
            tidb_logutil::bg_logger().warn(
                "the PD journal reconciler did not start",
                &[tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string()))],
            );
        }
    }

    fn on_retire_owner(&self) {
        if let Some(stop) = self.stop.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take() {
            stop.store(true, Ordering::Release);
        }
    }
}
