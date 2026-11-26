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

//! The PD journal and its reconciler over the embedded store (whose
//! transactions conflict as TiKV's do) and a fake PD that can hold a write
//! back, as a paused DDL owner's write lands late.
//!
//! A step is driven as the commit path drives it: the step's catalog
//! transaction begins, the journal is planned against its snapshot and the
//! intents commit, PD is written, and the step's transaction commits the
//! records. A step of an old owner runs in "another process" (another
//! scope), so this process's live holds do not cover it.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use tidb_ddl_pdjournal::journal::{self, Attempt, Prepared};
use tidb_ddl_pdjournal::pd::{bundle_entry, label_rule_value, resource_group_value, PdObjects};
use tidb_ddl_pdjournal::reconcile::{reconcile_once, Options, Report};
use tidb_ddl_pdjournal::{ObjectKey, PdValue, Planned, TxnRead};
use tidb_infoschema_issyncer::real_tikv_catalog::TransactionMetaSnapshot;
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{BufferMutation, OptimisticCommitOutcome};

use super::external_delivery::{ExternalDelivery, StepJournal};
use super::pd_baseline::SnapshotRead;
use crate::embedded_store::{in_process_write_stack, InProcessOpener};
use tidb_unistore_tikv::tso::InProcessPd;

const TIMEOUT: Duration = Duration::from_secs(10);

/// A write the fake PD holds until it is released.
#[derive(Default)]
struct Gate {
    released: Mutex<bool>,
    cond: Condvar,
}

impl Gate {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.cond.notify_all();
    }

    fn wait(&self) {
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.cond.wait(released).unwrap();
        }
    }
}

/// PD as every tenant shares it: objects by tenant and key (a resource
/// group's name is per keyspace; bundles and rules carry their keyspace in
/// their IDs), and every write by tenant.
#[derive(Default)]
struct SharedPd {
    objects: Mutex<BTreeMap<(u32, ObjectKey), PdValue>>,
    writes: Mutex<Vec<(u32, ObjectKey)>>,
}

impl SharedPd {
    fn set(&self, tenant: u32, key: &ObjectKey, value: Option<PdValue>) {
        let mut objects = self.objects.lock().unwrap();
        match value {
            Some(value) => objects.insert((tenant, key.clone()), value),
            None => objects.remove(&(tenant, key.clone())),
        };
    }
}

/// One tenant's client of [`SharedPd`]: it can hold back its next write of
/// an object, time one out while PD applies it later, and lose the reply to
/// one (applied, but answered with an error).
struct TenantPd {
    shared: Arc<SharedPd>,
    tenant: u32,
    held: Mutex<HashMap<ObjectKey, Arc<Gate>>>,
    delayed: Mutex<HashMap<ObjectKey, Arc<Gate>>>,
    unreadable: Mutex<std::collections::BTreeSet<ObjectKey>>,
    /// Runs once after the next read.
    after_read: Mutex<Option<Hook>>,
    lose_reply: AtomicBool,
}

type Hook = Box<dyn FnOnce() + Send>;

impl TenantPd {
    fn new(shared: &Arc<SharedPd>, tenant: u32) -> Self {
        Self {
            shared: Arc::clone(shared),
            tenant,
            held: Mutex::default(),
            delayed: Mutex::default(),
            unreadable: Mutex::default(),
            after_read: Mutex::default(),
            lose_reply: AtomicBool::new(false),
        }
    }

    fn get(&self, key: &ObjectKey) -> Option<PdValue> {
        self.shared.objects.lock().unwrap().get(&(self.tenant, key.clone())).cloned()
    }

    fn set(&self, key: &ObjectKey, value: Option<PdValue>) {
        self.shared.set(self.tenant, key, value);
    }

    /// Times the next write of `key` out at once; PD applies it when the
    /// gate is released.
    fn delay(&self, key: &ObjectKey) -> Arc<Gate> {
        let gate = Arc::new(Gate::default());
        self.delayed.lock().unwrap().insert(key.clone(), Arc::clone(&gate));
        gate
    }

    /// Waits until PD holds `value` under `key`.
    fn wait_for(&self, key: &ObjectKey, value: &PdValue) {
        let started = Instant::now();
        while self.get(key).as_ref() != Some(value) {
            assert!(started.elapsed() < TIMEOUT, "{key}: the delayed write never landed");
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    /// Waits until a writer is blocked on the hold of `key`.
    fn wait_held(&self, key: &ObjectKey) {
        while self.held.lock().unwrap().contains_key(key) {
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    /// Holds the next write of `key` until the gate is released.
    fn hold(&self, key: &ObjectKey) -> Arc<Gate> {
        let gate = Arc::new(Gate::default());
        self.held.lock().unwrap().insert(key.clone(), Arc::clone(&gate));
        gate
    }
}

impl PdObjects for TenantPd {
    fn read(&self, keys: &[ObjectKey]) -> Result<BTreeMap<ObjectKey, PdValue>, String> {
        let unreadable = self.unreadable.lock().unwrap();
        if let Some(key) = keys.iter().find(|key| unreadable.contains(*key)) {
            return Err(format!("PD cannot read {key}"));
        }
        drop(unreadable);
        let held = keys.iter().filter_map(|key| self.get(key).map(|value| (key.clone(), value))).collect();
        let hook = self.after_read.lock().unwrap().take();
        if let Some(hook) = hook {
            hook();
        }
        Ok(held)
    }

    fn write(&self, key: &ObjectKey, value: Option<&PdValue>) -> Result<(), String> {
        let delayed = self.delayed.lock().unwrap().remove(key);
        if let Some(gate) = delayed {
            let (shared, tenant, key, value) = (Arc::clone(&self.shared), self.tenant, key.clone(), value.cloned());
            std::thread::spawn(move || {
                gate.wait();
                shared.set(tenant, &key, value);
                shared.writes.lock().unwrap().push((tenant, key));
            });
            return Err("PD did not answer in time".to_owned());
        }
        let gate = self.held.lock().unwrap().remove(key);
        if let Some(gate) = gate {
            gate.wait();
        }
        self.set(key, value.cloned());
        self.shared.writes.lock().unwrap().push((self.tenant, key.clone()));
        if self.lose_reply.swap(false, Ordering::SeqCst) {
            return Err("the PD reply was lost".to_owned());
        }
        Ok(())
    }
}

/// A meta store that runs a hook once, after the next transaction it runs
/// once armed and `skip` more have passed.
struct HookedStore {
    inner: Arc<dyn tidb_meta::MetaStore>,
    after_next: Mutex<Option<Hook>>,
    skip: std::sync::atomic::AtomicUsize,
}

impl tidb_meta::MetaStore for HookedStore {
    fn run_in_new_txn(
        &self,
        ctx: &tidb_gocontext::Context,
        source_type: &str,
        retryable: bool,
        f: &mut dyn FnMut(&mut dyn tidb_meta::transaction::MetaTransaction) -> tidb_meta::Result<()>,
    ) -> Result<(), String> {
        let result = self.inner.run_in_new_txn(ctx, source_type, retryable, f);
        let mut armed = self.after_next.lock().unwrap();
        if armed.is_some() && self.skip.load(Ordering::SeqCst) > 0 {
            self.skip.fetch_sub(1, Ordering::SeqCst);
            return result;
        }
        let hook = armed.take();
        drop(armed);
        if let Some(hook) = hook {
            hook();
        }
        result
    }
}

/// One tenant: its own embedded store (its own meta, so its own journal)
/// over the shared PD.
struct Tenant {
    _pd: InProcessPd,
    opener: Arc<InProcessOpener>,
    store: Arc<dyn tidb_meta::MetaStore>,
    pd: Arc<TenantPd>,
    id: u32,
    /// This process's scope of the tenant, unique per test (tests run in
    /// parallel, and a classic store's IDs carry no keyspace).
    scope: u32,
}

/// The scope of a step that runs in another process.
fn elsewhere(tenant: &Tenant) -> u32 {
    tenant.scope + 1
}

impl Tenant {
    fn new(shared: &Arc<SharedPd>, id: u32) -> Self {
        let (pd, opener) = in_process_write_stack().expect("an embedded store");
        let opener = Arc::new(opener);
        let store = Arc::new(tidb_store_driver_txn::meta_txn::MetaStore::new(Arc::clone(&opener), TIMEOUT));
        static SCOPES: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1000);
        let scope = SCOPES.fetch_add(2, Ordering::Relaxed);
        Self { _pd: pd, opener, store, pd: Arc::new(TenantPd::new(shared, id)), id, scope }
    }

    fn options(&self, owner: &str) -> Options {
        let mut options = Options::new(owner, self.scope, Some(self.id));
        options.non_job_grace = Duration::from_millis(0);
        options
    }

    fn reconcile(&self, owner: &str) -> Report {
        reconcile_once(self.store.as_ref(), self.pd.as_ref(), &self.options(owner), &|| true).expect("a pass")
    }

    fn record(&self, key: &ObjectKey) -> Option<tidb_ddl_pdjournal::Record> {
        let mut found = None;
        self.store
            .run_in_new_txn(&tidb_gocontext::Context::background(), "test", false, &mut |txn| {
                found = journal::read_record(&mut TxnRead(txn), key).unwrap();
                Ok(())
            })
            .unwrap();
        found
    }

    fn intents(&self) -> usize {
        let mut count = 0;
        self.store
            .run_in_new_txn(&tidb_gocontext::Context::background(), "test", false, &mut |txn| {
                count = journal::scan_intents(&mut TxnRead(txn)).unwrap().len();
                Ok(())
            })
            .unwrap();
        count
    }

    /// Deletes the record of `key`, as the expiry of a tombstone did before
    /// tombstones were kept.
    fn forget_record(&self, key: &ObjectKey) {
        let (raw, _) = journal::record_mutation(&self.record(key).unwrap());
        self.store
            .run_in_new_txn(&tidb_gocontext::Context::background(), "test", false, &mut |txn| txn.delete(&raw))
            .unwrap();
    }

    fn cutover(&self) {
        journal::seed_cutover(self.store.as_ref(), &BTreeMap::new()).unwrap();
    }
}

/// A job step in flight: its catalog transaction and its prepared journal.
struct Step {
    txn: tidb_store_driver_txn::transaction::TikvTxn<tidb_unistore::mock::StorePdClient>,
    prepared: Prepared,
}

/// Begins a step of `owner` writing `planned`: its transaction begins, the
/// journal is planned against its snapshot and the intents commit. `scope`
/// is the process the owner runs in.
fn begin_step(tenant: &Tenant, owner: &str, scope: u32, planned: &[Planned]) -> Result<Step, String> {
    let mut txn = tenant.opener.begin().map_err(|error| error.to_string())?;
    let start_ts = txn.start_ts();
    let attempt = Attempt { op_id: format!("{owner}/1/{start_ts}"), owner: owner.to_owned(), job_id: 1, scope };
    let prepared = journal::plan(
        &mut SnapshotRead(&mut TransactionMetaSnapshot::new(&mut txn, TIMEOUT)),
        &attempt,
        planned,
        &mut |_| Ok(BTreeMap::new()),
        start_ts >> 18,
    )
    .map_err(|error| error.to_string())?;
    prepared.commit_intents(tenant.store.as_ref())?;
    Ok(Step { txn, prepared })
}

/// The step's PD writes.
fn write_pd(tenant: &Tenant, step: &Step) -> Result<(), String> {
    for intent in step.prepared.intents() {
        tenant.pd.write(&intent.key, intent.value.as_ref())?;
    }
    Ok(())
}

/// Commits the step's catalog transaction with its records; answers
/// whether it committed (false: rolled back by a conflict).
fn commit_step(tenant: &Tenant, step: Step) -> bool {
    let start_ms = step.txn.start_ts() >> 18;
    let mutations = step
        .prepared
        .records(start_ms)
        .iter()
        .map(journal::record_mutation)
        .map(|(key, value)| BufferMutation::set(key, value).unwrap())
        .collect();
    let outcome = step.txn.commit(mutations, &UnaryCallContext::with_timeout(TIMEOUT)).unwrap();
    let committed = matches!(outcome, OptimisticCommitOutcome::Committed(_));
    if committed {
        step.prepared.clear_intents(tenant.store.as_ref()).unwrap();
    }
    committed
}

/// A whole step that commits.
fn run_step(tenant: &Tenant, owner: &str, planned: &[Planned]) {
    let step = begin_step(tenant, owner, tenant.scope, planned).unwrap();
    write_pd(tenant, &step).unwrap();
    assert!(commit_step(tenant, step));
}

fn bundle(tenant: u32, table_id: i64, followers: u64) -> (ObjectKey, PdValue) {
    let settings = tidb_model::placement::PlacementSettings { followers, ..Default::default() };
    let mut bundle = tidb_placement::new_bundle_from_options(Some(&settings)).unwrap();
    bundle.reset(tidb_placement::RULE_INDEX_TABLE, &[table_id]);
    let codec = tidb_domain_infosync::info_syncer::store_codec(tenant);
    let encoded = tidb_domain_infosync::placement_manager::encode_bundle(codec.as_ref(), &bundle).unwrap();
    let (key, value) = bundle_entry(&encoded);
    (key, value.unwrap())
}

fn label(tenant: u32, table: &str, merge: &str) -> (ObjectKey, PdValue) {
    let codec = tidb_domain_infosync::info_syncer::store_codec(tenant).unwrap();
    let mut rule = tidb_ddl_label::Rule::new();
    rule.apply_attributes_spec(&tidb_ast::AttributesSpec { attributes: Some(format!("merge_option={merge}")) })
        .unwrap();
    rule.reset(&codec, "db", table, "", &[100]);
    (ObjectKey::label_rule(&rule.id), label_rule_value(&rule))
}

/// `tenant`'s bundle of the range `range` (global or meta).
fn range_bundle(tenant: u32, range: &str, followers: u64) -> (ObjectKey, PdValue) {
    let settings = tidb_model::placement::PlacementSettings { followers, ..Default::default() };
    let mut bundle = tidb_placement::new_bundle_from_options(Some(&settings)).unwrap();
    bundle.rebuild_for_range(range, "p");
    let codec = tidb_domain_infosync::info_syncer::store_codec(tenant);
    let encoded = tidb_domain_infosync::placement_manager::encode_bundle(codec.as_ref(), &bundle).unwrap();
    let (key, value) = bundle_entry(&encoded);
    (key, value.unwrap())
}

fn group(name: &str, priority: u32) -> (ObjectKey, PdValue) {
    let group = tidb_proto::resource_manager::ResourceGroup {
        name: name.to_owned(),
        priority,
        r_u_settings: Some(tidb_proto::resource_manager::GroupRequestUnitSettings {
            r_u: Some(tidb_proto::resource_manager::TokenBucket {
                settings: Some(tidb_proto::resource_manager::TokenLimitSettings {
                    fill_rate: 100,
                    ..Default::default()
                }),
                tokens: 0.0,
            }),
        }),
        ..Default::default()
    };
    (ObjectKey::resource_group(name), resource_group_value(&group))
}

/// The three kinds' objects of `tenant` in three versions each.
fn kinds(tenant: u32) -> Vec<[(ObjectKey, PdValue); 3]> {
    vec![
        [bundle(tenant, 100, 1), bundle(tenant, 100, 2), bundle(tenant, 100, 3)],
        [label(tenant, "t", "allow"), label(tenant, "t", "deny"), label(tenant, "t", "allow_always")],
        [group("rg1", 1), group("rg1", 2), group("rg1", 3)],
    ]
}

fn set(key: &ObjectKey, value: &PdValue) -> Planned {
    Planned::new(key.clone(), Some(value.clone()))
}

/// Reconciles every 100 ms until PD holds `wanted` under `key`; answers how
/// long it took.
fn converge(tenant: &Tenant, key: &ObjectKey, wanted: Option<&PdValue>) -> Duration {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(10) {
        tenant.reconcile("owner-b");
        if tenant.pd.get(key).as_ref() == wanted {
            return started.elapsed();
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("{key} never converged: PD holds {:?}", tenant.pd.get(key));
}

/// A paused old owner (A) prepared a write and was paused before PD took
/// it; the successor (B) wrote and committed the same object and read it
/// back; A's write lands late and its commit conflicts. With no more DDL,
/// B's reconciler puts B's value back. Then B deletes the object and
/// another of A's delayed writes resurrects it; the reconciler deletes it
/// again. For a bundle, a label rule and a resource group.
#[test]
fn a_paused_old_owners_late_write_is_repaired_and_a_deleted_object_stays_deleted() {
    let shared = Arc::new(SharedPd::default());
    let tenant = Tenant::new(&shared, 1);
    tenant.cutover();
    for [(key, v0), (_, v1), (_, v2)] in kinds(1) {
        run_step(&tenant, "owner-a", &[set(&key, &v0)]);
        // A prepares v1 and is paused; it runs in another process.
        let old = begin_step(&tenant, "owner-a", elsewhere(&tenant), &[set(&key, &v1)]).unwrap();
        let gate = tenant.pd.hold(&key);
        let late = {
            let pd = Arc::clone(&tenant.pd);
            let (key, v1) = (key.clone(), v1.clone());
            std::thread::spawn(move || pd.write(&key, Some(&v1)))
        };
        tenant.pd.wait_held(&key);
        run_step(&tenant, "owner-b", &[set(&key, &v2)]);
        assert_eq!(tenant.pd.get(&key), Some(v2.clone()), "B reads its write back");
        // A's write lands, and its commit conflicts with B's.
        gate.release();
        late.join().unwrap().unwrap();
        assert_eq!(tenant.pd.get(&key), Some(v1.clone()));
        assert!(!commit_step(&tenant, old), "{key}: the old owner's step must not commit");
        let took = converge(&tenant, &key, Some(&v2));
        eprintln!("{key}: converged {took:?} after the late write");
        assert_eq!(tenant.intents(), 0);

        // Resurrection after delete.
        let old = begin_step(&tenant, "owner-a", elsewhere(&tenant), &[set(&key, &v1)]).unwrap();
        run_step(&tenant, "owner-b", &[Planned::new(key.clone(), None)]);
        assert_eq!(tenant.pd.get(&key), None);
        write_pd(&tenant, &old).unwrap();
        assert!(!commit_step(&tenant, old));
        assert_eq!(tenant.pd.get(&key), Some(v1.clone()), "the late write resurrected it");
        converge(&tenant, &key, None);
        assert_eq!(tenant.record(&key).unwrap().value, None, "the tombstone stays");
    }
}

/// A second tenant whose objects have the same logical names (table 100's
/// bundle and rule, group `rg1`) is never written while the first tenant's
/// objects are overwritten, repaired and deleted.
#[test]
fn a_second_tenant_with_the_same_names_is_untouched() {
    let shared = Arc::new(SharedPd::default());
    let first = Tenant::new(&shared, 1);
    let second = Tenant::new(&shared, 2);
    first.cutover();
    second.cutover();
    let theirs = kinds(2).into_iter().map(|[(key, value), _, _]| (key, value)).collect::<Vec<_>>();
    for (key, value) in &theirs {
        run_step(&second, "owner-2", &[set(key, value)]);
    }
    let writes_before = shared.writes.lock().unwrap().len();
    for [(key, v0), (_, v1), (_, v2)] in kinds(1) {
        run_step(&first, "owner-1", &[set(&key, &v0)]);
        first.pd.set(&key, Some(v1.clone()));
        converge(&first, &key, Some(&v0));
        run_step(&first, "owner-1", &[set(&key, &v2)]);
        run_step(&first, "owner-1", &[Planned::new(key.clone(), None)]);
        converge(&first, &key, None);
    }
    for (key, value) in &theirs {
        assert_eq!(second.pd.get(key).as_ref(), Some(value), "{key} of the second tenant");
    }
    let writes = shared.writes.lock().unwrap();
    assert!(writes[writes_before..].iter().all(|(tenant, _)| *tenant == 1), "{writes:?}");
}

/// A step that crashed right after its intents committed (PD untouched), or
/// after PD applied its write but before the reply: the reconciler aborts
/// it, PD gets the record's value back and the record's generation moves
/// past the attempt. One that crashed after its catalog commit keeps its
/// write; its intents go.
#[test]
fn a_crashed_step_is_resolved_from_its_records() {
    let shared = Arc::new(SharedPd::default());
    let tenant = Tenant::new(&shared, 1);
    tenant.cutover();
    for [(key, v0), (_, v1), _] in kinds(1) {
        run_step(&tenant, "owner-a", &[set(&key, &v0)]);
        let generation = tenant.record(&key).unwrap().generation;

        // At intent commit.
        drop(begin_step(&tenant, "owner-a", elsewhere(&tenant), &[set(&key, &v1)]).unwrap());
        let report = tenant.reconcile("owner-b");
        assert_eq!(report.aborted, 1, "{key}: {report:?}");
        assert_eq!(tenant.pd.get(&key), Some(v0.clone()));
        assert_eq!(tenant.record(&key).unwrap().generation, generation + 2, "the record moves above the attempt's generation");

        // After PD applied, before the reply.
        let step = begin_step(&tenant, "owner-a", elsewhere(&tenant), &[set(&key, &v1)]).unwrap();
        tenant.pd.lose_reply.store(true, Ordering::SeqCst);
        assert!(write_pd(&tenant, &step).is_err());
        drop(step);
        assert_eq!(tenant.pd.get(&key), Some(v1.clone()), "the lost reply was applied");
        let report = tenant.reconcile("owner-b");
        assert_eq!((report.aborted, report.repaired.len()), (1, 1), "{key}: {report:?}");
        assert_eq!(tenant.pd.get(&key), Some(v0.clone()));

        // After the catalog commit, before the intents went.
        let step = begin_step(&tenant, "owner-a", elsewhere(&tenant), &[set(&key, &v1)]).unwrap();
        write_pd(&tenant, &step).unwrap();
        let mutations = step
            .prepared
            .records(step.txn.start_ts() >> 18)
            .iter()
            .map(journal::record_mutation)
            .map(|(key, value)| BufferMutation::set(key, value).unwrap())
            .collect();
        let outcome = step.txn.commit(mutations, &UnaryCallContext::with_timeout(TIMEOUT)).unwrap();
        assert!(matches!(outcome, OptimisticCommitOutcome::Committed(_)));
        let report = tenant.reconcile("owner-b");
        assert_eq!((report.committed, report.aborted), (1, 0), "{key}: {report:?}");
        assert_eq!(tenant.pd.get(&key), Some(v1.clone()));
        assert_eq!(tenant.intents(), 0);
    }
}

/// An undetermined commit leaves its intents: the reconciler finds from the
/// record whether the step committed, and keeps or undoes its write.
#[test]
fn an_undetermined_commit_is_resolved_either_way() {
    let shared = Arc::new(SharedPd::default());
    let tenant = Tenant::new(&shared, 1);
    let (key, v0) = group("rg_undetermined", 1);
    let (_, v1) = group("rg_undetermined", 2);
    run_step(&tenant, "owner-a", &[set(&key, &v0)]);
    for landed in [false, true] {
        let step = begin_step(&tenant, "owner-a", elsewhere(&tenant), &[set(&key, &v1)]).unwrap();
        write_pd(&tenant, &step).unwrap();
        if landed {
            let Step { txn, prepared } = step;
            let mutations = prepared
                .records(txn.start_ts() >> 18)
                .iter()
                .map(journal::record_mutation)
                .map(|(key, value)| BufferMutation::set(key, value).unwrap())
                .collect();
            let _ = txn.commit(mutations, &UnaryCallContext::with_timeout(TIMEOUT)).unwrap();
        } else {
            drop(step);
        }
        let report = tenant.reconcile("owner-b");
        let want = if landed { &v1 } else { &v0 };
        assert_eq!(tenant.pd.get(&key).as_ref(), Some(want), "landed {landed}: {report:?}");
        assert_eq!(tenant.record(&key).unwrap().value.as_ref(), Some(want));
        assert_eq!((report.committed, report.aborted), (usize::from(landed), usize::from(!landed)));
    }
}

/// A label rule PD held before the journal: before the cutover the first
/// mutation is refused and PD keeps the rule; after the cutover seeded it
/// from PD, a step that changes it and aborts puts the original back, and
/// one that creates a new rule and aborts leaves it absent.
#[test]
fn an_untracked_rule_is_refused_before_the_cutover_and_restored_after_it() {
    let shared = Arc::new(SharedPd::default());
    let tenant = Tenant::new(&shared, 1);
    let (key, original) = label(1, "t", "deny");
    let (_, changed) = label(1, "t", "allow");
    let (new_key, created) = label(1, "t_new", "allow");
    tenant.pd.set(&key, Some(original.clone()));

    let refused = begin_step(&tenant, "owner-a", tenant.scope, &[set(&key, &changed)]).err().unwrap();
    assert!(refused.contains("PD journal cutover"), "{refused}");
    assert_eq!(tenant.pd.get(&key), Some(original.clone()), "the refused mutation never reached PD");
    assert_eq!(tenant.intents(), 0);

    let objects = BTreeMap::from([(key.clone(), tenant.pd.get(&key).unwrap())]);
    assert_eq!(journal::seed_cutover(tenant.store.as_ref(), &objects).unwrap(), Some(1));
    for (key, value, before) in [(&key, &changed, Some(&original)), (&new_key, &created, None)] {
        let mut delivery = ExternalDelivery::default();
        let txn_reader = &mut tenant.opener.begin().unwrap();
        let start_ts = txn_reader.start_ts();
        delivery
            .prepare(
                StepJournal {
                    store: Arc::clone(&tenant.store),
                    attempt: Attempt { op_id: format!("owner-a/1/{start_ts}"), owner: "owner-a".to_owned(), job_id: 1, scope: tenant.scope },
                },
                &mut SnapshotRead(&mut TransactionMetaSnapshot::new(txn_reader, TIMEOUT)),
                &[set(key, value)],
                &mut |_| Ok(BTreeMap::new()),
                start_ts >> 18,
            )
            .unwrap();
        tenant.pd.write(key, Some(value)).unwrap();
        delivery.compensate(&|| Ok(()), tenant.pd.as_ref()).unwrap();
        assert_eq!(tenant.pd.get(key).as_ref(), before, "{key}");
        assert_eq!(tenant.intents(), 0);
    }
}

/// Two ALTER RANGE sessions prepare the same range bundle: the one whose PD
/// write was held (on another node: this process's writers of an object
/// take turns) finalizes last, is refused, and PD keeps the winner's
/// bundle; only the winner's record committed.
#[test]
fn a_delayed_alter_range_cannot_commit_over_its_successor() {
    let shared = Arc::new(SharedPd::default());
    let tenant = Arc::new(Tenant::new(&shared, 1));
    tenant.cutover();
    let (key, first) = range_bundle(1, tidb_placement::KEY_RANGE_GLOBAL, 1);
    let (_, second) = range_bundle(1, tidb_placement::KEY_RANGE_GLOBAL, 2);
    let gate = tenant.pd.hold(&key);
    let delayed = {
        let tenant = Arc::clone(&tenant);
        let planned = set(&key, &first);
        std::thread::spawn(move || {
            tidb_ddl_pdjournal::direct::write_through(tenant.store.as_ref(), tenant.pd.as_ref(), "session/1", elsewhere(&tenant), &[planned])
        })
    };
    tenant.pd.wait_held(&key);
    tidb_ddl_pdjournal::direct::write_through(tenant.store.as_ref(), tenant.pd.as_ref(), "session/2", tenant.scope, &[set(&key, &second)])
        .unwrap();
    gate.release();
    let refused = delayed.join().unwrap().unwrap_err().to_string();
    assert!(refused.contains("concurrent change"), "{refused}");
    assert_eq!(tenant.pd.get(&key), Some(second.clone()));
    assert_eq!(tenant.record(&key).unwrap().value, Some(second));
    assert_eq!(tenant.intents(), 0);
}

/// GC chose a rule by its record; a job then rewrote the rule under the same
/// name (a new table took the name). GC's delete is refused before PD: the
/// rule PD holds is the new one.
#[test]
fn a_gc_delete_needs_the_generation_it_chose() {
    let shared = Arc::new(SharedPd::default());
    let tenant = Tenant::new(&shared, 1);
    tenant.cutover();
    let (key, dropped) = label(1, "t", "deny");
    let (_, reused) = label(1, "t", "allow");
    run_step(&tenant, "owner-a", &[set(&key, &dropped)]);
    let chosen = tenant.record(&key).map(|record| (record.generation, record.op_id));
    run_step(&tenant, "owner-a", &[set(&key, &reused)]);
    let delete = Planned { key: key.clone(), value: None, expect: Some(chosen) };
    let refused = tidb_ddl_pdjournal::direct::write_through(tenant.store.as_ref(), tenant.pd.as_ref(), "gc", tenant.scope, &[delete])
        .unwrap_err()
        .to_string();
    assert!(refused.contains("changed since it was chosen"), "{refused}");
    assert_eq!(tenant.pd.get(&key), Some(reused));
}

/// The first ALTER RANGE after the cutover: PD times out with the write in
/// flight, and the session's compensation removes the bundle and records
/// the abort -- a tombstone above the attempt's generation, since the range
/// had none. A pass agrees with it and keeps it; when the original write
/// lands later, the next pass reverts it.
#[test]
fn a_timed_out_first_alter_range_is_reverted_when_its_write_lands() {
    let shared = Arc::new(SharedPd::default());
    let tenant = Tenant::new(&shared, 1);
    tenant.cutover();
    let (key, value) = range_bundle(1, tidb_placement::KEY_RANGE_GLOBAL, 1);
    let landing = tenant.pd.delay(&key);
    let failed = tidb_ddl_pdjournal::direct::write_through(tenant.store.as_ref(), tenant.pd.as_ref(), "session/1", tenant.scope, &[set(&key, &value)])
        .unwrap_err()
        .to_string();
    assert!(failed.contains("did not answer"), "{failed}");
    assert_eq!(tenant.pd.get(&key), None);
    assert_eq!(tenant.intents(), 0);
    let record = tenant.record(&key).expect("the abort records the baseline");
    assert_eq!((record.generation, record.value), (2, None));
    let report = tenant.reconcile("owner-b");
    assert_eq!((report.in_sync, report.repaired.len()), (1, 0), "{report:?}");

    landing.release();
    tenant.pd.wait_for(&key, &value);
    let report = tenant.reconcile("owner-b");
    assert_eq!(report.repaired, [key.clone()], "{report:?}");
    assert_eq!(tenant.pd.get(&key), None);
    assert_eq!(tenant.record(&key).unwrap().generation, 2, "the tombstone stays");
}

/// A job step's first write of a label rule after the cutover times out
/// with the write in flight, and the step is definitely aborted: its
/// compensation records the abort, and the reconciler reverts the write
/// when it lands.
#[test]
fn a_timed_out_first_step_write_is_reverted_when_it_lands() {
    let shared = Arc::new(SharedPd::default());
    let tenant = Tenant::new(&shared, 1);
    tenant.cutover();
    let (key, value) = label(1, "t_late", "deny");
    let mut delivery = ExternalDelivery::default();
    let txn_reader = &mut tenant.opener.begin().unwrap();
    let start_ts = txn_reader.start_ts();
    delivery
        .prepare(
            StepJournal {
                store: Arc::clone(&tenant.store),
                attempt: Attempt { op_id: format!("owner-a/1/{start_ts}"), owner: "owner-a".to_owned(), job_id: 1, scope: tenant.scope },
            },
            &mut SnapshotRead(&mut TransactionMetaSnapshot::new(txn_reader, TIMEOUT)),
            &[set(&key, &value)],
            &mut |_| Ok(BTreeMap::new()),
            start_ts >> 18,
        )
        .unwrap();
    let landing = tenant.pd.delay(&key);
    assert!(tenant.pd.write(&key, Some(&value)).is_err());
    delivery.compensate(&|| Ok(()), tenant.pd.as_ref()).unwrap();
    assert_eq!(tenant.pd.get(&key), None);
    assert_eq!(tenant.intents(), 0);
    let record = tenant.record(&key).expect("the abort records the baseline");
    assert_eq!((record.generation, record.value), (2, None));
    // The step is over: its hold on the rule goes.
    drop(delivery);

    landing.release();
    tenant.pd.wait_for(&key, &value);
    let report = tenant.reconcile("owner-b");
    assert_eq!(report.repaired, [key.clone()], "{report:?}");
    assert_eq!(tenant.pd.get(&key), None);
}

/// An ALTER RANGE session of another node prepared against absence, and its
/// PD write was held until after the owner's reconciler aborted its intent:
/// its finalize is refused (the SQL statement fails) and PD gets the
/// tombstone back, which further passes keep. Its intent is gone, so it is
/// refused even where the tombstone is gone too (as its expiry left it
/// before tombstones were kept).
#[test]
fn an_alter_range_the_owner_aborted_never_reports_success() {
    let shared = Arc::new(SharedPd::default());
    let tenant = Arc::new(Tenant::new(&shared, 1));
    tenant.cutover();
    for (range, forget) in [(tidb_placement::KEY_RANGE_GLOBAL, false), (tidb_placement::KEY_RANGE_META, true)] {
        let (key, value) = range_bundle(1, range, 1);
        let gate = tenant.pd.hold(&key);
        let session = {
            let tenant = Arc::clone(&tenant);
            let planned = set(&key, &value);
            std::thread::spawn(move || {
                tidb_ddl_pdjournal::direct::write_through(tenant.store.as_ref(), tenant.pd.as_ref(), "session/1", elsewhere(&tenant), &[planned])
            })
        };
        tenant.pd.wait_held(&key);
        let report = tenant.reconcile("owner-b");
        assert_eq!(report.aborted, 1, "{range}: {report:?}");
        assert_eq!(tenant.record(&key).unwrap().value, None);
        if forget {
            tenant.forget_record(&key);
        } else {
            assert_eq!(tenant.reconcile("owner-b").in_sync, 1);
        }
        gate.release();
        let refused = session.join().unwrap().unwrap_err().to_string();
        assert!(refused.contains("aborted before it committed"), "{range}: {refused}");
        assert_eq!(tenant.pd.get(&key), None, "{range}");
        assert_eq!(tenant.intents(), 0);
        if !forget {
            assert_eq!(tenant.record(&key).unwrap().value, None);
            tenant.reconcile("owner-b");
            assert_eq!(tenant.pd.get(&key), None);
            assert_eq!(tenant.record(&key).unwrap().generation, 2, "the tombstone stays");
        }
    }
}

/// Two non-job writers with the same owner name (GC's is the literal `gc`
/// on every node) get distinct op IDs: each is its prepare transaction's
/// start timestamp.
#[test]
fn a_direct_attempt_is_named_by_its_prepare_timestamp() {
    let shared = Arc::new(SharedPd::default());
    let tenant = Tenant::new(&shared, 1);
    tenant.cutover();
    let (key, value) = range_bundle(1, tidb_placement::KEY_RANGE_GLOBAL, 1);
    let before = tenant.opener.begin().unwrap().start_ts();
    tidb_ddl_pdjournal::direct::write_through(tenant.store.as_ref(), tenant.pd.as_ref(), "gc", tenant.scope, &[set(&key, &value)]).unwrap();
    let after = tenant.opener.begin().unwrap().start_ts();
    let op_id = tenant.record(&key).unwrap().op_id;
    let start_ts = op_id.strip_prefix("gc/direct/").and_then(|ts| ts.parse::<u64>().ok());
    assert!(start_ts.is_some_and(|ts| before < ts && ts < after), "{op_id} is not a timestamp in ({before}, {after})");
}

/// Two independent resource groups that PD lost, one of which PD cannot
/// read: the same pass restores the other and reports the unreadable one,
/// which the next pass restores once PD reads it again.
#[test]
fn an_unreadable_object_does_not_hold_up_the_others_repair() {
    let shared = Arc::new(SharedPd::default());
    let tenant = Tenant::new(&shared, 1);
    let (healthy, healthy_value) = group("rg_healthy", 1);
    let (broken, broken_value) = group("rg_unreadable", 1);
    run_step(&tenant, "owner-a", &[set(&healthy, &healthy_value)]);
    run_step(&tenant, "owner-a", &[set(&broken, &broken_value)]);
    tenant.pd.set(&healthy, None);
    tenant.pd.set(&broken, None);
    tenant.pd.unreadable.lock().unwrap().insert(broken.clone());
    let report = tenant.reconcile("owner-b");
    assert_eq!(report.repaired, [healthy.clone()], "{report:?}");
    assert_eq!(report.failed.iter().map(|(key, _)| key).collect::<Vec<_>>(), [&broken], "{report:?}");
    assert_eq!(tenant.pd.get(&healthy), Some(healthy_value));
    assert_eq!(tenant.pd.get(&broken), None);
    tenant.pd.unreadable.lock().unwrap().clear();
    assert_eq!(tenant.reconcile("owner-b").repaired, [broken.clone()]);
    assert_eq!(tenant.pd.get(&broken), Some(broken_value));
}

/// `tenant`'s TiDB rule of table `table` of `db` over the physical `ids`,
/// as `Rule::reset` builds it.
fn table_rule(tenant: u32, table: &str, merge: &str, ids: &[i64]) -> tidb_ddl_label::Rule {
    let codec = tidb_domain_infosync::info_syncer::store_codec(tenant).unwrap();
    let mut rule = tidb_ddl_label::Rule::new();
    rule.apply_attributes_spec(&tidb_ast::AttributesSpec { attributes: Some(format!("merge_option={merge}")) })
        .unwrap();
    rule.reset(&codec, "db", table, "", ids);
    rule
}

/// A catalog of database `db` whose tables are `tables` (name, physical
/// IDs), for the cutover's check of a rule's ranges.
fn catalog_of(tables: &[(&str, i64)]) -> tidb_ddl_pdjournal::owner::CatalogIds {
    let tables = tables.iter().map(|(name, id)| ((*name).to_owned(), *id)).collect::<HashMap<_, _>>();
    Arc::new(move |db: &str, table: &str, partition: &str| {
        Ok((db == "db" && partition.is_empty()).then(|| tables.get(table).map(|id| vec![*id])).flatten())
    })
}

/// The keyspace's rule ID prefix outside TiDB's (`keyspace/1/` where rules
/// are keyspace aware, else empty).
fn keyspace_prefix(syncer: &tidb_domain_infosync::info_syncer::InfoSyncer) -> String {
    tidb_ddl_label::new_rule_id(syncer.label_codec(), "", "", "").trim_end_matches("schema//").to_owned()
}

/// At the operator's cutover PD holds TiDB's rules of `t` and of `t/x/y`
/// (a name with slashes: the labels carry it whole) and the global range
/// bundle, beside an operator's rule outside TiDB's IDs, an operator rule
/// under a TiDB-shaped ID without TiDB's labels, a rule with TiDB's labels
/// over another table's range, and an operator bundle. Only TiDB's three
/// objects get a record; the two ambiguous rules are held back and
/// reported; editing the operator's rule is never reverted, while a change
/// of TiDB's rule is.
#[test]
fn the_cutover_adopts_only_tidbs_own_rules_and_holds_back_the_ambiguous() {
    use tidb_ddl_pdjournal::owner::seed_from_pd;
    use tidb_ddl_pdjournal::pd::InfoSyncerPd;

    let shared = Arc::new(SharedPd::default());
    let tenant = Tenant::new(&shared, 1);
    let syncer = Arc::new(tidb_domain_infosync::info_syncer::InfoSyncer::new(1));
    let pd = InfoSyncerPd(Arc::clone(&syncer));
    let prefix = keyspace_prefix(&syncer);
    let catalog = catalog_of(&[("t", 100), ("t/x/y", 101), ("t_range", 102)]);
    let rule_entry = |rule: &tidb_ddl_label::Rule| (ObjectKey::label_rule(&rule.id), label_rule_value(rule));
    let tidb = [rule_entry(&table_rule(1, "t", "deny", &[100])), rule_entry(&table_rule(1, "t/x/y", "deny", &[101]))];
    let (range_key, range_value) = range_bundle(1, tidb_placement::KEY_RANGE_GLOBAL, 1);
    let mut foreign = table_rule(1, "t_operator", "deny", &[200]);
    foreign.id = format!("{prefix}operator/hot");
    // A TiDB-shaped ID whose rule carries only the operator's label.
    let mut unlabeled = rule_entry(&table_rule(1, "t_op", "deny", &[103]));
    let only_merge = unlabeled.1["labels"].as_array().unwrap().iter().filter(|label| label["key"] == "merge_option").cloned().collect();
    unlabeled.1["labels"] = PdValue::Array(only_merge);
    let wrong_range = table_rule(1, "t_range", "deny", &[999]);
    let ambiguous = [unlabeled, rule_entry(&wrong_range)];
    let foreign = rule_entry(&foreign);
    let mut operator_bundle = range_value.clone();
    operator_bundle["group_id"] = PdValue::String("operator_group".to_owned());
    let operator_bundle_key = ObjectKey::placement_bundle("operator_group");
    let everything = tidb
        .iter()
        .chain(&ambiguous)
        .chain([&foreign])
        .cloned()
        .chain([(range_key.clone(), range_value), (operator_bundle_key.clone(), operator_bundle)])
        .collect::<Vec<_>>();
    for (key, value) in &everything {
        pd.write(key, Some(value)).unwrap();
    }
    let keys = everything.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
    assert_eq!(pd.read(&keys).unwrap().len(), keys.len(), "PD holds every object");

    let report = seed_from_pd(tenant.store.as_ref(), &syncer, &catalog).unwrap();
    assert!(report.first);
    assert_eq!(report.seeded, 3, "{report:?}");
    let held = report.held_back.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
    assert_eq!(held, ambiguous.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>(), "{report:?}");
    for (key, _) in &tidb {
        assert!(tenant.record(key).is_some(), "{key} is TiDB's");
    }
    assert!(tenant.record(&range_key).is_some());
    for key in [&operator_bundle_key, &foreign.0, &ambiguous[0].0, &ambiguous[1].0] {
        assert_eq!(tenant.record(key), None, "{key} is not adopted");
    }
    let mut journaled = Vec::new();
    tenant
        .store
        .run_in_new_txn(&tidb_gocontext::Context::background(), "test", false, &mut |txn| {
            journaled = journal::scan_held_back(&mut TxnRead(txn)).unwrap();
            Ok(())
        })
        .unwrap();
    assert_eq!(journaled.into_iter().map(|held| held.key).collect::<Vec<_>>(), held, "held back durably");

    let mut edited = foreign.1.clone();
    edited["labels"] = PdValue::Array(Vec::new());
    pd.write(&foreign.0, Some(&edited)).unwrap();
    let (_, changed) = rule_entry(&table_rule(1, "t", "allow", &[100]));
    pd.write(&tidb[0].0, Some(&changed)).unwrap();
    let report = reconcile_once(tenant.store.as_ref(), &pd, &tenant.options("owner-b"), &|| true).unwrap();
    assert_eq!(report.repaired, [tidb[0].0.clone()], "{report:?}");
    let held = pd.read(&keys).unwrap();
    assert!(tidb_ddl_pdjournal::normalize::in_sync(&foreign.0, Some(&edited), held.get(&foreign.0), Some(1)).unwrap());
    assert!(tidb_ddl_pdjournal::normalize::in_sync(&tidb[0].0, Some(&tidb[0].1), held.get(&tidb[0].0), Some(1)).unwrap());
}

/// A rule the cutover held back (TiDB's labels for `t`, another table's
/// range): after the journal's store is reopened by another DDL owner, a
/// job step that changes or drops `t`'s attributes, and ALTER-RANGE-style
/// direct writes, are refused before PD, and PD keeps the rule. Once the
/// operator removes the rule from PD and runs the cutover again, it is
/// resolved as absent and the step goes through.
#[test]
fn a_rule_held_back_at_the_cutover_refuses_writes_after_a_restart() {
    use tidb_ddl_pdjournal::owner::seed_from_pd;
    use tidb_ddl_pdjournal::pd::InfoSyncerPd;

    let shared = Arc::new(SharedPd::default());
    let tenant = Tenant::new(&shared, 1);
    let syncer = Arc::new(tidb_domain_infosync::info_syncer::InfoSyncer::new(1));
    let pd = InfoSyncerPd(Arc::clone(&syncer));
    let catalog = catalog_of(&[("t", 100)]);
    let stale = table_rule(1, "t", "deny", &[999]);
    let key = ObjectKey::label_rule(&stale.id);
    let stale = label_rule_value(&stale);
    pd.write(&key, Some(&stale)).unwrap();
    let report = seed_from_pd(tenant.store.as_ref(), &syncer, &catalog).unwrap();
    assert_eq!(report.held_back.len(), 1, "{report:?}");

    // Another owner, over a store opened afresh.
    let store: Arc<dyn tidb_meta::MetaStore> =
        Arc::new(tidb_store_driver_txn::meta_txn::MetaStore::new(Arc::clone(&tenant.opener), TIMEOUT));
    let step = |value: Option<PdValue>| {
        let mut delivery = ExternalDelivery::default();
        let txn_reader = &mut tenant.opener.begin().unwrap();
        let start_ts = txn_reader.start_ts();
        delivery
            .prepare(
                StepJournal {
                    store: Arc::clone(&store),
                    attempt: Attempt { op_id: format!("owner-c/2/{start_ts}"), owner: "owner-c".to_owned(), job_id: 2, scope: elsewhere(&tenant) },
                },
                &mut SnapshotRead(&mut TransactionMetaSnapshot::new(txn_reader, TIMEOUT)),
                &[Planned::new(key.clone(), value)],
                &mut |_| Ok(BTreeMap::new()),
                start_ts >> 18,
            )
            .map(|_| delivery)
    };
    let (_, changed) = label(1, "t", "allow");
    for value in [Some(changed.clone()), None] {
        let refused = step(value).err().expect("a held-back rule is refused");
        assert!(refused.contains("held back at the PD journal cutover"), "{refused}");
    }
    let direct = tidb_ddl_pdjournal::direct::write_through(store.as_ref(), &pd, "gc", elsewhere(&tenant), &[Planned::new(key.clone(), None)]);
    assert!(matches!(direct, Err(tidb_ddl_pdjournal::JournalError::HeldBack(..))), "{direct:?}");
    assert_eq!(pd.read(std::slice::from_ref(&key)).unwrap().get(&key), Some(&stale), "PD keeps the rule");
    assert_eq!(tenant.intents(), 0);

    // The operator deletes the rule from PD and runs the cutover again.
    pd.write(&key, None).unwrap();
    let report = seed_from_pd(store.as_ref(), &syncer, &catalog).unwrap();
    assert_eq!((report.first, report.seeded, report.held_back.len()), (false, 1, 0), "{report:?}");
    assert_eq!(tenant.record(&key).unwrap().value, None);
    drop(step(Some(changed)).expect("a resolved rule is written"));
}

/// A pass finds a record PD differs from, and a writer of this process
/// commits a newer value and releases its hold right after the pass's
/// repair would have checked the record (the hook runs after the first
/// journal read that follows the pass's PD read). The repair must not put
/// the older record back over the writer's value: it checks the record
/// under the object's lock, so either it sees the newer record or the
/// writer waits for the repair and writes after it.
#[test]
fn a_repair_never_writes_back_a_value_a_writer_replaced() {
    let shared = Arc::new(SharedPd::default());
    let tenant = Arc::new(Tenant::new(&shared, 1));
    let (key, v1) = group("rg_race", 1);
    let (_, late) = group("rg_race", 2);
    let (_, v3) = group("rg_race", 3);
    run_step(&tenant, "owner-a", &[set(&key, &v1)]);
    tenant.pd.set(&key, Some(late));
    let store = Arc::new(HookedStore { inner: Arc::clone(&tenant.store), after_next: Mutex::default(), skip: Default::default() });
    let (done, written) = std::sync::mpsc::channel();
    let writer = Arc::new(Mutex::new(None));
    *tenant.pd.after_read.lock().unwrap() = Some(Box::new({
        let (store, tenant, writer, key, v3) = (Arc::clone(&store), Arc::clone(&tenant), Arc::clone(&writer), key.clone(), v3.clone());
        move || {
            *store.after_next.lock().unwrap() = Some(Box::new(move || {
                *writer.lock().unwrap() = Some(std::thread::spawn(move || {
                    run_step(&tenant, "owner-a", &[set(&key, &v3)]);
                    // The hook stopped waiting when the writer was blocked.
                    let _ = done.send(());
                }));
                // Long enough for an unblocked writer to finish.
                let _ = written.recv_timeout(Duration::from_millis(500));
            }));
        }
    }));
    let report = reconcile_once(store.as_ref(), tenant.pd.as_ref(), &tenant.options("owner-b"), &|| true).unwrap();
    writer.lock().unwrap().take().expect("the writer ran").join().unwrap();
    assert_eq!(tenant.record(&key).unwrap().value.as_ref(), Some(&v3));
    assert_eq!(tenant.pd.get(&key).as_ref(), Some(&v3), "{report:?}");
    assert_eq!(tenant.reconcile("owner-b").repaired, Vec::<ObjectKey>::new());
}

/// A GC delete of two label rules in another process: the owner's
/// reconciler aborts the first intent while the deletes are held in PD,
/// then steps down. The session's finalize is refused, and it resolves the
/// whole attempt: right after the failed call, both rules are back in PD as
/// their records hold them and no intent is left.
#[test]
fn a_partly_aborted_two_object_delete_is_resolved_whole() {
    let shared = Arc::new(SharedPd::default());
    let tenant = Arc::new(Tenant::new(&shared, 1));
    tenant.cutover();
    let rules = [label(1, "t1", "deny"), label(1, "t2", "deny")];
    for (key, value) in &rules {
        run_step(&tenant, "owner-a", &[set(key, value)]);
    }
    let planned = rules
        .iter()
        .map(|(key, _)| Planned {
            key: key.clone(),
            value: None,
            expect: Some(tenant.record(key).map(|record| (record.generation, record.op_id))),
        })
        .collect::<Vec<_>>();
    let gate = tenant.pd.hold(&rules[0].0);
    let session = {
        let tenant = Arc::clone(&tenant);
        std::thread::spawn(move || {
            tidb_ddl_pdjournal::direct::write_through(tenant.store.as_ref(), tenant.pd.as_ref(), "gc", elsewhere(&tenant), &planned)
        })
    };
    tenant.pd.wait_held(&rules[0].0);
    let checks = std::sync::atomic::AtomicUsize::new(0);
    let report = reconcile_once(tenant.store.as_ref(), tenant.pd.as_ref(), &tenant.options("owner-b"), &|| {
        checks.fetch_add(1, Ordering::SeqCst) == 0
    })
    .unwrap();
    assert_eq!((report.aborted, tenant.intents()), (1, 1), "the owner aborted one intent and stepped down: {report:?}");
    gate.release();
    let refused = session.join().unwrap().unwrap_err().to_string();
    assert!(refused.contains("aborted before it committed"), "{refused}");
    assert_eq!(tenant.intents(), 0);
    for (key, value) in &rules {
        let record = tenant.record(key).unwrap();
        assert_eq!(record.value.as_ref(), Some(value), "{key}");
        assert_eq!(tenant.pd.get(key).as_ref(), Some(value), "{key}");
    }
}

/// The production catalog lookup of the cutover reads a table's physical
/// IDs by its lower-case names from a bootstrapped catalog, and answers
/// `None` for a table or partition it does not hold.
#[test]
fn the_cutover_reads_a_rules_table_ids_from_the_catalog() {
    let shared = Arc::new(SharedPd::default());
    let tenant = Tenant::new(&shared, 1);
    tidb_session::bootstrap_publish::publish_bootstrap(&tenant.opener, TIMEOUT).unwrap();
    let mut txn = tenant.opener.begin().unwrap();
    let mut snapshot = TransactionMetaSnapshot::new(&mut txn, TIMEOUT);
    let ids = super::pd_baseline::rule_table_ids(&mut snapshot, "mysql", "tidb_ddl_job", "").unwrap();
    assert!(ids.is_some_and(|ids| ids.len() == 1 && ids[0] > 0));
    assert_eq!(super::pd_baseline::rule_table_ids(&mut snapshot, "mysql", "no_such_table", "").unwrap(), None);
    assert_eq!(super::pd_baseline::rule_table_ids(&mut snapshot, "mysql", "tidb_ddl_job", "p0").unwrap(), None);
    assert_eq!(super::pd_baseline::rule_table_ids(&mut snapshot, "no_such_db", "tidb_ddl_job", "").unwrap(), None);
}

/// An ALTER RANGE whose PD write timed out is abandoned: it reads the
/// range's record to put it back, and right then a second session of the
/// same process commits a newer ALTER RANGE (the hook runs after the
/// abandon's record read, before its PD write). PD must end with the newer
/// value: the abandoning session writes its objects alone until it put PD
/// back, so the second one writes after it.
#[test]
fn an_abandoned_alter_range_never_overwrites_a_newer_one() {
    use tidb_ddl_pdjournal::direct::write_through;

    let shared = Arc::new(SharedPd::default());
    let tenant = Arc::new(Tenant::new(&shared, 1));
    tenant.cutover();
    let (key, v0) = range_bundle(1, tidb_placement::KEY_RANGE_GLOBAL, 1);
    let (_, v1) = range_bundle(1, tidb_placement::KEY_RANGE_GLOBAL, 2);
    let (_, v2) = range_bundle(1, tidb_placement::KEY_RANGE_GLOBAL, 3);
    write_through(tenant.store.as_ref(), tenant.pd.as_ref(), "session/1", tenant.scope, &[set(&key, &v0)]).unwrap();
    let landing = tenant.pd.delay(&key);
    // The abandoning session's transactions: its prepare, its abort, then
    // the record read the hook follows.
    let store = Arc::new(HookedStore { inner: Arc::clone(&tenant.store), after_next: Mutex::default(), skip: 2.into() });
    let second = Arc::new(Mutex::new(None));
    *store.after_next.lock().unwrap() = Some(Box::new({
        let (tenant, second, key, v2) = (Arc::clone(&tenant), Arc::clone(&second), key.clone(), v2.clone());
        move || {
            let (done, finished) = std::sync::mpsc::channel();
            *second.lock().unwrap() = Some(std::thread::spawn(move || {
                let written = write_through(tenant.store.as_ref(), tenant.pd.as_ref(), "session/2", tenant.scope, &[set(&key, &v2)]);
                let _ = done.send(());
                written
            }));
            // Long enough for an unordered second session to commit.
            let _ = finished.recv_timeout(Duration::from_millis(500));
        }
    }));
    let failed = write_through(store.as_ref(), tenant.pd.as_ref(), "session/1", tenant.scope, &[set(&key, &v1)]);
    assert!(failed.is_err());
    second.lock().unwrap().take().expect("the second session ran").join().unwrap().unwrap();
    assert_eq!(tenant.record(&key).unwrap().value.as_ref(), Some(&v2));
    assert_eq!(tenant.pd.get(&key).as_ref(), Some(&v2), "the newer ALTER RANGE stays in PD");
    assert_eq!(tenant.intents(), 0);

    // The timed-out write lands at last; the reconciler reverts it.
    landing.release();
    tenant.pd.wait_for(&key, &v1);
    tenant.reconcile("owner-b");
    assert_eq!(tenant.pd.get(&key).as_ref(), Some(&v2));
}
