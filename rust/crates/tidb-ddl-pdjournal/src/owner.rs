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

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tidb_domain_infosync::info_syncer::InfoSyncer;

use crate::pd::{bundle_entry, label_rule_value, InfoSyncerPd};
use crate::{ObjectKey, PdValue};

/// The variable that carries the operator's cutover: `*`, or the
/// comma-separated IDs of the keyspaces whose writers that bypass the journal
/// (nodes of an older build) the operator has drained. Named again after
/// the cutover, it re-checks the objects the cutover held back.
pub const CUTOVER_VARIABLE: &str = "TIDB_PD_JOURNAL_CUTOVER";

fn cutover_asked(keyspace_id: Option<u32>) -> bool {
    std::env::var(CUTOVER_VARIABLE).is_ok_and(|ids| {
        ids.split(',')
            .map(str::trim)
            .any(|id| id == "*" || keyspace_id.is_some_and(|keyspace_id| id == keyspace_id.to_string()))
    })
}

/// One table of a database as the cutover checks a TiDB label rule against
/// it: its lower-case name, its physical ID, and its partitions'
/// lower-case names and IDs in definition order.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CatalogTable {
    /// Go `tblInfo.Name.L`.
    pub name: String,
    /// Go `tblInfo.ID`.
    pub id: i64,
    /// The partitions' `Name.L` and `ID`.
    pub partitions: Vec<(String, i64)>,
}

/// The catalog's tables of the database the argument names (lower-case),
/// read afresh; `None` when the catalog holds no such database.
pub type CatalogTables = Arc<dyn Fn(&str) -> Result<Option<Vec<CatalogTable>>, String> + Send + Sync>;

/// One database's tables by lower-case name, `None` for a database the
/// catalog does not hold.
type Tables = Option<HashMap<String, CatalogTable>>;

/// One cutover run's reads of the catalog: each database's tables are
/// listed once, indexed by name, and borrowed for every rule in it, so a
/// cutover over many rules costs one listing per database and one lookup
/// per rule.
struct Catalog<'a> {
    tables: &'a CatalogTables,
    by_db: HashMap<String, Result<Tables, String>>,
}

impl<'a> Catalog<'a> {
    fn new(tables: &'a CatalogTables) -> Self {
        Self { tables, by_db: HashMap::new() }
    }

    /// The physical IDs a TiDB rule of `(db, table, partition)` covers: the
    /// table's and then its partitions' (Go `getIDs`) for a table rule
    /// (partition ""), the partition's for a partition rule; `None` when
    /// the catalog holds no such table or partition.
    fn ids(&mut self, db: &str, table: &str, partition: &str) -> Result<Option<Vec<i64>>, String> {
        if !self.by_db.contains_key(db) {
            let listed = (self.tables)(db)
                .map(|tables| tables.map(|tables| tables.into_iter().map(|table| (table.name.clone(), table)).collect()));
            self.by_db.insert(db.to_owned(), listed);
        }
        let listed = self.by_db[db].as_ref().map_err(Clone::clone)?;
        let Some(found) = listed.as_ref().and_then(|tables| tables.get(table)) else {
            return Ok(None);
        };
        if partition.is_empty() {
            return Ok(Some(std::iter::once(found.id).chain(found.partitions.iter().map(|(_, id)| *id)).collect()));
        }
        Ok(found.partitions.iter().find(|(name, _)| name == partition).map(|(_, id)| vec![*id]))
    }
}

/// What the cutover makes of a label rule PD holds.
#[derive(Debug, PartialEq)]
enum Ownership {
    /// TiDB's rule of a table or partition of this keyspace: adopted.
    Tidb,
    /// Its ID has TiDB's shape (`[keyspace/<id>/]schema/...`), but it is not
    /// provably TiDB's: held back, with why.
    Ambiguous(String),
    /// Not under TiDB's rule IDs: the operator's (or PD's) own, left alone.
    Foreign,
}

/// Whether `rule` is TiDB's own for this keyspace. `Rule::reset` keeps the
/// database, table, partition and keyspace in labels and sets every other
/// field TiDB writes, so the rule is TiDB's only when its ID is the one
/// [`tidb_ddl_label::new_rule_id`] rebuilds from those labels for the
/// keyspace's codec and the whole rule equals the one `Rule::reset` builds
/// from its own labels over the catalog's physical IDs of that table or
/// partition: its index (2 for a table, 3 for a partition), type, reserved
/// labels (the keyspace's on a keyspace-aware codec) and key ranges. The
/// expected rule is decoded afresh from the PD rule, so resetting it never
/// touches the rule it is compared with (a `Rule` clone shares its labels).
/// A rule under TiDB's ID prefix that fails any of that is ambiguous.
fn ownership(syncer: &InfoSyncer, catalog: &mut Catalog<'_>, rule: &tidb_ddl_label::Rule) -> Ownership {
    let codec = syncer.label_codec();
    let prefix = tidb_ddl_label::new_rule_id(codec, "", "", "");
    if !rule.id.starts_with(&format!("{}/", prefix.trim_end_matches('/'))) {
        return Ownership::Foreign;
    }
    let ambiguous = |reason: String| Ownership::Ambiguous(reason);
    let labels = rule.labels.snapshot();
    let label = |key: &str| labels.iter().find(|label| label.key == key).map(|label| label.value.clone());
    let (Some(db), Some(table)) = (label("db"), label("table")) else {
        return ambiguous("it has no db and table labels".to_owned());
    };
    let partition = label("partition").unwrap_or_default();
    let rebuilt = tidb_ddl_label::new_rule_id(codec, &db, &table, &partition);
    if rebuilt != rule.id {
        return ambiguous(format!("its labels make the rule ID {rebuilt}"));
    }
    let ids = match catalog.ids(&db, &table, &partition) {
        Ok(Some(ids)) => ids,
        Ok(None) => return ambiguous("the catalog holds no such table or partition".to_owned()),
        Err(error) => return ambiguous(format!("the catalog could not be read: {error}")),
    };
    let held = label_rule_value(rule);
    let mut expected = match serde_json::from_value::<tidb_ddl_label::Rule>(held.clone()) {
        Ok(expected) => expected,
        Err(error) => return ambiguous(format!("it does not decode: {error}")),
    };
    expected.reset(codec, &db, &table, &partition, &ids);
    let key = ObjectKey::label_rule(&rule.id);
    let project = |value: &PdValue| crate::normalize::projection(&key, value, syncer.keyspace_id());
    match (project(&label_rule_value(&expected)), project(&held)) {
        (Ok(wanted), Ok(held)) if wanted == held => Ownership::Tidb,
        (Ok(wanted), Ok(held)) => {
            let differs = ["index", "rule_type", "labels", "data"]
                .into_iter()
                .filter(|field| wanted.get(field) != held.get(field))
                .map(|field| if field == "data" { "key ranges" } else { field })
                .collect::<Vec<_>>();
            ambiguous(format!(
                "it differs from the rule TiDB builds for physical tables {ids:?} in its {}",
                if differs.is_empty() { "fields".to_owned() } else { differs.join(", ") }
            ))
        }
        (Err(error), _) | (_, Err(error)) => ambiguous(format!("it could not be compared: {error}")),
    }
}

/// The keyspace's PD-only objects as PD holds them now: TiDB's own (its
/// tables' and partitions' label rules ([`ownership`]) and its range
/// bundles, read by their own group IDs only, so no other group is
/// adopted), and the label rules held back with why.
type PdOnly = (BTreeMap<ObjectKey, PdValue>, BTreeMap<ObjectKey, String>);

fn pd_only_objects(syncer: &InfoSyncer, catalog: &mut Catalog<'_>) -> Result<PdOnly, String> {
    let (mut objects, mut held_back) = (BTreeMap::new(), BTreeMap::new());
    let rules = syncer.label_rule_manager().get_all_label_rules(syncer.label_codec())?;
    for rule in tidb_domain_infosync::label_manager::filter_rules_by_keyspace(rules, syncer.label_codec()) {
        match ownership(syncer, catalog, &rule) {
            Ownership::Tidb => {
                objects.insert(ObjectKey::label_rule(&rule.id), label_rule_value(&rule));
            }
            Ownership::Ambiguous(reason) => {
                held_back.insert(ObjectKey::label_rule(&rule.id), reason);
            }
            Ownership::Foreign => {}
        }
    }
    for range in [tidb_placement::KEY_RANGE_GLOBAL, tidb_placement::KEY_RANGE_META] {
        let mut bundle = tidb_placement::Bundle::default();
        bundle.rebuild_for_range(range, "");
        let (key, _) = bundle_entry(&syncer.pd_bundle(&bundle)?);
        if let (_, Some(value)) = bundle_entry(&syncer.placement_manager().get_rule_bundle(&key.id)?) {
            objects.insert(key, value);
        }
    }
    Ok((objects, held_back))
}

/// What one run of the operator's cutover did.
#[derive(Debug, Default, PartialEq)]
pub struct CutoverReport {
    /// Whether this run was the keyspace's cutover (false: an earlier one
    /// had run, and this one re-checked the objects it held back).
    pub first: bool,
    /// The records it seeded (or, re-checking, the held-back objects it
    /// resolved).
    pub seeded: usize,
    /// The objects held back, with why: every write of one is refused
    /// until the operator resolves it.
    pub held_back: Vec<(ObjectKey, String)>,
}

/// The operator's cutover of the keyspace `syncer` serves, from what PD
/// holds now. It reads the cutover marker and the held-back objects first.
/// On the first cutover: a record of each of TiDB's PD-only objects, and
/// each label rule of TiDB's ID shape that is not provably TiDB's
/// ([`ownership`], against `catalog`, which it lists once per database)
/// held back durably ([`crate::journal::seed_cutover_holding_back`]).
/// After an earlier cutover it reads only the held-back rules from PD,
/// which the operator has fixed there: one that is now TiDB's is adopted,
/// one PD no longer holds gets a tombstone, and the others stay held back
/// ([`crate::journal::resolve_held_back`]); with none held back it reads
/// neither PD nor the catalog.
///
/// # Errors
/// PD or the journal could not be read, or the seeding not committed.
pub fn seed_from_pd(
    store: &dyn tidb_meta::MetaStore,
    syncer: &InfoSyncer,
    catalog: &CatalogTables,
) -> Result<CutoverReport, String> {
    let (mut cutover, mut pending) = (false, Vec::new());
    crate::journal::run(store, &mut |txn| {
        let mut reader = crate::TxnRead(txn);
        cutover = crate::journal::read_cutover(&mut reader)?.is_some();
        pending = crate::journal::scan_held_back(&mut reader)?;
        Ok(())
    })?;
    let mut catalog = Catalog::new(catalog);
    if !cutover {
        let (objects, held_back) = pd_only_objects(syncer, &mut catalog)?;
        let seeded = crate::journal::seed_cutover_holding_back(store, &objects, &held_back)?;
        return Ok(match seeded {
            Some(seeded) => CutoverReport { first: true, seeded, held_back: held_back.into_iter().collect() },
            // Another owner's cutover committed meanwhile.
            None => CutoverReport::default(),
        });
    }
    if pending.is_empty() {
        return Ok(CutoverReport::default());
    }
    let ids = pending.iter().map(|held| held.key.id.clone()).collect::<Vec<_>>();
    let rules = syncer.label_rule_manager().get_label_rules(&ids)?;
    let mut resolved = BTreeMap::new();
    let mut still = Vec::new();
    for held in pending {
        match rules.get(&held.key.id) {
            None => {
                resolved.insert(held.key, None);
            }
            Some(rule) => match ownership(syncer, &mut catalog, rule) {
                Ownership::Tidb => {
                    resolved.insert(held.key, Some(label_rule_value(rule)));
                }
                Ownership::Ambiguous(reason) => still.push((held.key, reason)),
                Ownership::Foreign => still.push((held.key, held.reason)),
            },
        }
    }
    let seeded = crate::journal::resolve_held_back(store, &resolved)?;
    Ok(CutoverReport { first: false, seeded, held_back: still })
}

/// The operator's cutover of the keyspace `syncer` serves, when
/// [`CUTOVER_VARIABLE`] names it ([`seed_from_pd`]). No attempt of this
/// build writes the keyspace's PD-only objects before (they are refused
/// until the cutover); the variable is the operator's word that no older
/// node does either. Each object held back is logged for the operator.
fn cutover_if_asked(store: &dyn tidb_meta::MetaStore, syncer: &InfoSyncer, catalog: &CatalogTables) {
    use tidb_log::{Field, Value};
    if !cutover_asked(syncer.keyspace_id()) {
        return;
    }
    let keyspace = Field::new("keyspace", Value::Str(format!("{:?}", syncer.keyspace_id())));
    let logger = tidb_logutil::bg_logger();
    match seed_from_pd(store, syncer, catalog) {
        Ok(report) => {
            if report.first || report.seeded > 0 {
                logger.info(
                    "PD journal cutover: seeded the keyspace's PD-only objects",
                    &[keyspace.clone(), Field::new("seeded", Value::U64(report.seeded as u64))],
                );
            }
            for (key, reason) in &report.held_back {
                logger.warn(
                    "PD journal cutover held an object back: TiDB refuses to write it until the operator resolves it",
                    &[keyspace.clone(), Field::new("object", Value::Str(key.to_string())), Field::new("reason", Value::Str(reason.clone()))],
                );
            }
        }
        Err(error) => logger.warn(
            "PD journal cutover failed; label rules and range bundles stay refused",
            &[keyspace, Field::new("error", Value::Str(error))],
        ),
    }
}

/// The DDL owner's PD journal duties over the keyspace's meta `store`, as
/// a listener of its owner manager beside the job scheduler (`scheduler`):
/// at a takeover, the operator's cutover (which reads table IDs from
/// `catalog`) and then the reconciler of the keyspace `info_syncer`
/// answers, which stops at the retirement or once the node no longer owns
/// the DDL.
pub fn with_scheduler(
    store: Arc<dyn tidb_meta::MetaStore>,
    info_syncer: SyncerOf,
    catalog: CatalogTables,
    owner: &Arc<dyn tidb_owner::Manager>,
    scheduler: Arc<dyn tidb_owner::Listener>,
) -> Arc<dyn tidb_owner::Listener> {
    let duties = PdJournalDuties {
        store,
        info_syncer,
        catalog,
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
    catalog: CatalogTables,
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
        let catalog = Arc::clone(&self.catalog);
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
            cutover_if_asked(store.as_ref(), &syncer, &catalog);
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::{Catalog, CatalogTable, CatalogTables};

    /// A cutover over thousands of rules of one database lists the database
    /// once and finds each rule's table by name; a partition and a missing
    /// table or database answer as before.
    #[test]
    fn the_catalog_is_listed_once_per_database_and_indexed_by_name() {
        let listed = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&listed);
        let tables: CatalogTables = Arc::new(move |db: &str| {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok((db == "db").then(|| {
                (0..4_000)
                    .map(|index| CatalogTable {
                        name: format!("t{index}"),
                        id: 100 + index,
                        partitions: vec![(format!("p{index}"), 10_000 + index)],
                    })
                    .collect()
            }))
        });
        let mut catalog = Catalog::new(&tables);
        for index in 0..4_000 {
            let ids = catalog.ids("db", &format!("t{index}"), "").unwrap();
            assert_eq!(ids, Some(vec![100 + index, 10_000 + index]));
        }
        assert_eq!(catalog.ids("db", "t7", "p7").unwrap(), Some(vec![10_007]));
        assert_eq!(catalog.ids("db", "t7", "p8").unwrap(), None);
        assert_eq!(catalog.ids("db", "missing", "").unwrap(), None);
        assert_eq!(listed.load(Ordering::SeqCst), 1);
        assert_eq!(catalog.ids("other", "t1", "").unwrap(), None);
        assert_eq!(listed.load(Ordering::SeqCst), 2);
    }
}
