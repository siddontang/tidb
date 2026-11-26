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

//! The production collaborators of Go's GC worker on a node against TiKV and
//! PD -- its sessions (Go `session.CreateSession`), placement rules and store
//! client -- and its start at boot (Go `store/driver` -> `gcworker.NewGCWorker`
//! + `Start`), which the server's main runs.
//!
//! The store's keyspace metadata selects Go's full or unified-GC procedure.

use tidb_session::domain::cluster_session_node::DomainSessions as _;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use crate::seams::{GcError, GcResult, GcRuleClient, GcSession, GcSessionFactory};
use tidb_ddl::ddl_delete_range::DelRangeTask;
use crate::tikv_store::TikvStoreClient;
use crate::{GcWorker, GcWorkerDeps, GcWorkerHandle};
use tidb_sqlescape::SqlArg;

use tidb_session::domain::cluster_session_node::SystemSqlExecutor;
use tidb_domain_core::domain::Domain;
use tidb_session::domain::session_api::SessionContext;

/// How long one internal request to PD or TiKV may take before the worker
/// gives up on it.
const GC_RPC_TIMEOUT: Duration = Duration::from_secs(60);

/// A started GC worker. Dropping it stops the worker (Go `GCWorker.Close`).
pub struct NodeGcWorker {
    _handle: GcWorkerHandle,
}

/// Go `gcworker.NewGCWorker(store, pdClient)` and `Start` for this node, over
/// the node's one store.
pub fn start_gc_worker(
    factory: &Arc<Domain>,
    store: &tidb_txnkv::driver::tikv_store::TikvStore,
    opener: Arc<tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener>,
) -> Result<NodeGcWorker, String> {
    let worker = GcWorker::new(gc_worker_deps(factory, store, opener)).map_err(|error| error.to_string())?;
    Ok(NodeGcWorker {
        _handle: worker.start(),
    })
}

/// The store, PD and sessions this node's GC worker (and Go's test API's
/// `RunResolveLocks`) runs over.
pub fn gc_worker_deps<S: tidb_store_driver_txn::transaction::StoreClient + 'static>(
    factory: &Arc<Domain>,
    store: &tidb_txnkv::driver::tikv_store::TikvStore<S>,
    opener: Arc<tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener<S>>,
) -> GcWorkerDeps {
    let client = Arc::new(TikvStoreClient::new(
        Arc::clone(store.runtime()),
        store.store().clone(),
        store.store().keyspace_meta().cloned(),
    ));
    let journal_opener = Arc::clone(&opener);
    GcWorkerDeps {
        store: client.clone(),
        pd: client.clone(),
        kv: client,
        sessions: Arc::new(NodeGcSessions::new(
            factory,
            Arc::new(move |job_id| {
                tidb_ddl::real_tikv_ddl::load_history_persisted_ddl_job(
                    Arc::clone(&opener),
                    job_id,
                    GC_RPC_TIMEOUT,
                )
                .map_err(|error| error.to_string())
            }),
        )),
        rules: Arc::new(NodeGcRules {
            journal: Arc::new(tidb_store_driver_txn::meta_txn::MetaStore::new(Arc::clone(&journal_opener), GC_RPC_TIMEOUT)),
            // Go `w.store.GetCodec()`: the keyspace the bundles are keyed by,
            // as the DDL job's delivery keys them.
            codec: tidb_domain_infosync::info_syncer::store_codec(tidb_keyspace::KeyspaceCodec::keyspace_id(store)),
            info_syncer: factory.info_syncer().cloned(),
        }),
        // Go `extworkload.GetManagerFromStore(w.store)`.
        gcv2: tidb_extworkload::get_manager_from_store()
            .map(|manager| Arc::new(ExtWorkloadGcV2(manager)) as Arc<dyn crate::seams::GcV2Manager>),
    }
}

/// Go `extworkload.Manager` as the GC worker's `notifyGCV2AfterGC` uses it.
struct ExtWorkloadGcV2(Arc<dyn tidb_extworkload::Manager>);

impl crate::seams::GcV2Manager for ExtWorkloadGcV2 {
    fn role(&self) -> tidb_config::external_workload::ExternalWorkloadRole {
        self.0.role()
    }

    fn meta(&self) -> Option<tikv_client::proto::keyspacepb::KeyspaceMeta> {
        Some(self.0.meta().clone())
    }

    fn register_gcv2(&self, safe_point: u64, gc_life_time: std::time::Duration) -> crate::seams::GcResult<()> {
        self.0.register_gcv2(safe_point, gc_life_time).map_err(gc_error)
    }

    fn recycle_gcv2(&self, safe_point: u64) -> crate::seams::GcResult<()> {
        self.0.recycle_gcv2(safe_point).map_err(gc_error)
    }
}

fn gc_error(error: impl std::fmt::Display) -> GcError {
    GcError::new(error.to_string())
}

/// Internal connection ids of the GC worker's sessions, apart from the
/// workload repository's (`1 << 62`).
const GC_CONNECTION_ID_BASE: u64 = (1_u64 << 62) | (1_u64 << 60);

/// Go `ddl.GetHistoryJobByID`: the finished job, read from the meta keys.
type HistoryJobLoader = Arc<dyn Fn(i64) -> Result<Option<tidb_model::Job>, String> + Send + Sync>;

/// Go `createSession(store)`: internal sessions without privilege checks.
pub struct NodeGcSessions {
    factory: Weak<Domain>,
    next_connection_id: AtomicU64,
    history_job: HistoryJobLoader,
}

impl NodeGcSessions {
    pub fn new(factory: &Arc<Domain>, history_job: HistoryJobLoader) -> Self {
        Self {
            factory: Arc::downgrade(factory),
            next_connection_id: AtomicU64::new(GC_CONNECTION_ID_BASE),
            history_job,
        }
    }
}

impl GcSessionFactory for NodeGcSessions {
    fn create_session(&self) -> Box<dyn GcSession + '_> {
        let session = self
            .factory
            .upgrade()
            .ok_or_else(|| GcError::new("cluster session factory is stopped"))
            .and_then(|factory| {
                factory
                    .open_session(SessionContext {
                        connection_id: self.next_connection_id.fetch_add(1, Ordering::Relaxed),
                        peer_addr: tidb_session::domain::peer_address::PeerAddress::Tcp(
                "127.0.0.1:0".parse().expect("loopback socket address"),
            ),
                        identity: tidb_session::domain::configured_user_store::AuthenticatedIdentity::internal(),
                        client_capability: 0,
                        tls_state: None,
                        compression_algorithm: tidb_mysql::consts::CompressionNone,
                        compression_level: 0,
                        cancellation: tidb_session::domain::session_api::ConnectionCancellation::default(),
                        close: tidb_session::domain::session_api::ConnectionClose::default(),
                    })
                    .map(SystemSqlExecutor::new)
                    .map_err(|error| GcError::new(error.message))
            });
        Box::new(NodeGcSession {
            session,
            history_job: &self.history_job,
        })
    }
}

struct NodeGcSession<'a> {
    session: GcResult<SystemSqlExecutor>,
    history_job: &'a HistoryJobLoader,
}

fn mysql_error(error: tidb_terror::MysqlError) -> GcError {
    GcError::new(error.message)
}

/// Go `kv.WithInternalSourceType(ctx, kv.InternalTxnGC)`, the context the
/// worker's statements run under.
fn gc_context() -> impl tidb_sqlexec::ExecutionContext {
    tikv_client::util::with_internal_source_type(
        &tikv_client::trace::TraceContext::new(),
        tidb_txnkv::INTERNAL_TXN_GC,
    )
}

fn escaped(sql: &str, arguments: &[SqlArg<'_>]) -> GcResult<String> {
    tidb_sqlexec::sql_with_params(sql, arguments)
        .map(std::borrow::Cow::into_owned)
        .map_err(gc_error)
}

fn datum_string(value: &tidb_datatype::Datum) -> GcResult<String> {
    match value {
        tidb_datatype::Datum::Bytes(bytes) | tidb_datatype::Datum::Raw(bytes) => {
            String::from_utf8(bytes.clone()).map_err(gc_error)
        }
        tidb_datatype::Datum::String(value) => value.as_utf8().map(str::to_owned).map_err(gc_error),
        other => Err(GcError::new(format!("unexpected string value {other:?}"))),
    }
}

impl NodeGcSession<'_> {
    /// Go `se.GetSQLExecutor()`.
    fn sql_executor(&self) -> GcResult<&SystemSqlExecutor> {
        self.session.as_ref().map_err(Clone::clone)
    }

    /// Go `ExecuteInternal` of a read: every row.
    fn query(&mut self, sql: &str) -> GcResult<Vec<Vec<tidb_datatype::Datum>>> {
        tidb_sqlexec::execute_sql(&gc_context(), self.sql_executor()?, sql, &[])
            .map_err(tidb_store_driver_error::sql_query_error::mysql_error_of)
            .map_err(mysql_error)
    }

    /// Go `ExecuteInternal` of a write.
    fn write(&mut self, sql: &str) -> GcResult<()> {
        self.query(sql).map(|_rows| ())
    }

    fn control(&mut self, statement: &str) -> GcResult<()> {
        self.query(statement).map(|_rows| ())
    }
}

/// Go `getRaftKvVersionSQL`.
const GET_RAFT_KV_VERSION_SQL: &str = "select `value` from information_schema.cluster_config where type = 'tikv' and `key` = 'storage.engine'";

impl GcSession for NodeGcSession<'_> {
    fn begin(&mut self) -> GcResult<()> {
        self.control("BEGIN")
    }

    fn commit(&mut self) -> GcResult<()> {
        self.control("COMMIT")
    }

    fn rollback(&mut self) {
        let _ = self.control("ROLLBACK");
    }

    fn load_value(&mut self, key: &str) -> GcResult<Option<String>> {
        let sql = escaped(
            "SELECT HIGH_PRIORITY (variable_value) FROM mysql.tidb WHERE variable_name=%? FOR UPDATE",
            &[key.into()],
        )?;
        let rows = self.query(&sql)?;
        match rows.first().and_then(|row| row.first()) {
            None => Ok(None),
            Some(value) => datum_string(value).map(Some),
        }
    }

    fn save_value(&mut self, key: &str, value: &str, comment: &str) -> GcResult<()> {
        let sql = escaped(
            "INSERT HIGH_PRIORITY INTO mysql.tidb VALUES (%?, %?, %?) ON DUPLICATE KEY UPDATE variable_value = %?, comment = %?",
            &[
                key.into(),
                value.into(),
                comment.into(),
                value.into(),
                comment.into(),
            ],
        )?;
        self.write(&sql)
    }

    /// Go `util.LoadDeleteRanges` / `util.LoadDoneDeleteRanges`.
    fn load_delete_ranges(&mut self, done: bool, safe_point: u64) -> GcResult<Vec<DelRangeTask>> {
        let load = if done {
            tidb_ddl::ddl_delete_range::load_done_delete_ranges
        } else {
            tidb_ddl::ddl_delete_range::load_delete_ranges
        };
        load(&gc_context(), self.sql_executor()?, safe_point).map_err(mysql_error)
    }

    /// Go `util.CompleteDeleteRange`.
    fn complete_delete_range(&mut self, range: &DelRangeTask, record_done: bool) -> GcResult<()> {
        tidb_ddl::ddl_delete_range::complete_delete_range(self.sql_executor()?, range, record_done)
            .map_err(mysql_error)
    }

    /// Go `util.DeleteDoneRecord`.
    fn delete_done_record(&mut self, range: &DelRangeTask) -> GcResult<()> {
        tidb_ddl::ddl_delete_range::delete_done_record(self.sql_executor()?, range).map_err(mysql_error)
    }

    /// Go `util.IsRaftKv2`: every TiKV runs the same engine.
    fn is_raft_kv2(&mut self) -> GcResult<bool> {
        let rows = self.query(GET_RAFT_KV_VERSION_SQL)?;
        match rows.first().and_then(|row| row.first()) {
            None => Ok(false),
            Some(value) => Ok(datum_string(value)? == "raft-kv2"),
        }
    }

    /// Go `ddl.GetHistoryJobByID`.
    fn history_job(&mut self, job_id: i64) -> GcResult<Option<tidb_model::Job>> {
        (self.history_job)(job_id).map_err(GcError::new)
    }
}

/// `infosync`'s placement and label rule calls, over the store's PD HTTP
/// client and the domain's info syncer.
struct NodeGcRules {
    /// The keyspace's meta store, which holds its PD journal: GC's deletes
    /// go through it ([`tidb_ddl_pdjournal::direct::write_through`]).
    journal: Arc<dyn tidb_meta::MetaStore>,
    /// The store's keyspace codec, `None` for an API-V1 store.
    codec: Option<tikv_client::request::ApiV2Codec>,
    /// The domain's PD managers (design §6.2); `None` reads the process's.
    info_syncer: Option<Arc<tidb_domain_infosync::info_syncer::InfoSyncer>>,
}

impl NodeGcRules {
    fn info_syncer(&self) -> Arc<tidb_domain_infosync::info_syncer::InfoSyncer> {
        self.info_syncer.clone().unwrap_or_else(tidb_domain_infosync::info_syncer::get)
    }

    /// Deletes `chosen` (each with the record GC chose it by) through the
    /// keyspace's PD journal.
    fn delete_through_journal(&self, chosen: Vec<tidb_ddl_pdjournal::Planned>) -> GcResult<()> {
        let info_syncer = self.info_syncer();
        let scope = tidb_ddl_pdjournal::pd::scope(&info_syncer);
        tidb_ddl_pdjournal::direct::write_through(
            self.journal.as_ref(),
            &tidb_ddl_pdjournal::pd::InfoSyncerPd(info_syncer),
            "gc",
            scope,
            &chosen,
        )
        .map_err(|error| GcError::new(error.to_string()))
    }

    /// The records of `keys`; refuses PD-only objects before the keyspace's
    /// cutover.
    fn records(
        &self,
        keys: &[tidb_ddl_pdjournal::ObjectKey],
    ) -> GcResult<std::collections::BTreeMap<tidb_ddl_pdjournal::ObjectKey, tidb_ddl_pdjournal::Record>> {
        let (records, cutover) =
            tidb_ddl_pdjournal::direct::read_current(self.journal.as_ref(), keys).map_err(GcError::new)?;
        if let Some(key) = keys
            .iter()
            .find(|key| !cutover && !key.catalog_reconstructible() && !records.contains_key(*key))
        {
            return Err(GcError::new(tidb_ddl_pdjournal::JournalError::NoBaseline(key.clone()).to_string()));
        }
        Ok(records)
    }
}

/// A multi-keyspace node leaves PD's TiFlash rules alone when it GCs a
/// tenant's dropped objects (design §10 phase 2): their IDs (`table-<id>-r`)
/// carry no keyspace, so a tenant's cleanup could delete another keyspace's
/// rules. Label rules and placement bundles carry their keyspace and are
/// cleaned up.
fn skip_pd_rule_cleanup() -> bool {
    tidb_keyspace::is_multi_keyspace_mode()
}

impl GcRuleClient for NodeGcRules {
    /// Go `infosync.DeleteTiFlashPlacementRules`.
    fn delete_tiflash_placement_rules(&self, physical_table_ids: &[i64]) -> GcResult<()> {
        if skip_pd_rule_cleanup() {
            return Ok(());
        }
        let _info_syncer = tidb_domain_infosync::info_syncer::enter(self.info_syncer());
        tidb_domain_infosync::info_syncer::delete_tiflash_placement_rules(physical_table_ids)
            .map_err(GcError::new)
    }

    /// GC's deletes of dropped tables' bundles (`bundles` are empty), each
    /// over the record it found, so a delayed retry cannot delete a bundle
    /// written since.
    fn put_rule_bundles(&self, bundles: Vec<tidb_placement::Bundle>) -> GcResult<()> {
        let keys = bundles
            .iter()
            .map(|bundle| {
                tidb_domain_infosync::placement_manager::encode_bundle(self.codec.as_ref(), bundle)
                    .map(|encoded| tidb_ddl_pdjournal::pd::bundle_entry(&encoded).0)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(GcError::new)?;
        let records = self.records(&keys)?;
        let chosen = keys
            .into_iter()
            .map(|key| {
                let expect = records.get(&key).map(|record| (record.generation, record.op_id.clone()));
                tidb_ddl_pdjournal::Planned { key, value: None, expect: Some(expect) }
            })
            .collect();
        self.delete_through_journal(chosen)
    }

    /// Go `infosync.GetLabelRules`, kept to the rules of the store's keyspace
    /// (`filterRulesByKeyspace`): an ID a keyspace-less codec built (a job
    /// an older node ran) names no keyspace, and another keyspace may hold
    /// it, so the keyspace's GC leaves it alone.
    fn get_label_rules(
        &self,
        rule_ids: &[String],
    ) -> GcResult<HashMap<String, tidb_ddl_label::Rule>> {
        let info_syncer = self.info_syncer();
        let rules = info_syncer
            .label_rule_manager()
            .get_label_rules(rule_ids)
            .map_err(GcError::new)?;
        Ok(tidb_domain_infosync::label_manager::filter_rules_by_keyspace(
            rules.into_values().collect(),
            info_syncer.label_codec(),
        )
        .into_iter()
        .map(|rule| (rule.id.clone(), rule))
        .collect())
    }

    fn label_codec(&self) -> Box<dyn tidb_ddl_label::LabelCodec> {
        match self.codec {
            Some(codec) => Box::new(codec),
            None => Box::new(tidb_ddl_label::CodecV1),
        }
    }

    /// GC's deletes of the rules it chose from PD: each only while its
    /// record still holds the rule GC read (a reused name may hold another
    /// table's rule by now), and over that record's generation.
    fn delete_label_rules(&self, rules: Vec<tidb_ddl_label::Rule>) -> GcResult<()> {
        let keys = rules
            .iter()
            .map(|rule| tidb_ddl_pdjournal::ObjectKey::label_rule(&rule.id))
            .collect::<Vec<_>>();
        let records = self.records(&keys)?;
        let keyspace_id = self.codec.as_ref().map(tikv_client::request::ApiV2Codec::keyspace_id);
        let mut chosen = Vec::new();
        for (key, rule) in keys.into_iter().zip(&rules) {
            let chose = tidb_ddl_pdjournal::pd::label_rule_value(rule);
            let record = records.get(&key);
            let same = record.is_some_and(|record| {
                tidb_ddl_pdjournal::normalize::in_sync(&key, record.value.as_ref(), Some(&chose), keyspace_id)
                    .unwrap_or(false)
            });
            if !same {
                tidb_logutil::bg_logger().warn(
                    "GC leaves a label rule whose record no longer holds the rule it chose",
                    &[tidb_log::Field::new("rule", tidb_log::Value::Str(key.id.clone()))],
                );
                continue;
            }
            let expect = record.map(|record| (record.generation, record.op_id.clone()));
            chosen.push(tidb_ddl_pdjournal::Planned { key, value: None, expect: Some(expect) });
        }
        self.delete_through_journal(chosen)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tidb_domain_infosync::info_syncer::InfoSyncer;
    use tidb_domain_infosync::label_manager::{LabelRuleManager, MockLabelManager};

    use super::*;

    /// A keyspace journal over an embedded store, and the store's PD (kept
    /// alive with it).
    fn journal() -> (tidb_unistore_tikv::tso::InProcessPd, Arc<dyn tidb_meta::MetaStore>) {
        let (pd, opener) = tidb_ddl::embedded_store::in_process_write_stack().unwrap();
        (pd, Arc::new(tidb_store_driver_txn::meta_txn::MetaStore::new(Arc::new(opener), GC_RPC_TIMEOUT)))
    }

    /// A keyspace's GC finds a dropped table's label rule by the ranges the
    /// rule was built with, its keyspace's (design §6.2). Go's `getGCRules`
    /// compares keyspace-less ranges, which never match a next-generation
    /// rule, so the rule stayed in PD forever.
    #[test]
    fn gc_finds_a_dropped_tables_keyspace_label_rule() {
        if !tidb_config_kerneltype::is_next_gen() {
            // Keyspace-aware label rules are next-generation only.
            return;
        }
        let own = InfoSyncer::new(1);
        let mut rule: tidb_ddl_label::Rule =
            serde_json::from_value(serde_json::json!({"labels": [{"key": "merge_option", "value": "deny"}]})).unwrap();
        rule.reset(own.label_codec(), "d", "t", "", &[100]);
        let (_pd, store) = journal();
        let rules = NodeGcRules {
            journal: store,
            codec: tidb_domain_infosync::info_syncer::store_codec(1),
            info_syncer: Some(Arc::new(own)),
        };
        let held = [(rule.id.clone(), rule)].into_iter().collect();
        assert_eq!(
            crate::gc_worker::get_gc_rules(&[100], &held, GcRuleClient::label_codec(&rules).as_ref()),
            ["keyspace/1/schema/d/t"]
        );
        assert!(crate::gc_worker::get_gc_rules(&[100], &held, &tidb_ddl_label::CodecV1).is_empty());
        assert!(crate::gc_worker::get_gc_rules(&[101], &held, GcRuleClient::label_codec(&rules).as_ref()).is_empty());
    }

    /// GC's deletes go through the keyspace's PD journal: a dropped table's
    /// bundle leaves a tombstone record, and a label rule is refused before
    /// the keyspace's cutover (PD keeps it).
    #[test]
    fn gc_deletes_through_the_journal() {
        let (_pd, store) = journal();
        let syncer = Arc::new(InfoSyncer::default());
        let rules = NodeGcRules { journal: Arc::clone(&store), codec: None, info_syncer: Some(Arc::clone(&syncer)) };
        let pd = tidb_ddl_pdjournal::pd::InfoSyncerPd(Arc::clone(&syncer));
        let settings = tidb_model::placement::PlacementSettings { followers: 2, ..Default::default() };
        let mut bundle = tidb_placement::new_bundle_from_options(Some(&settings)).unwrap();
        bundle.reset(tidb_placement::RULE_INDEX_TABLE, &[300]);
        let (key, value) = tidb_ddl_pdjournal::pd::bundle_entry(&bundle);
        let planned = tidb_ddl_pdjournal::Planned::new(key.clone(), value);
        tidb_ddl_pdjournal::direct::write_through(store.as_ref(), &pd, "test", 7, &[planned]).unwrap();
        assert_eq!(tidb_ddl_pdjournal::pd::PdObjects::read(&pd, std::slice::from_ref(&key)).unwrap().len(), 1);

        GcRuleClient::put_rule_bundles(&rules, vec![tidb_placement::new_bundle(300)]).unwrap();
        assert!(tidb_ddl_pdjournal::pd::PdObjects::read(&pd, std::slice::from_ref(&key)).unwrap().is_empty());
        let (records, cutover) = tidb_ddl_pdjournal::direct::read_current(store.as_ref(), std::slice::from_ref(&key)).unwrap();
        assert_eq!(records[&key].value, None, "a tombstone");
        assert!(!cutover);

        let mut rule = tidb_ddl_label::Rule::new();
        rule.reset(&tidb_ddl_label::CodecV1, "d", "t", "", &[300]);
        syncer.label_rule_manager().put_label_rule(&rule).unwrap();
        let refused = GcRuleClient::delete_label_rules(&rules, vec![rule.clone()]).unwrap_err();
        assert!(refused.to_string().contains("PD journal cutover"), "{refused}");
        assert_eq!(syncer.label_rule_manager().get_label_rules(&[rule.id.clone()]).unwrap().len(), 1);
    }

    /// A keyspace's GC reads only its own keyspace's label rules (design
    /// §6.2): the same table rule of another keyspace, and one whose ID
    /// names no keyspace, stay for their own GC.
    #[test]
    fn gc_reads_only_its_keyspaces_label_rules() {
        if !tidb_config_kerneltype::is_next_gen() {
            // Keyspace-aware label rules are next-generation only.
            return;
        }
        let pd = Arc::new(MockLabelManager::default());
        let mut ids = Vec::new();
        for keyspace_id in [1, 2, u32::MAX] {
            let syncer = InfoSyncer::new(keyspace_id);
            let mut rule = tidb_ddl_label::Rule::new();
            rule.reset(syncer.label_codec(), "d", "t", "", &[100]);
            pd.put_label_rule(&rule).unwrap();
            ids.push(rule.id);
        }
        let own = InfoSyncer::new(1);
        own.init_label_rule_manager(Some(pd.clone()));
        let (_pd, store) = journal();
        let rules = NodeGcRules {
            journal: Arc::clone(&store),
            codec: None,
            info_syncer: Some(Arc::new(own)),
        };
        let found = GcRuleClient::get_label_rules(&rules, &ids).unwrap();
        assert_eq!(found.keys().cloned().collect::<Vec<_>>(), ["keyspace/1/schema/d/t"]);
        // The cutover seeds the rules from PD; GC deletes the one it chose
        // through the journal.
        let seeded = found
            .values()
            .map(|rule| (tidb_ddl_pdjournal::ObjectKey::label_rule(&rule.id), tidb_ddl_pdjournal::pd::label_rule_value(rule)))
            .collect();
        tidb_ddl_pdjournal::journal::seed_cutover(store.as_ref(), &seeded).unwrap();
        GcRuleClient::delete_label_rules(&rules, found.into_values().collect()).unwrap();
        let left = pd.get_all_label_rules(&tidb_ddl_label::CodecV1).unwrap();
        assert_eq!(
            left.into_iter().map(|rule| rule.id).collect::<Vec<_>>(),
            ["keyspace/2/schema/d/t", "schema/d/t"]
        );
    }
}

#[cfg(test)]
mod playground_tests {
    /// Counts the keys of the table `GC_ACC_TABLE_ID` (its record and index
    /// keys, `t{id}` .. `t{id+1}`) on the cluster at `GC_ACC_PD`, read at a
    /// fresh timestamp. Run by hand against a playground: after `DROP TABLE` the rows are
    /// still there (Go deletes nothing until GC), and after the GC worker
    /// destroyed the table's delete range there is none, at any timestamp.
    #[test]
    #[ignore = "needs a TiKV playground: GC_ACC_PD and GC_ACC_TABLE_ID"]
    fn playground_table_keys() {
        use tikv_client::TimestampExt;

        let pd = std::env::var("GC_ACC_PD").expect("GC_ACC_PD");
        let table_id: i64 = std::env::var("GC_ACC_TABLE_ID")
            .expect("GC_ACC_TABLE_ID")
            .parse()
            .expect("a table id");
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let count = runtime.block_on(async {
            let store = tikv_client::tikv::KvStore::new_with_config(
                vec![pd],
                tikv_client::Config::default(),
            )
            .await
            .expect("store");
            let timestamp = store.current_timestamp().await.expect("timestamp");
            let mut snapshot = store.snapshot(
                tikv_client::proto::pdpb::Timestamp::from_version(timestamp.version()),
                tikv_client::TransactionOptions::new_optimistic().read_only(),
            );
            let start = tidb_tablecodec::table_key::encode_table_prefix(table_id);
            let end = tidb_tablecodec::table_key::encode_table_prefix(table_id + 1);
            snapshot
                .scan(start..end, 10_000)
                .await
                .expect("scan")
                .count()
        });
        println!("GC_ACC table {table_id} keys: {count}");
    }

    /// Resolves locks over the whole key space through the client, as the GC
    /// worker's runner does, after `GC_ACC_IDLE_SECS` of idleness.
    #[test]
    #[ignore = "needs a TiKV playground: GC_ACC_PD"]
    fn playground_resolve_locks() {
        use tikv_client::TimestampExt;

        let pd = std::env::var("GC_ACC_PD").expect("GC_ACC_PD");
        let idle: u64 = std::env::var("GC_ACC_IDLE_SECS")
            .unwrap_or_default()
            .parse()
            .unwrap_or(0);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            let store = tikv_client::tikv::KvStore::new_with_config(
                vec![pd],
                tikv_client::Config::default(),
            )
            .await
            .expect("store");
            tokio::time::sleep(std::time::Duration::from_secs(idle)).await;
            let timestamp = store.current_timestamp().await.expect("timestamp");
            let (stat, result) = store
                .resolve_locks_for_range(
                    tikv_client::async_util::Cancellation::default(),
                    &tikv_client::proto::pdpb::Timestamp::from_version(timestamp.version() - 1),
                    (Vec::new(), Vec::new()),
                    1024,
                )
                .await;
            println!("GC_ACC resolve after {idle}s idle: {stat:?} {result:?}");
        });
    }
}

