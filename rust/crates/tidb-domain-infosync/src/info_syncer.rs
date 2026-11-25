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

//! Go `InfoSyncer`: the PD-facing managers of one domain. Go keeps one per
//! process (`globalInfoSyncer`), which the last domain to initialize owns.
//! Here each domain owns one (multi-keyspace design §6.2): a session enters
//! its domain's while it runs a statement, a DDL job step its DDL's, and the
//! package-level calls below ([`label_rule_manager`], [`put_rule_bundles`],
//! ...; Go `info.go`'s) answer from the entered one. A thread that entered
//! none reads the process's, which the configured keyspace's domain installs
//! ([`set_global_info_syncer`]). The manager modules hold the managers
//! themselves and do not reach the info syncer.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::Duration;

use tidb_ddl_label::{CodecV1, LabelCodec};
use tidb_log::{Field, Value};
use tidb_placement::Bundle;
use tidb_proto::resource_manager as rmpb;
use tikv_client::pd_http;
use tikv_client::request::{ApiV2Codec, KeyMode};

use crate::label_manager::{LabelRuleManager, MockLabelManager};
use crate::pd_http::PdHttpClient;
use crate::placement_manager::{self, MockPlacementManager, PlacementManager};
use crate::resource_manager_client::{MockResourceManagerClient, ResourceManagerClient};
use crate::schedule_manager::{MockScheduleManager, ScheduleManager};
use crate::tiflash_manager::{
    self, collect_columnar_status, is_tiflash_http_resp, is_tiflash_write_http_resp, make_new_rule,
    ColumnarStatusResp, MockTiFlash, MockTiFlashReplicaManagerCtx, TiFlashReplicaManager,
};

/// Go `InfoSyncer`'s PD managers. A new one holds Go's managers for a node
/// without PD (the mocks); the `init_*` methods install PD's.
pub struct InfoSyncer {
    /// Go `InfoSyncer.tikvCodec`: the codec of the domain's store, `None`
    /// for an API-V1 store.
    codec: Option<ApiV2Codec>,
    label_rule_manager: RwLock<Arc<dyn LabelRuleManager>>,
    placement_manager: RwLock<Arc<dyn PlacementManager>>,
    schedule_manager: RwLock<Arc<dyn ScheduleManager>>,
    tiflash_replica_manager: RwLock<Arc<dyn TiFlashReplicaManager>>,
    resource_manager_client: RwLock<Arc<dyn ResourceManagerClient>>,
    pd_http_cli: RwLock<Option<Arc<dyn PdHttpClient>>>,
}

impl std::fmt::Debug for InfoSyncer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("InfoSyncer").finish_non_exhaustive()
    }
}

impl Default for InfoSyncer {
    fn default() -> Self {
        Self {
            codec: None,
            label_rule_manager: RwLock::new(Arc::new(MockLabelManager::default())),
            placement_manager: RwLock::new(Arc::new(MockPlacementManager::default())),
            schedule_manager: RwLock::new(Arc::new(MockScheduleManager::default())),
            tiflash_replica_manager: RwLock::new(Arc::new(MockTiFlashReplicaManagerCtx::default())),
            resource_manager_client: RwLock::new(Arc::new(MockResourceManagerClient::new())),
            pd_http_cli: RwLock::new(None),
        }
    }
}

fn read<T: Clone>(slot: &RwLock<T>) -> T {
    slot.read().unwrap_or_else(PoisonError::into_inner).clone()
}

fn write<T>(slot: &RwLock<T>, value: T) {
    *slot.write().unwrap_or_else(PoisonError::into_inner) = value;
}

impl InfoSyncer {
    /// The info syncer of a store whose keyspace ID is `keyspace_id` (Go
    /// `tikv.Codec.GetKeyspaceID`, the null keyspace for an API-V1 store),
    /// with Go's managers for a node without PD.
    #[must_use]
    pub fn new(keyspace_id: u32) -> Self {
        Self {
            codec: store_codec(keyspace_id),
            ..Self::default()
        }
    }

    /// Go `InfoSyncer.tikvCodec` when the store serves a keyspace.
    pub fn codec(&self) -> Option<&ApiV2Codec> {
        self.codec.as_ref()
    }

    /// The bundle `name` of this keyspace from its placement manager, read
    /// back from the group PD keeps it under
    /// ([`placement_manager::decode_bundle`]).
    fn get_rule_bundle(&self, name: &str) -> Result<Bundle, String> {
        let bundle = self
            .placement_manager()
            .get_rule_bundle(&placement_manager::keyspace_bundle_id(self.codec.as_ref(), name))?;
        placement_manager::decode_bundle(self.codec.as_ref(), name, bundle)
    }

    /// The range bundle `range_bundle_id` (`TiDB_GLOBAL`) as Go and the
    /// nodes before keyspace-qualified bundles keep it, unqualified, when
    /// this store serves a keyspace on a next-generation kernel and PD holds
    /// it with rules (multi-keyspace design §6.2). Its rules range over
    /// keyspace-less keys: the global range's empty bounds cover every
    /// keyspace's keys.
    fn legacy_range_bundle(&self, range_bundle_id: &str) -> Result<Option<Bundle>, String> {
        if placement_manager::keyspace_bundle_id(self.codec.as_ref(), range_bundle_id) == range_bundle_id {
            return Ok(None);
        }
        let bundle = self.placement_manager().get_rule_bundle(range_bundle_id)?;
        Ok((!bundle.rules.is_empty()).then_some(bundle))
    }

    /// Puts `bundles` through this keyspace's placement manager, keyed and
    /// ranged by its keyspace ([`placement_manager::encode_bundle`]).
    fn put_rule_bundles(&self, bundles: &[Bundle]) -> Result<(), String> {
        let bundles = bundles
            .iter()
            .map(|bundle| placement_manager::encode_bundle(self.codec.as_ref(), bundle))
            .collect::<Result<Vec<_>, _>>()?;
        self.placement_manager().put_rule_bundles(&bundles)
    }

    /// Go `InfoSyncer.tikvCodec` as the label rules read it: the keyspace
    /// codec, or the keyspace-less one of an API-V1 store.
    pub fn label_codec(&self) -> &dyn LabelCodec {
        match &self.codec {
            Some(codec) => codec,
            None => &CodecV1,
        }
    }

    /// Go `initLabelRuleManager`: PD's manager, or a fresh mock.
    pub fn init_label_rule_manager(&self, manager: Option<Arc<dyn LabelRuleManager>>) {
        write(
            &self.label_rule_manager,
            manager.unwrap_or_else(|| Arc::new(MockLabelManager::default())),
        );
    }

    /// Go `initPlacementManager`: PD's manager, or a fresh mock.
    pub fn init_placement_manager(&self, manager: Option<Arc<dyn PlacementManager>>) {
        write(
            &self.placement_manager,
            manager.unwrap_or_else(|| Arc::new(MockPlacementManager::default())),
        );
    }

    /// Go `initScheduleManager`: PD's manager, or a fresh mock.
    pub fn init_schedule_manager(&self, manager: Option<Arc<dyn ScheduleManager>>) {
        write(
            &self.schedule_manager,
            manager.unwrap_or_else(|| Arc::new(MockScheduleManager::default())),
        );
    }

    /// Go `initTiFlashReplicaManager`: PD's manager, or a fresh mock.
    pub fn init_tiflash_replica_manager(&self, manager: Option<Arc<dyn TiFlashReplicaManager>>) {
        write(
            &self.tiflash_replica_manager,
            manager.unwrap_or_else(|| Arc::new(MockTiFlashReplicaManagerCtx::default())),
        );
    }

    /// Go `initResourceManagerClient`: PD's client, or a fresh mock.
    pub fn init_resource_manager_client(&self, client: Option<Arc<dyn ResourceManagerClient>>) {
        write(
            &self.resource_manager_client,
            client.unwrap_or_else(|| Arc::new(MockResourceManagerClient::new())),
        );
    }

    /// Go `InfoSyncer.pdHTTPCli`; `None` on a node without PD.
    pub fn init_pd_http_cli(&self, cli: Option<Arc<dyn PdHttpClient>>) {
        write(&self.pd_http_cli, cli);
    }

    /// The label-rule manager.
    pub fn label_rule_manager(&self) -> Arc<dyn LabelRuleManager> {
        read(&self.label_rule_manager)
    }

    /// The placement manager.
    pub fn placement_manager(&self) -> Arc<dyn PlacementManager> {
        read(&self.placement_manager)
    }

    /// The schedule manager.
    pub fn schedule_manager(&self) -> Arc<dyn ScheduleManager> {
        read(&self.schedule_manager)
    }

    /// The TiFlash replica manager.
    pub fn tiflash_replica_manager(&self) -> Arc<dyn TiFlashReplicaManager> {
        read(&self.tiflash_replica_manager)
    }

    /// The resource-manager client.
    pub fn resource_manager_client(&self) -> Arc<dyn ResourceManagerClient> {
        read(&self.resource_manager_client)
    }

    /// The PD HTTP client, if the node has PD.
    pub fn pd_http_cli(&self) -> Option<Arc<dyn PdHttpClient>> {
        read(&self.pd_http_cli)
    }
}

/// Go `store.GetCodec()` as a keyspace codec: the transaction codec of a store
/// whose keyspace ID is `keyspace_id`, `None` for an API-V1 store (the null
/// keyspace).
#[must_use]
pub fn store_codec(keyspace_id: u32) -> Option<ApiV2Codec> {
    ApiV2Codec::new(KeyMode::Txn, keyspace_id).ok()
}

/// Go `globalInfoSyncer`: the process's, which a thread that entered no
/// domain's reads.
static GLOBAL_INFO_SYNCER: Mutex<Option<Arc<InfoSyncer>>> = Mutex::new(None);

/// Go `setGlobalInfoSyncer`: the configured keyspace's domain installs its
/// own as the process's.
pub fn set_global_info_syncer(syncer: Arc<InfoSyncer>) {
    *GLOBAL_INFO_SYNCER.lock().unwrap_or_else(PoisonError::into_inner) = Some(syncer);
}

/// The process's info syncer; a process that installed none gets Go's
/// managers for a node without PD.
pub fn global_info_syncer() -> Arc<InfoSyncer> {
    Arc::clone(
        GLOBAL_INFO_SYNCER
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert_with(Arc::default),
    )
}

thread_local! {
    static ENTERED: RefCell<Option<Arc<InfoSyncer>>> = const { RefCell::new(None) };
}

/// The info syncer the current thread reads until dropped (then the
/// previous one).
#[must_use = "the thread leaves the info syncer when the guard drops"]
pub struct EnteredInfoSyncer {
    previous: Option<Arc<InfoSyncer>>,
    /// Bound to the entering thread: the drop restores its state.
    _thread_bound: std::marker::PhantomData<*const ()>,
}

impl Drop for EnteredInfoSyncer {
    fn drop(&mut self) {
        let previous = self.previous.take();
        ENTERED.with(|entered| *entered.borrow_mut() = previous);
    }
}

/// Makes `syncer` the one [`get`] answers on this thread: a session enters
/// its domain's while it runs a statement, a DDL job step its DDL's.
pub fn enter(syncer: Arc<InfoSyncer>) -> EnteredInfoSyncer {
    let previous = ENTERED.with(|entered| entered.borrow_mut().replace(syncer));
    EnteredInfoSyncer {
        previous,
        _thread_bound: std::marker::PhantomData,
    }
}

/// Go `getGlobalInfoSyncer`: the info syncer this thread entered, else the
/// process's.
pub fn get() -> Arc<InfoSyncer> {
    ENTERED
        .with(|entered| entered.borrow().clone())
        .unwrap_or_else(global_info_syncer)
}

// Label rules (Go `info.go`'s label-rule calls).

/// The label-rule manager of the info syncer this thread reads
/// ([`get`]).
pub fn label_rule_manager() -> Arc<dyn LabelRuleManager> {
    get().label_rule_manager()
}

// Placement (Go `info.go`'s rule-bundle calls): a keyspace's bundles are keyed and ranged by
// its keyspace where they meet PD (`placement_manager::encode_bundle`).

/// The placement manager of the info syncer this thread reads
/// ([`get`]).
pub fn placement_manager() -> Arc<dyn PlacementManager> {
    get().placement_manager()
}

/// Go `GetRuleBundle`: the bundle `name` of the keyspace this thread's info
/// syncer serves.
///
/// # Errors
/// The placement manager's.
pub fn get_rule_bundle(name: &str) -> Result<Bundle, String> {
    get().get_rule_bundle(name)
}

/// The unqualified range bundle `range_bundle_id` a Go or older node left
/// in PD, which covers every keyspace on a next-generation kernel; `None`
/// where bundles are not keyspace-qualified or PD holds no such bundle.
///
/// # Errors
/// The placement manager's.
pub fn get_legacy_range_bundle(range_bundle_id: &str) -> Result<Option<Bundle>, String> {
    get().legacy_range_bundle(range_bundle_id)
}

/// Go `RequestPDMaxRetry`.
const REQUEST_PD_MAX_RETRY: usize = 3;

/// Go `RequestRetryInterval`.
const REQUEST_RETRY_INTERVAL: Duration = Duration::from_millis(200);

/// Go `PutRuleBundles`.
///
/// # Errors
/// The placement manager's.
pub fn put_rule_bundles(bundles: &[Bundle]) -> Result<(), String> {
    if let Some(value) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/domain/infosync/putRuleBundlesError") {
        return Err(if value.bool() {
            crate::infosync_errors::ERR_HTTP_SERVICE_ERROR.fast_gen("mock service error", &[]).to_string()
        } else {
            "mock other error".to_owned()
        });
    }
    get().put_rule_bundles(bundles)
}

/// Go `PutRuleBundlesWithRetry`: retries a failure other than PD's own
/// answer (`ErrHTTPServiceError`).
///
/// # Errors
/// The last attempt's.
pub fn put_rule_bundles_with_retry(
    bundles: &[Bundle],
    max_retry: usize,
    interval: Duration,
) -> Result<(), String> {
    let mut result = Ok(());
    for i in 0..=max_retry {
        result = put_rule_bundles(bundles);
        match &result {
            Ok(()) => return result,
            Err(error) if crate::pd_http::is_http_service_error(error) => return result,
            Err(error) => {
                if i != max_retry {
                    tidb_logutil::bg_logger().warn(
                        "Error occurs when PutRuleBundles, retry",
                        &[tidb_logutil::zap_error(error)],
                    );
                    std::thread::sleep(interval);
                }
            }
        }
    }
    result
}

/// Go `PutRuleBundlesWithDefaultRetry`.
///
/// # Errors
/// The last attempt's.
pub fn put_rule_bundles_with_default_retry(bundles: &[Bundle]) -> Result<(), String> {
    put_rule_bundles_with_retry(bundles, REQUEST_PD_MAX_RETRY, REQUEST_RETRY_INTERVAL)
}

// Resource groups (Go `info.go`'s resource-manager calls).

fn resource_manager() -> Arc<dyn ResourceManagerClient> {
    get().resource_manager_client()
}

/// Go `InfoSyncer.initResourceManagerClient` on the process's info syncer:
/// PD's client, or the mock when there is none.
pub fn init_resource_manager_client(client: Option<Arc<dyn ResourceManagerClient>>) {
    global_info_syncer().init_resource_manager_client(client);
}

/// Go `infosync.GetResourceGroup`.
pub fn get_resource_group(name: &str) -> Result<rmpb::ResourceGroup, String> {
    resource_manager().get_resource_group(name)
}

/// The group `name` of this keyspace, `None` when PD holds none.
pub fn lookup_resource_group(name: &str) -> Result<Option<rmpb::ResourceGroup>, String> {
    resource_manager().lookup_resource_group(name)
}

/// Go `infosync.ListResourceGroups`.
pub fn list_resource_groups() -> Result<Vec<rmpb::ResourceGroup>, String> {
    resource_manager().list_resource_groups()
}

/// Go `infosync.AddResourceGroup`.
pub fn add_resource_group(group: rmpb::ResourceGroup) -> Result<(), String> {
    resource_manager().add_resource_group(group).map(|_| ())
}

/// Go `infosync.ModifyResourceGroup`.
pub fn modify_resource_group(group: rmpb::ResourceGroup) -> Result<(), String> {
    resource_manager().modify_resource_group(group).map(|_| ())
}

/// Go `infosync.DeleteResourceGroup`.
pub fn delete_resource_group(name: &str) -> Result<(), String> {
    resource_manager().delete_resource_group(name).map(|_| ())
}

// PD's schedule configuration (Go `info.go`'s schedule calls).

/// Go `InfoSyncer.initScheduleManager` on the process's info syncer: PD's
/// manager, or a fresh mock when the node has no PD.
pub fn init_schedule_manager(manager: Option<Arc<dyn ScheduleManager>>) {
    global_info_syncer().init_schedule_manager(manager);
}

fn schedule_manager() -> Arc<dyn ScheduleManager> {
    get().schedule_manager()
}

/// Go `GetPDScheduleConfig`: the schedule information from PD.
pub fn get_pd_schedule_config() -> Result<HashMap<String, serde_json::Value>, String> {
    schedule_manager().get_schedule_config()
}

/// Go `SetPDScheduleConfig`: sets the schedule information for PD.
pub fn set_pd_schedule_config(config: &HashMap<String, serde_json::Value>) -> Result<(), String> {
    schedule_manager().set_schedule_config(config)
}

// TiFlash replicas (Go `info.go`'s TiFlash calls).

/// Go `InfoSyncer.initTiFlashReplicaManager` on the process's info syncer:
/// PD's manager, or a mock when the node has no PD.
pub fn init_tiflash_replica_manager(manager: Option<Arc<dyn TiFlashReplicaManager>>) {
    global_info_syncer().init_tiflash_replica_manager(manager);
}

/// The TiFlash replica manager of the info syncer this thread reads
/// ([`get`]).
pub fn tiflash_replica_manager() -> Arc<dyn TiFlashReplicaManager> {
    get().tiflash_replica_manager()
}

/// Go `infosync.SetMockTiFlash`: the mock manager's TiFlash.
pub fn set_mock_tiflash(tiflash: Arc<MockTiFlash>) {
    tiflash_replica_manager().set_mock_tiflash(tiflash);
}

/// Go `infosync.GetTiFlashStoresStat`.
pub fn get_tiflash_stores_stat() -> Result<pd_http::StoresInfo, String> {
    tiflash_replica_manager().get_stores_stat()
}

/// Go `infosync.SyncTiFlashTableSchema`: the table's schema synced to every
/// store labelled `engine=tiflash`.
pub fn sync_tiflash_table_schema(table_id: i64) -> Result<(), String> {
    let manager = tiflash_replica_manager();
    let tikv_stats = manager.get_stores_stat()?;
    let tiflash_stores: Vec<pd_http::StoreInfo> = tikv_stats
        .stores
        .into_iter()
        .filter(|store| {
            store
                .store
                .labels
                .iter()
                .any(|label| label.key == "engine" && label.value == "tiflash")
        })
        .collect();
    manager.sync_tiflash_table_schema(table_id, &tiflash_stores)
}

/// The loop Go's `calculateColumnarProgressWithCtx` and
/// `CalculateColumnarIndexProgress` share: the `ready_of` count of every
/// TiKV store over their total, a failed store an error unless it is a
/// tombstone.
fn columnar_progress(
    table_id: i64,
    index_id: Option<i64>,
    tikv_stores: &[pd_http::StoreInfo],
    ready_of: fn(&ColumnarStatusResp) -> u64,
) -> Result<f64, String> {
    let security = tiflash_replica_manager().security();
    let mut ready = 0;
    let mut total = 0;
    for store_stat in tikv_stores {
        let addr = &store_stat.store.status_address;
        let Ok(columnar_status) = collect_columnar_status(&security, addr, table_id, index_id) else {
            tidb_logutil::bg_logger().error(
                "Fail to get columnar status from TiKV.",
                &[
                    Field::new("tableID", Value::I64(table_id)),
                    Field::new("addr", Value::Str(addr.clone())),
                ],
            );
            // If the store is not tombstone, return error
            if store_stat.store.state_name != "Tombstone" {
                return Err(format!(
                    "Failed to get columnar status from TiKV, store {addr} is {}",
                    store_stat.store.state_name
                ));
            }
            continue;
        };
        ready += ready_of(&columnar_status);
        total += columnar_status.total;
    }
    if total == 0 {
        return Ok(0.0);
    }
    Ok(ready as f64 / total as f64)
}

/// Go `infosync.CalculateColumnarProgress` calculates columnar progress.
pub fn calculate_columnar_progress(table_id: i64, tikv_stores: &[pd_http::StoreInfo]) -> Result<f64, String> {
    columnar_progress(table_id, None, tikv_stores, |status| status.ready)
}

/// Go `infosync.CalculateColumnarIndexProgress` calculates columnar index
/// progress.
pub fn calculate_columnar_index_progress(
    table_id: i64,
    index_id: i64,
    tikv_stores: &[pd_http::StoreInfo],
) -> Result<f64, String> {
    columnar_progress(table_id, Some(index_id), tikv_stores, |status| status.vector_index_ready)
}

/// Go `infosync.GetTiFlashProgressStores`: the TiFlash write stores and the
/// TiKV stores, TiFlash compute nodes left out.
///
/// # Errors
/// PD's error.
pub fn get_tiflash_progress_stores(
) -> Result<(Vec<pd_http::StoreInfo>, Vec<pd_http::StoreInfo>), String> {
    let stats = get_tiflash_stores_stat()?;
    let (mut tiflash_stores, mut tikv_stores) = (Vec::new(), Vec::new());
    for store in stats.stores {
        if is_tiflash_http_resp(&store.store) {
            if is_tiflash_write_http_resp(&store.store) {
                tiflash_stores.push(store);
            }
            continue;
        }
        tikv_stores.push(store);
    }
    Ok((tiflash_stores, tikv_stores))
}

/// Go `infosync.MustGetTiFlashProgress`: the cached progress, or the
/// lower of the TiFlash progress (with TiFlash stores) and the columnar
/// progress (with TiKV stores), cached.
///
/// # Errors
/// The calculation's error.
pub fn must_get_tiflash_progress(
    table_id: i64,
    replica_count: u64,
    tiflash_stores: &[pd_http::StoreInfo],
    tikv_stores: &[pd_http::StoreInfo],
) -> Result<f64, String> {
    let manager = tiflash_replica_manager();
    if let Some(progress) = manager.get_tiflash_progress_from_cache(table_id) {
        return Ok(progress);
    }
    let mut tiflash_progress = 1.0;
    let mut columnar_progress = 1.0;
    if !tiflash_stores.is_empty() {
        tiflash_progress = manager
            .calculate_tiflash_progress(table_id, replica_count, tiflash_stores)?
            .0;
    }
    if !tikv_stores.is_empty() {
        columnar_progress = calculate_columnar_progress(table_id, tikv_stores)?;
    }
    let progress = f64::min(tiflash_progress, columnar_progress);
    manager.update_tiflash_progress_cache(table_id, progress);
    Ok(progress)
}

/// Go `infosync.UpdateTiFlashProgressCache`.
pub fn update_tiflash_progress_cache(table_id: i64, progress: f64) {
    tiflash_replica_manager().update_tiflash_progress_cache(table_id, progress);
}

/// Go `infosync.GetTiFlashProgressFromCache`.
pub fn get_tiflash_progress_from_cache(table_id: i64) -> Option<f64> {
    tiflash_replica_manager().get_tiflash_progress_from_cache(table_id)
}

/// Go `infosync.DeleteTiFlashProgressFromCache`.
pub fn delete_tiflash_progress_from_cache(table_id: i64) {
    tiflash_replica_manager().delete_tiflash_progress_from_cache(table_id);
}

/// Go `infosync.CleanTiFlashProgressCache`.
pub fn clean_tiflash_progress_cache() {
    tiflash_replica_manager().clean_tiflash_progress_cache();
}

/// Go `infosync.DeleteTiFlashTableSyncProgress`: drops the cached progress
/// of the table, or of each of its partitions.
pub fn delete_tiflash_table_sync_progress(table_info: &tidb_model::table_info::TableInfo) {
    let manager = tiflash_replica_manager();
    if let Some(pi) = table_info.get_partition_info() {
        for p in pi.read().definitions.snapshot() {
            manager.delete_tiflash_progress_from_cache(p.id);
        }
    } else {
        manager.delete_tiflash_progress_from_cache(table_info.id);
    }
}

/// Go `infosync.SetTiFlashGroupConfig`.
pub fn set_tiflash_group_config() -> Result<(), String> {
    tiflash_manager::info("SetTiFlashGroupConfig", &[]);
    tiflash_replica_manager().set_tiflash_group_config()
}

/// Go `infosync.ConfigureTiFlashPDForTable`.
pub fn configure_tiflash_pd_for_table(id: i64, count: u64, location_labels: &[String]) -> Result<(), String> {
    if !tiflash_manager::tiflash_enabled() {
        return Ok(());
    }
    tiflash_manager::info(
        "ConfigureTiFlashPDForTable",
        &[Field::new("tableID", Value::I64(id)), Field::new("count", Value::U64(count))],
    );
    tiflash_replica_manager().set_placement_rule(&make_new_rule(id, count, location_labels))
}

/// Go `infosync.ConfigureTiFlashPDForPartitions`: one rule per partition in
/// one batch, then, when `accel`, the accelerated scheduling of them.
pub fn configure_tiflash_pd_for_partitions(
    accel: bool,
    partition_ids: &[i64],
    count: u64,
    location_labels: &[String],
    table_id: i64,
) -> Result<(), String> {
    if !tiflash_manager::tiflash_enabled() {
        return Ok(());
    }
    let rules: Vec<_> = partition_ids
        .iter()
        .map(|&id| {
            tiflash_manager::info(
                "ConfigureTiFlashPDForPartitions",
                &[
                    Field::new("tableID", Value::I64(table_id)),
                    Field::new("partID", Value::I64(id)),
                    Field::new("accel", Value::Bool(accel)),
                    Field::new("count", Value::U64(count)),
                ],
            );
            make_new_rule(id, count, location_labels)
        })
        .collect();
    let manager = tiflash_replica_manager();
    manager.set_placement_rule_batch(&rules)?;
    if accel {
        manager.post_accelerate_schedule_batch(partition_ids)?;
    }
    Ok(())
}

/// Go `infosync.DeleteTiFlashPlacementRules`: a zero-count rule per table,
/// which the batch deletes.
pub fn delete_tiflash_placement_rules(physical_table_ids: &[i64]) -> Result<(), String> {
    tiflash_manager::info("DeleteTiFlashPlacementRules", &[]);
    let rules: Vec<_> = physical_table_ids
        .iter()
        .map(|&id| make_new_rule(id, 0, &[]))
        .collect();
    tiflash_replica_manager().set_placement_rule_batch(&rules)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A thread reads the info syncer it entered, and the process's again
    /// once it leaves; another thread reads the process's.
    #[test]
    fn a_thread_reads_the_info_syncer_it_entered() {
        let own = Arc::new(InfoSyncer::default());
        {
            let _entered = enter(Arc::clone(&own));
            assert!(Arc::ptr_eq(&get(), &own));
            std::thread::spawn(move || assert!(!Arc::ptr_eq(&get(), &own)))
                .join()
                .unwrap();
        }
        assert!(Arc::ptr_eq(&get(), &global_info_syncer()));
    }
}
