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
//! package-level calls (`label_manager::label_rule_manager`,
//! `placement_manager::put_rule_bundles`, ...) answer from the entered one.
//! A thread that entered none reads the process's, which the configured
//! keyspace's domain installs ([`set_global_info_syncer`]).

use std::cell::RefCell;
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use tidb_ddl_label::{CodecV1, LabelCodec};
use tikv_client::request::{ApiV2Codec, KeyMode};

use crate::label_manager::{LabelRuleManager, MockLabelManager};
use crate::placement_manager::{MockPlacementManager, PlacementManager};
use crate::region::PdHttpClient;
use crate::resource_manager_client::{MockResourceManagerClient, ResourceManagerClient};
use crate::schedule_manager::{MockScheduleManager, ScheduleManager};
use crate::tiflash_manager::{MockTiFlashReplicaManagerCtx, TiFlashReplicaManager};

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
            codec: ApiV2Codec::new(KeyMode::Txn, keyspace_id).ok(),
            ..Self::default()
        }
    }

    /// Go `InfoSyncer.tikvCodec` when the store serves a keyspace.
    pub fn codec(&self) -> Option<&ApiV2Codec> {
        self.codec.as_ref()
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
