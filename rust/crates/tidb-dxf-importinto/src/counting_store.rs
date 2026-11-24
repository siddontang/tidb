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

//! A test [`ImportStore`] that serves nothing and counts every call, so a
//! test can assert that a statement never reached the process's import
//! store.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::ImportStore;

/// An [`ImportStore`] that refuses or answers empty, counting each call.
#[derive(Debug, Default)]
pub struct CountingImportStore {
    calls: AtomicUsize,
}

impl CountingImportStore {
    /// How many of its methods were called.
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn called(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

impl ImportStore for CountingImportStore {
    fn import_client(&self) -> Result<Arc<dyn tidb_ingestor_ingestctrl::region_job::ImportClient>, String> {
        self.called();
        Err("counting import store".to_owned())
    }

    fn keyspace(&self) -> Vec<u8> {
        self.called();
        Vec::new()
    }

    fn rebase_allocator(
        &self,
        _db_id: i64,
        _table: &tidb_model::TableInfo,
        _tp: tidb_lightning_backend_kv::AllocatorType,
        _base: i64,
    ) -> Result<(), String> {
        self.called();
        Err("counting import store".to_owned())
    }

    fn etcd(&self) -> Option<Arc<dyn tidb_domain_serverinfo::serverinfo_syncer::EtcdOps>> {
        self.called();
        None
    }

    fn local_server_info(&self) -> Option<serde_json::Value> {
        self.called();
        None
    }

    fn cluster_id(&self) -> Option<u64> {
        self.called();
        None
    }

    fn table_is_empty(&self, _table: &tidb_model::TableInfo) -> Result<bool, String> {
        self.called();
        Ok(true)
    }

    fn max_node_count_by_stores(&self) -> usize {
        self.called();
        0
    }

    fn region_split_size_keys(&self) -> Result<(i64, i64), String> {
        self.called();
        Err("counting import store".to_owned())
    }

    fn kv_storage(&self) -> Option<Arc<dyn tidb_txnkv::kv_storage::Storage>> {
        self.called();
        None
    }
}
