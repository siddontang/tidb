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

//! Opening another keyspace of a multi-keyspace node leaves the process's
//! bindings to the configured keyspace alone
//! (`rust/docs/design/2026-11-24-multi-keyspace-server.md`, §6.3): the cloud
//! storage URI, the PD client option callbacks and the external timestamp
//! (PD keeps one per cluster, so a tenant never writes it).
//! Separate executable with one test: the bindings are process slots.

use std::sync::atomic::{AtomicIsize, AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex};

use tidb_domain::global_sysvars::GlobalSysvars;

/// A PD client that records the options it was given.
#[derive(Debug, Default)]
struct RecordingPd {
    rpc_concurrency: AtomicIsize,
    calls: Mutex<Vec<&'static str>>,
}

impl tidb_variable::vars::PdRegionPolicy for RecordingPd {
    fn set_follower_handle(&self, _enabled: bool) {
        self.calls.lock().unwrap().push("follower_handle");
    }
    fn set_tso_rpc_concurrency(&self, concurrency: isize) {
        self.calls.lock().unwrap().push("tso_rpc_concurrency");
        self.rpc_concurrency.store(concurrency, SeqCst);
    }
}

/// A store's view of the PD cluster's external timestamp, recording writes.
#[derive(Debug, Default)]
struct Store {
    ts: AtomicU64,
    sets: Mutex<Vec<u64>>,
}

impl tidb_variable::vars::ExternalTimestampProvider for Store {
    fn get(&self) -> Result<u64, String> {
        Ok(self.ts.load(SeqCst))
    }
    fn set(&self, ts: u64) -> Result<(), String> {
        self.sets.lock().unwrap().push(ts);
        self.ts.store(ts, SeqCst);
        Ok(())
    }
}

#[test]
fn a_secondary_open_keeps_the_process_bindings() {
    // The configured keyspace: its table over the process statics binds the
    // process's PD callbacks and external timestamp to its store.
    let configured = GlobalSysvars::new();
    let configured_pd = Arc::new(RecordingPd::default());
    configured.set_pd_region_policy(configured_pd.clone());
    configured.arm_domain_hooks();
    let configured_store = Arc::new(Store::default());
    tidb_variable::domain_slots::set_external_timestamp_funcs(configured_store.clone());
    // The configured cloud storage URI.
    *tidb_vardef::CLOUD_STORAGE_URI.write().unwrap() = "local:///tmp/configured-uri".to_owned();

    // Another keyspace opens with NO stored URI: its rebuild loads the
    // default (empty) value. It has a stored `tidb_external_ts`, which its
    // rebuild must not write to PD: PD keeps one external timestamp per
    // cluster, so the tenant's row would move every keyspace's.
    let tenant = GlobalSysvars::new().with_keyspace_vardef();
    let tenant_pd = Arc::new(RecordingPd::default());
    let tenant_store = Arc::new(Store::default());
    tenant.set_external_timestamp_provider(tenant_store.clone());
    tenant.set_pd_region_policy(tenant_pd.clone());
    tenant.load_from_cluster(vec![("tidb_external_ts".to_owned(), "2000".to_owned())]);
    tenant.rebuild_sys_var_cache().unwrap();
    tenant.arm_domain_hooks();
    assert_eq!(*tidb_vardef::CLOUD_STORAGE_URI.read().unwrap(), "local:///tmp/configured-uri");

    // A PD option refresh on the configured keyspace reaches its store's
    // client; the tenant's is untouched (not even seeded).
    configured.set("tidb_tso_client_rpc_mode", "PARALLEL-FAST".to_owned()).unwrap();
    assert_eq!(configured_pd.rpc_concurrency.load(SeqCst), 4);
    assert!(tenant_pd.calls.lock().unwrap().is_empty(), "{:?}", tenant_pd.calls.lock().unwrap());

    // The tenant's rebuild wrote no external timestamp.
    assert!(tenant_store.sets.lock().unwrap().is_empty(), "{:?}", tenant_store.sets.lock().unwrap());
    assert!(configured_store.sets.lock().unwrap().is_empty(), "{:?}", configured_store.sets.lock().unwrap());

    // Only the configured keyspace writes the cluster's external timestamp
    // (a tenant's `SET GLOBAL` is refused before it reaches the table); a
    // write that reaches a tenant's table anyway runs no hook.
    configured.set("tidb_external_ts", "1000".to_owned()).unwrap();
    assert_eq!(*configured_store.sets.lock().unwrap(), [1000]);
    let _ = tenant.set("tidb_external_ts", "3000".to_owned());
    tenant.rebuild_sys_var_cache().unwrap();
    assert!(tenant_store.sets.lock().unwrap().is_empty(), "{:?}", tenant_store.sets.lock().unwrap());
    assert_eq!(*configured_store.sets.lock().unwrap(), [1000]);
    assert_eq!(configured.get("tidb_external_ts").unwrap(), "1000");
}
