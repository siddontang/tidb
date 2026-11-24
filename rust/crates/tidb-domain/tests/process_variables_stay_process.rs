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

//! A keyspace that owns its vardef values never writes process-wide state
//! through a process variable's `SetGlobal` hook
//! (`rust/docs/design/2026-11-24-multi-keyspace-server.md`, §6.3): neither
//! for a value its cache rebuild loads from its stored rows (a runtime's open
//! and its reload loop) nor for a write. The configured keyspace's table,
//! over the process statics, still does. Separate executable: the test
//! changes process state.

use tidb_domain::global_sysvars::GlobalSysvars;

/// The process-wide products of the process variables under test, as the
/// process (the configured keyspace's table) reports them.
#[derive(Debug, PartialEq, Eq)]
struct ProcessState {
    mem_quota_analyze: String,
    top_sql_series: String,
    general_log: bool,
    resource_control_client: bool,
    resource_control_static: bool,
}

fn process_state(configured: &GlobalSysvars) -> ProcessState {
    use std::sync::atomic::Ordering::SeqCst;
    ProcessState {
        mem_quota_analyze: configured.get("tidb_mem_quota_analyze").unwrap(),
        top_sql_series: configured.get("tidb_top_sql_max_time_series_count").unwrap(),
        general_log: tidb_vardef::PROCESS_GENERAL_LOG.load(SeqCst),
        resource_control_client: tikv_client::resource_control_enabled(),
        resource_control_static: tidb_vardef::ENABLE_RESOURCE_CONTROL.load(SeqCst),
    }
}

/// Stored rows a tenant persisted, each different from the process's.
fn tenant_rows(before: &ProcessState) -> Vec<(String, String)> {
    let mem_quota: i64 = before.mem_quota_analyze.parse().unwrap();
    let series: i64 = before.top_sql_series.parse().unwrap();
    vec![
        ("tidb_mem_quota_analyze".to_owned(), (mem_quota + 1024).to_string()),
        ("tidb_top_sql_max_time_series_count".to_owned(), (series + 7).to_string()),
        ("tidb_general_log".to_owned(), if before.general_log { "OFF" } else { "ON" }.to_owned()),
        (
            "tidb_enable_resource_control".to_owned(),
            if before.resource_control_static { "OFF" } else { "ON" }.to_owned(),
        ),
    ]
}

#[test]
fn a_tenant_rebuild_leaves_process_state_alone() {
    let configured = GlobalSysvars::new();
    let tenant = GlobalSysvars::new().with_keyspace_vardef();
    let before = process_state(&configured);
    let rows = tenant_rows(&before);

    // The tenant runtime's open and reload loop: its cache rebuilt from its
    // stored rows, running every GLOBAL hook with the stored value.
    tenant.load_from_cluster(rows.clone());
    tenant.rebuild_sys_var_cache().unwrap();
    assert_eq!(process_state(&configured), before, "a tenant rebuild");
    // A write through the tenant's table (the SQL refusal aside).
    for (name, value) in &rows {
        let _ = tenant.set(name, value.clone());
    }
    assert_eq!(process_state(&configured), before, "a tenant write");

    // The configured keyspace's rebuild does apply them to the process.
    configured.load_from_cluster(rows);
    configured.rebuild_sys_var_cache().unwrap();
    let after = process_state(&configured);
    assert_ne!(after.mem_quota_analyze, before.mem_quota_analyze);
    assert_ne!(after.top_sql_series, before.top_sql_series);
    assert_ne!(after.resource_control_static, before.resource_control_static);
    assert_eq!(after.resource_control_client, after.resource_control_static, "the TiKV client follows");
    assert_ne!(after.resource_control_client, before.resource_control_client);
}
