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

//! `SET GLOBAL` of a vardef global static in one keyspace leaves every
//! other keyspace's value alone
//! (`rust/docs/design/2026-11-24-multi-keyspace-server.md`, §6.3), for each
//! static a keyspace snapshot keeps. Separate executable: the process-backed
//! table's hooks write the process statics.

use tidb_domain::global_sysvars::GlobalSysvars;
use tidb_vardef::classification::PROCESS_STATICS;
use tidb_vardef::snapshot::SNAPSHOT_STATICS;
use tidb_variable::sysvar::{get_sys_var, TypeFlag, SCOPE_GLOBAL};

/// A valid GLOBAL value of `name` other than its default.
fn other_value(name: &str) -> String {
    match name {
        "tidb_ttl_job_schedule_window_start_time" => return "01:30 +0000".to_owned(),
        "tidb_ttl_job_schedule_window_end_time" => return "22:15 +0000".to_owned(),
        "tidb_advancer_check_point_lag_limit" => return "1h0m0s".to_owned(),
        "tidb_cloud_storage_uri" => return "s3://bucket/prefix".to_owned(),
        "tidb_analyze_column_options" => return "PREDICATE".to_owned(),
        "tidb_ddl_reorg_max_write_speed" => return "1MiB".to_owned(),
        _ => {}
    }
    let def = get_sys_var(name).unwrap();
    let default = def.validate_in_scope(def.value, SCOPE_GLOBAL).map_or_else(|_| def.value.to_owned(), |v| v.value);
    match def.var_type {
        TypeFlag::Bool => if default.eq_ignore_ascii_case("ON") || default == "1" { "OFF" } else { "ON" }.to_owned(),
        TypeFlag::Enum => def
            .possible_values
            .iter()
            .find(|value| !value.eq_ignore_ascii_case(&default))
            .unwrap()
            .to_string(),
        TypeFlag::Int | TypeFlag::Unsigned => {
            let value: i64 = default.parse().unwrap();
            if u64::try_from(value + 1).is_ok_and(|next| next <= def.max_value) {
                (value + 1).to_string()
            } else {
                (value - 1).to_string()
            }
        }
        other => panic!("{name}: give a value for {other:?}"),
    }
}

/// The kept statics whose variable no keyspace can change in this test, and
/// why. A listed variable must really refuse the value (so the list cannot
/// go stale), and an unlisted one must change.
fn expected_refusal(sysvar: &str) -> Option<&'static str> {
    // The nextgen read-only variables (Go `IsReadOnlyVarInNextGen`) are
    // refused by the SQL layer; their tables take the value, so they are
    // verified here like the rest.
    (sysvar == "tidb_enable_auto_analyze_priority_queue")
        .then_some("deprecated: Go refuses OFF and the default is ON")
}

/// For each global static a keyspace keeps: a keyspace that owns its values
/// changes its own variable and neither the process statics (the configured
/// keyspace's values) nor any other keyspace's; and the configured keyspace
/// changes the process statics without reaching the other keyspace.
#[test]
fn set_global_in_one_keyspace_leaves_the_others_alone() {
    let configured = GlobalSysvars::new();
    let tenant = GlobalSysvars::new().with_keyspace_vardef();
    let other_tenant = GlobalSysvars::new().with_keyspace_vardef();
    let mut checked = Vec::new();
    let mut refused = Vec::new();
    for name in SNAPSHOT_STATICS {
        let sysvar = PROCESS_STATICS.iter().find(|entry| entry.name == *name).unwrap().sysvar.unwrap();
        if sysvar == "tidb_auto_analyze_concurrency" {
            // Its validation requires both auto-analyze switches of the
            // table being written; earlier iterations may have turned them
            // off.
            for table in [&configured, &tenant, &other_tenant] {
                table.set("tidb_enable_auto_analyze", "ON".to_owned()).unwrap();
            }
        }
        let value = other_value(sysvar);
        let before = configured.get(sysvar).unwrap();
        let other_before = other_tenant.get(sysvar).unwrap();

        if let Err(error) = tenant.set(sysvar, value.clone()) {
            let reason = expected_refusal(sysvar).unwrap_or_else(|| panic!("{sysvar}={value} refused: {error:?}"));
            assert!(configured.set(sysvar, value.clone()).is_err(), "{sysvar}: {reason}, in every keyspace");
            refused.push(sysvar);
            continue;
        }
        assert!(expected_refusal(sysvar).is_none(), "{sysvar} changed: drop it from the expected refusals");
        let changed = tenant.get(sysvar).unwrap();
        assert_ne!(changed, before, "{sysvar}: the tenant's SET GLOBAL took effect");
        assert_eq!(configured.get(sysvar).unwrap(), before, "{sysvar}: the process statics are unchanged");
        assert_eq!(other_tenant.get(sysvar).unwrap(), other_before, "{sysvar}: another tenant is unchanged");

        configured.set(sysvar, value.clone()).unwrap();
        assert_eq!(configured.get(sysvar).unwrap(), changed, "{sysvar}: the configured keyspace's SET GLOBAL");
        assert_eq!(other_tenant.get(sysvar).unwrap(), other_before, "{sysvar}: the process statics are not a tenant's");
        checked.push(sysvar);
    }
    // Every kept static was either verified or refused for a listed reason.
    assert_eq!(checked.len() + refused.len(), SNAPSHOT_STATICS.len());
    assert!(checked.contains(&"tidb_enable_foreign_key"));
    assert!(checked.contains(&"tidb_auto_analyze_concurrency"));

    // The snapshot itself: the tenant's hook wrote its own static.
    use std::sync::atomic::Ordering::SeqCst;
    assert!(!tenant.vardef().enable_foreign_key().load(SeqCst));
    assert!(other_tenant.vardef().enable_foreign_key().load(SeqCst));
}
