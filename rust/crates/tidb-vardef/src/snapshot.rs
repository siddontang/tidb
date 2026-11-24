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

//! The per-keyspace values of this crate's *global* statics
//! (`rust/docs/design/2026-11-24-multi-keyspace-server.md`, §6.3).
//!
//! A [`VardefSnapshot`] is owned by one keyspace runtime's `GlobalSysvars`:
//! its `SetGlobal` hooks write it and its readers read it. A
//! single-keyspace node -- and the configured keyspace of a multi-keyspace
//! node -- uses the *process-backed* snapshot, whose accessors are the
//! statics themselves, so it reads exactly what it always read. Another
//! keyspace of a multi-keyspace node owns its values.
//!
//! A reader takes the snapshot from its session (`SessionVars::vardef`) or
//! its runtime (`Domain::vardef`); code below the session tiers (the DDL a
//! statement submits, the DDL owner's steps) takes the snapshot its thread
//! entered ([`enter`], [`current`]): a session enters its own while it runs
//! a statement on its store, and a DDL step enters its DDL's.

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU64, AtomicU8};
use std::sync::{Arc, LazyLock};

#[allow(unused_imports)]
use crate::{defaults, DayTime};
use crate::*;

macro_rules! vardef_snapshot {
    ($($field:ident: $ty:ty = $static:ident ($init:expr);)*) => {
        /// The values a keyspace owns.
        struct OwnedStatics {
            $($field: $ty,)*
        }

        impl OwnedStatics {
            fn new() -> Self {
                Self { $($field: $init,)* }
            }
        }

        impl VardefSnapshot {
            $(
                #[doc = concat!("The keyspace's `", stringify!($static), "`.")]
                pub fn $field(&self) -> &$ty {
                    match &self.owned {
                        Some(owned) => &owned.$field,
                        None => &$static,
                    }
                }
            )*
        }

        /// The global statics a [`VardefSnapshot`] keeps per keyspace, in
        /// declaration order.
        pub const SNAPSHOT_STATICS: &[&str] = &[$(stringify!($static),)*];
    };
}

/// One keyspace runtime's values of the global statics.
pub struct VardefSnapshot {
    owned: Option<Box<OwnedStatics>>,
}

impl std::fmt::Debug for VardefSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.owned.is_some() { "VardefSnapshot(per keyspace)" } else { "VardefSnapshot(process)" })
    }
}

impl VardefSnapshot {
    /// The snapshot whose values are the process statics: a single-keyspace
    /// node's, and the configured keyspace's on a multi-keyspace node.
    pub const fn process_backed() -> Self {
        Self { owned: None }
    }

    /// A keyspace's own values, starting from the statics' defaults.
    pub fn per_keyspace() -> Self {
        Self { owned: Some(Box::new(OwnedStatics::new())) }
    }

    /// Whether the keyspace owns its values.
    pub fn is_per_keyspace(&self) -> bool {
        self.owned.is_some()
    }

    /// Go `vardef.OOMAction.Load()` for this keyspace.
    pub fn oom_action_name(&self) -> &'static str {
        oom_action_of(self.oom_action())
    }

    /// Go `vardef.OOMAction.Store(value)` for this keyspace.
    pub fn set_oom_action(&self, value: &str) {
        store_oom_action(self.oom_action(), value);
    }
}

vardef_snapshot! {
    ddl_flashback_concurrency: std::sync::atomic::AtomicI32 = DDL_FLASHBACK_CONCURRENCY (std::sync::atomic::AtomicI32::new(defaults::DEF_TIDB_DDL_FLASHBACK_CONCURRENCY as i32));
    ddl_reorg_worker_counter: std::sync::atomic::AtomicI32 = DDL_REORG_WORKER_COUNTER (std::sync::atomic::AtomicI32::new(defaults::DEF_TIDB_DDL_REORG_WORKER_COUNT as i32));
    ddl_reorg_batch_size: std::sync::atomic::AtomicI32 = DDL_REORG_BATCH_SIZE (std::sync::atomic::AtomicI32::new(defaults::DEF_TIDB_DDL_REORG_BATCH_SIZE as i32));
    max_auto_analyze_time: AtomicI64 = MAX_AUTO_ANALYZE_TIME (AtomicI64::new(defaults::DEF_TIDB_MAX_AUTO_ANALYZE_TIME));
    enable_fast_create_table: std::sync::atomic::AtomicBool = ENABLE_FAST_CREATE_TABLE (std::sync::atomic::AtomicBool::new(defaults::DEF_TIDB_ENABLE_FAST_CREATE_TABLE));
    table_cache_lease: AtomicI64 = TABLE_CACHE_LEASE (AtomicI64::new(defaults::DEF_TIDB_TABLE_CACHE_LEASE));
    connect_attrs_size: AtomicI64 = CONNECT_ATTRS_SIZE (AtomicI64::new(defaults::DEF_CONNECT_ATTRS_SIZE));
    enable_row_level_checksum: AtomicBool = ENABLE_ROW_LEVEL_CHECKSUM (AtomicBool::new(defaults::DEF_TIDB_ENABLE_ROW_LEVEL_CHECKSUM));
    enable_foreign_key: AtomicBool = ENABLE_FOREIGN_KEY (AtomicBool::new(true));
    enable_check_constraint: AtomicBool = ENABLE_CHECK_CONSTRAINT (AtomicBool::new(defaults::DEF_TIDB_ENABLE_CHECK_CONSTRAINT));
    enable_fast_reorg: AtomicBool = ENABLE_FAST_REORG (AtomicBool::new(defaults::DEF_TIDB_ENABLE_FAST_REORG));
    ddl_disk_quota: AtomicU64 = DDL_DISK_QUOTA (AtomicU64::new(defaults::DEF_TIDB_DDL_DISK_QUOTA as u64));
    enable_ttl_job: AtomicBool = ENABLE_TTL_JOB (AtomicBool::new(defaults::DEF_TIDB_TTL_JOB_ENABLE));
    ttl_scan_batch_size: AtomicI64 = TTL_SCAN_BATCH_SIZE (AtomicI64::new(defaults::DEF_TIDB_TTL_SCAN_BATCH_SIZE));
    ttl_delete_batch_size: AtomicI64 = TTL_DELETE_BATCH_SIZE (AtomicI64::new(defaults::DEF_TIDB_TTL_DELETE_BATCH_SIZE));
    ttl_delete_rate_limit: AtomicI64 = TTL_DELETE_RATE_LIMIT (AtomicI64::new(defaults::DEF_TIDB_TTL_DELETE_RATE_LIMIT));
    ttl_job_schedule_window_start_time: std::sync::RwLock<DayTime> = TTL_JOB_SCHEDULE_WINDOW_START_TIME (std::sync::RwLock::new(DayTime::new(0, 0, 0)));
    ttl_job_schedule_window_end_time: std::sync::RwLock<DayTime> = TTL_JOB_SCHEDULE_WINDOW_END_TIME (std::sync::RwLock::new(DayTime::new(23, 59, 0)));
    ttl_scan_worker_count: AtomicI32 = TTL_SCAN_WORKER_COUNT (AtomicI32::new(defaults::DEF_TIDB_TTL_SCAN_WORKER_COUNT as i32));
    ttl_delete_worker_count: AtomicI32 = TTL_DELETE_WORKER_COUNT (AtomicI32::new(defaults::DEF_TIDB_TTL_DELETE_WORKER_COUNT as i32));
    ttl_running_tasks: AtomicI32 = TTL_RUNNING_TASKS (AtomicI32::new(defaults::DEF_TIDB_TTL_RUNNING_TASKS as i32));
    txn_entry_size_limit: AtomicU64 = TXN_ENTRY_SIZE_LIMIT (AtomicU64::new(defaults::DEF_TIDB_TXN_ENTRY_SIZE_LIMIT as u64));
    run_auto_analyze: AtomicBool = RUN_AUTO_ANALYZE (AtomicBool::new(defaults::DEF_TIDB_ENABLE_AUTO_ANALYZE));
    enable_auto_analyze_priority_queue: AtomicBool = ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE (AtomicBool::new(defaults::DEF_TIDB_ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE));
    auto_analyze_concurrency: AtomicI64 = AUTO_ANALYZE_CONCURRENCY (AtomicI64::new(defaults::DEF_TIDB_AUTO_ANALYZE_CONCURRENCY));
    enable_resource_control: AtomicBool = ENABLE_RESOURCE_CONTROL (AtomicBool::new(false));
    max_user_connections_value: std::sync::atomic::AtomicU32 = MAX_USER_CONNECTIONS_VALUE (std::sync::atomic::AtomicU32::new(defaults::DEF_MAX_USER_CONNECTIONS as u32));
    accelerate_user_creation_update: AtomicBool = ACCELERATE_USER_CREATION_UPDATE (AtomicBool::new(defaults::DEF_TIDB_ACCELERATE_USER_CREATION_UPDATE));
    enable_resource_control_strict_mode: AtomicBool = ENABLE_RESOURCE_CONTROL_STRICT_MODE (AtomicBool::new(defaults::DEF_TIDB_RESOURCE_CONTROL_STRICT_MODE));
    oom_action: AtomicU8 = OOM_ACTION (AtomicU8::new(OOM_ACTION_CANCEL));
    ti_flash_enable_pipeline_mode: AtomicBool = TI_FLASH_ENABLE_PIPELINE_MODE (AtomicBool::new(defaults::DEF_TIDB_ENABLE_TIFLASH_PIPELINE_MODE));
    persist_analyze_options: AtomicBool = PERSIST_ANALYZE_OPTIONS (AtomicBool::new(defaults::DEF_TIDB_PERSIST_ANALYZE_OPTIONS));
    stats_load_pseudo_timeout: AtomicBool = STATS_LOAD_PSEUDO_TIMEOUT (AtomicBool::new(defaults::DEF_TIDB_STATS_LOAD_PSEUDO_TIMEOUT));
    enable_batch_dml: AtomicBool = ENABLE_BATCH_DML (AtomicBool::new(false));
    enable_noop_variables: AtomicBool = ENABLE_NOOP_VARIABLES (AtomicBool::new(defaults::DEF_TIDB_ENABLE_NOOP_VARIABLES));
    enable_tmp_storage_on_oom: AtomicBool = ENABLE_TMP_STORAGE_ON_OOM (AtomicBool::new(defaults::DEF_TIDB_ENABLE_TMP_STORAGE_ON_OOM));
    enable_binding_usage: AtomicBool = ENABLE_BINDING_USAGE (AtomicBool::new(defaults::DEF_TIDB_ENABLE_BINDING_USAGE));
    m_log_log_slow_purge: AtomicBool = M_LOG_LOG_SLOW_PURGE (AtomicBool::new(defaults::DEF_TIDB_MLOG_LOG_SLOW_PURGE));
    ignore_inlist_plan_digest: AtomicBool = IGNORE_INLIST_PLAN_DIGEST (AtomicBool::new(defaults::DEF_TIDB_IGNORE_INLIST_PLAN_DIGEST));
    enable_dist_task: AtomicBool = ENABLE_DIST_TASK (AtomicBool::new(defaults::DEF_TIDB_ENABLE_DIST_TASK));
    password_validation_length: AtomicI32 = PASSWORD_VALIDATION_LENGTH (AtomicI32::new(8));
    password_validation_mixed_case_count: AtomicI32 = PASSWORD_VALIDATION_MIXED_CASE_COUNT (AtomicI32::new(1));
    password_validtaion_number_count: AtomicI32 = PASSWORD_VALIDTAION_NUMBER_COUNT (AtomicI32::new(1));
    password_validation_special_char_count: AtomicI32 = PASSWORD_VALIDATION_SPECIAL_CHAR_COUNT (AtomicI32::new(1));
    password_history: AtomicI64 = PASSWORD_HISTORY (AtomicI64::new(defaults::DEF_PASSWORD_REUSE_HISTORY));
    password_reuse_interval: AtomicI64 = PASSWORD_REUSE_INTERVAL (AtomicI64::new(defaults::DEF_PASSWORD_REUSE_TIME));
    stats_load_sync_wait: AtomicI64 = STATS_LOAD_SYNC_WAIT (AtomicI64::new(defaults::DEF_TIDB_STATS_LOAD_SYNC_WAIT));
    paging_size_bytes: AtomicI64 = PAGING_SIZE_BYTES (AtomicI64::new(defaults::DEF_PAGING_SIZE_BYTES));
    analyze_column_options: std::sync::RwLock<String> = ANALYZE_COLUMN_OPTIONS (std::sync::RwLock::new(String::new()));
    advancer_check_point_lag_limit: AtomicI64 = ADVANCER_CHECK_POINT_LAG_LIMIT (AtomicI64::new(defaults::DEF_TIDB_ADVANCER_CHECK_POINT_LAG_LIMIT));
    enable_local_txn: AtomicBool = ENABLE_LOCAL_TXN (AtomicBool::new(defaults::DEF_TIDB_ENABLE_LOCAL_TXN));
    ddl_reorg_max_write_speed: std::sync::atomic::AtomicI64 = DDL_REORG_MAX_WRITE_SPEED (std::sync::atomic::AtomicI64::new(defaults::DEF_TIDB_DDL_REORG_MAX_WRITE_SPEED));
}

/// The process-backed snapshot.
static PROCESS: LazyLock<Arc<VardefSnapshot>> = LazyLock::new(|| Arc::new(VardefSnapshot::process_backed()));

/// The process-backed snapshot every reader without a keyspace runtime uses.
pub fn process() -> Arc<VardefSnapshot> {
    Arc::clone(&PROCESS)
}

thread_local! {
    static CURRENT: RefCell<Option<Arc<VardefSnapshot>>> = const { RefCell::new(None) };
}

/// The snapshot the current thread reads until dropped (then the previous
/// one).
#[must_use = "the thread leaves the snapshot when the guard drops"]
pub struct EnteredVardef {
    previous: Option<Arc<VardefSnapshot>>,
    /// Bound to the entering thread: the drop restores its state.
    _thread_bound: std::marker::PhantomData<*const ()>,
}

impl Drop for EnteredVardef {
    fn drop(&mut self) {
        let previous = self.previous.take();
        CURRENT.with(|current| *current.borrow_mut() = previous);
    }
}

/// Makes `snapshot` the one [`current`] answers on this thread: a session
/// enters its own while it runs a statement, a DDL step its DDL's.
pub fn enter(snapshot: Arc<VardefSnapshot>) -> EnteredVardef {
    let previous = CURRENT.with(|current| current.borrow_mut().replace(snapshot));
    EnteredVardef {
        previous,
        _thread_bound: std::marker::PhantomData,
    }
}

/// The snapshot this thread entered, else the process-backed one.
pub fn current() -> Arc<VardefSnapshot> {
    CURRENT.with(|current| current.borrow().clone()).unwrap_or_else(process)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering::SeqCst;

    /// The snapshot keeps exactly the classified global statics whose only
    /// writers are their `SetGlobal` hooks; the other global statics are
    /// still process-wide (listed in the design's §6.3 follow-up).
    #[test]
    fn the_snapshot_keeps_global_statics_only() {
        for name in SNAPSHOT_STATICS {
            let entry = crate::classification::PROCESS_STATICS.iter().find(|entry| entry.name == *name).unwrap();
            assert_eq!(entry.scope, crate::classification::StaticScope::Global, "{name}");
        }
    }

    /// A keyspace's own values start at the statics' defaults and are
    /// independent of the process's; the process-backed snapshot is the
    /// statics.
    #[test]
    fn a_keyspace_snapshot_is_independent() {
        let own = VardefSnapshot::per_keyspace();
        let process = process();
        assert_eq!(own.enable_foreign_key().load(SeqCst), ENABLE_FOREIGN_KEY.load(SeqCst));
        assert!(std::ptr::eq(process.enable_foreign_key(), &ENABLE_FOREIGN_KEY));
        own.enable_foreign_key().store(false, SeqCst);
        assert!(ENABLE_FOREIGN_KEY.load(SeqCst));
        own.set_oom_action("LOG");
        assert_eq!(own.oom_action_name(), "LOG");
        assert_eq!(oom_action(), tidb_vars::OOM_ACTION_CANCEL);
    }

    /// The entered snapshot is the thread's until its guard drops.
    #[test]
    fn current_follows_the_entered_snapshot() {
        let own = Arc::new(VardefSnapshot::per_keyspace());
        assert!(!current().is_per_keyspace());
        {
            let _entered = enter(Arc::clone(&own));
            assert!(current().is_per_keyspace());
        }
        assert!(!current().is_per_keyspace());
    }
}
