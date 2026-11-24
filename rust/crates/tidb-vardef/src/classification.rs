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

//! Which of this crate's process-wide statics a node serving several
//! keyspaces keeps per keyspace
//! (`rust/docs/design/2026-11-24-multi-keyspace-server.md`, §6.3).
//!
//! Go keeps these values in process atomics because a Go process serves
//! one keyspace. Each static is classified here:
//!
//! - [`StaticScope::Global`]: Go's "for this cluster" semantics. A
//!   keyspace's `SET GLOBAL` changes it for that keyspace only, so a
//!   multi-keyspace node keeps one value per keyspace runtime.
//! - [`StaticScope::Instance`]: truly per process (its memory, its log, its
//!   PD client, Go `ScopeInstance` variables). It stays process-wide, set
//!   by the process configuration or the operator, not by a tenant.
//! - [`StaticScope::Internal`]: not written by a system variable's
//!   `SetGlobal` (a status counter, a configuration-derived flag, a value
//!   with no system variable).
//!
//! The tests keep this table equal to the statics `lib.rs` declares, and the
//! design appendix equal to this table, so a new static cannot go
//! unclassified.

/// How a node serving several keyspaces scopes one process-wide static.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StaticScope {
    /// One value per keyspace (Go "for this cluster").
    Global,
    /// One value per process.
    Instance,
    /// Not written by a `SetGlobal` hook.
    Internal,
}

impl StaticScope {
    /// The class's name in the design appendix.
    pub fn name(self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::Instance => "instance",
            Self::Internal => "internal",
        }
    }
}

/// One classified static of this crate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProcessStatic {
    /// The static's name in `lib.rs`.
    pub name: &'static str,
    /// The system variable whose `SetGlobal` writes it, if any.
    pub sysvar: Option<&'static str>,
    /// How a multi-keyspace node scopes it.
    pub scope: StaticScope,
    /// What the value governs, which decides its scope.
    pub governs: &'static str,
}

/// Every static `lib.rs` declares, in declaration order.
pub const PROCESS_STATICS: &[ProcessStatic] = &[
    ProcessStatic { name: "ENABLE_MDL", sysvar: Some("tidb_enable_metadata_lock"), scope: StaticScope::Instance, governs: "metadata locking on a classic node (next-generation kernels, every multi-keyspace node among them, always lock)" },
    ProcessStatic { name: "DDL_FLASHBACK_CONCURRENCY", sysvar: Some("tidb_ddl_flashback_concurrency"), scope: StaticScope::Global, governs: "the keyspace's DDL jobs" },
    ProcessStatic { name: "DDL_REORG_WORKER_COUNTER", sysvar: Some("tidb_ddl_reorg_worker_cnt"), scope: StaticScope::Global, governs: "the keyspace's DDL jobs" },
    ProcessStatic { name: "DDL_REORG_BATCH_SIZE", sysvar: Some("tidb_ddl_reorg_batch_size"), scope: StaticScope::Global, governs: "the keyspace's DDL jobs" },
    ProcessStatic { name: "DDL_ERROR_COUNT_LIMIT", sysvar: Some("tidb_ddl_error_count_limit"), scope: StaticScope::Global, governs: "the keyspace's DDL jobs" },
    ProcessStatic { name: "MAX_AUTO_ANALYZE_TIME", sysvar: Some("tidb_max_auto_analyze_time"), scope: StaticScope::Global, governs: "the keyspace's auto-analyze" },
    ProcessStatic { name: "ENABLE_WORKLOAD_BASED_LEARNING", sysvar: None, scope: StaticScope::Internal, governs: "no system variable" },
    ProcessStatic { name: "WORKLOAD_BASED_LEARNING_INTERVAL", sysvar: None, scope: StaticScope::Internal, governs: "no system variable" },
    ProcessStatic { name: "ENABLE_FAST_CREATE_TABLE", sysvar: Some("tidb_enable_fast_create_table"), scope: StaticScope::Global, governs: "the keyspace's DDL submission" },
    ProcessStatic { name: "IS_SAND_BOX_MODE_ENABLED", sysvar: None, scope: StaticScope::Internal, governs: "process config (`security.disconnect-on-expired-password`)" },
    ProcessStatic { name: "TABLE_CACHE_LEASE", sysvar: Some("tidb_table_cache_lease"), scope: StaticScope::Global, governs: "the keyspace's cached tables" },
    ProcessStatic { name: "ANALYZE_DEFAULT_NUM_BUCKETS", sysvar: Some("tidb_analyze_default_num_buckets"), scope: StaticScope::Global, governs: "the keyspace's statistics" },
    ProcessStatic { name: "ANALYZE_DEFAULT_NUM_TOP_N", sysvar: Some("tidb_analyze_default_num_topn"), scope: StaticScope::Global, governs: "the keyspace's statistics" },
    ProcessStatic { name: "STATS_CACHE_MEM_QUOTA", sysvar: Some("tidb_stats_cache_mem_quota"), scope: StaticScope::Instance, governs: "process memory" },
    ProcessStatic { name: "MEMORY_USAGE_ALARM_RATIO", sysvar: Some("tidb_memory_usage_alarm_ratio"), scope: StaticScope::Instance, governs: "process memory" },
    ProcessStatic { name: "FORCE_PRIORITY", sysvar: Some("tidb_force_priority"), scope: StaticScope::Instance, governs: "Go `ScopeInstance`" },
    ProcessStatic { name: "MEMORY_USAGE_ALARM_KEEP_RECORD_NUM", sysvar: Some("tidb_memory_usage_alarm_keep_record_num"), scope: StaticScope::Instance, governs: "process memory" },
    ProcessStatic { name: "PLAN_REPLAYER_FILE_RETENTION_TIME", sysvar: None, scope: StaticScope::Internal, governs: "no SetGlobal writer" },
    ProcessStatic { name: "CONNECT_ATTRS_SIZE", sysvar: Some("performance_schema_session_connect_attrs_size"), scope: StaticScope::Global, governs: "the keyspace's connections" },
    ProcessStatic { name: "CONNECT_ATTRS_LONGEST_SEEN", sysvar: None, scope: StaticScope::Internal, governs: "status counter" },
    ProcessStatic { name: "CONNECT_ATTRS_LOST", sysvar: None, scope: StaticScope::Internal, governs: "status counter" },
    ProcessStatic { name: "MAX_DELTA_SCHEMA_COUNT", sysvar: Some("tidb_max_delta_schema_count"), scope: StaticScope::Global, governs: "the keyspace's schema validator" },
    ProcessStatic { name: "ENABLE_ROW_LEVEL_CHECKSUM", sysvar: Some("tidb_enable_row_level_checksum"), scope: StaticScope::Global, governs: "the keyspace's row encoding" },
    ProcessStatic { name: "ENABLE_FOREIGN_KEY", sysvar: Some("tidb_enable_foreign_key"), scope: StaticScope::Global, governs: "the keyspace's DDL policy" },
    ProcessStatic { name: "ENABLE_CHECK_CONSTRAINT", sysvar: Some("tidb_enable_check_constraint"), scope: StaticScope::Global, governs: "the keyspace's DDL policy" },
    ProcessStatic { name: "ENABLE_FAST_REORG", sysvar: Some("tidb_ddl_enable_fast_reorg"), scope: StaticScope::Global, governs: "the keyspace's DDL jobs" },
    ProcessStatic { name: "SERVICE_SCOPE", sysvar: Some("tidb_service_scope"), scope: StaticScope::Instance, governs: "Go `ScopeInstance`: this node's DXF scope" },
    ProcessStatic { name: "CLOUD_STORAGE_URI", sysvar: Some("tidb_cloud_storage_uri"), scope: StaticScope::Global, governs: "the keyspace's import and DDL storage" },
    ProcessStatic { name: "DDL_DISK_QUOTA", sysvar: Some("tidb_ddl_disk_quota"), scope: StaticScope::Instance, governs: "the node's local disk, which every keyspace's ingest shares" },
    ProcessStatic { name: "ENABLE_TTL_JOB", sysvar: Some("tidb_ttl_job_enable"), scope: StaticScope::Global, governs: "the keyspace's TTL jobs" },
    ProcessStatic { name: "TTL_SCAN_BATCH_SIZE", sysvar: Some("tidb_ttl_scan_batch_size"), scope: StaticScope::Global, governs: "the keyspace's TTL jobs" },
    ProcessStatic { name: "TTL_DELETE_BATCH_SIZE", sysvar: Some("tidb_ttl_delete_batch_size"), scope: StaticScope::Global, governs: "the keyspace's TTL jobs" },
    ProcessStatic { name: "TTL_DELETE_RATE_LIMIT", sysvar: Some("tidb_ttl_delete_rate_limit"), scope: StaticScope::Global, governs: "the keyspace's TTL jobs" },
    ProcessStatic { name: "TTL_JOB_SCHEDULE_WINDOW_START_TIME", sysvar: Some("tidb_ttl_job_schedule_window_start_time"), scope: StaticScope::Global, governs: "the keyspace's TTL jobs" },
    ProcessStatic { name: "TTL_JOB_SCHEDULE_WINDOW_END_TIME", sysvar: Some("tidb_ttl_job_schedule_window_end_time"), scope: StaticScope::Global, governs: "the keyspace's TTL jobs" },
    ProcessStatic { name: "TTL_SCAN_WORKER_COUNT", sysvar: Some("tidb_ttl_scan_worker_count"), scope: StaticScope::Global, governs: "the keyspace's TTL jobs" },
    ProcessStatic { name: "TTL_DELETE_WORKER_COUNT", sysvar: Some("tidb_ttl_delete_worker_count"), scope: StaticScope::Global, governs: "the keyspace's TTL jobs" },
    ProcessStatic { name: "TTL_RUNNING_TASKS", sysvar: Some("tidb_ttl_running_tasks"), scope: StaticScope::Global, governs: "the keyspace's TTL jobs" },
    ProcessStatic { name: "ENABLE_INSTANCE_PLAN_CACHE", sysvar: Some("tidb_enable_instance_plan_cache"), scope: StaticScope::Instance, governs: "the process's plan cache" },
    ProcessStatic { name: "INSTANCE_PLAN_CACHE_MAX_MEM_SIZE", sysvar: Some("tidb_instance_plan_cache_max_size"), scope: StaticScope::Instance, governs: "process memory" },
    ProcessStatic { name: "INSTANCE_PLAN_CACHE_RESERVED_PERCENTAGE", sysvar: Some("tidb_instance_plan_cache_reserved_percentage"), scope: StaticScope::Instance, governs: "process memory" },
    ProcessStatic { name: "TXN_ENTRY_SIZE_LIMIT", sysvar: Some("tidb_txn_entry_size_limit"), scope: StaticScope::Global, governs: "the keyspace's transactions" },
    ProcessStatic { name: "SCHEMA_CACHE_SIZE", sysvar: Some("tidb_schema_cache_size"), scope: StaticScope::Instance, governs: "process memory" },
    ProcessStatic { name: "ENABLE_TSO_FOLLOWER_PROXY", sysvar: Some("tidb_enable_tso_follower_proxy"), scope: StaticScope::Instance, governs: "the PD client" },
    ProcessStatic { name: "ENABLE_PD_FOLLOWER_HANDLE_REGION", sysvar: Some("pd_enable_follower_handle_region"), scope: StaticScope::Instance, governs: "the PD client" },
    ProcessStatic { name: "ENABLE_BATCH_QUERY_REGION", sysvar: Some("tidb_enable_batch_query_region"), scope: StaticScope::Instance, governs: "the PD client" },
    ProcessStatic { name: "MAX_TSO_BATCH_WAIT_INTERVAL", sysvar: Some("tidb_tso_client_batch_max_wait_time"), scope: StaticScope::Instance, governs: "the PD client" },
    ProcessStatic { name: "LOW_RESOLUTION_TSO_UPDATE_INTERVAL", sysvar: Some("tidb_low_resolution_tso_update_interval"), scope: StaticScope::Instance, governs: "the PD client" },
    ProcessStatic { name: "SCHEMA_CACHE_SIZE_ORIGIN_TEXT", sysvar: Some("tidb_schema_cache_size"), scope: StaticScope::Instance, governs: "process memory (the text `SCHEMA_CACHE_SIZE` was set with)" },
    ProcessStatic { name: "CIRCUIT_BREAKER_PD_METADATA_ERROR_RATE_THRESHOLD_RATIO", sysvar: Some("tidb_cb_pd_metadata_error_rate_threshold_ratio"), scope: StaticScope::Instance, governs: "the PD client" },
    ProcessStatic { name: "RUN_AUTO_ANALYZE", sysvar: Some("tidb_enable_auto_analyze"), scope: StaticScope::Global, governs: "the keyspace's auto-analyze" },
    ProcessStatic { name: "ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE", sysvar: Some("tidb_enable_auto_analyze_priority_queue"), scope: StaticScope::Global, governs: "the keyspace's auto-analyze" },
    ProcessStatic { name: "AUTO_ANALYZE_CONCURRENCY", sysvar: Some("tidb_auto_analyze_concurrency"), scope: StaticScope::Global, governs: "the keyspace's auto-analyze" },
    ProcessStatic { name: "ENABLE_RESOURCE_CONTROL", sysvar: Some("tidb_enable_resource_control"), scope: StaticScope::Instance, governs: "the process TiKV client's resource-control switch" },
    ProcessStatic { name: "MAX_USER_CONNECTIONS_VALUE", sysvar: Some("max_user_connections"), scope: StaticScope::Global, governs: "the keyspace's accounts" },
    ProcessStatic { name: "ACCELERATE_USER_CREATION_UPDATE", sysvar: Some("tidb_accelerate_user_creation_update"), scope: StaticScope::Global, governs: "the keyspace's privilege cache" },
    ProcessStatic { name: "ENABLE_RESOURCE_CONTROL_STRICT_MODE", sysvar: Some("tidb_resource_control_strict_mode"), scope: StaticScope::Global, governs: "the keyspace's resource groups" },
    ProcessStatic { name: "OOM_ACTION", sysvar: Some("tidb_mem_oom_action"), scope: StaticScope::Global, governs: "the keyspace's statements" },
    ProcessStatic { name: "GOGC_TUNER_THRESHOLD", sysvar: Some("tidb_gogc_tuner_threshold"), scope: StaticScope::Instance, governs: "process memory" },
    ProcessStatic { name: "PLAN_REPLAYER_GC_LEASE", sysvar: None, scope: StaticScope::Internal, governs: "process internal" },
    ProcessStatic { name: "TI_FLASH_ENABLE_PIPELINE_MODE", sysvar: Some("tidb_enable_tiflash_pipeline_model"), scope: StaticScope::Global, governs: "the keyspace's TiFlash requests" },
    ProcessStatic { name: "PROCESS_GENERAL_LOG", sysvar: Some("tidb_general_log"), scope: StaticScope::Instance, governs: "Go `ScopeInstance`: the process's general log" },
    ProcessStatic { name: "ENABLE_RC_READ_CHECK_TS", sysvar: Some("tidb_rc_read_check_ts"), scope: StaticScope::Instance, governs: "Go `ScopeInstance`" },
    ProcessStatic { name: "PERSIST_ANALYZE_OPTIONS", sysvar: Some("tidb_persist_analyze_options"), scope: StaticScope::Global, governs: "the keyspace's statistics" },
    ProcessStatic { name: "STATS_LOAD_PSEUDO_TIMEOUT", sysvar: Some("tidb_stats_load_pseudo_timeout"), scope: StaticScope::Global, governs: "the keyspace's statistics" },
    ProcessStatic { name: "ENABLE_BATCH_DML", sysvar: Some("tidb_enable_batch_dml"), scope: StaticScope::Global, governs: "the keyspace's statements" },
    ProcessStatic { name: "ENABLE_NOOP_VARIABLES", sysvar: Some("tidb_enable_noop_variables"), scope: StaticScope::Global, governs: "the keyspace's sessions" },
    ProcessStatic { name: "ENABLE_TMP_STORAGE_ON_OOM", sysvar: Some("tidb_enable_tmp_storage_on_oom"), scope: StaticScope::Global, governs: "the keyspace's statements" },
    ProcessStatic { name: "ENABLE_BINDING_USAGE", sysvar: Some("tidb_enable_binding_usage"), scope: StaticScope::Global, governs: "the keyspace's bindings" },
    ProcessStatic { name: "M_LOG_LOG_SLOW_PURGE", sysvar: Some("tidb_mlog_log_slow_purge"), scope: StaticScope::Global, governs: "the keyspace's materialized views" },
    ProcessStatic { name: "IGNORE_INLIST_PLAN_DIGEST", sysvar: Some("tidb_ignore_inlist_plan_digest"), scope: StaticScope::Global, governs: "the keyspace's statement digests" },
    ProcessStatic { name: "ENABLE_CONNECTION_EVENT_LOG", sysvar: Some("tidb_enable_connection_event_log"), scope: StaticScope::Instance, governs: "the process's log" },
    ProcessStatic { name: "ENABLE_DIST_TASK", sysvar: Some("tidb_enable_dist_task"), scope: StaticScope::Global, governs: "the keyspace's DXF tasks" },
    ProcessStatic { name: "PASSWORD_VALIDATION_LENGTH", sysvar: Some("validate_password.length"), scope: StaticScope::Global, governs: "the keyspace's accounts" },
    ProcessStatic { name: "PASSWORD_VALIDATION_MIXED_CASE_COUNT", sysvar: Some("validate_password.mixed_case_count"), scope: StaticScope::Global, governs: "the keyspace's accounts" },
    ProcessStatic { name: "PASSWORD_VALIDTAION_NUMBER_COUNT", sysvar: Some("validate_password.number_count"), scope: StaticScope::Global, governs: "the keyspace's accounts" },
    ProcessStatic { name: "PASSWORD_VALIDATION_SPECIAL_CHAR_COUNT", sysvar: Some("validate_password.special_char_count"), scope: StaticScope::Global, governs: "the keyspace's accounts" },
    ProcessStatic { name: "PASSWORD_HISTORY", sysvar: Some("password_history"), scope: StaticScope::Global, governs: "the keyspace's accounts" },
    ProcessStatic { name: "PASSWORD_REUSE_INTERVAL", sysvar: Some("password_reuse_interval"), scope: StaticScope::Global, governs: "the keyspace's accounts" },
    ProcessStatic { name: "MEM_QUOTA_BINDING_CACHE", sysvar: Some("tidb_mem_quota_binding_cache"), scope: StaticScope::Instance, governs: "process memory" },
    ProcessStatic { name: "STATS_LOAD_SYNC_WAIT", sysvar: Some("tidb_stats_load_sync_wait"), scope: StaticScope::Global, governs: "the keyspace's statistics" },
    ProcessStatic { name: "QUERY_LOG_MAX_LEN", sysvar: Some("tidb_query_log_max_len"), scope: StaticScope::Instance, governs: "the process's log" },
    ProcessStatic { name: "PAGING_SIZE_BYTES", sysvar: Some("tidb_paging_size_bytes"), scope: StaticScope::Global, governs: "the keyspace's coprocessor requests" },
    ProcessStatic { name: "EXPENSIVE_QUERY_TIME_THRESHOLD", sysvar: Some("tidb_expensive_query_time_threshold"), scope: StaticScope::Instance, governs: "Go `ScopeInstance`: the process's watchdog" },
    ProcessStatic { name: "EXPENSIVE_TXN_TIME_THRESHOLD", sysvar: Some("tidb_expensive_txn_time_threshold"), scope: StaticScope::Instance, governs: "Go `ScopeInstance`: the process's watchdog" },
    ProcessStatic { name: "ENABLE_P_PROF_SQLCPU", sysvar: Some("tidb_pprof_sql_cpu"), scope: StaticScope::Instance, governs: "Go `ScopeInstance`: the process's profiler" },
    ProcessStatic { name: "PREPARED_PLAN_CACHE_MEMORY_GUARD_RATIO", sysvar: Some("tidb_prepared_plan_cache_memory_guard_ratio"), scope: StaticScope::Instance, governs: "process memory" },
    ProcessStatic { name: "ANALYZE_COLUMN_OPTIONS", sysvar: Some("tidb_analyze_column_options"), scope: StaticScope::Global, governs: "the keyspace's statistics" },
    ProcessStatic { name: "ADVANCER_CHECK_POINT_LAG_LIMIT", sysvar: Some("tidb_advancer_check_point_lag_limit"), scope: StaticScope::Global, governs: "the keyspace's log backup" },
    ProcessStatic { name: "ENABLE_LOCAL_TXN", sysvar: Some("tidb_enable_local_txn"), scope: StaticScope::Global, governs: "the keyspace's transactions" },
    ProcessStatic { name: "DDL_REORG_MAX_WRITE_SPEED", sysvar: Some("tidb_ddl_reorg_max_write_speed"), scope: StaticScope::Global, governs: "the keyspace's DDL jobs" },
    ProcessStatic { name: "DDL_SLOW_OPR_THRESHOLD", sysvar: Some("ddl_slow_threshold"), scope: StaticScope::Instance, governs: "Go `ScopeInstance`: the process's log" },
];

/// The statics classified `scope`.
pub fn statics_scoped(scope: StaticScope) -> impl Iterator<Item = &'static ProcessStatic> {
    PROCESS_STATICS.iter().filter(move |entry| entry.scope == scope)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The statics `lib.rs` declares, in order.
    fn declared_statics() -> Vec<String> {
        include_str!("lib.rs")
            .lines()
            .filter_map(|line| {
                let line = line.strip_prefix("pub ").unwrap_or(line);
                let rest = line.strip_prefix("static ")?;
                let name: String = rest.chars().take_while(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit() || *ch == '_').collect();
                (!name.is_empty()).then_some(name)
            })
            .collect()
    }

    /// Every static of `lib.rs` is classified, once, in declaration order:
    /// adding a static without classifying it fails here.
    #[test]
    fn every_static_is_classified() {
        let classified: Vec<&str> = PROCESS_STATICS.iter().map(|entry| entry.name).collect();
        assert_eq!(declared_statics(), classified);
    }

    /// A `SetGlobal`-written static names its system variable; an internal
    /// one names none.
    #[test]
    fn sysvars_follow_the_scope() {
        for entry in PROCESS_STATICS {
            assert_eq!(entry.sysvar.is_none(), entry.scope == StaticScope::Internal, "{entry:?}");
        }
    }
}
