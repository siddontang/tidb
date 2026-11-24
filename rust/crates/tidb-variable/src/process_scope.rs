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

//! Which system variables a keyspace of a multi-keyspace node may set
//! (`rust/docs/design/2026-11-24-multi-keyspace-server.md`, §6.3), decided by
//! where each `SetGlobal` hook writes.
//!
//! A hook that writes only the keyspace's vardef snapshot (or nothing) keeps
//! the variable the keyspace's own. A hook that writes process-wide state --
//! another crate's static, singleton, collector or client, the process
//! configuration, or a vardef static not kept per keyspace -- makes it a
//! *process variable*: in multi-keyspace mode a tenant cannot set it, and a
//! keyspace that owns its vardef values never runs its hook (neither for a
//! `SET` nor for the values its cache rebuild loads), so a tenant's stored
//! value cannot reach the process.

use tidb_vardef::classification::{StaticScope, PROCESS_STATICS};
use tidb_vardef::snapshot::SNAPSHOT_STATICS;

use crate::sysvar::{get_sys_var, SCOPE_GLOBAL, SCOPE_INSTANCE, SCOPE_SESSION};

/// Where a `SetGlobal` hook's write lands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HookScope {
    /// The keyspace's vardef snapshot, or nothing outside the session.
    Keyspace,
    /// State the whole process shares.
    Process,
}

impl HookScope {
    /// The scope's name in the design appendix.
    pub fn name(self) -> &'static str {
        match self {
            Self::Keyspace => "keyspace",
            Self::Process => "process",
        }
    }
}

/// One registry variable with a `SetGlobal` hook.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HookDestination {
    /// The system variable.
    pub sysvar: &'static str,
    /// Where its hook writes.
    pub scope: HookScope,
    /// What its hook writes.
    pub writes: &'static str,
}

/// Every registry variable with a `SetGlobal` hook, by name.
pub const HOOK_DESTINATIONS: &[HookDestination] = &[
    HookDestination { sysvar: "authentication_ldap_sasl_auth_method_name", scope: HookScope::Process, writes: "the process LDAP authentication" },
    HookDestination { sysvar: "authentication_ldap_sasl_bind_base_dn", scope: HookScope::Process, writes: "the process LDAP authentication" },
    HookDestination { sysvar: "authentication_ldap_sasl_bind_root_dn", scope: HookScope::Process, writes: "the process LDAP authentication" },
    HookDestination { sysvar: "authentication_ldap_sasl_bind_root_pwd", scope: HookScope::Process, writes: "the process LDAP authentication" },
    HookDestination { sysvar: "authentication_ldap_sasl_ca_path", scope: HookScope::Process, writes: "the process LDAP authentication" },
    HookDestination { sysvar: "authentication_ldap_sasl_init_pool_size", scope: HookScope::Process, writes: "the process LDAP authentication" },
    HookDestination { sysvar: "authentication_ldap_sasl_max_pool_size", scope: HookScope::Process, writes: "the process LDAP authentication" },
    HookDestination { sysvar: "authentication_ldap_sasl_server_host", scope: HookScope::Process, writes: "the process LDAP authentication" },
    HookDestination { sysvar: "authentication_ldap_sasl_server_port", scope: HookScope::Process, writes: "the process LDAP authentication" },
    HookDestination { sysvar: "authentication_ldap_sasl_tls", scope: HookScope::Process, writes: "the process LDAP authentication" },
    HookDestination { sysvar: "authentication_ldap_sasl_user_search_attr", scope: HookScope::Process, writes: "the process LDAP authentication" },
    HookDestination { sysvar: "authentication_ldap_simple_auth_method_name", scope: HookScope::Process, writes: "the process LDAP authentication" },
    HookDestination { sysvar: "authentication_ldap_simple_bind_base_dn", scope: HookScope::Process, writes: "the process LDAP authentication" },
    HookDestination { sysvar: "authentication_ldap_simple_bind_root_dn", scope: HookScope::Process, writes: "the process LDAP authentication" },
    HookDestination { sysvar: "authentication_ldap_simple_bind_root_pwd", scope: HookScope::Process, writes: "the process LDAP authentication" },
    HookDestination { sysvar: "authentication_ldap_simple_ca_path", scope: HookScope::Process, writes: "the process LDAP authentication" },
    HookDestination { sysvar: "authentication_ldap_simple_init_pool_size", scope: HookScope::Process, writes: "the process LDAP authentication" },
    HookDestination { sysvar: "authentication_ldap_simple_max_pool_size", scope: HookScope::Process, writes: "the process LDAP authentication" },
    HookDestination { sysvar: "authentication_ldap_simple_server_host", scope: HookScope::Process, writes: "the process LDAP authentication" },
    HookDestination { sysvar: "authentication_ldap_simple_server_port", scope: HookScope::Process, writes: "the process LDAP authentication" },
    HookDestination { sysvar: "authentication_ldap_simple_tls", scope: HookScope::Process, writes: "the process LDAP authentication" },
    HookDestination { sysvar: "authentication_ldap_simple_user_search_attr", scope: HookScope::Process, writes: "the process LDAP authentication" },
    HookDestination { sysvar: "ddl_slow_threshold", scope: HookScope::Process, writes: "the process vardef static `DDL_SLOW_OPR_THRESHOLD`" },
    HookDestination { sysvar: "max_connections", scope: HookScope::Process, writes: "the process configuration" },
    HookDestination { sysvar: "max_user_connections", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "password_history", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "password_reuse_interval", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "pd_enable_follower_handle_region", scope: HookScope::Process, writes: "the process vardef static `ENABLE_PD_FOLLOWER_HANDLE_REGION`" },
    HookDestination { sysvar: "performance_schema_session_connect_attrs_size", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "require_secure_transport", scope: HookScope::Process, writes: "the process TLS transport policy" },
    HookDestination { sysvar: "tidb_accelerate_user_creation_update", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_advancer_check_point_lag_limit", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_analyze_column_options", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_analyze_default_num_buckets", scope: HookScope::Process, writes: "the process vardef static `ANALYZE_DEFAULT_NUM_BUCKETS` (not yet per keyspace)" },
    HookDestination { sysvar: "tidb_analyze_default_num_topn", scope: HookScope::Process, writes: "the process vardef static `ANALYZE_DEFAULT_NUM_TOP_N` (not yet per keyspace)" },
    HookDestination { sysvar: "tidb_auto_analyze_concurrency", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_cb_pd_metadata_error_rate_threshold_ratio", scope: HookScope::Process, writes: "the process vardef static `CIRCUIT_BREAKER_PD_METADATA_ERROR_RATE_THRESHOLD_RATIO`" },
    HookDestination { sysvar: "tidb_check_mb4_value_in_utf8", scope: HookScope::Process, writes: "the process configuration" },
    HookDestination { sysvar: "tidb_committer_concurrency", scope: HookScope::Process, writes: "the process TiKV client" },
    HookDestination { sysvar: "tidb_ddl_disk_quota", scope: HookScope::Process, writes: "the process vardef static `DDL_DISK_QUOTA`" },
    HookDestination { sysvar: "tidb_ddl_enable_fast_reorg", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_ddl_error_count_limit", scope: HookScope::Process, writes: "the process vardef static `DDL_ERROR_COUNT_LIMIT` (not yet per keyspace)" },
    HookDestination { sysvar: "tidb_ddl_flashback_concurrency", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_ddl_reorg_batch_size", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_ddl_reorg_max_write_speed", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_ddl_reorg_worker_cnt", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_enable_auto_analyze", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_enable_auto_analyze_priority_queue", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_enable_batch_dml", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_enable_batch_query_region", scope: HookScope::Process, writes: "the process vardef static `ENABLE_BATCH_QUERY_REGION`" },
    HookDestination { sysvar: "tidb_enable_binding_usage", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_enable_check_constraint", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_enable_collect_execution_info", scope: HookScope::Process, writes: "the process configuration" },
    HookDestination { sysvar: "tidb_enable_connection_event_log", scope: HookScope::Process, writes: "the process vardef static `ENABLE_CONNECTION_EVENT_LOG`" },
    HookDestination { sysvar: "tidb_enable_ddl", scope: HookScope::Process, writes: "the process configuration" },
    HookDestination { sysvar: "tidb_enable_dist_task", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_enable_fast_create_table", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_enable_foreign_key", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_enable_gc_aware_memory_track", scope: HookScope::Process, writes: "the process memory tuner and arbitrator" },
    HookDestination { sysvar: "tidb_enable_instance_plan_cache", scope: HookScope::Process, writes: "the process vardef static `ENABLE_INSTANCE_PLAN_CACHE`" },
    HookDestination { sysvar: "tidb_enable_local_txn", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_enable_noop_variables", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_enable_resource_control", scope: HookScope::Process, writes: "the process TiKV client resource-control switch" },
    HookDestination { sysvar: "tidb_enable_row_level_checksum", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_enable_slow_log", scope: HookScope::Process, writes: "the process configuration" },
    HookDestination { sysvar: "tidb_enable_stats_owner", scope: HookScope::Process, writes: "the process configuration" },
    HookDestination { sysvar: "tidb_enable_stmt_summary", scope: HookScope::Process, writes: "the process statement summary" },
    HookDestination { sysvar: "tidb_enable_tiflash_pipeline_model", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_enable_tmp_storage_on_oom", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_enable_ts_validation", scope: HookScope::Process, writes: "the process TiKV client" },
    HookDestination { sysvar: "tidb_enable_tso_follower_proxy", scope: HookScope::Process, writes: "the process vardef static `ENABLE_TSO_FOLLOWER_PROXY`" },
    HookDestination { sysvar: "tidb_expensive_query_time_threshold", scope: HookScope::Process, writes: "the process vardef static `EXPENSIVE_QUERY_TIME_THRESHOLD`" },
    HookDestination { sysvar: "tidb_expensive_txn_time_threshold", scope: HookScope::Process, writes: "the process vardef static `EXPENSIVE_TXN_TIME_THRESHOLD`" },
    HookDestination { sysvar: "tidb_external_ts", scope: HookScope::Process, writes: "the PD external timestamp, which is cluster-scoped, not keyspace-scoped" },
    HookDestination { sysvar: "tidb_force_priority", scope: HookScope::Process, writes: "the process vardef static `FORCE_PRIORITY`" },
    HookDestination { sysvar: "tidb_general_log", scope: HookScope::Process, writes: "the process vardef static `PROCESS_GENERAL_LOG`" },
    HookDestination { sysvar: "tidb_generate_binary_plan", scope: HookScope::Process, writes: "the process log and trace" },
    HookDestination { sysvar: "tidb_gogc_tuner_max_value", scope: HookScope::Process, writes: "the process memory tuner and arbitrator" },
    HookDestination { sysvar: "tidb_gogc_tuner_min_value", scope: HookScope::Process, writes: "the process memory tuner and arbitrator" },
    HookDestination { sysvar: "tidb_gogc_tuner_threshold", scope: HookScope::Process, writes: "the process vardef static `GOGC_TUNER_THRESHOLD`" },
    HookDestination { sysvar: "tidb_ignore_inlist_plan_digest", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_instance_plan_cache_max_size", scope: HookScope::Process, writes: "the process vardef static `INSTANCE_PLAN_CACHE_MAX_MEM_SIZE`" },
    HookDestination { sysvar: "tidb_instance_plan_cache_reserved_percentage", scope: HookScope::Process, writes: "the process vardef static `INSTANCE_PLAN_CACHE_RESERVED_PERCENTAGE`" },
    HookDestination { sysvar: "tidb_log_file_max_days", scope: HookScope::Process, writes: "the process configuration" },
    HookDestination { sysvar: "tidb_low_resolution_tso_update_interval", scope: HookScope::Process, writes: "the process vardef static `LOW_RESOLUTION_TSO_UPDATE_INTERVAL`" },
    HookDestination { sysvar: "tidb_max_auto_analyze_time", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_max_delta_schema_count", scope: HookScope::Process, writes: "the process vardef static `MAX_DELTA_SCHEMA_COUNT` (not yet per keyspace)" },
    HookDestination { sysvar: "tidb_mem_arbitrator_mode", scope: HookScope::Process, writes: "the process memory tuner and arbitrator" },
    HookDestination { sysvar: "tidb_mem_arbitrator_soft_limit", scope: HookScope::Process, writes: "the process memory tuner and arbitrator" },
    HookDestination { sysvar: "tidb_mem_oom_action", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_mem_quota_analyze", scope: HookScope::Process, writes: "the process memory tuner and arbitrator" },
    HookDestination { sysvar: "tidb_mem_quota_binding_cache", scope: HookScope::Process, writes: "the process vardef static `MEM_QUOTA_BINDING_CACHE`" },
    HookDestination { sysvar: "tidb_memory_usage_alarm_keep_record_num", scope: HookScope::Process, writes: "the process vardef static `MEMORY_USAGE_ALARM_KEEP_RECORD_NUM`" },
    HookDestination { sysvar: "tidb_memory_usage_alarm_ratio", scope: HookScope::Process, writes: "the process vardef static `MEMORY_USAGE_ALARM_RATIO`" },
    HookDestination { sysvar: "tidb_mlog_log_slow_purge", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_paging_size_bytes", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_partition_prune_mode", scope: HookScope::Keyspace, writes: "none: a statement warning" },
    HookDestination { sysvar: "tidb_persist_analyze_options", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_pprof_sql_cpu", scope: HookScope::Process, writes: "the process vardef static `ENABLE_P_PROF_SQLCPU`" },
    HookDestination { sysvar: "tidb_prepared_plan_cache_memory_guard_ratio", scope: HookScope::Process, writes: "the process vardef static `PREPARED_PLAN_CACHE_MEMORY_GUARD_RATIO`" },
    HookDestination { sysvar: "tidb_query_log_max_len", scope: HookScope::Process, writes: "the process vardef static `QUERY_LOG_MAX_LEN`" },
    HookDestination { sysvar: "tidb_rc_read_check_ts", scope: HookScope::Process, writes: "the process vardef static `ENABLE_RC_READ_CHECK_TS`" },
    HookDestination { sysvar: "tidb_record_plan_in_slow_log", scope: HookScope::Process, writes: "the process configuration" },
    HookDestination { sysvar: "tidb_redact_log", scope: HookScope::Process, writes: "the process log and trace" },
    HookDestination { sysvar: "tidb_resource_control_strict_mode", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_schema_cache_size", scope: HookScope::Process, writes: "the process vardef static `SCHEMA_CACHE_SIZE_ORIGIN_TEXT`" },
    HookDestination { sysvar: "tidb_server_memory_limit", scope: HookScope::Process, writes: "the process memory tuner and arbitrator" },
    HookDestination { sysvar: "tidb_server_memory_limit_gc_trigger", scope: HookScope::Process, writes: "the process memory tuner and arbitrator" },
    HookDestination { sysvar: "tidb_server_memory_limit_sess_min_size", scope: HookScope::Process, writes: "the process memory tuner and arbitrator" },
    HookDestination { sysvar: "tidb_service_scope", scope: HookScope::Process, writes: "the process vardef static `SERVICE_SCOPE`" },
    HookDestination { sysvar: "tidb_slow_log_max_per_sec", scope: HookScope::Process, writes: "the process log and trace" },
    HookDestination { sysvar: "tidb_slow_log_rules", scope: HookScope::Process, writes: "the process log and trace" },
    HookDestination { sysvar: "tidb_slow_log_threshold", scope: HookScope::Process, writes: "the process configuration" },
    HookDestination { sysvar: "tidb_stats_cache_mem_quota", scope: HookScope::Process, writes: "the process vardef static `STATS_CACHE_MEM_QUOTA`" },
    HookDestination { sysvar: "tidb_stats_load_pseudo_timeout", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_stats_load_sync_wait", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_stmt_summary_group_by_user", scope: HookScope::Process, writes: "the process statement summary" },
    HookDestination { sysvar: "tidb_stmt_summary_history_size", scope: HookScope::Process, writes: "the process statement summary" },
    HookDestination { sysvar: "tidb_stmt_summary_internal_query", scope: HookScope::Process, writes: "the process statement summary" },
    HookDestination { sysvar: "tidb_stmt_summary_max_sql_length", scope: HookScope::Process, writes: "the process statement summary" },
    HookDestination { sysvar: "tidb_stmt_summary_max_stmt_count", scope: HookScope::Process, writes: "the process statement summary" },
    HookDestination { sysvar: "tidb_stmt_summary_persist_evicted", scope: HookScope::Process, writes: "the process statement summary" },
    HookDestination { sysvar: "tidb_stmt_summary_refresh_interval", scope: HookScope::Process, writes: "the process statement summary" },
    HookDestination { sysvar: "tidb_store_limit", scope: HookScope::Process, writes: "the process TiKV client" },
    HookDestination { sysvar: "tidb_table_cache_lease", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_top_sql_max_meta_count", scope: HookScope::Process, writes: "the process Top SQL collector" },
    HookDestination { sysvar: "tidb_top_sql_max_time_series_count", scope: HookScope::Process, writes: "the process Top SQL collector" },
    HookDestination { sysvar: "tidb_trace_event", scope: HookScope::Process, writes: "the process log and trace" },
    HookDestination { sysvar: "tidb_tso_client_batch_max_wait_time", scope: HookScope::Process, writes: "the process vardef static `MAX_TSO_BATCH_WAIT_INTERVAL`" },
    HookDestination { sysvar: "tidb_tso_client_rpc_mode", scope: HookScope::Process, writes: "the process PD client" },
    HookDestination { sysvar: "tidb_ttl_delete_batch_size", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_ttl_delete_rate_limit", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_ttl_delete_worker_count", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_ttl_job_enable", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_ttl_job_schedule_window_end_time", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_ttl_job_schedule_window_start_time", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_ttl_running_tasks", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_ttl_scan_batch_size", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_ttl_scan_worker_count", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "tidb_txn_commit_batch_size", scope: HookScope::Process, writes: "the process TiKV client" },
    HookDestination { sysvar: "tidb_txn_entry_size_limit", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "validate_password.length", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "validate_password.mixed_case_count", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "validate_password.number_count", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
    HookDestination { sysvar: "validate_password.special_char_count", scope: HookScope::Keyspace, writes: "the keyspace vardef snapshot" },
];

/// Whether `name` sets process-wide state: its `SetGlobal` hook writes the
/// process's, it is the variable of a vardef static not kept per keyspace,
/// or Go scopes it to the instance alone.
pub fn is_process_variable(name: &str) -> bool {
    if HOOK_DESTINATIONS
        .iter()
        .any(|hook| hook.scope == HookScope::Process && hook.sysvar.eq_ignore_ascii_case(name))
    {
        return true;
    }
    if PROCESS_STATICS.iter().any(|entry| {
        entry.sysvar.is_some_and(|sysvar| sysvar.eq_ignore_ascii_case(name))
            && entry.scope != StaticScope::Internal
            && !SNAPSHOT_STATICS.contains(&entry.name)
    }) {
        return true;
    }
    get_sys_var(name).is_some_and(|def| def.scope & SCOPE_INSTANCE != 0 && def.scope & (SCOPE_GLOBAL | SCOPE_SESSION) == 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every registry variable with a `SetGlobal` hook is classified, once:
    /// a new hook cannot go unclassified.
    #[test]
    fn every_set_global_hook_is_classified() {
        let mut hooked: Vec<&str> = crate::sysvar::sys_vars()
            .into_iter()
            .filter(|def| def.set_global.is_some())
            .map(|def| def.name)
            .collect();
        hooked.sort_unstable();
        let mut classified: Vec<&str> = HOOK_DESTINATIONS.iter().map(|hook| hook.sysvar).collect();
        classified.sort_unstable();
        assert_eq!(hooked, classified);
    }

    /// A keyspace hook writes the snapshot: its variable is a kept static's
    /// (or writes nothing at all); every kept static's variable is a
    /// keyspace hook.
    #[test]
    fn keyspace_hooks_are_the_snapshot_ones() {
        let kept: Vec<&str> = PROCESS_STATICS
            .iter()
            .filter(|entry| SNAPSHOT_STATICS.contains(&entry.name))
            .filter_map(|entry| entry.sysvar)
            .collect();
        for hook in HOOK_DESTINATIONS {
            let is_kept = kept.contains(&hook.sysvar);
            match hook.scope {
                HookScope::Keyspace => assert!(
                    is_kept || hook.writes.starts_with("none"),
                    "{hook:?}"
                ),
                HookScope::Process => assert!(!is_kept, "{hook:?}"),
            }
        }
        for sysvar in kept {
            assert!(!is_process_variable(sysvar), "{sysvar}");
        }
    }

    #[test]
    fn process_variables_are_the_process_ones() {
        // An instance static of tidb-vardef behind a GLOBAL variable.
        assert!(is_process_variable("tidb_enable_instance_plan_cache"));
        assert!(is_process_variable("TIDB_SCHEMA_CACHE_SIZE"));
        // Another crate's process state.
        assert!(is_process_variable("tidb_mem_quota_analyze"));
        assert!(is_process_variable("tidb_top_sql_max_time_series_count"));
        assert!(is_process_variable("tidb_enable_resource_control"));
        // The PD cluster's state: PD keeps one external timestamp per
        // cluster, not per keyspace.
        assert!(is_process_variable("tidb_external_ts"));
        // A vardef global static not yet kept per keyspace.
        assert!(is_process_variable("tidb_ddl_error_count_limit"));
        assert!(is_process_variable("tidb_cloud_storage_uri"));
        // Go `ScopeInstance` alone.
        assert!(is_process_variable("tidb_general_log"));
        // A keyspace's own policy.
        assert!(!is_process_variable("tidb_enable_foreign_key"));
        assert!(!is_process_variable("tidb_ttl_job_enable"));
        assert!(!is_process_variable("tidb_partition_prune_mode"));
        // A session variable.
        assert!(!is_process_variable("sql_mode"));
    }
}
