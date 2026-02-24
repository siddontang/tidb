// Copyright 2025 PingCAP, Inc.
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

package vardef

/*
	Steps to add a new TiDB specific system variable:

	1. Add a new variable name with comment in this file.
	2. Add the default value of the new variable in this file.
	3. Add SysVar instance in 'defaultSysVars' slice.
*/

// TiDB system variable names that only in session scope.
const (
	TiDBDDLSlowOprThreshold = "ddl_slow_threshold"

	// TiDBSnapshot is used for reading history data, the default value is empty string.
	// The value can be a datetime string like '2017-11-11 20:20:20' or a tso string. When this variable is set, the session reads history data of that time.
	TiDBSnapshot = "tidb_snapshot"

	// TiDBOptAggPushDown is used to enable/disable the optimizer rule of aggregation push down.
	TiDBOptAggPushDown = "tidb_opt_agg_push_down"

	// TiDBOptDeriveTopN is used to enable/disable the optimizer rule of deriving topN.
	TiDBOptDeriveTopN = "tidb_opt_derive_topn"

	// TiDBOptCartesianBCJ is used to disable/enable broadcast cartesian join in MPP mode
	TiDBOptCartesianBCJ = "tidb_opt_broadcast_cartesian_join"

	TiDBOptMPPOuterJoinFixedBuildSide = "tidb_opt_mpp_outer_join_fixed_build_side"

	// TiDBOptDistinctAggPushDown is used to decide whether agg with distinct should be pushed to tikv/tiflash.
	TiDBOptDistinctAggPushDown = "tidb_opt_distinct_agg_push_down"

	// TiDBOptSkewDistinctAgg is used to indicate the distinct agg has data skew
	TiDBOptSkewDistinctAgg = "tidb_opt_skew_distinct_agg"

	// TiDBOpt3StageDistinctAgg is used to indicate whether to plan and execute the distinct agg in 3 stages
	TiDBOpt3StageDistinctAgg = "tidb_opt_three_stage_distinct_agg"

	// TiDBOptEnable3StageMultiDistinctAgg is used to indicate whether to plan and execute the multi distinct agg in 3 stages
	TiDBOptEnable3StageMultiDistinctAgg = "tidb_opt_enable_three_stage_multi_distinct_agg"

	TiDBOptExplainNoEvaledSubQuery = "tidb_opt_enable_non_eval_scalar_subquery"

	// TiDBBCJThresholdSize is used to limit the size of small table for mpp broadcast join.
	// Its unit is bytes, if the size of small table is larger than it, we will not use bcj.
	TiDBBCJThresholdSize = "tidb_broadcast_join_threshold_size"

	// TiDBBCJThresholdCount is used to limit the count of small table for mpp broadcast join.
	// If we can't estimate the size of one side of join child, we will check if its row number exceeds this limitation.
	TiDBBCJThresholdCount = "tidb_broadcast_join_threshold_count"

	// TiDBPreferBCJByExchangeDataSize indicates the method used to choose mpp broadcast join
	TiDBPreferBCJByExchangeDataSize = "tidb_prefer_broadcast_join_by_exchange_data_size"

	// TiDBOptWriteRowID is used to enable/disable the operations of insert、replace and update to _tidb_rowid.
	TiDBOptWriteRowID = "tidb_opt_write_row_id"

	// TiDBAutoAnalyzeRatio will run if (table modify count)/(table row count) is greater than this value.
	TiDBAutoAnalyzeRatio = "tidb_auto_analyze_ratio"

	// TiDBAutoAnalyzeStartTime will run if current time is within start time and end time.
	TiDBAutoAnalyzeStartTime = "tidb_auto_analyze_start_time"
	TiDBAutoAnalyzeEndTime   = "tidb_auto_analyze_end_time"

	// TiDBChecksumTableConcurrency is used to speed up the ADMIN CHECKSUM TABLE
	// statement, when a table has multiple indices, those indices can be
	// scanned concurrently, with the cost of higher system performance impact.
	TiDBChecksumTableConcurrency = "tidb_checksum_table_concurrency"

	// TiDBCurrentTS is used to get the current transaction timestamp.
	// It is read-only.
	TiDBCurrentTS = "tidb_current_ts"

	// TiDBLastTxnInfo is used to get the last transaction info within the current session.
	TiDBLastTxnInfo = "tidb_last_txn_info"

	// TiDBLastQueryInfo is used to get the last query info within the current session.
	TiDBLastQueryInfo = "tidb_last_query_info"

	// TiDBLastDDLInfo is used to get the last ddl info within the current session.
	TiDBLastDDLInfo = "tidb_last_ddl_info"

	// TiDBLastPlanReplayerToken is used to get the last plan replayer token within the current session
	TiDBLastPlanReplayerToken = "tidb_last_plan_replayer_token"

	// TiDBConfig is a read-only variable that shows the config of the current server.
	TiDBConfig = "tidb_config"

	// TiDBBatchInsert is used to enable/disable auto-split insert data. If set this option on, insert executor will automatically
	// insert data into multiple batches and use a single txn for each batch. This will be helpful when inserting large data.
	TiDBBatchInsert = "tidb_batch_insert"

	// TiDBBatchDelete is used to enable/disable auto-split delete data. If set this option on, delete executor will automatically
	// split data into multiple batches and use a single txn for each batch. This will be helpful when deleting large data.
	TiDBBatchDelete = "tidb_batch_delete"

	// TiDBBatchCommit is used to enable/disable auto-split the transaction.
	// If set this option on, the transaction will be committed when it reaches stmt-count-limit and starts a new transaction.
	TiDBBatchCommit = "tidb_batch_commit"

	// TiDBDMLBatchSize is used to split the insert/delete data into small batches.
	// It only takes effort when tidb_batch_insert/tidb_batch_delete is on.
	// Its default value is 20000. When the row size is large, 20k rows could be larger than 100MB.
	// User could change it to a smaller one to avoid breaking the transaction size limitation.
	TiDBDMLBatchSize = "tidb_dml_batch_size"

	// The following session variables controls the memory quota during query execution.

	// TiDBMemQuotaQuery controls the memory quota of a query.
	TiDBMemQuotaQuery = "tidb_mem_quota_query" // Bytes.
	// TiDBMemQuotaApplyCache controls the memory quota of a query.
	TiDBMemQuotaApplyCache = "tidb_mem_quota_apply_cache"

	// TiDBGeneralLog is used to log every query in the server in info level.
	TiDBGeneralLog = "tidb_general_log"

	// TiDBTraceEvent controls the experimental trace event instrumentation.
	TiDBTraceEvent = "tidb_trace_event"

	// TiDBLogFileMaxDays is used to log every query in the server in info level.
	TiDBLogFileMaxDays = "tidb_log_file_max_days"

	// TiDBPProfSQLCPU is used to add label sql label to pprof result.
	TiDBPProfSQLCPU = "tidb_pprof_sql_cpu"

	// TiDBRetryLimit is the maximum number of retries when committing a transaction.
	TiDBRetryLimit = "tidb_retry_limit"

	// TiDBDisableTxnAutoRetry disables transaction auto retry.
	// Deprecated: This variable is deprecated, please do not use this variable.
	TiDBDisableTxnAutoRetry = "tidb_disable_txn_auto_retry"

	// TiDBEnableChunkRPC enables TiDB to use Chunk format for coprocessor requests.
	TiDBEnableChunkRPC = "tidb_enable_chunk_rpc"

	// TiDBOptimizerSelectivityLevel is used to control the selectivity estimation level.
	TiDBOptimizerSelectivityLevel = "tidb_optimizer_selectivity_level"

	// TiDBOptIndexPruneThreshold is used to control the threshold for index pruning optimization.
	TiDBOptIndexPruneThreshold = "tidb_opt_index_prune_threshold"

	// TiDBOptimizerEnableNewOnlyFullGroupByCheck is used to open the newly only_full_group_by check by maintaining functional dependency.
	TiDBOptimizerEnableNewOnlyFullGroupByCheck = "tidb_enable_new_only_full_group_by_check"

	TiDBOptimizerEnableOuterJoinReorder = "tidb_enable_outer_join_reorder"

	// TiDBOptimizerEnableNAAJ is used to open the newly null-aware anti join
	TiDBOptimizerEnableNAAJ = "tidb_enable_null_aware_anti_join"

	// TiDBTxnMode is used to control the transaction behavior.
	TiDBTxnMode = "tidb_txn_mode"

	// TiDBRowFormatVersion is used to control tidb row format version current.
	TiDBRowFormatVersion = "tidb_row_format_version"

	// TiDBEnableRowLevelChecksum is used to control whether to append checksum to row values.
	TiDBEnableRowLevelChecksum = "tidb_enable_row_level_checksum"

	// TiDBEnableTablePartition is used to control table partition feature.
	// The valid value include auto/on/off:
	// on or auto: enable table partition if the partition type is implemented.
	// off: always disable table partition.
	TiDBEnableTablePartition = "tidb_enable_table_partition"

	// TiDBEnableListTablePartition is used to control list table partition feature.
	// Deprecated: This variable is deprecated, please do not use this variable.
	TiDBEnableListTablePartition = "tidb_enable_list_partition"

	// TiDBSkipIsolationLevelCheck is used to control whether to return error when set unsupported transaction
	// isolation level.
	TiDBSkipIsolationLevelCheck = "tidb_skip_isolation_level_check"

	// TiDBLowResolutionTSO is used for reading data with low resolution TSO which is updated once every two seconds
	TiDBLowResolutionTSO = "tidb_low_resolution_tso"

	// TiDBReplicaRead is used for reading data from replicas, followers for example.
	TiDBReplicaRead = "tidb_replica_read"

	// TiDBAdaptiveClosestReadThreshold is for reading data from closest replicas(with same 'zone' label).
	// TiKV client should send read request to the closest replica(leader/follower) if the estimated response
	// size exceeds this threshold; otherwise, this request should be sent to leader.
	// This variable only take effect when `tidb_replica_read` is 'closest-adaptive'.
	TiDBAdaptiveClosestReadThreshold = "tidb_adaptive_closest_read_threshold"

	// TiDBAllowRemoveAutoInc indicates whether a user can drop the auto_increment column attribute or not.
	TiDBAllowRemoveAutoInc = "tidb_allow_remove_auto_inc"

	// TiDBMultiStatementMode enables multi statement at the risk of SQL injection
	// provides backwards compatibility
	TiDBMultiStatementMode = "tidb_multi_statement_mode"

	// TiDBEvolvePlanTaskMaxTime controls the max time of a single evolution task.
	TiDBEvolvePlanTaskMaxTime = "tidb_evolve_plan_task_max_time"

	// TiDBEvolvePlanTaskStartTime is the start time of evolution task.
	TiDBEvolvePlanTaskStartTime = "tidb_evolve_plan_task_start_time"
	// TiDBEvolvePlanTaskEndTime is the end time of evolution task.
	TiDBEvolvePlanTaskEndTime = "tidb_evolve_plan_task_end_time"

	// TiDBSlowLogThreshold is used to set the slow log threshold in the server.
	TiDBSlowLogThreshold = "tidb_slow_log_threshold"

	// TiDBSlowLogRules defines multi-dimensional trigger rules for flexible slow log control.
	TiDBSlowLogRules = "tidb_slow_log_rules"

	// TiDBSlowLogMaxPerSec is the maximum number of slow logs that can be recorded per second in the server.
	// The default value is 0, which means no rate limiting is applied.
	TiDBSlowLogMaxPerSec = "tidb_slow_log_max_per_sec"

	// TiDBSlowTxnLogThreshold is used to set the slow transaction log threshold in the server.
	TiDBSlowTxnLogThreshold = "tidb_slow_txn_log_threshold"

	// TiDBRecordPlanInSlowLog is used to log the plan of the slow query.
	TiDBRecordPlanInSlowLog = "tidb_record_plan_in_slow_log"

	// TiDBEnableSlowLog enables TiDB to log slow queries.
	TiDBEnableSlowLog = "tidb_enable_slow_log"

	// TiDBCheckMb4ValueInUTF8 is used to control whether to enable the check wrong utf8 value.
	TiDBCheckMb4ValueInUTF8 = "tidb_check_mb4_value_in_utf8"

	// TiDBFoundInPlanCache indicates whether the last statement was found in plan cache
	TiDBFoundInPlanCache = "last_plan_from_cache"

	// TiDBFoundInBinding indicates whether the last statement was matched with the hints in the binding.
	TiDBFoundInBinding = "last_plan_from_binding"

	// TiDBAllowAutoRandExplicitInsert indicates whether explicit insertion on auto_random column is allowed.
	TiDBAllowAutoRandExplicitInsert = "allow_auto_random_explicit_insert"

	// TiDBTxnReadTS indicates the next transaction should be staleness transaction and provide the startTS
	TiDBTxnReadTS = "tx_read_ts"

	// TiDBReadStaleness indicates the staleness duration for following statement
	TiDBReadStaleness = "tidb_read_staleness"

	// TiDBEnablePaging indicates whether paging is enabled in coprocessor requests.
	TiDBEnablePaging = "tidb_enable_paging"

	// TiDBReadConsistency indicates whether the autocommit read statement goes through TiKV RC.
	TiDBReadConsistency = "tidb_read_consistency"

	// TiDBSysdateIsNow is the name of the `tidb_sysdate_is_now` system variable
	TiDBSysdateIsNow = "tidb_sysdate_is_now"

	// RequireSecureTransport indicates the secure mode for data transport
	RequireSecureTransport = "require_secure_transport"

	// TiFlashFastScan indicates whether use fast scan in tiflash.
	TiFlashFastScan = "tiflash_fastscan"

	// TiDBEnableUnsafeSubstitute indicates whether to enable generate column takes unsafe substitute.
	TiDBEnableUnsafeSubstitute = "tidb_enable_unsafe_substitute"

	// TiDBEnableTiFlashReadForWriteStmt indicates whether to enable TiFlash to read for write statements.
	TiDBEnableTiFlashReadForWriteStmt = "tidb_enable_tiflash_read_for_write_stmt"

	// TiDBUseAlloc indicates whether the last statement used chunk alloc
	TiDBUseAlloc = "last_sql_use_alloc"

	// TiDBExplicitRequestSourceType indicates the source of the request, it's a complement of RequestSourceType.
	// The value maybe "lightning", "br", "dumpling" etc.
	TiDBExplicitRequestSourceType = "tidb_request_source_type"
)

// TiDB vars that have only global scope
const (
	// TiDBGCEnable turns garbage collection on or OFF
	TiDBGCEnable = "tidb_gc_enable"
	// TiDBGCRunInterval sets the interval that GC runs
	TiDBGCRunInterval = "tidb_gc_run_interval"
	// TiDBGCLifetime sets the retention window of older versions
	TiDBGCLifetime = "tidb_gc_life_time"
	// TiDBGCConcurrency sets the concurrency of garbage collection. -1 = AUTO value
	TiDBGCConcurrency = "tidb_gc_concurrency"
	// TiDBGCScanLockMode enables the green GC feature (deprecated)
	TiDBGCScanLockMode = "tidb_gc_scan_lock_mode"
	// TiDBGCMaxWaitTime sets max time for gc advances the safepoint delayed by active transactions
	TiDBGCMaxWaitTime = "tidb_gc_max_wait_time"
	// TiDBEnableEnhancedSecurity restricts SUPER users from certain operations.
	TiDBEnableEnhancedSecurity = "tidb_enable_enhanced_security"
	// TiDBEnableHistoricalStats enables the historical statistics feature (default off)
	TiDBEnableHistoricalStats = "tidb_enable_historical_stats"
	// TiDBPersistAnalyzeOptions persists analyze options for later analyze and auto-analyze
	TiDBPersistAnalyzeOptions = "tidb_persist_analyze_options"
	// TiDBEnableColumnTracking enables collecting predicate columns.
	// DEPRECATED: This variable is deprecated, please do not use this variable.
	TiDBEnableColumnTracking = "tidb_enable_column_tracking"
	// TiDBAnalyzeColumnOptions specifies the default column selection strategy for both manual and automatic analyze operations.
	// It accepts two values:
	// `PREDICATE`: Analyze only the columns that are used in the predicates of the query.
	// `ALL`: Analyze all columns in the table.
	TiDBAnalyzeColumnOptions = "tidb_analyze_column_options"
	// TiDBDisableColumnTrackingTime records the last time TiDBEnableColumnTracking is set off.
	// It is used to invalidate the collected predicate columns after turning off TiDBEnableColumnTracking, which avoids physical deletion.
	// It doesn't have cache in memory, and we directly get/set the variable value from/to mysql.tidb.
	// DEPRECATED: This variable is deprecated, please do not use this variable.
	TiDBDisableColumnTrackingTime = "tidb_disable_column_tracking_time"
	// TiDBStatsLoadPseudoTimeout indicates whether to fallback to pseudo stats after load timeout.
	TiDBStatsLoadPseudoTimeout = "tidb_stats_load_pseudo_timeout"
	// TiDBMemQuotaBindingCache indicates the memory quota for the bind cache.
	TiDBMemQuotaBindingCache = "tidb_mem_quota_binding_cache"
	// TiDBRCReadCheckTS indicates the tso optimization for read-consistency read is enabled.
	TiDBRCReadCheckTS = "tidb_rc_read_check_ts"
	// TiDBRCWriteCheckTs indicates whether some special write statements don't get latest tso from PD at RC
	TiDBRCWriteCheckTs = "tidb_rc_write_check_ts"
	// TiDBCommitterConcurrency controls the number of running concurrent requests in the commit phase.
	TiDBCommitterConcurrency = "tidb_committer_concurrency"
	// TiDBPipelinedDmlResourcePolicy controls the number of running concurrent requests in the
	// pipelined flush action.
	TiDBPipelinedDmlResourcePolicy = "tidb_pipelined_dml_resource_policy"
	// TiDBEnableBatchDML enables batch dml.
	TiDBEnableBatchDML = "tidb_enable_batch_dml"
	// TiDBStatsCacheMemQuota records stats cache quota.
	TiDBStatsCacheMemQuota = "tidb_stats_cache_mem_quota"
	// TiDBMemQuotaAnalyze indicates the memory quota for all analyze jobs.
	TiDBMemQuotaAnalyze = "tidb_mem_quota_analyze"
	// TiDBEnableAutoAnalyze determines whether TiDB executes automatic analysis.
	// In test, we disable it by default. See GlobalSystemVariableInitialValue for details.
	TiDBEnableAutoAnalyze = "tidb_enable_auto_analyze"
	// TiDBEnableAutoAnalyzePriorityQueue determines whether TiDB executes automatic analysis with priority queue.
	// DEPRECATED: This variable is deprecated, please do not use this variable.
	TiDBEnableAutoAnalyzePriorityQueue = "tidb_enable_auto_analyze_priority_queue"
	// TiDBMemOOMAction indicates what operation TiDB perform when a single SQL statement exceeds
	// the memory quota specified by tidb_mem_quota_query and cannot be spilled to disk.
	TiDBMemOOMAction = "tidb_mem_oom_action"
	// TiDBPrepPlanCacheMemoryGuardRatio is used to prevent [performance.max-memory] from being exceeded
	TiDBPrepPlanCacheMemoryGuardRatio = "tidb_prepared_plan_cache_memory_guard_ratio"
	// TiDBMaxAutoAnalyzeTime is the max time that auto analyze can run. If auto analyze runs longer than the value, it
	// will be killed. 0 indicates that there is no time limit.
	TiDBMaxAutoAnalyzeTime = "tidb_max_auto_analyze_time"
	// TiDBAutoAnalyzeConcurrency is the concurrency of the auto analyze
	TiDBAutoAnalyzeConcurrency = "tidb_auto_analyze_concurrency"
	// TiDBEnableDistTask indicates whether to enable the distributed execute background tasks(For example DDL, Import etc).
	TiDBEnableDistTask = "tidb_enable_dist_task"
	// TiDBMaxDistTaskNodes indicates the max node count that could be used by distributed execution framework.
	TiDBMaxDistTaskNodes = "tidb_max_dist_task_nodes"
	// TiDBEnableFastCreateTable indicates whether to enable the fast create table feature.
	TiDBEnableFastCreateTable = "tidb_enable_fast_create_table"
	// TiDBGenerateBinaryPlan indicates whether binary plan should be generated in slow log and statements summary.
	TiDBGenerateBinaryPlan = "tidb_generate_binary_plan"
	// TiDBEnableDDLAnalyze indicates whether ddl(create/reorg index) is with embedded index analyze.
	TiDBEnableDDLAnalyze = "tidb_stats_update_during_ddl"
	// TiDBEnableGCAwareMemoryTrack indicates whether to turn-on GC-aware memory track.
	TiDBEnableGCAwareMemoryTrack = "tidb_enable_gc_aware_memory_track"
	// TiDBEnableTmpStorageOnOOM controls whether to enable the temporary storage for some operators
	// when a single SQL statement exceeds the memory quota specified by the memory quota.
	TiDBEnableTmpStorageOnOOM = "tidb_enable_tmp_storage_on_oom"
	// TiDBDDLEnableFastReorg indicates whether to use lighting backfill process for adding index.
	TiDBDDLEnableFastReorg = "tidb_ddl_enable_fast_reorg"
	// TiDBDDLDiskQuota used to set disk quota for lightning add index.
	TiDBDDLDiskQuota = "tidb_ddl_disk_quota"
	// TiDBCloudStorageURI used to set a cloud storage uri for ddl add index and import into.
	TiDBCloudStorageURI = "tidb_cloud_storage_uri"
	// TiDBAutoBuildStatsConcurrency is the number of concurrent workers to automatically analyze tables or partitions.
	// It is very similar to the `tidb_build_stats_concurrency` variable, but it is used for the auto analyze feature.
	TiDBAutoBuildStatsConcurrency = "tidb_auto_build_stats_concurrency"
	// TiDBSysProcScanConcurrency is used to set the scan concurrency of for backend system processes, like auto-analyze.
	// For now, it controls the number of concurrent workers to scan regions to collect statistics (FMSketch, Samples).
	TiDBSysProcScanConcurrency = "tidb_sysproc_scan_concurrency"
	// TiDBServerMemoryLimit indicates the memory limit of the tidb-server instance.
	TiDBServerMemoryLimit = "tidb_server_memory_limit"
	// TiDBServerMemoryLimitSessMinSize indicates the minimal memory used of a session, that becomes a candidate for session kill.
	TiDBServerMemoryLimitSessMinSize = "tidb_server_memory_limit_sess_min_size"
	// TiDBServerMemoryLimitGCTrigger indicates the gc percentage of the TiDBServerMemoryLimit.
	TiDBServerMemoryLimitGCTrigger = "tidb_server_memory_limit_gc_trigger"
	// TiDBMemArbitratorSoftLimit indicates the soft memory quota limit of the global memory arbitrator
	TiDBMemArbitratorSoftLimit = "tidb_mem_arbitrator_soft_limit"
	// TiDBMemArbitratorMode indicates work modes of the global memory arbitrator
	TiDBMemArbitratorMode = "tidb_mem_arbitrator_mode"
	// TiDBMemArbitratorQueryReserved indicates the memory quota query needs to subscribe from the global memory arbitrator before execution
	TiDBMemArbitratorQueryReserved = "tidb_mem_arbitrator_query_reserved"
	// TiDBMemArbitratorWaitAverse indicates whether the query is wait averse
	TiDBMemArbitratorWaitAverse = "tidb_mem_arbitrator_wait_averse"
	// TiDBEnableGOGCTuner is to enable GOGC tuner. it can tuner GOGC
	TiDBEnableGOGCTuner = "tidb_enable_gogc_tuner"
	// TiDBGOGCTunerThreshold is to control the threshold of GOGC tuner.
	TiDBGOGCTunerThreshold = "tidb_gogc_tuner_threshold"
	// TiDBGOGCTunerMaxValue is the max value of GOGC that GOGC tuner can change to.
	TiDBGOGCTunerMaxValue = "tidb_gogc_tuner_max_value"
	// TiDBGOGCTunerMinValue is the min value of GOGC that GOGC tuner can change to.
	TiDBGOGCTunerMinValue = "tidb_gogc_tuner_min_value"
	// TiDBExternalTS is the ts to read through when the `TiDBEnableExternalTsRead` is on
	TiDBExternalTS = "tidb_external_ts"
	// TiDBTTLJobEnable is used to enable/disable scheduling ttl job
	TiDBTTLJobEnable = "tidb_ttl_job_enable"
	// TiDBTTLScanBatchSize is used to control the batch size in the SELECT statement for TTL jobs
	TiDBTTLScanBatchSize = "tidb_ttl_scan_batch_size"
	// TiDBTTLDeleteBatchSize is used to control the batch size in the DELETE statement for TTL jobs
	TiDBTTLDeleteBatchSize = "tidb_ttl_delete_batch_size"
	// TiDBTTLDeleteRateLimit is used to control the delete rate limit for TTL jobs in each node
	TiDBTTLDeleteRateLimit = "tidb_ttl_delete_rate_limit"
	// TiDBTTLJobScheduleWindowStartTime is used to restrict the start time of the time window of scheduling the ttl jobs.
	TiDBTTLJobScheduleWindowStartTime = "tidb_ttl_job_schedule_window_start_time"
	// TiDBTTLJobScheduleWindowEndTime is used to restrict the end time of the time window of scheduling the ttl jobs.
	TiDBTTLJobScheduleWindowEndTime = "tidb_ttl_job_schedule_window_end_time"
	// TiDBTTLScanWorkerCount indicates the count of the scan workers in each TiDB node
	TiDBTTLScanWorkerCount = "tidb_ttl_scan_worker_count"
	// TiDBTTLDeleteWorkerCount indicates the count of the delete workers in each TiDB node
	TiDBTTLDeleteWorkerCount = "tidb_ttl_delete_worker_count"
	// PasswordReuseHistory limit a few passwords to reuse.
	PasswordReuseHistory = "password_history"
	// PasswordReuseTime limit how long passwords can be reused.
	PasswordReuseTime = "password_reuse_interval"
	// TiDBHistoricalStatsDuration indicates the duration to remain tidb historical stats
	TiDBHistoricalStatsDuration = "tidb_historical_stats_duration"
	// TiDBEnableHistoricalStatsForCapture indicates whether use historical stats in plan replayer capture
	TiDBEnableHistoricalStatsForCapture = "tidb_enable_historical_stats_for_capture"
	// TiDBEnableResourceControl indicates whether resource control feature is enabled
	TiDBEnableResourceControl = "tidb_enable_resource_control"
	// TiDBResourceControlStrictMode indicates whether resource control strict mode is enabled.
	// When strict mode is enabled, user need certain privilege to change session or statement resource group.
	TiDBResourceControlStrictMode = "tidb_resource_control_strict_mode"
	// TiDBStmtSummaryEnablePersistent indicates whether to enable file persistence for stmtsummary.
	TiDBStmtSummaryEnablePersistent = "tidb_stmt_summary_enable_persistent"
	// TiDBStmtSummaryFilename indicates the file name written by stmtsummary.
	TiDBStmtSummaryFilename = "tidb_stmt_summary_filename"
	// TiDBStmtSummaryFileMaxDays indicates how many days the files written by stmtsummary will be kept.
	TiDBStmtSummaryFileMaxDays = "tidb_stmt_summary_file_max_days"
	// TiDBStmtSummaryFileMaxSize indicates the maximum size (in mb) of a single file written by stmtsummary.
	TiDBStmtSummaryFileMaxSize = "tidb_stmt_summary_file_max_size"
	// TiDBStmtSummaryFileMaxBackups indicates the maximum number of files written by stmtsummary.
	TiDBStmtSummaryFileMaxBackups = "tidb_stmt_summary_file_max_backups"
	// TiDBTTLRunningTasks limits the count of running ttl tasks. Default to 0, means 3 times the count of TiKV (or no
	// limitation, if the storage is not TiKV).
	TiDBTTLRunningTasks = "tidb_ttl_running_tasks"
	// AuthenticationLDAPSASLAuthMethodName defines the authentication method used by LDAP SASL authentication plugin
	AuthenticationLDAPSASLAuthMethodName = "authentication_ldap_sasl_auth_method_name"
	// AuthenticationLDAPSASLCAPath defines the ca certificate to verify LDAP connection in LDAP SASL authentication plugin
	AuthenticationLDAPSASLCAPath = "authentication_ldap_sasl_ca_path"
	// AuthenticationLDAPSASLTLS defines whether to use TLS connection in LDAP SASL authentication plugin
	AuthenticationLDAPSASLTLS = "authentication_ldap_sasl_tls"
	// AuthenticationLDAPSASLServerHost defines the server host of LDAP server for LDAP SASL authentication plugin
	AuthenticationLDAPSASLServerHost = "authentication_ldap_sasl_server_host"
	// AuthenticationLDAPSASLServerPort defines the port of LDAP server for LDAP SASL authentication plugin
	AuthenticationLDAPSASLServerPort = "authentication_ldap_sasl_server_port"
	// AuthenticationLDAPSASLReferral defines whether to enable LDAP referral for LDAP SASL authentication plugin
	AuthenticationLDAPSASLReferral = "authentication_ldap_sasl_referral"
	// AuthenticationLDAPSASLUserSearchAttr defines the attribute of username in LDAP server
	AuthenticationLDAPSASLUserSearchAttr = "authentication_ldap_sasl_user_search_attr"
	// AuthenticationLDAPSASLBindBaseDN defines the `dn` to search the users in. It's used to limit the search scope of TiDB.
	AuthenticationLDAPSASLBindBaseDN = "authentication_ldap_sasl_bind_base_dn"
	// AuthenticationLDAPSASLBindRootDN defines the `dn` of the user to login the LDAP server and perform search.
	AuthenticationLDAPSASLBindRootDN = "authentication_ldap_sasl_bind_root_dn"
	// AuthenticationLDAPSASLBindRootPWD defines the password of the user to login the LDAP server and perform search.
	AuthenticationLDAPSASLBindRootPWD = "authentication_ldap_sasl_bind_root_pwd"
	// AuthenticationLDAPSASLInitPoolSize defines the init size of connection pool to LDAP server for SASL plugin.
	AuthenticationLDAPSASLInitPoolSize = "authentication_ldap_sasl_init_pool_size"
	// AuthenticationLDAPSASLMaxPoolSize defines the max size of connection pool to LDAP server for SASL plugin.
	AuthenticationLDAPSASLMaxPoolSize = "authentication_ldap_sasl_max_pool_size"
	// AuthenticationLDAPSimpleAuthMethodName defines the authentication method used by LDAP Simple authentication plugin
	AuthenticationLDAPSimpleAuthMethodName = "authentication_ldap_simple_auth_method_name"
	// AuthenticationLDAPSimpleCAPath defines the ca certificate to verify LDAP connection in LDAP Simple authentication plugin
	AuthenticationLDAPSimpleCAPath = "authentication_ldap_simple_ca_path"
	// AuthenticationLDAPSimpleTLS defines whether to use TLS connection in LDAP Simple authentication plugin
	AuthenticationLDAPSimpleTLS = "authentication_ldap_simple_tls"
	// AuthenticationLDAPSimpleServerHost defines the server host of LDAP server for LDAP Simple authentication plugin
	AuthenticationLDAPSimpleServerHost = "authentication_ldap_simple_server_host"
	// AuthenticationLDAPSimpleServerPort defines the port of LDAP server for LDAP Simple authentication plugin
	AuthenticationLDAPSimpleServerPort = "authentication_ldap_simple_server_port"
	// AuthenticationLDAPSimpleReferral defines whether to enable LDAP referral for LDAP Simple authentication plugin
	AuthenticationLDAPSimpleReferral = "authentication_ldap_simple_referral"
	// AuthenticationLDAPSimpleUserSearchAttr defines the attribute of username in LDAP server
	AuthenticationLDAPSimpleUserSearchAttr = "authentication_ldap_simple_user_search_attr"
	// AuthenticationLDAPSimpleBindBaseDN defines the `dn` to search the users in. It's used to limit the search scope of TiDB.
	AuthenticationLDAPSimpleBindBaseDN = "authentication_ldap_simple_bind_base_dn"
	// AuthenticationLDAPSimpleBindRootDN defines the `dn` of the user to login the LDAP server and perform search.
	AuthenticationLDAPSimpleBindRootDN = "authentication_ldap_simple_bind_root_dn"
	// AuthenticationLDAPSimpleBindRootPWD defines the password of the user to login the LDAP server and perform search.
	AuthenticationLDAPSimpleBindRootPWD = "authentication_ldap_simple_bind_root_pwd"
	// AuthenticationLDAPSimpleInitPoolSize defines the init size of connection pool to LDAP server for SASL plugin.
	AuthenticationLDAPSimpleInitPoolSize = "authentication_ldap_simple_init_pool_size"
	// AuthenticationLDAPSimpleMaxPoolSize defines the max size of connection pool to LDAP server for SASL plugin.
	AuthenticationLDAPSimpleMaxPoolSize = "authentication_ldap_simple_max_pool_size"
	// TiDBRuntimeFilterTypeName the value of is string, a runtime filter type list split by ",", such as: "IN,MIN_MAX"
	TiDBRuntimeFilterTypeName = "tidb_runtime_filter_type"
	// TiDBRuntimeFilterModeName the mode of runtime filter, such as "OFF", "LOCAL"
	TiDBRuntimeFilterModeName = "tidb_runtime_filter_mode"
	// TiDBSkipMissingPartitionStats controls how to handle missing partition stats when merging partition stats to global stats.
	// When set to true, skip missing partition stats and continue to merge other partition stats to global stats.
	// When set to false, give up merging partition stats to global stats.
	TiDBSkipMissingPartitionStats = "tidb_skip_missing_partition_stats"
	// TiDBSessionAlias indicates the alias of a session which is used for tracing.
	TiDBSessionAlias = "tidb_session_alias"
	// TiDBServiceScope indicates the role for tidb for distributed task framework.
	TiDBServiceScope = "tidb_service_scope"
	// TiDBSchemaVersionCacheLimit defines the capacity size of domain infoSchema cache.
	TiDBSchemaVersionCacheLimit = "tidb_schema_version_cache_limit"
	// TiDBEnableTiFlashPipelineMode means if we should use pipeline model to execute query or not in tiflash.
	// It's deprecated and setting it will not have any effect.
	TiDBEnableTiFlashPipelineMode = "tidb_enable_tiflash_pipeline_model"
	// TiDBIdleTransactionTimeout indicates the maximum time duration a transaction could be idle, unit is second.
	// Any idle transaction will be killed after being idle for `tidb_idle_transaction_timeout` seconds.
	// This is similar to https://docs.percona.com/percona-server/5.7/management/innodb_kill_idle_trx.html and https://mariadb.com/kb/en/transaction-timeouts/
	TiDBIdleTransactionTimeout = "tidb_idle_transaction_timeout"
	// TiDBLowResolutionTSOUpdateInterval defines how often to refresh low resolution timestamps.
	TiDBLowResolutionTSOUpdateInterval = "tidb_low_resolution_tso_update_interval"
	// TiDBDMLType indicates the execution type of DML in TiDB.
	// The value can be STANDARD, BULK.
	// Currently, the BULK mode only affects auto-committed DML.
	TiDBDMLType = "tidb_dml_type"
	// TiFlashHashAggPreAggMode indicates the policy of 1st hashagg.
	TiFlashHashAggPreAggMode = "tiflash_hashagg_preaggregation_mode"
	// TiDBEnableLazyCursorFetch defines whether to enable the lazy cursor fetch. If it's `OFF`, all results of
	// of a cursor will be stored in the tidb node in `EXECUTE` command.
	TiDBEnableLazyCursorFetch = "tidb_enable_lazy_cursor_fetch"
	// TiDBTSOClientRPCMode controls how the TSO client performs the TSO RPC requests. It internally controls the
	// concurrency of the RPC. This variable provides an approach to tune the latency of getting timestamps from PD.
	TiDBTSOClientRPCMode = "tidb_tso_client_rpc_mode"
	// TiDBCircuitBreakerPDMetadataErrorRateThresholdRatio variable is used to set ratio of errors to trip the circuit breaker for get region calls to PD
	// https://github.com/tikv/rfcs/blob/master/text/0115-circuit-breaker.md
	TiDBCircuitBreakerPDMetadataErrorRateThresholdRatio = "tidb_cb_pd_metadata_error_rate_threshold_ratio"

	// TiDBEnableTSValidation controls whether to enable the timestamp validation in client-go.
	TiDBEnableTSValidation = "tidb_enable_ts_validation"

	// TiDBAdvancerCheckPointLagLimit controls the maximum lag could be tolerated for the checkpoint lag.
	// The log backup task will be paused if the checkpoint lag is larger than it.
	TiDBAdvancerCheckPointLagLimit = "tidb_advancer_check_point_lag_limit"

	// TiDBIndexLookUpPushDownPolicy controls the push down policy of index lookup.
	TiDBIndexLookUpPushDownPolicy = "tidb_index_lookup_pushdown_policy"
)

// TiDB intentional limits, can be raised in the future.
const (
	// MaxConfigurableConcurrency is the maximum number of "threads" (goroutines) that can be specified
	// for any type of configuration item that has concurrent workers.
	MaxConfigurableConcurrency = 256

	// MaxShardRowIDBits is the maximum number of bits that can be used for row-id sharding.
	MaxShardRowIDBits = 15

	// MaxPreSplitRegions is the maximum number of regions that can be pre-split.
	MaxPreSplitRegions = 15
)

// Pipelined-DML related constants
const (
	// MinPipelinedDMLConcurrency is the minimum acceptable concurrency
	MinPipelinedDMLConcurrency = 1
	// MaxPipelinedDMLConcurrency is the maximum acceptable concurrency
	MaxPipelinedDMLConcurrency = 8192

	// DefaultFlushConcurrency is the default flush concurrency
	DefaultFlushConcurrency = 128
	// DefaultResolveConcurrency is the default resolve_lock concurrency
	DefaultResolveConcurrency = 8

	// ConservativeFlushConcurrency is the flush concurrency in conservative mode
	ConservativeFlushConcurrency = 2
	// ConservativeResolveConcurrency is the resolve_lock concurrency in conservative mode
	ConservativeResolveConcurrency = 2
)
