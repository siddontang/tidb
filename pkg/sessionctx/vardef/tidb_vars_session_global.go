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

// TiDB system variable names that both in session and global scope.
const (
	// TiDBBuildStatsConcurrency specifies the number of concurrent workers used for analyzing tables or partitions.
	// When multiple tables or partitions are specified in the analyze statement, TiDB will process them concurrently.
	TiDBBuildStatsConcurrency = "tidb_build_stats_concurrency"

	// TiDBBuildSamplingStatsConcurrency is used to control the concurrency of building stats using sampling.
	// 1. The number of concurrent workers to merge FMSketches and Sample Data from different regions.
	// 2. The number of concurrent workers to build TopN and Histogram concurrently.
	// Additionally, this setting controls the concurrency for building NDV (Number of Distinct Values) for special indexes,
	// such as generated columns composed indexes.
	TiDBBuildSamplingStatsConcurrency = "tidb_build_sampling_stats_concurrency"

	// TiDBDistSQLScanConcurrency is used to set the concurrency of a distsql scan task.
	// A distsql scan task can be a table scan or a index scan, which may be distributed to many TiKV nodes.
	// Higher concurrency may reduce latency, but with the cost of higher memory usage and system performance impact.
	// If the query has a LIMIT clause, high concurrency makes the system do much more work than needed.
	TiDBDistSQLScanConcurrency = "tidb_distsql_scan_concurrency"

	// TiDBAnalyzeDistSQLScanConcurrency is the number of concurrent workers to scan regions to collect statistics (FMSketch, Samples).
	// For auto analyze, the value is controlled by tidb_sysproc_scan_concurrency variable.
	// This variable was introduced in v7.6.0 to separate the scan concurrency of ANALYZE operations from normal queries. See: https://github.com/pingcap/tidb/pull/48829
	// For versions earlier than v7.6.0, the scan concurrency of regions during ANALYZE is controlled by the tidb_distsql_scan_concurrency variable.
	// Starting from v7.6.0, this variable also controls the scan concurrency of index serial scans during ANALYZE. See: https://github.com/pingcap/tidb/pull/50639
	// For versions earlier than v7.6.0, the scan concurrency of index serial scans during ANALYZE is controlled by the tidb_index_serial_scan_concurrency variable.
	TiDBAnalyzeDistSQLScanConcurrency = "tidb_analyze_distsql_scan_concurrency"

	// TiDBOptInSubqToJoinAndAgg is used to enable/disable the optimizer rule of rewriting IN subquery.
	TiDBOptInSubqToJoinAndAgg = "tidb_opt_insubq_to_join_and_agg"

	// TiDBOptPreferRangeScan is used to enable/disable the optimizer to always prefer range scan over table scan, ignoring their costs.
	TiDBOptPreferRangeScan = "tidb_opt_prefer_range_scan"

	// TiDBOptEnableNoDecorrelateInSelect is used to control whether to enable the NO_DECORRELATE hint for subqueries in the select list.
	TiDBOptEnableNoDecorrelateInSelect = "tidb_opt_enable_no_decorrelate_in_select"

	// TiDBEnableSemiJoinRewrite controls automatic rewrite of semi-join to
	// inner-join with aggregation (equivalent to SEMI_JOIN_REWRITE() hint).
	TiDBOptEnableSemiJoinRewrite = "tidb_opt_enable_semi_join_rewrite"

	// TiDBOptEnableCorrelationAdjustment is used to indicates if enable correlation adjustment.
	TiDBOptEnableCorrelationAdjustment = "tidb_opt_enable_correlation_adjustment"

	// TiDBOptLimitPushDownThreshold determines if push Limit or TopN down to TiKV forcibly.
	TiDBOptLimitPushDownThreshold = "tidb_opt_limit_push_down_threshold"

	// TiDBOptCorrelationThreshold is a guard to enable row count estimation using column order correlation.
	TiDBOptCorrelationThreshold = "tidb_opt_correlation_threshold"

	// TiDBOptCorrelationExpFactor is an exponential factor to control heuristic approach when tidb_opt_correlation_threshold is not satisfied.
	TiDBOptCorrelationExpFactor = "tidb_opt_correlation_exp_factor"

	// TiDBOptRiskEqSkewRatio controls the amount of skew is applied to equal predicate estimation when a value is not found in TopN/buckets.
	TiDBOptRiskEqSkewRatio = "tidb_opt_risk_eq_skew_ratio"

	// TiDBOptRiskRangeSkewRatio controls the amount of skew that is applied to range predicate estimation when a range falls within a bucket or outside the histogram bucket range.
	TiDBOptRiskRangeSkewRatio = "tidb_opt_risk_range_skew_ratio"

	// TiDBOptRiskScaleNDVSkewRatio controls the NDV estimation risk strategy for scaling NDV estimation.
	TiDBOptRiskScaleNDVSkewRatio = "tidb_opt_scale_ndv_skew_ratio"

	// TiDBOptRiskGroupNDVSkewRatio controls the NDV estimation risk strategy for multi-column operations
	// including GROUP BY, JOIN, and DISTINCT operations.
	// When 0: uses conservative estimate (max of individual column NDVs, production default)
	// When > 0: blends conservative and exponential backoff estimates (0.1=mostly conservative, 1.0=full exponential)
	TiDBOptRiskGroupNDVSkewRatio = "tidb_opt_group_ndv_skew_ratio"

	// TiDBOptAlwaysKeepJoinKey indicates the optimizer to always keep join keys during optimization.
	// Join keys are crucial for join optimization like Join Order and Join Algorithm selection, removing
	// join keys might lead to suboptimal plans in some cases.
	TiDBOptAlwaysKeepJoinKey = "tidb_opt_always_keep_join_key"

	// TiDBOptCartesianJoinOrderThreshold controls whether to allow do Cartesian Join first in Join Reorder.
	// This variable is used as a penalty to trade off the risk and join order quality.
	// When 0: never do Cartesian Join first.
	// When > 0: allow Cartesian Join if cost(cartesian join) * threshold < cost(non cartesian join).
	TiDBOptCartesianJoinOrderThreshold = "tidb_opt_cartesian_join_order_threshold"

	// TiDBOptCPUFactor is the CPU cost of processing one expression for one row.
	TiDBOptCPUFactor = "tidb_opt_cpu_factor"
	// TiDBOptCopCPUFactor is the CPU cost of processing one expression for one row in coprocessor.
	TiDBOptCopCPUFactor = "tidb_opt_copcpu_factor"
	// TiDBOptTiFlashConcurrencyFactor is concurrency number of tiflash computation.
	TiDBOptTiFlashConcurrencyFactor = "tidb_opt_tiflash_concurrency_factor"
	// TiDBOptNetworkFactor is the network cost of transferring 1 byte data.
	TiDBOptNetworkFactor = "tidb_opt_network_factor"
	// TiDBOptScanFactor is the IO cost of scanning 1 byte data on TiKV.
	TiDBOptScanFactor = "tidb_opt_scan_factor"
	// TiDBOptDescScanFactor is the IO cost of scanning 1 byte data on TiKV in desc order.
	TiDBOptDescScanFactor = "tidb_opt_desc_factor"
	// TiDBOptSeekFactor is the IO cost of seeking the start value in a range on TiKV or TiFlash.
	TiDBOptSeekFactor = "tidb_opt_seek_factor"
	// TiDBOptMemoryFactor is the memory cost of storing one tuple.
	TiDBOptMemoryFactor = "tidb_opt_memory_factor"
	// TiDBOptDiskFactor is the IO cost of reading/writing one byte to temporary disk.
	TiDBOptDiskFactor = "tidb_opt_disk_factor"
	// TiDBOptConcurrencyFactor is the CPU cost of additional one goroutine.
	TiDBOptConcurrencyFactor = "tidb_opt_concurrency_factor"

	// The following optimizer cost factors represent a multiplier for each optimizer physical operator.
	// These factors are used to adjust the cost of each operator to influence the optimizer's plan selection.
	TiDBOptIndexScanCostFactor        = "tidb_opt_index_scan_cost_factor"
	TiDBOptIndexReaderCostFactor      = "tidb_opt_index_reader_cost_factor"
	TiDBOptTableReaderCostFactor      = "tidb_opt_table_reader_cost_factor"
	TiDBOptTableFullScanCostFactor    = "tidb_opt_table_full_scan_cost_factor"
	TiDBOptTableRangeScanCostFactor   = "tidb_opt_table_range_scan_cost_factor"
	TiDBOptTableRowIDScanCostFactor   = "tidb_opt_table_rowid_scan_cost_factor"
	TiDBOptTableTiFlashScanCostFactor = "tidb_opt_table_tiflash_scan_cost_factor"
	TiDBOptIndexLookupCostFactor      = "tidb_opt_index_lookup_cost_factor"
	TiDBOptIndexMergeCostFactor       = "tidb_opt_index_merge_cost_factor"
	TiDBOptSortCostFactor             = "tidb_opt_sort_cost_factor"
	TiDBOptTopNCostFactor             = "tidb_opt_topn_cost_factor"
	TiDBOptLimitCostFactor            = "tidb_opt_limit_cost_factor"
	TiDBOptStreamAggCostFactor        = "tidb_opt_stream_agg_cost_factor"
	TiDBOptHashAggCostFactor          = "tidb_opt_hash_agg_cost_factor"
	TiDBOptMergeJoinCostFactor        = "tidb_opt_merge_join_cost_factor"
	TiDBOptHashJoinCostFactor         = "tidb_opt_hash_join_cost_factor"
	TiDBOptIndexJoinCostFactor        = "tidb_opt_index_join_cost_factor"

	// The following selectivity factors represent a multiplier for the selectivity of each predicate.
	// These factors are used to determine the selectivity of predicates in the optimizer's cost model.
	// TiDBOptSelectivityFactor: If one condition can't be calculated,
	// we will assume that the selectivity of this condition is 0.8 by default.
	TiDBOptSelectivityFactor = "tidb_opt_selectivity_factor"

	// TiDBOptForceInlineCTE is used to enable/disable inline CTE
	TiDBOptForceInlineCTE = "tidb_opt_force_inline_cte"

	// TiDBIndexJoinBatchSize is used to set the batch size of an index lookup join.
	// The index lookup join fetches batches of data from outer executor and constructs ranges for inner executor.
	// This value controls how much of data in a batch to do the index join.
	// Large value may reduce the latency but consumes more system resource.
	TiDBIndexJoinBatchSize = "tidb_index_join_batch_size"

	// TiDBIndexLookupSize is used for index lookup executor.
	// The index lookup executor first scan a batch of handles from a index, then use those handles to lookup the table
	// rows, this value controls how much of handles in a batch to do a lookup task.
	// Small value sends more RPCs to TiKV, consume more system resource.
	// Large value may do more work than needed if the query has a limit.
	TiDBIndexLookupSize = "tidb_index_lookup_size"

	// TiDBIndexLookupConcurrency is used for index lookup executor.
	// A lookup task may have 'tidb_index_lookup_size' of handles at maximum, the handles may be distributed
	// in many TiKV nodes, we execute multiple concurrent index lookup tasks concurrently to reduce the time
	// waiting for a task to finish.
	// Set this value higher may reduce the latency but consumes more system resource.
	// tidb_index_lookup_concurrency is deprecated, use tidb_executor_concurrency instead.
	TiDBIndexLookupConcurrency = "tidb_index_lookup_concurrency"

	// TiDBIndexLookupJoinConcurrency is used for index lookup join executor.
	// IndexLookUpJoin starts "tidb_index_lookup_join_concurrency" inner workers
	// to fetch inner rows and join the matched (outer, inner) row pairs.
	// tidb_index_lookup_join_concurrency is deprecated, use tidb_executor_concurrency instead.
	TiDBIndexLookupJoinConcurrency = "tidb_index_lookup_join_concurrency"

	// TiDBIndexSerialScanConcurrency is used for controlling the concurrency of index scan operation
	// when we need to keep the data output order the same as the order of index data.
	// Deprecated: Use tidb_executor_concurrency for sequential scans and tidb_analyze_distsql_scan_concurrency for ANALYZE.
	// Before v5.0.0, this variable was used to control the concurrency of index scan operations for both regular queries and ANALYZE statements. See: https://github.com/pingcap/tidb/pull/16999
	// From version v5.0.0 up to (and including) v8.0.0, this variable was used only to control the concurrency of index scan operations for ANALYZE statements. See: https://github.com/pingcap/tidb/pull/50639
	TiDBIndexSerialScanConcurrency = "tidb_index_serial_scan_concurrency"

	// TiDBMaxChunkSize is used to control the max chunk size during query execution.
	TiDBMaxChunkSize = "tidb_max_chunk_size"

	// TiDBAllowBatchCop means if we should send batch coprocessor to TiFlash. It can be set to 0, 1 and 2.
	// 0 means never use batch cop, 1 means use batch cop in case of aggregation and join, 2, means to force sending batch cop for any query.
	// The default value is 0
	TiDBAllowBatchCop = "tidb_allow_batch_cop"

	// TiDBShardRowIDBits means all the tables created in the current session will be sharded.
	// The default value is 0
	TiDBShardRowIDBits = "tidb_shard_row_id_bits"

	// TiDBPreSplitRegions means all the tables created in the current session will be pre-splited.
	// The default value is 0
	TiDBPreSplitRegions = "tidb_pre_split_regions"

	// TiDBAllowMPPExecution means if we should use mpp way to execute query or not.
	// Default value is `true`, means to be determined by the optimizer.
	// Value set to `false` means never use mpp.
	TiDBAllowMPPExecution = "tidb_allow_mpp"

	// TiDBAllowTiFlashCop means we only use MPP mode to query data.
	// Default value is `true`, means to be determined by the optimizer.
	// Value set to `false` means we may fall back to TiFlash cop plan if possible.
	TiDBAllowTiFlashCop = "tidb_allow_tiflash_cop"

	// TiDBHashExchangeWithNewCollation means if hash exchange is supported when new collation is on.
	// Default value is `true`, means support hash exchange when new collation is on.
	// Value set to `false` means not support hash exchange when new collation is on.
	TiDBHashExchangeWithNewCollation = "tidb_hash_exchange_with_new_collation"

	// TiDBEnforceMPPExecution means if we should enforce mpp way to execute query or not.
	// Default value is `false`, means to be determined by variable `tidb_allow_mpp`.
	// Value set to `true` means enforce use mpp.
	// Note if you want to set `tidb_enforce_mpp` to `true`, you must set `tidb_allow_mpp` to `true` first.
	TiDBEnforceMPPExecution = "tidb_enforce_mpp"

	// TiDBMaxTiFlashThreads is the maximum number of threads to execute the request which is pushed down to tiflash.
	// Default value is -1, means it will not be pushed down to tiflash.
	// If the value is bigger than -1, it will be pushed down to tiflash and used to create db context in tiflash.
	TiDBMaxTiFlashThreads = "tidb_max_tiflash_threads"

	// TiDBMaxBytesBeforeTiFlashExternalJoin is the maximum bytes used by a TiFlash join before spill to disk
	TiDBMaxBytesBeforeTiFlashExternalJoin = "tidb_max_bytes_before_tiflash_external_join"

	// TiDBMaxBytesBeforeTiFlashExternalGroupBy is the maximum bytes used by a TiFlash hash aggregation before spill to disk
	TiDBMaxBytesBeforeTiFlashExternalGroupBy = "tidb_max_bytes_before_tiflash_external_group_by"

	// TiDBMaxBytesBeforeTiFlashExternalSort is the maximum bytes used by a TiFlash sort/TopN before spill to disk
	TiDBMaxBytesBeforeTiFlashExternalSort = "tidb_max_bytes_before_tiflash_external_sort"

	// TiFlashMemQuotaQueryPerNode is the maximum bytes used by a TiFlash Query on each TiFlash node
	TiFlashMemQuotaQueryPerNode = "tiflash_mem_quota_query_per_node"

	// TiFlashQuerySpillRatio is the threshold that TiFlash will trigger auto spill when the memory usage is above this percentage
	TiFlashQuerySpillRatio = "tiflash_query_spill_ratio"

	// TiFlashHashJoinVersion indicates whether to use hash join implementation v2 in TiFlash.
	TiFlashHashJoinVersion = "tiflash_hash_join_version"

	// TiDBMPPStoreFailTTL is the unavailable time when a store is detected failed. During that time, tidb will not send any task to
	// TiFlash even though the failed TiFlash node has been recovered.
	TiDBMPPStoreFailTTL = "tidb_mpp_store_fail_ttl"

	// TiDBInitChunkSize is used to control the init chunk size during query execution.
	TiDBInitChunkSize = "tidb_init_chunk_size"

	// TiDBMinPagingSize is used to control the min paging size in the coprocessor paging protocol.
	TiDBMinPagingSize = "tidb_min_paging_size"

	// TiDBMaxPagingSize is used to control the max paging size in the coprocessor paging protocol.
	TiDBMaxPagingSize = "tidb_max_paging_size"

	// TiDBEnableCascadesPlanner is used to control whether to enable the cascades planner.
	TiDBEnableCascadesPlanner = "tidb_enable_cascades_planner"

	// TiDBSkipUTF8Check skips the UTF8 validate process, validate UTF8 has performance cost, if we can make sure
	// the input string values are valid, we can skip the check.
	TiDBSkipUTF8Check = "tidb_skip_utf8_check"

	// TiDBSkipASCIICheck skips the ASCII validate process
	// old tidb may already have fields with invalid ASCII bytes
	// disable ASCII validate can guarantee a safe replication
	TiDBSkipASCIICheck = "tidb_skip_ascii_check"

	// TiDBHashJoinConcurrency is used for hash join executor.
	// The hash join outer executor starts multiple concurrent join workers to probe the hash table.
	// tidb_hash_join_concurrency is deprecated, use tidb_executor_concurrency instead.
	TiDBHashJoinConcurrency = "tidb_hash_join_concurrency"

	// TiDBProjectionConcurrency is used for projection operator.
	// This variable controls the worker number of projection operator.
	// tidb_projection_concurrency is deprecated, use tidb_executor_concurrency instead.
	TiDBProjectionConcurrency = "tidb_projection_concurrency"

	// TiDBHashAggPartialConcurrency is used for hash agg executor.
	// The hash agg executor starts multiple concurrent partial workers to do partial aggregate works.
	// tidb_hashagg_partial_concurrency is deprecated, use tidb_executor_concurrency instead.
	TiDBHashAggPartialConcurrency = "tidb_hashagg_partial_concurrency"

	// TiDBHashAggFinalConcurrency is used for hash agg executor.
	// The hash agg executor starts multiple concurrent final workers to do final aggregate works.
	// tidb_hashagg_final_concurrency is deprecated, use tidb_executor_concurrency instead.
	TiDBHashAggFinalConcurrency = "tidb_hashagg_final_concurrency"

	// TiDBWindowConcurrency is used for window parallel executor.
	// tidb_window_concurrency is deprecated, use tidb_executor_concurrency instead.
	TiDBWindowConcurrency = "tidb_window_concurrency"

	// TiDBMergeJoinConcurrency is used for merge join parallel executor
	TiDBMergeJoinConcurrency = "tidb_merge_join_concurrency"

	// TiDBStreamAggConcurrency is used for stream aggregation parallel executor.
	// tidb_stream_agg_concurrency is deprecated, use tidb_executor_concurrency instead.
	TiDBStreamAggConcurrency = "tidb_streamagg_concurrency"

	// TiDBIndexMergeIntersectionConcurrency is used for parallel worker of index merge intersection.
	TiDBIndexMergeIntersectionConcurrency = "tidb_index_merge_intersection_concurrency"

	// TiDBEnableParallelApply is used for parallel apply.
	TiDBEnableParallelApply = "tidb_enable_parallel_apply"

	// TiDBBackoffLockFast is used for tikv backoff base time in milliseconds.
	TiDBBackoffLockFast = "tidb_backoff_lock_fast"

	// TiDBBackOffWeight is used to control the max back off time in TiDB.
	// The default maximum back off time is a small value.
	// BackOffWeight could multiply it to let the user adjust the maximum time for retrying.
	// Only positive integers can be accepted, which means that the maximum back off time can only grow.
	TiDBBackOffWeight = "tidb_backoff_weight"

	// TiDBDDLReorgWorkerCount defines the count of ddl reorg workers.
	TiDBDDLReorgWorkerCount = "tidb_ddl_reorg_worker_cnt"

	// TiDBDDLFlashbackConcurrency defines the count of ddl flashback workers.
	TiDBDDLFlashbackConcurrency = "tidb_ddl_flashback_concurrency"

	// TiDBDDLReorgBatchSize defines the transaction batch size of ddl reorg workers.
	TiDBDDLReorgBatchSize = "tidb_ddl_reorg_batch_size"

	// TiDBDDLErrorCountLimit defines the count of ddl error limit.
	TiDBDDLErrorCountLimit = "tidb_ddl_error_count_limit"

	// TiDBDDLReorgPriority defines the operations' priority of adding indices.
	// It can be: PRIORITY_LOW, PRIORITY_NORMAL, PRIORITY_HIGH
	TiDBDDLReorgPriority = "tidb_ddl_reorg_priority"

	// TiDBDDLReorgMaxWriteSpeed defines the max write limitation for the lightning local backend
	TiDBDDLReorgMaxWriteSpeed = "tidb_ddl_reorg_max_write_speed"

	// TiDBEnableAutoIncrementInGenerated disables the mysql compatibility check on using auto-incremented columns in
	// expression indexes and generated columns described here https://dev.mysql.com/doc/refman/5.7/en/create-table-generated-columns.html for details.
	TiDBEnableAutoIncrementInGenerated = "tidb_enable_auto_increment_in_generated"

	// TiDBEnablePointGetCache is used to control whether to enable the point get cache for special scenario.
	TiDBEnablePointGetCache = "tidb_enable_point_get_cache"

	// TiDBPlacementMode is used to control the mode for placement
	TiDBPlacementMode = "tidb_placement_mode"

	// TiDBMaxDeltaSchemaCount defines the max length of deltaSchemaInfos.
	// deltaSchemaInfos is a queue that maintains the history of schema changes.
	TiDBMaxDeltaSchemaCount = "tidb_max_delta_schema_count"

	// TiDBScatterRegion will scatter the regions for DDLs when it is "table" or "global", "" indicates not trigger scatter.
	TiDBScatterRegion = "tidb_scatter_region"

	// TiDBWaitSplitRegionFinish defines the split region behaviour is sync or async.
	TiDBWaitSplitRegionFinish = "tidb_wait_split_region_finish"

	// TiDBWaitSplitRegionTimeout uses to set the split and scatter region back off time.
	TiDBWaitSplitRegionTimeout = "tidb_wait_split_region_timeout"

	// TiDBForcePriority defines the operations' priority of all statements.
	// It can be "NO_PRIORITY", "LOW_PRIORITY", "HIGH_PRIORITY", "DELAYED"
	TiDBForcePriority = "tidb_force_priority"

	// TiDBConstraintCheckInPlace indicates to check the constraint when the SQL executing.
	// It could hurt the performance of bulking insert when it is ON.
	TiDBConstraintCheckInPlace = "tidb_constraint_check_in_place"

	// TiDBEnableWindowFunction is used to control whether to enable the window function.
	TiDBEnableWindowFunction = "tidb_enable_window_function"

	// TiDBEnablePipelinedWindowFunction is used to control whether to use pipelined window function, it only works when tidb_enable_window_function = true.
	TiDBEnablePipelinedWindowFunction = "tidb_enable_pipelined_window_function"

	// TiDBEnableStrictDoubleTypeCheck is used to control table field double type syntax check.
	TiDBEnableStrictDoubleTypeCheck = "tidb_enable_strict_double_type_check"

	// TiDBOptProjectionPushDown is used to control whether to pushdown projection to coprocessor.
	TiDBOptProjectionPushDown = "tidb_opt_projection_push_down"

	// TiDBEnableVectorizedExpression is used to control whether to enable the vectorized expression evaluation.
	TiDBEnableVectorizedExpression = "tidb_enable_vectorized_expression"

	// TiDBOptJoinReorderThreshold defines the threshold less than which
	// we'll choose a rather time-consuming algorithm to calculate the join order.
	TiDBOptJoinReorderThreshold = "tidb_opt_join_reorder_threshold"

	// TiDBOptJoinReorderThroughSel enables pushing selection conditions down to
	// reordered join trees when applicable.
	TiDBOptJoinReorderThroughSel = "tidb_opt_join_reorder_through_sel"

	// TiDBSlowQueryFile indicates which slow query log file for SLOW_QUERY table to parse.
	TiDBSlowQueryFile = "tidb_slow_query_file"

	// TiDBEnableFastAnalyze indicates to use fast analyze.
	// Deprecated: This variable is deprecated, please do not use this variable.
	TiDBEnableFastAnalyze = "tidb_enable_fast_analyze"

	// TiDBExpensiveQueryTimeThreshold indicates the time threshold of expensive query.
	TiDBExpensiveQueryTimeThreshold = "tidb_expensive_query_time_threshold"

	// TiDBExpensiveTxnTimeThreshold indicates the time threshold of expensive transaction.
	TiDBExpensiveTxnTimeThreshold = "tidb_expensive_txn_time_threshold"

	// TiDBEnableIndexMerge indicates to generate IndexMergePath.
	TiDBEnableIndexMerge = "tidb_enable_index_merge"

	// TiDBEnableNoopFuncs set true will enable using fake funcs(like get_lock release_lock)
	TiDBEnableNoopFuncs = "tidb_enable_noop_functions"

	// TiDBEnableStmtSummary indicates whether the statement summary is enabled.
	TiDBEnableStmtSummary = "tidb_enable_stmt_summary"

	// TiDBStmtSummaryInternalQuery indicates whether the statement summary contain internal query.
	TiDBStmtSummaryInternalQuery = "tidb_stmt_summary_internal_query"

	// TiDBStmtSummaryRefreshInterval indicates the refresh interval in seconds for each statement summary.
	TiDBStmtSummaryRefreshInterval = "tidb_stmt_summary_refresh_interval"

	// TiDBStmtSummaryHistorySize indicates the history size of each statement summary.
	TiDBStmtSummaryHistorySize = "tidb_stmt_summary_history_size"

	// TiDBStmtSummaryMaxStmtCount indicates the max number of statements kept in memory.
	TiDBStmtSummaryMaxStmtCount = "tidb_stmt_summary_max_stmt_count"

	// TiDBStmtSummaryMaxSQLLength indicates the max length of displayed normalized sql and sample sql.
	TiDBStmtSummaryMaxSQLLength = "tidb_stmt_summary_max_sql_length"

	// TiDBIgnoreInlistPlanDigest enables TiDB to generate the same plan digest with SQL using different in-list arguments.
	TiDBIgnoreInlistPlanDigest = "tidb_ignore_inlist_plan_digest"

	// TiDBCapturePlanBaseline indicates whether the capture of plan baselines is enabled.
	TiDBCapturePlanBaseline = "tidb_capture_plan_baselines"

	// TiDBUsePlanBaselines indicates whether the use of plan baselines is enabled.
	TiDBUsePlanBaselines = "tidb_use_plan_baselines"

	// TiDBEvolvePlanBaselines indicates whether the evolution of plan baselines is enabled.
	TiDBEvolvePlanBaselines = "tidb_evolve_plan_baselines"

	// TiDBOptEnableFuzzyBinding indicates whether to enable the universal binding.
	TiDBOptEnableFuzzyBinding = "tidb_opt_enable_fuzzy_binding"

	// TiDBEnableExtendedStats indicates whether the extended statistics feature is enabled.
	TiDBEnableExtendedStats = "tidb_enable_extended_stats"

	// TiDBIsolationReadEngines indicates the tidb only read from the stores whose engine type is involved in IsolationReadEngines.
	// Now, only support TiKV and TiFlash.
	TiDBIsolationReadEngines = "tidb_isolation_read_engines"

	// TiDBStoreLimit indicates the limit of sending request to a store, 0 means without limit.
	TiDBStoreLimit = "tidb_store_limit"

	// TiDBMetricSchemaStep indicates the step when query metric schema.
	TiDBMetricSchemaStep = "tidb_metric_query_step"

	// TiDBCDCWriteSource indicates the following data is written by TiCDC if it is not 0.
	TiDBCDCWriteSource = "tidb_cdc_write_source"

	// TiDBMetricSchemaRangeDuration indicates the range duration when query metric schema.
	TiDBMetricSchemaRangeDuration = "tidb_metric_query_range_duration"

	// TiDBEnableCollectExecutionInfo indicates that whether execution info is collected.
	TiDBEnableCollectExecutionInfo = "tidb_enable_collect_execution_info"

	// TiDBExecutorConcurrency is used for controlling the concurrency of all types of executors.
	TiDBExecutorConcurrency = "tidb_executor_concurrency"

	// TiDBEnableClusteredIndex indicates if clustered index feature is enabled.
	TiDBEnableClusteredIndex = "tidb_enable_clustered_index"

	// TiDBEnableGlobalIndex means if we could create an global index on a partition table or not.
	// Deprecated, will always be ON
	TiDBEnableGlobalIndex = "tidb_enable_global_index"

	// TiDBPartitionPruneMode indicates the partition prune mode used.
	TiDBPartitionPruneMode = "tidb_partition_prune_mode"

	// TiDBRedactLog indicates that whether redact log.
	TiDBRedactLog = "tidb_redact_log"

	// TiDBRestrictedReadOnly is meant for the cloud admin to toggle the cluster read only
	TiDBRestrictedReadOnly = "tidb_restricted_read_only"

	// TiDBSuperReadOnly is tidb's variant of mysql's super_read_only, which has some differences from mysql's super_read_only.
	TiDBSuperReadOnly = "tidb_super_read_only"

	// TiDBShardAllocateStep indicates the max size of continuous rowid shard in one transaction.
	TiDBShardAllocateStep = "tidb_shard_allocate_step"
	// TiDBEnableTelemetry indicates that whether usage data report to PingCAP is enabled.
	// Deprecated: it is 'off' always since Telemetry has been removed from TiDB.
	TiDBEnableTelemetry = "tidb_enable_telemetry"

	// TiDBMemoryUsageAlarmRatio indicates the alarm threshold when memory usage of the tidb-server exceeds.
	TiDBMemoryUsageAlarmRatio = "tidb_memory_usage_alarm_ratio"

	// TiDBMemoryUsageAlarmKeepRecordNum indicates the number of saved alarm files.
	TiDBMemoryUsageAlarmKeepRecordNum = "tidb_memory_usage_alarm_keep_record_num"

	// TiDBEnableRateLimitAction indicates whether enabled ratelimit action
	TiDBEnableRateLimitAction = "tidb_enable_rate_limit_action"

	// TiDBEnableAsyncCommit indicates whether to enable the async commit feature.
	TiDBEnableAsyncCommit = "tidb_enable_async_commit"

	// TiDBEnable1PC indicates whether to enable the one-phase commit feature.
	TiDBEnable1PC = "tidb_enable_1pc"

	// TiDBGuaranteeLinearizability indicates whether to guarantee linearizability.
	TiDBGuaranteeLinearizability = "tidb_guarantee_linearizability"

	// TiDBAnalyzeVersion indicates how tidb collects the analyzed statistics and how use to it.
	TiDBAnalyzeVersion = "tidb_analyze_version"

	// TiDBAutoAnalyzePartitionBatchSize indicates the batch size for partition tables for auto analyze in dynamic mode
	// Deprecated: This variable is deprecated, please do not use this variable.
	TiDBAutoAnalyzePartitionBatchSize = "tidb_auto_analyze_partition_batch_size"

	// TiDBEnableIndexMergeJoin indicates whether to enable index merge join.
	TiDBEnableIndexMergeJoin = "tidb_enable_index_merge_join"

	// TiDBTrackAggregateMemoryUsage indicates whether track the memory usage of aggregate function.
	TiDBTrackAggregateMemoryUsage = "tidb_track_aggregate_memory_usage"

	// TiDBEnableExchangePartition indicates whether to enable exchange partition.
	TiDBEnableExchangePartition = "tidb_enable_exchange_partition"

	// TiDBAllowFallbackToTiKV indicates the engine types whose unavailability triggers fallback to TiKV.
	// Now we only support TiFlash.
	TiDBAllowFallbackToTiKV = "tidb_allow_fallback_to_tikv"

	// TiDBEnableTopSQL indicates whether the top SQL is enabled.
	TiDBEnableTopSQL = "tidb_enable_top_sql"

	// TiDBSourceID indicates the source ID of the TiDB server.
	TiDBSourceID = "tidb_source_id"

	// TiDBTopSQLMaxTimeSeriesCount indicates the max number of statements been collected in each time series.
	TiDBTopSQLMaxTimeSeriesCount = "tidb_top_sql_max_time_series_count"

	// TiDBTopSQLMaxMetaCount indicates the max capacity of the collect meta per second.
	TiDBTopSQLMaxMetaCount = "tidb_top_sql_max_meta_count"

	// TiDBEnableLocalTxn indicates whether to enable Local Txn.
	TiDBEnableLocalTxn = "tidb_enable_local_txn"

	// TiDBEnableMDL indicates whether to enable MDL.
	TiDBEnableMDL = "tidb_enable_metadata_lock"

	// TiDBTSOClientBatchMaxWaitTime indicates the max value of the TSO Batch Wait interval time of PD client.
	TiDBTSOClientBatchMaxWaitTime = "tidb_tso_client_batch_max_wait_time"

	// TiDBTxnCommitBatchSize is used to control the batch size of transaction commit related requests sent by TiDB to TiKV.
	// If a single transaction has a large amount of writes, you can increase the batch size to improve the batch effect,
	// setting too large will exceed TiKV's raft-entry-max-size limit and cause commit failure.
	TiDBTxnCommitBatchSize = "tidb_txn_commit_batch_size"

	// TiDBEnableTSOFollowerProxy indicates whether to enable the TSO Follower Proxy feature of PD client.
	TiDBEnableTSOFollowerProxy = "tidb_enable_tso_follower_proxy"

	// PDEnableFollowerHandleRegion indicates whether to enable the PD Follower handle region API.
	// TODO: deprecated this variable to use a format like `tidb_enable_pd_follower_handle_region`.
	PDEnableFollowerHandleRegion = "pd_enable_follower_handle_region"

	// TiDBEnableBatchQueryRegion indicates whether to enable the batch query region feature.
	TiDBEnableBatchQueryRegion = "tidb_enable_batch_query_region"

	// TiDBEnableOrderedResultMode indicates if stabilize query results.
	TiDBEnableOrderedResultMode = "tidb_enable_ordered_result_mode"

	// TiDBRemoveOrderbyInSubquery indicates whether to remove ORDER BY in subquery.
	TiDBRemoveOrderbyInSubquery = "tidb_remove_orderby_in_subquery"

	// TiDBEnablePseudoForOutdatedStats indicates whether use pseudo for outdated stats
	TiDBEnablePseudoForOutdatedStats = "tidb_enable_pseudo_for_outdated_stats"

	// TiDBRegardNULLAsPoint indicates whether regard NULL as point when optimizing
	TiDBRegardNULLAsPoint = "tidb_regard_null_as_point"

	// TiDBTmpTableMaxSize indicates the max memory size of temporary tables.
	TiDBTmpTableMaxSize = "tidb_tmp_table_max_size"

	// TiDBEnableLegacyInstanceScope indicates if instance scope can be set with SET SESSION.
	TiDBEnableLegacyInstanceScope = "tidb_enable_legacy_instance_scope"

	// TiDBTableCacheLease indicates the read lock lease of a cached table.
	TiDBTableCacheLease = "tidb_table_cache_lease"

	// TiDBStatsLoadSyncWait indicates the time sql execution will sync-wait for stats load.
	TiDBStatsLoadSyncWait = "tidb_stats_load_sync_wait"

	// TiDBEnableMutationChecker indicates whether to check data consistency for mutations
	TiDBEnableMutationChecker = "tidb_enable_mutation_checker"
	// TiDBTxnAssertionLevel indicates how strict the assertion will be, which helps to detect and preventing data &
	// index inconsistency problems.
	TiDBTxnAssertionLevel = "tidb_txn_assertion_level"

	// TiDBIgnorePreparedCacheCloseStmt indicates whether to ignore close-stmt commands for prepared statements.
	TiDBIgnorePreparedCacheCloseStmt = "tidb_ignore_prepared_cache_close_stmt"

	// TiDBEnableNewCostInterface is a internal switch to indicates whether to use the new cost calculation interface.
	TiDBEnableNewCostInterface = "tidb_enable_new_cost_interface"

	// TiDBCostModelVersion is a internal switch to indicates the cost model version.
	TiDBCostModelVersion = "tidb_cost_model_version"

	// TiDBIndexJoinDoubleReadPenaltyCostRate indicates whether to add some penalty cost to IndexJoin and how much of it.
	// IndexJoin can cause plenty of extra double read tasks, which consume lots of resources and take a long time.
	// Since the number of double read tasks is hard to estimated accurately, we leave this variable to let us can adjust this
	// part of cost manually.
	TiDBIndexJoinDoubleReadPenaltyCostRate = "tidb_index_join_double_read_penalty_cost_rate"

	// TiDBBatchPendingTiFlashCount indicates the maximum count of non-available TiFlash tables.
	TiDBBatchPendingTiFlashCount = "tidb_batch_pending_tiflash_count"

	// TiDBQueryLogMaxLen is used to set the max length of the query in the log.
	TiDBQueryLogMaxLen = "tidb_query_log_max_len"

	// TiDBEnableNoopVariables is used to indicate if noops appear in SHOW [GLOBAL] VARIABLES
	TiDBEnableNoopVariables = "tidb_enable_noop_variables"

	// TiDBNonTransactionalIgnoreError is used to ignore error in non-transactional DMLs.
	// When set to false, a non-transactional DML returns when it meets the first error.
	// When set to true, a non-transactional DML finishes all batches even if errors are met in some batches.
	TiDBNonTransactionalIgnoreError = "tidb_nontransactional_ignore_error"

	// Fine grained shuffle is disabled when TiFlashFineGrainedShuffleStreamCount is zero.
	TiFlashFineGrainedShuffleStreamCount = "tiflash_fine_grained_shuffle_stream_count"
	TiFlashFineGrainedShuffleBatchSize   = "tiflash_fine_grained_shuffle_batch_size"

	// TiDBSimplifiedMetrics controls whether to unregister some unused metrics.
	TiDBSimplifiedMetrics = "tidb_simplified_metrics"

	// TiDBMemoryDebugModeMinHeapInUse is used to set tidb memory debug mode trigger threshold.
	// When set to 0, the function is disabled.
	// When set to a negative integer, use memory debug mode to detect the issue of frequent allocation and release of memory.
	// We do not actively trigger gc, and check whether the `tracker memory * (1+bias ratio) > heap in use` each 5s.
	// When set to a positive integer, use memory debug mode to detect the issue of memory tracking inaccurate.
	// We trigger runtime.GC() each 5s, and check whether the `tracker memory * (1+bias ratio) > heap in use`.
	TiDBMemoryDebugModeMinHeapInUse = "tidb_memory_debug_mode_min_heap_inuse"
	// TiDBMemoryDebugModeAlarmRatio is used set tidb memory debug mode bias ratio. Treat memory bias less than this ratio as noise.
	TiDBMemoryDebugModeAlarmRatio = "tidb_memory_debug_mode_alarm_ratio"

	// TiDBEnableAnalyzeSnapshot indicates whether to read data on snapshot when collecting statistics.
	// When set to false, ANALYZE reads the latest data.
	// When set to true, ANALYZE reads data on the snapshot at the beginning of ANALYZE.
	TiDBEnableAnalyzeSnapshot = "tidb_enable_analyze_snapshot"

	// TiDBDefaultStrMatchSelectivity controls some special cardinality estimation strategy for string match functions (like and regexp).
	// When set to 0, Selectivity() will try to evaluate those functions with TopN and NULL in the stats to estimate,
	// and the default selectivity and the selectivity for the histogram part will be 0.1.
	// When set to (0, 1], Selectivity() will use the value of this variable as the default selectivity of those
	// functions instead of the selectionFactor (0.8).
	TiDBDefaultStrMatchSelectivity = "tidb_default_string_match_selectivity"

	// TiDBEnablePrepPlanCache indicates whether to enable prepared plan cache
	TiDBEnablePrepPlanCache = "tidb_enable_prepared_plan_cache"
	// TiDBPrepPlanCacheSize indicates the number of cached statements.
	// This variable is deprecated, use tidb_session_plan_cache_size instead.
	TiDBPrepPlanCacheSize = "tidb_prepared_plan_cache_size"
	// TiDBEnablePrepPlanCacheMemoryMonitor indicates whether to enable prepared plan cache monitor
	TiDBEnablePrepPlanCacheMemoryMonitor = "tidb_enable_prepared_plan_cache_memory_monitor"

	// TiDBEnableNonPreparedPlanCache indicates whether to enable non-prepared plan cache.
	TiDBEnableNonPreparedPlanCache = "tidb_enable_non_prepared_plan_cache"
	// TiDBEnableNonPreparedPlanCacheForDML indicates whether to enable non-prepared plan cache for DML statements.
	TiDBEnableNonPreparedPlanCacheForDML = "tidb_enable_non_prepared_plan_cache_for_dml"
	// TiDBNonPreparedPlanCacheSize controls the size of non-prepared plan cache.
	// This variable is deprecated, use tidb_session_plan_cache_size instead.
	TiDBNonPreparedPlanCacheSize = "tidb_non_prepared_plan_cache_size"
	// TiDBPlanCacheMaxPlanSize controls the maximum size of a plan that can be cached.
	TiDBPlanCacheMaxPlanSize = "tidb_plan_cache_max_plan_size"
	// TiDBPlanCacheInvalidationOnFreshStats controls if plan cache will be invalidated automatically when
	// related stats are analyzed after the plan cache is generated.
	TiDBPlanCacheInvalidationOnFreshStats = "tidb_plan_cache_invalidation_on_fresh_stats"
	// TiDBSessionPlanCacheSize controls the size of session plan cache.
	TiDBSessionPlanCacheSize = "tidb_session_plan_cache_size"

	// TiDBEnableInstancePlanCache indicates whether to enable instance plan cache.
	// If this variable is false, session-level plan cache will be used.
	TiDBEnableInstancePlanCache = "tidb_enable_instance_plan_cache"
	// TiDBInstancePlanCacheReservedPercentage indicates the percentage memory to evict.
	TiDBInstancePlanCacheReservedPercentage = "tidb_instance_plan_cache_reserved_percentage"
	// TiDBInstancePlanCacheMaxMemSize indicates the maximum memory size of instance plan cache.
	TiDBInstancePlanCacheMaxMemSize = "tidb_instance_plan_cache_max_size"

	// TiDBConstraintCheckInPlacePessimistic controls whether to skip certain kinds of pessimistic locks.
	TiDBConstraintCheckInPlacePessimistic = "tidb_constraint_check_in_place_pessimistic"

	// TiDBEnableForeignKey indicates whether to enable foreign key feature.
	// TODO(crazycs520): remove this after foreign key GA.
	TiDBEnableForeignKey = "tidb_enable_foreign_key"

	// TiDBForeignKeyCheckInSharedLock indicates whether to use shared lock for foreign key check.
	TiDBForeignKeyCheckInSharedLock = "tidb_foreign_key_check_in_shared_lock"

	// TiDBOptRangeMaxSize is the max memory limit for ranges. When the optimizer estimates that the memory usage of complete
	// ranges would exceed the limit, it chooses less accurate ranges such as full range. 0 indicates that there is no memory
	// limit for ranges.
	TiDBOptRangeMaxSize = "tidb_opt_range_max_size"

	// TiDBOptAdvancedJoinHint indicates whether the join method hint is compatible with join order hint.
	TiDBOptAdvancedJoinHint = "tidb_opt_advanced_join_hint"
	// TiDBOptUseInvisibleIndexes indicates whether to use invisible indexes.
	TiDBOptUseInvisibleIndexes = "tidb_opt_use_invisible_indexes"
	// TiDBAnalyzePartitionConcurrency is the number of concurrent workers to save statistics to the system tables.
	TiDBAnalyzePartitionConcurrency = "tidb_analyze_partition_concurrency"
	// TiDBMergePartitionStatsConcurrency indicates the concurrency when merge partition stats into global stats
	TiDBMergePartitionStatsConcurrency = "tidb_merge_partition_stats_concurrency"
	// TiDBEnableAsyncMergeGlobalStats indicates whether to enable async merge global stats
	TiDBEnableAsyncMergeGlobalStats = "tidb_enable_async_merge_global_stats"
	// TiDBOptPrefixIndexSingleScan indicates whether to do some optimizations to avoid double scan for prefix index.
	// When set to true, `col is (not) null`(`col` is index prefix column) is regarded as index filter rather than table filter.
	TiDBOptPrefixIndexSingleScan = "tidb_opt_prefix_index_single_scan"
	// TiDBOptPartialOrderedIndexForTopN indicates whether to enable partial ordered index optimization for TOPN queries.
	// Examples of queries that can benefit from this optimization:
	// 1. index a -> order by a, b limit
	// 2. index a, prefix(b) -> order by a, b limit
	TiDBOptPartialOrderedIndexForTopN = "tidb_opt_partial_ordered_index_for_topn"

	// TiDBEnableExternalTSRead indicates whether to enable read through an external ts
	TiDBEnableExternalTSRead = "tidb_enable_external_ts_read"

	// TiDBEnablePlanReplayerCapture indicates whether to enable plan replayer capture
	TiDBEnablePlanReplayerCapture = "tidb_enable_plan_replayer_capture"

	// TiDBEnablePlanReplayerContinuousCapture indicates whether to enable continuous capture
	TiDBEnablePlanReplayerContinuousCapture = "tidb_enable_plan_replayer_continuous_capture"
	// TiDBEnableReusechunk indicates whether to enable chunk alloc
	TiDBEnableReusechunk = "tidb_enable_reuse_chunk"

	// TiDBStoreBatchSize indicates the batch size of coprocessor in the same store.
	TiDBStoreBatchSize = "tidb_store_batch_size"

	// MppExchangeCompressionMode indicates the data compression method in mpp exchange operator
	MppExchangeCompressionMode = "mpp_exchange_compression_mode"

	// MppVersion indicates the mpp-version used to build mpp plan
	MppVersion = "mpp_version"

	// TiDBPessimisticTransactionFairLocking controls whether fair locking for pessimistic transaction
	// is enabled.
	TiDBPessimisticTransactionFairLocking = "tidb_pessimistic_txn_fair_locking"

	// TiDBEnablePlanCacheForParamLimit controls whether prepare statement with parameterized limit can be cached
	TiDBEnablePlanCacheForParamLimit = "tidb_enable_plan_cache_for_param_limit"

	// TiDBEnableINLJoinInnerMultiPattern indicates whether enable multi pattern for inner side of inl join
	TiDBEnableINLJoinInnerMultiPattern = "tidb_enable_inl_join_inner_multi_pattern"

	// TiFlashComputeDispatchPolicy indicates how to dispatch task to tiflash_compute nodes.
	TiFlashComputeDispatchPolicy = "tiflash_compute_dispatch_policy"

	// TiDBEnablePlanCacheForSubquery controls whether prepare statement with subquery can be cached
	TiDBEnablePlanCacheForSubquery = "tidb_enable_plan_cache_for_subquery"

	// TiDBOptEnableLateMaterialization indicates whether to enable late materialization
	TiDBOptEnableLateMaterialization = "tidb_opt_enable_late_materialization"
	// TiDBLoadBasedReplicaReadThreshold is the wait duration threshold to enable replica read automatically.
	TiDBLoadBasedReplicaReadThreshold = "tidb_load_based_replica_read_threshold"

	// TiDBOptOrderingIdxSelThresh is the threshold for optimizer to consider the ordering index.
	TiDBOptOrderingIdxSelThresh = "tidb_opt_ordering_index_selectivity_threshold"

	// TiDBOptOrderingIdxSelRatio is the ratio the optimizer will assume applies when non indexed filtering rows are found
	// via the ordering index.
	TiDBOptOrderingIdxSelRatio = "tidb_opt_ordering_index_selectivity_ratio"

	// TiDBOptEnableMPPSharedCTEExecution indicates whether the optimizer try to build shared CTE scan during MPP execution.
	TiDBOptEnableMPPSharedCTEExecution = "tidb_opt_enable_mpp_shared_cte_execution"
	// TiDBOptFixControl makes the user able to control some details of the optimizer behavior.
	TiDBOptFixControl = "tidb_opt_fix_control"

	// TiFlashReplicaRead is used to set the policy of TiFlash replica read when the query needs the TiFlash engine.
	TiFlashReplicaRead = "tiflash_replica_read"

	// TiDBLockUnchangedKeys indicates whether to lock duplicate keys in INSERT IGNORE and REPLACE statements,
	// or unchanged unique keys in UPDATE statements, see PR #42210 and #42713
	TiDBLockUnchangedKeys = "tidb_lock_unchanged_keys"

	// TiDBFastCheckTable enables fast check table.
	TiDBFastCheckTable = "tidb_enable_fast_table_check"

	// TiDBAnalyzeSkipColumnTypes indicates the column types whose statistics would not be collected when executing the ANALYZE command.
	TiDBAnalyzeSkipColumnTypes = "tidb_analyze_skip_column_types"

	// TiDBEnableCheckConstraint indicates whether to enable check constraint feature.
	TiDBEnableCheckConstraint = "tidb_enable_check_constraint"

	// TiDBOptEnableHashJoin indicates whether to enable hash join.
	TiDBOptEnableHashJoin = "tidb_opt_enable_hash_join"

	// TiDBHashJoinVersion indicates whether to use hash join implementation v2.
	TiDBHashJoinVersion = "tidb_hash_join_version"

	// TiDBOptIndexJoinBuild indicates which way to build index join.
	TiDBOptIndexJoinBuild = "tidb_opt_index_join_build_v2"

	// TiDBOptObjective indicates whether the optimizer should be more stable, predictable or more aggressive.
	// Please see comments of SessionVars.OptObjective for details.
	TiDBOptObjective = "tidb_opt_objective"

	// TiDBEnableParallelHashaggSpill is the name of the `tidb_enable_parallel_hashagg_spill` system variable
	TiDBEnableParallelHashaggSpill = "tidb_enable_parallel_hashagg_spill"

	// TiDBTxnEntrySizeLimit indicates the max size of a entry in membuf.
	TiDBTxnEntrySizeLimit = "tidb_txn_entry_size_limit"

	// TiDBSchemaCacheSize indicates the size of infoschema meta data which are cached in V2 implementation.
	TiDBSchemaCacheSize = "tidb_schema_cache_size"

	// DivPrecisionIncrement indicates the number of digits by which to increase the scale of the result of
	// division operations performed with the / operator.
	DivPrecisionIncrement = "div_precision_increment"

	// TiDBEnableSharedLockPromotion indicates whether the `select for share` statement would be executed
	// as `select for update` statements which do acquire pessimistic locks.
	TiDBEnableSharedLockPromotion = "tidb_enable_shared_lock_promotion"

	// TiDBAccelerateUserCreationUpdate decides whether tidb will load & update the whole user's data in-memory.
	TiDBAccelerateUserCreationUpdate = "tidb_accelerate_user_creation_update"
)
