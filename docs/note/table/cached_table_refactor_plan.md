# Cached Table Refactor Plan

Date: 2026-02-16

## Context

Current Cached Table in TiDB is a full-table cache with table-level invalidation and lease-based write blocking.
This has two major problems:

1. Coverage limitations:
- Partition mode is blocked.
- Many operations are blocked or degraded on cached tables (for example DDL variants, `IMPORT INTO`, stale read, pipelined DML).
- Cache is all-or-nothing; workloads with localized hotspots cannot benefit if the full table is too large.

2. Performance limitations:
- Writes are expensive because commit waits for write-lock acquisition and lease conditions.
- Any write causes coarse invalidation behavior.
- Initial cache load and refresh are full-table operations.

This note defines a staged refactor to support more scenarios and improve performance.

## Objectives

1. Add segmented cache modes in addition to full-table cache:
- `FULL`
- `RANGE`
- `HOT_RANGE`

2. Replace commit-path write blocking with asynchronous invalidation propagation to all TiDB instances.

3. Keep strict correctness guarantees for snapshot reads and transaction semantics.

4. Expand compatibility with existing SQL features in controlled rollout phases.

## Non-goals (initial phases)

1. No cross-version behavior changes without explicit compatibility gates.
2. No immediate removal of current full-table cache implementation.
3. No hard requirement to support every blocked DDL in phase 1.

## Current Behavior Summary (baseline)

Main code paths:
- Cache data and lease handling: `pkg/table/tables/cache.go`
- Remote lock state machine: `pkg/table/tables/state_remote.go`
- Commit-time write lock orchestration: `pkg/session/session.go`
- Cache eligibility and feature guardrails: `pkg/executor/builder.go`, `pkg/planner/core/*`, `pkg/ddl/executor.go`

Observed constraints:
- Full-table only.
- Table-level lock and invalidation granularity.
- Write path pays lock/lease overhead on commit.
- Several planner/executor features are explicitly blocked on cached tables.

## Target Architecture

### 1) Segmented Cache

Introduce segment-based cache entries keyed by:
- `table_id`
- `physical_id` (table or partition physical ID)
- `start_key`, `end_key`
- `epoch`
- `cached_start_ts`, `cached_end_ts` (or equivalent validity markers)
- `size_bytes`, `last_access_ts`, `hit_count`

Cache modes:
- `FULL`: one segment covering whole table or whole partition.
- `RANGE`: user-defined key ranges.
- `HOT_RANGE`: system-managed hot spans admitted via runtime access statistics.

Read path:
1. Resolve required key spans from plan/executor.
2. Probe segment index.
3. Use matching valid segments.
4. Missed spans fallback to TiKV and become admission candidates.

Admission policy for `HOT_RANGE`:
- Start with simple LFU + recency scoring.
- Enforce memory budget and per-table cap.
- Promote only when repeated access count reaches threshold.

### 2) Async Invalidation Broadcast

Replace write-path lease blocking with event-driven invalidation:

1. On write commit, record invalidation event:
- `event_id`, `table_id`, `physical_id`, `affected_spans`, `commit_ts`, `new_epoch`

2. Publish event to all TiDB instances via cluster notification channel (DDL-like fanout pattern, but not schema-version DDL per write).

3. On each TiDB instance:
- Consume event.
- Invalidate or mark stale only overlapping segments.
- Advance local table/partition epoch.

Correctness constraints:
- Reader can use segment only when segment epoch is not stale against known invalidation epoch and segment validity satisfies snapshot requirements.
- On uncertainty or lag, fallback to storage read.

### 3) Partition-aware cache metadata

Make cache state and invalidation partition-aware:
- Metadata keyed by `table_id + physical_id`.
- Allow independent cache warmup and invalidation per partition.

## Metadata and Control Plane

Add metadata for segments and invalidation log:
- `mysql.table_cache_segment_meta`
- `mysql.table_cache_invalidation_log`

Keep existing `mysql.table_cache_meta` for compatibility while migrating behavior.

Garbage collection:
- Invalidation log retention by watermark.
- Segment metadata cleanup on table/partition drop and mode changes.

## Rollout Plan

### Phase 0: Instrumentation and Guardrails

Deliverables:
1. Add metrics:
- cache hit/miss by mode and table
- segment load duration/bytes
- invalidation event lag
- invalidation queue depth
- write amplification from invalidation
2. Add failpoint tests for event lag/reorder/drop and fallback behavior.
3. Add benchmark matrix for:
- point get / batch point get
- range scan
- join with small-hot inner table
- mixed read/write contention

Exit criteria:
- Baseline numbers recorded.
- No correctness regression.

### Phase 1: Introduce segmented cache in `FULL` compatibility mode

Deliverables:
1. Internal segment abstractions and in-memory index.
2. Keep SQL surface unchanged.
3. Existing full-table behavior mapped into one segment.

Exit criteria:
- Existing cached-table tests pass with no behavior change.

### Phase 2: `RANGE` and `HOT_RANGE` mode (read path)

Deliverables:
1. Add cache mode metadata and parser/DDL plumbing for mode selection.
2. Implement range admission and hot-range admission.
3. Add memory budgeting and eviction.

Exit criteria:
- Hot-range benchmarks show improved hit rate with lower memory than full cache.

### Phase 3: Async invalidation propagation (write path)

Deliverables:
1. Invalidation event creation on writes.
2. Broadcast + subscriber handling.
3. Reader validation against epochs.
4. Keep legacy write-lock path behind compatibility flag during migration.

Exit criteria:
- Write latency reduced versus baseline under mixed workloads.
- Correctness tests pass under event lag/failure failpoints.

### Phase 4: Partition support + feature expansion

Deliverables:
1. Partition-level cache state and segmenting.
2. Relax selected restrictions currently blocked on cached tables.
3. Add managed transition mode for operations that temporarily require recache.

Exit criteria:
- Partition cached-table scenarios covered in integration tests.

## Detailed PR Slices (execution backlog)

PR-1 (now): Planning and scaffolding
1. Add this note.
2. Add TODO issue checklist in code comments and package docs where needed.
3. Add metrics design section and naming conventions.

PR-2: Segment abstraction
1. Add segment structs and interfaces in `pkg/table/tables`.
2. Implement in-memory segment index.
3. Bridge existing full-table data into segment representation.

PR-3: Read-path integration
1. Update cache probe path in executor/builder to resolve span-level probes.
2. Add miss accounting and admission hooks.

PR-4: Invalidation log + notifier
1. Add metadata table definitions and upgrade path.
2. Add invalidation event producer on write commit.
3. Add subscriber and local cache invalidator.

PR-5: Partition-aware mode
1. Physical ID based segmentation and invalidation.
2. Partition test coverage.

PR-6: Compatibility expansion
1. Re-enable selected operations behind gates.
2. Add managed fallback mode for blocked operations.

## Testing Strategy

Unit tests:
1. Segment overlap lookup, eviction, admission thresholds.
2. Epoch comparison logic and fallback behavior.
3. Invalidation ordering and idempotency.

Integration tests:
1. Existing cached-table suites remain green.
2. New suites:
- hot-range read promotion
- range overlap invalidation
- lagging subscriber fallback correctness
- partition cache invalidation independence

Concurrency and fault tests:
1. Event delay/reorder.
2. Subscriber restart.
3. Instance join/leave while writes continue.

Performance tests:
1. Compare baseline and new path for:
- P95 write latency
- read throughput
- cache memory usage
- invalidation propagation delay

## Risks and Mitigations

1. Event lag causes stale cache window.
- Mitigation: epoch validation on read and strict fallback on uncertainty.

2. Metadata growth from invalidation logs.
- Mitigation: retention GC by low-watermark.

3. Complexity increase in planner/executor.
- Mitigation: mode-gated rollout and strict compatibility path.

4. Operational surprises.
- Mitigation: session/global switches to disable segmented mode or async invalidation quickly.

## Success Criteria

1. Range/hot-range cache yields better memory efficiency than full cache for skewed workloads.
2. Mixed workload write latency improves significantly over lease-blocking baseline.
3. No correctness regressions under failpoint and integration test matrices.
4. Partitioned cached table scenario is supported.

## Implementation Progress

### 2026-02-16 (initial refactor slices)

Completed:
1. Added segment primitives and in-memory segment index in `pkg/table/tables/cache_segment.go`.
2. Added segment overlap/invalidating tests in `pkg/table/tables/cache_segment_test.go`.
3. Wired current full-table cache data path to mirror segment state in `pkg/table/tables/cache.go` using one full-table segment.
4. Added local invalidation event scaffold in `pkg/table/tables/cache_invalidation.go` and tests in `pkg/table/tables/cache_invalidation_test.go`.
5. Added epoch-aware cache validation in `TryReadFromCache` and local invalidation epoch tracking.
6. Added global feature gate `tidb_enable_cached_table_async_invalidation` and session commit-path scaffold:
   - when enabled, skip cached-table write lease waiting and apply local invalidation events after successful commit.
7. Added cached-table invalidation notifier scaffolding in `pkg/table/tables`:
   - publish/watch API with an in-memory notifier implementation.
   - session commit path now publishes invalidation events after local application.
8. Added persistent invalidation log table scaffolding:
   - new system table `mysql.table_cache_invalidation_log` in bootstrap and upgrade.
   - session commit path now appends invalidation events into the log table (best-effort).
9. Wired a domain-level invalidation replayer loop:
   - each TiDB polls `mysql.table_cache_invalidation_log` in batches and applies local cache invalidation to matching cached tables.
   - replay uses table/physical IDs and falls back to partition lookup when needed.
10. Switched invalidation epoch generation to commit-ts based ordering on write commit:
   - events now use cluster-orderable epochs for cross-instance replay correctness.
   - local monotonic fallback remains only when commit-ts is unavailable.
11. Added focused unit tests for invalidation event normalization/application helpers in `pkg/domain/cached_table_invalidation_test.go`.
12. Optimized write-path invalidation-log persistence:
   - commit path now batches multiple table invalidation events into one `INSERT ... VALUES (...), ...` statement.
13. Added replay tuning knobs for operations/performance:
   - `tidb_cached_table_invalidation_pull_interval` (ms) to tune pull latency/overhead.
   - `tidb_cached_table_invalidation_batch_size` to tune catch-up throughput.
   - domain replay loop reloads these values dynamically.
14. Optimized replay CPU path by coalescing duplicate events per `(table_id, physical_id)` within each pull batch before local invalidation.
15. Added low-latency cluster fanout fast path:
   - writer now notifies cached-table invalidation events via etcd key watch (`/tidb/cached-table/invalidation`).
   - each TiDB watches and applies remote invalidation events immediately.
   - log polling remains as durability/recovery path.
16. Added invalidation-log GC controls and background worker:
   - `tidb_cached_table_invalidation_log_keep_count` (default `0`, disabled) keeps latest N rows.
   - `tidb_cached_table_invalidation_log_gc_batch_size` controls delete batch size.
17. Added `HOT_RANGE` first-step behavior for point-get:
   - new switch `tidb_enable_cached_table_hot_range_point_get`.
   - when enabled, point-get skips full-table cache loading and caches queried row keys as tiny segments.
   - subsequent point-get on the same key can be served from cache (`ReadFromTableCache=true`).

Notes:
1. Current behavior remains full-table cache for reads; segment path is internal scaffolding.
2. Invalidation notifier currently uses in-memory implementation; cluster-wide replay now comes from log polling, while low-latency cross-instance push transport (DDL-like watch/notify) is still pending.
