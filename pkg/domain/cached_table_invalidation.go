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

package domain

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	tablecache "github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	etcdutil "github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/oracle"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	cachedTableInvalidationLogTable         = "table_cache_invalidation_log"
	cachedTableInvalidationNotifyKey        = "/tidb/cached-table/invalidation"
	cachedTableInvalidationLogGCTickTime    = 10 * time.Second
	cachedTableInvalidationPersistBatch     = 256
	cachedTableInvalidationPersistQueueSize = 4096
	cachedTableInvalidationNotifyBatch      = 256
	cachedTableInvalidationNotifyQueueSize  = 4096
	cachedTableInvalidationCoalesceRangeCap = 1024
	cachedTableInvalidationTypeFull         = "full"
	cachedTableInvalidationTypeRange        = "range"
	cachedTableInvalidationQueueTypeNotify  = "notify"
	cachedTableInvalidationQueueTypePersist = "persist"
	cachedTableInvalidationLagSourceNotify  = "notify"
	cachedTableInvalidationLagSourcePull    = "pull"
)

type cachedTableInvalidationTarget interface {
	ApplyLocalInvalidation(epoch, commitTS uint64) int
}

type cachedTableRangeInvalidationTarget interface {
	ApplyLocalInvalidationByRanges(epoch, commitTS uint64, ranges []kv.KeyRange) int
}

type cachedTableInvalidationEventKey struct {
	tableID    int64
	physicalID int64
}

type cachedTableInvalidationNotifyEvent struct {
	ServerID uint64                                    `json:"server_id"`
	Events   []tablecache.CachedTableInvalidationEvent `json:"events"`
}

type cachedTableInvalidationPersistTask struct {
	events []tablecache.CachedTableInvalidationEvent
}

type cachedTableInvalidationNotifyTask struct {
	events []tablecache.CachedTableInvalidationEvent
}

func normalizeCachedTableInvalidationEvent(event tablecache.CachedTableInvalidationEvent) tablecache.CachedTableInvalidationEvent {
	if event.Epoch == 0 {
		event.Epoch = event.CommitTS
	}
	return event
}

func cloneCachedTableInvalidationRanges(ranges []kv.KeyRange) []kv.KeyRange {
	if len(ranges) == 0 {
		return nil
	}
	out := make([]kv.KeyRange, 0, len(ranges))
	for _, keyRange := range ranges {
		out = append(out, kv.KeyRange{
			StartKey: keyRange.StartKey.Clone(),
			EndKey:   keyRange.EndKey.Clone(),
		})
	}
	return out
}

func cloneCachedTableInvalidationEvents(events []tablecache.CachedTableInvalidationEvent) []tablecache.CachedTableInvalidationEvent {
	if len(events) == 0 {
		return nil
	}
	out := make([]tablecache.CachedTableInvalidationEvent, 0, len(events))
	for _, event := range events {
		event = normalizeCachedTableInvalidationEvent(event)
		event.Ranges = cloneCachedTableInvalidationRanges(event.Ranges)
		out = append(out, event)
	}
	return out
}

func observeCachedTableInvalidationQueueSize(queueType string, queueLen int) {
	metrics.CachedTableInvalidationQueueSize.WithLabelValues(queueType).Set(float64(queueLen))
}

func observeCachedTableInvalidationLag(events []tablecache.CachedTableInvalidationEvent, source string) {
	if len(events) == 0 {
		return
	}
	now := time.Now()
	maxLagSeconds := 0.0
	for _, event := range events {
		if event.CommitTS == 0 {
			continue
		}
		lagSeconds := now.Sub(oracle.GetTimeFromTS(event.CommitTS)).Seconds()
		if lagSeconds < 0 {
			lagSeconds = 0
		}
		if lagSeconds > maxLagSeconds {
			maxLagSeconds = lagSeconds
		}
	}
	metrics.CachedTableInvalidationLagGauge.WithLabelValues(source).Set(maxLagSeconds)
}

func mergeCachedTableInvalidationRanges(left, right []kv.KeyRange) []kv.KeyRange {
	if len(left) == 0 || len(right) == 0 {
		// Empty means full-table invalidation.
		return nil
	}
	total := len(left) + len(right)
	if total > cachedTableInvalidationCoalesceRangeCap {
		// Degrade to full invalidation when merged ranges are too many.
		return nil
	}
	merged := make([]kv.KeyRange, 0, total)
	merged = append(merged, cloneCachedTableInvalidationRanges(left)...)
	merged = append(merged, cloneCachedTableInvalidationRanges(right)...)
	return merged
}

func coalesceCachedTableInvalidationEvents(events []tablecache.CachedTableInvalidationEvent) []tablecache.CachedTableInvalidationEvent {
	if len(events) <= 1 {
		return events
	}
	latest := make(map[cachedTableInvalidationEventKey]tablecache.CachedTableInvalidationEvent, len(events))
	for _, event := range events {
		event = normalizeCachedTableInvalidationEvent(event)
		key := cachedTableInvalidationEventKey{
			tableID:    event.TableID,
			physicalID: event.PhysicalID,
		}
		prev, ok := latest[key]
		if !ok {
			event.Ranges = cloneCachedTableInvalidationRanges(event.Ranges)
			latest[key] = event
			continue
		}
		mergedRanges := mergeCachedTableInvalidationRanges(prev.Ranges, event.Ranges)
		if event.Epoch > prev.Epoch || (event.Epoch == prev.Epoch && event.CommitTS > prev.CommitTS) {
			event.Ranges = mergedRanges
			latest[key] = event
			continue
		}
		prev.Ranges = mergedRanges
		latest[key] = prev
	}
	result := make([]tablecache.CachedTableInvalidationEvent, 0, len(latest))
	for _, event := range latest {
		result = append(result, event)
	}
	return result
}

func encodeCachedTableInvalidationRanges(ranges []kv.KeyRange) ([]byte, error) {
	if len(ranges) == 0 {
		return nil, nil
	}
	return json.Marshal(ranges)
}

func decodeCachedTableInvalidationRanges(raw []byte) ([]kv.KeyRange, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var ranges []kv.KeyRange
	if err := json.Unmarshal(raw, &ranges); err != nil {
		return nil, err
	}
	return ranges, nil
}

func applyCachedTableInvalidationEventToTargets(event tablecache.CachedTableInvalidationEvent, targets []cachedTableInvalidationTarget) int {
	event = normalizeCachedTableInvalidationEvent(event)
	if event.Epoch == 0 {
		return 0
	}
	eventType := cachedTableInvalidationTypeFull
	if len(event.Ranges) > 0 {
		eventType = cachedTableInvalidationTypeRange
	}
	applied := 0
	for _, target := range targets {
		if len(event.Ranges) > 0 {
			if rangeTarget, ok := target.(cachedTableRangeInvalidationTarget); ok {
				applied += rangeTarget.ApplyLocalInvalidationByRanges(event.Epoch, event.CommitTS, event.Ranges)
				continue
			}
		}
		applied += target.ApplyLocalInvalidation(event.Epoch, event.CommitTS)
	}
	metrics.CachedTableInvalidationEventCounter.WithLabelValues(eventType).Inc()
	if applied > 0 {
		metrics.CachedTableInvalidationApplyCounter.WithLabelValues(eventType).Add(float64(applied))
	}
	return applied
}

func (do *Domain) applyCachedTableInvalidationEvents(events []tablecache.CachedTableInvalidationEvent) int {
	applied := 0
	for _, event := range coalesceCachedTableInvalidationEvents(events) {
		applied += do.applyCachedTableInvalidationEvent(event)
	}
	return applied
}

// ApplyCachedTableInvalidationEvents applies invalidation events to local cached-table targets.
func (do *Domain) ApplyCachedTableInvalidationEvents(events []tablecache.CachedTableInvalidationEvent) int {
	if do == nil || len(events) == 0 {
		return 0
	}
	return do.applyCachedTableInvalidationEvents(events)
}

// NotifyCachedTableInvalidation publishes fast-path invalidation events to other TiDB instances.
// Polling from mysql.table_cache_invalidation_log remains the durability/recovery path.
func (do *Domain) NotifyCachedTableInvalidation(events []tablecache.CachedTableInvalidationEvent) {
	if do == nil || do.etcdClient == nil || len(events) == 0 || !vardef.EnableCachedTableAsyncInvalidation.Load() {
		return
	}
	if do.enqueueCachedTableInvalidationNotify(events) {
		return
	}
	logutil.BgLogger().Warn("enqueue cached-table invalidation notify failed, fallback to log-puller path", zap.Int("events", len(events)))
}

func (do *Domain) notifyCachedTableInvalidationEvents(events []tablecache.CachedTableInvalidationEvent) error {
	payload := cachedTableInvalidationNotifyEvent{
		ServerID: do.serverID,
		Events:   events,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return ddlutil.PutKVToEtcd(context.Background(), do.etcdClient, etcdutil.KeyOpDefaultRetryCnt, cachedTableInvalidationNotifyKey, string(data))
}

func (do *Domain) enqueueCachedTableInvalidationNotify(events []tablecache.CachedTableInvalidationEvent) bool {
	if do == nil || len(events) == 0 || do.cachedTableInvalidationNotifyCh == nil {
		return false
	}
	task := cachedTableInvalidationNotifyTask{
		events: cloneCachedTableInvalidationEvents(events),
	}
	select {
	case <-do.exit:
		return false
	case do.cachedTableInvalidationNotifyCh <- task:
		observeCachedTableInvalidationQueueSize(cachedTableInvalidationQueueTypeNotify, len(do.cachedTableInvalidationNotifyCh))
		return true
	default:
		observeCachedTableInvalidationQueueSize(cachedTableInvalidationQueueTypeNotify, len(do.cachedTableInvalidationNotifyCh))
		return false
	}
}

func (do *Domain) cachedTableInvalidationWatchLoop() {
	defer util.Recover(metrics.LabelDomain, "cachedTableInvalidationWatchLoop", nil, false)
	defer logutil.BgLogger().Info("cachedTableInvalidationWatchLoop exited")
	if do.etcdClient == nil {
		return
	}
	watchCh := do.etcdClient.Watch(context.Background(), cachedTableInvalidationNotifyKey)
	var retry int
	for {
		select {
		case <-do.exit:
			return
		case resp, ok := <-watchCh:
			if !ok {
				watchCh = do.etcdClient.Watch(context.Background(), cachedTableInvalidationNotifyKey)
				retry++
				if retry > 10 {
					time.Sleep(time.Duration(retry) * time.Second)
				}
				continue
			}
			retry = 0
			do.applyCachedTableInvalidationNotifyResponse(resp)
		}
	}
}

func (do *Domain) applyCachedTableInvalidationNotifyResponse(resp clientv3.WatchResponse) {
	if err := resp.Err(); err != nil {
		logutil.BgLogger().Warn("watch cached-table invalidation failed", zap.Error(err))
		return
	}
	for _, raw := range resp.Events {
		if raw == nil || raw.Kv == nil || len(raw.Kv.Value) == 0 {
			continue
		}
		var payload cachedTableInvalidationNotifyEvent
		if err := json.Unmarshal(raw.Kv.Value, &payload); err != nil {
			logutil.BgLogger().Warn("decode cached-table invalidation event failed", zap.Error(err))
			continue
		}
		if payload.ServerID == do.serverID || len(payload.Events) == 0 {
			continue
		}
		observeCachedTableInvalidationLag(payload.Events, cachedTableInvalidationLagSourceNotify)
		do.applyCachedTableInvalidationEvents(payload.Events)
	}
}

func (do *Domain) cachedTableInvalidationPullerLoop() {
	defer util.Recover(metrics.LabelDomain, "cachedTableInvalidationPullerLoop", nil, false)
	pullInterval := loadCachedTableInvalidationPullInterval()
	ticker := time.NewTicker(pullInterval)
	defer func() {
		ticker.Stop()
		logutil.BgLogger().Info("cachedTableInvalidationPullerLoop exited")
	}()

	lastID, err := do.getLatestCachedTableInvalidationLogID()
	if err != nil && !terror.ErrorEqual(err, infoschema.ErrTableNotExists) {
		logutil.BgLogger().Warn("failed to initialize cached-table invalidation checkpoint", zap.Error(err))
	}

	for {
		select {
		case <-do.exit:
			return
		case <-ticker.C:
		}

		if !vardef.EnableCachedTableAsyncInvalidation.Load() {
			continue
		}

		latestPullInterval := loadCachedTableInvalidationPullInterval()
		if latestPullInterval != pullInterval {
			ticker.Reset(latestPullInterval)
			pullInterval = latestPullInterval
		}

		batchSize := loadCachedTableInvalidationBatchSize()
		for {
			nextID, loaded, loadErr := do.pullCachedTableInvalidationEvents(lastID, batchSize)
			if loadErr != nil {
				if terror.ErrorEqual(loadErr, infoschema.ErrTableNotExists) {
					break
				}
				logutil.BgLogger().Warn("pull cached-table invalidation events failed", zap.Error(loadErr))
				break
			}
			lastID = nextID
			if loaded < batchSize {
				break
			}
		}
	}
}

func (do *Domain) cachedTableInvalidationLogGCLoop() {
	defer util.Recover(metrics.LabelDomain, "cachedTableInvalidationLogGCLoop", nil, false)
	ticker := time.NewTicker(cachedTableInvalidationLogGCTickTime)
	defer func() {
		ticker.Stop()
		logutil.BgLogger().Info("cachedTableInvalidationLogGCLoop exited")
	}()

	for {
		select {
		case <-do.exit:
			return
		case <-ticker.C:
		}

		if !vardef.EnableCachedTableAsyncInvalidation.Load() {
			continue
		}
		keepCount := loadCachedTableInvalidationLogKeepCount()
		if keepCount == 0 {
			continue
		}
		maxID, err := do.getLatestCachedTableInvalidationLogID()
		if err != nil {
			if !terror.ErrorEqual(err, infoschema.ErrTableNotExists) {
				logutil.BgLogger().Warn("read cached-table invalidation log max id failed", zap.Error(err))
			}
			continue
		}
		if maxID <= keepCount {
			continue
		}
		cutoff := maxID - keepCount
		if err := do.gcCachedTableInvalidationLogByID(cutoff, loadCachedTableInvalidationLogGCBatchSize()); err != nil && !terror.ErrorEqual(err, infoschema.ErrTableNotExists) {
			logutil.BgLogger().Warn("cached-table invalidation log gc failed", zap.Error(err), zap.Uint64("cutoffID", cutoff))
		}
	}
}

func (do *Domain) cachedTableInvalidationPersistLoop() {
	defer util.Recover(metrics.LabelDomain, "cachedTableInvalidationPersistLoop", nil, false)
	defer logutil.BgLogger().Info("cachedTableInvalidationPersistLoop exited")
	if do.cachedTableInvalidationPersistCh == nil {
		return
	}

	for {
		select {
		case <-do.exit:
			return
		case task := <-do.cachedTableInvalidationPersistCh:
			observeCachedTableInvalidationQueueSize(cachedTableInvalidationQueueTypePersist, len(do.cachedTableInvalidationPersistCh))
			batch := make([]tablecache.CachedTableInvalidationEvent, 0, cachedTableInvalidationPersistBatch)
			batch = append(batch, task.events...)
		drain:
			for len(batch) < cachedTableInvalidationPersistBatch {
				select {
				case next := <-do.cachedTableInvalidationPersistCh:
					batch = append(batch, next.events...)
				default:
					break drain
				}
			}
			observeCachedTableInvalidationQueueSize(cachedTableInvalidationQueueTypePersist, len(do.cachedTableInvalidationPersistCh))
			batch = coalesceCachedTableInvalidationEvents(batch)
			if err := do.persistCachedTableInvalidationEvents(batch); err != nil && !terror.ErrorEqual(err, infoschema.ErrTableNotExists) {
				logutil.BgLogger().Warn("persist cached-table invalidation events failed", zap.Error(err), zap.Int("events", len(batch)))
			}
		}
	}
}

func (do *Domain) cachedTableInvalidationNotifyLoop() {
	defer util.Recover(metrics.LabelDomain, "cachedTableInvalidationNotifyLoop", nil, false)
	defer logutil.BgLogger().Info("cachedTableInvalidationNotifyLoop exited")
	if do.cachedTableInvalidationNotifyCh == nil || do.etcdClient == nil {
		return
	}

	for {
		select {
		case <-do.exit:
			return
		case task := <-do.cachedTableInvalidationNotifyCh:
			observeCachedTableInvalidationQueueSize(cachedTableInvalidationQueueTypeNotify, len(do.cachedTableInvalidationNotifyCh))
			batch := make([]tablecache.CachedTableInvalidationEvent, 0, cachedTableInvalidationNotifyBatch)
			batch = append(batch, task.events...)
		drain:
			for len(batch) < cachedTableInvalidationNotifyBatch {
				select {
				case next := <-do.cachedTableInvalidationNotifyCh:
					batch = append(batch, next.events...)
				default:
					break drain
				}
			}
			observeCachedTableInvalidationQueueSize(cachedTableInvalidationQueueTypeNotify, len(do.cachedTableInvalidationNotifyCh))
			batch = coalesceCachedTableInvalidationEvents(batch)
			if err := do.notifyCachedTableInvalidationEvents(batch); err != nil {
				logutil.BgLogger().Warn("notify cached-table invalidation failed", zap.Error(err), zap.Int("events", len(batch)))
			}
		}
	}
}

func (do *Domain) enqueueCachedTableInvalidationPersist(events []tablecache.CachedTableInvalidationEvent) bool {
	if do == nil || len(events) == 0 || do.cachedTableInvalidationPersistCh == nil {
		return false
	}
	task := cachedTableInvalidationPersistTask{
		events: cloneCachedTableInvalidationEvents(events),
	}
	select {
	case <-do.exit:
		return false
	case do.cachedTableInvalidationPersistCh <- task:
		observeCachedTableInvalidationQueueSize(cachedTableInvalidationQueueTypePersist, len(do.cachedTableInvalidationPersistCh))
		return true
	default:
		observeCachedTableInvalidationQueueSize(cachedTableInvalidationQueueTypePersist, len(do.cachedTableInvalidationPersistCh))
		return false
	}
}

// TryEnqueueCachedTableInvalidationPersist tries queueing invalidation events for async persistence.
// It returns false when queue is unavailable/full so caller can fallback to synchronous persistence.
func (do *Domain) TryEnqueueCachedTableInvalidationPersist(events []tablecache.CachedTableInvalidationEvent) bool {
	return do.enqueueCachedTableInvalidationPersist(events)
}

// PersistCachedTableInvalidation persists invalidation events synchronously.
func (do *Domain) PersistCachedTableInvalidation(events []tablecache.CachedTableInvalidationEvent) {
	if do == nil || len(events) == 0 {
		return
	}
	err := do.persistCachedTableInvalidationEvents(coalesceCachedTableInvalidationEvents(events))
	if err == nil || terror.ErrorEqual(err, infoschema.ErrTableNotExists) {
		return
	}
	logutil.BgLogger().Warn("persist cached-table invalidation events failed", zap.Error(err), zap.Int("events", len(events)))
}

func (do *Domain) persistCachedTableInvalidationEvents(events []tablecache.CachedTableInvalidationEvent) error {
	if len(events) == 0 {
		return nil
	}
	sql, args := buildCachedTableInvalidationInsertSQL(events)
	_, err := do.execCachedTableInvalidationRestrictedSQL(sql, args...)
	return err
}

func buildCachedTableInvalidationInsertSQL(events []tablecache.CachedTableInvalidationEvent) (string, []any) {
	var sqlBuilder strings.Builder
	sqlBuilder.Grow(128 + len(events)*24)
	sqlBuilder.WriteString("INSERT HIGH_PRIORITY INTO %n.%n (table_id, physical_id, commit_ts, invalidation_epoch, invalidation_ranges) VALUES ")

	args := make([]any, 0, 2+len(events)*5)
	args = append(args, mysql.SystemDB, cachedTableInvalidationLogTable)
	for i, event := range events {
		if i > 0 {
			sqlBuilder.WriteString(", ")
		}
		sqlBuilder.WriteString("(%?, %?, %?, %?, %?)")
		encodedRanges, err := encodeCachedTableInvalidationRanges(event.Ranges)
		if err != nil {
			encodedRanges = nil
		}
		args = append(args, event.TableID, event.PhysicalID, event.CommitTS, event.Epoch, encodedRanges)
	}
	return sqlBuilder.String(), args
}

func loadCachedTableInvalidationPullInterval() time.Duration {
	ms := vardef.CachedTableInvalidationPullInterval.Load()
	if ms <= 0 {
		ms = vardef.DefTiDBCachedTableInvalidationPullInterval
	}
	return time.Duration(ms) * time.Millisecond
}

func loadCachedTableInvalidationBatchSize() int {
	size := vardef.CachedTableInvalidationBatchSize.Load()
	if size <= 0 {
		size = vardef.DefTiDBCachedTableInvalidationBatchSize
	}
	return int(size)
}

func loadCachedTableInvalidationLogKeepCount() uint64 {
	size := vardef.CachedTableInvalidationLogKeepCount.Load()
	if size <= 0 {
		return 0
	}
	return uint64(size)
}

func loadCachedTableInvalidationLogGCBatchSize() int {
	size := vardef.CachedTableInvalidationLogGCBatchSize.Load()
	if size <= 0 {
		size = vardef.DefTiDBCachedTableInvalidationLogGCBatchSize
	}
	return int(size)
}

func (do *Domain) getLatestCachedTableInvalidationLogID() (uint64, error) {
	rows, err := do.execCachedTableInvalidationRestrictedSQL(
		"SELECT IFNULL(MAX(id), 0) FROM %n.%n",
		mysql.SystemDB,
		cachedTableInvalidationLogTable,
	)
	if err != nil || len(rows) == 0 {
		return 0, err
	}
	return rows[0].GetUint64(0), nil
}

func (do *Domain) pullCachedTableInvalidationEvents(afterID uint64, limit int) (uint64, int, error) {
	rows, err := do.execCachedTableInvalidationRestrictedSQL(
		"SELECT id, table_id, physical_id, commit_ts, invalidation_epoch, invalidation_ranges FROM %n.%n WHERE id > %? ORDER BY id LIMIT %?",
		mysql.SystemDB,
		cachedTableInvalidationLogTable,
		afterID,
		limit,
	)
	if err != nil {
		return afterID, 0, err
	}

	events := make([]tablecache.CachedTableInvalidationEvent, 0, len(rows))
	lastID := afterID
	for _, row := range rows {
		var ranges []kv.KeyRange
		if !row.IsNull(5) {
			decoded, decodeErr := decodeCachedTableInvalidationRanges(row.GetBytes(5))
			if decodeErr != nil {
				logutil.BgLogger().Warn("decode cached-table invalidation ranges failed, fallback to full invalidation", zap.Error(decodeErr))
			} else {
				ranges = decoded
			}
		}
		events = append(events, tablecache.CachedTableInvalidationEvent{
			TableID:    row.GetInt64(1),
			PhysicalID: row.GetInt64(2),
			CommitTS:   row.GetUint64(3),
			Epoch:      row.GetUint64(4),
			Ranges:     ranges,
		})
		lastID = row.GetUint64(0)
	}

	for _, event := range coalesceCachedTableInvalidationEvents(events) {
		do.applyCachedTableInvalidationEvent(event)
	}
	observeCachedTableInvalidationLag(events, cachedTableInvalidationLagSourcePull)

	return lastID, len(rows), nil
}

func (do *Domain) gcCachedTableInvalidationLogByID(cutoffID uint64, limit int) error {
	_, err := do.execCachedTableInvalidationRestrictedSQL(
		"DELETE FROM %n.%n WHERE id <= %? ORDER BY id LIMIT %?",
		mysql.SystemDB,
		cachedTableInvalidationLogTable,
		cutoffID,
		limit,
	)
	return err
}

func (do *Domain) execCachedTableInvalidationRestrictedSQL(sql string, args ...any) ([]chunk.Row, error) {
	se, err := do.sysSessionPool.Get()
	if err != nil {
		return nil, err
	}
	defer do.sysSessionPool.Put(se)

	sctx := se.(sessionctx.Context)
	exec := sctx.GetRestrictedSQLExecutor()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnCacheTable)
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, sql, args...)
	return rows, err
}

func (do *Domain) resolveCachedTableInvalidationTargets(event tablecache.CachedTableInvalidationEvent) []cachedTableInvalidationTarget {
	is := do.InfoSchema()
	if is == nil {
		return nil
	}

	seen := make(map[table.CachedTable]struct{}, 2)
	targets := make([]cachedTableInvalidationTarget, 0, 2)
	addTarget := func(tbl table.Table) bool {
		cached, ok := tbl.(table.CachedTable)
		if !ok {
			return false
		}
		if _, ok = seen[cached]; ok {
			return false
		}
		seen[cached] = struct{}{}
		targets = append(targets, cached)
		return true
	}

	ctx := context.Background()
	resolvedPhysicalTarget := false
	if event.PhysicalID > 0 {
		if tbl, ok := is.TableByID(ctx, event.PhysicalID); ok {
			resolvedPhysicalTarget = addTarget(tbl) || resolvedPhysicalTarget
		} else if tbl, _, _ := is.FindTableByPartitionID(event.PhysicalID); tbl != nil {
			addedPartition := false
			if pt := tbl.GetPartitionedTable(); pt != nil {
				if part := pt.GetPartition(event.PhysicalID); part != nil {
					addedPartition = addTarget(part)
				}
			}
			if !addedPartition {
				addedPartition = addTarget(tbl)
			}
			resolvedPhysicalTarget = addedPartition || resolvedPhysicalTarget
		}
	}
	if event.TableID > 0 {
		shouldResolveTableID := event.PhysicalID == 0 || event.PhysicalID == event.TableID || !resolvedPhysicalTarget
		if shouldResolveTableID {
			if tbl, ok := is.TableByID(ctx, event.TableID); ok {
				addTarget(tbl)
			}
		}
	}
	return targets
}

func (do *Domain) applyCachedTableInvalidationEvent(event tablecache.CachedTableInvalidationEvent) int {
	targets := do.resolveCachedTableInvalidationTargets(event)
	return applyCachedTableInvalidationEventToTargets(event, targets)
}
