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
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	cachedTableInvalidationLogTable         = "table_cache_invalidation_log"
	cachedTableInvalidationNotifyKey        = "/tidb/cached-table/invalidation"
	cachedTableInvalidationLogGCTickTime    = 10 * time.Second
	cachedTableInvalidationPersistBatch     = 256
	cachedTableInvalidationPersistQueueSize = 4096
)

type cachedTableInvalidationTarget interface {
	ApplyLocalInvalidation(epoch, commitTS uint64) int
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

func normalizeCachedTableInvalidationEvent(event tablecache.CachedTableInvalidationEvent) tablecache.CachedTableInvalidationEvent {
	if event.Epoch == 0 {
		event.Epoch = event.CommitTS
	}
	return event
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
		if !ok || event.Epoch > prev.Epoch || (event.Epoch == prev.Epoch && event.CommitTS > prev.CommitTS) {
			latest[key] = event
		}
	}
	result := make([]tablecache.CachedTableInvalidationEvent, 0, len(latest))
	for _, event := range latest {
		result = append(result, event)
	}
	return result
}

func applyCachedTableInvalidationEventToTargets(event tablecache.CachedTableInvalidationEvent, targets []cachedTableInvalidationTarget) int {
	event = normalizeCachedTableInvalidationEvent(event)
	if event.Epoch == 0 {
		return 0
	}
	applied := 0
	for _, target := range targets {
		applied += target.ApplyLocalInvalidation(event.Epoch, event.CommitTS)
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

// NotifyCachedTableInvalidation publishes fast-path invalidation events to other TiDB instances.
// Polling from mysql.table_cache_invalidation_log remains the durability/recovery path.
func (do *Domain) NotifyCachedTableInvalidation(events []tablecache.CachedTableInvalidationEvent) {
	if do.etcdClient == nil || len(events) == 0 || !vardef.EnableCachedTableAsyncInvalidation.Load() {
		return
	}
	payload := cachedTableInvalidationNotifyEvent{
		ServerID: do.serverID,
		Events:   coalesceCachedTableInvalidationEvents(events),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		logutil.BgLogger().Warn("marshal cached-table invalidation notify payload failed", zap.Error(err))
		return
	}
	err = ddlutil.PutKVToEtcd(context.Background(), do.etcdClient, etcdutil.KeyOpDefaultRetryCnt, cachedTableInvalidationNotifyKey, string(data))
	if err != nil {
		logutil.BgLogger().Warn("notify cached-table invalidation failed", zap.Error(err))
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
			batch = coalesceCachedTableInvalidationEvents(batch)
			if err := do.persistCachedTableInvalidationEvents(batch); err != nil && !terror.ErrorEqual(err, infoschema.ErrTableNotExists) {
				logutil.BgLogger().Warn("persist cached-table invalidation events failed", zap.Error(err), zap.Int("events", len(batch)))
			}
		}
	}
}

func (do *Domain) enqueueCachedTableInvalidationPersist(events []tablecache.CachedTableInvalidationEvent) bool {
	if do == nil || len(events) == 0 || do.cachedTableInvalidationPersistCh == nil {
		return false
	}
	task := cachedTableInvalidationPersistTask{
		events: append([]tablecache.CachedTableInvalidationEvent(nil), coalesceCachedTableInvalidationEvents(events)...),
	}
	select {
	case <-do.exit:
		return false
	case do.cachedTableInvalidationPersistCh <- task:
		return true
	default:
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
	sqlBuilder.WriteString("INSERT HIGH_PRIORITY INTO %n.%n (table_id, physical_id, commit_ts, invalidation_epoch) VALUES ")

	args := make([]any, 0, 2+len(events)*4)
	args = append(args, mysql.SystemDB, cachedTableInvalidationLogTable)
	for i, event := range events {
		if i > 0 {
			sqlBuilder.WriteString(", ")
		}
		sqlBuilder.WriteString("(%?, %?, %?, %?)")
		args = append(args, event.TableID, event.PhysicalID, event.CommitTS, event.Epoch)
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
		"SELECT id, table_id, physical_id, commit_ts, invalidation_epoch FROM %n.%n WHERE id > %? ORDER BY id LIMIT %?",
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
		events = append(events, tablecache.CachedTableInvalidationEvent{
			TableID:    row.GetInt64(1),
			PhysicalID: row.GetInt64(2),
			CommitTS:   row.GetUint64(3),
			Epoch:      row.GetUint64(4),
		})
		lastID = row.GetUint64(0)
	}

	for _, event := range coalesceCachedTableInvalidationEvents(events) {
		do.applyCachedTableInvalidationEvent(event)
	}

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
	addTarget := func(tbl table.Table) {
		cached, ok := tbl.(table.CachedTable)
		if !ok {
			return
		}
		if _, ok = seen[cached]; ok {
			return
		}
		seen[cached] = struct{}{}
		targets = append(targets, cached)
	}

	ctx := context.Background()
	if event.PhysicalID > 0 {
		if tbl, ok := is.TableByID(ctx, event.PhysicalID); ok {
			addTarget(tbl)
		} else if tbl, _, _ := is.FindTableByPartitionID(event.PhysicalID); tbl != nil {
			addTarget(tbl)
		}
	}
	if event.TableID > 0 {
		if tbl, ok := is.TableByID(ctx, event.TableID); ok {
			addTarget(tbl)
		}
	}
	return targets
}

func (do *Domain) applyCachedTableInvalidationEvent(event tablecache.CachedTableInvalidationEvent) int {
	targets := do.resolveCachedTableInvalidationTargets(event)
	return applyCachedTableInvalidationEventToTargets(event, targets)
}
