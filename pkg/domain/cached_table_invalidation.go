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
	"time"

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
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	cachedTableInvalidationPullInterval = 200 * time.Millisecond
	cachedTableInvalidationBatchSize    = 256
	cachedTableInvalidationLogTable     = "table_cache_invalidation_log"
)

type cachedTableInvalidationTarget interface {
	ApplyLocalInvalidation(epoch, commitTS uint64) int
}

func normalizeCachedTableInvalidationEvent(event tablecache.CachedTableInvalidationEvent) tablecache.CachedTableInvalidationEvent {
	if event.Epoch == 0 {
		event.Epoch = event.CommitTS
	}
	return event
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

func (do *Domain) cachedTableInvalidationPullerLoop() {
	defer util.Recover(metrics.LabelDomain, "cachedTableInvalidationPullerLoop", nil, false)
	ticker := time.NewTicker(cachedTableInvalidationPullInterval)
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

		for {
			nextID, loaded, loadErr := do.pullCachedTableInvalidationEvents(lastID, cachedTableInvalidationBatchSize)
			if loadErr != nil {
				if terror.ErrorEqual(loadErr, infoschema.ErrTableNotExists) {
					break
				}
				logutil.BgLogger().Warn("pull cached-table invalidation events failed", zap.Error(loadErr))
				break
			}
			lastID = nextID
			if loaded < cachedTableInvalidationBatchSize {
				break
			}
		}
	}
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
	lastID := afterID
	for _, row := range rows {
		event := tablecache.CachedTableInvalidationEvent{
			TableID:    row.GetInt64(1),
			PhysicalID: row.GetInt64(2),
			CommitTS:   row.GetUint64(3),
			Epoch:      row.GetUint64(4),
		}
		do.applyCachedTableInvalidationEvent(event)
		lastID = row.GetUint64(0)
	}
	return lastID, len(rows), nil
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
