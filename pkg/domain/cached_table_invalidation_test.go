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
	"testing"

	tablecache "github.com/pingcap/tidb/pkg/table/tables"
	"github.com/stretchr/testify/require"
)

type mockCachedTableInvalidationTarget struct {
	epoch    uint64
	commitTS uint64
	called   int
	ret      int
}

func (m *mockCachedTableInvalidationTarget) ApplyLocalInvalidation(epoch, commitTS uint64) int {
	m.epoch = epoch
	m.commitTS = commitTS
	m.called++
	return m.ret
}

func TestNormalizeCachedTableInvalidationEvent(t *testing.T) {
	event := normalizeCachedTableInvalidationEvent(tablecache.CachedTableInvalidationEvent{CommitTS: 101})
	require.Equal(t, uint64(101), event.Epoch)

	event = normalizeCachedTableInvalidationEvent(tablecache.CachedTableInvalidationEvent{CommitTS: 101, Epoch: 7})
	require.Equal(t, uint64(7), event.Epoch)
}

func TestApplyCachedTableInvalidationEvent(t *testing.T) {
	t1 := &mockCachedTableInvalidationTarget{ret: 1}
	t2 := &mockCachedTableInvalidationTarget{ret: 2}
	event := tablecache.CachedTableInvalidationEvent{CommitTS: 99}

	applied := applyCachedTableInvalidationEventToTargets(event, []cachedTableInvalidationTarget{t1, t2})
	require.Equal(t, 3, applied)
	require.Equal(t, 1, t1.called)
	require.Equal(t, 1, t2.called)
	require.Equal(t, uint64(99), t1.epoch)
	require.Equal(t, uint64(99), t2.epoch)
}

func TestApplyCachedTableInvalidationEventSkipZeroEpoch(t *testing.T) {
	t1 := &mockCachedTableInvalidationTarget{ret: 1}
	event := tablecache.CachedTableInvalidationEvent{}

	applied := applyCachedTableInvalidationEventToTargets(event, []cachedTableInvalidationTarget{t1})
	require.Equal(t, 0, applied)
	require.Equal(t, 0, t1.called)
}

func TestCoalesceCachedTableInvalidationEvents(t *testing.T) {
	events := []tablecache.CachedTableInvalidationEvent{
		{TableID: 11, PhysicalID: 11, CommitTS: 100, Epoch: 100},
		{TableID: 11, PhysicalID: 11, CommitTS: 101, Epoch: 101},
		{TableID: 12, PhysicalID: 1201, CommitTS: 90, Epoch: 0},
		{TableID: 12, PhysicalID: 1201, CommitTS: 91, Epoch: 91},
	}
	coalesced := coalesceCachedTableInvalidationEvents(events)
	require.Len(t, coalesced, 2)

	got := make(map[cachedTableInvalidationEventKey]tablecache.CachedTableInvalidationEvent, len(coalesced))
	for _, event := range coalesced {
		got[cachedTableInvalidationEventKey{tableID: event.TableID, physicalID: event.PhysicalID}] = event
	}

	require.Equal(t, uint64(101), got[cachedTableInvalidationEventKey{tableID: 11, physicalID: 11}].Epoch)
	require.Equal(t, uint64(101), got[cachedTableInvalidationEventKey{tableID: 11, physicalID: 11}].CommitTS)
	require.Equal(t, uint64(91), got[cachedTableInvalidationEventKey{tableID: 12, physicalID: 1201}].Epoch)
	require.Equal(t, uint64(91), got[cachedTableInvalidationEventKey{tableID: 12, physicalID: 1201}].CommitTS)
}

func TestBuildCachedTableInvalidationInsertSQL(t *testing.T) {
	events := []tablecache.CachedTableInvalidationEvent{
		{TableID: 11, PhysicalID: 11, CommitTS: 101, Epoch: 101},
		{TableID: 12, PhysicalID: 1201, CommitTS: 102, Epoch: 102},
	}
	sql, args := buildCachedTableInvalidationInsertSQL(events)
	require.Equal(t, "INSERT HIGH_PRIORITY INTO %n.%n (table_id, physical_id, commit_ts, invalidation_epoch) VALUES (%?, %?, %?, %?), (%?, %?, %?, %?)", sql)
	require.Equal(t, []any{"mysql", cachedTableInvalidationLogTable, int64(11), int64(11), uint64(101), uint64(101), int64(12), int64(1201), uint64(102), uint64(102)}, args)
}

func TestTryEnqueueCachedTableInvalidationPersistCopiesEvents(t *testing.T) {
	do := &Domain{
		exit:                             make(chan struct{}),
		cachedTableInvalidationPersistCh: make(chan cachedTableInvalidationPersistTask, 1),
	}
	events := []tablecache.CachedTableInvalidationEvent{
		{TableID: 11, PhysicalID: 11, CommitTS: 101, Epoch: 101},
	}
	require.True(t, do.TryEnqueueCachedTableInvalidationPersist(events))
	events[0].TableID = 999

	task := <-do.cachedTableInvalidationPersistCh
	require.Equal(t, int64(11), task.events[0].TableID)
}

func TestTryEnqueueCachedTableInvalidationPersistQueueFull(t *testing.T) {
	do := &Domain{
		exit:                             make(chan struct{}),
		cachedTableInvalidationPersistCh: make(chan cachedTableInvalidationPersistTask, 1),
	}
	require.True(t, do.TryEnqueueCachedTableInvalidationPersist([]tablecache.CachedTableInvalidationEvent{{TableID: 1}}))
	require.False(t, do.TryEnqueueCachedTableInvalidationPersist([]tablecache.CachedTableInvalidationEvent{{TableID: 2}}))
}
