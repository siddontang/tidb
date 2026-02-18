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
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	tablecache "github.com/pingcap/tidb/pkg/table/tables"
	"github.com/stretchr/testify/require"
)

type mockCachedTableInvalidationTarget struct {
	epoch    uint64
	commitTS uint64
	called   int
	ret      int
	ranges   []kv.KeyRange
}

func (m *mockCachedTableInvalidationTarget) ApplyLocalInvalidation(epoch, commitTS uint64) int {
	m.epoch = epoch
	m.commitTS = commitTS
	m.called++
	return m.ret
}

func (m *mockCachedTableInvalidationTarget) ApplyLocalInvalidationByRanges(epoch, commitTS uint64, ranges []kv.KeyRange) int {
	m.epoch = epoch
	m.commitTS = commitTS
	m.called++
	m.ranges = ranges
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

func TestApplyCachedTableInvalidationEventWithRanges(t *testing.T) {
	t1 := &mockCachedTableInvalidationTarget{ret: 1}
	event := tablecache.CachedTableInvalidationEvent{
		CommitTS: 99,
		Ranges: []kv.KeyRange{
			{StartKey: kv.Key("k1"), EndKey: kv.Key("k2")},
		},
	}

	applied := applyCachedTableInvalidationEventToTargets(event, []cachedTableInvalidationTarget{t1})
	require.Equal(t, 1, applied)
	require.Equal(t, 1, t1.called)
	require.Len(t, t1.ranges, 1)
	require.Equal(t, kv.Key("k1"), t1.ranges[0].StartKey)
	require.Equal(t, kv.Key("k2"), t1.ranges[0].EndKey)
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

func TestCoalesceCachedTableInvalidationEventsMergeRanges(t *testing.T) {
	events := []tablecache.CachedTableInvalidationEvent{
		{
			TableID:    11,
			PhysicalID: 11,
			CommitTS:   100,
			Epoch:      100,
			Ranges: []kv.KeyRange{
				{StartKey: kv.Key("k1"), EndKey: kv.Key("k2")},
			},
		},
		{
			TableID:    11,
			PhysicalID: 11,
			CommitTS:   101,
			Epoch:      101,
			Ranges: []kv.KeyRange{
				{StartKey: kv.Key("k3"), EndKey: kv.Key("k4")},
			},
		},
	}
	coalesced := coalesceCachedTableInvalidationEvents(events)
	require.Len(t, coalesced, 1)
	require.Equal(t, uint64(101), coalesced[0].Epoch)
	require.Len(t, coalesced[0].Ranges, 2)
}

func TestCoalesceCachedTableInvalidationEventsFullInvalidationWins(t *testing.T) {
	events := []tablecache.CachedTableInvalidationEvent{
		{
			TableID:    11,
			PhysicalID: 11,
			CommitTS:   100,
			Epoch:      100,
			Ranges: []kv.KeyRange{
				{StartKey: kv.Key("k1"), EndKey: kv.Key("k2")},
			},
		},
		{
			TableID:    11,
			PhysicalID: 11,
			CommitTS:   101,
			Epoch:      101,
		},
	}
	coalesced := coalesceCachedTableInvalidationEvents(events)
	require.Len(t, coalesced, 1)
	require.Len(t, coalesced[0].Ranges, 0)
}

func TestEncodeDecodeCachedTableInvalidationRanges(t *testing.T) {
	encoded, err := encodeCachedTableInvalidationRanges([]kv.KeyRange{
		{StartKey: kv.Key("k1"), EndKey: kv.Key("k2")},
	})
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	var raw []map[string]string
	require.NoError(t, json.Unmarshal(encoded, &raw))

	decoded, decodeErr := decodeCachedTableInvalidationRanges(encoded)
	require.NoError(t, decodeErr)
	require.Len(t, decoded, 1)
	require.Equal(t, kv.Key("k1"), decoded[0].StartKey)
	require.Equal(t, kv.Key("k2"), decoded[0].EndKey)
}

func TestBuildCachedTableInvalidationInsertSQL(t *testing.T) {
	events := []tablecache.CachedTableInvalidationEvent{
		{TableID: 11, PhysicalID: 11, CommitTS: 101, Epoch: 101},
		{
			TableID:    12,
			PhysicalID: 1201,
			CommitTS:   102,
			Epoch:      102,
			Ranges: []kv.KeyRange{
				{StartKey: kv.Key("k1"), EndKey: kv.Key("k2")},
			},
		},
	}
	sql, args := buildCachedTableInvalidationInsertSQL(events)
	require.Equal(t, "INSERT HIGH_PRIORITY INTO %n.%n (table_id, physical_id, commit_ts, invalidation_epoch, invalidation_ranges) VALUES (%?, %?, %?, %?, %?), (%?, %?, %?, %?, %?)", sql)
	require.Len(t, args, 12)
	require.Equal(t, []any{"mysql", cachedTableInvalidationLogTable, int64(11), int64(11), uint64(101), uint64(101), []byte(nil), int64(12), int64(1201), uint64(102), uint64(102)}, args[:11])
	encoded, ok := args[11].([]byte)
	require.True(t, ok)
	decoded, decodeErr := decodeCachedTableInvalidationRanges(encoded)
	require.NoError(t, decodeErr)
	require.Len(t, decoded, 1)
	require.Equal(t, kv.Key("k1"), decoded[0].StartKey)
	require.Equal(t, kv.Key("k2"), decoded[0].EndKey)
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

func TestTryEnqueueCachedTableInvalidationPersistCoalesces(t *testing.T) {
	do := &Domain{
		exit:                             make(chan struct{}),
		cachedTableInvalidationPersistCh: make(chan cachedTableInvalidationPersistTask, 1),
	}
	events := []tablecache.CachedTableInvalidationEvent{
		{TableID: 11, PhysicalID: 11, CommitTS: 100, Epoch: 100},
		{TableID: 11, PhysicalID: 11, CommitTS: 101, Epoch: 101},
	}
	require.True(t, do.TryEnqueueCachedTableInvalidationPersist(events))
	task := <-do.cachedTableInvalidationPersistCh
	require.Len(t, task.events, 1)
	require.Equal(t, uint64(101), task.events[0].Epoch)
}

func TestTryEnqueueCachedTableInvalidationPersistQueueFull(t *testing.T) {
	do := &Domain{
		exit:                             make(chan struct{}),
		cachedTableInvalidationPersistCh: make(chan cachedTableInvalidationPersistTask, 1),
	}
	require.True(t, do.TryEnqueueCachedTableInvalidationPersist([]tablecache.CachedTableInvalidationEvent{{TableID: 1}}))
	require.False(t, do.TryEnqueueCachedTableInvalidationPersist([]tablecache.CachedTableInvalidationEvent{{TableID: 2}}))
}

func TestEnqueueCachedTableInvalidationNotifyCopiesEvents(t *testing.T) {
	do := &Domain{
		exit:                            make(chan struct{}),
		cachedTableInvalidationNotifyCh: make(chan cachedTableInvalidationNotifyTask, 1),
	}
	events := []tablecache.CachedTableInvalidationEvent{
		{TableID: 11, PhysicalID: 11, CommitTS: 101, Epoch: 101},
	}
	require.True(t, do.enqueueCachedTableInvalidationNotify(events))
	events[0].TableID = 999

	task := <-do.cachedTableInvalidationNotifyCh
	require.Equal(t, int64(11), task.events[0].TableID)
}

func TestEnqueueCachedTableInvalidationNotifyCoalesces(t *testing.T) {
	do := &Domain{
		exit:                            make(chan struct{}),
		cachedTableInvalidationNotifyCh: make(chan cachedTableInvalidationNotifyTask, 1),
	}
	events := []tablecache.CachedTableInvalidationEvent{
		{TableID: 11, PhysicalID: 11, CommitTS: 100, Epoch: 100},
		{TableID: 11, PhysicalID: 11, CommitTS: 101, Epoch: 101},
	}
	require.True(t, do.enqueueCachedTableInvalidationNotify(events))
	task := <-do.cachedTableInvalidationNotifyCh
	require.Len(t, task.events, 1)
	require.Equal(t, uint64(101), task.events[0].Epoch)
}

func TestEnqueueCachedTableInvalidationNotifyQueueFull(t *testing.T) {
	do := &Domain{
		exit:                            make(chan struct{}),
		cachedTableInvalidationNotifyCh: make(chan cachedTableInvalidationNotifyTask, 1),
	}
	require.True(t, do.enqueueCachedTableInvalidationNotify([]tablecache.CachedTableInvalidationEvent{{TableID: 1}}))
	require.False(t, do.enqueueCachedTableInvalidationNotify([]tablecache.CachedTableInvalidationEvent{{TableID: 2}}))
}

func BenchmarkCoalesceCachedTableInvalidationEvents(b *testing.B) {
	const keyCount = 128
	events := make([]tablecache.CachedTableInvalidationEvent, 0, keyCount*8)
	for i := range keyCount * 8 {
		key := int64(i % keyCount)
		epoch := uint64(1000 + i)
		events = append(events, tablecache.CachedTableInvalidationEvent{
			TableID:    key,
			PhysicalID: key,
			Epoch:      epoch,
			CommitTS:   epoch,
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		coalesced := coalesceCachedTableInvalidationEvents(events)
		if len(coalesced) != keyCount {
			b.Fatalf("unexpected coalesced size: %d", len(coalesced))
		}
	}
}

func BenchmarkBuildCachedTableInvalidationInsertSQL(b *testing.B) {
	const eventCount = 256
	events := make([]tablecache.CachedTableInvalidationEvent, 0, eventCount)
	for i := range eventCount {
		events = append(events, tablecache.CachedTableInvalidationEvent{
			TableID:    int64(1000 + i),
			PhysicalID: int64(1000 + i),
			Epoch:      uint64(2000 + i),
			CommitTS:   uint64(2000 + i),
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sql, args := buildCachedTableInvalidationInsertSQL(events)
		if len(sql) == 0 || len(args) != 2+eventCount*5 {
			b.Fatalf("unexpected sql build output: sqlLen=%d args=%d", len(sql), len(args))
		}
	}
}

func BenchmarkEnqueueCachedTableInvalidationNotify(b *testing.B) {
	do := &Domain{
		exit:                            make(chan struct{}),
		cachedTableInvalidationNotifyCh: make(chan cachedTableInvalidationNotifyTask, 1),
	}
	events := []tablecache.CachedTableInvalidationEvent{
		{TableID: 11, PhysicalID: 11, CommitTS: 100, Epoch: 100},
		{TableID: 11, PhysicalID: 11, CommitTS: 101, Epoch: 101},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !do.enqueueCachedTableInvalidationNotify(events) {
			b.Fatal("enqueue failed")
		}
		<-do.cachedTableInvalidationNotifyCh
	}
}
