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

package tables

import (
	"encoding/binary"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
)

func benchKey(i int) kv.Key {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(i))
	out := make([]byte, 9)
	out[0] = 'k'
	copy(out[1:], encoded[:])
	return kv.Key(out)
}

func buildBenchSegmentIndex(segmentCount int) *segmentIndex {
	idx := newSegmentIndex()
	for i := range segmentCount {
		start := benchKey(i * 2)
		end := benchKey(i*2 + 1)
		err := idx.upsert(cacheSegment{
			span:  keySpan{start: start, end: end},
			epoch: 1,
		})
		if err != nil {
			panic(err)
		}
	}
	return idx
}

func buildBenchCachedTable(tableID, physicalID int64, segmentCount int) *cachedTable {
	c := &cachedTable{
		TableCommon: TableCommon{tableID: tableID, physicalTableID: physicalID},
		segments:    newSegmentIndex(),
	}
	c.setCacheData(&cacheData{
		Start: 100,
		Lease: 200,
		Epoch: 1,
	}, int64(segmentCount))
	for i := range segmentCount {
		start := benchKey(i * 2)
		end := benchKey(i*2 + 1).Next()
		err := c.segments.upsert(cacheSegment{
			span:      keySpan{start: start, end: end},
			epoch:     1,
			startTS:   100,
			leaseTS:   200,
			sizeBytes: int64(len(start)),
		})
		if err != nil {
			panic(err)
		}
	}
	return c
}

func buildBenchPartitionedCachedTable(partitionCount, segmentPerPartition int) *partitionedTable {
	pt := &partitionedTable{
		partitions: make(map[int64]*partition, partitionCount),
	}
	for i := range partitionCount {
		physicalID := int64(10000 + i)
		pt.partitions[physicalID] = &partition{
			cached: buildBenchCachedTable(42, physicalID, segmentPerPartition),
		}
	}
	return pt
}

func BenchmarkSegmentIndexFindByKey(b *testing.B) {
	const segmentCount = 4096
	idx := buildBenchSegmentIndex(segmentCount)
	keys := make([]kv.Key, segmentCount)
	for i := range segmentCount {
		keys[i] = benchKey(i * 2)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok := idx.findByKey(keys[i%segmentCount])
		if !ok {
			b.Fatal("segment not found")
		}
	}
}

func BenchmarkSegmentIndexInvalidate(b *testing.B) {
	const segmentCount = 4096
	spans := []keySpan{
		{start: benchKey(segmentCount / 2), end: benchKey(segmentCount*3/2 + 1)},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		idx := buildBenchSegmentIndex(segmentCount)
		b.StartTimer()
		_ = idx.invalidate(spans, 2)
	}
}

func BenchmarkCachedTableHotRangeAdmissionThreshold(b *testing.B) {
	const keyCount = 4096
	const maxSegments = 4096
	keys := make([]kv.Key, keyCount)
	for i := range keyCount {
		keys[i] = benchKey(i)
	}

	b.Run("threshold=1", func(b *testing.B) {
		c := &cachedTable{}
		admitted := 0
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if c.shouldAdmitHotRangeKey(keys[i%keyCount], maxSegments, 1) {
				admitted++
			}
		}
		b.ReportMetric(float64(admitted)/float64(b.N), "admit/op")
	})

	b.Run("threshold=2", func(b *testing.B) {
		c := &cachedTable{}
		admitted := 0
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if c.shouldAdmitHotRangeKey(keys[i%keyCount], maxSegments, 2) {
				admitted++
			}
		}
		b.ReportMetric(float64(admitted)/float64(b.N), "admit/op")
	})
}

func BenchmarkCachedTableApplyLocalInvalidationHotRange(b *testing.B) {
	const segmentCount = 4096
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		c := buildBenchCachedTable(42, 4201, segmentCount)
		b.StartTimer()
		removed := c.ApplyLocalInvalidation(uint64(i+2), uint64(i+2))
		if removed == 0 {
			b.Fatal("invalidation removed nothing")
		}
	}
}

func BenchmarkPartitionedTableApplyLocalInvalidation(b *testing.B) {
	const partitionCount = 64
	const segmentPerPartition = 256
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		pt := buildBenchPartitionedCachedTable(partitionCount, segmentPerPartition)
		b.StartTimer()
		removed := pt.ApplyLocalInvalidation(uint64(i+2), uint64(i+2))
		if removed == 0 {
			b.Fatal("partition invalidation removed nothing")
		}
	}
}
