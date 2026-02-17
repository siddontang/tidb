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

import "sync/atomic"

// cacheInvalidationEvent models one write-side invalidation event.
// It is designed for future cross-instance fanout and currently supports local application.
type cacheInvalidationEvent struct {
	tableID    int64
	physicalID int64
	epoch      uint64
	commitTS   uint64
	spans      []keySpan
}

func (c *cachedTable) updateInvalidationEpoch(newEpoch uint64) uint64 {
	for {
		old := atomic.LoadUint64(&c.invalidationEpoch)
		if newEpoch <= old {
			return old
		}
		if atomic.CompareAndSwapUint64(&c.invalidationEpoch, old, newEpoch) {
			return newEpoch
		}
	}
}

func (c *cachedTable) applyInvalidation(event cacheInvalidationEvent) int {
	if event.tableID != 0 && event.tableID != c.tableID {
		return 0
	}
	if event.physicalID != 0 && event.physicalID != c.GetPhysicalID() {
		// Backward compatibility: older events may store logical table ID in physical_id.
		if event.physicalID != c.tableID {
			return 0
		}
	}
	currentEpoch := c.updateInvalidationEpoch(event.epoch)

	removed := 0
	if c.segments != nil {
		removed = c.segments.invalidate(event.spans, currentEpoch)
	}

	if data := c.cacheData.Load(); data != nil {
		if data.Epoch >= currentEpoch {
			c.resetHotAccessCount()
			return removed
		}
		// Full-table cache always overlaps any invalidated span.
		c.cacheData.Store(nil)
		atomic.StoreInt64(&c.totalSize, 0)
		if removed == 0 {
			removed = 1
		}
	}
	c.resetHotAccessCount()
	return removed
}
