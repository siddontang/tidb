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

func (c *cachedTable) applyInvalidation(event cacheInvalidationEvent) int {
	if event.tableID != 0 && event.tableID != c.tableID {
		return 0
	}

	removed := 0
	if c.segments != nil {
		removed = c.segments.invalidate(event.spans, event.epoch)
	}

	if data := c.cacheData.Load(); data != nil {
		// Full-table cache always overlaps any invalidated span.
		c.cacheData.Store(nil)
		atomic.StoreInt64(&c.totalSize, 0)
		if removed == 0 {
			removed = 1
		}
	}
	return removed
}
