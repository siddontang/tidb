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
	"bytes"
	"errors"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
)

// cacheMode describes how cached table data is managed.
type cacheMode int

const (
	cacheModeFull cacheMode = iota
	cacheModeRange
	cacheModeHotRange
)

func (m cacheMode) String() string {
	switch m {
	case cacheModeFull:
		return "FULL"
	case cacheModeRange:
		return "RANGE"
	case cacheModeHotRange:
		return "HOT_RANGE"
	default:
		return "UNKNOWN"
	}
}

var errInvalidKeySpan = errors.New("invalid key span")

// keySpan is a half-open interval [start, end). Empty start and end means full table.
type keySpan struct {
	start kv.Key
	end   kv.Key
}

func (s keySpan) isFullTable() bool {
	return len(s.start) == 0 && len(s.end) == 0
}

func (s keySpan) isValid() bool {
	if s.isFullTable() {
		return true
	}
	if len(s.start) == 0 || len(s.end) == 0 {
		return false
	}
	return bytes.Compare(s.start, s.end) < 0
}

func (s keySpan) overlaps(other keySpan) bool {
	if s.isFullTable() || other.isFullTable() {
		return true
	}
	if !s.isValid() || !other.isValid() {
		return false
	}
	return bytes.Compare(s.start, other.end) < 0 && bytes.Compare(other.start, s.end) < 0
}

func (s keySpan) containsKey(key kv.Key) bool {
	if s.isFullTable() {
		return true
	}
	if !s.isValid() {
		return false
	}
	return bytes.Compare(s.start, key) <= 0 && bytes.Compare(key, s.end) < 0
}

func sameSpan(lhs, rhs keySpan) bool {
	return bytes.Equal(lhs.start, rhs.start) && bytes.Equal(lhs.end, rhs.end)
}

func overlapsAny(span keySpan, spans []keySpan) bool {
	for _, other := range spans {
		if span.overlaps(other) {
			return true
		}
	}
	return false
}

// cacheSegment is one cached span. The epoch is used with invalidation events.
type cacheSegment struct {
	span keySpan

	epoch   uint64
	startTS uint64
	leaseTS uint64

	sizeBytes  int64
	hitCount   uint64
	lastAccess time.Time

	memBuffer kv.MemBuffer
}

// segmentIndex is a span-aware segment registry for one cached table or partition.
type segmentIndex struct {
	mu       sync.RWMutex
	segments []cacheSegment
}

func newSegmentIndex() *segmentIndex {
	return &segmentIndex{}
}

func (i *segmentIndex) upsert(seg cacheSegment) error {
	if !seg.span.isValid() {
		return errInvalidKeySpan
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	for idx := range i.segments {
		if sameSpan(i.segments[idx].span, seg.span) {
			i.segments[idx] = seg
			return nil
		}
	}
	i.segments = append(i.segments, seg)
	return nil
}

func (i *segmentIndex) findOverlaps(span keySpan) ([]cacheSegment, error) {
	if !span.isValid() {
		return nil, errInvalidKeySpan
	}

	i.mu.RLock()
	defer i.mu.RUnlock()

	result := make([]cacheSegment, 0, len(i.segments))
	for _, seg := range i.segments {
		if seg.span.overlaps(span) {
			result = append(result, seg)
		}
	}
	return result, nil
}

func (i *segmentIndex) get(span keySpan) (cacheSegment, bool, error) {
	if !span.isValid() {
		return cacheSegment{}, false, errInvalidKeySpan
	}

	i.mu.RLock()
	defer i.mu.RUnlock()

	for _, seg := range i.segments {
		if sameSpan(seg.span, span) {
			return seg, true, nil
		}
	}
	return cacheSegment{}, false, nil
}

func (i *segmentIndex) findByKey(key kv.Key) (cacheSegment, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	var full cacheSegment
	hasFull := false
	for _, seg := range i.segments {
		if seg.span.isFullTable() {
			full = seg
			hasFull = true
			continue
		}
		if seg.span.containsKey(key) {
			return seg, true
		}
	}
	if hasFull {
		return full, true
	}
	return cacheSegment{}, false
}

// invalidate removes stale segments that overlap invalidation spans.
// Empty spans means invalidating the full table.
func (i *segmentIndex) invalidate(spans []keySpan, newEpoch uint64) int {
	i.mu.Lock()
	defer i.mu.Unlock()

	if len(spans) == 0 {
		spans = []keySpan{{}}
	}

	keep := i.segments[:0]
	removed := 0
	for _, seg := range i.segments {
		if seg.epoch < newEpoch && overlapsAny(seg.span, spans) {
			removed++
			continue
		}
		keep = append(keep, seg)
	}
	i.segments = keep
	return removed
}

func (i *segmentIndex) len() int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return len(i.segments)
}

func (i *segmentIndex) hotRangeLen() int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	count := 0
	for _, seg := range i.segments {
		if !seg.span.isFullTable() {
			count++
		}
	}
	return count
}
