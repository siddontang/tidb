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
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
)

func key(v string) kv.Key {
	return kv.Key(v)
}

func TestKeySpanValidationAndOverlap(t *testing.T) {
	full := keySpan{}
	require.True(t, full.isValid())
	require.True(t, full.overlaps(keySpan{start: key("a"), end: key("b")}))

	require.False(t, keySpan{start: key("a"), end: key("a")}.isValid())
	require.False(t, keySpan{start: key("a")}.isValid())
	require.False(t, keySpan{end: key("a")}.isValid())

	left := keySpan{start: key("a"), end: key("c")}
	right := keySpan{start: key("c"), end: key("e")}
	cross := keySpan{start: key("b"), end: key("d")}

	require.False(t, left.overlaps(right))
	require.True(t, left.overlaps(cross))
	require.True(t, right.overlaps(cross))
}

func TestKeySpanContainsKey(t *testing.T) {
	span := keySpan{start: key("m"), end: key("z")}
	require.True(t, span.containsKey(key("m")))
	require.True(t, span.containsKey(key("x")))
	require.False(t, span.containsKey(key("z")))
	require.False(t, span.containsKey(key("a")))

	require.True(t, (keySpan{}).containsKey(key("any")))
}

func TestSegmentIndexUpsertAndFindOverlaps(t *testing.T) {
	idx := newSegmentIndex()
	require.NoError(t, idx.upsert(cacheSegment{
		span:  keySpan{start: key("a"), end: key("d")},
		epoch: 1,
	}))
	require.NoError(t, idx.upsert(cacheSegment{
		span:  keySpan{start: key("f"), end: key("h")},
		epoch: 1,
	}))

	// Upsert same span should replace previous segment instead of appending.
	require.NoError(t, idx.upsert(cacheSegment{
		span:  keySpan{start: key("a"), end: key("d")},
		epoch: 2,
	}))
	require.Equal(t, 2, idx.len())

	matched, err := idx.findOverlaps(keySpan{start: key("c"), end: key("g")})
	require.NoError(t, err)
	require.Len(t, matched, 2)

	_, err = idx.findOverlaps(keySpan{start: key("x")})
	require.ErrorIs(t, err, errInvalidKeySpan)

	seg, ok, err := idx.get(keySpan{start: key("a"), end: key("d")})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(2), seg.epoch)
}

func TestSegmentIndexInvalidate(t *testing.T) {
	idx := newSegmentIndex()
	require.NoError(t, idx.upsert(cacheSegment{
		span:  keySpan{start: key("a"), end: key("c")},
		epoch: 1,
	}))
	require.NoError(t, idx.upsert(cacheSegment{
		span:  keySpan{start: key("c"), end: key("e")},
		epoch: 1,
	}))
	require.NoError(t, idx.upsert(cacheSegment{
		span:  keySpan{start: key("e"), end: key("g")},
		epoch: 2,
	}))

	removed := idx.invalidate([]keySpan{{start: key("b"), end: key("f")}}, 2)
	require.Equal(t, 2, removed)
	require.Equal(t, 1, idx.len())

	// Empty spans means full-table invalidation.
	removed = idx.invalidate(nil, 3)
	require.Equal(t, 1, removed)
	require.Equal(t, 0, idx.len())
}

func TestSegmentIndexFindByKey(t *testing.T) {
	idx := newSegmentIndex()
	require.NoError(t, idx.upsert(cacheSegment{
		span:  keySpan{start: key("a"), end: key("c")},
		epoch: 1,
	}))
	require.NoError(t, idx.upsert(cacheSegment{
		span:  keySpan{},
		epoch: 2,
	}))

	seg, ok := idx.findByKey(key("b"))
	require.True(t, ok)
	require.Equal(t, uint64(1), seg.epoch)

	seg, ok = idx.findByKey(key("z"))
	require.True(t, ok)
	require.Equal(t, uint64(2), seg.epoch)
	require.Equal(t, 1, idx.hotRangeLen())
}

func TestSegmentIndexInvalidatePromotesUntouchedEpoch(t *testing.T) {
	idx := newSegmentIndex()
	require.NoError(t, idx.upsert(cacheSegment{
		span:  keySpan{start: key("a"), end: key("c")},
		epoch: 1,
	}))
	require.NoError(t, idx.upsert(cacheSegment{
		span:  keySpan{start: key("c"), end: key("e")},
		epoch: 1,
	}))

	removed := idx.invalidate([]keySpan{{start: key("a"), end: key("b")}}, 2)
	require.Equal(t, 1, removed)

	seg, ok, err := idx.get(keySpan{start: key("c"), end: key("e")})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(2), seg.epoch)
}
