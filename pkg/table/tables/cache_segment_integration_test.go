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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCachedTableSetCacheDataSyncsSegmentLease(t *testing.T) {
	c := &cachedTable{
		segments: newSegmentIndex(),
	}

	c.setCacheData(&cacheData{
		Start:     100,
		Lease:     200,
		MemBuffer: nil,
	}, 64)

	seg, ok, err := c.segments.get(keySpan{})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(100), seg.startTS)
	require.Equal(t, uint64(200), seg.leaseTS)
	require.Nil(t, seg.memBuffer)
	// Loading stage should not update total size yet.
	require.Equal(t, int64(0), atomic.LoadInt64(&c.totalSize))

	c.setCacheData(&cacheData{
		Start:     120,
		Lease:     240,
		MemBuffer: nil,
	}, 128)
	seg, ok, err = c.segments.get(keySpan{})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(120), seg.startTS)
	require.Equal(t, uint64(240), seg.leaseTS)
}
