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

	"github.com/stretchr/testify/require"
)

func TestApplyInvalidationByTableID(t *testing.T) {
	c := &cachedTable{
		TableCommon: TableCommon{tableID: 42},
		segments:    newSegmentIndex(),
	}
	c.setCacheData(&cacheData{
		Start: 100,
		Lease: 200,
	}, 64)

	removed := c.applyInvalidation(cacheInvalidationEvent{
		tableID: 100,
		epoch:   2,
	})
	require.Equal(t, 0, removed)
	require.NotNil(t, c.cacheData.Load())
}

func TestApplyInvalidationClearsLocalCache(t *testing.T) {
	c := &cachedTable{
		TableCommon: TableCommon{tableID: 42},
		segments:    newSegmentIndex(),
	}
	c.setCacheData(&cacheData{
		Start: 100,
		Lease: 200,
	}, 64)

	removed := c.applyInvalidation(cacheInvalidationEvent{
		tableID: 42,
		epoch:   2,
		spans: []keySpan{
			{start: key("a"), end: key("b")},
		},
	})
	require.GreaterOrEqual(t, removed, 1)
	require.Nil(t, c.cacheData.Load())
	_, ok, err := c.segments.get(keySpan{})
	require.NoError(t, err)
	require.False(t, ok)
}
