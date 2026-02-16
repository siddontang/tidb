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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCachedTableInvalidationNotifierWatchNotify(t *testing.T) {
	n := newMemCachedTableInvalidationNotifier()
	defer n.Close()
	restore := SetCachedTableInvalidationNotifierForTest(n)
	defer restore()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := WatchCachedTableInvalidation(ctx)

	expected := CachedTableInvalidationEvent{
		TableID:    42,
		PhysicalID: 42,
		Epoch:      3,
		CommitTS:   123456,
	}
	PublishCachedTableInvalidationEvent(expected)

	select {
	case got := <-ch:
		require.Equal(t, expected, got)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting invalidation event")
	}
}

func TestCachedTableInvalidationNotifierWatchContextCancel(t *testing.T) {
	n := newMemCachedTableInvalidationNotifier()
	defer n.Close()
	restore := SetCachedTableInvalidationNotifierForTest(n)
	defer restore()

	ctx, cancel := context.WithCancel(context.Background())
	ch := WatchCachedTableInvalidation(ctx)
	cancel()

	select {
	case _, ok := <-ch:
		require.False(t, ok)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting watch channel close")
	}
}
