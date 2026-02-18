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
	"slices"
	"sync"

	"github.com/pingcap/tidb/pkg/kv"
)

// CachedTableInvalidationEvent is the public invalidation event payload.
// It is currently a scaffolding API and can be backed by domain-level fanout later.
type CachedTableInvalidationEvent struct {
	TableID    int64
	PhysicalID int64
	Epoch      uint64
	CommitTS   uint64
	Ranges     []kv.KeyRange
}

type cachedTableInvalidationNotifier interface {
	Watch(ctx context.Context) <-chan CachedTableInvalidationEvent
	Notify(event CachedTableInvalidationEvent)
	Close()
}

type cachedTableInvalidationWatcher struct {
	ctx context.Context
	ch  chan CachedTableInvalidationEvent
}

type memCachedTableInvalidationNotifier struct {
	mu       sync.RWMutex
	ctx      context.Context
	cancel   func()
	wg       sync.WaitGroup
	watchers []*cachedTableInvalidationWatcher
}

func newMemCachedTableInvalidationNotifier() *memCachedTableInvalidationNotifier {
	ctx, cancel := context.WithCancel(context.Background())
	return &memCachedTableInvalidationNotifier{
		ctx:      ctx,
		cancel:   cancel,
		watchers: make([]*cachedTableInvalidationWatcher, 0, 8),
	}
}

func (n *memCachedTableInvalidationNotifier) Watch(ctx context.Context) <-chan CachedTableInvalidationEvent {
	n.mu.Lock()
	defer n.mu.Unlock()

	ch := make(chan CachedTableInvalidationEvent)
	if n.cancel == nil {
		close(ch)
		return ch
	}

	watcher := &cachedTableInvalidationWatcher{
		ctx: ctx,
		ch:  make(chan CachedTableInvalidationEvent, 8),
	}
	n.watchers = append(n.watchers, watcher)
	n.wg.Add(1)
	go func() {
		defer func() {
			n.mu.Lock()
			defer n.mu.Unlock()
			close(ch)
			if i := slices.Index(n.watchers, watcher); i >= 0 {
				n.watchers = slices.Delete(n.watchers, i, i+1)
			}
			n.wg.Done()
		}()

		for {
			select {
			case <-n.ctx.Done():
				return
			case <-ctx.Done():
				return
			case evt := <-watcher.ch:
				select {
				case <-n.ctx.Done():
					return
				case <-ctx.Done():
					return
				case ch <- evt:
				}
			}
		}
	}()
	return ch
}

func (n *memCachedTableInvalidationNotifier) Notify(event CachedTableInvalidationEvent) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.cancel == nil {
		return
	}

	for _, w := range n.watchers {
		select {
		case <-n.ctx.Done():
			return
		case <-w.ctx.Done():
			continue
		case w.ch <- event:
		default:
			watcher := w
			n.wg.Add(1)
			go func() {
				defer n.wg.Done()
				select {
				case <-n.ctx.Done():
					return
				case <-watcher.ctx.Done():
					return
				case watcher.ch <- event:
				}
			}()
		}
	}
}

func (n *memCachedTableInvalidationNotifier) Close() {
	n.mu.Lock()
	if n.cancel != nil {
		n.cancel()
		n.cancel = nil
	}
	n.mu.Unlock()
	n.wg.Wait()
}

var globalCachedTableInvalidationNotifier cachedTableInvalidationNotifier = newMemCachedTableInvalidationNotifier()

// PublishCachedTableInvalidationEvent publishes one cached-table invalidation event.
func PublishCachedTableInvalidationEvent(event CachedTableInvalidationEvent) {
	globalCachedTableInvalidationNotifier.Notify(event)
}

// PublishCachedTableInvalidationEvents publishes a batch of invalidation events.
func PublishCachedTableInvalidationEvents(events []CachedTableInvalidationEvent) {
	for _, event := range events {
		PublishCachedTableInvalidationEvent(event)
	}
}

// WatchCachedTableInvalidation subscribes invalidation events.
func WatchCachedTableInvalidation(ctx context.Context) <-chan CachedTableInvalidationEvent {
	return globalCachedTableInvalidationNotifier.Watch(ctx)
}

// SetCachedTableInvalidationNotifierForTest swaps global notifier in tests.
// The cleanup function restores the previous notifier and closes the new one.
func SetCachedTableInvalidationNotifierForTest(n cachedTableInvalidationNotifier) func() {
	origin := globalCachedTableInvalidationNotifier
	globalCachedTableInvalidationNotifier = n
	return func() {
		globalCachedTableInvalidationNotifier = origin
		n.Close()
	}
}
