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

//! The objects this process's live attempts are writing.
//!
//! The reconciler of a keyspace leaves an object a live attempt holds: the
//! attempt has written (or is about to write) PD and has not committed its
//! record yet. An attempt registers its objects before its intent commits,
//! and the reconciler checks and writes an object under that object's own
//! lock, so the reconciler's repair of an object either lands before the
//! attempt's own PD write or does not happen. The lock is per keyspace and
//! object: a repair waiting on PD blocks only the writers of that object, and
//! the shared map is never locked across PD I/O.
//!
//! Writers of one object in this process are ordered too: from its first
//! PD write until its records committed or its compensation put PD back
//! ([`Writing`]), an attempt writes its objects alone, so a compensation
//! that re-reads a record and writes it to PD cannot be overtaken by a
//! second writer's commit, nor overwrite it.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, LazyLock, Mutex, MutexGuard, PoisonError};

use crate::ObjectKey;

/// One object of one keyspace: how many live attempts hold it, and whether
/// one of them is writing it.
#[derive(Debug, Default)]
struct State {
    holds: usize,
    writing: bool,
}

#[derive(Debug, Default)]
struct SlotInner {
    state: Mutex<State>,
    /// Signalled when a writer is done.
    done: Condvar,
}

type Slot = Arc<SlotInner>;

static BUSY: LazyLock<Mutex<HashMap<(u32, ObjectKey), Slot>>> = LazyLock::new(Mutex::default);

fn map() -> MutexGuard<'static, HashMap<(u32, ObjectKey), Slot>> {
    BUSY.lock().unwrap_or_else(PoisonError::into_inner)
}

fn lock(slot: &Slot) -> MutexGuard<'_, State> {
    slot.state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The slot of `key` in keyspace `scope`, created when absent. The map's lock
/// is held only for the lookup.
fn slot(scope: u32, key: &ObjectKey) -> Slot {
    Arc::clone(map().entry((scope, key.clone())).or_default())
}

/// One live attempt's hold on its objects in keyspace `scope`, released on
/// drop.
#[derive(Debug)]
pub(crate) struct LiveHold {
    scope: u32,
    keys: Vec<ObjectKey>,
}

impl LiveHold {
    pub(crate) fn new(scope: u32, keys: Vec<ObjectKey>) -> Self {
        for key in &keys {
            lock(&slot(scope, key)).holds += 1;
        }
        Self { scope, keys }
    }

    /// Waits until no other attempt of this process writes any of the
    /// hold's objects, then writes them alone until the answer is dropped.
    /// Takes the objects in key order, so two attempts sharing objects never
    /// wait for each other in a circle.
    pub(crate) fn write(&self) -> Writing {
        let mut keys = self.keys.clone();
        keys.sort();
        keys.dedup();
        let slots = keys
            .iter()
            .map(|key| {
                let held = slot(self.scope, key);
                let mut state = lock(&held);
                while state.writing {
                    state = held.done.wait(state).unwrap_or_else(PoisonError::into_inner);
                }
                state.writing = true;
                drop(state);
                held
            })
            .collect();
        Writing { slots }
    }
}

impl Drop for LiveHold {
    fn drop(&mut self) {
        for key in &self.keys {
            let entry = (self.scope, key.clone());
            let Some(held) = map().get(&entry).cloned() else {
                continue;
            };
            let mut state = lock(&held);
            state.holds = state.holds.saturating_sub(1);
            if state.holds == 0 {
                drop(state);
                forget_if_idle(&entry, &held);
            }
        }
    }
}

/// An attempt's exclusive write of its objects ([`LiveHold::write`]),
/// released on drop. It lives no longer than the attempt's hold.
#[derive(Debug)]
pub struct Writing {
    slots: Vec<Slot>,
}

impl Drop for Writing {
    fn drop(&mut self) {
        for held in &self.slots {
            lock(held).writing = false;
            held.done.notify_all();
        }
    }
}

/// Forgets `entry` when its slot `held` has no hold and nobody but the map
/// and the caller references it (so nobody can be holding its lock).
fn forget_if_idle(entry: &(u32, ObjectKey), held: &Slot) {
    let mut map = map();
    let idle = map
        .get(entry)
        .is_some_and(|slot| Arc::ptr_eq(slot, held) && Arc::strong_count(slot) == 2 && lock(slot).holds == 0);
    if idle {
        map.remove(entry);
    }
}

/// Runs `write` unless a live attempt of this process holds `key` in
/// keyspace `scope`; `None` when one does. Holds only that object's lock
/// while `write` runs.
pub(crate) fn unless_busy<T>(scope: u32, key: &ObjectKey, write: impl FnOnce() -> T) -> Option<T> {
    let held = slot(scope, key);
    let state = lock(&held);
    if state.holds > 0 {
        return None;
    }
    let result = write();
    drop(state);
    forget_if_idle(&(scope, key.clone()), &held);
    Some(result)
}

/// Whether a live attempt of this process holds `key` in keyspace `scope`.
pub(crate) fn is_busy(scope: u32, key: &ObjectKey) -> bool {
    let held = map().get(&(scope, key.clone())).cloned();
    held.is_some_and(|held| lock(&held).holds > 0)
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::channel;
    use std::time::Duration;

    use super::*;

    /// A repair of one tenant's object waiting on PD blocks neither another
    /// tenant's prepare and release nor another object's, but does hold off
    /// a writer of the same object until it is done.
    #[test]
    fn a_delayed_repair_blocks_only_its_own_object() {
        let key = ObjectKey::resource_group("rg_live");
        let (entered, repairing) = channel();
        let (release, released) = channel::<()>();
        let repair = std::thread::spawn({
            let key = key.clone();
            move || {
                unless_busy(1_000_001, &key, || {
                    entered.send(()).unwrap();
                    released.recv().unwrap();
                })
            }
        });
        repairing.recv().unwrap();

        let (done, finished) = channel();
        for (scope, key) in [(1_000_002, key.clone()), (1_000_001, ObjectKey::resource_group("rg_other"))] {
            let done = done.clone();
            std::thread::spawn(move || {
                drop(LiveHold::new(scope, vec![key]));
                done.send(()).unwrap();
            });
        }
        for _ in 0..2 {
            finished.recv_timeout(Duration::from_secs(5)).expect("another tenant's or object's hold is not blocked");
        }

        let same = std::thread::spawn({
            let (key, done) = (key.clone(), done.clone());
            move || {
                let hold = LiveHold::new(1_000_001, vec![key]);
                done.send(()).unwrap();
                hold
            }
        });
        assert!(finished.recv_timeout(Duration::from_millis(300)).is_err(), "the same object waits for the repair");
        release.send(()).unwrap();
        assert_eq!(repair.join().unwrap(), Some(()));
        finished.recv_timeout(Duration::from_secs(5)).unwrap();
        let hold = same.join().unwrap();
        assert!(is_busy(1_000_001, &key));
        assert_eq!(unless_busy(1_000_001, &key, || ()), None);
        drop(hold);
        assert!(!is_busy(1_000_001, &key));
        assert!(!map().contains_key(&(1_000_001, key)), "an idle object is forgotten");
    }
}
