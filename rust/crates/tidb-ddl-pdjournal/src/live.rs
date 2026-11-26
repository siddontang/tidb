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
//! and the reconciler checks and writes an object under the same lock, so
//! the reconciler's repair of an object either lands before the attempt's
//! own PD write or does not happen.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex, MutexGuard, PoisonError};

use crate::ObjectKey;

/// Busy objects by keyspace, with how many live attempts hold each.
static BUSY: LazyLock<Mutex<HashMap<(u32, ObjectKey), usize>>> = LazyLock::new(Mutex::default);

fn busy() -> MutexGuard<'static, HashMap<(u32, ObjectKey), usize>> {
    BUSY.lock().unwrap_or_else(PoisonError::into_inner)
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
        let mut busy = busy();
        for key in &keys {
            *busy.entry((scope, key.clone())).or_default() += 1;
        }
        Self { scope, keys }
    }
}

impl Drop for LiveHold {
    fn drop(&mut self) {
        let mut busy = busy();
        for key in &self.keys {
            let entry = (self.scope, key.clone());
            if let Some(count) = busy.get_mut(&entry) {
                *count -= 1;
                if *count == 0 {
                    busy.remove(&entry);
                }
            }
        }
    }
}

/// Runs `write` unless a live attempt of this process holds `key` in
/// keyspace `scope`; `None` when one does.
pub(crate) fn unless_busy<T>(scope: u32, key: &ObjectKey, write: impl FnOnce() -> T) -> Option<T> {
    let busy = busy();
    if busy.contains_key(&(scope, key.clone())) {
        return None;
    }
    let result = write();
    drop(busy);
    Some(result)
}

/// Whether a live attempt of this process holds `key` in keyspace `scope`.
pub(crate) fn is_busy(scope: u32, key: &ObjectKey) -> bool {
    busy().contains_key(&(scope, key.clone()))
}
