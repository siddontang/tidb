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

//! The tenant a log record belongs to on a node serving several keyspaces
//! (`rust/docs/design/2026-11-24-multi-keyspace-server.md`, §6.1).
//!
//! Go stamps every record with the process's one keyspace
//! (`keyspace.WrapZapcoreWithKeyspace`). A multi-keyspace node has no
//! process keyspace: its process-wide records carry none, and the work done
//! for a tenant -- a connection, a keyspace runtime's open -- enters that
//! tenant on its thread, so the records it writes there carry
//! `keyspaceName=<tenant>`.

use std::cell::RefCell;
use std::io;
use std::sync::Arc;

use tidb_log::{AtomicLevel, Core, Entry, Field, Level, Value};

thread_local! {
    static TENANT: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// The tenant this thread works for, until dropped (then the previous one).
/// Bound to the thread that entered it (`!Send`): its drop restores that
/// thread's state.
///
/// ```compile_fail
/// fn send<T: Send>(_: T) {}
/// send(tidb_logutil::enter_tenant("keyspace1"));
/// ```
#[must_use = "the thread leaves the tenant when the guard drops"]
pub struct TenantGuard {
    previous: Option<String>,
    _thread_bound: std::marker::PhantomData<*const ()>,
}

impl Drop for TenantGuard {
    fn drop(&mut self) {
        let previous = self.previous.take();
        TENANT.with(|tenant| *tenant.borrow_mut() = previous);
    }
}

/// Marks the current thread as working for keyspace `keyspace`: the records
/// it writes through the global logger carry `keyspaceName` until the guard
/// drops.
pub fn enter_tenant(keyspace: &str) -> TenantGuard {
    let previous = TENANT.with(|tenant| tenant.borrow_mut().replace(keyspace.to_owned()));
    TenantGuard {
        previous,
        _thread_bound: std::marker::PhantomData,
    }
}

/// The tenant the current thread works for, if any.
pub fn current_tenant() -> Option<String> {
    TENANT.with(|tenant| tenant.borrow().clone())
}

/// A core that appends the writing thread's tenant (`keyspaceName`) to each
/// record; a thread that entered no tenant writes the record unchanged.
pub(crate) struct TenantCore(pub(crate) Arc<dyn Core>);

impl Core for TenantCore {
    fn enabled(&self, level: Level) -> bool {
        self.0.enabled(level)
    }

    fn with(&self, fields: &[Field]) -> Arc<dyn Core> {
        Arc::new(TenantCore(self.0.with(fields)))
    }

    fn write(&self, entry: &Entry, fields: &[Field]) -> Result<(), String> {
        let Some(tenant) = current_tenant() else {
            return self.0.write(entry, fields);
        };
        let mut with_tenant = fields.to_vec();
        with_tenant.push(Field::new("keyspaceName", Value::Str(tenant)));
        self.0.write(entry, &with_tenant)
    }

    fn sync(&self) -> io::Result<()> {
        self.0.sync()
    }

    fn level(&self) -> AtomicLevel {
        self.0.level()
    }
}
