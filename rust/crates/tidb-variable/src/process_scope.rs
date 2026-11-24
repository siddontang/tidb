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

//! The system variables whose value belongs to the process, not to one of
//! the keyspaces it serves (`rust/docs/design/2026-11-24-multi-keyspace-server.md`,
//! §6.3): a tenant of a multi-keyspace node cannot set them.

use tidb_vardef::classification::{StaticScope, PROCESS_STATICS};

use crate::sysvar::{get_sys_var, SCOPE_GLOBAL, SCOPE_INSTANCE, SCOPE_SESSION};

/// Whether `name` sets process-wide state: a variable whose `SetGlobal`
/// writes an *instance* static of `tidb-vardef` (the process's memory, log,
/// PD client, watchdog), or one Go scopes to the instance alone.
pub fn is_process_variable(name: &str) -> bool {
    if PROCESS_STATICS
        .iter()
        .any(|entry| entry.scope == StaticScope::Instance && entry.sysvar.is_some_and(|sysvar| sysvar.eq_ignore_ascii_case(name)))
    {
        return true;
    }
    get_sys_var(name).is_some_and(|def| def.scope & SCOPE_INSTANCE != 0 && def.scope & (SCOPE_GLOBAL | SCOPE_SESSION) == 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_variables_are_the_instance_ones() {
        // An instance static of tidb-vardef behind a GLOBAL variable.
        assert!(is_process_variable("tidb_enable_instance_plan_cache"));
        assert!(is_process_variable("TIDB_SCHEMA_CACHE_SIZE"));
        // Go `ScopeInstance` alone.
        assert!(is_process_variable("tidb_general_log"));
        // A keyspace's own policy.
        assert!(!is_process_variable("tidb_enable_foreign_key"));
        assert!(!is_process_variable("tidb_ttl_job_enable"));
        // A session variable.
        assert!(!is_process_variable("sql_mode"));
    }
}
