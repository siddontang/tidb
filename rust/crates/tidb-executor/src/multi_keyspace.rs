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

//! Statements a multi-keyspace node refuses until their services are per
//! keyspace (`rust/docs/design/2026-11-24-multi-keyspace-server.md`, §10
//! phase 2, "unsupported operations are rejected").
//!
//! IMPORT INTO, its job statements, BR/BRIE and PLAN REPLAYER reach
//! process-wide singletons that belong to whichever keyspace installed them
//! last: the import store and the DXF task managers, the BRIE node and job
//! queue, and the global external storage (phase 3f moves them into each
//! keyspace runtime). Each statement checks here before it consults any of
//! them.

use crate::DriverError;

/// `ER_NOT_SUPPORTED_YET` for `feature` on a node serving several
/// keyspaces; nothing on a single-keyspace node.
///
/// # Errors
///
/// The node serves several keyspaces.
pub fn refuse_on_multi_keyspace_node(feature: &str) -> Result<(), DriverError> {
    if !tidb_keyspace::is_multi_keyspace_mode() {
        return Ok(());
    }
    Err(DriverError::from(tidb_plannererrors::ERR_NOT_SUPPORTED_YET.gen_with_stack_by_args(&[
        format!("{feature} on a multi-keyspace node").into(),
    ])))
}
