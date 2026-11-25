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
//! them. So do the DDL statements whose effect lives in PD (resource groups,
//! TiFlash replicas) or in the AutoID service
//! (`AUTO_ID_CACHE=1`).

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

/// The PD-backed part of `ddl`, if any: the feature a multi-keyspace node
/// refuses for it (design §10 phase 2). Resource groups, TiFlash replicas
/// and FLASHBACK CLUSTER's
/// schedule limits are written through the process's PD clients, which
/// belong to whichever keyspace opened last (phase 3b); an `AUTO_ID_CACHE=1` table allocates through the AutoID
/// service only the configured keyspace runs (phase 3f).
#[must_use]
pub fn pd_backed_ddl_feature(ddl: &tidb_ast::DdlStmt) -> Option<&'static str> {
    use tidb_ast::{AddPartitionSpec, AlterPartitionAction, AlterTableAction, DatabaseOption, DdlStmt};
    let database_options = |options: &[DatabaseOption]| {
        options.iter().find_map(|option| match option {
            DatabaseOption::SetTiFlashReplica { .. } => Some("SET TIFLASH REPLICA"),
            _ => None,
        })
    };
    match ddl {
        DdlStmt::CreateResourceGroup(_) | DdlStmt::AlterResourceGroup(_) | DdlStmt::DropResourceGroup(_) => {
            Some("RESOURCE GROUP DDL")
        }
        // Closes and restores PD's schedule limits for the whole cluster.
        DdlStmt::FlashbackToTimestamp(flashback) if flashback.tables.is_empty() && flashback.database_name.is_empty() => {
            Some("FLASHBACK CLUSTER")
        }
        DdlStmt::CreateDatabase { options, .. } | DdlStmt::AlterDatabase { options, .. } => database_options(options),
        DdlStmt::CreateTable(create) => table_options_feature(&create.table_options)
            .or_else(|| create.partitioning.as_ref().and_then(|p| partition_definitions_feature(&p.definitions))),
        DdlStmt::AlterTable(alter) => alter.actions.iter().find_map(|action| match action {
            AlterTableAction::SetTableOptions { options } => table_options_feature(options),
            AlterTableAction::SetTiFlashReplica { .. } => Some("SET TIFLASH REPLICA"),
            AlterTableAction::Partition(partition) => match partition {
                AlterPartitionAction::SetOptions { options, .. } => table_options_feature(options),
                AlterPartitionAction::Add { spec: AddPartitionSpec::Definitions(definitions), .. }
                | AlterPartitionAction::Reorganize { definitions, .. } => partition_definitions_feature(definitions),
                AlterPartitionAction::Repartition(partitioning) => {
                    partition_definitions_feature(&partitioning.definitions)
                }
                _ => None,
            },
            _ => None,
        }),
        _ => None,
    }
}

fn table_options_feature(options: &[tidb_ast::TableOption]) -> Option<&'static str> {
    options.iter().find_map(|option| match option {
        tidb_ast::TableOption::AutoIdCache(value) if value.trim().parse::<u64>() == Ok(1) => Some("AUTO_ID_CACHE=1"),
        _ => None,
    })
}

fn partition_definitions_feature(definitions: &[tidb_ast::PartitionDefinition]) -> Option<&'static str> {
    definitions.iter().find_map(|definition| {
        table_options_feature(&definition.options).or_else(|| {
            definition.sub_partitions.iter().find_map(|sub| table_options_feature(&sub.options))
        })
    })
}

/// [`refuse_on_multi_keyspace_node`] for a DDL statement whose effect lives
/// in PD or the AutoID service ([`pd_backed_ddl_feature`]).
///
/// # Errors
///
/// The node serves several keyspaces and `ddl` is PD-backed.
pub fn refuse_pd_backed_ddl(ddl: &tidb_ast::DdlStmt) -> Result<(), DriverError> {
    match pd_backed_ddl_feature(ddl) {
        Some(feature) => refuse_on_multi_keyspace_node(feature),
        None => Ok(()),
    }
}
