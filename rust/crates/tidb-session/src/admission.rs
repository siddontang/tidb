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

//! Statement admission: the checks that refuse a statement before it
//! compiles or runs -- Go `session.ExecuteStmt`'s `validateStatementInTxn`
//! and `validateStatementReadOnlyInStaleness`, `PlanBuilder.checkSEMStmt`,
//! the stale transaction's refusal of an EXPLAINed write, and the pinned
//! historical read's refusal. Their callers in [`crate::dispatch`] and the
//! cluster node's compile keep their own order.

use tidb_executor::compiler::PrepareExec;
use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use tidb_ast::Stmt;
use tidb_driver_error::DriverError;

use crate::Session;

fn sem_table_option(option: &tidb_ast::TableOption) -> tidb_sem_v2::TableOptionType {
    match option {
        tidb_ast::TableOption::Ttl { .. } => tidb_sem_v2::TableOptionType::Ttl,
        tidb_ast::TableOption::TtlEnable(_) => tidb_sem_v2::TableOptionType::TtlEnable,
        tidb_ast::TableOption::TtlJobInterval(_) => {
            tidb_sem_v2::TableOptionType::TtlJobInterval
        }
        _ => tidb_sem_v2::TableOptionType::Other,
    }
}

fn sem_stmt_kind(stmt: &Stmt) -> tidb_sem_v2::StmtKind {
    use tidb_sem_v2::{AlterTableSpec, AlterTableType, StmtKind};

    match stmt {
        Stmt::Query(query) => match query.as_ref() {
            tidb_ast::QueryStmt::Select(select) => StmtKind::Select {
                select_into: select.into_outfile.is_some(),
            },
            tidb_ast::QueryStmt::SetOpr(_) => StmtKind::Other,
        },
        Stmt::Dml(dml) => match dml.as_ref() {
            tidb_ast::DmlStmt::ImportInto(import) => match &import.source {
                tidb_ast::ImportSource::File { path, .. } => StmtKind::ImportInto {
                    from_select: false,
                    path: path.clone(),
                },
                tidb_ast::ImportSource::Select { .. } => StmtKind::ImportInto {
                    from_select: true,
                    path: String::new(),
                },
            },
            tidb_ast::DmlStmt::LoadData(load) => StmtKind::LoadData {
                file_loc_client: load.local,
                path: load.path.clone(),
            },
            _ => StmtKind::Other,
        },
        Stmt::Ddl(ddl) => match ddl.as_ref() {
            tidb_ast::DdlStmt::CreateTable(create) => StmtKind::CreateTable {
                options: create.table_options.iter().map(sem_table_option).collect(),
            },
            tidb_ast::DdlStmt::AlterTable(alter) => StmtKind::AlterTable {
                specs: alter
                    .actions
                    .iter()
                    .map(|action| match action {
                        tidb_ast::AlterTableAction::RemoveTtl(_) => AlterTableSpec {
                            tp: AlterTableType::RemoveTtl,
                            options: Vec::new(),
                        },
                        tidb_ast::AlterTableAction::SetTableOptions { options } => AlterTableSpec {
                            tp: AlterTableType::Option,
                            options: options.iter().map(sem_table_option).collect(),
                        },
                        tidb_ast::AlterTableAction::SetAttributes(_) => AlterTableSpec {
                            tp: AlterTableType::Attributes,
                            options: Vec::new(),
                        },
                        tidb_ast::AlterTableAction::Partition(
                            tidb_ast::AlterPartitionAction::SetAttributes { .. },
                        ) => AlterTableSpec {
                            tp: AlterTableType::PartitionAttributes,
                            options: Vec::new(),
                        },
                        _ => AlterTableSpec {
                            tp: AlterTableType::Other,
                            options: Vec::new(),
                        },
                    })
                    .collect(),
            },
            _ => StmtKind::Other,
        },
        Stmt::Admin(_) | Stmt::Session(_) => StmtKind::Other,
    }
}

fn sem_stmt_view(stmt: &Stmt) -> tidb_sem_v2::StmtView {
    tidb_sem_v2::StmtView {
        sem_command: stmt.sem_command().to_owned(),
        kind: sem_stmt_kind(stmt),
    }
}

impl Session {
    /// Go `PlanBuilder.checkSEMStmt`, the first step of `Build` for every
    /// statement: SQL the SEM v2 policy restricts needs
    /// `RESTRICTED_SQL_ADMIN`, ahead of any privilege check.
    pub(crate) fn check_sem_stmt(&self, sql: &str, stmt: &Stmt) -> Result<(), DriverError> {
        if tidb_sem_v2::is_enabled()
            && tidb_sem_v2::is_restricted_sql(&sem_stmt_view(stmt))
            && !self.has_dynamic_privilege("RESTRICTED_SQL_ADMIN", false)
        {
            let statement = if stmt.text().is_empty() {
                sql.to_owned()
            } else {
                String::from_utf8_lossy(stmt.text()).into_owned()
            };
            return Err(DriverError::from(
                tidb_plannererrors::ERR_NOT_SUPPORTED_WITH_SEM
                    .gen_with_stack_by_args(&[statement.into()]),
            ));
        }
        Ok(())
    }

    /// Go `validateStatementInTxn`, which `ExecuteStmt` runs before
    /// `validateStatementReadOnlyInStaleness`: IMPORT INTO, PURGE
    /// MATERIALIZED VIEW LOG and REFRESH MATERIALIZED VIEW (also as the
    /// statement an EXECUTE names) cannot run in an explicit transaction.
    pub fn validate_statement_in_txn(&self, stmt: &Stmt, in_txn: bool) -> Result<(), DriverError> {
        if !in_txn {
            return Ok(());
        }
        let refused = |stmt: &Stmt| match stmt {
            Stmt::Dml(dml) if matches!(dml.as_ref(), tidb_ast::DmlStmt::ImportInto(_)) => Some("IMPORT INTO"),
            Stmt::Admin(admin) => match admin.as_ref() {
                tidb_ast::AdminStmt::PurgeMaterializedViewLog(_) => Some("PURGE MATERIALIZED VIEW LOG"),
                tidb_ast::AdminStmt::RefreshMaterializedView(_) => Some("REFRESH MATERIALIZED VIEW"),
                _ => None,
            },
            _ => None,
        };
        let refused = match stmt {
            Stmt::Session(session) => match session.as_ref() {
                tidb_ast::SessionStmt::Execute { name, .. } => self
                    .prepared_statement_sql(name)
                    .and_then(|sql| self.parse(sql).ok())
                    .and_then(|inner| refused(&inner)),
                _ => None,
            },
            stmt => refused(stmt),
        };
        if let Some(statement) = refused {
            return Err(DriverError::from(tidb_terror::MysqlError::unknown(format!(
                "cannot run {statement} in explicit transaction"
            ))));
        }
        Ok(())
    }

    /// Go `validateStatementReadOnlyInStaleness` (`session.go:2822`), which
    /// `ExecuteStmt` runs before `Compile`. It switches on the statement node
    /// itself: a top-level SELECT's lock, the read-only question for the
    /// statements it lists, and any DML; a lock inside a derived table or a
    /// view body, and every DDL, pass it.
    pub fn validate_statement_read_only_in_staleness(&self, stmt: &Stmt) -> Result<(), DriverError> {
        if !self.vars.restricted_sql
            && (self.is_staleness_txn()
                || self.vars.txn_read_ts() != 0
                || self.external_timestamp_read_enabled())
        {
            // Go master: COMPARE reads its snapshots on its own; only its
            // OUTPUT table is a write.
            if let Stmt::Admin(admin) = stmt {
                if let tidb_ast::AdminStmt::CompareMaterializedView(compare) = admin.as_ref() {
                    if compare.output_table.is_some() {
                        return Err(DriverError::unsupported(
                            "only support read-only statement during read-only staleness transactions",
                        ));
                    }
                    return Ok(());
                }
            }
            let locked = matches!(stmt, Stmt::Query(query) if matches!(
                query.as_ref(),
                tidb_ast::QueryStmt::Select(select) if select.lock.is_some()
            ));
            if locked {
                return Err(DriverError::unsupported(
                    "select lock hasn't been supported in stale read yet",
                ));
            }
            let read_only_checked = matches!(&stmt, Stmt::Dml(_) | Stmt::Query(_))
                || matches!(&stmt, Stmt::Admin(admin) if matches!(admin.as_ref(), tidb_ast::AdminStmt::Explain(
                    _,
                ) | tidb_ast::AdminStmt::Trace(_)));
            if read_only_checked && !stmt.is_read_only(true) {
                return Err(DriverError::unsupported(
                    "only support read-only statement during read-only staleness transactions",
                ));
            }
        }
        Ok(())
    }

    /// Go `buildExplain` builds the EXPLAIN's target executor; for an INSERT,
    /// UPDATE or DELETE that is `updateForUpdateTS`, which a stale
    /// transaction's provider refuses (`StalenessTxnContextProvider
    /// .GetStmtForUpdateTS`). A SELECT ... FOR UPDATE is refused before this
    /// (`validateStatementReadOnlyInStaleness`).
    pub(crate) fn check_explain_target_for_update_ts(&self, stmt: &Stmt) -> Result<(), DriverError> {
        if !self.is_staleness_txn() {
            return Ok(());
        }
        let Stmt::Admin(admin) = stmt else {
            return Ok(());
        };
        let tidb_ast::AdminStmt::Explain(explain) = admin.as_ref() else {
            return Ok(());
        };
        if matches!(
            explain.statement(),
            Some(Stmt::Dml(dml)) if matches!(
                dml.as_ref(),
                tidb_ast::DmlStmt::Insert(_) | tidb_ast::DmlStmt::Update(_) | tidb_ast::DmlStmt::Delete(_)
            )
        ) {
            return Err(DriverError::unsupported("GetForUpdateTS not supported for stalenessTxnProvider"));
        }
        Ok(())
    }

    pub(crate) fn refuse_pinned_historical_read(&mut self, stmt: &Stmt) -> Result<(), DriverError> {
        if self.historical_read_provider.is_some() {
            self.validate_statement_read_only_in_staleness(stmt)?;
            return tidb_executor::adapter::check_no_delay_snapshot_writes(self, stmt);
        }

        if self.vars.snapshot_ts() != 0 {
            return Err(DriverError::unsupported(
                "reading at @@tidb_snapshot is not supported yet",
            ));
        }
        // `@@tidb_read_staleness` needs no history here. Go applies it to a
        // SELECT only (preprocess `p.stmtTp == TypeSelect`), reading at
        // `CalAppropriateTime(now + staleness, now, minSafeTS)`. This store
        // reports no safe ts, as unistore's `GetStoreSafeTS` does, which
        // client-go's `getMinSafeTSByStores` reads as MaxUint64: the read is
        // clamped to now, the current read. Any other statement ignores it.
        Ok(())
    }
}
