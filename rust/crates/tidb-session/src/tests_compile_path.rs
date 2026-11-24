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

//! The checks Go's `session.ExecuteStmt` makes before `Compile`.

use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use crate::Session;

/// Go `validateStatementReadOnlyInStaleness` (`session.go:2822`) switches on
/// the statement node itself: a top-level SELECT's lock, the read-only
/// question for EXPLAIN, a set operation and the like, and any DML. A lock
/// inside a derived table or a view body, and every DDL, pass it.
#[test]
fn the_staleness_check_reads_only_the_statement_node() {
    let mut session = Session::new();
    session
        .run("SET @@tidb_enable_external_ts_read = ON")
        .unwrap();
    let check = |sql: &str| {
        let parsed = session.parse_statement(sql).unwrap();
        session
            .validate_statement_read_only_in_staleness(&parsed)
            .map_err(|error| error.to_mysql_error().message)
    };
    assert_eq!(
        check("SELECT * FROM t FOR UPDATE"),
        Err("select lock hasn't been supported in stale read yet".to_owned())
    );
    assert_eq!(
        check("INSERT INTO t VALUES (1)"),
        Err("only support read-only statement during read-only staleness transactions".to_owned())
    );
    for sql in [
        "SELECT * FROM (SELECT * FROM t FOR UPDATE) x",
        "EXPLAIN SELECT * FROM t FOR UPDATE",
        "CREATE VIEW v AS SELECT * FROM t FOR UPDATE",
        "CREATE TABLE t2 (a INT)",
    ] {
        assert_eq!(check(sql), Ok(()), "{sql}");
    }
}

/// How a refused statement reaches the session's admission checks: as the
/// statement itself, through SQL `PREPARE` / `EXECUTE`, or through the binary
/// protocol (Go `session.PrepareStmt` / `ExecutePreparedStmt`). A prepared
/// statement is prepared BEFORE the session enters the refusing context, so
/// a refusal after a successful prepare comes from the execution.
#[derive(Clone, Copy, Debug)]
enum Route {
    Direct,
    SqlPrepare,
    BinaryProtocol,
}

const ROUTES: [Route; 3] = [Route::Direct, Route::SqlPrepare, Route::BinaryProtocol];

type Refusal = (u16, String);

fn refusal(message: &str) -> Refusal {
    (1105, message.to_owned())
}

fn error_of<T: std::fmt::Debug>(sql: &str, result: Result<T, tidb_driver_error::DriverError>) -> Refusal {
    let error = result.map(|output| panic!("{sql} must be refused, got {output:?}")).unwrap_err();
    let rendered = error.to_mysql_error();
    (rendered.code, rendered.message)
}

/// The first error `sql` reports on `route`, on a session `setup` builds and
/// `enter` then moves into the refusing context.
fn first_refusal(
    route: Route,
    setup: &dyn Fn() -> Session,
    enter: &dyn Fn(&mut Session),
    sql: &str,
) -> Refusal {
    let mut session = setup();
    match route {
        Route::Direct => {
            enter(&mut session);
            error_of(sql, session.run(sql))
        }
        Route::SqlPrepare => {
            let prepare = format!("PREPARE admission_stmt FROM '{}'", sql.replace('\'', "''"));
            if let Err(error) = session.run(&prepare) {
                return error_of(&prepare, Err::<(), _>(error));
            }
            enter(&mut session);
            error_of(sql, session.run("EXECUTE admission_stmt"))
        }
        Route::BinaryProtocol => {
            let prepared = match session.prepare_stmt(sql) {
                Ok(prepared) => prepared,
                Err(error) => return error_of(sql, Err::<(), _>(error)),
            };
            enter(&mut session);
            error_of(sql, session.execute_prepared_stmt(&prepared, &[]))
        }
    }
}

/// `sql`'s first refusal on each route, in [`ROUTES`] order.
fn refusals(setup: &dyn Fn() -> Session, enter: &dyn Fn(&mut Session), sql: &str) -> Vec<Refusal> {
    ROUTES
        .iter()
        .map(|route| {
            let refused = first_refusal(*route, setup, enter, sql);
            eprintln!("admission {sql:?} {route:?}: {} {}", refused.0, refused.1);
            refused
        })
        .collect()
}

/// Asserts `sql` is refused with `expected` on every route.
fn assert_refused_on_every_route(
    setup: &dyn Fn() -> Session,
    enter: &dyn Fn(&mut Session),
    sql: &str,
    expected: Refusal,
) {
    assert_eq!(refusals(setup, enter, sql), vec![expected; ROUTES.len()], "{sql}");
}

/// Root on a fresh store with `test.t` and a row in it.
fn admission_session() -> Session {
    let mut session =
        crate::tests_support::bootstrap_session(&tidb_privilege::PrivilegeRegistry::default());
    session.run("USE test").unwrap();
    session.run("CREATE TABLE t (a INT PRIMARY KEY, b INT)").unwrap();
    session.run("INSERT INTO t VALUES (1, 10)").unwrap();
    session
}

fn current_ts(session: &mut Session) -> String {
    session.run("BEGIN").unwrap();
    let crate::StmtResult::Rows(rows) = session.run("SELECT @@tidb_current_ts").unwrap() else {
        panic!("expected rows");
    };
    session.run("COMMIT").unwrap();
    String::from_utf8(rows[0][0].sql_bytes().unwrap()).unwrap()
}

fn enter_stale_transaction(session: &mut Session) {
    let ts = current_ts(session);
    session
        .run(&format!("START TRANSACTION READ ONLY AS OF TIMESTAMP {ts}"))
        .unwrap();
}

fn enable_external_ts_read(session: &mut Session) {
    session
        .run("SET @@tidb_enable_external_ts_read = ON")
        .unwrap();
}

fn enable_low_resolution_tso(session: &mut Session) {
    session.run("SET @@tidb_low_resolution_tso = ON").unwrap();
}

/// The session's admission refusals (Go `ExecuteStmt`'s
/// `validateStatementInTxn` and `validateStatementReadOnlyInStaleness`, the
/// stale provider's `GetStmtForUpdateTS` under EXPLAIN, and the pinned or
/// low-resolution read's write refusal) read the same whether the statement
/// runs directly, through `PREPARE` / `EXECUTE`, or through the binary
/// protocol. A statement two checks refuse reports the one Go runs first.
#[test]
fn admission_refusals_read_the_same_direct_and_prepared() {
    let setup = &admission_session;
    let begin = &|session: &mut Session| {
        session.run("BEGIN").unwrap();
    };
    assert_refused_on_every_route(
        setup,
        begin,
        "PURGE MATERIALIZED VIEW LOG ON t",
        refusal("cannot run PURGE MATERIALIZED VIEW LOG in explicit transaction"),
    );
    assert_refused_on_every_route(
        setup,
        &enable_external_ts_read,
        "SELECT * FROM t FOR UPDATE",
        refusal("select lock hasn't been supported in stale read yet"),
    );
    assert_refused_on_every_route(
        setup,
        &enable_external_ts_read,
        "INSERT INTO t VALUES (2, 20)",
        refusal("only support read-only statement during read-only staleness transactions"),
    );
    assert_refused_on_every_route(
        setup,
        &enter_stale_transaction,
        "EXPLAIN INSERT INTO t VALUES (2, 20)",
        refusal("GetForUpdateTS not supported for stalenessTxnProvider"),
    );
    assert_refused_on_every_route(
        setup,
        &enable_low_resolution_tso,
        "INSERT INTO t VALUES (2, 20)",
        refusal("can not execute write statement when 'tidb_low_resolution_tso' is set"),
    );
    let snapshot = &|session: &mut Session| {
        // Go's tests write the GC safe point `SET @@tidb_snapshot` validates
        // against (`gcutil.ValidateSnapshot`).
        session
            .run("INSERT INTO mysql.tidb VALUES ('tikv_gc_safe_point', '20060102-15:04:05.000 -0700', '')")
            .unwrap();
        let ts = current_ts(session);
        session.run(&format!("SET @@tidb_snapshot = '{ts}'")).unwrap();
    };
    assert_refused_on_every_route(
        setup,
        snapshot,
        "INSERT INTO t VALUES (2, 20)",
        refusal("can not execute write statement when 'tidb_snapshot' is set"),
    );

    // Two rules at once: a write under an external-timestamp read AND under
    // `tidb_low_resolution_tso`. Go `ExecuteStmt` runs
    // `validateStatementReadOnlyInStaleness` before `Compile`; the
    // low-resolution write refusal is `ExecStmt`'s, after it.
    let external_and_low_resolution = &|session: &mut Session| {
        enable_external_ts_read(session);
        enable_low_resolution_tso(session);
    };
    assert_refused_on_every_route(
        setup,
        external_and_low_resolution,
        "INSERT INTO t VALUES (2, 20)",
        refusal("only support read-only statement during read-only staleness transactions"),
    );
}

/// The admission refusals whose prepared routes do NOT report the direct
/// statement's error today, pinned as found so a reordering of the checks
/// shows here: SQL `PREPARE` refuses IMPORT INTO outright (1295), and inside
/// a `START TRANSACTION READ ONLY AS OF` the executions of a prepared locking
/// read or write fail on the stale provider's for-update TS instead of
/// `validateStatementReadOnlyInStaleness`'s message.
#[test]
fn admission_refusals_whose_prepared_routes_differ_are_pinned() {
    let setup = &admission_session;
    let unsupported_ps = (1295, "This command is not supported in the prepared statement protocol yet".to_owned());
    let for_update_ts = refusal("GetSnapshotWithStmtForUpdateTS not supported for stalenessTxnProvider");
    let read_only = refusal("only support read-only statement during read-only staleness transactions");
    let in_txn = refusal("cannot run IMPORT INTO in explicit transaction");

    // Two rules at once: IMPORT INTO in an explicit transaction AND a write
    // under an external-timestamp read. `validateStatementInTxn` runs first.
    let begin_external = &|session: &mut Session| {
        enable_external_ts_read(session);
        session.run("BEGIN").unwrap();
    };
    assert_eq!(
        refusals(setup, begin_external, "IMPORT INTO t FROM '/tmp/admission.csv'"),
        [in_txn.clone(), unsupported_ps, in_txn]
    );
    assert_eq!(
        refusals(setup, &enter_stale_transaction, "SELECT * FROM t FOR UPDATE"),
        [
            refusal("select lock hasn't been supported in stale read yet"),
            for_update_ts.clone(),
            for_update_ts.clone(),
        ]
    );
    assert_eq!(
        refusals(setup, &enter_stale_transaction, "INSERT INTO t VALUES (2, 20)"),
        [read_only.clone(), read_only, for_update_ts]
    );
}

/// Go `PlanBuilder.checkSEMStmt` under a SEM v2 policy that restricts INSERT.
/// SEM is process-wide, so the cases run in a child process.
#[test]
fn a_sem_restricted_statement_is_refused_the_same_direct_and_prepared() {
    let module = module_path!().strip_prefix("tidb_session::").unwrap();
    let child = format!("{module}::sem_admission_child");
    let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
        .args(["--ignored", "--exact", &child, "--nocapture"])
        .output()
        .expect("run the SEM admission child");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    eprint!("{stderr}");
    assert!(
        output.status.success() && stdout.contains("1 passed; 0 failed"),
        "the SEM admission child failed or was not selected:\nstdout:\n{stdout}\nstderr:\n{stderr}",
    );
}

#[test]
#[ignore = "subprocess helper"]
fn sem_admission_child() {
    tidb_sem::disable();
    tidb_sem_v2::disable();
    tidb_sem_v2::install_variable_registry();

    let privileges = tidb_privilege::PrivilegeRegistry::default();
    let mut boot = crate::tests_support::bootstrap_session(&privileges);
    boot.run("USE test").unwrap();
    boot.run("CREATE TABLE t (a INT PRIMARY KEY, b INT)").unwrap();
    boot.run("INSERT INTO t VALUES (1, 10)").unwrap();
    boot.run("CREATE USER 'nobody'@'%'").unwrap();
    boot.run("GRANT SELECT, INSERT ON test.* TO 'nobody'@'%'").unwrap();
    let catalog = boot.shared_catalog();

    let config = tidb_sem_v2::Config {
        version: "1.0".to_owned(),
        tidb_version: tidb_sem_v2::tidb_release_version(),
        restricted_sql: tidb_sem_v2::SQLRestriction {
            sql: vec!["INSERT".to_owned()],
            rule: Vec::new(),
        },
        ..tidb_sem_v2::Config::default()
    };
    tidb_sem_v2::enable_by(&config).unwrap();

    let setup = &|| {
        let mut session =
            crate::tests_support::session_as(&privileges, catalog.clone(), "nobody", "%");
        session.run("USE test").unwrap();
        session
    };
    let sem = (
        8132,
        "Feature 'INSERT INTO t VALUES (2, 20)' is not supported when security enhanced mode is enabled"
            .to_owned(),
    );
    assert_refused_on_every_route(setup, &|_| {}, "INSERT INTO t VALUES (2, 20)", sem.clone());

    // Two rules at once: a SEM-restricted write under an external-timestamp
    // read. Go `ExecuteStmt` runs `validateStatementReadOnlyInStaleness`
    // before `Compile`, whose `Build` runs `checkSEMStmt`, and the direct
    // statement reports the staleness refusal. A SQL `PREPARE` builds the
    // statement, so SEM refuses it there; the binary protocol's execution
    // reaches the compile phase's `checkSEMStmt` before its staleness check
    // today, and reports SEM too. Pinned as found.
    assert_eq!(
        refusals(setup, &enable_external_ts_read, "INSERT INTO t VALUES (2, 20)"),
        [
            refusal("only support read-only statement during read-only staleness transactions"),
            sem.clone(),
            sem,
        ]
    );
    tidb_sem_v2::disable();
}
