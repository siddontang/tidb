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

//! Keyspace identity and the Starter username policy
//! (`rust/docs/design/2026-11-24-multi-keyspace-server.md`, §4.1, §6.1). Each
//! test changes the configured keyspace, the deploy mode or the
//! multi-keyspace mode, so it holds the serial lock through
//! [`ProcessModes`], which restores all three even when an assertion fails.

use prost::Message as _;
use tidb_executor::exec_stmt_observation::ExecStmtObservation as _;
use tidb_session::domain::mock_store::MockStoreDomain;
use tidb_session::Session;

use crate::support::testkit::{update_global_config, ConfigGuard};

/// The configured keyspace, deploy mode and multi-keyspace mode a test runs
/// under, restored on drop (the configuration by its guard, which also
/// holds the serial lock until the modes are back).
struct ProcessModes {
    deploy_mode: tidb_config_deploymode::Mode,
    multi_keyspace: bool,
    _config: ConfigGuard,
}

impl ProcessModes {
    fn set(keyspace: &str, deploy_mode: tidb_config_deploymode::Mode, multi_keyspace: bool) -> Self {
        let keyspace = keyspace.to_owned();
        let config = update_global_config(|config| config.keyspace_name = keyspace);
        let modes = Self {
            deploy_mode: tidb_config_deploymode::get(),
            multi_keyspace: tidb_keyspace::is_multi_keyspace_mode(),
            _config: config,
        };
        tidb_config_deploymode::set(deploy_mode).unwrap();
        tidb_keyspace::set_multi_keyspace_mode(multi_keyspace);
        modes
    }
}

impl Drop for ProcessModes {
    fn drop(&mut self) {
        tidb_keyspace::set_multi_keyspace_mode(self.multi_keyspace);
        let _ = tidb_config_deploymode::set(self.deploy_mode);
    }
}

fn cell_text(value: &tidb_datatype::Datum) -> String {
    if value.is_null() {
        return "NULL".to_owned();
    }
    String::from_utf8_lossy(&value.to_bytes().expect("a result cell")).into_owned()
}

/// The statement's rows as text cells.
fn row_text<E: std::fmt::Debug>(result: Result<tidb_session::StmtResult, E>) -> Vec<Vec<String>> {
    match result.unwrap() {
        tidb_session::StmtResult::Rows(rows) => {
            rows.into_iter().map(|row| row.iter().map(cell_text).collect()).collect()
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

/// Go `TestStarterUsernamePolicyInSimpleExec`
/// (`executor/test/simpletest/simple_test.go:43`): in Starter mode the
/// account statements require new names to carry the keyspace prefix and
/// resolve an unprefixed target to its prefixed account.
#[test]
fn starter_username_policy_in_simple_exec() {
    if !tidb_config_kerneltype::is_next_gen() {
        // The starter deploy mode exists only in the nextgen kernel.
        return;
    }
    let _modes = ProcessModes::set("SYSTEM", tidb_config_deploymode::Mode::Starter, false);

    let privs = tidb_privilege::PrivilegeRegistry::default();
    let store = MockStoreDomain::create_with_accounts(Some(privs.clone())).unwrap();
    let mut tk = store.new_session().unwrap();
    let rows = |session: &mut Session, sql: &str| row_text(session.run(sql));
    let run = |session: &mut Session, sql: &str| {
        session
            .run(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    };
    let contains_err = |session: &mut Session, sql: &str, want: &str| {
        let message = session
            .run(sql)
            .expect_err(sql)
            .to_mysql_error()
            .message;
        assert!(message.contains(want), "{sql}: {message}");
    };
    let need_prefix = "User name must start with `SYSTEM.`";

    run(&mut tk, "create user if not exists `SYSTEM.r1`@`%`");
    run(&mut tk, "create user if not exists `SYSTEM.u1`@`%`");

    contains_err(&mut tk, "create user u2@'%' identified by 'pwd'", need_prefix);
    contains_err(&mut tk, "create role r2", need_prefix);
    run(&mut tk, "create role `SYSTEM.r2`");
    assert_eq!(
        rows(&mut tk, "select User from mysql.user where User='SYSTEM.r2' and Host='%'"),
        [["SYSTEM.r2"]]
    );
    contains_err(&mut tk, "rename user `SYSTEM.u1`@`%` to u2@'%'", need_prefix);

    run(&mut tk, "grant r1 to u1");
    assert_eq!(
        rows(
            &mut tk,
            "select TO_USER from mysql.role_edges where FROM_USER='SYSTEM.r1' and TO_USER='SYSTEM.u1' and TO_HOST='%'"
        ),
        [["SYSTEM.u1"]]
    );

    run(&mut tk, "set default role r1 to u1");
    assert_eq!(
        rows(
            &mut tk,
            "select USER, DEFAULT_ROLE_USER from mysql.default_roles where USER='SYSTEM.u1' and DEFAULT_ROLE_USER='SYSTEM.r1'"
        ),
        [["SYSTEM.u1", "SYSTEM.r1"]]
    );

    run(&mut tk, "set default role none to u1");
    assert!(rows(&mut tk, "select USER from mysql.default_roles where USER='SYSTEM.u1'").is_empty());

    run(&mut tk, "set default role all to u1");
    assert_eq!(
        rows(
            &mut tk,
            "select USER, DEFAULT_ROLE_USER from mysql.default_roles where USER='SYSTEM.u1' and DEFAULT_ROLE_USER='SYSTEM.r1'"
        ),
        [["SYSTEM.u1", "SYSTEM.r1"]]
    );

    run(&mut tk, "revoke r1 from u1");
    assert!(rows(
        &mut tk,
        "select TO_USER from mysql.role_edges where FROM_USER='SYSTEM.r1' and TO_USER='SYSTEM.u1' and TO_HOST='%'"
    )
    .is_empty());
    assert!(rows(&mut tk, "select USER from mysql.default_roles where USER='SYSTEM.u1'").is_empty());

    run(&mut tk, "alter user u1 identified by 'pwd2'");
    assert_eq!(
        rows(&mut tk, "select authentication_string from mysql.user where user='SYSTEM.u1' and host='%'"),
        [[tidb_privilege::encode_password("pwd2")]]
    );

    run(&mut tk, "create user if not exists `SYSTEM.keao.yang`@`%`");
    run(&mut tk, "alter user `keao.yang`@`%` identified by 'pwd3'");
    assert_eq!(
        rows(
            &mut tk,
            "select authentication_string from mysql.user where user='SYSTEM.keao.yang' and host='%'"
        ),
        [[tidb_privilege::encode_password("pwd3")]]
    );

    run(&mut tk, "create user if not exists `SYSTEM.admin`@`%`");
    run(&mut tk, "grant create user on *.* to `SYSTEM.admin`@`%`");
    run(&mut tk, "create user if not exists `SYSTEM.u_sys`@`%`");
    run(&mut tk, "grant system_user on *.* to `SYSTEM.u_sys`@`%`");
    let mut admin_tk = store.new_session().unwrap();
    admin_tk.set_user("SYSTEM.admin@%".to_owned(), "SYSTEM.admin@%".to_owned());
    admin_tk.attach_privileges(privs.clone());
    contains_err(&mut admin_tk, "alter user u_sys identified by 'pwd4'", "SYSTEM_USER or SUPER");
    assert_eq!(
        rows(&mut tk, "select authentication_string from mysql.user where user='SYSTEM.u_sys' and host='%'"),
        [[tidb_privilege::encode_password("")]]
    );

    run(&mut tk, "set sql_mode=''");
    contains_err(&mut tk, "grant select on *.* to u_auto@'%'", need_prefix);
    assert!(rows(&mut tk, "select User from mysql.user where User='u_auto' and Host='%'").is_empty());
    run(&mut tk, "set sql_mode=default");

}

/// A session bound to keyspace `ks_b` on a node whose configured keyspace is
/// `ks_a` (`rust/docs/design/2026-11-24-multi-keyspace-server.md`, §6.1)
/// validates new account names against `ks_b`'s prefix, with Go's Starter
/// `ErrUserNameNeedPrefix`, and resolves unprefixed targets to `ks_b.`
/// accounts.
#[test]
fn account_statements_use_the_session_keyspace_prefix() {
    if !tidb_config_kerneltype::is_next_gen() {
        // The starter deploy mode exists only in the nextgen kernel.
        return;
    }
    let _modes = ProcessModes::set("ks_a", tidb_config_deploymode::Mode::Starter, false);
    let store = MockStoreDomain::create_for_keyspace("ks_b").unwrap();

    let mut tk = store.new_session().unwrap();
    assert_eq!(tk.keyspace_context().name(), "ks_b");
    // Go `exeerrors.ErrUserNameNeedPrefix`: `mysql.ErrUsername` (1468).
    let error = |session: &mut Session, sql: &str| {
        let error = session.run(sql).expect_err(sql).to_mysql_error();
        assert_eq!(error.code, 1468, "{sql}: {}", error.message);
        error.message
    };
    let run = |session: &mut Session, sql: &str| {
        session
            .run(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    };

    assert_eq!(
        error(&mut tk, "create user `ks_a.x`@`%`"),
        "User name must start with `ks_b.` (use `ks_b.ks_a.x` instead)"
    );
    assert_eq!(
        error(&mut tk, "create user x@`%`"),
        "User name must start with `ks_b.` (use `ks_b.x` instead)"
    );
    run(&mut tk, "create user `ks_b.x`@`%`");
    assert_eq!(
        row_text(tk.run("select User from mysql.user where User like 'ks_%'")),
        [["ks_b.x"]]
    );
    assert_eq!(
        error(&mut tk, "rename user `ks_b.x`@`%` to `ks_a.y`@`%`"),
        "User name must start with `ks_b.` (use `ks_b.ks_a.y` instead)"
    );
    // An unprefixed target resolves to the session keyspace's account.
    run(&mut tk, "alter user x@`%` identified by 'pwd'");
    assert_eq!(
        row_text(tk.run("select authentication_string from mysql.user where User='ks_b.x'")),
        [[tidb_privilege::encode_password("pwd")]]
    );

}

/// A session bound to keyspace `ks_b` on a node whose configured
/// keyspace is `ks_a` (multi-keyspace design §6.1) reports `ks_b` in its
/// slow log and statement summary, and its requests' resource-group tags
/// carry `ks_b`.
#[test]
fn observation_and_resource_tags_name_the_session_keyspace() {
    if !tidb_config_kerneltype::is_next_gen() {
        // Named mock keyspaces exist only in the nextgen kernel.
        return;
    }
    let _modes = ProcessModes::set("ks_a", tidb_config_deploymode::get(), false);
    let store = MockStoreDomain::create_for_keyspace("ks_b").unwrap();
    let mut session = store.new_session().unwrap();

    assert_eq!(session.observed_keyspace_name(), "ks_b");
    let keyspace_id = session.keyspace_context().id().expect("a named store's keyspace ID");
    assert_eq!(session.observed_keyspace_id(), keyspace_id);

    session.context_with_initialized_exec_details("select 1");
    let tagger = session
        .statement_kv_context_cell()
        .get()
        .resource_group_tagger
        .expect("the statement's tagger");
    let tag = tidb_proto::ResourceGroupTag::decode(tagger().encode_tag_with_key(&[]).as_slice()).unwrap();
    assert_eq!(tag.keyspace_name.as_deref(), Some(&b"ks_b"[..]));

}

/// The refusal a statement answered with (code and message), or `None`
/// for a statement that ran.
fn refusal(session: &mut Session, sql: &str) -> Option<(u16, String)> {
    session.run(sql).err().map(|error| {
        let error = error.to_mysql_error();
        (error.code, error.message)
    })
}

/// A multi-keyspace node refuses IMPORT INTO, its job statements, BR (and
/// its job listings) and PLAN REPLAYER with `ER_NOT_SUPPORTED_YET` (1235) before they reach the
/// process-wide import store, DXF task managers, BRIE node or external
/// storage (design §10 phase 2, until phase 3f moves them into each keyspace
/// runtime). The counting import store and the BRIE queue see no access,
/// and no statement reports the missing task manager it would have consulted. A
/// single-keyspace node runs the same statements as before: they reach the
/// import store and the task manager.
#[test]
fn a_multi_keyspace_node_refuses_import_br_and_plan_replayer() {
    if !tidb_config_kerneltype::is_next_gen() {
        // Multi-keyspace serving is nextgen only (a classic store names no
        // keyspace for the session to act for).
        return;
    }
    let _modes = ProcessModes::set(tidb_keyspace::SYSTEM, tidb_config_deploymode::get(), false);
    let store = MockStoreDomain::create().unwrap();
    let mut session = store.new_session().unwrap();
    session.run("use test").unwrap();
    session.run("create table t (a int)").unwrap();
    let import_store = std::sync::Arc::new(tidb_dxf_importinto::counting_store::CountingImportStore::default());
    tidb_dxf_importinto::set_import_store(Some(import_store.clone()));
    tidb_dxf_storage::set_task_manager(None);

    let statements = [
        "import into t from 's3://bucket/data.csv'",
        "import into t from select a from t",
        "show import jobs",
        "show import job 1",
        "cancel import job 1",
        "backup database * to 'local:///tmp/br-multi-keyspace'",
        "restore database * from 'local:///tmp/br-multi-keyspace'",
        "show backups",
        "show restores",
        "show br job 1",
        "show br job query 1",
        "cancel br job 1",
        "plan replayer dump explain select a from t",
    ];
    let brie_queue = &*tidb_executor::brie::GLOBAL_BRIE_QUEUE;
    let brie_accesses = brie_queue.accesses();
    tidb_keyspace::set_multi_keyspace_mode(true);
    for sql in statements {
        let (code, message) = refusal(&mut session, sql).unwrap_or_else(|| panic!("{sql} ran"));
        assert_eq!(code, 1235, "{sql}: {message}");
        assert!(message.ends_with("on a multi-keyspace node'"), "{sql}: {message}");
    }
    assert_eq!(import_store.calls(), 0);
    assert_eq!(brie_queue.accesses(), brie_accesses);

    // A single-keyspace node goes on to the task manager and the import store.
    tidb_keyspace::set_multi_keyspace_mode(false);
    for sql in statements {
        if let Some((_, message)) = refusal(&mut session, sql) {
            assert!(!message.contains("multi-keyspace"), "{sql}: {message}");
        }
    }
    assert_eq!(
        refusal(&mut session, "show import jobs").map(|(_, message)| message).as_deref(),
        Some("task manager is not initialized")
    );
    assert!(import_store.calls() > 0);
    assert!(brie_queue.accesses() > brie_accesses);
    tidb_dxf_importinto::set_import_store(None);
}

/// A multi-keyspace node refuses the DDL whose effect PD keeps by bare
/// object IDs -- TiFlash replicas -- and `AUTO_ID_CACHE=1` tables, whose
/// IDs come from the AutoID service only the configured keyspace runs, with
/// `ER_NOT_SUPPORTED_YET` (1235) before any PD client is called (design §10
/// phase 2; 3f for the AutoID service). No table is created.
#[test]
fn a_multi_keyspace_node_refuses_pd_backed_ddl() {
    if !tidb_config_kerneltype::is_next_gen() {
        // Multi-keyspace serving is nextgen only.
        return;
    }
    let _modes = ProcessModes::set(tidb_keyspace::SYSTEM, tidb_config_deploymode::get(), false);
    let store = MockStoreDomain::create().unwrap();
    let mut session = store.new_session().unwrap();
    for sql in ["use test", "create table t (a int) partition by range (a) (partition p0 values less than (10))"] {
        session.run(sql).unwrap();
    }

    let statements = [
        "alter database test set tiflash replica 1",
        "alter table t set tiflash replica 1",
        "create table t3 (a int primary key auto_increment) auto_id_cache = 1",
        "alter table t auto_id_cache = 1",
    ];
    tidb_keyspace::set_multi_keyspace_mode(true);
    for sql in statements {
        let (code, message) = refusal(&mut session, sql).unwrap_or_else(|| panic!("{sql} ran"));
        assert_eq!(code, 1235, "{sql}: {message}");
        assert!(message.ends_with("on a multi-keyspace node'"), "{sql}: {message}");
    }
    let created = row_text(session.run(
        "select count(*) from information_schema.tables where table_schema = 'test' and table_name = 't3'",
    ));
    assert_eq!(created, [["0"]]);
    // The tenant's other DDL runs.
    assert_eq!(refusal(&mut session, "create table t4 (a int) auto_id_cache = 100"), None);
    assert_eq!(refusal(&mut session, "alter table t4 add column b int"), None);
    tidb_keyspace::set_multi_keyspace_mode(false);
}

/// A TiFlash replica manager that counts its PD calls over another one.
struct CountingTiFlash {
    calls: std::sync::atomic::AtomicUsize,
    inner: std::sync::Arc<dyn tidb_domain_infosync::tiflash_manager::TiFlashReplicaManager>,
}

impl CountingTiFlash {
    fn calls(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }
    fn count(&self) {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

impl tidb_domain_infosync::tiflash_manager::TiFlashReplicaManager for CountingTiFlash {
    fn set_tiflash_group_config(&self) -> Result<(), String> {
        self.count();
        self.inner.set_tiflash_group_config()
    }
    fn set_placement_rule(&self, rule: &tikv_client::pd_http::Rule) -> Result<(), String> {
        self.count();
        self.inner.set_placement_rule(rule)
    }
    fn set_placement_rule_batch(&self, rules: &[tikv_client::pd_http::Rule]) -> Result<(), String> {
        self.count();
        self.inner.set_placement_rule_batch(rules)
    }
    fn get_placement_rule(&self, table_id: i64) -> Result<tikv_client::pd_http::Rule, String> {
        self.count();
        self.inner.get_placement_rule(table_id)
    }
    fn post_accelerate_schedule_batch(&self, table_ids: &[i64]) -> Result<(), String> {
        self.count();
        self.inner.post_accelerate_schedule_batch(table_ids)
    }
    fn update_tiflash_progress_cache(&self, table_id: i64, progress: f64) {
        self.inner.update_tiflash_progress_cache(table_id, progress);
    }
    fn get_tiflash_progress_from_cache(&self, table_id: i64) -> Option<f64> {
        self.inner.get_tiflash_progress_from_cache(table_id)
    }
    fn delete_tiflash_progress_from_cache(&self, table_id: i64) {
        self.inner.delete_tiflash_progress_from_cache(table_id);
    }
    fn clean_tiflash_progress_cache(&self) {
        self.inner.clean_tiflash_progress_cache();
    }
    fn get_stores_stat(&self) -> Result<tikv_client::pd_http::StoresInfo, String> {
        self.count();
        self.inner.get_stores_stat()
    }
    fn sync_tiflash_table_schema(&self, table_id: i64, stores: &[tikv_client::pd_http::StoreInfo]) -> Result<(), String> {
        self.count();
        self.inner.sync_tiflash_table_schema(table_id, stores)
    }
    fn get_region_count_from_pd(&self, table_id: i64) -> Result<i64, String> {
        self.count();
        self.inner.get_region_count_from_pd(table_id)
    }
    fn calculate_tiflash_progress(
        &self,
        table_id: i64,
        replica_count: u64,
        tiflash_stores: &[tikv_client::pd_http::StoreInfo],
    ) -> Result<(f64, f64), String> {
        self.count();
        self.inner.calculate_tiflash_progress(table_id, replica_count, tiflash_stores)
    }
}

/// Inherited TiFlash replicas on a multi-keyspace node (design §10 phase
/// 2): a table whose TiFlash replica was set before keeps it in its
/// TableInfo, and a partition change or TRUNCATE would configure its new IDs
/// in PD inside the step, under rule IDs that carry no keyspace: refused
/// with 1235 before any TiFlash manager call. A single-keyspace node makes
/// the calls as before.
#[test]
fn a_multi_keyspace_node_writes_no_inherited_tiflash_rules() {
    if !tidb_config_kerneltype::is_next_gen() {
        // Multi-keyspace serving is nextgen only.
        return;
    }
    let _modes = ProcessModes::set(tidb_keyspace::SYSTEM, tidb_config_deploymode::get(), false);
    let tenant = MockStoreDomain::create_with_mock_tiflash(1).unwrap();
    let tiflash = std::sync::Arc::new(CountingTiFlash {
        calls: std::sync::atomic::AtomicUsize::new(0),
        inner: tenant.info_syncer().tiflash_replica_manager(),
    });
    tenant.info_syncer().init_tiflash_replica_manager(Some(tiflash.clone()));

    // Single-keyspace: the TiFlash replica reaches PD.
    let mut a = tenant.new_session().unwrap();
    for sql in [
        "use test",
        "create table ft (a int) partition by range (a) (partition p0 values less than (10))",
        "alter table ft set tiflash replica 1",
    ] {
        a.run(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    assert!(tiflash.calls() > 0);
    let tiflash_calls = tiflash.calls();

    tidb_keyspace::set_multi_keyspace_mode(true);
    // The inherited TiFlash replica: refused before the manager is called.
    for sql in ["alter table ft add partition (partition p1 values less than (20))", "truncate table ft"] {
        let (code, message) = refusal(&mut a, sql).unwrap_or_else(|| panic!("{sql} ran"));
        assert_eq!(code, 1235, "{sql}: {message}");
        assert!(message.ends_with("TIFLASH REPLICA on a multi-keyspace node'"), "{sql}: {message}");
    }
    assert_eq!(tiflash.calls(), tiflash_calls, "no TiFlash manager call in multi-keyspace mode");
    assert_eq!(row_text(a.run("select count(*) from mysql.tidb_ddl_job")), [["0"]]);

    tidb_keyspace::set_multi_keyspace_mode(false);
}

/// A tenant of a multi-keyspace node sets its keyspace's GLOBAL variables
/// but not the process's (design §6.3): a variable whose value is the
/// process's memory, log, PD client or watchdog, or one Go scopes to the
/// instance, answers `ER_SPECIFIC_ACCESS_DENIED_ERROR` (1227) for `SET
/// GLOBAL` and `SET INSTANCE`. A single-keyspace node sets them as before.
#[test]
fn a_tenant_cannot_set_process_variables() {
    if !tidb_config_kerneltype::is_next_gen() {
        // Multi-keyspace serving is nextgen only.
        return;
    }
    let _modes = ProcessModes::set(tidb_keyspace::SYSTEM, tidb_config_deploymode::get(), false);
    let store = MockStoreDomain::create().unwrap();
    let mut session = store.new_session().unwrap();
    let process_variables = [
        "set global tidb_enable_instance_plan_cache = on",
        "set global tidb_schema_cache_size = 1073741824",
        "set global tidb_memory_usage_alarm_ratio = 0.5",
        "set global tidb_general_log = 1",
        "set instance tidb_general_log = 1",
        "set tidb_general_log = 1",
        "set global tidb_expensive_query_time_threshold = 120",
        // Hooks writing another crate's process state.
        "set global tidb_mem_quota_analyze = 1073741824",
        "set global tidb_top_sql_max_time_series_count = 77",
        "set global tidb_enable_resource_control = off",
    ];

    tidb_keyspace::set_multi_keyspace_mode(true);
    let resource_control_client = tikv_client::resource_control_enabled();
    for sql in process_variables {
        let (code, message) = refusal(&mut session, sql).unwrap_or_else(|| panic!("{sql} ran"));
        assert_eq!(code, 1227, "{sql}: {message}");
        assert!(message.contains("RESTRICTED_VARIABLES_ADMIN"), "{sql}: {message}");
    }
    // PD keeps one external timestamp per cluster, not per keyspace: a
    // tenant cannot move it for every keyspace.
    let (code, message) =
        refusal(&mut session, "set global tidb_external_ts = 1000").expect("set global tidb_external_ts ran");
    assert_eq!(code, 1227, "{message}");
    // The refused SET reached no process switch.
    assert_eq!(tikv_client::resource_control_enabled(), resource_control_client);
    // The keyspace's own variables, and the session's.
    for sql in ["set global tidb_enable_foreign_key = off", "set global tidb_enable_foreign_key = on", "set tidb_mem_quota_query = 1073741824"] {
        assert_eq!(refusal(&mut session, sql), None, "{sql}");
    }

    tidb_keyspace::set_multi_keyspace_mode(false);
    for sql in process_variables {
        assert_eq!(refusal(&mut session, sql), None, "{sql}");
    }
    for sql in [
        "set global tidb_enable_instance_plan_cache = default",
        "set global tidb_schema_cache_size = default",
        "set global tidb_memory_usage_alarm_ratio = default",
        "set global tidb_general_log = 0",
        "set global tidb_expensive_query_time_threshold = default",
        "set global tidb_mem_quota_analyze = default",
        "set global tidb_top_sql_max_time_series_count = default",
        "set global tidb_enable_resource_control = on",
    ] {
        assert_eq!(refusal(&mut session, sql), None, "{sql}");
    }
}

/// A PD affinity client that counts its calls and holds no groups.
#[derive(Default)]
struct CountingAffinity(std::sync::atomic::AtomicUsize);

impl CountingAffinity {
    fn calls(&self) -> usize {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
    fn count(&self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

impl tidb_domain_affinity::PdAffinityClient for CountingAffinity {
    fn create_affinity_groups(
        &self,
        _groups: &tidb_domain_affinity::AffinityGroups,
        _skip_exist_check: bool,
    ) -> Result<tidb_domain_affinity::AffinityGroupStates, tidb_domain_affinity::PdHttpError> {
        self.count();
        Ok(tidb_domain_affinity::AffinityGroupStates::new())
    }
    fn get_affinity_groups(
        &self,
        _ids: &[String],
    ) -> Result<tidb_domain_affinity::AffinityGroupStates, tidb_domain_affinity::PdHttpError> {
        self.count();
        Ok(tidb_domain_affinity::AffinityGroupStates::new())
    }
    fn get_all_affinity_groups(
        &self,
    ) -> Result<tidb_domain_affinity::AffinityGroupStates, tidb_domain_affinity::PdHttpError> {
        self.count();
        Ok(tidb_domain_affinity::AffinityGroupStates::new())
    }
    fn batch_delete_affinity_groups(&self, _ids: &[String], _force: bool) -> Result<(), tidb_domain_affinity::PdHttpError> {
        self.count();
        Ok(())
    }
}

/// The table's (or partition's) physical ID in `test`.
fn physical_id(session: &mut Session, table: &str, partition: Option<&str>) -> i64 {
    let sql = match partition {
        None => format!("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = '{table}'"),
        Some(partition) => format!(
            "select tidb_partition_id from information_schema.partitions where table_schema = 'test' and table_name = '{table}' and partition_name = '{partition}'"
        ),
    };
    row_text(session.run(&sql))[0][0].parse().unwrap()
}

/// Runs `arm` once, before the first step of a job of `action` whose saved
/// state `ready` accepts: the job was persisted by a single-keyspace node,
/// and the step that resumes it runs on a multi-keyspace node.
fn before_resumed_step(
    action: tidb_model::ActionType,
    ready: impl Fn(&tidb_model::Job) -> bool + Send + Sync + 'static,
    arm: impl Fn(&mut tidb_model::Job) + Send + Sync + 'static,
) -> tidb_failpoint::Guard {
    let armed = std::sync::atomic::AtomicBool::new(false);
    tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", move |job| {
        let Some(job) = job.downcast_mut::<tidb_model::Job>() else {
            return;
        };
        if job.type_ == action && ready(job) && !armed.swap(true, std::sync::atomic::Ordering::SeqCst) {
            arm(job);
            tidb_keyspace::set_multi_keyspace_mode(true);
        }
    })
}

/// A materialized view's out-of-place cutover job, DONE and persisted, is
/// finished by a separate step that cleans up after the old MV ID (Go
/// `transitOneJobStep`'s done branch). On a multi-keyspace node that
/// cleanup deletes the tenant's own refresh alert, but neither the PD
/// affinity group nor the process-wide TiFlash progress entries, which are
/// named by bare table IDs another tenant may also use (design §10 phase 2).
/// A single-keyspace node does both as before.
#[test]
fn a_multi_keyspace_node_leaves_pd_and_tiflash_progress_alone_after_a_cutover() {
    if !tidb_config_kerneltype::is_next_gen() {
        // Multi-keyspace serving is nextgen only.
        return;
    }
    let _modes = ProcessModes::set(tidb_keyspace::SYSTEM, tidb_config_deploymode::get(), false);
    let store = MockStoreDomain::create().unwrap();
    let mut session = store.new_session().unwrap();
    // Go master's materialized-view system tables, which the mock store's
    // bootstrap does not create.
    for table in tidb_metadef::BOOTSTRAP_TABLES.iter().filter(|table| {
        matches!(
            table.name,
            "tidb_mview_refresh_info" | "tidb_mlog_purge_info" | "tidb_mview_refresh_hist" | "tidb_mview_refresh_alert" | "tidb_mlog_purge_hist"
        )
    }) {
        session.run(table.create_sql).unwrap_or_else(|error| panic!("{}: {error:?}", table.name));
    }
    session.run(tidb_metadef::system_tables_def::CREATE_TI_DBMVIEW_FAST_REFRESH_SOURCE_INFO_TABLE).unwrap();
    for sql in [
        "set tidb_mview_enable = on",
        "use test",
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10), (2, 7)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
    ] {
        session.run(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    let affinity = std::sync::Arc::new(CountingAffinity::default());
    tidb_domain_affinity::init_manager(Some(affinity.clone()));
    let alert = |id: i64| {
        format!(
            "insert into mysql.tidb_mview_refresh_alert (MVIEW_ID, MVIEW_SCHEMA, MVIEW_NAME, ALERT_LEVEL, LAST_SUCCESS_SNAPSHOT_TIME, UPDATE_TIME) values ({id}, 'test', 'mv', 'warning', UTC_TIMESTAMP(), UTC_TIMESTAMP())"
        )
    };
    let alerts = |session: &mut Session, id: i64| {
        row_text(session.run(&format!("select count(*) from mysql.tidb_mview_refresh_alert where MVIEW_ID = {id}")))
    };
    let cache = |id| tidb_domain_infosync::info_syncer::get_tiflash_progress_from_cache(id);

    // Single-keyspace: the cutover deletes the alert, the affinity group
    // and the progress entry.
    let old = physical_id(&mut session, "mv", None);
    session.run(&alert(old)).unwrap();
    tidb_domain_infosync::info_syncer::update_tiflash_progress_cache(old, 0.5);
    session.run("refresh materialized view mv complete out of place").unwrap();
    assert_ne!(physical_id(&mut session, "mv", None), old);
    assert_eq!(alerts(&mut session, old), [["0"]]);
    assert_eq!(cache(old), None);
    assert!(affinity.calls() > 0, "the single-keyspace cutover deletes the old affinity group");
    let affinity_calls = affinity.calls();

    // Multi-keyspace: another keyspace's progress entry for the same ID survives.
    tidb_keyspace::set_multi_keyspace_mode(true);
    let old = physical_id(&mut session, "mv", None);
    session.run(&alert(old)).unwrap();
    tidb_domain_infosync::info_syncer::update_tiflash_progress_cache(old, 0.5);
    assert_eq!(refusal(&mut session, "refresh materialized view mv complete out of place"), None);
    assert_ne!(physical_id(&mut session, "mv", None), old);
    assert_eq!(
        row_text(session.run(
            "select state from information_schema.ddl_jobs where job_type = 'refresh materialized view complete out-of-place cutover' order by job_id desc limit 1"
        )),
        [["synced"]]
    );
    assert_eq!(alerts(&mut session, old), [["0"]], "the tenant's own alert is cleaned up");
    assert_eq!(cache(old), Some(0.5), "another keyspace's progress entry survives");
    assert_eq!(affinity.calls(), affinity_calls, "no affinity call in multi-keyspace mode");

    tidb_keyspace::set_multi_keyspace_mode(false);
    tidb_domain_infosync::info_syncer::delete_tiflash_progress_from_cache(old);
    tidb_domain_affinity::init_manager(None);
}

/// An ADD PARTITION of a table with an available TiFlash replica, saved in
/// its REPLICA_ONLY state by a single-keyspace node, would check the new
/// partitions' replication in PD and write their progress into the
/// process-wide cache by bare partition ID. Resumed on a multi-keyspace node
/// the step is refused with 1235 before either: the job is cancelled and
/// rolled back, the table keeps its partitions, and another keyspace's cache
/// entry for the same partition ID survives. A single-keyspace node writes
/// the entry as before.
#[test]
fn a_resumed_add_partition_writes_no_tiflash_progress_on_a_multi_keyspace_node() {
    if !tidb_config_kerneltype::is_next_gen() {
        // Multi-keyspace serving is nextgen only.
        return;
    }
    let _modes = ProcessModes::set(tidb_keyspace::SYSTEM, tidb_config_deploymode::get(), false);
    let store = MockStoreDomain::create_with_mock_tiflash(1).unwrap();
    let mut session = store.new_session().unwrap();
    for sql in [
        "use test",
        "create table ft (a int) partition by range (a) (partition p0 values less than (10))",
        "alter table ft set tiflash replica 1",
    ] {
        session.run(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    use tidb_session::domain::cluster_session_node::DomainTestSessions as _;
    store.factory().update_table_replica_info(physical_id(&mut session, "ft", Some("p0")), true).unwrap();
    // The mock's regions have no TiFlash peers: Go's failpoint reports the
    // new partitions replicated, so only the guard keeps the cache write out.
    let _replicated = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/mockWaitTiFlashReplicaOK", "return(true)");
    let cache = |id| tidb_domain_infosync::info_syncer::get_tiflash_progress_from_cache(id);

    // Single-keyspace: the new partition's progress is cached.
    session.run("alter table ft add partition (partition p1 values less than (20))").unwrap();
    let p1 = physical_id(&mut session, "ft", Some("p1"));
    assert_eq!(cache(p1), Some(1.0));

    // Saved at REPLICA_ONLY, resumed on a multi-keyspace node.
    let adding = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let seeded = adding.clone();
    let _resume = before_resumed_step(
        tidb_model::ActionType::ACTION_ADD_TABLE_PARTITION,
        |job| job.schema_state == tidb_model::SchemaState::REPLICA_ONLY && job.is_running(),
        move |job| {
            let args = tidb_model::get_table_partition_args(&mut job.clone()).unwrap().unwrap();
            let part_info = args.read().part_info.get().unwrap().read().clone_like_go();
            for definition in part_info.definitions.snapshot() {
                // Another keyspace's table with the same partition ID.
                tidb_domain_infosync::info_syncer::update_tiflash_progress_cache(definition.id, 0.25);
                seeded.lock().unwrap().push(definition.id);
            }
        },
    );
    let (code, message) = refusal(&mut session, "alter table ft add partition (partition p2 values less than (30))")
        .expect("the resumed ADD PARTITION is refused");
    assert_eq!(code, 1235, "{message}");
    assert!(message.ends_with("TIFLASH REPLICA on a multi-keyspace node'"), "{message}");
    let adding = adding.lock().unwrap().clone();
    assert!(!adding.is_empty(), "the job reached REPLICA_ONLY");
    for id in &adding {
        assert_eq!(cache(*id), Some(0.25), "another keyspace's progress entry survives");
    }
    assert_eq!(
        row_text(session.run(
            "select state from information_schema.ddl_jobs where job_type = 'add partition' order by job_id desc limit 1"
        )),
        [["rollback done"]]
    );
    assert_eq!(
        row_text(session.run(
            "select group_concat(partition_name order by partition_ordinal_position) from information_schema.partitions where table_schema = 'test' and table_name = 'ft'"
        )),
        [["p0,p1"]]
    );
    assert_eq!(row_text(session.run("select count(*) from mysql.tidb_ddl_job")), [["0"]]);

    tidb_keyspace::set_multi_keyspace_mode(false);
    for id in adding.into_iter().chain([p1]) {
        tidb_domain_infosync::info_syncer::delete_tiflash_progress_from_cache(id);
    }
}

/// A columnar index job, saved in WRITE_REORGANIZATION with its snapshot
/// version set by a single-keyspace node, would poll the index build's
/// progress by bare table and index IDs and could make the index public on
/// another tenant's progress. Resumed on a multi-keyspace node the step is
/// refused with 1235 before the poll: the job is cancelled and rolled back,
/// and the index does not exist. The poll here is the mock that reports the
/// build done, so without the guard the index would become public.
#[test]
fn a_resumed_columnar_index_does_not_poll_progress_on_a_multi_keyspace_node() {
    if !tidb_config_kerneltype::is_next_gen() {
        // Multi-keyspace serving is nextgen only.
        return;
    }
    let _modes = ProcessModes::set(tidb_keyspace::SYSTEM, tidb_config_deploymode::get(), false);
    let store = MockStoreDomain::create_with_mock_tiflash(2).unwrap();
    tidb_domain_infosync::info_syncer::set_mock_tiflash(std::sync::Arc::new(
        tidb_domain_infosync::tiflash_manager::MockTiFlash::new(),
    ));
    let mut session = store.new_session().unwrap();
    for sql in ["use test", "create table vt (a int, c vector(3))", "alter table vt set tiflash replica 2"] {
        session.run(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    let _done = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess", "return(1)");
    let _resume = before_resumed_step(
        tidb_model::ActionType::ACTION_ADD_COLUMNAR_INDEX,
        |job| {
            job.schema_state == tidb_model::SchemaState::WRITE_REORGANIZATION
                && job.snapshot_version != 0
                && job.is_running()
        },
        |_| {},
    );
    let (code, message) =
        refusal(&mut session, "alter table vt add vector index vidx((vec_cosine_distance(c))) using hnsw")
            .expect("the resumed columnar index is refused");
    assert_eq!(code, 1235, "{message}");
    assert!(message.ends_with("COLUMNAR INDEX on a multi-keyspace node'"), "{message}");
    assert_eq!(
        row_text(session.run(
            "select state from information_schema.ddl_jobs where job_type = 'add columnar index' order by job_id desc limit 1"
        )),
        [["rollback done"]]
    );
    assert_eq!(
        row_text(session.run(
            "select count(*) from information_schema.tidb_indexes where table_schema = 'test' and table_name = 'vt' and key_name = 'vidx'"
        )),
        [["0"]]
    );
    assert_eq!(row_text(session.run("select count(*) from mysql.tidb_ddl_job")), [["0"]]);
    tidb_keyspace::set_multi_keyspace_mode(false);
}

/// FLASHBACK CLUSTER closes and later restores PD's schedule limits, which
/// are the whole PD cluster's: a multi-keyspace node refuses it.
#[test]
fn a_multi_keyspace_node_refuses_flashback_cluster() {
    if !tidb_config_kerneltype::is_next_gen() {
        // Multi-keyspace serving is nextgen only.
        return;
    }
    let _modes = ProcessModes::set(tidb_keyspace::SYSTEM, tidb_config_deploymode::get(), false);
    let store = MockStoreDomain::create().unwrap();
    let mut session = store.new_session().unwrap();
    tidb_keyspace::set_multi_keyspace_mode(true);
    let (code, message) = refusal(&mut session, "flashback cluster to timestamp '2026-01-01 00:00:00'")
        .expect("FLASHBACK CLUSTER ran");
    assert_eq!(code, 1235, "{message}");
    assert!(message.ends_with("FLASHBACK CLUSTER on a multi-keyspace node'"), "{message}");
    assert_eq!(row_text(session.run("select count(*) from mysql.tidb_ddl_job")), [["0"]]);
    tidb_keyspace::set_multi_keyspace_mode(false);
}

/// A schedule manager that keeps its configuration and counts its writes.
#[derive(Debug, Default)]
struct CountingSchedule {
    config: std::sync::Mutex<std::collections::HashMap<String, serde_json::Value>>,
    writes: std::sync::atomic::AtomicUsize,
}

impl tidb_domain_infosync::schedule_manager::ScheduleManager for CountingSchedule {
    fn get_schedule_config(&self) -> Result<std::collections::HashMap<String, serde_json::Value>, String> {
        Ok(self.config.lock().unwrap().clone())
    }

    fn set_schedule_config(&self, config: &std::collections::HashMap<String, serde_json::Value>) -> Result<(), String> {
        self.writes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.config.lock().unwrap().extend(config.iter().map(|(key, value)| (key.clone(), value.clone())));
        Ok(())
    }
}

/// A FLASHBACK CLUSTER job saved PD's schedule limits in `DELETE_ONLY` and
/// carries no record of its close (a Go node's job, or this port's before
/// the record). Resumed on a multi-keyspace node, as on any node, it is
/// cancelled in `DELETE_ONLY`, writes nothing to PD, and hands the saved
/// value off to the operator.
#[test]
fn a_resumed_flashback_without_a_pd_schedule_record_is_cancelled_on_a_multi_keyspace_node() {
    if !tidb_config_kerneltype::is_next_gen() {
        // Multi-keyspace serving is nextgen only.
        return;
    }
    let _modes = ProcessModes::set(tidb_keyspace::SYSTEM, tidb_config_deploymode::get(), false);
    let store = MockStoreDomain::create().unwrap();
    let mut session = store.new_session().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));
    session.run("begin").unwrap();
    let ts: u64 = row_text(session.run("select @@tidb_current_ts"))[0][0].parse().unwrap();
    session.run("rollback").unwrap();
    let inject_safe_ts = (((ts >> 18) + 10_000) << 18).to_string();
    let _mock = [
        tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/mockFlashbackTest", "return(true)"),
        tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/injectSafeTS", &format!("return({inject_safe_ts})")),
    ];
    let safe_point = (chrono::Local::now() - chrono::Duration::hours(48)).format("%Y%m%d-%H:%M:%S%.3f %z").to_string();
    session.run("delete from mysql.tidb where variable_name in ('tikv_gc_safe_point', 'tikv_gc_enable')").unwrap();
    session
        .run(&format!("insert into mysql.tidb values ('tikv_gc_safe_point', '{safe_point}', '')"))
        .unwrap();
    let schedule = std::sync::Arc::new(CountingSchedule::default());
    schedule.config.lock().unwrap().insert("merge-schedule-limit".to_owned(), serde_json::json!(9));
    tidb_domain_infosync::info_syncer::init_schedule_manager(Some(schedule.clone()));
    let states = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = states.clone();
    let _after = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", move |job| {
        if let Some(job) = job.downcast_mut::<tidb_model::Job>() {
            seen.lock().unwrap().push(job.schema_state);
        }
    });
    let _resumed = before_resumed_step(
        tidb_model::ActionType::ACTION_FLASHBACK_CLUSTER,
        |job| job.schema_state == tidb_model::SchemaState::DELETE_ONLY,
        |job| {
            // The arguments as a node without the record left them.
            let args = tidb_model::get_flashback_cluster_args(job).unwrap().unwrap();
            assert_eq!(args.read().pd_schedule_state, tidb_model::PD_SCHEDULE_SAVED);
            args.write().pd_schedule_state.clear();
            job.fill_args(Some(args));
            job.encode(true).unwrap();
        },
    );
    let time = chrono::TimeZone::timestamp_millis_opt(&chrono::Local, (ts >> 18) as i64).unwrap();
    let sql = format!("flashback cluster to timestamp '{}'", time.format("%Y-%m-%d %H:%M:%S%.6f"));
    let refused = refusal(&mut session, &sql);
    tidb_keyspace::set_multi_keyspace_mode(false);
    tidb_domain_infosync::info_syncer::init_schedule_manager(None);
    let (code, message) = refused.expect("the resumed job was cancelled");
    assert_eq!(code, 1105, "{message}");
    assert!(message.contains("hands them off to the operator"), "{message}");
    let states = states.lock().unwrap().clone();
    assert!(
        !states.iter().any(|state| matches!(*state, tidb_model::SchemaState::WRITE_ONLY | tidb_model::SchemaState::WRITE_REORGANIZATION)),
        "the job advanced: {states:?}"
    );
    assert_eq!(schedule.writes.load(std::sync::atomic::Ordering::SeqCst), 0, "PD's limits were written");
    assert_eq!(schedule.config.lock().unwrap()["merge-schedule-limit"], serde_json::json!(9));
    let meta = row_text(session.run("select job_meta from mysql.tidb_ddl_history order by job_id desc limit 1")).remove(0).remove(0);
    let job: serde_json::Value = serde_json::from_str(&meta).unwrap();
    assert_eq!(job["state"], serde_json::json!(tidb_model::JobState::CANCELLED.0), "{job}");
    let warning = job["warning"].to_string();
    assert!(warning.contains("left as they are") && warning.contains("merge-schedule-limit"), "{warning}");
}

/// A keyspace runtime's DDL status provider is its keyspace's from its first
/// registration: while the second keyspace's startup is held right after
/// it registered its DDL statuses, before its keyspace identity is bound,
/// the first keyspace still reads only its own `server_id`, and the opening
/// keyspace reads its own.
#[test]
fn an_opening_keyspace_shows_its_ddl_statuses_to_no_other_keyspace() {
    if !tidb_config_kerneltype::is_next_gen() {
        // Multi-keyspace serving is nextgen only.
        return;
    }
    let _modes = ProcessModes::set("ks_a", tidb_config_deploymode::get(), false);
    let server_id = |keyspace: &str| {
        tidb_variable::statusvar::get_keyspace_status_vars(None, Some(keyspace)).unwrap()["server_id"].value.clone()
    };
    let _a = MockStoreDomain::create_for_keyspace("ks_a").unwrap();
    let a_id = server_id("ks_a");
    let held = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = held.clone();
    let hold = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/domain/afterRegisterDDLStatistics",
        move |id| {
            let id = id.downcast_ref::<String>().expect("the provider's server_id").clone();
            seen.lock().unwrap().push((id, server_id("ks_a"), server_id("ks_b")));
        },
    );
    let _b = MockStoreDomain::create_for_keyspace("ks_b").unwrap();
    drop(hold);
    let held = held.lock().unwrap().clone();
    assert!(!held.is_empty(), "the opening keyspace registered no DDL statuses");
    for (b_id, a_sees, b_sees) in &held {
        assert_ne!(b_id, &a_id);
        assert_eq!(a_sees, &a_id, "ks_a read the opening keyspace's DDL statuses");
        assert_eq!(b_sees, b_id, "the opening keyspace reads its own DDL statuses");
    }
    assert_eq!(server_id("ks_a"), a_id);
}

/// Design §6.4: the information-schema row counts come from each domain's
/// own cache. Two keyspaces hold a table under the same physical ID; one
/// has its row count in `mysql.stats_meta`, the other none: the other reads
/// zero rows, not the first keyspace's count (the cache keeps entries a
/// refresh does not mention).
#[test]
fn a_keyspace_reads_only_its_own_table_row_counts() {
    if !tidb_config_kerneltype::is_next_gen() {
        // Multi-keyspace serving is nextgen only.
        return;
    }
    let _modes = ProcessModes::set("ks_a", tidb_config_deploymode::get(), false);
    let a_store = MockStoreDomain::create_for_keyspace("ks_a").unwrap();
    let b_store = MockStoreDomain::create_for_keyspace("ks_b").unwrap();
    let (mut a, mut b) = (a_store.new_session().unwrap(), b_store.new_session().unwrap());
    for session in [&mut a, &mut b] {
        session.run("create table test.row_count_probe (id int primary key)").unwrap();
    }
    let id = |session: &mut Session| physical_id(session, "row_count_probe", None);
    assert_eq!(id(&mut a), id(&mut b), "the keyspaces' DDL histories differ");
    let table_id = id(&mut a);
    b.run("insert into test.row_count_probe values (1), (2), (3)").unwrap();
    b.run(&format!("insert into mysql.stats_meta (version, table_id, count, modify_count) values (1, {table_id}, 3, 0) on duplicate key update count = 3")).unwrap();
    a.run(&format!("delete from mysql.stats_meta where table_id = {table_id}")).unwrap();
    tidb_keyspace::set_multi_keyspace_mode(true);
    let rows = |session: &mut Session| {
        row_text(session.run(
            "select table_rows from information_schema.tables where table_schema = 'test' and table_name = 'row_count_probe'",
        ))
    };
    assert_eq!(rows(&mut b), [["3"]]);
    assert_eq!(rows(&mut a), [["0"]], "ks_a read ks_b's row count");
    tidb_keyspace::set_multi_keyspace_mode(false);
}

/// Design §6.4: the DDL job version and the global index V1 flag are not a
/// tenant's. Only the classic kernel's DDL detects them from its peers'
/// versions; a multi-keyspace node is next-generation, where no DDL writes
/// them, so opening keyspaces and running their DDL leaves the process's
/// one value as it was.
#[test]
fn opening_keyspaces_leaves_the_job_version_alone() {
    if !tidb_config_kerneltype::is_next_gen() {
        // Multi-keyspace serving is nextgen only.
        return;
    }
    let _modes = ProcessModes::set("ks_a", tidb_config_deploymode::get(), false);
    let version = tidb_model::get_job_ver_in_use();
    let global_index_v1 = tidb_model::index::get_global_index_v_1_supported();
    let a_store = MockStoreDomain::create_for_keyspace("ks_a").unwrap();
    let b_store = MockStoreDomain::create_for_keyspace("ks_b").unwrap();
    for store in [&a_store, &b_store] {
        let mut session = store.new_session().unwrap();
        session.run("create table test.job_version_probe (a int, key i (a))").unwrap();
    }
    assert_eq!(tidb_model::get_job_ver_in_use(), version);
    assert_eq!(tidb_model::index::get_global_index_v_1_supported(), global_index_v1);
}

/// Phase 3d's gate (design §6.4, §10): two keyspaces of one node run DDL
/// at the same time with colliding job and table IDs, with ANALYZE and
/// statistics loads, and neither sees the other's: each keeps its own
/// indexes, columns, rows, job history and statistics.
#[test]
fn two_keyspaces_run_ddl_with_colliding_ids_at_once() {
    if !tidb_config_kerneltype::is_next_gen() {
        // Multi-keyspace serving is nextgen only.
        return;
    }
    let _modes = ProcessModes::set("ks_a", tidb_config_deploymode::get(), false);
    let stores = [
        ("a", 3, MockStoreDomain::create_for_keyspace("ks_a").unwrap()),
        ("b", 5, MockStoreDomain::create_for_keyspace("ks_b").unwrap()),
    ];
    let table_id = |session: &mut Session| physical_id(session, "t", None);
    let mut ids = Vec::new();
    for (_, rows, store) in &stores {
        let mut session = store.new_session().unwrap();
        session.run("create table test.t (a int, b int)").unwrap();
        let values = (1..=*rows).map(|i| format!("({i}, {i})")).collect::<Vec<_>>().join(", ");
        session.run(&format!("insert into test.t values {values}")).unwrap();
        session.run("use test").unwrap();
        ids.push(table_id(&mut session));
    }
    assert_eq!(ids[0], ids[1], "the keyspaces' table IDs do not collide");
    tidb_keyspace::set_multi_keyspace_mode(true);
    std::thread::scope(|scope| {
        for (tag, _, store) in &stores {
            scope.spawn(move || {
                let mut session = store.new_session().unwrap();
                session.run("use test").unwrap();
                session.run("set tidb_stats_load_sync_wait = 0").unwrap();
                // Each ADD INDEX analyzes the table in its job, through the
                // job's context (its done channel and timeouts).
                session.run("set tidb_stats_update_during_ddl = on").unwrap();
                for round in 0..3 {
                    for sql in [
                        format!("alter table t add index i_{tag}{round} (a)"),
                        format!("alter table t add column c_{tag}{round} int"),
                        "analyze table t".to_owned(),
                        "select * from t where a = 1 and b = 1".to_owned(),
                    ] {
                        session.run(&sql).unwrap_or_else(|error| panic!("{tag}: {sql}: {error:?}"));
                    }
                }
            });
        }
    });
    let mut jobs = Vec::new();
    for (tag, rows, store) in &stores {
        let other = if *tag == "a" { "b" } else { "a" };
        let mut session = store.new_session().unwrap();
        session.run("use test").unwrap();
        let names = |session: &mut Session, sql: &str, column: usize| -> Vec<String> {
            row_text(session.run(sql)).into_iter().map(|row| row[column].clone()).collect()
        };
        let indexes = names(&mut session, "show index from t", 2);
        assert_eq!(indexes, [format!("i_{tag}0"), format!("i_{tag}1"), format!("i_{tag}2")], "{tag}");
        let columns = names(&mut session, "show columns from t", 0);
        assert_eq!(columns, ["a".to_owned(), "b".to_owned(), format!("c_{tag}0"), format!("c_{tag}1"), format!("c_{tag}2")]);
        assert_eq!(row_text(session.run("select count(*) from t")), [[rows.to_string()]], "{tag}");
        assert_eq!(
            row_text(session.run("select count from mysql.stats_meta where table_id = (select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 't')")),
            [[rows.to_string()]],
            "{tag}'s statistics"
        );
        let history = row_text(session.run(
            "select job_id, query from information_schema.ddl_jobs where table_name = 't' and query like 'alter table t add%' order by job_id",
        ));
        assert_eq!(history.len(), 6, "{tag}: {history:?}");
        assert!(history.iter().all(|row| row[1].contains(&format!("_{tag}")) && !row[1].contains(&format!("_{other}"))), "{tag}: {history:?}");
        jobs.push(history.into_iter().map(|row| row[0].clone()).collect::<std::collections::BTreeSet<_>>());
    }
    tidb_keyspace::set_multi_keyspace_mode(false);
    assert!(!jobs[0].is_disjoint(&jobs[1]), "the keyspaces' job IDs do not collide: {jobs:?}");
}

/// Design §6.8 (phase 3e): two keyspaces of one node read only their own
/// diagnostics from the process-wide owners. Both run the same statement:
/// each sees only its own statement-summary rows (current, history,
/// `CLUSTER_` and cumulative) and its own `ADMIN SHOW SLOW` entries, never
/// the evicted rollup; each sees only its own deadlocks; `SHOW STATUS`
/// reports each keyspace's own DDL statuses. The sources that cannot tell
/// keyspaces apart are refused or read as empty.
#[test]
fn a_tenant_reads_only_its_own_diagnostics() {
    if !tidb_config_kerneltype::is_next_gen() {
        // Multi-keyspace serving is nextgen only.
        return;
    }
    // The mock bootstrap's resource-group DDL is refused in
    // multi-keyspace mode, so the keyspaces open before the mode is on.
    let _modes = ProcessModes::set("ks_a", tidb_config_deploymode::get(), false);
    let a_store = MockStoreDomain::create_for_keyspace("ks_a").unwrap();
    let b_store = MockStoreDomain::create_for_keyspace("ks_b").unwrap();
    let (mut a, mut b) = (a_store.new_session().unwrap(), b_store.new_session().unwrap());
    // A user's statements, which the summary records (an internal
    // session's are not).
    a.set_user("root@%".into(), "root@localhost".into());
    b.set_user("root@%".into(), "root@localhost".into());
    tidb_keyspace::set_multi_keyspace_mode(true);
    let run = |session: &mut Session, sql: &str| {
        session.run(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    };
    let one = |session: &mut Session, sql: &str| -> String {
        row_text(session.run(sql)).into_iter().next().and_then(|row| row.into_iter().next()).unwrap_or_default()
    };

    // The statement summary, keyed by keyspace.
    tidb_stmtsummary::statement_summary::STMT_SUMMARY_BY_DIGEST_MAP.clear();
    for _ in 0..2 {
        run(&mut a, "select 1 as tenant_probe");
    }
    run(&mut b, "select 1 as tenant_probe");
    let probe = "where digest_text like '%tenant_probe%'";
    for (session, want) in [(&mut a, "2"), (&mut b, "1")] {
        for table in ["statements_summary", "statements_summary_history", "cluster_statements_summary"] {
            assert_eq!(one(session, &format!("select sum(exec_count) from information_schema.{table} {probe}")), want, "{table}");
        }
        assert_eq!(one(session, &format!("select count(*) from information_schema.tidb_statements_stats {probe}")), "1");
        assert_eq!(one(session, "select count(*) from information_schema.statements_summary_evicted"), "0");
        assert_eq!(one(session, "select count(*) from information_schema.statements_summary where digest is null"), "0");
    }

    // ADMIN SHOW SLOW, one memory per keyspace.
    tidb_keyspace::set_multi_keyspace_mode(false);
    run(&mut a, "set global tidb_slow_log_threshold = 0");
    tidb_keyspace::set_multi_keyspace_mode(true);
    run(&mut a, "select 'tenant slow probe a'");
    run(&mut b, "select 'tenant slow probe b'");
    let slow = |session: &mut Session| -> Vec<String> {
        row_text(session.run("admin show slow recent 100"))
            .into_iter()
            .map(|row| row[0].clone())
            .filter(|sql| sql.contains("tenant slow probe"))
            .collect()
    };
    assert_eq!(slow(&mut a), ["select 'tenant slow probe a'"]);
    assert_eq!(slow(&mut b), ["select 'tenant slow probe b'"]);
    tidb_keyspace::set_multi_keyspace_mode(false);
    run(&mut a, "set global tidb_slow_log_threshold = 300");
    tidb_keyspace::set_multi_keyspace_mode(true);

    // Deadlocks, recorded with the detecting transaction's keyspace.
    tidb_deadlockhistory::recording::configure_deadlock_history(10, false);
    let record = || tidb_deadlockhistory::DeadlockRecord {
        occur_time: tidb_datatype::Time::new(
            tidb_datatype::CoreTime::from_date(2026, 11, 25, 9, 0, 0, 0),
            tidb_datatype::TimeType::Timestamp,
            6,
        )
        .unwrap(),
        wait_chain: vec![tidb_deadlockhistory::WaitChainItem {
            sql_digest: String::new(),
            key: Vec::new(),
            all_sql_digests: Vec::new(),
            try_lock_txn: 1,
            txn_holding_lock: 2,
        }],
        id: 0,
        is_retryable: false,
    };
    tidb_deadlockhistory::GLOBAL_DEADLOCK_HISTORY.push_in_keyspace("ks_a", record());
    tidb_deadlockhistory::GLOBAL_DEADLOCK_HISTORY.push_in_keyspace("ks_a", record());
    tidb_deadlockhistory::GLOBAL_DEADLOCK_HISTORY.push_in_keyspace("ks_b", record());
    assert_eq!(one(&mut a, "select count(*) from information_schema.deadlocks"), "2");
    assert_eq!(one(&mut b, "select count(*) from information_schema.cluster_deadlocks"), "1");
    tidb_deadlockhistory::recording::configure_deadlock_history(0, false);

    // SHOW STATUS: each domain's DDL statuses are its keyspace's.
    let status = |session: &mut Session, name: &str| -> String {
        row_text(session.run(&format!("show status like '{name}'"))).remove(0).remove(1)
    };
    let (a_version, b_version) = (status(&mut a, "ddl_schema_version"), status(&mut b, "ddl_schema_version"));
    run(&mut a, "create table test.status_probe (a int)");
    assert_ne!(status(&mut a, "ddl_schema_version"), a_version, "the keyspace's own DDL shows");
    assert_eq!(status(&mut b, "ddl_schema_version"), b_version, "another keyspace's DDL does not");

    // No keyspace-correct source: refused, or nothing.
    for sql in [
        "select * from metrics_schema.up",
        "select * from information_schema.cluster_log where time > '2026-01-01 00:00:00' and time < '2027-01-01 00:00:00'",
        "flush client_errors_summary",
        "select * from information_schema.tidb_hot_regions",
        "select * from performance_schema.tidb_profile_cpu",
    ] {
        let (code, message) = refusal(&mut b, sql).unwrap_or_else(|| panic!("{sql} ran"));
        assert_eq!(code, 1235, "{sql}: {message}");
    }
    for table in [
        "client_errors_summary_global",
        "client_errors_summary_by_user",
        "client_errors_summary_by_host",
        "memory_usage_ops_history",
    ] {
        assert!(row_text(b.run(&format!("select * from information_schema.{table}"))).is_empty(), "{table}");
    }
}

/// Each keyspace runtime keeps its own PD managers (Go's `InfoSyncer`,
/// design §6.2): opening a second keyspace leaves the first one's label
/// rules and placement where they were. Before, the second open replaced
/// the process's managers, so the first keyspace's `ATTRIBUTES` lost its
/// rule and its range placement no longer held its policy.
#[test]
fn each_keyspace_keeps_its_own_pd_managers() {
    if !tidb_config_kerneltype::is_next_gen() {
        // Multi-keyspace serving is nextgen only.
        return;
    }
    let _modes = ProcessModes::set(tidb_keyspace::SYSTEM, tidb_config_deploymode::get(), false);
    let a_store = MockStoreDomain::create_for_keyspace("ks_a").unwrap();
    let mut a = a_store.new_session().unwrap();
    for sql in [
        "use test",
        "create table t (a int)",
        "alter table t attributes = 'merge_option=deny'",
        "create placement policy p1 followers = 1",
        "alter range global placement policy p1",
    ] {
        a.run(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    let b_store = MockStoreDomain::create_for_keyspace("ks_b").unwrap();
    let mut b = b_store.new_session().unwrap();

    let attributes = "select attributes from information_schema.attributes where id like '%schema/test/t'";
    assert_eq!(row_text(a.run(attributes)), [["\"merge_option=deny\""]]);
    let (code, message) = refusal(&mut a, "drop placement policy p1").expect("the range uses p1");
    assert_eq!(code, 8241, "{message}");
    // The other keyspace sees neither.
    assert!(row_text(b.run(attributes)).is_empty());
    for sql in ["create placement policy p1 followers = 1", "drop placement policy p1"] {
        assert_eq!(refusal(&mut b, sql), None, "{sql}");
    }
    assert_eq!(refusal(&mut a, "alter range global placement policy default"), None);
    assert_eq!(refusal(&mut a, "drop placement policy p1"), None);
}

/// The keyspace codec of a mock store's domain.
fn keyspace_codec(store: &MockStoreDomain) -> tikv_client::request::ApiV2Codec {
    *store.info_syncer().codec().expect("a keyspace store")
}

/// Go `hex.EncodeToString`.
fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// `start_key`..`end_key` of a label rule's first range.
fn label_rule_range(rule: &tidb_ddl_label::Rule) -> (String, String) {
    let data = rule.data.as_ref().expect("a key-range rule").read().clone();
    let range = &data[0];
    (range["start_key"].as_str().unwrap().to_owned(), range["end_key"].as_str().unwrap().to_owned())
}

/// Label rules of two keyspaces on one node (design §6.2). A DDL step
/// builds a rule's ID and range with its keyspace's codec (Go
/// `jobCtx.store.GetCodec()`), so the same table name under the same table
/// ID in two keyspaces gets two rules in PD, `keyspace/<id>/schema/test/t`,
/// each over its own keyspace's keys. Each keyspace's ATTRIBUTES lists only
/// its own rule, and RENAME and TRUNCATE in one keyspace move only its own.
/// Before, a multi-keyspace node refused `ATTRIBUTES` (1235) and wrote no
/// label rule.
#[test]
fn two_keyspaces_keep_their_own_label_rules() {
    if !tidb_config_kerneltype::is_next_gen() {
        // Multi-keyspace serving is nextgen only.
        return;
    }
    let _modes = ProcessModes::set(tidb_keyspace::SYSTEM, tidb_config_deploymode::get(), false);
    let stores = [
        ("deny", MockStoreDomain::create_for_keyspace("ks_a").unwrap()),
        ("allow", MockStoreDomain::create_for_keyspace("ks_b").unwrap()),
    ];
    tidb_keyspace::set_multi_keyspace_mode(true);
    // PD's label rules, which both keyspaces reach.
    let pd = std::sync::Arc::new(tidb_domain_infosync::label_manager::MockLabelManager::default());
    let mut sessions = Vec::new();
    for (option, store) in &stores {
        store.info_syncer().init_label_rule_manager(Some(pd.clone()));
        let mut session = store.new_session().unwrap();
        for sql in [
            "use test".to_owned(),
            "create table t (a int)".to_owned(),
            format!("alter table t attributes = 'merge_option={option}'"),
        ] {
            session.run(&sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        }
        sessions.push(session);
    }
    let table_id = physical_id(&mut sessions[0], "t", None);
    assert_eq!(physical_id(&mut sessions[1], "t", None), table_id, "the table IDs collide");

    let rules = || {
        tidb_domain_infosync::label_manager::LabelRuleManager::get_all_label_rules(&*pd, &tidb_ddl_label::CodecV1)
            .unwrap()
    };
    let rule_id = |store: &MockStoreDomain, table: &str| {
        format!("keyspace/{}/schema/test/{table}", keyspace_codec(store).keyspace_id())
    };
    let expected_range = |store: &MockStoreDomain, id: i64| {
        let (start, end) = keyspace_codec(store).encode_region_range(
            &tidb_tablecodec::table_key::gen_table_prefix(id),
            &tidb_tablecodec::table_key::gen_table_prefix(id + 1),
        );
        (hex_lower(&start), hex_lower(&end))
    };
    let stored = rules();
    assert_eq!(stored.len(), 2, "{stored:?}");
    for (option, store) in &stores {
        let rule = stored.iter().find(|rule| rule.id == rule_id(store, "t")).expect("the keyspace's rule");
        assert_eq!(label_rule_range(rule), expected_range(store, table_id));
        assert_eq!(
            tidb_ddl_label::restore_region_labels(&rule.labels.snapshot()),
            format!("\"merge_option={option}\"")
        );
    }
    let attributes = "select id, attributes from information_schema.attributes";
    for ((option, _), session) in stores.iter().zip(&mut sessions) {
        assert_eq!(
            row_text(session.run(attributes)),
            [["schema/test/t".to_owned(), format!("\"merge_option={option}\"")]]
        );
    }

    // One keyspace's RENAME and TRUNCATE move only its own rule.
    assert_eq!(refusal(&mut sessions[0], "rename table t to t2"), None);
    assert_eq!(refusal(&mut sessions[1], "truncate table t"), None);
    let truncated = physical_id(&mut sessions[1], "t", None);
    let stored = rules();
    let ids = stored.iter().map(|rule| rule.id.clone()).collect::<std::collections::BTreeSet<_>>();
    assert_eq!(ids, [rule_id(&stores[0].1, "t2"), rule_id(&stores[1].1, "t")].into_iter().collect());
    for (store, table, id) in [(&stores[0].1, "t2", table_id), (&stores[1].1, "t", truncated)] {
        let rule = stored.iter().find(|rule| rule.id == rule_id(store, table)).unwrap();
        assert_eq!(label_rule_range(rule), expected_range(store, id), "{table}");
    }
    assert_eq!(row_text(sessions[0].run("select id from information_schema.attributes")), [["schema/test/t2"]]);
    assert_eq!(row_text(sessions[1].run("select id from information_schema.attributes")), [["schema/test/t"]]);
}

/// Placement of two keyspaces on one node (design §6.2). PD keeps one
/// namespace of rule groups, so a keyspace's bundle is keyed and ranged by
/// its keyspace where it meets PD: two keyspaces whose tables share a table
/// ID put two bundles, `keyspace-<id>-TiDB_DDL_<table ID>`, each over its
/// own keyspace's keys and with its own policy, and a keyspace's `ALTER
/// RANGE global` covers only its keyspace. Each keyspace's policies and its
/// range placement are its own, and TRUNCATE in one keyspace moves only its
/// own bundle. Before, a multi-keyspace node refused placement DDL (1235).
#[test]
fn two_keyspaces_keep_their_own_placement() {
    if !tidb_config_kerneltype::is_next_gen() {
        // Multi-keyspace serving is nextgen only.
        return;
    }
    let _modes = ProcessModes::set(tidb_keyspace::SYSTEM, tidb_config_deploymode::get(), false);
    let stores = [
        (1_i64, MockStoreDomain::create_for_keyspace("ks_a").unwrap()),
        (2, MockStoreDomain::create_for_keyspace("ks_b").unwrap()),
    ];
    tidb_keyspace::set_multi_keyspace_mode(true);
    // PD's placement rules, which both keyspaces reach.
    let pd = std::sync::Arc::new(tidb_domain_infosync::placement_manager::MockPlacementManager::default());
    let mut sessions = Vec::new();
    for (followers, store) in &stores {
        store.info_syncer().init_placement_manager(Some(pd.clone()));
        let mut session = store.new_session().unwrap();
        for sql in [
            "use test".to_owned(),
            format!("create placement policy p1 followers = {followers}"),
            "create table t (a int) placement policy = p1".to_owned(),
        ] {
            session.run(&sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        }
        sessions.push(session);
    }
    let table_id = physical_id(&mut sessions[0], "t", None);
    assert_eq!(physical_id(&mut sessions[1], "t", None), table_id, "the table IDs collide");

    let stored = |store: &MockStoreDomain, id: &str| {
        let group = format!("keyspace-{}-{id}", keyspace_codec(store).keyspace_id());
        tidb_domain_infosync::placement_manager::PlacementManager::get_rule_bundle(&*pd, &group).unwrap()
    };
    let table_range = |store: &MockStoreDomain, id: i64| {
        let (start, end) = keyspace_codec(store).encode_region_range(
            &tidb_tablecodec::table_key::gen_table_prefix(id),
            &tidb_tablecodec::table_key::gen_table_prefix(id + 1),
        );
        (hex_lower(&start), hex_lower(&end))
    };
    for (followers, store) in &stores {
        let bundle = stored(store, &tidb_placement::group_id(table_id));
        let group = format!("keyspace-{}-TiDB_DDL_{table_id}", keyspace_codec(store).keyspace_id());
        assert_eq!(bundle.id, group);
        assert!(!bundle.rules.is_empty());
        for rule in &bundle.rules {
            assert_eq!(rule.group_id, group);
            assert_eq!((rule.start_key_hex.clone(), rule.end_key_hex.clone()), table_range(store, table_id));
        }
        // The keyspace's own policy: a leader and its followers.
        assert_eq!(bundle.rules.iter().map(|rule| rule.count).sum::<i64>(), followers + 1);
    }
    let policies = "select policy_name, followers from information_schema.placement_policies";
    for ((followers, _), session) in stores.iter().zip(&mut sessions) {
        assert_eq!(row_text(session.run(policies)), [["p1".to_owned(), followers.to_string()]]);
    }

    // A keyspace's global range is its keyspace's keys.
    assert_eq!(refusal(&mut sessions[0], "alter range global placement policy p1"), None);
    let global = stored(&stores[0].1, tidb_placement::TIDB_BUNDLE_RANGE_PREFIX_FOR_GLOBAL);
    let (start, end) = keyspace_codec(&stores[0].1).encode_region_range(&[], &[]);
    assert_eq!(
        (global.rules[0].start_key_hex.clone(), global.rules[0].end_key_hex.clone()),
        (hex_lower(&start), hex_lower(&end))
    );
    assert!(stored(&stores[1].1, tidb_placement::TIDB_BUNDLE_RANGE_PREFIX_FOR_GLOBAL).rules.is_empty());
    let (code, message) = refusal(&mut sessions[0], "drop placement policy p1").expect("the range uses p1");
    assert_eq!(code, 8241, "{message}");
    assert_eq!(refusal(&mut sessions[0], "alter range global placement policy default"), None);

    // One keyspace's TRUNCATE puts only its own bundle (the old one stays
    // until GC, as in Go).
    let before = stored(&stores[0].1, &tidb_placement::group_id(table_id));
    assert_eq!(refusal(&mut sessions[1], "truncate table t"), None);
    let truncated = physical_id(&mut sessions[1], "t", None);
    assert_eq!(stored(&stores[0].1, &tidb_placement::group_id(table_id)), before);
    let bundle = stored(&stores[1].1, &tidb_placement::group_id(truncated));
    assert_eq!(
        (bundle.rules[0].start_key_hex.clone(), bundle.rules[0].end_key_hex.clone()),
        table_range(&stores[1].1, truncated)
    );
}

/// PD's resource manager as the node's keyspaces reach it: one store of
/// groups keyed by keyspace ID, which each runtime's client names (Go's
/// `ResourceGroup.KeyspaceId`, which the store's PD client fills in).
#[derive(Default)]
struct KeyspacedResourceManager(std::sync::Mutex<std::collections::BTreeMap<(u32, String), tidb_proto::resource_manager::ResourceGroup>>);

/// One keyspace's view of [`KeyspacedResourceManager`].
struct KeyspaceResourceClient {
    pd: std::sync::Arc<KeyspacedResourceManager>,
    keyspace_id: u32,
}

impl tidb_domain_infosync::resource_manager_client::ResourceManagerClient for KeyspaceResourceClient {
    fn add_resource_group(&self, group: tidb_proto::resource_manager::ResourceGroup) -> Result<String, String> {
        let mut groups = self.pd.0.lock().unwrap();
        let key = (self.keyspace_id, group.name.clone());
        if groups.contains_key(&key) {
            return Err(format!("the group {} already exists", group.name));
        }
        groups.insert(key, group);
        Ok("Success!".to_owned())
    }
    fn modify_resource_group(&self, group: tidb_proto::resource_manager::ResourceGroup) -> Result<String, String> {
        self.pd.0.lock().unwrap().insert((self.keyspace_id, group.name.clone()), group);
        Ok("Success!".to_owned())
    }
    fn delete_resource_group(&self, name: &str) -> Result<String, String> {
        self.pd.0.lock().unwrap().remove(&(self.keyspace_id, name.to_owned()));
        Ok("Success!".to_owned())
    }
    fn get_resource_group(&self, name: &str) -> Result<tidb_proto::resource_manager::ResourceGroup, String> {
        self.pd.0.lock().unwrap().get(&(self.keyspace_id, name.to_owned())).cloned()
            .ok_or_else(|| format!("the group {name} does not exist"))
    }
    fn list_resource_groups(&self) -> Result<Vec<tidb_proto::resource_manager::ResourceGroup>, String> {
        Ok(self.pd.0.lock().unwrap().iter()
            .filter(|((keyspace_id, _), _)| *keyspace_id == self.keyspace_id)
            .map(|(_, group)| group.clone())
            .collect())
    }
}

/// Resource groups of two keyspaces on one node (design §6.2). Each
/// runtime's resource-group jobs write PD's resource manager through its own
/// client, which names its keyspace, so two keyspaces create, alter and
/// drop groups of the same name independently: PD holds one group per
/// keyspace, and each keyspace's RESOURCE_GROUPS lists only its own. Before,
/// a multi-keyspace node refused resource-group DDL (1235).
#[test]
fn two_keyspaces_keep_their_own_resource_groups() {
    if !tidb_config_kerneltype::is_next_gen() {
        // Multi-keyspace serving is nextgen only.
        return;
    }
    let _modes = ProcessModes::set(tidb_keyspace::SYSTEM, tidb_config_deploymode::get(), true);
    let stores = [
        (100, MockStoreDomain::create_for_keyspace("ks_a").unwrap()),
        (200, MockStoreDomain::create_for_keyspace("ks_b").unwrap()),
    ];
    let pd = std::sync::Arc::new(KeyspacedResourceManager::default());
    let mut sessions = Vec::new();
    for (ru, store) in &stores {
        let keyspace_id = keyspace_codec(store).keyspace_id();
        // The keyspace's bootstrapped `default` group moves along.
        let default = store.info_syncer().resource_manager_client().get_resource_group("default").unwrap();
        pd.0.lock().unwrap().insert((keyspace_id, "default".to_owned()), default);
        let client = KeyspaceResourceClient { pd: pd.clone(), keyspace_id };
        store.info_syncer().init_resource_manager_client(Some(std::sync::Arc::new(client)));
        let mut session = store.new_session().unwrap();
        for sql in [format!("create resource group rg1 ru_per_sec = {ru}"), "create resource group rg2 ru_per_sec = 10".to_owned()] {
            session.run(&sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        }
        sessions.push(session);
    }
    assert_eq!(refusal(&mut sessions[0], "alter resource group rg1 ru_per_sec = 150"), None);
    assert_eq!(refusal(&mut sessions[1], "drop resource group rg2"), None);

    let held = pd.0.lock().unwrap().keys().filter(|(_, name)| name != "default").cloned().collect::<Vec<_>>();
    let [a_id, b_id] = [&stores[0].1, &stores[1].1].map(|store| keyspace_codec(store).keyspace_id());
    let mut expected = vec![(a_id, "rg1".to_owned()), (a_id, "rg2".to_owned()), (b_id, "rg1".to_owned())];
    expected.sort();
    assert_eq!(held, expected);
    let ru_of = |keyspace_id: u32| {
        let groups = pd.0.lock().unwrap();
        let group = &groups[&(keyspace_id, "rg1".to_owned())];
        group.r_u_settings.as_ref().unwrap().r_u.as_ref().unwrap().settings.as_ref().unwrap().fill_rate
    };
    assert_eq!((ru_of(a_id), ru_of(b_id)), (150, 200));

    let groups = "select name, ru_per_sec from information_schema.resource_groups where name != 'default' order by name";
    assert_eq!(row_text(sessions[0].run(groups)), [["rg1", "150"], ["rg2", "10"]]);
    assert_eq!(row_text(sessions[1].run(groups)), [["rg1", "200"]]);
}

/// The statements that read or change PD's cluster-wide schedulers and
/// affinity groups by keyspace-less key ranges and bare table IDs are
/// refused (1235) on a multi-keyspace node (design §6.2): DISTRIBUTE TABLE
/// creates a scheduler job over keyspace-less ranges, its job statements
/// list and cancel every keyspace's jobs, SHOW TABLE DISTRIBUTIONS reads
/// keyspace-less ranges, and SHOW AFFINITY reads groups another keyspace's
/// table of the same ID may own. A single-keyspace node runs them (here
/// they fail without a PD HTTP client, as in Go).
#[test]
fn a_multi_keyspace_node_refuses_cluster_wide_pd_statements() {
    if !tidb_config_kerneltype::is_next_gen() {
        // Multi-keyspace serving is nextgen only.
        return;
    }
    let _modes = ProcessModes::set(tidb_keyspace::SYSTEM, tidb_config_deploymode::get(), false);
    let store = MockStoreDomain::create().unwrap();
    let mut session = store.new_session().unwrap();
    for sql in ["use test", "create table t (a int)"] {
        session.run(sql).unwrap();
    }
    let statements = [
        "distribute table t rule = 'leader-scatter' engine = 'tikv'",
        "show distribution jobs",
        "cancel distribution job 1",
        "show table t distributions",
        "show affinity",
    ];
    for sql in statements {
        if let Some((code, message)) = refusal(&mut session, sql) {
            assert_ne!(code, 1235, "{sql}: {message}");
        }
    }
    tidb_keyspace::set_multi_keyspace_mode(true);
    for sql in statements {
        let (code, message) = refusal(&mut session, sql).unwrap_or_else(|| panic!("{sql} ran"));
        assert_eq!(code, 1235, "{sql}: {message}");
        assert!(message.ends_with("on a multi-keyspace node'"), "{sql}: {message}");
    }
    tidb_keyspace::set_multi_keyspace_mode(false);
}

/// An unqualified `TiDB_GLOBAL` bundle a Go or older node left in PD (Go
/// keys range bundles without a keyspace; its empty bounds cover every
/// keyspace's keys) through the upgrade to keyspace-qualified placement, on a
/// single-keyspace and on a multi-keyspace node (design §6.2): SHOW
/// PLACEMENT lists it as the legacy global range, its policy cannot be
/// dropped, and a keyspace's `ALTER RANGE global` (to a policy or back to
/// default) writes only its own qualified bundle and leaves it. Once the
/// operator removes it from PD, the policy is free. Before, the qualified
/// lookups missed it: SHOW listed nothing and the policy could be dropped
/// while PD still placed every keyspace by it.
#[test]
fn an_unqualified_global_bundle_stays_visible_and_in_use_until_the_operator_removes_it() {
    if !tidb_config_kerneltype::is_next_gen() {
        // Keyspace-qualified placement is next-generation only.
        return;
    }
    for multi_keyspace in [false, true] {
        let _modes = ProcessModes::set(tidb_keyspace::SYSTEM, tidb_config_deploymode::get(), multi_keyspace);
        let store = MockStoreDomain::create_for_keyspace("ks_a").unwrap();
        let pd = std::sync::Arc::new(tidb_domain_infosync::placement_manager::MockPlacementManager::default());
        store.info_syncer().init_placement_manager(Some(pd.clone()));
        let mut session = store.new_session().unwrap();
        session.run("create placement policy p1 followers = 1").unwrap();
        session.run("create placement policy p2 followers = 2").unwrap();
        // What the node before the upgrade put for `ALTER RANGE global
        // PLACEMENT POLICY p1`.
        let settings = tidb_model::placement::PlacementSettings { followers: 1, ..Default::default() };
        let mut legacy = tidb_placement::new_bundle_from_options(Some(&settings)).unwrap();
        legacy.rebuild_for_range(tidb_placement::KEY_RANGE_GLOBAL, "p1");
        assert!(legacy.rules.iter().all(|rule| rule.start_key_hex.is_empty() && rule.end_key_hex.is_empty()));
        tidb_domain_infosync::placement_manager::PlacementManager::put_rule_bundles(&*pd, &[legacy]).unwrap();

        let legacy_row = || ["RANGE TiDB_GLOBAL (legacy, every keyspace)".to_owned(), "FOLLOWERS=1".to_owned()];
        let ranges = |session: &mut tidb_session::Session| {
            row_text(session.run("show placement"))
                .into_iter()
                .filter(|row| row[0].starts_with("RANGE"))
                .map(|row| [row[0].clone(), row[1].clone()])
                .collect::<Vec<_>>()
        };
        assert_eq!(ranges(&mut session), [legacy_row()], "multi_keyspace={multi_keyspace}");
        let (code, message) = refusal(&mut session, "drop placement policy p1").expect("the legacy bundle uses p1");
        assert_eq!(code, 8241, "{message}");
        // The keyspace's own global placement, and its reset, leave it.
        for sql in ["alter range global placement policy p2", "alter range global placement policy default"] {
            assert_eq!(refusal(&mut session, sql), None, "{sql}");
        }
        assert_eq!(ranges(&mut session), [legacy_row()]);
        assert!(refusal(&mut session, "drop placement policy p1").is_some());

        // The operator's cutover: PD drops the unqualified group.
        tidb_domain_infosync::placement_manager::PlacementManager::put_rule_bundles(
            &*pd,
            &[tidb_placement::Bundle { id: tidb_placement::TIDB_BUNDLE_RANGE_PREFIX_FOR_GLOBAL.to_owned(), ..Default::default() }],
        )
        .unwrap();
        assert!(ranges(&mut session).is_empty());
        for sql in ["drop placement policy p1", "drop placement policy p2"] {
            assert_eq!(refusal(&mut session, sql), None, "{sql}");
        }
    }
}
