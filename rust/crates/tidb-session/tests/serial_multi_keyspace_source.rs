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

/// A PD client that counts the calls it gets.
#[derive(Debug, Default)]
struct CountingPd(std::sync::atomic::AtomicUsize);

impl CountingPd {
    fn calls(&self) -> usize {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
    fn count(&self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

impl tidb_domain_infosync::placement_manager::PlacementManager for CountingPd {
    fn get_rule_bundle(&self, name: &str) -> Result<tidb_placement::Bundle, String> {
        self.count();
        Ok(tidb_placement::Bundle { id: name.to_owned(), ..Default::default() })
    }
    fn put_rule_bundles(&self, _bundles: &[tidb_placement::Bundle]) -> Result<(), String> {
        self.count();
        Ok(())
    }
}

impl tidb_domain_infosync::resource_manager_client::ResourceManagerClient for CountingPd {
    fn add_resource_group(&self, _group: tidb_proto::resource_manager::ResourceGroup) -> Result<String, String> {
        self.count();
        Ok("Success!".to_owned())
    }
    fn modify_resource_group(&self, _group: tidb_proto::resource_manager::ResourceGroup) -> Result<String, String> {
        self.count();
        Ok("Success!".to_owned())
    }
    fn delete_resource_group(&self, _name: &str) -> Result<String, String> {
        self.count();
        Ok("Success!".to_owned())
    }
}

/// A multi-keyspace node refuses the DDL whose effect lives in PD --
/// resource groups, placement policies and options, label rules
/// (`ATTRIBUTES`), TiFlash replicas -- and `AUTO_ID_CACHE=1` tables, whose
/// IDs come from the AutoID service only the configured keyspace runs, with
/// `ER_NOT_SUPPORTED_YET` (1235) before any PD client is called (design §10
/// phase 2, until 3b and 3f make them per keyspace). The counting placement
/// manager and resource-manager client see no call, and no table, policy
/// or group is created. A single-keyspace node runs the same statements:
/// the resource-group DDL reaches its client.
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
    let pd = std::sync::Arc::new(CountingPd::default());
    let placement = tidb_domain_infosync::placement_manager::placement_manager();
    tidb_domain_infosync::placement_manager::init_placement_manager(Some(pd.clone()));
    tidb_domain_infosync::resource_manager_client::init_resource_manager_client(Some(pd.clone()));

    let statements = [
        "create resource group rg1 ru_per_sec = 100",
        "alter resource group default ru_per_sec = 100",
        "drop resource group if exists rg1",
        "create placement policy p1 followers = 1",
        "alter placement policy p1 followers = 2",
        "drop placement policy if exists p1",
        "create database d1 placement policy = p1",
        "alter database test placement policy = default",
        "alter database test set tiflash replica 1",
        "create table t1 (a int) placement policy = p1",
        "create table t2 (a int) partition by range (a) (partition p0 values less than (10) placement policy = p1)",
        "alter table t placement policy = default",
        "alter table t partition p0 placement policy = default",
        "alter table t set tiflash replica 1",
        "alter table t attributes = 'merge_option=deny'",
        "alter table t partition p0 attributes = 'merge_option=deny'",
        "create table t3 (a int primary key auto_increment) auto_id_cache = 1",
        "alter table t auto_id_cache = 1",
    ];
    tidb_keyspace::set_multi_keyspace_mode(true);
    for sql in statements {
        let (code, message) = refusal(&mut session, sql).unwrap_or_else(|| panic!("{sql} ran"));
        assert_eq!(code, 1235, "{sql}: {message}");
        assert!(message.ends_with("on a multi-keyspace node'"), "{sql}: {message}");
    }
    assert_eq!(pd.calls(), 0);
    let created = row_text(session.run(
        "select count(*) from information_schema.tables where table_schema in ('test', 'd1') and table_name in ('t1', 't2', 't3')",
    ));
    assert_eq!(created, [["0"]]);
    // The tenant's other DDL runs.
    assert_eq!(refusal(&mut session, "create table t4 (a int) auto_id_cache = 100"), None);
    assert_eq!(refusal(&mut session, "alter table t4 add column b int"), None);

    // A single-keyspace node runs them as before.
    tidb_keyspace::set_multi_keyspace_mode(false);
    for sql in ["create resource group rg1 ru_per_sec = 100", "drop resource group rg1"] {
        assert_eq!(refusal(&mut session, sql), None, "{sql}");
    }
    assert!(pd.calls() > 0);
    tidb_domain_infosync::placement_manager::init_placement_manager(Some(placement));
    tidb_domain_infosync::resource_manager_client::init_resource_manager_client(Some(std::sync::Arc::new(
        tidb_domain_infosync::resource_manager_client::MockResourceManagerClient::new(),
    )));
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
