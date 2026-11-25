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

/// The PD restriction holds for the metadata a DDL job resolves, not only
/// the statement's text (design §10 phase 2): a table that got placement
/// before the node served several keyspaces carries it in its TableInfo,
/// and a TRUNCATE or partition change of it would re-put its bundles
/// through the process's PD client. In multi-keyspace mode such a job
/// step is refused with 1235 before any PD call and the job is cancelled:
/// the counting placement manager sees no call and the rows stay. DDL that
/// writes no PD state still runs.
#[test]
fn a_multi_keyspace_node_refuses_ddl_jobs_that_carry_placement() {
    if !tidb_config_kerneltype::is_next_gen() {
        // Multi-keyspace serving is nextgen only.
        return;
    }
    let _modes = ProcessModes::set(tidb_keyspace::SYSTEM, tidb_config_deploymode::get(), false);
    let store = MockStoreDomain::create().unwrap();
    let mut session = store.new_session().unwrap();
    let pd = std::sync::Arc::new(CountingPd::default());
    let placement = tidb_domain_infosync::placement_manager::placement_manager();
    tidb_domain_infosync::placement_manager::init_placement_manager(Some(pd.clone()));
    // Created by a single-keyspace node: the tables carry placement.
    for sql in [
        "use test",
        "create placement policy p1 followers = 1",
        "create table tp (a int) placement policy = p1",
        "insert into tp values (1), (2)",
        "create table pp (a int) partition by range (a) (partition p0 values less than (10) placement policy = p1, partition p9 values less than (100))",
        "insert into pp values (1)",
        "create table plain (a int)",
    ] {
        session.run(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    assert!(pd.calls() > 0, "the single-keyspace DDL put its bundles");
    let before = pd.calls();

    tidb_keyspace::set_multi_keyspace_mode(true);
    for sql in [
        "truncate table tp",
        "alter table pp truncate partition p0",
        "alter table pp add partition (partition p10 values less than (200))",
        "alter table pp reorganize partition p9 into (partition p90 values less than (50), partition p91 values less than (100))",
    ] {
        let (code, message) = refusal(&mut session, sql).unwrap_or_else(|| panic!("{sql} ran"));
        assert_eq!(code, 1235, "{sql}: {message}");
        assert!(message.ends_with("PLACEMENT on a multi-keyspace node'"), "{sql}: {message}");
    }
    assert_eq!(pd.calls(), before, "no PD call in multi-keyspace mode");
    assert_eq!(row_text(session.run("select count(*) from tp")), [["2"]]);
    assert_eq!(row_text(session.run("select count(*) from pp")), [["1"]]);
    // DROP PARTITION puts its bundles past the point the job can be
    // cancelled: it finishes without the PD write (the dropped partition's
    // rules stay until phase 3b).
    assert_eq!(refusal(&mut session, "alter table pp drop partition p0"), None);
    assert_eq!(pd.calls(), before);
    assert_eq!(row_text(session.run("select count(*) from pp")), [["0"]]);
    // Jobs without PD state run, and the refused jobs left none queued.
    for sql in ["truncate table plain", "alter table tp add column b int"] {
        assert_eq!(refusal(&mut session, sql), None, "{sql}");
    }
    assert_eq!(pd.calls(), before);
    assert_eq!(
        row_text(session.run("select count(*) from mysql.tidb_ddl_job")),
        [["0"]]
    );

    tidb_keyspace::set_multi_keyspace_mode(false);
    for sql in ["drop table tp, pp, plain", "drop placement policy p1"] {
        session.run(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    tidb_domain_infosync::placement_manager::init_placement_manager(Some(placement));
}

/// A label rule manager that counts its calls over an in-memory one.
#[derive(Debug, Default)]
struct CountingLabels {
    calls: std::sync::atomic::AtomicUsize,
    inner: tidb_domain_infosync::label_manager::MockLabelManager,
}

impl CountingLabels {
    fn calls(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }
    fn count(&self) {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

impl tidb_domain_infosync::label_manager::LabelRuleManager for CountingLabels {
    fn put_label_rule(&self, rule: &tidb_ddl_label::Rule) -> Result<(), String> {
        self.count();
        self.inner.put_label_rule(rule)
    }
    fn update_label_rules(&self, patch: &tidb_ddl_label::LabelRulePatch) -> Result<(), String> {
        self.count();
        self.inner.update_label_rules(patch)
    }
    fn get_all_label_rules(&self, codec: &dyn tidb_ddl_label::LabelCodec) -> Result<Vec<tidb_ddl_label::Rule>, String> {
        self.count();
        self.inner.get_all_label_rules(codec)
    }
    fn get_label_rules(
        &self,
        rule_ids: &[String],
    ) -> Result<std::collections::BTreeMap<String, tidb_ddl_label::Rule>, String> {
        self.count();
        self.inner.get_label_rules(rule_ids)
    }
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

/// Label rules and inherited TiFlash replicas on a multi-keyspace node
/// (design §10 phase 2, until 3b). A DDL step builds label rule IDs with
/// the keyspace-less codec (`schema/<db>/<table>`), so two tenants with the
/// same names would share rules: label operations are suppressed, and the
/// counting label manager sees no call while TRUNCATE, RENAME and partition
/// DDL of same-named tables run in two keyspaces. A table whose TiFlash
/// replica was set before keeps it in its TableInfo, and a partition change
/// or TRUNCATE would configure its new IDs in PD inside the step: refused
/// with 1235 before any TiFlash manager call. A single-keyspace node makes
/// both kinds of calls as before.
#[test]
fn a_multi_keyspace_node_writes_no_label_rules_or_inherited_tiflash_rules() {
    if !tidb_config_kerneltype::is_next_gen() {
        // Multi-keyspace serving is nextgen only.
        return;
    }
    let _modes = ProcessModes::set(tidb_keyspace::SYSTEM, tidb_config_deploymode::get(), false);
    let tenant_a = MockStoreDomain::create_with_mock_tiflash(1).unwrap();
    let tenant_b = MockStoreDomain::create().unwrap();
    let labels = std::sync::Arc::new(CountingLabels::default());
    let previous_labels = tidb_domain_infosync::label_manager::label_rule_manager();
    tidb_domain_infosync::label_manager::init_label_rule_manager(Some(labels.clone()));
    let tiflash = std::sync::Arc::new(CountingTiFlash {
        calls: std::sync::atomic::AtomicUsize::new(0),
        inner: tidb_domain_infosync::tiflash_manager::tiflash_replica_manager(),
    });
    tidb_domain_infosync::tiflash_manager::init_tiflash_replica_manager(Some(tiflash.clone()));

    // Single-keyspace: the label rules and the TiFlash replica reach PD.
    let mut a = tenant_a.new_session().unwrap();
    for sql in [
        "use test",
        "create table lt (a int) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))",
        "alter table lt partition p0 attributes = 'merge_option=deny'",
        "truncate table lt",
        "create table ft (a int) partition by range (a) (partition p0 values less than (10))",
        "alter table ft set tiflash replica 1",
    ] {
        a.run(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    assert!(labels.calls() > 0);
    assert!(tiflash.calls() > 0);
    let (label_calls, tiflash_calls) = (labels.calls(), tiflash.calls());

    tidb_keyspace::set_multi_keyspace_mode(true);
    // The same names in both keyspaces: no label call from either.
    let mut b = tenant_b.new_session().unwrap();
    b.run("use test").unwrap();
    b.run("create table lt (a int) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))")
        .unwrap();
    for session in [&mut a, &mut b] {
        for sql in [
            "truncate table lt",
            "alter table lt truncate partition p0",
            "alter table lt add partition (partition p2 values less than (30))",
            "rename table lt to lt2",
            "rename table lt2 to lt",
        ] {
            assert_eq!(refusal(session, sql), None, "{sql}");
        }
    }
    assert_eq!(labels.calls(), label_calls, "no label call in multi-keyspace mode");
    // The inherited TiFlash replica: refused before the manager is called.
    for sql in ["alter table ft add partition (partition p1 values less than (20))", "truncate table ft"] {
        let (code, message) = refusal(&mut a, sql).unwrap_or_else(|| panic!("{sql} ran"));
        assert_eq!(code, 1235, "{sql}: {message}");
        assert!(message.ends_with("TIFLASH REPLICA on a multi-keyspace node'"), "{sql}: {message}");
    }
    assert_eq!(tiflash.calls(), tiflash_calls, "no TiFlash manager call in multi-keyspace mode");
    assert_eq!(row_text(a.run("select count(*) from mysql.tidb_ddl_job")), [["0"]]);

    tidb_keyspace::set_multi_keyspace_mode(false);
    tidb_domain_infosync::label_manager::init_label_rule_manager(Some(previous_labels));
    tidb_domain_infosync::tiflash_manager::init_tiflash_replica_manager(Some(tiflash.inner.clone()));
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
    let cache = |id| tidb_domain_infosync::tiflash_manager::get_tiflash_progress_from_cache(id);

    // Single-keyspace: the cutover deletes the alert, the affinity group
    // and the progress entry.
    let old = physical_id(&mut session, "mv", None);
    session.run(&alert(old)).unwrap();
    tidb_domain_infosync::tiflash_manager::update_tiflash_progress_cache(old, 0.5);
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
    tidb_domain_infosync::tiflash_manager::update_tiflash_progress_cache(old, 0.5);
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
    tidb_domain_infosync::tiflash_manager::delete_tiflash_progress_from_cache(old);
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
    let cache = |id| tidb_domain_infosync::tiflash_manager::get_tiflash_progress_from_cache(id);

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
                tidb_domain_infosync::tiflash_manager::update_tiflash_progress_cache(definition.id, 0.25);
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
        tidb_domain_infosync::tiflash_manager::delete_tiflash_progress_from_cache(id);
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
    tidb_domain_infosync::tiflash_manager::set_mock_tiflash(std::sync::Arc::new(
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

/// INFORMATION_SCHEMA.ATTRIBUTES lists label rules, whose IDs carry no
/// keyspace: its name and physical-ID checks cannot tell one tenant's rules
/// from another's. A multi-keyspace node lists none and does not ask the
/// label manager (design §10 phase 2, until 3b); a single-keyspace node
/// lists the table's rule as before.
#[test]
fn a_multi_keyspace_node_lists_no_label_rule_attributes() {
    if !tidb_config_kerneltype::is_next_gen() {
        // Multi-keyspace serving is nextgen only.
        return;
    }
    let _modes = ProcessModes::set(tidb_keyspace::SYSTEM, tidb_config_deploymode::get(), false);
    let store = MockStoreDomain::create().unwrap();
    let labels = std::sync::Arc::new(CountingLabels::default());
    let previous_labels = tidb_domain_infosync::label_manager::label_rule_manager();
    tidb_domain_infosync::label_manager::init_label_rule_manager(Some(labels.clone()));
    let mut session = store.new_session().unwrap();
    for sql in ["use test", "create table at (a int)", "alter table at attributes = 'merge_option=deny'"] {
        session.run(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    let attributes = "select id, attributes from information_schema.attributes where id = 'schema/test/at'";
    assert_eq!(row_text(session.run(attributes)), [["schema/test/at", "\"merge_option=deny\""]]);

    tidb_keyspace::set_multi_keyspace_mode(true);
    let calls = labels.calls();
    assert!(row_text(session.run("select * from information_schema.attributes")).is_empty());
    assert_eq!(labels.calls(), calls, "no label manager call in multi-keyspace mode");

    tidb_keyspace::set_multi_keyspace_mode(false);
    tidb_domain_infosync::label_manager::init_label_rule_manager(Some(previous_labels));
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
/// the record). Resumed on a node that cannot close the limits, it would
/// run the flashback with PD scheduling on and then be taken for a job that
/// closed them: it is cancelled in `DELETE_ONLY` instead, writes nothing to
/// PD, and hands the saved value off to the operator.
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
    tidb_domain_infosync::schedule_manager::init_schedule_manager(Some(schedule.clone()));
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
    tidb_domain_infosync::schedule_manager::init_schedule_manager(None);
    let (code, message) = refused.expect("the resumed job was cancelled");
    assert!(code == 1235 || code == 8214, "{code}: {message}");
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
