//! A session runs its statements under its own keyspace's vardef values
//! (`rust/docs/design/2026-11-24-multi-keyspace-server.md`, §6.3), whichever
//! thread runs it and whatever scope that thread was in. None of these tests
//! changes the process statics: the tenant keyspace owns its values.

use std::sync::Arc;

use tidb_vardef::snapshot::{current, enter};
use tidb_vardef::VardefSnapshot;

use crate::domain::configured_user_store::ConfiguredUserStore;
use crate::domain::mock_store::MockStoreDomain;

/// A mock store whose keyspace owns its vardef values, with foreign keys off
/// in it.
fn tenant_without_foreign_keys() -> Arc<MockStoreDomain> {
    let users = ConfiguredUserStore::parse("root\t%\tmysql_native_password\t\n")
        .unwrap()
        .with_global_vars(tidb_domain::global_sysvars::GlobalSysvars::new().with_keyspace_vardef());
    let tenant = MockStoreDomain::create_with_users(Arc::new(users)).unwrap();
    let mut admin = tenant.new_session().unwrap();
    admin.run("set global tidb_enable_foreign_key = off").unwrap();
    tenant
}

/// Creates `p<n>`/`c<n>` with a foreign key and inserts an orphan child:
/// `Ok` when the keyspace's DDL built the key without enforcement.
fn orphan_insert(session: &mut crate::Session, n: u32) -> Result<(), String> {
    for sql in [
        "use test".to_owned(),
        format!("create table p{n} (id int primary key)"),
        format!("create table c{n} (id int primary key, pid int, foreign key (pid) references p{n}(id))"),
    ] {
        session.run(&sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    session
        .run(&format!("insert into c{n} values (1, 99)"))
        .map(drop)
        .map_err(|error| error.to_mysql_error().message)
}

#[test]
fn a_session_runs_its_statements_under_its_own_keyspace_values() {
    let tenant = tenant_without_foreign_keys();
    assert!(tidb_vardef::ENABLE_FOREIGN_KEY.load(std::sync::atomic::Ordering::SeqCst), "the process statics are untouched");

    // An internal session on a fresh worker thread, which entered nothing.
    let worker_tenant = Arc::clone(&tenant);
    std::thread::spawn(move || {
        let mut session = worker_tenant.new_session().unwrap();
        assert_eq!(orphan_insert(&mut session, 1), Ok(()), "the tenant's FK policy applies");
        assert!(!current().is_per_keyspace(), "the worker's scope is restored");
    })
    .join()
    .unwrap();

    // The same, on a thread inside another keyspace's scope (foreign keys
    // on there): the session's own values apply, and the caller's scope is
    // back afterwards.
    let outer = Arc::new(VardefSnapshot::per_keyspace());
    let _outer = enter(Arc::clone(&outer));
    let mut session = tenant.new_session().unwrap();
    assert_eq!(orphan_insert(&mut session, 2), Ok(()));
    assert!(Arc::ptr_eq(&current(), &outer));
    drop(session);

    // A keyspace reading the process statics enforces the key.
    let configured = MockStoreDomain::create().unwrap();
    let mut session = configured.new_session().unwrap();
    let refused = orphan_insert(&mut session, 3).unwrap_err();
    assert!(refused.contains("a foreign key constraint fails"), "{refused}");
    assert!(Arc::ptr_eq(&current(), &outer));
}

/// TTL (the first batch of migrated readers): a keyspace's TTL settings
/// govern its own job manager's decisions -- its sessions' job switch, its
/// running-task limit and its schedule window -- and not the configured
/// keyspace's.
#[test]
fn ttl_decisions_follow_the_keyspace_settings() {
    use std::sync::atomic::Ordering::SeqCst;
    use tidb_ttl_ttlworker::session::{SessionPool as _, SysSessionPool};

    let users = ConfiguredUserStore::parse("root\t%\tmysql_native_password\t\n")
        .unwrap()
        .with_global_vars(tidb_domain::global_sysvars::GlobalSysvars::new().with_keyspace_vardef());
    let tenant = MockStoreDomain::create_with_users(Arc::new(users)).unwrap();
    // A window that excludes now: from two to three hours ahead, in UTC.
    let now = chrono::Utc::now();
    let clock = |hours: i64| (now + chrono::Duration::hours(hours)).format("%H:%M +0000").to_string();
    let mut admin = tenant.new_session().unwrap();
    for sql in [
        "set global tidb_ttl_job_enable = off".to_owned(),
        "set global tidb_ttl_running_tasks = 3".to_owned(),
        format!("set global tidb_ttl_job_schedule_window_start_time = '{}'", clock(2)),
        format!("set global tidb_ttl_job_schedule_window_end_time = '{}'", clock(3)),
    ] {
        admin.run(&sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    let configured = MockStoreDomain::create().unwrap();

    let decisions = |store: &Arc<MockStoreDomain>| {
        let domain = store.factory();
        let pool = SysSessionPool::new(domain.advanced_sys_session_pool(), domain.vardef());
        let mut session_switch = None;
        pool.with_session(&mut |se| {
            session_switch = Some(se.get_session_vars().vardef().enable_ttl_job().load(SeqCst));
            Ok(())
        })
        .unwrap();
        let vardef = pool.vardef();
        (
            session_switch.unwrap(),
            tidb_ttl_ttlworker::task_manager::get_max_running_tasks_limit(&vardef, &None),
            tidb_ttl_ttlworker::timer::within_ttl_job_schedule_window(&vardef, &tidb_timer::go_time::GoTime::now()),
        )
    };
    let (enabled, limit, in_window) = decisions(&tenant);
    assert!(!enabled, "the tenant's TTL job switch is off");
    assert_eq!(limit, 3, "the tenant's running-task limit");
    assert!(!in_window, "the tenant's window excludes now");

    let (enabled, limit, in_window) = decisions(&configured);
    assert_eq!(enabled, tidb_vardef::ENABLE_TTL_JOB.load(SeqCst), "the configured keyspace reads the process switch");
    assert_ne!(limit, 3);
    let minute = now.format("%H:%M").to_string();
    if minute != "23:59" {
        assert!(in_window, "the default window is the whole day");
    }
}

/// Statistics (reader batch 2): a keyspace's `tidb_persist_analyze_options`
/// decides whether its ANALYZE stores its options, not the configured
/// keyspace's.
#[test]
fn analyze_persists_options_by_the_keyspace_setting() {
    let users = ConfiguredUserStore::parse("root\t%\tmysql_native_password\t\n")
        .unwrap()
        .with_global_vars(tidb_domain::global_sysvars::GlobalSysvars::new().with_keyspace_vardef());
    let tenant = MockStoreDomain::create_with_users(Arc::new(users)).unwrap();
    tenant
        .new_session()
        .unwrap()
        .run("set global tidb_persist_analyze_options = off")
        .unwrap();
    let configured = MockStoreDomain::create().unwrap();
    assert!(tidb_vardef::PERSIST_ANALYZE_OPTIONS.load(std::sync::atomic::Ordering::SeqCst));

    let persisted = |store: &Arc<MockStoreDomain>| {
        let mut session = store.new_session().unwrap();
        for sql in ["use test", "create table t (a int, key (a))", "insert into t values (1), (2), (3)", "analyze table t with 2 buckets"] {
            session.run(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        }
        crate::tests_support::row_text(session.run("select count(*) from mysql.analyze_options"))
    };
    assert_eq!(persisted(&tenant), [["0"]], "the tenant keeps no analyze options");
    assert_eq!(persisted(&configured), [["1"]], "the configured keyspace keeps them");
}

/// DDL reorg (reader batch 3): an ADD INDEX job takes its reorg write
/// speed from its keyspace's `tidb_ddl_reorg_max_write_speed`, not the
/// configured keyspace's.
#[test]
fn add_index_takes_the_keyspace_reorg_write_speed() {
    if tidb_config_kerneltype::is_next_gen() {
        // Nextgen keeps the variable read-only and hides the job's
        // parameters.
        return;
    }
    let users = ConfiguredUserStore::parse("root\t%\tmysql_native_password\t\n")
        .unwrap()
        .with_global_vars(tidb_domain::global_sysvars::GlobalSysvars::new().with_keyspace_vardef());
    let tenant = MockStoreDomain::create_with_users(Arc::new(users)).unwrap();
    tenant
        .new_session()
        .unwrap()
        .run("set global tidb_ddl_reorg_max_write_speed = '1MiB'")
        .unwrap();
    let configured = MockStoreDomain::create().unwrap();

    let job_comments = |store: &Arc<MockStoreDomain>| {
        let mut session = store.new_session().unwrap();
        for sql in ["use test", "create table t (a int)", "insert into t values (1), (2)", "alter table t add index i (a)"] {
            session.run(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        }
        let rows = crate::tests_support::row_text(session.run("admin show ddl jobs 1"));
        rows[0].last().cloned().unwrap_or_default()
    };
    let tenant_comments = job_comments(&tenant);
    assert!(tenant_comments.contains("max_write_speed=1048576"), "{tenant_comments}");
    let configured_comments = job_comments(&configured);
    assert!(!configured_comments.contains("max_write_speed"), "{configured_comments}");
}

/// Resource-control strict mode (3c): `SET RESOURCE GROUP` needs
/// `RESOURCE_GROUP_ADMIN` or `RESOURCE_GROUP_USER` while the session's
/// keyspace keeps `tidb_resource_control_strict_mode` on. A tenant that
/// turns it off lets its users switch groups; the configured keyspace
/// (strict, the default) still refuses its own.
#[test]
fn set_resource_group_follows_the_keyspace_strict_mode() {
    let users = ConfiguredUserStore::parse("root\t%\tmysql_native_password\t\n")
        .unwrap()
        .with_global_vars(tidb_domain::global_sysvars::GlobalSysvars::new().with_keyspace_vardef());
    let tenant = MockStoreDomain::create_with_users(Arc::new(users)).unwrap();
    let configured = MockStoreDomain::create().unwrap();
    let mut tenant_admin = tenant.new_session().unwrap();
    tenant_admin.run("set global tidb_resource_control_strict_mode = off").unwrap();
    assert!(
        tidb_vardef::ENABLE_RESOURCE_CONTROL_STRICT_MODE.load(std::sync::atomic::Ordering::SeqCst),
        "the process value is untouched"
    );

    let switch_group = |store: &Arc<MockStoreDomain>| {
        let mut admin = store.new_session().unwrap();
        admin.run("create user if not exists 'u'@'%'").unwrap();
        let mut user = store.new_session().unwrap();
        user.set_user("u@%".to_owned(), "u@127.0.0.1".to_owned());
        user.run("set resource group default")
            .map(drop)
            .map_err(|error| error.to_mysql_error().code)
    };
    assert_eq!(switch_group(&tenant), Ok(()));
    assert_eq!(switch_group(&configured), Err(1227));
}

/// Password policy (3c): `validate_password.*` and `password_history` are
/// the keyspace's. A tenant that turns validation on with a 12-character
/// minimum and keeps one password of history refuses a short password
/// (1819) and a reused one (3638); the configured keyspace, at the
/// defaults, accepts both, and the process statics are untouched.
#[test]
fn password_policy_follows_the_keyspace() {
    use std::sync::atomic::Ordering::SeqCst;
    let users = ConfiguredUserStore::parse("root\t%\tmysql_native_password\t\n")
        .unwrap()
        .with_global_vars(tidb_domain::global_sysvars::GlobalSysvars::new().with_keyspace_vardef());
    let tenant = MockStoreDomain::create_with_users(Arc::new(users)).unwrap();
    let configured = MockStoreDomain::create().unwrap();
    let (length, history) = (
        tidb_vardef::PASSWORD_VALIDATION_LENGTH.load(SeqCst),
        tidb_vardef::PASSWORD_HISTORY.load(SeqCst),
    );
    let mut tenant_admin = tenant.new_session().unwrap();
    for sql in [
        "set global validate_password.enable = on",
        "set global validate_password.length = 12",
        "set global password_history = 1",
    ] {
        tenant_admin.run(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    assert_eq!(tidb_vardef::PASSWORD_VALIDATION_LENGTH.load(SeqCst), length, "the process value is untouched");
    assert_eq!(tidb_vardef::PASSWORD_HISTORY.load(SeqCst), history, "the process value is untouched");

    let code = |store: &Arc<MockStoreDomain>, sql: &str| {
        store
            .new_session()
            .unwrap()
            .run(sql)
            .map(drop)
            .map_err(|error| error.to_mysql_error().code)
    };
    // A password that satisfies the default policy but not the tenant's
    // length.
    let short = "create user 'pw'@'%' identified by 'Aa1!aaaa'";
    assert_eq!(code(&tenant, short), Err(1819));
    assert_eq!(code(&configured, short), Ok(()));
    let long = "create user 'pw2'@'%' identified by 'Aa1!aaaaaaaa'";
    assert_eq!(code(&tenant, long), Ok(()));
    let reuse = "alter user 'pw2'@'%' identified by 'Aa1!aaaaaaaa'";
    assert_eq!(code(&tenant, reuse), Err(3638));
    assert_eq!(code(&configured, "create user 'pw2'@'%' identified by 'Aa1!aaaaaaaa'"), Ok(()));
    assert_eq!(code(&configured, reuse), Ok(()));
}

/// Analyze defaults (3c): `tidb_analyze_default_num_buckets` / `_topn` are
/// the keyspace's. A tenant at 4 buckets and 1 TopN value keeps at most one
/// TopN value for a 300-value column; the configured keyspace, at the
/// defaults (256 / 100), keeps more; the process statics are untouched.
#[test]
fn analyze_takes_the_keyspace_default_sizes() {
    use std::sync::atomic::Ordering::SeqCst;
    let users = ConfiguredUserStore::parse("root\t%\tmysql_native_password\t\n")
        .unwrap()
        .with_global_vars(tidb_domain::global_sysvars::GlobalSysvars::new().with_keyspace_vardef());
    let tenant = MockStoreDomain::create_with_users(Arc::new(users)).unwrap();
    let configured = MockStoreDomain::create().unwrap();
    let defaults = (
        tidb_vardef::ANALYZE_DEFAULT_NUM_BUCKETS.load(SeqCst),
        tidb_vardef::ANALYZE_DEFAULT_NUM_TOP_N.load(SeqCst),
    );
    let mut admin = tenant.new_session().unwrap();
    for sql in ["set global tidb_analyze_default_num_buckets = 4", "set global tidb_analyze_default_num_topn = 1"] {
        admin.run(sql).unwrap();
    }
    assert_eq!(
        (tidb_vardef::ANALYZE_DEFAULT_NUM_BUCKETS.load(SeqCst), tidb_vardef::ANALYZE_DEFAULT_NUM_TOP_N.load(SeqCst)),
        defaults,
        "the process values are untouched"
    );
    let top_n = |store: &Arc<MockStoreDomain>| {
        let mut session = store.new_session().unwrap();
        session.run("use test").unwrap();
        session.run("create table an (a int, key ia (a))").unwrap();
        // 300 values, the first ten repeated so TopN has candidates.
        let values: Vec<String> = (0..300).chain(0..10).chain(0..10).map(|i| format!("({i})")).collect();
        session.run(&format!("insert into an values {}", values.join(","))).unwrap();
        session.run("analyze table an").unwrap();
        let rows = crate::tests_support::row_text(session.run(
            "select count(*) from mysql.stats_top_n t join information_schema.tables i \
             on t.table_id = i.tidb_table_id where i.table_schema = 'test' and i.table_name = 'an' and t.is_index = 0",
        ));
        rows[0][0].parse::<u64>().unwrap()
    };
    let tenant_top_n = top_n(&tenant);
    let configured_top_n = top_n(&configured);
    assert!(tenant_top_n <= 1, "the tenant keeps 1 TopN value: {tenant_top_n}");
    assert!(configured_top_n > 1, "the configured keyspace keeps up to 100: {configured_top_n}");
}

/// DDL error limit (3c): `tidb_ddl_error_count_limit` is the keyspace's. A
/// tenant's DDL refreshes its own value; the process value (what the
/// configured keyspace's DDL reads) is untouched.
#[test]
fn ddl_error_count_limit_is_the_keyspace_s() {
    let users = ConfiguredUserStore::parse("root\t%\tmysql_native_password\t\n")
        .unwrap()
        .with_global_vars(tidb_domain::global_sysvars::GlobalSysvars::new().with_keyspace_vardef());
    let tenant = MockStoreDomain::create_with_users(Arc::new(users)).unwrap();
    let process = tidb_vardef::ddl_error_count_limit();
    let mut session = tenant.new_session().unwrap();
    session.run("set global tidb_ddl_error_count_limit = 7").unwrap();
    session.run("use test").unwrap();
    // The job refreshes the limit from the keyspace's table on every step.
    session.run("create table el (a int)").unwrap();
    session.run("alter table el add column b int").unwrap();
    assert_eq!(tidb_vardef::ddl_error_count_limit(), process, "the process value is untouched");
    assert_eq!(
        crate::tests_support::row_text(session.run("select @@global.tidb_ddl_error_count_limit")),
        [["7"]]
    );
}
