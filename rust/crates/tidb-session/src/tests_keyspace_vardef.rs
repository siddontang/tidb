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
