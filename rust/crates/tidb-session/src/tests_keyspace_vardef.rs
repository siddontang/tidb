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
