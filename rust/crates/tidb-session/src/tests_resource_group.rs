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

//! Go's resource groups end to end on one store: the DDL
//! (`ddl/resource_group.go`), `information_schema.RESOURCE_GROUPS`
//! (`setDataFromResourceGroups`), `SET RESOURCE GROUP`, and the account's
//! `RESOURCE GROUP` with the login that applies it (`simple.go`, `Auth`).
//! A fresh store has resource control ON (Go's default, published by the
//! sysvar-cache rebuild at bootstrap) without any `SET GLOBAL`.

use tidb_privilege;
use crate::tests_support::{bootstrap_session, row_text, session_as};
use crate::Session;

fn error(session: &mut Session, sql: &str) -> (u16, String) {
    let error = session.run(sql).expect_err(sql).to_mysql_error();
    (error.code, error.message)
}

fn run(session: &mut Session, sql: &str) {
    session
        .run(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
}

#[test]
fn resource_groups_follow_go() {
    let privs = tidb_privilege::PrivilegeRegistry::default();
    let mut session = bootstrap_session(&privs);
    session.set_user("root@%".into(), "root@%".into());
    let groups = "select * from information_schema.resource_groups order by name";
    assert_eq!(
        row_text(session.run(groups)),
        [[
            "default",
            "UNLIMITED",
            "MEDIUM",
            "UNLIMITED",
            "NULL",
            "TASK_TYPES='stats'"
        ]]
    );

    run(&mut session, "create resource group rg1 ru_per_sec = 100");
    run(
        &mut session,
        "create resource group rg2 ru_per_sec = 200 priority = high burstable = moderated \
         query_limit = (exec_elapsed = '15s' action = kill watch = exact duration = '10m')",
    );
    assert_eq!(
        row_text(session.run(groups)),
        [
            vec![
                "default",
                "UNLIMITED",
                "MEDIUM",
                "UNLIMITED",
                "NULL",
                "TASK_TYPES='stats'"
            ],
            vec!["rg1", "100", "MEDIUM", "OFF", "NULL", "NULL"],
            vec![
                "rg2",
                "200",
                "HIGH",
                "MODERATED",
                "EXEC_ELAPSED='15s', ACTION=KILL, WATCH=EXACT DURATION='10m0s'",
                "NULL"
            ],
        ]
    );
    assert_eq!(
        error(&mut session, "create resource group rg1 ru_per_sec = 1"),
        (8248, "Resource group 'rg1' already exists".to_owned())
    );
    run(
        &mut session,
        "create resource group if not exists rg1 ru_per_sec = 1",
    );
    assert_eq!(
        error(&mut session, "drop resource group default"),
        (1105, "can't drop reserved resource group".to_owned())
    );
    assert_eq!(
        error(
            &mut session,
            "alter resource group rg1 background = (task_types = 'stats')"
        )
        .1,
        "unsupported operation. Currently, only the default resource group support change \
         background settings"
    );
    assert_eq!(
        error(
            &mut session,
            "alter resource group default background = (task_types = 'nosuch')"
        ),
        (8255, "Unknown background task name 'nosuch'".to_owned())
    );
    assert_eq!(
        error(
            &mut session,
            "alter resource group rg1 query_limit = (action = kill)"
        )
        .1,
        "please set at least one field(exec_elapsed_time_ms, processed_keys, ru)"
    );

    assert_eq!(
        error(&mut session, "set resource group nosuch"),
        (8249, "Unknown resource group 'nosuch'".to_owned())
    );
    run(&mut session, "set resource group rg1");
    assert_eq!(
        row_text(session.run("select current_resource_group()")),
        [["rg1"]]
    );
    run(&mut session, "set resource group ``");

    // An account's group, recorded in User_attributes and applied at login.
    run(&mut session, "create user u1");
    run(&mut session, "alter user u1 resource group rg1");
    assert_eq!(
        row_text(session.run("select user_attributes from mysql.user where user = 'u1'")),
        [[r#"{"resource_group": "rg1"}"#]]
    );
    run(&mut session, "create role r1");
    assert_eq!(
        error(&mut session, "alter user r1 resource group rg1"),
        (8257, "Cannot set resource group for a role".to_owned())
    );
    assert_eq!(
        error(&mut session, "create user u2 resource group nosuch"),
        (8249, "Unknown resource group 'nosuch'".to_owned())
    );
    let mut login = session_as(&privs, crate::SharedCatalog::clone(&session.catalog), "u1", "%");
    assert_eq!(
        row_text(login.run("select current_resource_group()")),
        [["rg1"]]
    );

    assert_eq!(
        error(&mut session, "drop resource group rg1"),
        (
            1105,
            "user [u1] depends on the resource group to drop".to_owned()
        )
    );
    run(&mut session, "drop resource group rg2");
    run(&mut session, "drop resource group if exists rg2");
}

/// The PD journal through the production entry points over the mock store:
/// the bootstrap, DDL jobs (resource groups, table placement, attributes)
/// and ALTER RANGE each leave a record of every PD object they write, equal
/// to what PD holds, so a writer that bypassed the journal shows as an
/// object without one; and the reconciler puts back an object PD lost.
fn journal_records(
    store: &dyn tidb_meta::MetaStore,
) -> std::collections::BTreeMap<tidb_ddl_pdjournal::ObjectKey, tidb_ddl_pdjournal::Record> {
    let mut found = Default::default();
    store
        .run_in_new_txn(&tidb_gocontext::Context::background(), "test", false, &mut |txn| {
            found = tidb_ddl_pdjournal::journal::scan_records(&mut tidb_ddl_pdjournal::TxnRead(txn)).unwrap();
            assert!(tidb_ddl_pdjournal::journal::read_cutover(&mut tidb_ddl_pdjournal::TxnRead(txn)).unwrap().is_some(), "bootstrap's cutover");
            Ok(())
        })
        .unwrap();
    found
}

#[test]
fn every_pd_entry_point_journals_what_it_writes_and_the_reconciler_repairs_pd() {
    let mut session = Session::new();
    let domain = crate::domain::mock_store::domain_of(&session).expect("a mock-store session");
    let syncer = domain.info_syncer();
    let store = std::sync::Arc::clone(domain.domain().meta_store.get().expect("a meta store"));
    let pd = tidb_ddl_pdjournal::pd::InfoSyncerPd(std::sync::Arc::clone(&syncer));
    let group = |bundle: tidb_placement::Bundle| tidb_ddl_pdjournal::pd::bundle_entry(&syncer.pd_bundle(&bundle).unwrap()).0;
    let range = |name: &str| {
        let mut bundle = tidb_placement::Bundle::default();
        bundle.rebuild_for_range(name, "");
        group(bundle)
    };

    let bootstrapped = journal_records(store.as_ref());
    assert!(bootstrapped.contains_key(&tidb_ddl_pdjournal::ObjectKey::resource_group("default")), "{bootstrapped:?}");

    run(&mut session, "use test");
    for sql in [
        "create resource group rg_journal ru_per_sec = 100",
        "create placement policy p_journal followers = 2",
        "create table t_journal (c int) placement policy = p_journal",
        "alter table t_journal attributes = 'merge_option=deny'",
        "alter range global placement policy p_journal",
        "alter resource group rg_journal ru_per_sec = 200",
    ] {
        run(&mut session, sql);
    }
    let table_id: i64 = crate::tests_support::row_text(session.run(
        "select tidb_table_id from information_schema.tables where table_name = 't_journal'",
    ))[0][0]
        .parse()
        .unwrap();
    let rule_id = tidb_ddl_label::new_rule_id(syncer.label_codec(), "test", "t_journal", "");
    let written = [
        tidb_ddl_pdjournal::ObjectKey::resource_group("rg_journal"),
        group(tidb_placement::new_bundle(table_id)),
        tidb_ddl_pdjournal::ObjectKey::label_rule(&rule_id),
        range(tidb_placement::KEY_RANGE_GLOBAL),
    ];
    let journaled = journal_records(store.as_ref());
    let held = tidb_ddl_pdjournal::pd::PdObjects::read(&pd, &written).unwrap();
    for key in &written {
        let record = journaled.get(key).unwrap_or_else(|| panic!("{key} has no record: a writer bypassed the journal"));
        assert!(
            tidb_ddl_pdjournal::normalize::in_sync(key, record.value.as_ref(), held.get(key), None).unwrap(),
            "{key}: record {:?}, PD {:?}",
            record.value,
            held.get(key)
        );
    }

    // PD loses every object; the owner's reconciler puts them back.
    for key in &written {
        tidb_ddl_pdjournal::pd::PdObjects::write(&pd, key, None).unwrap();
    }
    let options = tidb_ddl_pdjournal::reconcile::Options::new("test-owner", tidb_ddl_pdjournal::pd::scope(&syncer), None);
    let report = tidb_ddl_pdjournal::reconcile::reconcile_once(store.as_ref(), &pd, &options, &|| true).unwrap();
    assert!(report.failed.is_empty(), "{report:?}");
    for key in &written {
        assert!(report.repaired.contains(key), "{key}: {report:?}");
    }
    assert_eq!(tidb_ddl_pdjournal::pd::PdObjects::read(&pd, &written).unwrap().len(), written.len());

    // Drops and the range's reset leave tombstones PD agrees with.
    for sql in ["drop resource group rg_journal", "alter range global placement policy default"] {
        run(&mut session, sql);
    }
    let journaled = journal_records(store.as_ref());
    for key in [&written[0], &written[3]] {
        assert_eq!(journaled[key].value, None, "{key}");
        assert!(!tidb_ddl_pdjournal::pd::PdObjects::read(&pd, std::slice::from_ref(key)).unwrap().contains_key(key), "{key}");
    }

    // Without the keyspace's cutover, a label rule's first mutation fails
    // before PD and its job is cancelled; ALTER RANGE refuses too.
    let (cutover, _) = tidb_ddl_pdjournal::journal::bootstrap_cutover_mutation(0);
    store
        .run_in_new_txn(&tidb_gocontext::Context::background(), "test", false, &mut |txn| txn.delete(&cutover))
        .unwrap();
    run(&mut session, "create table t_unseeded (c int)");
    for sql in ["alter table t_unseeded attributes = 'merge_option=deny'", "alter range meta placement policy p_journal"] {
        let refused = format!("{:?}", session.run(sql).err().unwrap_or_else(|| panic!("{sql} must be refused")));
        assert!(refused.contains("PD journal cutover"), "{sql}: {refused}");
    }
    let unseeded = tidb_ddl_pdjournal::ObjectKey::label_rule(&tidb_ddl_label::new_rule_id(syncer.label_codec(), "test", "t_unseeded", ""));
    let meta = range(tidb_placement::KEY_RANGE_META);
    assert!(tidb_ddl_pdjournal::pd::PdObjects::read(&pd, &[unseeded, meta]).unwrap().is_empty(), "PD is untouched");
}
