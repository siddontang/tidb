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

//! Go `pkg/ddl/cluster_test.go`: FLASHBACK CLUSTER on a mock store, where
//! `mockFlashbackTest` stands for the TiKV the job requires and
//! `injectSafeTS` for the stores' resolved timestamp.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::TimeZone as _;
use tidb_domain_infosync::schedule_manager::{get_pd_schedule_config, set_pd_schedule_config};
use tidb_model::{ActionType, Job, JobState, SchemaState};

use super::Session;

/// Go `testfailpoint.EnableCall` with a `func(job *model.Job)` on the DDL
/// package's failpoint `name`.
fn enable_call(name: &str, callback: impl Fn(&mut Job) + Send + Sync + 'static) -> tidb_failpoint::Guard {
    tidb_failpoint::enable_call_for_test(&format!("github.com/pingcap/tidb/pkg/ddl/{name}"), move |job: &mut dyn Any| {
        callback(job.downcast_mut::<Job>().expect("the step's job"))
    })
}

fn exec(session: &mut Session, sql: &str) {
    if let Err(error) = session.run(sql) {
        panic!("{sql}: {:?}", error.to_mysql_error());
    }
}

fn error_of(session: &mut Session, sql: &str) -> (u16, String) {
    let error = session.run(sql).expect_err(sql).to_mysql_error();
    (error.code, error.message)
}

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    crate::tests_support::row_text(session.run(sql))
}

/// `show variables like '<name>'`'s value.
fn variable(session: &mut Session, name: &str) -> String {
    rows(session, &format!("show variables like '{name}'")).remove(0).remove(1)
}

/// Go `tk.Session().GetStore().GetOracle().GetTimestamp`.
fn get_timestamp(session: &mut Session) -> u64 {
    exec(session, "begin");
    let ts = rows(session, "select @@tidb_current_ts").remove(0).remove(0).parse().expect("a timestamp");
    exec(session, "rollback");
    ts
}

/// Go `oracle.GetTimeFromTS(ts)` as Unix milliseconds.
fn physical(ts: u64) -> i64 {
    (ts >> 18) as i64
}

/// Go `flashback cluster to timestamp '<oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)>'`.
fn flashback_to(ts: u64) -> String {
    let time = chrono::Local.timestamp_millis_opt(physical(ts)).unwrap();
    format!("flashback cluster to timestamp '{}'", time.format("%Y-%m-%d %H:%M:%S%.6f"))
}

/// The test's setup: `mockFlashbackTest`, `injectSafeTS` ten seconds after
/// `ts`, and Go `MockGC` with its safe point 48 hours back.
fn mock_flashback(session: &mut Session, ts: u64) -> [tidb_failpoint::Guard; 2] {
    let inject_safe_ts = ((physical(ts) + 10_000) as u64) << 18;
    let guards = [
        tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/mockFlashbackTest", "return(true)"),
        tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/injectSafeTS", &format!("return({inject_safe_ts})")),
    ];
    let time_before_drop = (chrono::Local::now() - chrono::Duration::hours(48))
        .format("%Y%m%d-%H:%M:%S%.3f %z")
        .to_string();
    exec(
        session,
        "delete from mysql.tidb where variable_name in ( 'tikv_gc_safe_point','tikv_gc_enable' )",
    );
    exec(
        session,
        &format!(
            "INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '{time_before_drop}', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '{time_before_drop}'"
        ),
    );
    guards
}

fn mock_store_session() -> Session {
    let domain = crate::domain::mock_store::MockStoreDomain::create()
        .unwrap_or_else(|error| panic!("boot the mock store: {error}"));
    domain.new_session().unwrap_or_else(|error| panic!("open a mock store session: {error}"))
}

/// Go `TestFlashbackCloseAndResetPDSchedule`.
#[test]
fn flashback_close_and_reset_pd_schedule_like_go() {
    let mut session = mock_store_session();
    let session = &mut session;
    set_pd_schedule_config(&HashMap::from([("merge-schedule-limit".to_owned(), serde_json::json!(1))])).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));
    let ts = get_timestamp(session);
    let _mock = mock_flashback(session, ts);

    let closed = Arc::new(Mutex::new(Vec::new()));
    let seen = closed.clone();
    // Go's `beforeRunOneJobStep` callback checks the closed schedule at
    // `StateWriteReorganization`, a step the cancellation below never lets
    // start; the value is read where the job is cancelled instead.
    let before = enable_call("beforeRunOneJobStep", |job| {
        assert_eq!(job.type_, ActionType::ACTION_FLASHBACK_CLUSTER);
    });
    let after = enable_call("afterRunOneJobStep", move |job| {
        assert_eq!(job.type_, ActionType::ACTION_FLASHBACK_CLUSTER);
        if job.schema_state == SchemaState::WRITE_REORGANIZATION {
            seen.lock().unwrap().push(get_pd_schedule_config().unwrap()["merge-schedule-limit"].clone());
            // cancel flashback job
            job.state = JobState::CANCELLED;
            job.error = Some(tidb_model::GoShared::new(tidb_dbterror::ERR_CANCELLED_DDL_JOB.clone()));
        }
    });
    assert_eq!(error_of(session, &flashback_to(ts)), (8214, "Cancelled DDL job".to_owned()));
    drop((before, after));
    assert_eq!(*closed.lock().unwrap(), [serde_json::json!(0)]);
    assert_eq!(get_pd_schedule_config().unwrap()["merge-schedule-limit"], serde_json::json!(1));
}

/// Go `TestAddDDLDuringFlashback`.
#[test]
fn add_ddl_during_flashback_like_go() {
    let mut session = mock_store_session();
    let session = &mut session;
    exec(session, "use test");
    exec(session, "create table t(a int)");
    std::thread::sleep(std::time::Duration::from_millis(10));
    let ts = get_timestamp(session);
    let _mock = mock_flashback(session, ts);

    let domain = crate::domain::mock_store::domain_of(session).expect("a mock store session");
    let refused = Arc::new(Mutex::new(Vec::new()));
    let seen = refused.clone();
    let before = enable_call("beforeRunOneJobStep", move |job| {
        assert_eq!(job.type_, ActionType::ACTION_FLASHBACK_CLUSTER);
        if job.schema_state == SchemaState::WRITE_ONLY {
            let mut tk1 = domain.new_session().expect("a second session");
            seen.lock().unwrap().push(error_of(&mut tk1, "alter table test.t add column b int").1);
        }
    });
    exec(session, &flashback_to(ts));
    drop(before);
    assert_eq!(*refused.lock().unwrap(), ["Can't add ddl job, have flashback cluster job"]);
}

/// Go `TestGlobalVariablesOnFlashback`.
#[test]
fn global_variables_on_flashback_like_go() {
    let mut session = mock_store_session();
    let session = &mut session;
    exec(session, "use test");
    exec(session, "create table t(a int)");
    std::thread::sleep(std::time::Duration::from_millis(10));
    let ts = get_timestamp(session);
    let _mock = mock_flashback(session, ts);

    let domain = crate::domain::mock_store::domain_of(session).expect("a mock store session");
    let during = Arc::new(Mutex::new(Vec::new()));
    let seen = during.clone();
    let before = enable_call("beforeRunOneJobStep", move |job| {
        assert_eq!(job.type_, ActionType::ACTION_FLASHBACK_CLUSTER);
        if job.schema_state == SchemaState::WRITE_REORGANIZATION {
            let mut tk = domain.new_session().expect("a second session");
            exec(&mut tk, "use test");
            seen.lock().unwrap().push(
                ["tidb_gc_enable", "tidb_enable_auto_analyze", "tidb_super_read_only", "tidb_ttl_job_enable"]
                    .map(|name| variable(&mut tk, name)),
            );
        }
    });
    // first try with `tidb_gc_enable` = on and `tidb_super_read_only` = off and `tidb_ttl_job_enable` = on
    exec(session, "set global tidb_gc_enable = on");
    exec(session, "set global tidb_super_read_only = off");
    exec(session, "set global tidb_ttl_job_enable = on");

    exec(session, &flashback_to(ts));
    assert_eq!(variable(session, "tidb_super_read_only"), "OFF");
    assert_eq!(variable(session, "tidb_gc_enable"), "ON");
    assert_eq!(variable(session, "tidb_ttl_job_enable"), "OFF");

    // second try with `tidb_gc_enable` = off and `tidb_super_read_only` = on and `tidb_ttl_job_enable` = off
    exec(session, "set global tidb_gc_enable = off");
    exec(session, "set global tidb_super_read_only = on");
    exec(session, "set global tidb_ttl_job_enable = off");

    let ts = get_timestamp(session);
    exec(session, &flashback_to(ts));
    assert_eq!(variable(session, "tidb_super_read_only"), "ON");
    assert_eq!(variable(session, "tidb_gc_enable"), "OFF");
    assert_eq!(variable(session, "tidb_ttl_job_enable"), "OFF");
    drop(before);
    assert_eq!(*during.lock().unwrap(), [["OFF", "OFF", "ON", "OFF"]; 2]);
}

/// Go `newCancelJobHook` over `afterWaitSchemaSynced`: the step that
/// reached the state has been synchronized when the hook cancels the job.
fn cancel_job_hook(session: &Session, state: SchemaState) -> (tidb_failpoint::Guard, Arc<Mutex<Vec<String>>>) {
    let domain = crate::domain::mock_store::domain_of(session).expect("a mock store session");
    let cancelled = Arc::new(Mutex::new(Vec::new()));
    let seen = cancelled.clone();
    let guard = enable_call("afterWaitSchemaSynced", move |job| {
        if job.schema_state == state && seen.lock().unwrap().is_empty() {
            let mut tk = domain.new_session().expect("a second session");
            let result = rows(&mut tk, &format!("admin cancel ddl jobs {}", job.id));
            seen.lock().unwrap().push(result[0][1].clone());
        }
    });
    (guard, cancelled)
}

/// Go `TestCancelFlashbackCluster`.
#[test]
fn cancel_flashback_cluster_like_go() {
    let mut session = mock_store_session();
    let session = &mut session;
    std::thread::sleep(std::time::Duration::from_millis(10));
    let ts = get_timestamp(session);
    let _mock = mock_flashback(session, ts);

    // Try canceled on StateDeleteOnly, cancel success
    let (hook, cancelled) = cancel_job_hook(session, SchemaState::DELETE_ONLY);
    exec(session, "set global tidb_ttl_job_enable = on");
    assert_eq!(error_of(session, &flashback_to(ts)), (8214, "Cancelled DDL job".to_owned()));
    drop(hook);
    assert_eq!(*cancelled.lock().unwrap(), ["successful"]);
    assert_eq!(variable(session, "tidb_ttl_job_enable"), "ON");

    // Try canceled on StateWriteReorganization, cancel failed
    let (hook, cancelled) = cancel_job_hook(session, SchemaState::WRITE_REORGANIZATION);
    exec(session, &flashback_to(ts));
    drop(hook);
    let cancelled = cancelled.lock().unwrap();
    assert_eq!(cancelled.len(), 1);
    assert!(cancelled[0].starts_with("error: [ddl:8226]This job:") && cancelled[0].ends_with(" is almost finished, can't be cancelled now"), "{cancelled:?}");
    assert_eq!(variable(session, "tidb_ttl_job_enable"), "OFF");
}

/// Go `DDLExec.Next`'s `*ast.FlashBackToTimestampStmt` arm refuses a table or
/// database target; Go `onFlashbackCluster` refuses a store that is not TiKV.
#[test]
fn flashback_to_timestamp_refusals_like_go() {
    let mut session = mock_store_session();
    let session = &mut session;
    exec(session, "use test");
    exec(session, "create table t(a int)");
    let ts = get_timestamp(session);
    let at = flashback_to(ts).replace("flashback cluster ", "");
    assert_eq!(
        error_of(session, &format!("flashback table t {at}")),
        (8200, "Unsupported FLASHBACK table TO TIMESTAMP".to_owned())
    );
    assert_eq!(
        error_of(session, &format!("flashback database test {at}")),
        (8200, "Unsupported FLASHBACK database TO TIMESTAMP".to_owned())
    );
    exec(session, "set global tidb_ttl_job_enable = off");
    assert_eq!(
        error_of(session, &flashback_to(ts)),
        (1105, "Not support flashback cluster in non-TiKV env".to_owned())
    );
    // Go `finishFlashbackCluster`: a job that failed in StateNone saved
    // nothing, so nothing is restored from the arguments it was queued with
    // (`EnableTTLJob: true`).
    assert_eq!(variable(session, "tidb_ttl_job_enable"), "OFF");
}

/// Go `checkAndSetFlashbackClusterInfo`: a DDL job that was running at the
/// flashback timestamp refuses the flashback.
#[test]
fn a_ddl_job_at_the_flashback_timestamp_is_refused_like_go() {
    let mut session = mock_store_session();
    let session = &mut session;
    exec(session, "use test");
    exec(session, "create table t(a int)");
    let domain = crate::domain::mock_store::domain_of(session).expect("a mock store session");
    let running = Arc::new(Mutex::new(None));
    let seen = running.clone();
    let before = enable_call("beforeRunOneJobStep", move |job| {
        assert_eq!(job.type_, ActionType::ACTION_ADD_COLUMN);
        let mut seen = seen.lock().unwrap();
        if seen.is_none() {
            std::thread::sleep(std::time::Duration::from_millis(20));
            *seen = Some(get_timestamp(&mut domain.new_session().expect("a second session")));
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    });
    exec(session, "alter table t add column b int");
    drop(before);
    let ts = running.lock().unwrap().expect("the job ran a step");
    let _mock = mock_flashback(session, ts);
    let flashback = flashback_to(ts);
    let at = flashback.trim_start_matches("flashback cluster to timestamp '").trim_end_matches('\'');
    assert_eq!(
        error_of(session, &flashback),
        (1105, format!("Detected another DDL job at {at}, can't do flashback"))
    );
}

/// Go `ValidateFlashbackTS`, called by `checkAndSetFlashbackClusterInfo`: a
/// timestamp ahead of the store's is refused and the job cancelled.
#[test]
fn flashback_to_a_future_timestamp_is_refused_like_go() {
    let mut session = mock_store_session();
    let session = &mut session;
    let ts = get_timestamp(session);
    let _mock = mock_flashback(session, ts);
    let future = ((physical(ts) + 3_600_000) as u64) << 18;
    assert_eq!(
        error_of(session, &format!("flashback cluster to tso {future}")),
        (1105, "cannot set flashback timestamp to future time".to_owned())
    );
}

/// Go's `tidb_ddl_flashback_concurrency` `SetGlobal`:
/// `SetDDLFlashbackConcurrency(int32(tidbOptPositiveInt32(val, DefTiDBDDLFlashbackConcurrency)))`.
#[test]
fn the_flashback_concurrency_follows_its_global_variable_like_go() {
    let mut session = mock_store_session();
    let session = &mut session;
    assert_eq!(tidb_vardef::get_ddl_flashback_concurrency(), 64);
    exec(session, "set global tidb_ddl_flashback_concurrency = 32");
    assert_eq!(tidb_vardef::get_ddl_flashback_concurrency(), 32);
}

/// A schedule manager that keeps its configuration and records every write,
/// installed as the node's until the guard drops.
#[derive(Debug, Default)]
struct RecordingScheduleManager {
    config: Mutex<HashMap<String, serde_json::Value>>,
    writes: Mutex<Vec<HashMap<String, serde_json::Value>>>,
}

impl tidb_domain_infosync::schedule_manager::ScheduleManager for RecordingScheduleManager {
    fn get_schedule_config(&self) -> Result<HashMap<String, serde_json::Value>, String> {
        Ok(self.config.lock().unwrap().clone())
    }

    fn set_schedule_config(&self, config: &HashMap<String, serde_json::Value>) -> Result<(), String> {
        self.writes.lock().unwrap().push(config.clone());
        self.config.lock().unwrap().extend(config.iter().map(|(key, value)| (key.clone(), value.clone())));
        Ok(())
    }
}

struct RecordedSchedule(Arc<RecordingScheduleManager>);

impl RecordedSchedule {
    /// Installs a recording manager whose `merge-schedule-limit` is `limit`.
    fn install(limit: i64) -> Self {
        let manager = Arc::new(RecordingScheduleManager::default());
        manager.config.lock().unwrap().insert("merge-schedule-limit".to_owned(), serde_json::json!(limit));
        tidb_domain_infosync::schedule_manager::init_schedule_manager(Some(manager.clone()));
        Self(manager)
    }

    /// The `merge-schedule-limit` of every write, in order.
    fn writes(&self) -> Vec<serde_json::Value> {
        self.0.writes.lock().unwrap().iter().map(|write| write["merge-schedule-limit"].clone()).collect()
    }

    fn limit(&self) -> serde_json::Value {
        self.0.config.lock().unwrap()["merge-schedule-limit"].clone()
    }
}

impl Drop for RecordedSchedule {
    fn drop(&mut self) {
        tidb_domain_infosync::schedule_manager::init_schedule_manager(None);
    }
}

fn pd_schedule_state(job: &mut Job) -> String {
    tidb_model::get_flashback_cluster_args(job).unwrap().unwrap().read().pd_schedule_state.clone()
}

/// Rewrites the job's arguments as a node without `pd_schedule_state` (a Go
/// node, or this port before the field) would have left them.
fn forget_pd_schedule_state(job: &mut Job) {
    let args = tidb_model::get_flashback_cluster_args(job).unwrap().unwrap();
    args.write().pd_schedule_state.clear();
    job.fill_args(Some(args));
    job.encode(true).unwrap();
}

fn cancel(job: &mut Job) {
    job.state = JobState::CANCELLED;
    job.error = Some(tidb_model::GoShared::new(tidb_dbterror::ERR_CANCELLED_DDL_JOB.clone()));
}

/// The last finished job's persisted warning, if any.
fn last_history_warning(session: &mut Session) -> Option<String> {
    let meta = rows(session, "select job_meta from mysql.tidb_ddl_history order by job_id desc limit 1").remove(0).remove(0);
    let job: serde_json::Value = serde_json::from_str(&meta).expect("a job's JSON");
    job["warning"].get("Message").or_else(|| job["warning"].get("message")).map(|message| message.to_string())
}

/// A job that closed PD's schedule limits restores them, whether it
/// finishes, is cancelled after the close, or is refused in the step that
/// closed them.
#[test]
fn flashback_restores_the_pd_schedule_it_closed() {
    let mut session = mock_store_session();
    let session = &mut session;
    exec(session, "use test");
    exec(session, "create table t(a int)");
    std::thread::sleep(std::time::Duration::from_millis(10));
    let ts = get_timestamp(session);
    let _mock = mock_flashback(session, ts);

    let schedule = RecordedSchedule::install(1);
    exec(session, &flashback_to(ts));
    assert_eq!(schedule.writes(), [serde_json::json!(0), serde_json::json!(1)]);
    assert_eq!(schedule.limit(), serde_json::json!(1));
    drop(schedule);

    let schedule = RecordedSchedule::install(3);
    let after = enable_call("afterRunOneJobStep", |job| {
        if job.schema_state == SchemaState::WRITE_REORGANIZATION {
            assert_eq!(pd_schedule_state(job), tidb_model::PD_SCHEDULE_CLOSED);
            cancel(job);
        }
    });
    let ts = get_timestamp(session);
    assert_eq!(error_of(session, &flashback_to(ts)), (8214, "Cancelled DDL job".to_owned()));
    drop(after);
    assert_eq!(schedule.writes(), [serde_json::json!(0), serde_json::json!(3)]);
    assert_eq!(last_history_warning(session), None);
}

/// A job refused in the step that closed the limits (here by a DDL job at
/// the flashback timestamp, checked after the close) restores them too.
#[test]
fn flashback_refused_after_closing_the_pd_schedule_restores_it() {
    let mut session = mock_store_session();
    let session = &mut session;
    exec(session, "use test");
    exec(session, "create table t(a int)");
    let domain = crate::domain::mock_store::domain_of(session).expect("a mock store session");
    let running = Arc::new(Mutex::new(None));
    let seen = running.clone();
    let before = enable_call("beforeRunOneJobStep", move |job| {
        assert_eq!(job.type_, ActionType::ACTION_ADD_COLUMN);
        let mut seen = seen.lock().unwrap();
        if seen.is_none() {
            std::thread::sleep(std::time::Duration::from_millis(20));
            *seen = Some(get_timestamp(&mut domain.new_session().expect("a second session")));
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    });
    exec(session, "alter table t add column b int");
    drop(before);
    let ts = running.lock().unwrap().expect("the job ran a step");
    let _mock = mock_flashback(session, ts);
    let schedule = RecordedSchedule::install(2);
    assert_eq!(error_of(session, &flashback_to(ts)).0, 1105);
    assert_eq!(schedule.writes(), [serde_json::json!(0), serde_json::json!(2)]);
}

/// A job cancelled after it saved the limits and before it asked PD to
/// close them writes nothing to PD: an operator's later setting stays.
#[test]
fn flashback_cancelled_before_closing_the_pd_schedule_writes_no_restore() {
    let mut session = mock_store_session();
    let session = &mut session;
    std::thread::sleep(std::time::Duration::from_millis(10));
    let ts = get_timestamp(session);
    let _mock = mock_flashback(session, ts);

    let schedule = RecordedSchedule::install(1);
    let manager = schedule.0.clone();
    let after = enable_call("afterRunOneJobStep", move |job| {
        if job.schema_state == SchemaState::DELETE_ONLY && pd_schedule_state(job) == tidb_model::PD_SCHEDULE_SAVED {
            // The operator raises the limit while the job waits.
            manager.config.lock().unwrap().insert("merge-schedule-limit".to_owned(), serde_json::json!(5));
            cancel(job);
        }
    });
    assert_eq!(error_of(session, &flashback_to(ts)), (8214, "Cancelled DDL job".to_owned()));
    drop(after);
    assert!(schedule.writes().is_empty(), "{:?}", schedule.writes());
    assert_eq!(schedule.limit(), serde_json::json!(5));
    assert_eq!(last_history_warning(session), None);
}

/// The crash window: the intent to close is persisted and PD took the
/// close, but the job stopped before it recorded the close. Ending there,
/// the job cannot tell its close from an operator's: it leaves the limits
/// and hands off, naming itself and the saved value.
#[test]
fn flashback_between_the_intent_and_the_recorded_close_hands_off_to_the_operator() {
    let mut session = mock_store_session();
    let session = &mut session;
    std::thread::sleep(std::time::Duration::from_millis(10));
    let ts = get_timestamp(session);
    let _mock = mock_flashback(session, ts);

    let schedule = RecordedSchedule::install(4);
    let manager = schedule.0.clone();
    let after = enable_call("afterRunOneJobStep", move |job| {
        if job.schema_state == SchemaState::DELETE_ONLY && pd_schedule_state(job) == tidb_model::PD_SCHEDULE_CLOSING {
            // The close that reached PD before the node went away.
            manager.config.lock().unwrap().insert("merge-schedule-limit".to_owned(), serde_json::json!(0));
            cancel(job);
        }
    });
    assert_eq!(error_of(session, &flashback_to(ts)), (8214, "Cancelled DDL job".to_owned()));
    drop(after);
    assert!(schedule.writes().is_empty(), "no restore: {:?}", schedule.writes());
    assert_eq!(schedule.limit(), serde_json::json!(0));
    let warning = last_history_warning(session).expect("the handoff warning");
    assert!(warning.contains("left as they are") && warning.contains(r#"\"merge-schedule-limit\":4"#), "{warning}");
}

/// A job whose arguments carry no record (a Go node's, or this port's before
/// the record): past `DELETE_ONLY` it has closed the limits and restores
/// them; in `DELETE_ONLY` nothing proves the close, so it hands off.
#[test]
fn flashback_without_a_pd_schedule_record_restores_only_past_delete_only() {
    let mut session = mock_store_session();
    let session = &mut session;
    std::thread::sleep(std::time::Duration::from_millis(10));
    let ts = get_timestamp(session);
    let _mock = mock_flashback(session, ts);

    let schedule = RecordedSchedule::install(6);
    let after = enable_call("afterRunOneJobStep", |job| {
        if job.schema_state == SchemaState::DELETE_ONLY && pd_schedule_state(job) == tidb_model::PD_SCHEDULE_SAVED {
            forget_pd_schedule_state(job);
            cancel(job);
        }
    });
    assert_eq!(error_of(session, &flashback_to(ts)), (8214, "Cancelled DDL job".to_owned()));
    drop(after);
    assert!(schedule.writes().is_empty(), "{:?}", schedule.writes());
    let warning = last_history_warning(session).expect("the handoff warning");
    assert!(warning.contains(r#"\"merge-schedule-limit\":6"#), "{warning}");
    drop(schedule);

    let schedule = RecordedSchedule::install(7);
    let after = enable_call("afterRunOneJobStep", |job| {
        if job.schema_state == SchemaState::WRITE_REORGANIZATION {
            forget_pd_schedule_state(job);
            cancel(job);
        }
    });
    let ts = get_timestamp(session);
    assert_eq!(error_of(session, &flashback_to(ts)), (8214, "Cancelled DDL job".to_owned()));
    drop(after);
    assert_eq!(schedule.writes(), [serde_json::json!(0), serde_json::json!(7)]);
}

/// A job with no record resumed in `DELETE_ONLY` (a Go node's, or this
/// port's before the record) does not advance on the strength of its saved
/// value: it records its intent, closes the limits and records the close
/// before it leaves `DELETE_ONLY`, and only then restores at the end.
#[test]
fn flashback_resumed_without_a_pd_schedule_record_closes_before_it_advances() {
    let mut session = mock_store_session();
    let session = &mut session;
    std::thread::sleep(std::time::Duration::from_millis(10));
    let ts = get_timestamp(session);
    let _mock = mock_flashback(session, ts);

    let schedule = RecordedSchedule::install(8);
    let manager = schedule.0.clone();
    let steps = Arc::new(Mutex::new(Vec::new()));
    let seen = steps.clone();
    let forgotten = std::sync::atomic::AtomicBool::new(false);
    let after = enable_call("afterRunOneJobStep", move |job| {
        if job.schema_state == SchemaState::DELETE_ONLY
            && pd_schedule_state(job) == tidb_model::PD_SCHEDULE_SAVED
            && !forgotten.swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            forget_pd_schedule_state(job);
        }
        let limit = manager.config.lock().unwrap()["merge-schedule-limit"].clone();
        seen.lock().unwrap().push((job.schema_state, pd_schedule_state(job), limit));
    });
    exec(session, &flashback_to(ts));
    drop(after);
    let steps = steps.lock().unwrap().clone();
    let first_past = steps
        .iter()
        .find(|(state, ..)| *state == SchemaState::WRITE_ONLY)
        .expect("the job reached WRITE_ONLY");
    assert_eq!(
        (first_past.1.as_str(), &first_past.2),
        (tidb_model::PD_SCHEDULE_CLOSED, &serde_json::json!(0)),
        "{steps:?}"
    );
    assert!(
        steps.iter().any(|(state, record, _)| *state == SchemaState::DELETE_ONLY && record == tidb_model::PD_SCHEDULE_CLOSING),
        "{steps:?}"
    );
    assert_eq!(schedule.writes(), [serde_json::json!(0), serde_json::json!(8)]);
    assert_eq!(schedule.limit(), serde_json::json!(8));
    assert_eq!(last_history_warning(session), None);
}
