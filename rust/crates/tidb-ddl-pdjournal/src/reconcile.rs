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

//! The current DDL owner's repair of its keyspace's PD objects.
//!
//! A pass (1) resolves every intent no live attempt holds: an intent whose op
//! committed the record is done, one a later generation superseded is
//! dropped, and any other is aborted -- its record's generation moves past it
//! (the pre-image becomes the record of an object that had none), which a
//! delayed commit of the same attempt then conflicts with or fails to
//! compare; then (2) compares every record without an open intent with PD
//! ([`crate::normalize`]) and rewrites, then reads back, each object PD holds
//! differently; and (3) drops the tombstones PD agrees with once they are
//! older than [`Options::tombstone_retention`].
//!
//! Timing: a pass runs at takeover and then every [`SCAN_INTERVAL`]. A late
//! write by a paused old owner is repaired by the first pass that starts
//! after it lands, so it stays in PD for at most the interval plus that
//! pass's duration: one journal scan, one PD read per record (one request
//! per bundle and group, one for all label rules), and for each differing
//! object a re-read of its record, one PD write and one PD read back, each
//! bounded by the PD client's timeout. A failed write or read back is retried
//! by the next pass, so each failure adds an interval. A non-job intent
//! (ALTER RANGE, GC) is left for [`Options::non_job_grace`] first, since its
//! session may still be finalizing it.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use tidb_meta::MetaStore;

use crate::journal::{self, TxnRead};
use crate::live;
use crate::normalize::in_sync;
use crate::pd::PdObjects;
use crate::record::wall_ms;
use crate::{Intent, ObjectKey, Record};

/// How often the owner reconciles after its takeover pass.
pub const SCAN_INTERVAL: Duration = Duration::from_secs(30);

/// One keyspace's reconciler.
#[derive(Clone, Debug)]
pub struct Options {
    /// The DDL owner ID of this node.
    pub owner_id: String,
    /// The keyspace (the store's keyspace ID) the live attempts are scoped by.
    pub scope: u32,
    /// The keyspace a resource group's client reads it for, `None` for an
    /// API-V1 store.
    pub keyspace_id: Option<u32>,
    /// How long a non-job intent is left to its session.
    pub non_job_grace: Duration,
    /// How long a tombstone PD agrees with is kept, against a delayed
    /// resurrection of its object.
    pub tombstone_retention: Duration,
}

impl Options {
    /// The reconciler of keyspace `scope` run by DDL owner `owner_id`.
    #[must_use]
    pub fn new(owner_id: &str, scope: u32, keyspace_id: Option<u32>) -> Self {
        Self {
            owner_id: owner_id.to_owned(),
            scope,
            keyspace_id,
            non_job_grace: Duration::from_secs(60),
            tombstone_retention: Duration::from_secs(24 * 3600),
        }
    }
}

/// What one pass did.
#[derive(Debug, Default, PartialEq)]
pub struct Report {
    /// Intents whose op had committed (or was superseded), dropped.
    pub committed: usize,
    /// Intents aborted.
    pub aborted: usize,
    /// Objects left to a live attempt (or a young non-job intent).
    pub left: usize,
    /// Records PD agreed with.
    pub in_sync: usize,
    /// Objects rewritten and read back equal.
    pub repaired: Vec<ObjectKey>,
    /// Objects whose repair failed, retried by the next pass.
    pub failed: Vec<(ObjectKey, String)>,
    /// Tombstones dropped.
    pub compacted: usize,
}

impl Report {
    fn changed_anything(&self) -> bool {
        self.committed + self.aborted + self.compacted > 0 || !self.repaired.is_empty() || !self.failed.is_empty()
    }
}

struct Journal {
    records: BTreeMap<ObjectKey, Record>,
    intents: Vec<Intent>,
    now_ms: u64,
}

fn read_journal(store: &dyn MetaStore) -> Result<Journal, String> {
    let mut read = None;
    journal::run(store, &mut |txn| {
        let now_ms = wall_ms();
        let mut reader = TxnRead(txn);
        read = Some(Journal {
            records: journal::scan_records(&mut reader)?,
            intents: journal::scan_intents(&mut reader)?,
            now_ms,
        });
        Ok(())
    })?;
    Ok(read.expect("the read ran"))
}

enum Verdict {
    Committed,
    Abort,
    Leave,
}

fn verdict(record: Option<&Record>, intent: &Intent, now_ms: u64, options: &Options) -> Verdict {
    if let Some(record) = record {
        if record.op_id == intent.op_id || record.generation >= intent.generation {
            return Verdict::Committed;
        }
    }
    let young = now_ms.saturating_sub(intent.at_ms) < u64::try_from(options.non_job_grace.as_millis()).unwrap_or(u64::MAX);
    if intent.job_id == 0 && young {
        return Verdict::Leave;
    }
    Verdict::Abort
}

/// Aborts `intent`: its record moves to the intent's generation with the
/// value it had (the pre-image for an object without one), and the intent
/// goes. Answers whether its op had committed after all.
fn abort(store: &dyn MetaStore, intent: &Intent) -> Result<bool, String> {
    let mut committed = false;
    journal::run(store, &mut |txn| {
        let record = journal::read_record(&mut TxnRead(&mut *txn), &intent.key)?;
        committed = record.as_ref().is_some_and(|record| record.op_id == intent.op_id);
        if !committed {
            let burned = match record {
                Some(record) => Record {
                    generation: record.generation.max(intent.generation),
                    op_id: format!("aborted:{}", intent.op_id),
                    ..record
                },
                None => Record {
                    key: intent.key.clone(),
                    generation: intent.generation,
                    op_id: format!("aborted:{}", intent.op_id),
                    value: intent.pre_image.clone(),
                    at_ms: wall_ms(),
                },
            };
            journal::put_record(txn, &burned)?;
        }
        journal::delete_intent(txn, intent)
    })?;
    Ok(committed)
}

fn drop_intent(store: &dyn MetaStore, intent: &Intent) -> Result<(), String> {
    journal::run(store, &mut |txn| journal::delete_intent(txn, intent))
}

/// Whether `record` is still the record of its object, with no intent open.
fn still_settled(store: &dyn MetaStore, record: &Record) -> Result<bool, String> {
    let mut settled = false;
    journal::run(store, &mut |txn| {
        let mut reader = TxnRead(txn);
        settled = journal::read_record(&mut reader, &record.key)?.as_ref() == Some(record)
            && !journal::scan_intents(&mut reader)?.iter().any(|intent| intent.key == record.key);
        Ok(())
    })?;
    Ok(settled)
}

fn resolve_intents(
    store: &dyn MetaStore,
    options: &Options,
    still_owner: &dyn Fn() -> bool,
    report: &mut Report,
) -> Result<(), String> {
    let Journal { records, mut intents, now_ms } = read_journal(store)?;
    intents.sort_by(|left, right| (&left.key, left.generation).cmp(&(&right.key, right.generation)));
    for intent in &intents {
        if live::is_busy(options.scope, &intent.key) {
            report.left += 1;
            continue;
        }
        match verdict(records.get(&intent.key), intent, now_ms, options) {
            Verdict::Leave => report.left += 1,
            Verdict::Committed => {
                drop_intent(store, intent)?;
                report.committed += 1;
            }
            Verdict::Abort => {
                if !still_owner() {
                    return Ok(());
                }
                if abort(store, intent)? {
                    report.committed += 1;
                } else {
                    report.aborted += 1;
                }
            }
        }
    }
    Ok(())
}

fn repair(store: &dyn MetaStore, pd: &dyn PdObjects, options: &Options, record: &Record) -> Result<bool, String> {
    if !still_settled(store, record)? {
        return Ok(false);
    }
    let Some(written) = live::unless_busy(options.scope, &record.key, || pd.write(&record.key, record.value.as_ref())) else {
        return Ok(false);
    };
    written?;
    let held = pd.read(std::slice::from_ref(&record.key))?;
    if in_sync(&record.key, record.value.as_ref(), held.get(&record.key), options.keyspace_id)? {
        Ok(true)
    } else {
        Err("PD still holds a different value after the rewrite".to_owned())
    }
}

/// One pass over the keyspace's journal; stops early once `still_owner`
/// answers false.
///
/// # Errors
/// The journal or PD could not be read.
pub fn reconcile_once(
    store: &dyn MetaStore,
    pd: &dyn PdObjects,
    options: &Options,
    still_owner: &dyn Fn() -> bool,
) -> Result<Report, String> {
    let mut report = Report::default();
    resolve_intents(store, options, still_owner, &mut report)?;
    let Journal { records, intents, now_ms } = read_journal(store)?;
    let open = intents.into_iter().map(|intent| intent.key).collect::<BTreeSet<_>>();
    let keys = records.keys().filter(|key| !open.contains(*key)).cloned().collect::<Vec<_>>();
    let held = pd.read(&keys)?;
    let retention = u64::try_from(options.tombstone_retention.as_millis()).unwrap_or(u64::MAX);
    let mut expired = Vec::new();
    for key in &keys {
        let record = &records[key];
        if in_sync(key, record.value.as_ref(), held.get(key), options.keyspace_id)? {
            report.in_sync += 1;
            if record.value.is_none() && now_ms.saturating_sub(record.at_ms) > retention {
                expired.push(record);
            }
            continue;
        }
        if !still_owner() {
            return Ok(report);
        }
        match repair(store, pd, options, record) {
            Ok(true) => report.repaired.push(key.clone()),
            Ok(false) => report.left += 1,
            Err(error) => report.failed.push((key.clone(), error)),
        }
    }
    for record in expired {
        let compacted = journal::run(store, &mut |txn| {
            if journal::read_record(&mut TxnRead(&mut *txn), &record.key)?.as_ref() == Some(record) {
                journal::delete_record(txn, &record.key)?;
            }
            Ok(())
        });
        if compacted.is_ok() {
            report.compacted += 1;
        }
    }
    Ok(report)
}

fn log_pass(options: &Options, outcome: &Result<Report, String>, took: Duration) {
    use tidb_log::{Field, Value};
    let logger = tidb_logutil::bg_logger();
    match outcome {
        Ok(report) if report.changed_anything() => logger.info(
            "PD journal reconciled",
            &[
                Field::new("owner", Value::Str(options.owner_id.clone())),
                Field::new("keyspace", Value::U64(u64::from(options.scope))),
                Field::new("report", Value::Str(format!("{report:?}"))),
                Field::new("took", Value::Duration(i64::try_from(took.as_nanos()).unwrap_or(i64::MAX))),
            ],
        ),
        Ok(_) => {}
        Err(error) => logger.warn(
            "PD journal reconcile failed, retried at the next scan",
            &[
                Field::new("keyspace", Value::U64(u64::from(options.scope))),
                Field::new("error", Value::Str(error.clone())),
            ],
        ),
    }
}

/// The owner's loop: a pass at once, then one every `interval` until `wait`
/// (which waits up to its argument) answers that the owner is stopping.
pub fn run_until_stopped(
    store: &dyn MetaStore,
    pd: &dyn PdObjects,
    options: &Options,
    interval: Duration,
    still_owner: &dyn Fn() -> bool,
    wait: &dyn Fn(Duration) -> bool,
) {
    loop {
        if !still_owner() {
            return;
        }
        let started = Instant::now();
        let outcome = reconcile_once(store, pd, options, still_owner);
        log_pass(options, &outcome, started.elapsed());
        if wait(interval) {
            return;
        }
    }
}
