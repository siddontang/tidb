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

//! The writers outside a DDL job (ALTER RANGE, GC's deletes): intent, PD,
//! then the records in a small meta transaction that commits only while the
//! attempt's intents are still outstanding as prepared and over the
//! generations they were prepared against. Two sessions that prepare the
//! same range both write PD, but only the first to finalize commits; the
//! other is refused, puts the record that won back into PD, and reports it.
//! An attempt that an abort resolved (its own, or the owner's reconciler's)
//! is refused too, and never reports success.

use tidb_meta::MetaStore;

use crate::journal::{self, Attempt, Planned, Refusal, TxnRead};
use crate::pd::PdObjects;
use crate::record::wall_ms;
use crate::JournalError;

/// The op ID of a non-job write by `owner` whose prepare transaction
/// started at `start_ts`: PD's TSO makes it unique among every process's
/// attempts, as a DDL step's start timestamp is (an owner name such as GC's
/// is not unique across processes).
fn op_id(owner: &str, start_ts: u64) -> String {
    format!("{owner}/direct/{start_ts}")
}

/// Makes PD hold `planned` through the journal, as `owner` (the node) of the
/// keyspace `scope`. Succeeds only once the records committed.
///
/// # Errors
/// [`JournalError::NoBaseline`] (PD untouched), a failed PD write (the
/// pre-images are put back and the abort recorded), a record that moved
/// since the prepare (the winner's value is put back), an attempt the
/// owner's reconciler aborted meanwhile (the records' values are put back),
/// or a failed final commit (the reconciler resolves it).
pub fn write_through(
    store: &dyn MetaStore,
    pd: &dyn PdObjects,
    owner: &str,
    scope: u32,
    planned: &[Planned],
) -> Result<(), JournalError> {
    if planned.is_empty() {
        return Ok(());
    }
    let mut prepared = None;
    let mut refused = None;
    journal::run(store, &mut |txn| {
        let at_ms = wall_ms();
        let attempt = Attempt { op_id: op_id(owner, txn.start_ts()), owner: owner.to_owned(), job_id: 0, scope };
        // No catalog baseline here: GC's bundles are of dropped tables, which
        // the catalog no longer holds.
        let plan = journal::plan(&mut TxnRead(&mut *txn), &attempt, planned, &mut |_| Ok(Default::default()), at_ms);
        match plan {
            Ok(plan) => {
                plan.put_intents(txn)?;
                prepared = Some(plan);
                Ok(())
            }
            Err(error) => {
                refused = Some(error.clone());
                Err(error.to_string())
            }
        }
    })
    .map_err(|error| refused.take().unwrap_or(JournalError::Storage(error)))?;
    let prepared = prepared.expect("a committed prepare planned");
    // A write that failed may still land; the abort records the baseline
    // first, so the reconciler reverts it.
    let abort = |detail: String| {
        let mut detail = detail;
        if let Err(error) = prepared.restore_pre_images(pd) {
            detail.push_str(&format!("; restoring the previous values failed: {error}"));
        }
        if let Err(error) = prepared.abort(store) {
            detail.push_str(&format!("; recording the abort failed, the reconciler resolves it: {error}"));
        }
        JournalError::Storage(detail)
    };
    for intent in prepared.intents() {
        if let Err(error) = pd.write(&intent.key, intent.value.as_ref()) {
            return Err(abort(format!("writing {}: {error}", intent.key)));
        }
    }
    match prepared.finalize(store) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(Refusal::Moved(moved))) => {
            // Nothing committed: every object goes back to its pre-image,
            // and the ones that moved to the record that won.
            let mut error = abort(String::from("a concurrent change committed first:"));
            for (key, record) in &moved {
                if let Err(write) = pd.write(key, record.as_ref().and_then(|record| record.value.as_ref())) {
                    error = JournalError::Storage(format!("{error}; putting {key} back failed: {write}"));
                }
            }
            Err(error)
        }
        Ok(Err(Refusal::Resolved(keys))) => {
            // An abort resolved the attempt and recorded each object's
            // baseline; PD gets the records back (the reconciler would too).
            let mut detail = format!("the write was aborted before it committed: {keys:?}");
            match read_current(store, &keys) {
                Ok((records, _)) => {
                    for key in &keys {
                        let value = records.get(key).and_then(|record| record.value.as_ref());
                        if let Err(error) = pd.write(key, value) {
                            detail.push_str(&format!("; putting {key} back failed: {error}"));
                        }
                    }
                }
                Err(error) => detail.push_str(&format!("; reading the records failed: {error}")),
            }
            Err(JournalError::Storage(detail))
        }
        Err(error) => Err(JournalError::Storage(format!(
            "committing the PD journal records failed, the reconciler resolves them: {error}"
        ))),
    }
}

/// The records of `keys` and whether the keyspace had its cutover, read in
/// one transaction: what a writer that chooses objects by their record (GC)
/// chooses from.
///
/// # Errors
/// The journal could not be read.
pub fn read_current(
    store: &dyn MetaStore,
    keys: &[crate::ObjectKey],
) -> Result<(std::collections::BTreeMap<crate::ObjectKey, crate::Record>, bool), String> {
    let mut read = (std::collections::BTreeMap::new(), false);
    journal::run(store, &mut |txn| {
        let mut reader = TxnRead(txn);
        read.0.clear();
        for key in keys {
            if let Some(record) = journal::read_record(&mut reader, key)? {
                read.0.insert(key.clone(), record);
            }
        }
        read.1 = journal::read_cutover(&mut reader)?.is_some();
        Ok(())
    })?;
    Ok(read)
}
