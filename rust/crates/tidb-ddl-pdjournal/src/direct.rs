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
    for intent in prepared.intents() {
        if let Err(error) = pd.write(&intent.key, intent.value.as_ref()) {
            return Err(abandon(store, pd, &prepared, format!("writing {}: {error}", intent.key)));
        }
    }
    match prepared.finalize(store) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(Refusal::Moved(moved))) => {
            let keys = moved.into_iter().map(|(key, _)| key).collect::<Vec<_>>();
            Err(abandon(store, pd, &prepared, format!("a concurrent change committed first: {}", names(&keys))))
        }
        Ok(Err(Refusal::Resolved(keys))) => Err(abandon(
            store,
            pd,
            &prepared,
            format!("the write was aborted before it committed: {}", names(&keys)),
        )),
        Err(error) => Err(JournalError::Storage(format!(
            "committing the PD journal records failed, the reconciler resolves them: {error}"
        ))),
    }
}

fn names(keys: &[crate::ObjectKey]) -> String {
    keys.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
}

/// Resolves the whole of a definitely refused or failed attempt: every one
/// of its intents is aborted durably ([`journal::Prepared::abort`]: an
/// intent an abort or a newer record already resolved is just removed),
/// then every object it wrote gets its current authoritative record back
/// in PD -- the baseline the abort recorded, or the newer record of a
/// writer that won. A write of the attempt that PD applies later is a
/// divergence the reconciler reverts. Answers the error, with `detail`.
fn abandon(store: &dyn MetaStore, pd: &dyn PdObjects, prepared: &journal::Prepared, detail: String) -> JournalError {
    let mut detail = detail;
    let aborted = prepared.abort(store);
    if let Err(error) = &aborted {
        detail.push_str(&format!("; recording the abort failed, the reconciler resolves it: {error}"));
    }
    let keys = prepared.intents().iter().map(|intent| intent.key.clone()).collect::<Vec<_>>();
    match read_current(store, &keys) {
        Ok((records, _)) => {
            for intent in prepared.intents() {
                // Without a record (the abort did not commit), the pre-image.
                let value = match records.get(&intent.key) {
                    Some(record) => record.value.as_ref(),
                    None => intent.pre_image.as_ref(),
                };
                if let Err(error) = pd.write(&intent.key, value) {
                    detail.push_str(&format!("; putting {} back failed: {error}", intent.key));
                }
            }
        }
        Err(error) => {
            detail.push_str(&format!("; reading the records failed, the reconciler repairs PD: {error}"));
        }
    }
    JournalError::Storage(detail)
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
