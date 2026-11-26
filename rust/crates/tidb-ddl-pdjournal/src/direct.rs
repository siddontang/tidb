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
//! then the records in a small meta transaction that commits only over the
//! generations the intent was prepared against. Two sessions that prepare
//! the same range both write PD, but only the first to finalize commits; the
//! other is refused, puts the record that won back into PD, and reports it.

use std::sync::atomic::{AtomicU64, Ordering};

use tidb_meta::MetaStore;

use crate::journal::{self, Attempt, Planned, TxnRead};
use crate::pd::PdObjects;
use crate::record::wall_ms;
use crate::JournalError;

/// A fresh op ID for a non-job write by `owner`.
fn next_op_id(owner: &str, at_ms: u64) -> String {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    format!("{owner}/direct/{at_ms}/{}", SEQUENCE.fetch_add(1, Ordering::Relaxed))
}

/// Makes PD hold `planned` through the journal, as `owner` (the node) of the
/// keyspace `scope`. Succeeds only once the records committed.
///
/// # Errors
/// [`JournalError::NoBaseline`] (PD untouched), a failed PD write (the
/// pre-images are put back), a record that moved since the prepare (the
/// winner's value is put back), or a failed final commit (the reconciler
/// resolves it).
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
        let attempt = Attempt { op_id: next_op_id(owner, at_ms), owner: owner.to_owned(), job_id: 0, scope };
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
            let mut detail = format!("writing {}: {error}", intent.key);
            match prepared.restore_pre_images(pd) {
                Ok(()) => {
                    let _ = prepared.clear_intents(store);
                }
                Err(restore) => detail.push_str(&format!("; and restoring the previous values failed: {restore}")),
            }
            return Err(JournalError::Storage(detail));
        }
    }
    match prepared.finalize(store) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(moved)) => {
            // Nothing committed: every object goes back to its pre-image,
            // and the ones that moved to the record that won.
            let mut detail = String::from("a concurrent change committed first:");
            if let Err(error) = prepared.restore_pre_images(pd) {
                detail = format!("restoring the previous values failed ({error}); {detail}");
            }
            for (key, record) in &moved {
                detail.push_str(&format!(" {key}"));
                if let Err(error) = pd.write(key, record.as_ref().and_then(|record| record.value.as_ref())) {
                    detail.push_str(&format!(" (putting its value back failed: {error})"));
                }
            }
            let _ = prepared.clear_intents(store);
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
