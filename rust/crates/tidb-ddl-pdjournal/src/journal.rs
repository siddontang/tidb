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

//! Reading and writing the journal: an attempt's prepare, the records it
//! commits, its abort, and the operator's cutover.

use std::collections::BTreeMap;

use tidb_meta::transaction::MetaTransaction;
use tidb_meta::MetaStore;

use crate::live::LiveHold;
use crate::pd::PdObjects;
use crate::record::{cutover_key, intent_key, intents_prefix, record_key, wall_ms, records_prefix};
use crate::{Cutover, Intent, ObjectKey, PdValue, Record};

/// Raw key/value pairs, in key order.
pub type Pairs = Vec<(Vec<u8>, Vec<u8>)>;

/// The objects whose record moved since an attempt's prepare, with the
/// record each holds now.
pub type Moved = Vec<(ObjectKey, Option<Record>)>;

/// Reads of the journal's keys, from a meta transaction or a DDL step's
/// snapshot.
pub trait JournalRead {
    /// The value under `key`.
    ///
    /// # Errors
    /// The read failed.
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, String>;
    /// Every pair under the raw `prefix`, in key order.
    ///
    /// # Errors
    /// The scan failed.
    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<Pairs, String>;
}

/// A meta transaction read as the journal.
pub struct TxnRead<'a>(pub &'a mut dyn MetaTransaction);

impl JournalRead for TxnRead<'_> {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        self.0.get(key).map_err(|error| error.to_string())
    }

    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<Pairs, String> {
        self.0.scan_prefix(prefix).map_err(|error| error.to_string())
    }
}

/// Why an attempt cannot write its PD objects.
#[derive(Clone, Debug, PartialEq)]
pub enum JournalError {
    /// A PD-only object has no record and the keyspace had no cutover, so
    /// nothing says what an abort would put back.
    NoBaseline(ObjectKey),
    /// The journal could not be read or written.
    Storage(String),
}

impl std::fmt::Display for JournalError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoBaseline(key) => write!(
                formatter,
                "{key} has no PD journal baseline in this keyspace: run the PD journal cutover \
                 (start the DDL owner with TIDB_PD_JOURNAL_CUTOVER naming the keyspace ID) \
                 after draining the nodes that write PD without the journal"
            ),
            Self::Storage(detail) => write!(formatter, "PD journal: {detail}"),
        }
    }
}

impl std::error::Error for JournalError {}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    serde_json::from_slice(bytes).map_err(|error| format!("corrupt PD journal entry: {error}"))
}

fn encode<T: serde::Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).expect("journal entries serialize")
}

/// The authoritative record of `key`.
///
/// # Errors
/// The read failed, or the record is corrupt.
pub fn read_record(reader: &mut dyn JournalRead, key: &ObjectKey) -> Result<Option<Record>, String> {
    reader.get(&record_key(key))?.as_deref().map(decode).transpose()
}

/// The keyspace's cutover, `None` before it.
///
/// # Errors
/// As [`read_record`].
pub fn read_cutover(reader: &mut dyn JournalRead) -> Result<Option<Cutover>, String> {
    reader.get(&cutover_key())?.as_deref().map(decode).transpose()
}

/// Every authoritative record, by object.
///
/// # Errors
/// As [`read_record`].
pub fn scan_records(reader: &mut dyn JournalRead) -> Result<BTreeMap<ObjectKey, Record>, String> {
    reader
        .scan_prefix(&records_prefix())?
        .into_iter()
        .map(|(_, value)| decode::<Record>(&value).map(|record| (record.key.clone(), record)))
        .collect()
}

/// Every prepared intent.
///
/// # Errors
/// As [`read_record`].
pub fn scan_intents(reader: &mut dyn JournalRead) -> Result<Vec<Intent>, String> {
    reader
        .scan_prefix(&intents_prefix())?
        .into_iter()
        .map(|(_, value)| decode(&value))
        .collect()
}

/// The record `record` as the mutation that writes it.
#[must_use]
pub fn record_mutation(record: &Record) -> (Vec<u8>, Vec<u8>) {
    (record_key(&record.key), encode(record))
}

pub(crate) fn put_record(txn: &mut dyn MetaTransaction, record: &Record) -> Result<(), String> {
    let (key, value) = record_mutation(record);
    txn.set(key, value).map_err(|error| error.to_string())
}

pub(crate) fn delete_record(txn: &mut dyn MetaTransaction, key: &ObjectKey) -> Result<(), String> {
    txn.delete(&record_key(key)).map_err(|error| error.to_string())
}

pub(crate) fn delete_intent(txn: &mut dyn MetaTransaction, intent: &Intent) -> Result<(), String> {
    txn.delete(&intent_key(&intent.key, &intent.op_id)).map_err(|error| error.to_string())
}

/// Runs `body` in one fresh meta transaction of `store`, without retrying a
/// conflict: a conflict means another writer of the journal moved first.
pub(crate) fn run(
    store: &dyn MetaStore,
    body: &mut dyn FnMut(&mut dyn MetaTransaction) -> Result<(), String>,
) -> Result<(), String> {
    let mut failure = None;
    let result = store.run_in_new_txn(
        &tidb_gocontext::Context::background(),
        tidb_txnkv::INTERNAL_TXN_DDL,
        false,
        &mut |txn| {
            body(txn).map_err(|error| {
                failure = Some(error.clone());
                tidb_meta::MetaError::Storage(error)
            })
        },
    );
    match (result, failure) {
        (Ok(()), _) => Ok(()),
        (Err(_), Some(error)) => Err(error),
        (Err(error), None) => Err(error),
    }
}

/// One object an attempt writes, and what it wants PD to hold.
#[derive(Clone, Debug, PartialEq)]
pub struct Planned {
    /// The object.
    pub key: ObjectKey,
    /// The value, `None` to delete it.
    pub value: Option<PdValue>,
    /// For a writer that chose the object by its record (GC): the record's
    /// generation and op it chose, `None` for "no record". The prepare and
    /// the final commit both require it.
    pub expect: Option<Option<(u64, String)>>,
}

impl Planned {
    /// `key` to hold `value`, whatever its record.
    #[must_use]
    pub fn new(key: ObjectKey, value: Option<PdValue>) -> Self {
        Self { key, value, expect: None }
    }
}

/// Who makes an attempt.
#[derive(Clone, Debug)]
pub struct Attempt {
    /// Its op ID, stable across its PD retries and unique among attempts.
    pub op_id: String,
    /// The DDL owner ID, or the node's ID for a non-job writer.
    pub owner: String,
    /// The DDL job, 0 for a non-job writer.
    pub job_id: i64,
    /// The keyspace (the store's keyspace ID), which scopes the attempt's
    /// hold on its objects in this process.
    pub scope: u32,
}

/// An attempt's intents, prepared against the records it read.
#[derive(Debug)]
pub struct Prepared {
    attempt: Attempt,
    intents: Vec<Intent>,
    /// Each intent's record as the prepare read it.
    read: Vec<Option<(u64, String)>>,
    _hold: LiveHold,
}

/// The catalog's value of the catalog-reconstructible objects among `keys`
/// that have no record, as of the attempt's snapshot; an object the catalog
/// does not hold is missing from the answer.
pub type CatalogBaseline<'a> =
    &'a mut dyn FnMut(&[ObjectKey]) -> Result<BTreeMap<ObjectKey, PdValue>, String>;

/// The last write of each object among `planned`, in first-mention order.
fn dedup(planned: &[Planned]) -> Vec<Planned> {
    let mut order = Vec::new();
    let mut last = BTreeMap::new();
    for entry in planned {
        if last.insert(entry.key.clone(), entry.clone()).is_none() {
            order.push(entry.key.clone());
        }
    }
    order.into_iter().map(|key| last.remove(&key).expect("recorded")).collect()
}

/// The first PD-only object among `planned` that has no record before the
/// keyspace's cutover: the object [`plan`] refuses.
///
/// # Errors
/// A failed read.
pub fn missing_baseline(reader: &mut dyn JournalRead, planned: &[Planned]) -> Result<Option<ObjectKey>, String> {
    let mut cutover = None;
    for entry in planned {
        if entry.key.catalog_reconstructible() || read_record(reader, &entry.key)?.is_some() {
            continue;
        }
        if cutover.is_none() {
            cutover = Some(read_cutover(reader)?.is_some());
        }
        if cutover == Some(false) {
            return Ok(Some(entry.key.clone()));
        }
    }
    Ok(None)
}

/// Plans `attempt`'s intents for `planned` from `reader` (the attempt's
/// snapshot, whose records its commit replaces): each intent commits the
/// next generation, and its pre-image is the record's value, the catalog's
/// for a catalog-reconstructible object without one, or absent for a PD-only
/// object after the cutover. Holds the objects for the attempt until the
/// result is dropped.
///
/// # Errors
/// [`JournalError::NoBaseline`] for a PD-only object without a record before
/// the cutover (nothing is written), or a failed read.
pub fn plan(
    reader: &mut dyn JournalRead,
    attempt: &Attempt,
    planned: &[Planned],
    catalog: CatalogBaseline<'_>,
    now_ms: u64,
) -> Result<Prepared, JournalError> {
    let planned = dedup(planned);
    if let Some(key) = missing_baseline(reader, &planned).map_err(JournalError::Storage)? {
        return Err(JournalError::NoBaseline(key));
    }
    let mut records = Vec::with_capacity(planned.len());
    let mut from_catalog = Vec::new();
    for entry in &planned {
        let record = read_record(reader, &entry.key).map_err(JournalError::Storage)?;
        if record.is_none() && entry.key.catalog_reconstructible() {
            from_catalog.push(entry.key.clone());
        }
        if let Some(expect) = &entry.expect {
            let found = record.as_ref().map(|record| (record.generation, record.op_id.clone()));
            if found != *expect {
                return Err(JournalError::Storage(format!(
                    "{} changed since it was chosen (now {found:?})",
                    entry.key
                )));
            }
        }
        records.push(record);
    }
    let baselines = if from_catalog.is_empty() {
        BTreeMap::new()
    } else {
        catalog(&from_catalog).map_err(JournalError::Storage)?
    };
    let read = records
        .iter()
        .map(|record| record.as_ref().map(|record| (record.generation, record.op_id.clone())))
        .collect();
    let intents = planned
        .iter()
        .zip(records)
        .map(|(entry, record)| {
            let (generation, pre_image) = match record {
                Some(record) => (record.generation + 1, record.value),
                None => (1, baselines.get(&entry.key).cloned()),
            };
            Intent {
                key: entry.key.clone(),
                op_id: attempt.op_id.clone(),
                owner: attempt.owner.clone(),
                job_id: attempt.job_id,
                generation,
                value: entry.value.clone(),
                pre_image,
                at_ms: now_ms,
            }
        })
        .collect::<Vec<_>>();
    let hold = LiveHold::new(attempt.scope, intents.iter().map(|intent| intent.key.clone()).collect());
    Ok(Prepared {
        attempt: attempt.clone(),
        intents,
        read,
        _hold: hold,
    })
}

impl Prepared {
    /// The intents.
    #[must_use]
    pub fn intents(&self) -> &[Intent] {
        &self.intents
    }

    /// The attempt.
    #[must_use]
    pub fn attempt(&self) -> &Attempt {
        &self.attempt
    }

    /// Writes the intents into `txn`.
    ///
    /// # Errors
    /// The write failed.
    pub fn put_intents(&self, txn: &mut dyn MetaTransaction) -> Result<(), String> {
        for intent in &self.intents {
            txn.set(intent_key(&intent.key, &intent.op_id), encode(intent))
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    /// Commits the intents in a transaction of their own, before the PD
    /// writes.
    ///
    /// # Errors
    /// The commit failed; the attempt must not write PD (an intent that did
    /// commit is resolved by the reconciler).
    pub fn commit_intents(&self, store: &dyn MetaStore) -> Result<(), String> {
        if self.intents.is_empty() {
            return Ok(());
        }
        run(store, &mut |txn| self.put_intents(txn))
    }

    /// The records the attempt's commit writes, at `now_ms`.
    #[must_use]
    pub fn records(&self, now_ms: u64) -> Vec<Record> {
        self.intents
            .iter()
            .map(|intent| Record {
                key: intent.key.clone(),
                generation: intent.generation,
                op_id: intent.op_id.clone(),
                value: intent.value.clone(),
                at_ms: now_ms,
            })
            .collect()
    }

    /// Removes the intents once the attempt is resolved (committed, or its
    /// pre-images restored).
    ///
    /// # Errors
    /// The removal failed; the reconciler resolves the intents.
    pub fn clear_intents(&self, store: &dyn MetaStore) -> Result<(), String> {
        if self.intents.is_empty() {
            return Ok(());
        }
        run(store, &mut |txn| {
            for intent in &self.intents {
                delete_intent(txn, intent)?;
            }
            Ok(())
        })
    }

    /// Puts every object's pre-image back into PD: a write that failed may
    /// still have been applied.
    ///
    /// # Errors
    /// The restores that failed.
    pub fn restore_pre_images(&self, pd: &dyn PdObjects) -> Result<(), String> {
        let failures = self
            .intents
            .iter()
            .filter_map(|intent| {
                pd.write(&intent.key, intent.pre_image.as_ref())
                    .err()
                    .map(|error| format!("{}: {error}", intent.key))
            })
            .collect::<Vec<_>>();
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }

    /// Commits the attempt's records in a meta transaction of its own,
    /// provided each record is still the one the prepare read, and removes
    /// the intents. Answers the objects whose record moved, with the record
    /// now there, when it does not commit for that reason.
    ///
    /// # Errors
    /// The transaction failed (its outcome may be unknown).
    pub fn finalize(&self, store: &dyn MetaStore) -> Result<Result<(), Moved>, String> {
        let mut moved = Vec::new();
        run(store, &mut |txn| {
            moved.clear();
            let start_ms = wall_ms();
            for (intent, read) in self.intents.iter().zip(&self.read) {
                let current = read_record(&mut TxnRead(&mut *txn), &intent.key)?;
                let found = current.as_ref().map(|record| (record.generation, record.op_id.clone()));
                if found != *read {
                    moved.push((intent.key.clone(), current));
                }
            }
            if !moved.is_empty() {
                return Ok(());
            }
            for record in self.records(start_ms) {
                put_record(txn, &record)?;
            }
            for intent in &self.intents {
                delete_intent(txn, intent)?;
            }
            Ok(())
        })?;
        Ok(if moved.is_empty() { Ok(()) } else { Err(moved) })
    }
}

/// The operator's cutover of a keyspace whose PD-only objects predate the
/// journal: in one transaction, a record for each such object PD holds now
/// (`objects`, which the caller read from PD after draining every writer
/// that bypasses the journal) that has none, and the cutover. Does nothing
/// after an earlier cutover. Answers how many records it seeded.
///
/// # Errors
/// The transaction failed.
pub fn seed_cutover(store: &dyn MetaStore, objects: &BTreeMap<ObjectKey, PdValue>) -> Result<Option<usize>, String> {
    let mut seeded = None;
    run(store, &mut |txn| {
        seeded = None;
        if read_cutover(&mut TxnRead(&mut *txn))?.is_some() {
            return Ok(());
        }
        let at_ms = wall_ms();
        let mut count = 0;
        for (key, value) in objects {
            if key.catalog_reconstructible() || read_record(&mut TxnRead(&mut *txn), key)?.is_some() {
                continue;
            }
            put_record(
                txn,
                &Record {
                    key: key.clone(),
                    generation: 1,
                    op_id: "cutover".to_owned(),
                    value: Some(value.clone()),
                    at_ms,
                },
            )?;
            count += 1;
        }
        txn.set(cutover_key(), encode(&Cutover { at_ms, source: format!("operator, {count} objects seeded") }))
            .map_err(|error| error.to_string())?;
        seeded = Some(count);
        Ok(())
    })?;
    Ok(seeded)
}

/// The cutover a keyspace bootstrapped with the journal records at
/// bootstrap: no PD object of the keyspace exists yet.
#[must_use]
pub fn bootstrap_cutover_mutation(at_ms: u64) -> (Vec<u8>, Vec<u8>) {
    (cutover_key(), encode(&Cutover { at_ms, source: "bootstrap".to_owned() }))
}
