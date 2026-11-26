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
use crate::record::{
    cutover_key, held_back_key, held_back_prefix, intent_key, intents_prefix, record_key, records_prefix, wall_ms,
};
use crate::{Cutover, HeldBack, Intent, ObjectKey, PdValue, Record};

/// Raw key/value pairs, in key order.
pub type Pairs = Vec<(Vec<u8>, Vec<u8>)>;

/// The objects whose record moved since an attempt's prepare, with the
/// record each holds now.
pub type Moved = Vec<(ObjectKey, Option<Record>)>;

/// Why [`Prepared::finalize`] commits nothing.
#[derive(Debug)]
pub enum Refusal {
    /// The records moved since the prepare.
    Moved(Moved),
    /// The attempt's intents on these objects are no longer there: an abort
    /// (the owner's reconciler, or the attempt's own compensation) resolved
    /// them, so the attempt must not report success.
    Resolved(Vec<ObjectKey>),
}

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
    /// The operator's cutover held the object back ([`HeldBack`]): PD may
    /// hold a rule of TiDB's shape that is not known to be TiDB's, so no
    /// value is known to put back.
    HeldBack(ObjectKey, String),
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
            Self::HeldBack(key, reason) => write!(
                formatter,
                "{key} was held back at the PD journal cutover ({reason}), so TiDB does not write it: \
                 inspect it in PD and delete it or make it the table's own rule, then start the DDL \
                 owner with TIDB_PD_JOURNAL_CUTOVER naming the keyspace ID again to resolve it"
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

pub(crate) fn delete_intent(txn: &mut dyn MetaTransaction, intent: &Intent) -> Result<(), String> {
    txn.delete(&intent_key(&intent.key, &intent.op_id)).map_err(|error| error.to_string())
}

/// Whether `intent` itself, as prepared, is still outstanding in `txn`.
fn outstanding(txn: &mut dyn MetaTransaction, intent: &Intent) -> Result<bool, String> {
    let held = txn.get(&intent_key(&intent.key, &intent.op_id)).map_err(|error| error.to_string())?;
    Ok(held.as_deref().map(decode::<Intent>).transpose()?.is_some_and(|held| held.generation == intent.generation))
}

/// Resolves the aborted `intent` durably in `txn`, which also removes it:
/// unless its op committed or a later generation superseded it, the
/// object's record moves to a generation above the intent's and holds the
/// baseline (the record's value, or the intent's pre-image for an object
/// without one; a tombstone when that was absent). A write of the attempt
/// that lands later is then a divergence from a recorded value, which the
/// reconciler reverts, and a delayed commit of the attempt conflicts with
/// the record or fails its compare. Answers whether the op had committed.
pub(crate) fn resolve_aborted(txn: &mut dyn MetaTransaction, intent: &Intent) -> Result<bool, String> {
    let record = read_record(&mut TxnRead(&mut *txn), &intent.key)?;
    let committed = record.as_ref().is_some_and(|record| record.op_id == intent.op_id);
    let superseded = record.as_ref().is_some_and(|record| record.generation >= intent.generation);
    if !committed && !superseded {
        let value = match record {
            Some(record) => record.value,
            None => intent.pre_image.clone(),
        };
        put_record(
            txn,
            &Record {
                key: intent.key.clone(),
                generation: intent.generation + 1,
                op_id: format!("aborted:{}", intent.op_id),
                value,
                at_ms: wall_ms(),
            },
        )?;
    }
    delete_intent(txn, intent)?;
    Ok(committed)
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

/// What the cutover held back of `key`, `None` for an object it did not.
///
/// # Errors
/// As [`read_record`].
pub fn read_held_back(reader: &mut dyn JournalRead, key: &ObjectKey) -> Result<Option<HeldBack>, String> {
    reader.get(&held_back_key(key))?.as_deref().map(decode).transpose()
}

/// Every object the cutover held back and the operator has not resolved.
///
/// # Errors
/// As [`read_record`].
pub fn scan_held_back(reader: &mut dyn JournalRead) -> Result<Vec<HeldBack>, String> {
    reader
        .scan_prefix(&held_back_prefix())?
        .into_iter()
        .map(|(_, value)| decode(&value))
        .collect()
}

/// Why [`plan`] refuses `planned` before any PD write: the first PD-only
/// object without a record that the cutover held back
/// ([`JournalError::HeldBack`]), or that has no baseline because the
/// keyspace had no cutover yet ([`JournalError::NoBaseline`]).
///
/// # Errors
/// A failed read.
pub fn missing_baseline(reader: &mut dyn JournalRead, planned: &[Planned]) -> Result<Option<JournalError>, String> {
    let mut cutover = None;
    for entry in planned {
        if entry.key.catalog_reconstructible() || read_record(reader, &entry.key)?.is_some() {
            continue;
        }
        if let Some(held) = read_held_back(reader, &entry.key)? {
            return Ok(Some(JournalError::HeldBack(held.key, held.reason)));
        }
        if cutover.is_none() {
            cutover = Some(read_cutover(reader)?.is_some());
        }
        if cutover == Some(false) {
            return Ok(Some(JournalError::NoBaseline(entry.key.clone())));
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
/// the cutover, [`JournalError::HeldBack`] for one the cutover held back
/// (nothing is written in either case), or a failed read.
pub fn plan(
    reader: &mut dyn JournalRead,
    attempt: &Attempt,
    planned: &[Planned],
    catalog: CatalogBaseline<'_>,
    now_ms: u64,
) -> Result<Prepared, JournalError> {
    let planned = dedup(planned);
    if let Some(refusal) = missing_baseline(reader, &planned).map_err(JournalError::Storage)? {
        return Err(refusal);
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

    /// Resolves the attempt's definite abort durably ([`resolve_aborted`])
    /// and removes its intents, in one transaction.
    ///
    /// # Errors
    /// The transaction failed; the reconciler resolves the intents.
    pub fn abort(&self, store: &dyn MetaStore) -> Result<(), String> {
        if self.intents.is_empty() {
            return Ok(());
        }
        run(store, &mut |txn| {
            for intent in &self.intents {
                resolve_aborted(txn, intent)?;
            }
            Ok(())
        })
    }

    /// Removes the intents once the attempt committed its records.
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
    /// provided each of its intents is still outstanding exactly as
    /// prepared (op and generation) and each record is still the one the
    /// prepare read, and removes the intents. Answers why nothing committed
    /// otherwise.
    ///
    /// # Errors
    /// The transaction failed (its outcome may be unknown).
    pub fn finalize(&self, store: &dyn MetaStore) -> Result<Result<(), Refusal>, String> {
        let mut refusal = None;
        run(store, &mut |txn| {
            refusal = None;
            let mut resolved = Vec::new();
            for intent in &self.intents {
                if !outstanding(txn, intent)? {
                    resolved.push(intent.key.clone());
                }
            }
            if !resolved.is_empty() {
                refusal = Some(Refusal::Resolved(resolved));
                return Ok(());
            }
            let mut moved = Vec::new();
            for (intent, read) in self.intents.iter().zip(&self.read) {
                let current = read_record(&mut TxnRead(&mut *txn), &intent.key)?;
                let found = current.as_ref().map(|record| (record.generation, record.op_id.clone()));
                if found != *read {
                    moved.push((intent.key.clone(), current));
                }
            }
            if !moved.is_empty() {
                refusal = Some(Refusal::Moved(moved));
                return Ok(());
            }
            for record in self.records(wall_ms()) {
                put_record(txn, &record)?;
            }
            for intent in &self.intents {
                delete_intent(txn, intent)?;
            }
            Ok(())
        })?;
        Ok(refusal.map_or(Ok(()), Err))
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
    seed_cutover_holding_back(store, objects, &BTreeMap::new())
}

/// [`seed_cutover`], which also records each of `held_back` (an object
/// without a record, with why it is not adopted) durably, so every write of
/// it is refused before PD until [`resolve_held_back`] resolves it.
///
/// # Errors
/// The transaction failed.
pub fn seed_cutover_holding_back(
    store: &dyn MetaStore,
    objects: &BTreeMap<ObjectKey, PdValue>,
    held_back: &BTreeMap<ObjectKey, String>,
) -> Result<Option<usize>, String> {
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
        let mut held = 0;
        for (key, reason) in held_back {
            if objects.contains_key(key) || read_record(&mut TxnRead(&mut *txn), key)?.is_some() {
                continue;
            }
            let entry = HeldBack { key: key.clone(), reason: reason.clone(), at_ms };
            txn.set(held_back_key(key), encode(&entry)).map_err(|error| error.to_string())?;
            held += 1;
        }
        let source = format!("operator, {count} objects seeded, {held} held back");
        txn.set(cutover_key(), encode(&Cutover { at_ms, source })).map_err(|error| error.to_string())?;
        seeded = Some(count);
        Ok(())
    })?;
    Ok(seeded)
}

/// The operator's resolution of objects the cutover held back: each of
/// `resolved` that is still held back and has no record gets a record of
/// what PD holds now (a tombstone for `None`, an object PD no longer holds)
/// and is no longer held back, in one transaction. The caller read PD after
/// the operator fixed the objects there. Answers how many it resolved.
///
/// # Errors
/// The transaction failed.
pub fn resolve_held_back(store: &dyn MetaStore, resolved: &BTreeMap<ObjectKey, Option<PdValue>>) -> Result<usize, String> {
    let mut count = 0;
    run(store, &mut |txn| {
        count = 0;
        let at_ms = wall_ms();
        for (key, value) in resolved {
            let mut reader = TxnRead(&mut *txn);
            if read_held_back(&mut reader, key)?.is_none() || read_record(&mut reader, key)?.is_some() {
                continue;
            }
            put_record(
                txn,
                &Record { key: key.clone(), generation: 1, op_id: "operator".to_owned(), value: value.clone(), at_ms },
            )?;
            txn.delete(&held_back_key(key)).map_err(|error| error.to_string())?;
            count += 1;
        }
        Ok(())
    })?;
    Ok(count)
}

/// The cutover a keyspace bootstrapped with the journal records at
/// bootstrap: no PD object of the keyspace exists yet.
#[must_use]
pub fn bootstrap_cutover_mutation(at_ms: u64) -> (Vec<u8>, Vec<u8>) {
    (cutover_key(), encode(&Cutover { at_ms, source: "bootstrap".to_owned() }))
}
