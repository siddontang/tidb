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

//! The durable journal of the PD objects a keyspace's TiDB writes, and the
//! reconciler that makes PD converge to it (multi-keyspace design §6.2).
//!
//! PD's resource-group, placement-bundle and label-rule APIs have no
//! conditional write. A DDL owner that was paused after its last lease check
//! can land a write long after a successor committed the same object, and
//! nothing in Go repairs it short of the next DDL on that object. Here every
//! physical PD object a keyspace writes has a journal record in the
//! keyspace's own meta, under fixed string keys (`PDJrnl/...`, not a meta
//! hash):
//!
//! - the *authoritative record* ([`Record`]): the object's desired value as
//!   PD keeps it (a tombstone for a deleted object) and its generation;
//! - *prepared intents* ([`Intent`]): one per attempt that is about to write
//!   the object, with the attempt's stable op ID, the value it writes, the
//!   generation it would commit and its pre-image (the last authoritative
//!   value, never a fresh PD read).
//!
//! A write goes (1) intent committed, (2) PD written, (3) the authoritative
//! record written: by a DDL job step in its own catalog transaction
//! (`DdlWrite.mutations`), so the record commits atomically with the catalog
//! change, and by the non-job writers (ALTER RANGE, GC, bootstrap) in a small
//! meta transaction that compares the generation it prepared against
//! ([`direct`]). Racing attempts write the same record key, so the
//! transaction layer lets one of them commit. An abort puts the pre-image
//! back into PD and removes its intents only in the transaction that moves
//! the record above the attempt's generation, holding the baseline (a
//! tombstone for an object that was absent), so a write of the attempt that
//! lands later is a divergence the reconciler reverts; a lost PD reply
//! counts as applied. Records are never deleted: a tombstone stays until a
//! newer record of its object replaces it.
//!
//! The current DDL owner reconciles ([`reconcile`]) at takeover and every
//! [`reconcile::SCAN_INTERVAL`]: it resolves the intents no live attempt
//! holds from the durable records (an intent whose op committed the record
//! is done; any other is aborted, and the record's generation is burned past
//! it so the attempt can no longer commit), then compares each record with
//! PD over the fields TiDB sets ([`normalize`]) and rewrites and reads back
//! the objects that differ. It never acts on an object without a record.
//!
//! Baselines: an object first written after the journal exists needs its
//! previous value. Resource groups and table or partition bundles have it in
//! the catalog, which the caller reads from the step's own snapshot. Label
//! rules and range bundles exist only in PD, and a PD read can return an old
//! owner's delayed write, so they need a seeded baseline before their first
//! mutation: a keyspace bootstrapped with the journal records its
//! [`Cutover`] at bootstrap, and an older keyspace needs the operator's
//! cutover ([`journal::seed_cutover`]), run once its old writers are
//! drained. Until then such a mutation fails before it reaches PD
//! ([`JournalError::NoBaseline`]). An object the cutover could not tell to
//! be TiDB's is held back ([`HeldBack`]): its writes fail before PD
//! ([`JournalError::HeldBack`]) until the operator resolves it.

mod live;
mod record;

pub mod direct;
pub mod journal;
pub mod normalize;
pub mod owner;
pub mod pd;
pub mod reconcile;

pub use journal::{JournalError, JournalRead, Planned, Prepared, TxnRead};
pub use live::Writing;
pub use record::{Cutover, HeldBack, Intent, ObjectKey, ObjectKind, PdValue, Record};
