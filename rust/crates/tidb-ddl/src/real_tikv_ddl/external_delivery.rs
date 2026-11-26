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

//! The PD writes a DDL job step makes before its catalog transaction
//! commits -- placement bundles, label rules and the resource-group write --
//! journaled, and what undoes them when the step is definitely aborted.
//!
//! Go's job step writes PD call by call and never undoes a write. Here every
//! physical object a step writes goes through the keyspace's PD journal
//! ([`tidb_ddl_pdjournal`]): before the first PD write the step commits one
//! intent per object (its op, the value, the generation it commits and the
//! pre-image: the object's authoritative record, or the catalog's value as of
//! the step's snapshot for a resource group or table bundle without one), and
//! the step's catalog transaction writes the records, so they commit with the
//! catalog change or not at all. A PD-only object (a label rule) without a
//! record before the keyspace's cutover refuses the step before PD is
//! touched.
//!
//! A definite abort puts every object's pre-image back -- a failed or
//! unanswered PD write may still have been applied -- and removes the
//! intents. An undetermined commit leaves them for the reconciler, which
//! finds whether the records committed. Only the DDL owner undoes: an attempt
//! that lost its ownership leaves PD and its intents as they are, and the
//! current owner's reconciler resolves them from the records (multi-keyspace
//! design §6.2).

use std::sync::Arc;
use std::time::Duration;

use tidb_ddl_pdjournal::journal::{Attempt, CatalogBaseline, Prepared};
use tidb_ddl_pdjournal::pd::PdObjects;
use tidb_ddl_pdjournal::{JournalRead, ObjectKey, Planned};
use tidb_proto::resource_manager as rmpb;
use tikv_client::pd_http;

use tidb_ddl_build::label_rules::LabelRuleWrite;

/// Where a step's placement bundles go: PD over the store's PD HTTP client,
/// keyed and ranged by the store's keyspace, or the info syncer's placement
/// manager on a node without PD.
pub(crate) enum PlacementTarget {
    /// PD's placement rule bundles.
    Pd {
        pd_http_client: pd_http::Client,
        codec: Option<tikv_client::request::ApiV2Codec>,
    },
    /// The info syncer's placement manager (Go's `mockPlacementManager`).
    InfoSyncer,
}

/// The PD writes a step's catalog change carries.
pub(crate) struct StepObjects<'a> {
    /// The codec of the step's keyspace, which keys and ranges its bundles.
    pub(crate) codec: Option<&'a tikv_client::request::ApiV2Codec>,
    /// Its placement bundles (one without rules deletes its group).
    pub(crate) bundles: &'a [tidb_placement::Bundle],
    /// Its label-rule calls, with Go's context of a failed one.
    pub(crate) labels: &'a [(&'a LabelRuleWrite, Option<&'a str>)],
    /// Its resource group's name and value (`None` for a drop).
    pub(crate) resource_group: Option<(&'a str, Option<&'a rmpb::ResourceGroup>)>,
}

impl StepObjects<'_> {
    /// The objects, as PD keeps them: each bundle, the final state of each
    /// rule the label calls touch (a patch deletes before it sets), and the
    /// resource group.
    ///
    /// # Errors
    /// A bundle that cannot be encoded for the keyspace.
    fn planned(&self) -> Result<Vec<Planned>, String> {
        use tidb_ddl_pdjournal::pd::{bundle_entry, label_rule_value, resource_group_value};
        let mut planned = Vec::new();
        for bundle in self.bundles {
            let (key, value) = bundle_entry(&tidb_domain_infosync::placement_manager::encode_bundle(self.codec, bundle)?);
            planned.push(Planned::new(key, value));
        }
        for (write, _) in self.labels {
            match write {
                LabelRuleWrite::Put(rule) => planned.push(Planned::new(ObjectKey::label_rule(&rule.id), Some(label_rule_value(rule)))),
                LabelRuleWrite::Patch(patch) => {
                    for id in patch.delete_rules.snapshot() {
                        planned.push(Planned::new(ObjectKey::label_rule(&id), None));
                    }
                    for rule in patch.set_rules.snapshot() {
                        planned.push(Planned::new(ObjectKey::label_rule(&rule.id), Some(label_rule_value(&rule))));
                    }
                }
            }
        }
        if let Some((name, group)) = self.resource_group {
            planned.push(Planned::new(ObjectKey::resource_group(name), group.map(resource_group_value)));
        }
        Ok(planned)
    }
}

/// Why the step may not write its objects: a label rule or range bundle
/// without a journal baseline before the keyspace's cutover, as of the
/// step's `snapshot`. `None` otherwise (any other failure surfaces at the
/// prepare).
pub(crate) fn refusal<S: tidb_infoschema::cluster_catalog::MetaSnapshot>(
    mut snapshot: S,
    objects: &StepObjects<'_>,
) -> Option<String> {
    let planned = objects.planned().ok()?;
    let missing = tidb_ddl_pdjournal::journal::missing_baseline(&mut super::pd_baseline::SnapshotRead(&mut snapshot), &planned);
    missing.ok().flatten().map(|key| tidb_ddl_pdjournal::JournalError::NoBaseline(key).to_string())
}

/// Where a step's journal lives and who writes it.
pub(crate) struct StepJournal {
    /// The keyspace's meta store, for the intents.
    pub(crate) store: Arc<dyn tidb_meta::MetaStore>,
    /// The step's attempt: its op ID is the owner, the job and the step's
    /// start timestamp.
    pub(crate) attempt: Attempt,
}

/// The PD writes one job step delivered, and their journal.
#[derive(Default)]
pub(crate) struct ExternalDelivery {
    journal: Option<(Arc<dyn tidb_meta::MetaStore>, Prepared)>,
}

fn failed(context: &str, error: impl std::fmt::Display) -> String {
    format!("{context}: {error}")
}

impl ExternalDelivery {
    /// Prepares `planned` against the records `reader` (the step's
    /// snapshot) holds and commits the intents, before any PD write. Answers
    /// the record mutations the step's catalog transaction commits.
    ///
    /// # Errors
    /// A PD-only object without a baseline, or the journal could not be
    /// read or written; PD is untouched.
    pub(crate) fn prepare(
        &mut self,
        journal: StepJournal,
        reader: &mut dyn JournalRead,
        planned: &[Planned],
        catalog: CatalogBaseline<'_>,
        now_ms: u64,
    ) -> Result<tidb_ddl_pdjournal::journal::Pairs, String> {
        if planned.is_empty() {
            return Ok(Vec::new());
        }
        let prepared = tidb_ddl_pdjournal::journal::plan(reader, &journal.attempt, planned, catalog, now_ms)
            .map_err(|error| error.to_string())?;
        prepared
            .commit_intents(journal.store.as_ref())
            .map_err(|error| failed("failed to prepare the PD journal", error))?;
        let records = prepared
            .records(now_ms)
            .iter()
            .map(tidb_ddl_pdjournal::journal::record_mutation)
            .collect();
        self.journal = Some((journal.store, prepared));
        Ok(records)
    }

    /// [`Self::prepare`] for a step's `objects`, against the step's own
    /// snapshots (one for the journal, one for the catalog baselines) at
    /// `start_ts`; answers the record mutations the step commits.
    ///
    /// # Errors
    /// As [`Self::prepare`].
    pub(crate) fn prepare_step<S: tidb_infoschema::cluster_catalog::MetaSnapshot>(
        &mut self,
        journal: StepJournal,
        (mut snapshot, mut catalog): (S, S),
        objects: &StepObjects<'_>,
        start_ts: u64,
    ) -> Result<Vec<tidb_txnkv::transaction::BufferMutation>, String> {
        let planned = objects.planned()?;
        let records = self.prepare(
            journal,
            &mut super::pd_baseline::SnapshotRead(&mut snapshot),
            &planned,
            &mut |keys| super::pd_baseline::catalog_values(&mut catalog, keys, objects.codec),
            start_ts >> 18,
        )?;
        records
            .into_iter()
            .map(|(key, value)| tidb_txnkv::transaction::BufferMutation::set(key, value).map_err(|error| error.to_string()))
            .collect()
    }

    /// Delivers the step's placement bundles.
    pub(crate) fn deliver_placement(
        &self,
        target: PlacementTarget,
        bundles: &[tidb_placement::Bundle],
        timeout: Duration,
    ) -> Result<(), String> {
        if bundles.is_empty() {
            return Ok(());
        }
        const CONTEXT: &str = "failed to notify PD the placement rules";
        match target {
            PlacementTarget::Pd { pd_http_client, codec } => {
                crate::placement_delivery::put_rule_bundles(&pd_http_client, codec.as_ref(), bundles, timeout)
                    .map_err(|error| failed(CONTEXT, error))
            }
            PlacementTarget::InfoSyncer => tidb_domain_infosync::info_syncer::put_rule_bundles_with_default_retry(bundles)
                .map_err(|error| failed(CONTEXT, error)),
        }
    }

    /// Go's job step makes its label-rule calls before the version is
    /// published and fails the job when one fails. `steps` are the step's
    /// calls with Go's `errors.Wrapf` context of a failed one.
    pub(crate) fn deliver_label_rules(&self, steps: &[(&LabelRuleWrite, Option<&str>)]) -> Result<(), String> {
        let manager = tidb_domain_infosync::info_syncer::label_rule_manager();
        for (write, context) in steps {
            if let Err(error) = write.apply(manager.as_ref()) {
                return Err(match context {
                    Some(context) => failed(context, error),
                    None => error,
                });
            }
        }
        Ok(())
    }

    /// The step committed: its intents are done.
    pub(crate) fn committed(&self) {
        if let Some((store, prepared)) = &self.journal {
            if let Err(error) = prepared.clear_intents(store.as_ref()) {
                tidb_ddl_logutil::ddl_logger().warn(
                    "a committed step's PD journal intents stay for the reconciler",
                    &[tidb_log::Field::new("error", tidb_log::Value::Str(error))],
                );
            }
        }
    }

    /// Undoes the step's PD writes after a definite abort, if this attempt
    /// still owns the DDL (`still_owner`): every object gets its pre-image
    /// back from `pd` and the intents go. Otherwise leaves PD and the
    /// intents to the owner's reconciler. Answers what the abort's cause
    /// should add: the undo's failures, or the undo skipped.
    pub(crate) fn compensate(&self, still_owner: &dyn Fn() -> Result<(), String>, pd: &dyn PdObjects) -> Result<(), String> {
        let Some((store, prepared)) = &self.journal else {
            return Ok(());
        };
        if let Err(error) = still_owner() {
            tidb_ddl_logutil::ddl_logger().warn(
                "a DDL attempt that lost its ownership leaves its PD writes to the owner",
                &[tidb_log::Field::new("error", tidb_log::Value::Str(error.clone()))],
            );
            return Err(format!(
                "this node is no longer the DDL owner, so it left the attempt's PD writes: {error}"
            ));
        }
        prepared
            .restore_pre_images(pd)
            .map_err(|error| format!("restoring the attempt's PD objects failed, the reconciler retries: {error}"))?;
        prepared
            .clear_intents(store.as_ref())
            .map_err(|error| format!("removing the attempt's PD journal intents failed: {error}"))
    }
}
