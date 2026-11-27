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

//! The catalog's value of a resource group or a table or partition bundle
//! that has no PD journal record yet, as of a step's snapshot: the baseline
//! an abort of the step's first journaled write puts back.

use std::collections::BTreeMap;

use tidb_ddl_pdjournal::{ObjectKey, ObjectKind, PdValue};
use tidb_infoschema::cluster_catalog::MetaSnapshot;
use tidb_meta::{key, value};

/// A step's snapshot read as the PD journal.
pub(crate) struct SnapshotRead<'a, S: MetaSnapshot>(pub(crate) &'a mut S);

impl<S: MetaSnapshot> tidb_ddl_pdjournal::JournalRead for SnapshotRead<'_, S> {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        self.0.get(key).map_err(|error| error.to_string())
    }

    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<tidb_ddl_pdjournal::journal::Pairs, String> {
        self.0.scan_prefix(prefix).map_err(|error| error.to_string())
    }
}

/// The stored policies, which bundles resolve by ID.
struct StoredPolicies(Vec<tidb_model::PolicyInfo>);

impl tidb_placement::PolicyGetter for StoredPolicies {
    fn get_policy(&self, policy_id: i64) -> Result<tidb_model::PolicyInfo, tidb_placement::PlacementError> {
        self.0.iter().find(|policy| policy.id == policy_id).cloned().ok_or_else(|| {
            tidb_placement::PlacementError::wrap(
                tidb_placement::PlacementErrorKind::InvalidBundleId,
                format!("no placement policy with id {policy_id}"),
            )
        })
    }
}

fn scan<S: MetaSnapshot>(snapshot: &mut S, prefix: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    Ok(snapshot
        .scan_prefix(prefix)
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|(_, value)| value)
        .collect())
}

/// The groups among `names` the catalog holds (its schema image as of the
/// snapshot), as the DDL builds them for PD.
fn resource_groups<S: MetaSnapshot>(
    snapshot: &mut S,
    names: &[&ObjectKey],
) -> Result<BTreeMap<ObjectKey, PdValue>, String> {
    let schema = tidb_infoschema::cluster_catalog::load_executor_info_schema(snapshot).map_err(|error| error.to_string())?;
    let mut found = BTreeMap::new();
    for key in names {
        if let Some(group) = schema.resource_group(&key.id) {
            let proto = tidb_ddl_resourcegroup::new_group_from_options(&key.id, group.settings.as_deref())
                .map_err(|error| error.to_string())?;
            found.insert((*key).clone(), tidb_ddl_pdjournal::pd::resource_group_value(&proto));
        }
    }
    Ok(found)
}

/// The bundles among `groups` the catalog's tables and partitions place
/// (Go `NewFullTableBundles`), keyed and ranged as PD keeps them.
fn bundles<S: MetaSnapshot>(
    snapshot: &mut S,
    groups: &[&ObjectKey],
    codec: Option<&tikv_client::request::ApiV2Codec>,
) -> Result<BTreeMap<ObjectKey, PdValue>, String> {
    let policies = scan(snapshot, &key::policies_kv_prefix())?
        .iter()
        .map(|stored| value::parse_policy_info(stored).map_err(|error| error.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    let policies = StoredPolicies(policies);
    let mut found = BTreeMap::new();
    for stored in scan(snapshot, &key::databases_kv_prefix())? {
        let database = value::parse_db_info(&stored).map_err(|error| error.to_string())?;
        for table in tidb_infoschema::cluster_catalog::list_tables(snapshot, database.id).map_err(|error| error.to_string())? {
            for bundle in tidb_placement::new_full_table_bundles(&policies, &table).map_err(|error| error.to_string())? {
                let encoded = tidb_domain_infosync::placement_manager::encode_bundle(codec, &bundle)?;
                if let (key, Some(value)) = tidb_ddl_pdjournal::pd::bundle_entry(&encoded) {
                    if groups.contains(&&key) {
                        found.insert(key, value);
                    }
                }
            }
        }
    }
    Ok(found)
}

/// The catalog's values of `keys` (catalog-reconstructible objects), as of
/// `snapshot`; one the catalog does not hold is missing. Scans the catalog's
/// tables once for bundles, which happens only for objects first written
/// after the journal exists.
pub(crate) fn catalog_values<S: MetaSnapshot>(
    snapshot: &mut S,
    keys: &[ObjectKey],
    codec: Option<&tikv_client::request::ApiV2Codec>,
) -> Result<BTreeMap<ObjectKey, PdValue>, String> {
    let groups = keys.iter().filter(|key| key.kind == ObjectKind::ResourceGroup).collect::<Vec<_>>();
    let placed = keys.iter().filter(|key| key.bundle_physical_id().is_some()).collect::<Vec<_>>();
    let mut found = BTreeMap::new();
    if !groups.is_empty() {
        found.extend(resource_groups(snapshot, &groups)?);
    }
    if !placed.is_empty() {
        found.extend(bundles(snapshot, &placed, codec)?);
    }
    Ok(found)
}

/// The tables of the database `db` (lower-case) as of `snapshot`, with
/// their lower-case names and their partitions', which the PD journal
/// cutover checks a label rule's ranges against
/// ([`tidb_ddl_pdjournal::owner::CatalogTables`]); `None` when the catalog
/// holds no such database.
///
/// # Errors
/// The catalog could not be read.
pub(crate) fn catalog_tables<S: MetaSnapshot>(
    snapshot: &mut S,
    db: &str,
) -> Result<Option<Vec<tidb_ddl_pdjournal::owner::CatalogTable>>, String> {
    for stored in scan(snapshot, &key::databases_kv_prefix())? {
        let database = value::parse_db_info(&stored).map_err(|error| error.to_string())?;
        if database.name.lowercase() != db {
            continue;
        }
        let tables = tidb_infoschema::cluster_catalog::list_tables(snapshot, database.id).map_err(|error| error.to_string())?;
        return Ok(Some(
            tables
                .iter()
                .map(|info| {
                    let names = tidb_ddl_build::label_rules::LabelRuleTable::from_table_info(info);
                    tidb_ddl_pdjournal::owner::CatalogTable { name: names.name, id: names.id, partitions: names.partitions }
                })
                .collect(),
        ));
    }
    Ok(None)
}

/// What a fresh keyspace's bootstrap commits with its catalog: the record of
/// the default resource group as the bootstrap leaves it (`default_group`),
/// so a crash before the resource manager hears of it is repaired by the
/// reconciler, and the keyspace's cutover, since none of its PD objects
/// exists yet. `snapshot` is the bootstrap transaction's.
///
/// # Errors
/// The journal could not be read, or a mutation not built.
pub fn bootstrap_journal<S: MetaSnapshot>(
    snapshot: &mut S,
    default_group: &tidb_proto::resource_manager::ResourceGroup,
) -> Result<Vec<tidb_txnkv::transaction::BufferMutation>, String> {
    let at_ms = snapshot.start_ts() >> 18;
    let mut reader = SnapshotRead(snapshot);
    let key = ObjectKey::resource_group(&default_group.name);
    let previous = tidb_ddl_pdjournal::journal::read_record(&mut reader, &key)?;
    let record = tidb_ddl_pdjournal::Record {
        generation: previous.map_or(1, |record| record.generation + 1),
        op_id: "bootstrap".to_owned(),
        value: Some(tidb_ddl_pdjournal::pd::resource_group_value(default_group)),
        at_ms,
        key,
    };
    let mut mutations = vec![tidb_ddl_pdjournal::journal::record_mutation(&record)];
    if tidb_ddl_pdjournal::journal::read_cutover(&mut reader)?.is_none() {
        mutations.push(tidb_ddl_pdjournal::journal::bootstrap_cutover_mutation(at_ms));
    }
    mutations
        .into_iter()
        .map(|(key, value)| tidb_txnkv::transaction::BufferMutation::set(key, value).map_err(|error| error.to_string()))
        .collect()
}
