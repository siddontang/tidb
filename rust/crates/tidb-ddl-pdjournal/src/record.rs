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

//! The journal's records and the meta keys they live under.

use serde::{Deserialize, Serialize};

/// An object's value as PD keeps it: a placement bundle's or label rule's
/// JSON with its keyspace-qualified ID and keyspace-encoded ranges, or a
/// resource group's protobuf (`{"pb": <hex>}`).
pub type PdValue = serde_json::Value;

/// The kinds of PD object TiDB writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ObjectKind {
    /// A resource-manager group, named per keyspace.
    ResourceGroup,
    /// A placement rule bundle (one PD rule group).
    PlacementBundle,
    /// A region label rule.
    LabelRule,
}

impl ObjectKind {
    fn tag(self) -> &'static str {
        match self {
            Self::ResourceGroup => "rg",
            Self::PlacementBundle => "pb",
            Self::LabelRule => "lr",
        }
    }
}

/// One physical PD object: its kind and the ID PD keeps it under.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ObjectKey {
    /// The object's kind.
    pub kind: ObjectKind,
    /// The ID PD keeps it under: the group name, the keyspace-qualified rule
    /// group ID (`keyspace-<id>-TiDB_DDL_<id>`), or the label rule ID.
    pub id: String,
}

impl ObjectKey {
    /// The resource group `name`.
    #[must_use]
    pub fn resource_group(name: &str) -> Self {
        Self { kind: ObjectKind::ResourceGroup, id: name.to_owned() }
    }

    /// The placement bundle PD keeps under the rule group `group_id`.
    #[must_use]
    pub fn placement_bundle(group_id: &str) -> Self {
        Self { kind: ObjectKind::PlacementBundle, id: group_id.to_owned() }
    }

    /// The label rule `id`.
    #[must_use]
    pub fn label_rule(id: &str) -> Self {
        Self { kind: ObjectKind::LabelRule, id: id.to_owned() }
    }

    /// The table or partition a `TiDB_DDL_<id>` bundle places, `None` for
    /// any other object (a range bundle, a label rule, a resource group).
    #[must_use]
    pub fn bundle_physical_id(&self) -> Option<i64> {
        if self.kind != ObjectKind::PlacementBundle {
            return None;
        }
        let unqualified = match self.id.strip_prefix("keyspace-") {
            Some(rest) => rest.split_once('-').map_or(rest, |(_, id)| id),
            None => self.id.as_str(),
        };
        unqualified.strip_prefix(tidb_placement::BUNDLE_ID_PREFIX)?.parse().ok()
    }

    /// Whether the catalog holds this object's desired value (a resource
    /// group, a table or partition bundle). Label rules and range bundles
    /// exist only in PD.
    #[must_use]
    pub fn catalog_reconstructible(&self) -> bool {
        self.kind == ObjectKind::ResourceGroup || self.bundle_physical_id().is_some()
    }
}

impl std::fmt::Display for ObjectKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match self.kind {
            ObjectKind::ResourceGroup => "resource group",
            ObjectKind::PlacementBundle => "placement bundle",
            ObjectKind::LabelRule => "label rule",
        };
        write!(formatter, "{kind} {}", self.id)
    }
}

/// An object's authoritative journal record.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    /// The object.
    pub key: ObjectKey,
    /// The committed generation: every committed write, and every resolved
    /// abort, moves it forward.
    pub generation: u64,
    /// The op that committed this generation.
    pub op_id: String,
    /// The desired value, `None` for a deleted object (a tombstone).
    pub value: Option<PdValue>,
    /// When the generation committed (PD TSO physical milliseconds).
    pub at_ms: u64,
}

/// An attempt's prepared write of one object.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Intent {
    /// The object.
    pub key: ObjectKey,
    /// The attempt's op ID, stable across its PD retries.
    pub op_id: String,
    /// Who prepared it: the DDL owner ID, or the node's ID for a non-job
    /// writer.
    pub owner: String,
    /// The DDL job, 0 for a non-job writer (ALTER RANGE, GC).
    pub job_id: i64,
    /// The generation the attempt commits.
    pub generation: u64,
    /// The value it writes, `None` for a delete.
    pub value: Option<PdValue>,
    /// The last authoritative value (or the catalog's, or "absent" after the
    /// cutover), which an abort puts back.
    pub pre_image: Option<PdValue>,
    /// When it was prepared (PD TSO physical milliseconds).
    pub at_ms: u64,
}

/// The keyspace's journal cutover: from here on, a PD-only object without a
/// record is absent.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Cutover {
    /// When it committed (PD TSO physical milliseconds).
    pub at_ms: u64,
    /// `bootstrap`, or the operator's seeding with how many objects it seeded.
    pub source: String,
}

/// A PD-only object the operator's cutover held back: PD held it under an ID
/// of TiDB's shape, but its labels or range are not TiDB's for that table
/// or partition, so it is neither TiDB's baseline nor known to be absent.
/// Every write of it is refused until the operator resolves it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HeldBack {
    /// The object.
    pub key: ObjectKey,
    /// Why the cutover did not adopt it.
    pub reason: String,
    /// When the cutover held it back (wall-clock milliseconds).
    pub at_ms: u64,
}

/// The string meta keys' first 8-byte group: every name under one of them
/// shares the encoded group, so a raw prefix scan finds them all.
const RECORDS: &[u8; 8] = b"PDJrnl/A";
const INTENTS: &[u8; 8] = b"PDJrnl/I";
const CUTOVER: &[u8; 8] = b"PDJrnl/C";
const HELD_BACK: &[u8; 8] = b"PDJrnl/H";

fn meta_key(name: &[u8]) -> Vec<u8> {
    tidb_structure::encode_string_data_key(tidb_tablecodec::table_key::META_PREFIX, name)
}

/// The raw prefix every name under `head` (and longer than it) encodes to:
/// the meta prefix and the encoded first group.
fn scan_prefix(head: &[u8; 8]) -> Vec<u8> {
    let mut name = head.to_vec();
    name.push(b'/');
    let mut key = meta_key(&name);
    key.truncate(tidb_tablecodec::table_key::META_PREFIX.len() + 9);
    key
}

fn object_name(head: &[u8; 8], key: &ObjectKey, suffix: &str) -> Vec<u8> {
    let mut name = head.to_vec();
    name.extend_from_slice(format!("/{}/{}{suffix}", key.kind.tag(), key.id).as_bytes());
    name
}

pub(crate) fn record_key(key: &ObjectKey) -> Vec<u8> {
    meta_key(&object_name(RECORDS, key, ""))
}

pub(crate) fn intent_key(key: &ObjectKey, op_id: &str) -> Vec<u8> {
    meta_key(&object_name(INTENTS, key, &format!("\0{op_id}")))
}

pub(crate) fn held_back_key(key: &ObjectKey) -> Vec<u8> {
    meta_key(&object_name(HELD_BACK, key, ""))
}

pub(crate) fn held_back_prefix() -> Vec<u8> {
    scan_prefix(HELD_BACK)
}

pub(crate) fn cutover_key() -> Vec<u8> {
    meta_key(CUTOVER)
}

pub(crate) fn records_prefix() -> Vec<u8> {
    scan_prefix(RECORDS)
}

pub(crate) fn intents_prefix() -> Vec<u8> {
    scan_prefix(INTENTS)
}

/// The wall clock in milliseconds since the epoch, which PD's TSO physical
/// part (a step's records' times) also counts.
pub(crate) fn wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| u64::try_from(since.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_key_of_a_kind_starts_with_its_scan_prefix() {
        let keys = [
            ObjectKey::resource_group("rg1"),
            ObjectKey::placement_bundle("keyspace-1-TiDB_DDL_100"),
            ObjectKey::label_rule("keyspace/1/schema/db/t"),
            ObjectKey::label_rule("a"),
        ];
        for key in &keys {
            assert!(record_key(key).starts_with(&records_prefix()), "{key}");
            assert!(intent_key(key, "op").starts_with(&intents_prefix()), "{key}");
            assert!(!record_key(key).starts_with(&intents_prefix()));
            assert!(held_back_key(key).starts_with(&held_back_prefix()), "{key}");
            assert!(!held_back_key(key).starts_with(&records_prefix()));
        }
        assert!(!cutover_key().starts_with(&records_prefix()));
    }

    #[test]
    fn only_table_and_partition_bundles_and_groups_are_in_the_catalog() {
        assert_eq!(ObjectKey::placement_bundle("keyspace-7-TiDB_DDL_100").bundle_physical_id(), Some(100));
        assert_eq!(ObjectKey::placement_bundle("TiDB_DDL_5").bundle_physical_id(), Some(5));
        assert!(!ObjectKey::placement_bundle("keyspace-7-TiDB_GLOBAL").catalog_reconstructible());
        assert!(!ObjectKey::label_rule("TiDB_DDL_5").catalog_reconstructible());
        assert!(ObjectKey::resource_group("rg").catalog_reconstructible());
    }
}
