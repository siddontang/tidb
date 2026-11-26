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

//! PD's objects as the journal reads and writes them: through a keyspace's
//! info syncer, whose managers are PD's (or the mocks of a node without PD).

use std::collections::BTreeMap;
use std::sync::Arc;

use prost::Message as _;
use tidb_domain_infosync::info_syncer::InfoSyncer;
use tidb_proto::resource_manager as rmpb;

use crate::{ObjectKey, ObjectKind, PdValue};

/// One keyspace's PD objects.
pub trait PdObjects: Send + Sync {
    /// What PD holds under `keys`, as PD keeps it; a key PD holds nothing
    /// under is missing.
    ///
    /// # Errors
    /// A failed read.
    fn read(&self, keys: &[ObjectKey]) -> Result<BTreeMap<ObjectKey, PdValue>, String>;

    /// Makes PD hold `value` under `key`, or nothing for `None`.
    ///
    /// # Errors
    /// A failed (or unanswered) write, which may still have been applied.
    fn write(&self, key: &ObjectKey, value: Option<&PdValue>) -> Result<(), String>;
}

/// A resource group as a [`PdValue`].
#[must_use]
pub fn resource_group_value(group: &rmpb::ResourceGroup) -> PdValue {
    serde_json::json!({ "pb": hex::encode(group.encode_to_vec()) })
}

/// The resource group a [`PdValue`] holds.
///
/// # Errors
/// The value is not a resource group's.
pub fn resource_group_of(value: &PdValue) -> Result<rmpb::ResourceGroup, String> {
    let bytes = value
        .get("pb")
        .and_then(PdValue::as_str)
        .ok_or_else(|| "not a resource group value".to_owned())
        .and_then(|text| hex::decode(text).map_err(|error| error.to_string()))?;
    rmpb::ResourceGroup::decode(bytes.as_slice()).map_err(|error| error.to_string())
}

/// A placement bundle (keyspace-encoded, as PD keeps it) as its object and
/// [`PdValue`]: `None` for one without rules, which PD does not keep.
#[must_use]
pub fn bundle_entry(bundle: &tidb_placement::Bundle) -> (ObjectKey, Option<PdValue>) {
    let value = serde_json::to_value(bundle).expect("bundles serialize");
    let key = ObjectKey::placement_bundle(value["group_id"].as_str().unwrap_or_default());
    let placed = value["rules"].as_array().is_some_and(|rules| !rules.is_empty());
    (key, placed.then_some(value))
}

/// A label rule as a [`PdValue`].
#[must_use]
pub fn label_rule_value(rule: &tidb_ddl_label::Rule) -> PdValue {
    serde_json::to_value(rule).expect("label rules serialize")
}

/// The bundle of group `id` that `value` holds (an empty one, which
/// removes the group, for `None`).
fn bundle_of(id: &str, value: Option<&PdValue>) -> Result<tidb_placement::Bundle, String> {
    let mut value = value.cloned().unwrap_or_else(|| serde_json::json!({ "rules": [] }));
    value["group_id"] = PdValue::String(id.to_owned());
    serde_json::from_value(value).map_err(|error| error.to_string())
}

/// The PD objects of the keyspace an info syncer serves.
pub struct InfoSyncerPd(pub Arc<InfoSyncer>);

impl PdObjects for InfoSyncerPd {
    fn read(&self, keys: &[ObjectKey]) -> Result<BTreeMap<ObjectKey, PdValue>, String> {
        let mut held = BTreeMap::new();
        let rule_ids = keys
            .iter()
            .filter(|key| key.kind == ObjectKind::LabelRule)
            .map(|key| key.id.clone())
            .collect::<Vec<_>>();
        if !rule_ids.is_empty() {
            for (id, rule) in self.0.label_rule_manager().get_label_rules(&rule_ids)? {
                held.insert(ObjectKey::label_rule(&id), label_rule_value(&rule));
            }
        }
        for key in keys {
            match key.kind {
                ObjectKind::LabelRule => {}
                ObjectKind::PlacementBundle => {
                    let bundle = self.0.placement_manager().get_rule_bundle(&key.id)?;
                    if let (_, Some(value)) = bundle_entry(&bundle) {
                        held.insert(key.clone(), value);
                    }
                }
                ObjectKind::ResourceGroup => {
                    if let Some(group) = self.0.resource_manager_client().lookup_resource_group(&key.id)? {
                        held.insert(key.clone(), resource_group_value(&group));
                    }
                }
            }
        }
        Ok(held)
    }

    fn write(&self, key: &ObjectKey, value: Option<&PdValue>) -> Result<(), String> {
        match key.kind {
            ObjectKind::PlacementBundle => {
                self.0.placement_manager().put_rule_bundles(&[bundle_of(&key.id, value)?])
            }
            ObjectKind::LabelRule => {
                let patch = match value {
                    Some(value) => {
                        let rule = serde_json::from_value(value.clone()).map_err(|error| error.to_string())?;
                        tidb_ddl_label::new_rule_patch(vec![rule], Vec::new())
                    }
                    None => tidb_ddl_label::new_rule_patch(Vec::new(), vec![key.id.clone()]),
                };
                self.0.label_rule_manager().update_label_rules(&patch)
            }
            ObjectKind::ResourceGroup => {
                let client = self.0.resource_manager_client();
                let current = client.lookup_resource_group(&key.id)?;
                match (value, current) {
                    (None, None) => Ok(()),
                    (None, Some(_)) => client.delete_resource_group(&key.id).map(drop),
                    (Some(value), None) => client.add_resource_group(resource_group_of(value)?).map(drop),
                    (Some(value), Some(_)) => client.modify_resource_group(resource_group_of(value)?).map(drop),
                }
            }
        }
    }
}

/// The scope of the keyspace an info syncer serves, which the live attempts
/// and the reconciler of this process agree on.
#[must_use]
pub fn scope(syncer: &InfoSyncer) -> u32 {
    syncer.keyspace_id().unwrap_or(u32::MAX)
}
