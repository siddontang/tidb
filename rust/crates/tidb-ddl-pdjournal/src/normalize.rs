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

//! Whether PD holds an object as its record wants it.
//!
//! PD answers with more than TiDB wrote: fields of its own (a rule's
//! `create_timestamp`, a label's `start_at`, a group's token state and
//! consumption) and defaults for fields TiDB left out. The comparison is over
//! the projection on the fields TiDB sets: both sides are read into TiDB's
//! own type (a bundle, a label rule, a resource group's settings), which
//! keeps exactly those fields, so a field TiDB set before and now leaves
//! empty compares unequal against PD still holding it, while a field only PD
//! knows never does. The projection keeps every object's keyspace: the
//! keyspace-qualified IDs, both bounds of every range (keyspace-encoded) and
//! a resource group's keyspace, so another tenant's range or group never
//! compares equal.

use tidb_proto::resource_manager as rmpb;

use crate::pd::resource_group_of;
use crate::{ObjectKey, ObjectKind, PdValue};

/// Lowercases every key-range bound in `value` (PD may answer hex in either
/// case) and sorts a bundle's rules by ID (PD keeps them by index).
fn canonical(value: &mut PdValue) {
    match value {
        PdValue::Object(fields) => {
            for (name, field) in fields.iter_mut() {
                if matches!(name.as_str(), "start_key" | "end_key") {
                    if let PdValue::String(text) = field {
                        *text = text.to_ascii_lowercase();
                    }
                } else {
                    canonical(field);
                }
            }
        }
        PdValue::Array(items) => items.iter_mut().for_each(canonical),
        _ => {}
    }
}

fn bundle_projection(key: &ObjectKey, value: &PdValue) -> Result<PdValue, String> {
    let bundle: tidb_placement::Bundle =
        serde_json::from_value(value.clone()).map_err(|error| format!("{key}: {error}"))?;
    let mut projected = serde_json::to_value(&bundle).map_err(|error| error.to_string())?;
    projected["group_id"] = PdValue::String(key.id.clone());
    if let Some(PdValue::Array(rules)) = projected.get_mut("rules") {
        rules.sort_by(|left, right| left.get("id").map(PdValue::to_string).cmp(&right.get("id").map(PdValue::to_string)));
    }
    canonical(&mut projected);
    Ok(projected)
}

fn label_rule_projection(key: &ObjectKey, value: &PdValue) -> Result<PdValue, String> {
    let rule: tidb_ddl_label::Rule =
        serde_json::from_value(value.clone()).map_err(|error| format!("{key}: {error}"))?;
    let mut projected = serde_json::to_value(&rule).map_err(|error| error.to_string())?;
    canonical(&mut projected);
    Ok(projected)
}

/// A token bucket's settings, without PD's token state; a missing bucket
/// is the default one PD may fill in.
fn bucket_settings(bucket: Option<&rmpb::TokenBucket>) -> (u64, i64) {
    bucket
        .and_then(|bucket| bucket.settings.as_ref())
        .map_or((0, 0), |settings| (settings.fill_rate, settings.burst_limit))
}

/// The settings TiDB writes into a resource group (`NewGroupFromOptions`),
/// a missing message read as its default (which PD may fill in), and the
/// group's keyspace: `keyspace_id` stands for the keyspace whose client
/// wrote it when PD leaves it out.
fn resource_group_projection(value: &PdValue, keyspace_id: Option<u32>) -> Result<PdValue, String> {
    let group = resource_group_of(value)?;
    let raw = group.raw_resource_settings.unwrap_or_default();
    Ok(serde_json::json!({
        "name": group.name,
        "mode": group.mode,
        "ru": bucket_settings(group.r_u_settings.as_ref().and_then(|settings| settings.r_u.as_ref())),
        "raw": [
            bucket_settings(raw.cpu.as_ref()),
            bucket_settings(raw.io_read.as_ref()),
            bucket_settings(raw.io_write.as_ref()),
        ],
        "priority": group.priority,
        "runaway": format!("{:?}", group.runaway_settings.clone().unwrap_or_default()),
        "background": format!("{:?}", group.background_settings.clone().unwrap_or_default()),
        "keyspace": match group.keyspace_id.and_then(|id| id.keyspace) {
            Some(rmpb::keyspace_id_value::Keyspace::Value(id)) => Some(id.to_string()),
            Some(identity) => Some(format!("{identity:?}")),
            None => keyspace_id.map(|id| id.to_string()),
        },
    }))
}

/// The projection of `value` on the fields TiDB sets, for comparing what PD
/// holds with what the record wants. `keyspace_id` is the keyspace whose
/// client reads and writes the object.
///
/// # Errors
/// The value is not one of `key`'s kind.
pub fn projection(key: &ObjectKey, value: &PdValue, keyspace_id: Option<u32>) -> Result<PdValue, String> {
    match key.kind {
        ObjectKind::PlacementBundle => bundle_projection(key, value),
        ObjectKind::LabelRule => label_rule_projection(key, value),
        ObjectKind::ResourceGroup => resource_group_projection(value, keyspace_id),
    }
}

/// Whether PD's `held` (`None`: nothing) is the record's `wanted`.
///
/// # Errors
/// As [`projection`].
pub fn in_sync(
    key: &ObjectKey,
    wanted: Option<&PdValue>,
    held: Option<&PdValue>,
    keyspace_id: Option<u32>,
) -> Result<bool, String> {
    match (wanted, held) {
        (None, None) => Ok(true),
        (Some(wanted), Some(held)) => Ok(projection(key, wanted, keyspace_id)? == projection(key, held, keyspace_id)?),
        _ => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::resource_group_value;

    fn bundle_json(group: &str, start: &str, end: &str) -> PdValue {
        serde_json::json!({
            "group_id": group,
            "group_index": 40,
            "group_override": true,
            "rules": [{
                "group_id": group,
                "id": "table_rule_100_0",
                "index": 40,
                "start_key": start,
                "end_key": end,
                "role": "voter",
                "count": 3,
                "label_constraints": [{"key": "region", "op": "in", "values": ["us"]}],
            }],
        })
    }

    /// PD's own fields and its defaults compare equal; its hex case does not
    /// matter.
    #[test]
    fn pd_additions_and_defaults_compare_equal() {
        let key = ObjectKey::placement_bundle("keyspace-1-TiDB_DDL_100");
        let wanted = bundle_json(&key.id, "78000001748000000000000064", "78000001748000000000000065");
        let mut held = bundle_json(&key.id, "78000001748000000000000064", "78000001748000000000000065");
        held["rules"][0]["create_timestamp"] = serde_json::json!(1_700_000_000);
        held["rules"][0]["is_witness"] = serde_json::json!(false);
        held["rules"][0]["start_key"] = serde_json::json!("78000001748000000000000064".to_uppercase());
        assert!(in_sync(&key, Some(&wanted), Some(&held), Some(1)).unwrap());
    }

    /// Another keyspace's range (either bound) and a field TiDB set but now
    /// clears never compare equal.
    #[test]
    fn another_tenants_range_or_a_cleared_field_compares_unequal() {
        let key = ObjectKey::placement_bundle("keyspace-1-TiDB_DDL_100");
        let wanted = bundle_json(&key.id, "78000001748000000000000064", "78000001748000000000000065");
        for held in [
            bundle_json(&key.id, "78000002748000000000000064", "78000001748000000000000065"),
            bundle_json(&key.id, "78000001748000000000000064", "78000002748000000000000065"),
        ] {
            assert!(!in_sync(&key, Some(&wanted), Some(&held), Some(1)).unwrap());
        }
        let mut cleared = wanted.clone();
        cleared["rules"][0]
            .as_object_mut()
            .unwrap()
            .remove("label_constraints");
        assert!(!in_sync(&key, Some(&cleared), Some(&wanted), Some(1)).unwrap());
        assert!(!in_sync(&key, None, Some(&wanted), Some(1)).unwrap());
    }

    fn label_rule(start: &str) -> PdValue {
        serde_json::json!({
            "id": "keyspace/1/schema/db/t",
            "index": 0,
            "labels": [{"key": "merge_option", "value": "allow"}],
            "rule_type": "key-range",
            "data": [{"start_key": start, "end_key": "7800000174800000000000006500"}],
        })
    }

    #[test]
    fn a_label_rule_compares_its_labels_and_both_bounds() {
        let key = ObjectKey::label_rule("keyspace/1/schema/db/t");
        let wanted = label_rule("7800000174800000000000006400");
        let mut held = wanted.clone();
        held["labels"][0]["start_at"] = serde_json::json!("");
        held["labels"][0]["ttl"] = serde_json::json!("");
        assert!(in_sync(&key, Some(&wanted), Some(&held), Some(1)).unwrap());
        assert!(!in_sync(&key, Some(&wanted), Some(&label_rule("7800000274800000000000006400")), Some(1)).unwrap());
        let mut relabeled = wanted.clone();
        relabeled["labels"][0]["value"] = serde_json::json!("deny");
        assert!(!in_sync(&key, Some(&wanted), Some(&relabeled), Some(1)).unwrap());
    }

    fn group(priority: u32, tokens: f64, keyspace: Option<u32>) -> rmpb::ResourceGroup {
        rmpb::ResourceGroup {
            name: "rg1".to_owned(),
            mode: rmpb::GroupMode::RuMode as i32,
            priority,
            r_u_settings: Some(rmpb::GroupRequestUnitSettings {
                r_u: Some(rmpb::TokenBucket {
                    settings: Some(rmpb::TokenLimitSettings { fill_rate: 100, burst_limit: 200, ..Default::default() }),
                    tokens,
                }),
            }),
            keyspace_id: keyspace.map(|id| rmpb::KeyspaceIdValue {
                keyspace: Some(rmpb::keyspace_id_value::Keyspace::Value(id)),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn a_resource_group_ignores_token_state_but_not_its_keyspace() {
        let key = ObjectKey::resource_group("rg1");
        let wanted = resource_group_value(&group(8, 0.0, None));
        assert!(in_sync(&key, Some(&wanted), Some(&resource_group_value(&group(8, 52.5, Some(1)))), Some(1)).unwrap());
        assert!(!in_sync(&key, Some(&wanted), Some(&resource_group_value(&group(8, 0.0, Some(2)))), Some(1)).unwrap());
        assert!(!in_sync(&key, Some(&wanted), Some(&resource_group_value(&group(9, 0.0, Some(1)))), Some(1)).unwrap());
        // PD's default messages compare equal; a query limit TiDB cleared
        // does not.
        let mut filled = group(8, 0.0, Some(1));
        filled.background_settings = Some(rmpb::BackgroundSettings::default());
        filled.raw_resource_settings = Some(rmpb::GroupRawResourceSettings::default());
        assert!(in_sync(&key, Some(&wanted), Some(&resource_group_value(&filled)), Some(1)).unwrap());
        let mut limited = group(8, 0.0, Some(1));
        limited.runaway_settings = Some(rmpb::RunawaySettings { action: 2, ..Default::default() });
        assert!(!in_sync(&key, Some(&wanted), Some(&resource_group_value(&limited)), Some(1)).unwrap());
    }
}
