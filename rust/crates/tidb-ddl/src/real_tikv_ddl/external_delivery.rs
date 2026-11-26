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
//! and what undoes them when the step is definitely aborted.
//!
//! Go's job step writes PD call by call and never undoes a write. Here a
//! step's catalog change can still fail after PD took its writes (a lost
//! owner, a definite commit failure, the MDL registration), so each delivery
//! first reads what PD held under the names it is about to write, and a
//! definite abort puts that back: an existing bundle, label rule or resource
//! group gets its previous value, and a name this attempt created is
//! removed. An undetermined commit undoes nothing.
//!
//! Only the DDL owner may undo: an attempt that lost its ownership leaves
//! PD as it is (a successor may already have committed the same names), and
//! the abort reports it. The check is the owner lease check right before the
//! undo; a lease that ends between that check and PD answering the undo is
//! the residual window (multi-keyspace design §6.2).

use std::time::Duration;

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

/// What undoes a delivered placement step: the groups' bundles before it.
enum PlacementReceipt {
    /// PD's bundles as PD keeps them (keyspace-encoded), an empty one for a
    /// group this attempt created.
    Pd {
        pd_http_client: pd_http::Client,
        previous: Vec<pd_http::GroupBundle>,
    },
    /// The info syncer's bundles, an empty one for a new group.
    InfoSyncer { previous: Vec<tidb_placement::Bundle> },
}

/// What undoes a delivered label step: the rules its IDs held before it.
struct LabelReceipt {
    manager: std::sync::Arc<dyn tidb_domain_infosync::label_manager::LabelRuleManager>,
    original: Vec<tidb_ddl_label::Rule>,
    rule_ids: Vec<String>,
}

/// What undoes a delivered resource-group write: the group before it, `None`
/// for a group this attempt created.
struct ResourceGroupReceipt {
    name: String,
    previous: Option<rmpb::ResourceGroup>,
}

/// The PD writes one job step delivered, and what undoes them.
#[derive(Default)]
pub(crate) struct ExternalDelivery {
    placement: Option<PlacementReceipt>,
    label: Option<LabelReceipt>,
    resource_group: Option<ResourceGroupReceipt>,
}

fn failed(context: &str, error: impl std::fmt::Display) -> String {
    format!("{context}: {error}")
}

/// The group IDs PD keeps `bundles` under for the keyspace of `codec`.
fn pd_group_ids(codec: Option<&tikv_client::request::ApiV2Codec>, bundles: &[tidb_placement::Bundle]) -> Vec<String> {
    bundles
        .iter()
        .map(|bundle| tidb_domain_infosync::placement_manager::keyspace_bundle_id(codec, &bundle.id))
        .collect()
}

impl ExternalDelivery {
    /// Delivers the step's placement bundles, after reading the bundles the
    /// groups held. A failure leaves PD as it was.
    pub(crate) fn deliver_placement(
        &mut self,
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
                let previous = crate::placement_delivery::get_group_bundles(
                    &pd_http_client,
                    &pd_group_ids(codec.as_ref(), bundles),
                    timeout,
                )
                .map_err(|error| failed(CONTEXT, error))?;
                crate::placement_delivery::put_rule_bundles(&pd_http_client, codec.as_ref(), bundles, timeout)
                    .map_err(|error| failed(CONTEXT, error))?;
                self.placement = Some(PlacementReceipt::Pd { pd_http_client, previous });
            }
            PlacementTarget::InfoSyncer => {
                let previous = bundles
                    .iter()
                    .map(|bundle| tidb_domain_infosync::info_syncer::get_rule_bundle(&bundle.id))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| failed(CONTEXT, error))?
                    .into_iter()
                    .zip(bundles)
                    .map(|(previous, bundle)| tidb_placement::Bundle {
                        id: bundle.id.clone(),
                        ..previous
                    })
                    .collect();
                tidb_domain_infosync::info_syncer::put_rule_bundles_with_default_retry(bundles)
                    .map_err(|error| failed(CONTEXT, error))?;
                self.placement = Some(PlacementReceipt::InfoSyncer { previous });
            }
        }
        Ok(())
    }

    /// Go's job step makes its label-rule calls before the version is
    /// published and fails the job when one fails. The rules the step
    /// touches are read first, so a later failure puts them back.
    /// `steps` are the step's calls with Go's `errors.Wrapf` context of a
    /// failed one.
    pub(crate) fn deliver_label_rules(&mut self, steps: &[(&LabelRuleWrite, Option<&str>)]) -> Result<(), String> {
        if steps.is_empty() {
            return Ok(());
        }
        let manager = tidb_domain_infosync::info_syncer::label_rule_manager();
        let mut rule_ids = steps
            .iter()
            .flat_map(|(write, _)| write.rule_ids())
            .collect::<Vec<_>>();
        rule_ids.sort();
        rule_ids.dedup();
        let original = manager
            .get_label_rules(&rule_ids)
            .map_err(|error| failed("failed to get PD the label rules", error))?
            .into_values()
            .collect();
        self.label = Some(LabelReceipt {
            manager,
            original,
            rule_ids,
        });
        for (write, context) in steps {
            let receipt = self.label.as_ref().expect("just set");
            if let Err(error) = write.apply(receipt.manager.as_ref()) {
                return Err(match context {
                    Some(context) => failed(context, error),
                    None => error,
                });
            }
        }
        Ok(())
    }

    /// Makes the resource-group job's write `apply` of the group `name`,
    /// after reading the group.
    pub(crate) fn deliver_resource_group(
        &mut self,
        name: &str,
        apply: impl FnOnce() -> Result<(), String>,
    ) -> Result<(), String> {
        let previous = tidb_domain_infosync::info_syncer::lookup_resource_group(name)
            .map_err(|error| failed("failed to get PD the resource group", error))?;
        apply()?;
        self.resource_group = Some(ResourceGroupReceipt {
            name: name.to_owned(),
            previous,
        });
        Ok(())
    }

    /// Undoes the delivered writes of a definitely aborted step, if this
    /// attempt still owns the DDL (`still_owner`); otherwise leaves PD as it
    /// is. Answers what the abort's cause should add: the undo's failures,
    /// or the undo skipped.
    pub(crate) fn compensate(
        &self,
        still_owner: &dyn Fn() -> Result<(), String>,
        timeout: Duration,
    ) -> Result<(), String> {
        if self.placement.is_none() && self.label.is_none() && self.resource_group.is_none() {
            return Ok(());
        }
        if let Err(error) = still_owner() {
            tidb_ddl_logutil::ddl_logger().warn(
                "a DDL attempt that lost its ownership leaves its PD writes to the owner",
                &[tidb_log::Field::new("error", tidb_log::Value::Str(error.clone()))],
            );
            return Err(format!(
                "this node is no longer the DDL owner, so it left the attempt's PD writes: {error}"
            ));
        }
        let mut notes = Vec::new();
        let mut failed = |kind: &str, error: String| {
            notes.push(format!("restoring the attempt's {kind} failed: {error}"));
        };
        if let Some(receipt) = &self.label {
            if let Err(error) = receipt.restore() {
                failed("label rules", error);
            }
        }
        if let Some(receipt) = &self.placement {
            if let Err(error) = receipt.restore(timeout) {
                failed("placement bundles", error);
            }
        }
        if let Some(receipt) = &self.resource_group {
            if let Err(error) = receipt.restore() {
                failed("resource group", error);
            }
        }
        if notes.is_empty() {
            Ok(())
        } else {
            Err(notes.join("; and "))
        }
    }
}

impl LabelReceipt {
    fn restore(&self) -> Result<(), String> {
        let present = self
            .original
            .iter()
            .map(|rule| rule.id.as_str())
            .collect::<std::collections::HashSet<_>>();
        let delete_rules = self
            .rule_ids
            .iter()
            .filter(|id| !present.contains(id.as_str()))
            .cloned()
            .collect();
        let patch = tidb_ddl_label::new_rule_patch(self.original.clone(), delete_rules);
        self.manager.update_label_rules(&patch)
    }
}

impl PlacementReceipt {
    fn restore(&self, timeout: Duration) -> Result<(), String> {
        match self {
            Self::Pd { pd_http_client, previous } => {
                crate::placement_delivery::put_group_bundles(pd_http_client, previous, timeout)
                    .map_err(|error| error.to_string())
            }
            Self::InfoSyncer { previous } => tidb_domain_infosync::info_syncer::put_rule_bundles(previous),
        }
    }
}

impl ResourceGroupReceipt {
    fn restore(&self) -> Result<(), String> {
        let current = tidb_domain_infosync::info_syncer::lookup_resource_group(&self.name)?;
        match (&self.previous, current) {
            (None, None) => Ok(()),
            (None, Some(_)) => tidb_domain_infosync::info_syncer::delete_resource_group(&self.name),
            (Some(previous), None) => tidb_domain_infosync::info_syncer::add_resource_group(previous.clone()),
            (Some(previous), Some(_)) => tidb_domain_infosync::info_syncer::modify_resource_group(previous.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    use tidb_domain_infosync::info_syncer::{enter, InfoSyncer};
    use tidb_domain_infosync::label_manager::{LabelRuleManager, MockLabelManager};
    use tidb_domain_infosync::resource_manager_client::MockResourceManagerClient;

    use super::*;

    /// PD's placement rule bundles behind its HTTP API: `GET
    /// /pd/api/v1/config/placement-rule/<group>` (404 for an unknown group)
    /// and `POST /pd/api/v1/config/placement-rule?partial=true`, where a
    /// bundle without rules removes its group.
    struct FakePd {
        endpoint: String,
        groups: Arc<Mutex<HashMap<String, serde_json::Value>>>,
    }

    impl FakePd {
        fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("a port");
            let endpoint = format!("http://{}", listener.local_addr().expect("an address"));
            let groups: Arc<Mutex<HashMap<String, serde_json::Value>>> = Arc::new(Mutex::new(HashMap::new()));
            let held = Arc::clone(&groups);
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(mut stream) = stream else { continue };
                    let mut reader = BufReader::new(stream.try_clone().expect("a stream"));
                    let mut request_line = String::new();
                    if reader.read_line(&mut request_line).is_err() {
                        continue;
                    }
                    let mut length = 0;
                    loop {
                        let mut header = String::new();
                        reader.read_line(&mut header).expect("a header");
                        if header == "\r\n" || header.is_empty() {
                            break;
                        }
                        if let Some(value) = header.to_ascii_lowercase().strip_prefix("content-length:") {
                            length = value.trim().parse().expect("a length");
                        }
                    }
                    let mut body = vec![0; length];
                    reader.read_exact(&mut body).expect("a body");
                    let mut parts = request_line.split(' ');
                    let (method, path) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
                    let (status, answer) = match method {
                        "GET" => {
                            let group = path.rsplit('/').next().unwrap_or("");
                            match held.lock().unwrap().get(group) {
                                Some(bundle) => ("200 OK", bundle.to_string()),
                                None => ("404 Not Found", String::new()),
                            }
                        }
                        _ => {
                            let bundles: Vec<serde_json::Value> = serde_json::from_slice(&body).expect("bundles");
                            let mut groups = held.lock().unwrap();
                            for bundle in bundles {
                                let id = bundle["group_id"].as_str().unwrap().to_owned();
                                if bundle["rules"].as_array().is_none_or(Vec::is_empty) {
                                    groups.remove(&id);
                                } else {
                                    groups.insert(id, bundle);
                                }
                            }
                            ("200 OK", String::new())
                        }
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{answer}",
                        answer.len()
                    );
                    let _ = stream.write_all(response.as_bytes());
                }
            });
            Self { endpoint, groups }
        }

        fn client(&self) -> pd_http::Client {
            tidb_txnkv::driver::tikv_store::new_pd_http_client_with_mock_service_discovery(
                "test",
                std::slice::from_ref(&self.endpoint),
                Vec::new(),
            )
            .expect("a PD HTTP client")
        }

        fn target(&self) -> PlacementTarget {
            PlacementTarget::Pd {
                pd_http_client: self.client(),
                codec: None,
            }
        }

        /// The peers the group's bundle places (a leader and its
        /// followers), `None` without one.
        fn peers(&self, group: &str) -> Option<i64> {
            self.groups.lock().unwrap().get(group).map(|bundle| {
                bundle["rules"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|rule| rule["count"].as_i64().unwrap())
                    .sum()
            })
        }
    }

    /// A table bundle of `followers` followers.
    fn bundle(table_id: i64, followers: u64) -> tidb_placement::Bundle {
        let settings = tidb_model::placement::PlacementSettings {
            followers,
            ..Default::default()
        };
        let mut bundle = tidb_placement::new_bundle_from_options(Some(&settings)).expect("a bundle");
        bundle.reset(tidb_placement::RULE_INDEX_TABLE, &[table_id]);
        bundle
    }

    const TIMEOUT: Duration = Duration::from_secs(5);

    fn owner() -> Result<(), String> {
        Ok(())
    }

    fn lost_owner() -> Result<(), String> {
        Err("not the DDL owner".to_owned())
    }


    /// A definitely aborted step that rewrote an existing table's bundle (an
    /// ALTER PLACEMENT POLICY) and created a new one through PD's HTTP API:
    /// the undo puts the existing bundle back and removes only the new one.
    /// Before, the undo sent an empty bundle for every group it wrote, which
    /// deleted the existing table's placement from PD while the catalog
    /// still had it.
    #[test]
    fn an_aborted_step_restores_pds_previous_bundles_and_removes_only_its_new_ones() {
        let pd = FakePd::start();
        let existing = tidb_placement::group_id(100);
        crate::placement_delivery::put_rule_bundles(&pd.client(), None, &[bundle(100, 1)], TIMEOUT).unwrap();
        assert_eq!(pd.peers(&existing), Some(2));

        let mut delivery = ExternalDelivery::default();
        delivery
            .deliver_placement(pd.target(), &[bundle(100, 2), bundle(200, 2)], TIMEOUT)
            .unwrap();
        assert_eq!(pd.peers(&existing), Some(3));
        assert_eq!(pd.peers(&tidb_placement::group_id(200)), Some(3));

        delivery.compensate(&owner, TIMEOUT).unwrap();
        assert_eq!(pd.peers(&existing), Some(2), "the existing table keeps its placement");
        assert_eq!(pd.peers(&tidb_placement::group_id(200)), None, "the new group is gone");
    }

    /// An attempt that lost its ownership leaves PD alone: a successor that
    /// committed the very value this attempt wrote keeps it.
    #[test]
    fn a_stale_owner_leaves_a_successors_bundle_of_the_same_value() {
        let pd = FakePd::start();
        let group = tidb_placement::group_id(300);
        let mut stale = ExternalDelivery::default();
        stale.deliver_placement(pd.target(), &[bundle(300, 2)], TIMEOUT).unwrap();
        // The successor commits the same bundle.
        let mut successor = ExternalDelivery::default();
        successor.deliver_placement(pd.target(), &[bundle(300, 2)], TIMEOUT).unwrap();

        let note = stale.compensate(&lost_owner, TIMEOUT).unwrap_err();
        assert!(note.contains("no longer the DDL owner"), "{note}");
        assert_eq!(pd.peers(&group), Some(3));
    }

    /// The label rules and the resource group a definitely aborted step
    /// wrote go back to what they were: a rewritten rule and a modified group
    /// get their previous values, a new rule and a new group are removed. A
    /// stale owner's abort leaves them.
    #[test]
    fn an_aborted_step_restores_label_rules_and_resource_groups() {
        let syncer = Arc::new(InfoSyncer::default());
        let labels = Arc::new(MockLabelManager::default());
        syncer.init_label_rule_manager(Some(labels.clone()));
        syncer.init_resource_manager_client(Some(Arc::new(MockResourceManagerClient::new())));
        let _entered = enter(Arc::clone(&syncer));
        let rule = |id: &str, value: &str| {
            let mut rule = tidb_ddl_label::Rule::new();
            rule.apply_attributes_spec(&tidb_ast::AttributesSpec {
                attributes: Some(format!("merge_option={value}")),
            })
            .unwrap();
            rule.reset(&tidb_ddl_label::CodecV1, "test", id, "", &[1]);
            rule
        };
        labels.put_label_rule(&rule("t", "deny")).unwrap();
        let group = |name: &str, priority: u32| rmpb::ResourceGroup {
            name: name.to_owned(),
            priority,
            ..rmpb::ResourceGroup::default()
        };
        tidb_domain_infosync::info_syncer::add_resource_group(group("rg", 1)).unwrap();

        for (still_owner, kept) in [(owner as fn() -> Result<(), String>, false), (lost_owner, true)] {
            let write = LabelRuleWrite::Patch(tidb_ddl_label::new_rule_patch(
                vec![rule("t", "allow"), rule("t2", "allow")],
                Vec::new(),
            ));
            let mut delivery = ExternalDelivery::default();
            delivery.deliver_label_rules(&[(&write, None)]).unwrap();
            delivery
                .deliver_resource_group("rg", || tidb_domain_infosync::info_syncer::modify_resource_group(group("rg", 9)))
                .unwrap();
            let mut created = ExternalDelivery::default();
            created
                .deliver_resource_group("new_rg", || tidb_domain_infosync::info_syncer::add_resource_group(group("new_rg", 2)))
                .unwrap();

            let _ = delivery.compensate(&still_owner, TIMEOUT);
            let _ = created.compensate(&still_owner, TIMEOUT);
            let rules = labels.get_label_rules(&["schema/test/t".to_owned(), "schema/test/t2".to_owned()]).unwrap();
            let rg = tidb_domain_infosync::info_syncer::lookup_resource_group("rg").unwrap().unwrap();
            let new_rg = tidb_domain_infosync::info_syncer::lookup_resource_group("new_rg").unwrap();
            if kept {
                assert_eq!(rules.len(), 2);
                assert_eq!(rg.priority, 9);
                assert!(new_rg.is_some());
            } else {
                assert_eq!(rules.keys().cloned().collect::<Vec<_>>(), ["schema/test/t"]);
                assert_eq!(
                    tidb_ddl_label::restore_region_labels(&rules["schema/test/t"].labels.snapshot()),
                    "\"merge_option=deny\""
                );
                assert_eq!(rg.priority, 1);
                assert!(new_rg.is_none());
            }
        }
    }
}
