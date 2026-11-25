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

//! Go `pkg/domain/infosync/placement_manager.go`: the `InfoSyncer`'s
//! `placementManager`, PD's placement rule bundles or, without PD, an
//! in-memory mock; and `info.go`'s `PutRuleBundlesWithDefaultRetry`.
//!
//! PD keeps one namespace of rule groups for the whole cluster, while table
//! IDs repeat across keyspaces. On a next-generation kernel a keyspace's
//! bundles are therefore keyed and ranged by its keyspace where they meet PD
//! ([`encode_bundle`], [`decode_bundle`]; multi-keyspace design §6.2): the
//! group ID gains the keyspace prefix Go's TiFlash manager gives its
//! placement rule IDs (`encodeRuleID`: `keyspace-<keyspace ID>-<ID>`), and
//! each rule's key range is encoded by the keyspace's codec
//! (`EncodeRegionRange`), as the keyspace's label rules and TiFlash rules
//! are. Go's own bundles carry neither, so the bundles a Go or an older node
//! wrote for a keyspace cover keyspace-less ranges and are not read back.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tidb_placement::pd::PeerRoleType;
use tidb_placement::Bundle;
use tikv_client::pd_http;
use tikv_client::request::ApiV2Codec;

/// Go `PlacementManager` (the methods TiDB calls).
pub trait PlacementManager: Send + Sync + std::fmt::Debug {
    /// Go `GetRuleBundle`: the bundle of group `name`.
    fn get_rule_bundle(&self, name: &str) -> Result<Bundle, String>;
    /// Go `PutRuleBundles`: posts `bundles` to PD.
    fn put_rule_bundles(&self, bundles: &[Bundle]) -> Result<(), String>;
}

/// Go `PDPlacementManager`.
pub struct PdPlacementManager {
    pd_http_client: pd_http::Client,
}

impl std::fmt::Debug for PdPlacementManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("PdPlacementManager").finish_non_exhaustive()
    }
}

impl PdPlacementManager {
    /// Go `initPlacementManager`'s `&PDPlacementManager{pdHTTPCli: is.pdHTTPCli}`.
    #[must_use]
    pub fn new(pd_http_client: &pd_http::Client) -> Self {
        Self {
            pd_http_client: crate::pd_http::info_syncer_pd_http_client(pd_http_client),
        }
    }
}

impl PlacementManager for PdPlacementManager {
    fn get_rule_bundle(&self, name: &str) -> Result<Bundle, String> {
        let group_bundle = tidb_store_driver_txn::cluster_http::block_on_pd_http(
            self.pd_http_client.get_placement_rule_bundle_by_group(name),
        )?;
        // Go's `(*placement.Bundle)(groupBundle)` cast, its ID the name.
        let mut bundle: Bundle = serde_json::to_value(&group_bundle)
            .and_then(serde_json::from_value)
            .map_err(|error| error.to_string())?;
        bundle.id = name.to_owned();
        Ok(bundle)
    }

    fn put_rule_bundles(&self, bundles: &[Bundle]) -> Result<(), String> {
        if bundles.is_empty() {
            return Ok(());
        }
        // Go's `(*pd.GroupBundle)(bundle)` cast.
        let rule_bundles = bundles
            .iter()
            .map(|bundle| serde_json::to_value(bundle).and_then(serde_json::from_value))
            .collect::<Result<Vec<pd_http::GroupBundle>, _>>()
            .map_err(|error| error.to_string())?;
        tidb_store_driver_txn::cluster_http::block_on_pd_http(
            self.pd_http_client.set_placement_rule_bundles(&rule_bundles, true),
        )
    }
}

/// Go `mockPlacementManager`.
#[derive(Debug, Default)]
pub struct MockPlacementManager {
    bundles: Mutex<BTreeMap<String, Bundle>>,
}

impl PlacementManager for MockPlacementManager {
    /// The stored bundle, or an empty one with the ID.
    fn get_rule_bundle(&self, name: &str) -> Result<Bundle, String> {
        Ok(self
            .bundles
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .cloned()
            .unwrap_or_else(|| Bundle {
                id: name.to_owned(),
                ..Bundle::default()
            }))
    }

    fn put_rule_bundles(&self, bundles: &[Bundle]) -> Result<(), String> {
        let mut stored = self.bundles.lock().unwrap_or_else(PoisonError::into_inner);
        for bundle in bundles {
            if bundle.is_empty() {
                stored.remove(&bundle.id);
            } else {
                stored.insert(bundle.id.clone(), bundle.clone());
            }
        }
        check_bundles(&stored)
    }
}

/// Go `CheckBundle`: the leader rules of a bundle do not overlap without an
/// explicit override (a simpler version of PD's `prepareRulesForApply` +
/// `checkApplyRules`).
///
/// # Errors
/// Go's `ErrBuildRuleList` text for two overlapping leader ranges.
pub fn check_bundle(bundle: &Bundle) -> Result<(), String> {
    let mut keys: Vec<(String, String)> = Vec::with_capacity(bundle.rules.len());
    for rule in &bundle.rules {
        if rule.role == PeerRoleType::LEADER {
            if rule.r#override {
                // PD would override the previous rules, not only the
                // overlapping key ranges.
                keys.clear();
            }
            keys.push((rule.start_key_hex.clone(), rule.end_key_hex.clone()));
        }
    }
    keys.sort();
    for pair in keys.windows(2) {
        if pair[1].0 < pair[0].1 {
            return Err(format!(
                r#"ERROR 8243 (HY000): "[PD:placement:ErrBuildRuleList]build rule list failed, multiple leader replicas for range {{{}, {}}}"#,
                pair[0].0, pair[1].1
            ));
        }
    }
    Ok(())
}

fn check_bundles(bundles: &BTreeMap<String, Bundle>) -> Result<(), String> {
    bundles.values().try_for_each(check_bundle)
}

/// Go `InfoSyncer.initPlacementManager` on the process's info syncer: PD's
/// manager, or a fresh mock when the node has no PD.
pub fn init_placement_manager(manager: Option<Arc<dyn PlacementManager>>) {
    crate::info_syncer::global_info_syncer().init_placement_manager(manager);
}

/// The placement manager of the info syncer this thread reads
/// ([`crate::info_syncer::get`]).
pub fn placement_manager() -> Arc<dyn PlacementManager> {
    crate::info_syncer::get().placement_manager()
}

/// The keyspace codec PD keys and ranges a bundle by: the store's, on a
/// next-generation kernel.
fn keyspace_codec(codec: Option<&ApiV2Codec>) -> Option<&ApiV2Codec> {
    codec.filter(|_| tidb_config_kerneltype::is_next_gen())
}

/// The group ID PD keeps the bundle `id` of the keyspace of `codec` under:
/// `keyspace-<keyspace ID>-<id>` on a next-generation kernel, `id` itself
/// otherwise.
#[must_use]
pub fn keyspace_bundle_id(codec: Option<&ApiV2Codec>, id: &str) -> String {
    match keyspace_codec(codec) {
        Some(codec) => format!("keyspace-{}-{id}", codec.keyspace_id()),
        None => id.to_owned(),
    }
}

fn hex_key(key_hex: &str) -> Result<Vec<u8>, String> {
    hex::decode(key_hex).map_err(|error| format!("invalid placement rule key {key_hex}: {error}"))
}

/// `bundle` as PD keeps it for the keyspace of `codec`: its group ID
/// [`keyspace_bundle_id`], and each rule's range (`codec.EncodeBytes` of a
/// key in the keyspace, or empty for the whole keyspace) encoded into the
/// keyspace (`EncodeRegionRange`). Unchanged without a keyspace codec.
///
/// # Errors
/// A rule key that is not a hex `codec.EncodeBytes` key.
pub fn encode_bundle(codec: Option<&ApiV2Codec>, bundle: &Bundle) -> Result<Bundle, String> {
    let Some(codec) = keyspace_codec(codec) else {
        return Ok(bundle.clone());
    };
    let raw = |key_hex: &str| -> Result<Vec<u8>, String> {
        let key = hex_key(key_hex)?;
        if key.is_empty() {
            return Ok(key);
        }
        tidb_codec::decode_bytes(&key)
            .map(|(_, raw)| raw)
            .map_err(|error| format!("invalid placement rule key {key_hex}: {error}"))
    };
    bundle.regroup(|id| keyspace_bundle_id(Some(codec), id), |start, end| {
        let (start, end) = codec.encode_region_range(&raw(start)?, &raw(end)?);
        Ok((hex::encode(start), hex::encode(end)))
    })
}

/// The bundle `id` back from the group PD keeps it under for the keyspace of
/// `codec` ([`encode_bundle`]'s inverse).
///
/// # Errors
/// A rule key outside the keyspace, or not a hex region key.
pub fn decode_bundle(codec: Option<&ApiV2Codec>, id: &str, bundle: Bundle) -> Result<Bundle, String> {
    let Some(codec) = keyspace_codec(codec) else {
        return Ok(bundle);
    };
    let encode = |key: &[u8]| {
        if key.is_empty() {
            return String::new();
        }
        let mut encoded = Vec::new();
        tidb_codec::encode_bytes(&mut encoded, key);
        hex::encode(encoded)
    };
    bundle.regroup(|_| id.to_owned(), |start, end| {
        let (start, end) = codec
            .decode_region_range(&hex_key(start)?, &hex_key(end)?)
            .map_err(|error| error.to_string())?;
        Ok((encode(&start), encode(&end)))
    })
}

/// Go `GetRuleBundle`: the bundle `name` of the keyspace this thread's info
/// syncer serves.
///
/// # Errors
/// The placement manager's.
pub fn get_rule_bundle(name: &str) -> Result<Bundle, String> {
    let syncer = crate::info_syncer::get();
    let bundle = syncer
        .placement_manager()
        .get_rule_bundle(&keyspace_bundle_id(syncer.codec(), name))?;
    decode_bundle(syncer.codec(), name, bundle)
}

/// Go `RequestPDMaxRetry`.
const REQUEST_PD_MAX_RETRY: usize = 3;
/// Go `RequestRetryInterval`.
const REQUEST_RETRY_INTERVAL: Duration = Duration::from_millis(200);

/// Go `PutRuleBundles`.
///
/// # Errors
/// The placement manager's.
pub fn put_rule_bundles(bundles: &[Bundle]) -> Result<(), String> {
    if let Some(value) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/domain/infosync/putRuleBundlesError") {
        return Err(if value.bool() {
            crate::infosync_errors::ERR_HTTP_SERVICE_ERROR.fast_gen("mock service error", &[]).to_string()
        } else {
            "mock other error".to_owned()
        });
    }
    let syncer = crate::info_syncer::get();
    let bundles = bundles
        .iter()
        .map(|bundle| encode_bundle(syncer.codec(), bundle))
        .collect::<Result<Vec<_>, _>>()?;
    syncer.placement_manager().put_rule_bundles(&bundles)
}

/// Go `PutRuleBundlesWithRetry`: retries a failure other than PD's own
/// answer (`ErrHTTPServiceError`).
///
/// # Errors
/// The last attempt's.
pub fn put_rule_bundles_with_retry(
    bundles: &[Bundle],
    max_retry: usize,
    interval: Duration,
) -> Result<(), String> {
    let mut result = Ok(());
    for i in 0..=max_retry {
        result = put_rule_bundles(bundles);
        match &result {
            Ok(()) => return result,
            Err(error) if crate::pd_http::is_http_service_error(error) => return result,
            Err(error) => {
                if i != max_retry {
                    tidb_logutil::bg_logger().warn(
                        "Error occurs when PutRuleBundles, retry",
                        &[tidb_logutil::zap_error(error)],
                    );
                    std::thread::sleep(interval);
                }
            }
        }
    }
    result
}

/// Go `PutRuleBundlesWithDefaultRetry`.
///
/// # Errors
/// The last attempt's.
pub fn put_rule_bundles_with_default_retry(bundles: &[Bundle]) -> Result<(), String> {
    put_rule_bundles_with_retry(bundles, REQUEST_PD_MAX_RETRY, REQUEST_RETRY_INTERVAL)
}
