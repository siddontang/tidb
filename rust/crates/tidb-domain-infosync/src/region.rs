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

//! Go `pkg/domain/infosync/region.go`: the region distribution and the
//! scheduler calls TiDB makes through the info syncer's PD HTTP client
//! (`is.pdHTTPCli`), which a test replaces (`SetPDHttpCliForTest`).

use std::collections::HashMap;
use std::sync::Arc;

use tikv_client::pd_http;

use crate::pd_http::PdHttpClient;

/// Go `SetPDHttpCliForTest` on the info syncer this thread reads: installs
/// `cli` and answers the function that puts the previous client back.
pub fn set_pd_http_cli_for_test(cli: Arc<dyn PdHttpClient>) -> impl FnOnce() {
    let syncer = crate::info_syncer::get();
    let original = syncer.pd_http_cli();
    syncer.init_pd_http_cli(Some(cli));
    move || syncer.init_pd_http_cli(original)
}

fn pd_http_cli(name: &str) -> Result<Arc<dyn PdHttpClient>, String> {
    crate::info_syncer::get()
        .pd_http_cli()
        // PD client `errs.ErrClientGetLeader.FastGenByArgs(name)`.
        .ok_or_else(|| format!("[PD:client:ErrClientGetLeader]get leader failed, {name}"))
}

/// Go `GetRegionDistributionByKeyRange`.
///
/// # Errors
/// No PD, or PD's.
pub fn get_region_distribution_by_key_range(
    start_key: &[u8],
    end_key: &[u8],
    engine: &str,
) -> Result<pd_http::RegionDistributions, String> {
    pd_http_cli("pd http cli is nil")?
        .get_region_distribution_by_key_range(&pd_http::KeyRange::new(start_key, end_key), engine)
}

/// Go `GetSchedulerConfig`.
///
/// # Errors
/// No PD, or PD's.
pub fn get_scheduler_config(scheduler_name: &str) -> Result<serde_json::Value, String> {
    pd_http_cli("pd http cli is nil")?.get_scheduler_config(scheduler_name)
}

/// Go `CreateSchedulerConfigWithInput`.
///
/// # Errors
/// No PD, or PD's.
pub fn create_scheduler_config_with_input(
    scheduler_name: &str,
    mut input: HashMap<String, serde_json::Value>,
) -> Result<(), String> {
    pd_http_cli(scheduler_name)?.create_scheduler_with_input(scheduler_name, &mut input)
}

/// Go `CancelSchedulerJob`.
///
/// # Errors
/// No PD, or PD's.
pub fn cancel_scheduler_job(scheduler_name: &str, job_id: u64) -> Result<(), String> {
    pd_http_cli(scheduler_name)?.cancel_scheduler_job(scheduler_name, job_id)
}
