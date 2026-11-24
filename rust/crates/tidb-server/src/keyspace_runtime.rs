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

//! One keyspace's serving stack: the store, domain, accounts and background
//! loops a node runs for the keyspace it serves.
//!
//! This is the per-keyspace half of the cluster node, separated from the
//! process half (the MySQL listener, the status port, the shutdown signal)
//! so a process can later hold more than one of them
//! (`rust/docs/design/2026-11-24-multi-keyspace-server.md`, §5.2). Today a
//! node opens exactly one runtime, for its configured keyspace, and the
//! boot and shutdown order is the one `cluster_boot` always had.

use std::sync::Arc;

use tidb_domain_core::node_config::NodeConfig;
use tidb_session::domain::real_tikv_node::RunConfiguredNodeError;
use tidb_store_driver::real_tikv_read::ProductionReadProcessAuthority;
use tidb_store_driver_error::sql_query_error::SqlQueryError;

use crate::cluster_stack::{cluster_session_stack, ClusterStack};

/// A keyspace's opened stack and the services started over it.
pub(crate) struct KeyspaceRuntime {
    /// The keyspace's domain, accounts and reload loops.
    pub(crate) stack: ClusterStack,
    /// Go `setupAutoIDService`, when the node reports status.
    pub(crate) auto_id: Option<Arc<tidb_autoid_service::Service>>,
    /// Go `TestHandler`'s lock resolution over this keyspace's store.
    pub(crate) gc_resolve_locks: Option<crate::http_status::GcResolveLocks>,
    /// Go `repository.SetupRepository(dom)`.
    workload_repository: Arc<tidb_workloadrepo::Worker>,
    /// The keyspace whose own vardef values this runtime registered
    /// (multi-keyspace design §6.3), unregistered when it retires.
    own_vardef_keyspace: Option<String>,
}

impl KeyspaceRuntime {
    /// Opens the stack for `config`'s keyspace and starts its services: the
    /// GC worker, the IMPORT INTO and BRIE hooks, the DXF loop, the workload
    /// repository and the AutoID service. The store authority comes back
    /// beside the runtime: the process shuts it down after the runtime is
    /// released.
    ///
    /// # Errors
    ///
    /// The stack, the GC worker or the DXF loop failed to start.
    ///
    /// With `own_vardef` (a multi-keyspace node's keyspace other than its
    /// configured one) the runtime keeps its own values of the vardef global
    /// statics (design §6.3); otherwise it reads and writes the process
    /// statics, as a single-keyspace node does.
    pub(crate) fn open(
        config: &NodeConfig,
        spill_storage: Arc<tidb_spill_storage::SpillStorage>,
        memory_arbitrator: Option<Arc<tidb_memory::MemArbitrator>>,
        own_vardef: bool,
    ) -> Result<(Self, ProductionReadProcessAuthority), RunConfiguredNodeError> {
        // Go `setGlobalVars`, before the store and the domain are created.
        let mut global_vars = tidb_domain::global_sysvars::GlobalSysvars::before_domain();
        let own_vardef_keyspace = own_vardef.then(|| config.global_config.keyspace_name.clone());
        if let Some(keyspace) = &own_vardef_keyspace {
            global_vars = global_vars.with_keyspace_vardef();
            // The keyspace's DDL steps find its values by its name.
            tidb_vardef::snapshot::register_keyspace(keyspace, global_vars.vardef());
        }
        crate::set_global_vars::set_global_vars(config, &global_vars);
        let (mut stack, authority) =
            cluster_session_stack(config, global_vars, spill_storage, memory_arbitrator)?;
        // Go `store/driver` -> `gcworker.NewGCWorker` + `Start`: the GC leader
        // lease, safe points, lock resolution and delete ranges on TiKV. It
        // retires with the stack's workers, right after the replica-read checker.
        let gc_worker = tidb_gcworker::node::start_gc_worker(
            &stack.factory,
            authority.tikv_store(),
            Arc::new(authority.transaction_opener()),
        )
        .map_err(|error| {
            RunConfiguredNodeError::Engine(SqlQueryError::unknown(format!(
                "start gc worker failed: {error}"
            )))
        })?;
        stack.workers.insert(1, Box::new(gc_worker));
        // Go `importinto`'s `init` registers the IMPORT INTO task type; its
        // tasks import into this node's TiKV store.
        tidb_dxf_importinto::register();
        // Go `executor.RunImportQuery`, which the query step's workers call.
        tidb_session::import_query::register_import_query_runner();
        // Go `handle.GetCloudStorageURI` reads the cluster ID from the store.
        tidb_dxf_handle::set_cluster_id(authority.tikv_store().cluster_id());
        tidb_dxf_importinto::set_import_store(Some(Arc::new(crate::import_store::TikvImportStore::new(
            authority.tikv_store().clone(),
            Arc::clone(&stack.factory),
        ))));
        crate::cluster_boot::install_brie_node(&stack, &authority);
        // Go `BootstrapSession` ends with `dom.InitDistTaskLoop()`; the loop
        // retires before the GC worker, as Go's `Domain.Close` stops it before
        // the store closes.
        let dist_task_loop = stack
            .factory
            .init_dist_task_loop()
            .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error)))?;
        stack.workers.insert(1, Box::new(dist_task_loop));
        // Go `TestHandler`'s `gcworker.RunResolveLocks(ctx, h.Store,
        // h.RegionCache.PDClient(), safePoint, "testGCWorker", 3)`.
        let gc_deps = tidb_gcworker::node::gc_worker_deps(
            &stack.factory,
            authority.tikv_store(),
            Arc::new(authority.transaction_opener()),
        );
        let gc_resolve_locks: Option<crate::http_status::GcResolveLocks> = Some(Arc::new(move |safe_point| {
            tidb_gcworker::run_resolve_locks(gc_deps.clone(), safe_point, "testGCWorker", 3)
                .map_err(|error| error.to_string())
        }));
        // Go `repository.SetupRepository(dom)` in the server's main; the worker
        // retires last of the workers, and `StopRepository` stops it first.
        let workload_repository = tidb_workloadrepo::node::setup_repository(&stack.factory);
        stack.workers.push(Box::new(Arc::clone(&workload_repository)));
        // Go `startStatusServerAndRPCServer`'s `setupAutoIDService`: a TiKV
        // node with a status port serves the AutoID service.
        let auto_id = if config.report_status {
            crate::cluster_boot::setup_auto_id_service(config, &stack, &authority)
        } else {
            None
        };
        Ok((
            Self {
                stack,
                auto_id,
                gc_resolve_locks,
                workload_repository,
                own_vardef_keyspace,
            },
            authority,
        ))
    }

    /// Stops serving the keyspace once its connections are gone: the AutoID
    /// service, the workload repository, the reload loops, then the domain
    /// (Go `closeDomainAndStorage`'s `dom.Close()`: the DDL stops and its
    /// owner lease is revoked now, whoever still holds the domain).
    ///
    /// The process drops whatever still retains the domain (the status
    /// server) between `retire` and [`Self::release`].
    pub(crate) fn retire(&mut self) {
        // Go `Server.closeListener`: `if !deploymode.IsStarter() ||
        // s.StandbyController == nil { s.AutoIDServiceClose() }` -- the
        // standby shutdown closed it already.
        if !tidb_config_deploymode::is_starter()
            || crate::standby_node::server_standby_controller().is_none()
        {
            if let Some(service) = &self.auto_id {
                service.close();
            }
        }
        self.workload_repository.stop();
        // The reload threads hold their own transaction openers; joining
        // them here releases those PD handles before the authority's
        // shutdown drain.
        for worker in self.stack.workers.drain(..) {
            drop(worker);
        }
        self.stack.factory.close();
        if let Some(keyspace) = &self.own_vardef_keyspace {
            tidb_vardef::snapshot::unregister_keyspace(keyspace);
        }
    }

    /// Releases what the keyspace's storage transactions ran under: the
    /// resource-group controller, then the server-id and server-info leases.
    /// After this the process may shut the store authority down.
    pub(crate) fn release(mut self) {
        self.stack.stop_resource_groups();
        for lease in self.stack.leases.drain(..) {
            drop(lease);
        }
    }
}
