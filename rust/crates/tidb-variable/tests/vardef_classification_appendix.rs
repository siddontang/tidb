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

//! The §6.3 appendix of `rust/docs/design/2026-11-24-multi-keyspace-server.md`
//! is generated from `tidb_vardef::classification` and the system-variable
//! registry. This test fails when the two drift apart; run it with
//! `UPDATE_MULTI_KEYSPACE_APPENDIX=1` to rewrite the appendix.

use tidb_vardef::classification::{StaticScope, PROCESS_STATICS};
use tidb_vardef::snapshot::SNAPSHOT_STATICS;
use tidb_variable::sysvar::{get_sys_var, SCOPE_GLOBAL, SCOPE_INSTANCE, SCOPE_SESSION};

const BEGIN: &str = "<!-- BEGIN generated: vardef classification -->";
const END: &str = "<!-- END generated: vardef classification -->";

fn design_doc() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/design/2026-11-24-multi-keyspace-server.md")
}

/// The registry scope of `sysvar`, as Go spells it.
fn registry_scope(sysvar: &str) -> String {
    let def = get_sys_var(sysvar).unwrap_or_else(|| panic!("{sysvar} is not a registered system variable"));
    let scopes: Vec<&str> = [(SCOPE_GLOBAL, "GLOBAL"), (SCOPE_SESSION, "SESSION"), (SCOPE_INSTANCE, "INSTANCE")]
        .into_iter()
        .filter(|(bit, _)| def.scope & bit != 0)
        .map(|(_, name)| name)
        .collect();
    if scopes.is_empty() {
        "NONE".to_owned()
    } else {
        scopes.join(" \\| ")
    }
}

fn appendix() -> String {
    let count = |scope| PROCESS_STATICS.iter().filter(|entry| entry.scope == scope).count();
    let mut out = format!(
        "{BEGIN}\n\n{} statics: {} global ({} kept per keyspace so far), {} instance, {} internal.\n\n\
         | Static | System variable | Registry scope | Class | Per keyspace | Governs |\n\
         | --- | --- | --- | --- | --- | --- |\n",
        PROCESS_STATICS.len(),
        count(StaticScope::Global),
        SNAPSHOT_STATICS.len(),
        count(StaticScope::Instance),
        count(StaticScope::Internal),
    );
    for entry in PROCESS_STATICS {
        let (sysvar, scope) = match entry.sysvar {
            Some(sysvar) => (format!("`{sysvar}`"), registry_scope(sysvar)),
            None => ("--".to_owned(), "--".to_owned()),
        };
        let per_keyspace = match entry.scope {
            StaticScope::Global if SNAPSHOT_STATICS.contains(&entry.name) => "yes",
            StaticScope::Global => "not yet",
            _ => "--",
        };
        out.push_str(&format!(
            "| `{}` | {sysvar} | {scope} | {} | {per_keyspace} | {} |\n",
            entry.name,
            entry.scope.name(),
            entry.governs
        ));
    }
    out.push('\n');
    out.push_str(END);
    out
}

#[test]
fn the_design_appendix_matches_the_classification() {
    let path = design_doc();
    let doc = std::fs::read_to_string(&path).expect("read the design doc");
    let generated = appendix();
    let (Some(begin), Some(end)) = (doc.find(BEGIN), doc.find(END)) else {
        panic!("{} has no generated appendix markers", path.display());
    };
    let current = &doc[begin..end + END.len()];
    if current == generated {
        return;
    }
    if std::env::var_os("UPDATE_MULTI_KEYSPACE_APPENDIX").is_some() {
        let updated = format!("{}{generated}{}", &doc[..begin], &doc[end + END.len()..]);
        std::fs::write(&path, updated).expect("write the design doc");
        return;
    }
    panic!(
        "the §6.3 appendix of {} is stale; rerun with UPDATE_MULTI_KEYSPACE_APPENDIX=1",
        path.display()
    );
}
