//! The multi-keyspace debug assertion
//! (`rust/docs/design/2026-11-24-multi-keyspace-server.md`, §6.1). Separate
//! executable with one test: it switches the process into multi-keyspace mode
//! and unwinds there.

/// A tenant-level path that finds no runtime identity on a multi-keyspace
/// node trips the debug assertion instead of silently using the process's
/// configured keyspace.
#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "multi-keyspace node")]
fn an_unbound_read_on_a_multi_keyspace_node_is_caught() {
    tidb_keyspace::set_multi_keyspace_mode(true);
    let _ = tidb_keyspace::KeyspaceContext::unbound();
}
