//! The per-runtime keyspace identity
//! (`rust/docs/design/2026-11-24-multi-keyspace-server.md`, §6.1). Separate
//! executable with one test: it changes the process's configured keyspace and
//! multi-keyspace mode, and restores the mode even when an assertion fails.

/// Restores the process's multi-keyspace mode when dropped, including while
/// a failed assertion unwinds.
struct RestoreMode(bool);

impl Drop for RestoreMode {
    fn drop(&mut self) {
        tidb_keyspace::set_multi_keyspace_mode(self.0);
    }
}

/// A runtime's identity answers for its own keyspace, not the process's
/// configured one: its name, its resource-tag bytes and its username policy.
#[test]
fn a_keyspace_context_names_its_own_keyspace() {
    tidb_config::config_tree::config::update_global(|config| config.keyspace_name = "ks_a".into());
    let tenant = tidb_keyspace::KeyspaceContext::new("ks_b", Some(7));
    assert_eq!(tenant.name(), "ks_b");
    assert_eq!(tenant.id(), Some(7));
    let expected = tidb_config_kerneltype::is_next_gen().then_some(&b"ks_b"[..]);
    assert_eq!(tenant.name_bytes(), expected);

    // A single-keyspace node outside Starter: the permissive policy, as
    // `get_username_policy` answers.
    assert_eq!(tenant.username_policy(), tidb_keyspace::UsernamePolicy::Default);
    assert_eq!(tenant.username_policy(), tidb_keyspace::get_username_policy());
    assert_eq!(tidb_keyspace::KeyspaceContext::from_settings().name(), "ks_a");

    // A node serving several keyspaces prefixes each tenant's accounts with
    // its own keyspace.
    let _restore = RestoreMode(tidb_keyspace::is_multi_keyspace_mode());
    tidb_keyspace::set_multi_keyspace_mode(true);
    let policy = tenant.username_policy();
    assert!(policy.validate_username("ks_b.x").is_ok());
    assert_eq!(
        policy.validate_username("ks_a.x").unwrap_err().to_string(),
        "[ddl:1468]User name must start with `ks_b.` (use `ks_b.ks_a.x` instead)"
    );
    assert_eq!(policy.get_username_variants("app"), ["ks_b.app"]);
}
