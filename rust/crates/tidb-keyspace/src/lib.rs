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

//! Complete transcreation of Go `pkg/keyspace` (`keyspace.go`,
//! `username_policy.go`): keyspace-name resolution from the global config,
//! the etcd namespace paths a keyspace scopes, and the username policy that
//! keyspace-qualifies account names in Starter deployments.
//!
//! Go reads the API version and keyspace ID off client-go's `tikv.Codec` and
//! answers PD's `pd.APIContext`; neither client library exists here, so the
//! same two facts arrive through [`KeyspaceCodec`] and the same answer leaves
//! as [`ApiContext`] — the values are unchanged, only the carrier types are
//! local. `WrapZapcoreWithKeyspace` wraps a zap core to stamp every record
//! with the keyspace name; its observable contract is the field itself, which
//! [`keyspace_name_log_field`] supplies for this workspace's logger
//! composition.
//!
//! One process may serve several keyspaces
//! (`rust/docs/design/2026-11-24-multi-keyspace-server.md`, §6.1), so the
//! identity a statement or a job acts under is a [`KeyspaceContext`] owned by
//! its keyspace's runtime. The `*_by_settings` readers stay for what is
//! process-level by nature and are the runtime identity of a single-keyspace
//! node.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

use tidb_config::config_tree::config::get_global_keyspace_name;
use tidb_config_deploymode as deploymode;
use tidb_config_kerneltype as kerneltype;
use tidb_mysql::errors::FormatArg;
use tidb_terror::TerrorError;
use tidb_errno::errcode;
use tidb_mysql::errors::ErrMessage;
use tidb_log::{Field, Value};

use tidb_dbterror::CLASS_DDL;

/// Go `System`: the SYSTEM keyspace name.
pub const SYSTEM: &str = "SYSTEM";

/// Go's private `tidbKeyspaceEtcdPathPrefix`.
const TIDB_KEYSPACE_ETCD_PATH_PREFIX: &str = "/keyspaces/tidb/";

/// The two facts Go reads off `tikv.Codec`.
pub trait KeyspaceCodec {
    /// Whether the codec speaks API v1 (Go `GetAPIVersion() == APIVersion_V1`).
    fn is_api_v1(&self) -> bool;
    /// Go `GetKeyspaceID`.
    fn keyspace_id(&self) -> u32;
}

/// Go `CodecV1`: the API-v1 codec, which has no keyspace.
#[derive(Clone, Copy, Debug, Default)]
pub struct CodecV1;

impl KeyspaceCodec for CodecV1 {
    fn is_api_v1(&self) -> bool {
        true
    }

    fn keyspace_id(&self) -> u32 {
        // Go `codecV1.GetKeyspaceID`: `apicodec.NullspaceID`.
        u32::MAX
    }
}

/// Go `MakeKeyspaceEtcdNamespace`: empty for API v1, the keyspace path
/// otherwise.
pub fn make_keyspace_etcd_namespace(codec: &dyn KeyspaceCodec) -> String {
    if codec.is_api_v1() {
        return String::new();
    }
    format!("{TIDB_KEYSPACE_ETCD_PATH_PREFIX}{}", codec.keyspace_id())
}

/// Go `MakeKeyspaceEtcdNamespaceSlash`: the same path with a trailing slash.
pub fn make_keyspace_etcd_namespace_slash(codec: &dyn KeyspaceCodec) -> String {
    if codec.is_api_v1() {
        return String::new();
    }
    format!("{TIDB_KEYSPACE_ETCD_PATH_PREFIX}{}/", codec.keyspace_id())
}

/// Go `GetKeyspaceNameBySettings`.
pub fn get_keyspace_name_by_settings() -> String {
    get_global_keyspace_name()
}

/// Go's `keyspaceNameBytes` + `genKeyspaceNameOnce` pair.
static KEYSPACE_NAME_BYTES: OnceLock<Option<Vec<u8>>> = OnceLock::new();

/// Go `GetKeyspaceNameBytesBySettings`: the keyspace name as bytes, computed
/// once; `None` (Go's nil slice) on a classic kernel, where the once-guard
/// stores nothing.
pub fn get_keyspace_name_bytes_by_settings() -> Option<&'static [u8]> {
    KEYSPACE_NAME_BYTES
        .get_or_init(|| {
            if !kerneltype::is_next_gen() {
                return None;
            }
            Some(get_global_keyspace_name().into_bytes())
        })
        .as_deref()
}

/// Go `IsKeyspaceNameEmpty`.
pub fn is_keyspace_name_empty(keyspace_name: &str) -> bool {
    keyspace_name.is_empty()
}

/// The log field Go's `WrapZapcoreWithKeyspace` stamps onto every record:
/// `keyspaceName`, present only when the name is set.
pub fn keyspace_name_log_field() -> Option<Field> {
    let keyspace_name = get_keyspace_name_by_settings();
    if is_keyspace_name_empty(&keyspace_name) {
        return None;
    }
    Some(Field::new("keyspaceName", Value::Str(keyspace_name)))
}

/// Go `pd.APIContext`, as [`build_api_context`] constructs it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApiContext {
    /// Go `pd.NewAPIContextV1()`: the default keyspace.
    V1,
    /// Go `pd.NewAPIContextV2(name)`: scoped to one keyspace.
    V2(String),
}

/// Go `BuildAPIContext`.
pub fn build_api_context(keyspace_name: &str) -> ApiContext {
    if keyspace_name.is_empty() {
        ApiContext::V1
    } else {
        ApiContext::V2(keyspace_name.to_owned())
    }
}

/// Go `exeerrors.ErrUserNameNeedPrefix` (class DDL, `mysql.ErrUsername`).
fn err_user_name_need_prefix() -> TerrorError {
    CLASS_DDL.new_std_err(
        errcode::ErrUsername,
        ErrMessage {
            raw: "User name must start with `%s.` (use `%s.%s` instead)",
            redact_arg_pos: &[],
        },
    )
}

/// Go `UsernamePolicy`, with `GetUsernamePolicy` deciding which rules apply.
///
/// Go models this as an interface with two implementations chosen by
/// deployment mode; both are stateless beyond the prefix, so one enum carries
/// them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UsernamePolicy {
    /// Go's `defaultUsernamePolicy`: everything is permitted untouched.
    Default,
    /// Go's `prefixPolicy`: usernames carry a `keyspace.` prefix. An empty
    /// prefix keeps the policy permissive — bootstrap may run before the
    /// keyspace name is configured.
    Prefix {
        /// The keyspace prefix, without the trailing dot.
        user_prefix: String,
    },
}

/// Go `GetUsernamePolicy`: the prefix policy in Starter deployments, the
/// permissive default otherwise. This is the process's configured keyspace's
/// policy; a statement or a login uses its keyspace's
/// [`KeyspaceContext::username_policy`].
pub fn get_username_policy() -> UsernamePolicy {
    username_policy_for(get_keyspace_name_by_settings())
}

/// Go `GetUsernamePolicy` for the keyspace `keyspace_name`. A node serving
/// several keyspaces (design §6.1) applies the prefix policy too: each
/// tenant's accounts carry its keyspace's prefix, as in Starter.
fn username_policy_for(keyspace_name: String) -> UsernamePolicy {
    if deploymode::is_starter() || is_multi_keyspace_mode() {
        UsernamePolicy::Prefix {
            user_prefix: keyspace_name,
        }
    } else {
        UsernamePolicy::Default
    }
}

/// Whether this process serves more than one keyspace.
static MULTI_KEYSPACE_MODE: AtomicBool = AtomicBool::new(false);

/// Marks the process as serving more than one keyspace (design §5.3),
/// before its first keyspace runtime opens.
pub fn set_multi_keyspace_mode(enabled: bool) {
    MULTI_KEYSPACE_MODE.store(enabled, Ordering::Release);
}

/// Whether this process serves more than one keyspace, so the process's
/// configured keyspace is not every statement's keyspace.
pub fn is_multi_keyspace_mode() -> bool {
    MULTI_KEYSPACE_MODE.load(Ordering::Acquire)
}

/// The keyspace one runtime serves (design §5.2, §6.1): its name, its codec
/// ID, the name bytes its requests' resource-group tags carry, and the
/// username policy its account statements and logins apply. A session reads
/// it from its domain, a background job from the runtime that runs it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyspaceContext {
    name: String,
    id: Option<u32>,
    /// Go `GetKeyspaceNameBytesBySettings` for this keyspace.
    name_bytes: Option<Vec<u8>>,
}

impl KeyspaceContext {
    /// The identity of keyspace `name`, whose store codec has keyspace ID
    /// `id` (`None` for an API-v1 store).
    pub fn new(name: impl Into<String>, id: Option<u32>) -> Self {
        let name = name.into();
        // Go's once-guard stores nothing on a classic kernel.
        let name_bytes = kerneltype::is_next_gen().then(|| name.clone().into_bytes());
        Self { name, id, name_bytes }
    }

    /// The process's configured keyspace (Go `GetKeyspaceNameBySettings`):
    /// the identity of a single-keyspace node.
    pub fn from_settings() -> Self {
        Self::new(get_keyspace_name_by_settings(), None)
    }

    /// What a tenant-level path reads when its session or job carries no
    /// runtime identity (a session with no domain, a test stack that names
    /// no keyspace): the process's configured keyspace, which is the only
    /// keyspace of a single-keyspace node. On a node serving several
    /// keyspaces every runtime binds its own identity, so reaching this
    /// there is a bug.
    pub fn unbound() -> Self {
        debug_assert!(
            !is_multi_keyspace_mode(),
            "a tenant-level path read the process keyspace on a multi-keyspace node"
        );
        Self::from_settings()
    }

    /// Go `GetKeyspaceNameBySettings` for this keyspace.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Go `GetCodec().GetKeyspaceID()`, when the runtime's store has one.
    pub fn id(&self) -> Option<u32> {
        self.id
    }

    /// Go `GetKeyspaceNameBytesBySettings` for this keyspace: `None` on a
    /// classic kernel.
    pub fn name_bytes(&self) -> Option<&[u8]> {
        self.name_bytes.as_deref()
    }

    /// Go `GetUsernamePolicy` for this keyspace.
    pub fn username_policy(&self) -> UsernamePolicy {
        username_policy_for(self.name.clone())
    }
}

impl UsernamePolicy {
    /// Go `ValidateUsername`: under a non-empty prefix, the name must start
    /// with `prefix.`.
    pub fn validate_username(&self, username: &str) -> Result<(), TerrorError> {
        let Self::Prefix { user_prefix } = self else {
            return Ok(());
        };
        if user_prefix.is_empty() || username.starts_with(&format!("{user_prefix}.")) {
            return Ok(());
        }
        Err(err_user_name_need_prefix().fast_gen(
            "User name must start with `%s.` (use `%s.%s` instead)",
            &[
                FormatArg::from(user_prefix.as_str()),
                FormatArg::from(user_prefix.as_str()),
                FormatArg::from(username),
            ],
        ))
    }

    /// Go `ValidateUsernameFormat`: a prefixed name has exactly one dot.
    pub fn validate_username_format(&self, username: &str) -> bool {
        match self {
            Self::Default => true,
            Self::Prefix { .. } => username.matches('.').count() == 1,
        }
    }

    /// Go `GetUsernameVariants`: the prefixed spelling of an unprefixed name,
    /// or nothing when the name already carries the prefix.
    pub fn get_username_variants(&self, username: &str) -> Vec<String> {
        let Self::Prefix { user_prefix } = self else {
            return Vec::new();
        };
        if user_prefix.is_empty() || username.starts_with(&format!("{user_prefix}.")) {
            return Vec::new();
        }
        vec![format!("{user_prefix}.{username}")]
    }

    /// Go `GetOriginalUsername`: strips the prefix, or answers empty when the
    /// policy does not transform this input.
    pub fn get_original_username(&self, username: &str) -> String {
        let Self::Prefix { user_prefix } = self else {
            return String::new();
        };
        if user_prefix.is_empty() {
            return String::new();
        }
        username
            .strip_prefix(&format!("{user_prefix}."))
            .map_or_else(String::new, ToOwned::to_owned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeCodec {
        v1: bool,
        keyspace_id: u32,
    }

    impl KeyspaceCodec for FakeCodec {
        fn is_api_v1(&self) -> bool {
            self.v1
        }

        fn keyspace_id(&self) -> u32 {
            self.keyspace_id
        }
    }

    // Go `MakeKeyspaceEtcdNamespace`/`Slash`: v1 has no namespace, v2 scopes
    // by keyspace ID.
    #[test]
    fn etcd_namespaces_scope_by_keyspace_id() {
        assert_eq!(make_keyspace_etcd_namespace(&CodecV1), "");
        assert_eq!(make_keyspace_etcd_namespace_slash(&CodecV1), "");

        let v2 = FakeCodec {
            v1: false,
            keyspace_id: 42,
        };
        assert_eq!(make_keyspace_etcd_namespace(&v2), "/keyspaces/tidb/42");
        assert_eq!(
            make_keyspace_etcd_namespace_slash(&v2),
            "/keyspaces/tidb/42/"
        );
    }

    // Go `TestNoKeyspaceNameSet`'s name-side assertions: with no keyspace
    // configured the name is empty and no log field is produced.
    #[test]
    fn an_unset_keyspace_name_is_empty() {
        let name = get_keyspace_name_by_settings();
        assert!(is_keyspace_name_empty(&name));
        assert!(keyspace_name_log_field().is_none());
        // On a classic kernel the bytes are Go's nil slice.
        if !kerneltype::is_next_gen() {
            assert!(get_keyspace_name_bytes_by_settings().is_none());
        }
    }

    // Go `BuildAPIContext`.
    #[test]
    fn the_api_context_follows_the_keyspace_name() {
        assert_eq!(build_api_context(""), ApiContext::V1);
        assert_eq!(build_api_context("ks"), ApiContext::V2("ks".to_owned()));
    }

    // Go `TestUsernamePolicy`'s default-policy half: everything is permitted
    // and nothing is transformed.
    #[test]
    fn the_default_policy_is_permissive() {
        let policy = UsernamePolicy::Default;
        assert!(policy.validate_username("user").is_ok());
        assert!(policy.validate_username_format("a.b.c"));
        assert!(policy.get_username_variants("user").is_empty());
        assert_eq!(policy.get_original_username("ks.user"), "");
    }

    // Go `TestUsernamePolicy`'s prefix-policy half.
    #[test]
    fn the_prefix_policy_qualifies_usernames() {
        let policy = UsernamePolicy::Prefix {
            user_prefix: "ks".to_owned(),
        };

        assert!(policy.validate_username("ks.user").is_ok());
        assert!(policy.validate_username_format("other.user"));
        assert!(!policy.validate_username_format("other.user.extra"));

        let error = policy.validate_username("user").unwrap_err();
        // The rendered form carries the identity: class ddl, code 1468
        // (mysql.ErrUsername), and the formatted message.
        assert_eq!(
            error.to_string(),
            "[ddl:1468]User name must start with `ks.` (use `ks.user` instead)"
        );

        assert_eq!(policy.get_username_variants("user"), vec!["ks.user".to_owned()]);
        assert!(policy.get_username_variants("ks.user").is_empty());
        assert_eq!(
            policy.get_username_variants("other.user"),
            vec!["ks.other.user".to_owned()]
        );
        assert_eq!(
            policy.get_username_variants("other.user.extra"),
            vec!["ks.other.user.extra".to_owned()]
        );
        assert_eq!(policy.get_original_username("ks.user"), "user");
        assert_eq!(policy.get_original_username("other.user"), "");
    }

    // An empty prefix keeps the prefix policy permissive, which is how
    // bootstrap runs before the keyspace name is configured.
    #[test]
    fn an_empty_prefix_keeps_the_policy_permissive() {
        let policy = UsernamePolicy::Prefix {
            user_prefix: String::new(),
        };
        assert!(policy.validate_username("user").is_ok());
        assert!(policy.get_username_variants("user").is_empty());
        assert_eq!(policy.get_original_username("user"), "");
    }
}
