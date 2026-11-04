# Operating the generic SQL token receiver

This document accompanies [the security design](2026-11-03-sql-token-identity.md), [the execution plan](2026-11-03-sql-token-identity-plan.md), [Issue #4](https://github.com/siddontang/tidb/issues/4), and [PR #3](https://github.com/siddontang/tidb/pull/3). The opt-in receiver has passed local Ready verification on TiDB X baseline `8935853b36c764a15ab7b6d5547446577f99844b`; this document does not claim a deployed fleet rollout.

## Responsibilities and trust

The operator chooses an opaque database audience, provisions a separate Ed25519 policy authority, manages native SQL accounts and grants, and distributes complete signed policy responses. A broker authenticates external identities and issues the fixed twelve-claim RS256 SQL token. TiDB verifies tokens, binds them to its actual storage cluster/codec/keyspace and exact native account, and enforces revocable connection lifetime. Neither service copies an identity-provider password into TiDB.

Native SQL administration remains authoritative. `desired_grant_revision == applied_grant_revision` is the policy authority's acknowledgement that it reconciled the intended native grants; it is not an independent native receipt or a signed cap on the grants currently stored in TiDB. A trusted native administrator can change grants for future logins under a still-enabled binding. An observed native account, TLS requirement, plugin, grant or role change invalidates already-held token sessions. To deny future login as well, disable/remove the binding or advance its authorization revisions. A stronger independent native-grant attestation protocol is outside v1.

Provision token accounts in a distinct namespace. Once a receiver has observed a managed username, it retains that classification across policy expiry, binding removal and restart: switching its native plugin to password does not enable fallback. Direct updates to privilege tables are not immediate until TiDB's privilege cache observes them. An administrative change that happens and is reverted entirely between cache loads is not observable by the receiver. Each published cache change receives a separate generation, so changes and reversions observed between guard polls cannot resurrect old sessions.

The operator owns the receiver filesystem and pinned authority. Durable state must survive process/container replacement and must not be deleted or restored to an older backup to recover service. File permissions and a process lock prevent accidental sharing; they do not defend against the trusted host administrator replacing both the receiver and its state. The policy authority must provide current, monotonically progressing authoritative time. The receiver cannot independently establish UTC from a signature.

## Bootstrap

Configure verified server TLS and explicitly enable the receiver:

    [security]
    ssl-cert = "/run/tidb-tls/server.pem"
    ssl-key = "/run/tidb-tls/server-key.pem"

    [security.sql-token]
    enabled = true
    audience = "urn:example:database:orders"
    authority-key = "/run/sql-token/policy-authority.pem"
    policy-file = "/run/sql-token/response.json"
    state-dir = "/var/lib/tidb/sql-token-state"

The authority file contains exactly one PEM `PUBLIC KEY` containing a PKIX Ed25519 public key. It is separate from the RSA keys used to sign SQL tokens. Startup rejects missing/invalid trust configuration, corrupt durable state, target/authority fingerprint changes, shared state locks, reserved `SYSTEM`, missing TLS and skip-grant-table. The receiver supports actual API-v1 cluster scope and API-v2 keyspace scope, including generic DEFAULT ID zero; exposing a keyspace remains an explicit operator decision. Numeric keyspace IDs are not globally unique: the actual storage cluster ID participates in every target comparison.

The state directory is created with mode 0700 or must already have that mode. Receiver files are mode 0600, atomically replaced and fsynced. The directory contains `runtime.lock`, `watermark.json`, `challenge.json`, and `status.json`. A missing response is allowed at startup, but token authentication remains closed. Ordinary users and the legacy `tidb_auth_token` profile keep their existing authentication contract.

Provision native users and grants through an administrative SQL connection, then verify the intended privileges before publishing an enabled binding:

    CREATE USER 'ext_orders_reader'@'%' IDENTIFIED WITH 'tidb_sql_token_v1';
    GRANT SELECT ON orders.* TO 'ext_orders_reader'@'%';
    SHOW GRANTS FOR 'ext_orders_reader'@'%';

The new profile accepts only an empty native authentication string. The SQL token is supplied at connection time, never stored as the account password. Token users cannot change their own authentication mode or password. Other native privileges remain governed by explicit SQL grants.

## Policy transport and freshness

The exact Go wire definitions are in `pkg/privilege/privileges/sql_token_runtime_wire.go`. All fields are required. JSON rejects duplicate keys (including escaped equivalents), unknown fields, nulls, missing fields, trailing JSON and noncanonical numbers. Revision/cluster IDs are positive canonical uint64 decimal **strings**; sequence is also a canonical positive decimal string. Times are nonnegative integer Unix seconds. The target has `audience`, `storage_cluster_id`, `storage_scope` (`cluster` or `keyspace`), and `keyspace_id` (uint32 in 0..0xFFFFFF). Cluster scope requires canonical keyspace ID zero; API-v2 DEFAULT/zero is a distinct keyspace scope.

The operator reads `challenge.json`. It contains `version: 1`, a random per-process `incarnation`, a random `nonce`, `sequence`, the actual `target`, and `minimum_issued_at`. The minimum is a lower bound, not a time source. If the authority's current time is below it, wait for time to catch up; do not fabricate a future timestamp. The challenge expires ten seconds after creation and rotates after acceptance or expiry.

For each fresh challenge, build a response with these fields:

    {
      "version": 1,
      "incarnation": "<challenge incarnation>",
      "nonce": "<challenge nonce>",
      "sequence": "<challenge sequence>",
      "target": { "audience": "urn:example:database:orders", "storage_cluster_id": "101", "storage_scope": "keyspace", "keyspace_id": 42 },
      "issued_at": 1793682000,
      "valid_until": 1793682030,
      "policy": {
        "target": { "audience": "urn:example:database:orders", "storage_cluster_id": "101", "storage_scope": "keyspace", "keyspace_id": 42 },
        "issuer": "https://identity.example.invalid/sql",
        "revision": "7",
        "valid_from": 1793681990,
        "valid_until": 1793682300,
        "max_token_lifetime_seconds": 300,
        "max_session_duration_seconds": 3600,
        "keys": { "rsa-2026-01": "<unpadded base64url DER PKIX RSA public key>" },
        "bindings": {
          "orders-reader-1": {
            "principal_id": "principal-1",
            "native_username": "ext_orders_reader",
            "native_host": "%",
            "binding_revision": "3",
            "policy_revision": "9",
            "enabled": true,
            "expires_at": 1793685600,
            "desired_grant_revision": "4",
            "applied_grant_revision": "4"
          }
        }
      }
    }

The example timestamp/target values are illustrative and must be replaced from current authority state and the actual challenge. A response cannot outlive its underlying policy. The authority signs the exact bytes `tidb-sql-policy-v1\n` followed by the UTF-8 response JSON with Ed25519, then atomically replaces `policy-file` with:

    { "payload": "<unpadded base64url response bytes>", "signature": "<unpadded base64url signature>" }

There is no network request in SQL verification. A local poll reads responses every 100ms. Respond promptly and renew well before expiry (for example, every second with a thirty-second response lifetime). The maximum lease is thirty seconds measured from challenge creation, never from reading a cached response. Signed integer time has one second of uncertainty, plus transport uncertainty. The runtime maintains monotonic lower and upper trusted-time bounds: lower bounds gate `iat`/`nbf`/policy not-before, and upper bounds gate token/binding/policy expiry. Conservative bounds can reject a very short response lifetime; use sufficient margin for delivery latency. Expired guards are terminal before a new lease is installed, even when no command arrived during the expiry interval.

A repeated response cannot renew a lease. Restart always creates a new process incarnation and nonce; restored policy files do not establish readiness. A fresh response from the pinned authority is required, subject to persisted revision/time/key/binding fences. Malformed or rejected responses preserve the last accepted policy only until its existing lease expires. Durable write failure closes token admission and revokes guards.

V1 allows one issuer in a complete snapshot, at most 64 active RSA keys and 4096 bindings, with at most 4096 retained historical keys/bindings/usernames per receiver. Envelope, decoded response, and durable files are each bounded by 1MiB. RSA keys are 2048–8192 bits. Token lifetime is at most fifteen minutes and session duration at most twenty-four hours. Capacity exhaustion fails closed and needs an explicitly designed state migration; deleting watermarks is not migration. One native username can appear in only one binding in a snapshot.

## Revision rules and acknowledgement

A lower snapshot revision is rejected. The same revision may receive fresh challenge attestations only with identical policy content and within the original policy expiry. A new policy body requires a higher snapshot revision. Content identity is the lowercase hexadecimal SHA-256 of `json.Marshal(SQLTokenPolicyDocument)` using the published Go field order and Go's sorted string map keys; it is **not** the hash of the envelope or arbitrary input JSON formatting. Non-Go authorities may compute the equivalent canonical typed document or compare the returned digest to an independently prepared expected value. Signature verification always covers the original exact response bytes.

Binding identity and authorization changes must obey the retained binding/policy revision rules. A changed binding body must advance at least one of `binding_revision` or `policy_revision`; neither may decrease. Advancing both is a simple conservative operator convention. Removing or disabling a binding leaves a tombstone, so restoration requires both `binding_revision` and `policy_revision` strictly greater than their retained values. The immutable external principal is `(issuer, principal_id)`, not an email address or native username. Change authorization through the broker/operator workflow, not by reusing a removed binding generation.

An `(issuer, kid)` is permanently bound to one public-key fingerprint. Removing a key terminally retires that key ID and closes its held sessions in v1. Normal rotation therefore overlaps a fresh key ID before removal; reintroduction uses another new ID. The pinned Ed25519 authority itself is also persisted by fingerprint; rotation requires an explicit state/trust migration protocol and is not a hot configuration edit.

`status.json` contains `version`, `target`, `incarnation`, `accepted_nonce`, `accepted_sequence`, `policy_digest`, `ready`, `revision`, `policy_valid_until`, `lease_valid_until`, and `last_error`. The last-error vocabulary is empty, `awaiting_policy`, `policy_unavailable`, or `closed`. Status contains no JWT or private key. An operator acknowledges installation only when target/incarnation, consumed challenge, expected revision/content, and readiness all match. Status is an observation, not a transferable credential or proof that a disconnected process is still alive; poll the actual receiver and respect the lease. `ready` describes policy freshness, not successful native grants for every binding.

## Client identity and session behavior

The SQL JWT header is exactly `alg: RS256`, `typ: tidb-sql+jwt`, and `kid`. Claims are exactly `ver` (number 1), `iss`, `aud`, `sub`, `principal_id`, `binding_id`, `binding_revision`, `policy_revision`, `iat`, `nbf`, `exp`, and `jti`. `sub` is the exact native SQL username; revision claims are strings. No email, group, role, product tier or platform-specific hierarchy is interpreted by TiDB. Generic OIDC ID/access tokens are not accepted as SQL tokens.

Connect using a MySQL-compatible client supporting `mysql_clear_password`, with server certificate and hostname verification. TiDB rejects plaintext before requesting this bearer token, even when ordinary password users do not globally require TLS. Issue the token shortly before connecting. Token expiry limits new authentication; an established session can outlive that token only within its original maximum session duration, live policy lease and binding expiry.

For immediate login, correlate the current acknowledgment to the exact signed response and use that response's `issued_at` as a conservative token `iat` and `nbf`. The receiver's trusted lower time is at least this acknowledged value. Setting those claims to the broker's newer wall-clock second can temporarily fail the receiver's not-before check because delivery and integer-second uncertainty are deliberately not treated as trusted elapsed time. Use a fresh acknowledgment, verify the current policy and binding, and cap `exp` by both the accepted lease and `iat + max_token_lifetime_seconds`. If there is insufficient remaining lifetime, renew policy and wait for its acknowledgment before issuing; never fabricate authority time, extend expiry to compensate, or weaken the receiver's not-before check. An acknowledgment authorizes no native grants on its own.

RESET CONNECTION clears ordinary session state but preserves the exact original native user/host and original guard/deadline. A competing more-specific host account cannot acquire the reset connection. Every token-reset failure is terminal. CHANGE USER stages a candidate, checks database selection/audit hooks, and atomically replaces authorization; ordinary authentication/database/audit failure preserves the original valid identity and deadline. Revocation of an already-staged candidate can terminalize the physical connection instead of falling back to its old identity. A delayed callback for an old guard cannot close a successfully replaced identity. Failed reauthentication does not revive an expired old guard.

Migration credential issuance and acceptance are disabled for managed token identities. Session-state JSON cannot install or export trusted token identity/guard fields. Direct/restricted statement execution, prepare and server command dispatch check the guard; pooled internal work retains the same guard and actual storage target. Native authorization generation changes terminate held sessions, including changes reverted after the privilege cache observed them.

Revocation marks guards terminal before subsequent admission can succeed, then cancels dispatch and closes idle/active transports through bounded callbacks. Session and transaction cleanup remain owned by the connection execution path. Closing a socket cannot undo a committed transaction or an already accepted durable background job. Native cancellation and responsive local filesystem/callback scheduling are operational prerequisites, not an effect-level transaction fence.

## Rollout and validation

Route token clients only to receivers acknowledging this profile, actual target, current authority/key policy, and fresh readiness. Do not fall back to legacy JWT or password after a new-profile failure. Revoke/withdraw bindings, drain token sessions, and reconcile only owned users before disabling the receiver. Keep its durable state for re-enablement.

The implementation has repeatable mock-storage tests using actual verified TLS/MySQL connections, a stock Go MySQL driver, literal reset/change-user packets, and independent synthetic policy/token signatures. The execution plan records exact test results as they complete. A deployed TiDB X/TiKV/keyspace acceptance run, an external stock MySQL CLI oracle, fleet rolling-upgrade acceptance and production load/telemetry validation remain separate rollout gates.
