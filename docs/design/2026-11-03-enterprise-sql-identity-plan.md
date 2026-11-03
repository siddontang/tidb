# Implement Native Enterprise SQL Identity

This ExecPlan is a living document maintained under `PLANS.md`. It accompanies `docs/design/2026-11-03-enterprise-sql-identity.md`. Execution ownership and discussion live in the Enterprise execution Issue; this plan records technical dependencies and evidence, not a second team assignment ledger.

## Purpose / Big Picture


A person or workload authenticated by Cloud should connect to its authorized TiDB Instance with a short-lived token while ordinary MySQL SQL grants still limit data access. A token for another Instance must fail. Removing authorization must stop new commands and close affected sessions without resetting unrelated users' credentials. This requires native token validation plus connection lifetime handling, not only an additional JWT field.

The present branch includes the design/source audit and an in-progress stateless-verifier/legacy-wire increment. No new server authentication path is enabled. Complete the implementation milestones below before claiming this capability is available. Begin from pinned TiDB X revision `30d6b0e7e5a993e0b15a32e923a0a576ad70aa8e`; the existing Cloud design calls this kernel capability K07. The runtime demo uses this pinned code and must remain untouched by experiments.

## Progress


- [x] Created isolated local branch `feature/enterprise-sql-identity` from the pinned kernel; preserved the detached build-source checkout.
- [x] Audited authentication, trusted keyspace identity, reset/change-user, migration-token bypass and native cancellation paths.
- [x] Written the proposed strict token, policy, session and rollout contract in the companion design.
- [x] Repository preparation and existing-token baseline tests passed; generated Bazel/dependency metadata is unchanged.
- [x] Independent design review completed with all findings resolved; final local links, whitespace and ExecPlan structure checks passed (documentation-only Ready profile).
- [x] Verified the repeatable native MySQL/TLS legacy-token fixture with owned mock storage and synthetic credentials on the `nextgen` build; this is not deployed TiKV evidence.
- [ ] Run a separate deployed TiDB X/TiKV/keyspace experiment with an external stock MySQL client before fleet integration.
- [x] Verified the strict stateless v1 verifier and secret-free identity value, including API-v2 keyspace-bound regressions; no runtime caller is installed.
- [x] Final independent review and combined-source wire verification passed; the signed local implementation checkpoint and claim release are recorded in the execution Issue.
- [ ] Implement and test the bounded policy engine, trusted storage/account adapter, restart freshness and anti-rollback handling.
- [ ] Implement and test typed identity publication, connection activation and terminal session guard.
- [ ] Implement and test reset/change-user/migration protections and cancellation/cleanup behavior.
- [ ] Integrate Regional issuance and fleet snapshot distribution, then validate mixed versions and outage bounds before enablement.

## Surprises & Discoveries


`handleResetConnection` reconstructs TiDBContext and calls `AuthWithoutVerification`, so a field placed only in the old session would disappear. `handleChangeUser` restores the old session on authentication failure. Therefore guard replacement must be transactional with authentication and use a generation counter to ignore obsolete callbacks.

`onConn` authenticates before `registerConn` adds the connection to `server.clients`. A revocation scan can miss a connection in that interval unless policy publication and guard activation synchronize. Dispatch clears the query kill flag, so the flag alone cannot represent permanent authorization removal.

A matched native user can authenticate with `tidb_session_token` before the normal stored-plugin branch. SHOW SESSION_STATES can issue that credential, while SET SESSION_STATES consumes caller-controlled JSON. The first Cloud-token implementation must reject migration credential issuance and acceptance for managed Cloud users and never deserialize trusted authentication from user JSON.

The process's configured keyspace name and its actual storage codec identify a keyspace, not a stable Cloud Instance; numeric keyspace IDs also repeat across storage clusters. An operator-provided Instance/storage-cluster/keyspace binding must be validated against `kv.Storage.GetClusterID()` and the actual codec. A client-supplied routing hint does not provide this trust.

The native liveness checker excludes noninterruptible transaction finalization and several long-running operation classes. Connection revocation is not an effect-level write fence and cannot undo a committed transaction or accepted durable job.

Keyspace ID zero is DEFAULT; SYSTEM is identified by its trusted name/classification and can have a positive ID. A numeric range check cannot enforce SYSTEM exclusion. The future adapter must perform this check before invoking the stateless verifier.

## Decision Log


- Decision: preserve legacy `tidb_auth_token` and introduce an opt-in `tidb_cloud_token_v1` server profile.
  Rationale: changing the old profile's claims/identity requirements would break existing clients and still would not provide session enforcement.
  Date/Author: 2026-11-03, coordinator `/root`.
- Decision: separate verified identity data from the connection's revocable lifetime guard.
  Rationale: reset replaces session state; change-user has rollback semantics; timers must address an exact connection incarnation and auth generation.
  Date/Author: 2026-11-03, coordinator `/root`, with read-only session audit.
- Decision: initially deny migration for Cloud-token users and expose no trusted auth fields in SessionStates JSON.
  Rationale: the existing migration token authenticates independently of the stored plugin and needs a separately reviewed envelope/capability.
  Date/Author: 2026-11-03, coordinator `/root`.
- Decision: define revocation as admission/dispatch cutoff and observed session termination, with native cancellation semantics.
  Rationale: an in-flight irreversible commit or durable background job cannot be rolled back by closing a socket; a stronger guarantee requires storage/commit fencing.
  Date/Author: 2026-11-03, coordinator `/root`.
- Decision: this first branch delivery contains the kernel contract and implementation plan rather than enabling a partially guarded login path.
  Rationale: the discovered reset/migration and activation races are part of the feature's security boundary. Runtime changes belong in the following individually verifiable milestones.
  Date/Author: 2026-11-03, coordinator `/root`.
- Decision: split the first executable increment into a real MySQL/TLS legacy-wire fixture backed by mock storage and an unwired stateless v1 verifier.
  Rationale: this provides repeatable native authentication/grant evidence without mutating retained instances. It does not complete deployed TiKV acceptance, the snapshot engine or held-session enforcement. The standard Go MySQL driver exercises the wire protocol; a separate stock CLI/deployed-instance oracle remains pending.
  Date/Author: 2026-11-03, coordinator `/root`, forked native-identity session.

## Outcomes & Retrospective


The first committed increment contains independently reviewed kernel insertion points, protocol/lifetime contracts and required negative tests. This implementation increment adds a stateless verifier, identity value and actual MySQL/TLS legacy-wire fixture, with passing verification evidence below. The existing production-like local demo continues to use its verified restricted passwords. Neither the legacy wire fixture nor isolated new verifier enables the new profile in a running server.

The native-wire fixture passed: verified TLS, native token login, CRUD, explicit SQL grant denials, expired/wrong-subject/bad-signature/plaintext/hostname negatives, expected legacy wrong-audience acceptance, and teardown checks. Strict-verifier and old claims/JWKS tests passed. Independent review identified an API-v2 numeric keyspace range omission and inaccurate SYSTEM wording; the new regression cases failed before the range fix and passed afterward. SYSTEM rejection remains an explicit trusted-adapter obligation. Required code lint, final independent review and combined-source wire verification passed. All remaining policy/session/Cloud integration milestones stay open.

## Context and Orientation


A JWT is a signed token carrying identity and scope claims. Its audience identifies the database Instance that may accept it. A key set contains public signature-verification keys. A policy snapshot is a versioned, expiring local copy of authoritative Cloud grants. A connection guard binds successful authentication to a particular connection lifetime; an authentication generation distinguishes successive identities on one TCP connection.

Read `AGENTS.md`, `pkg/server/AGENTS.md`, `docs/agents/testing-flow.md`, and the repository verification/test skills before editing. `pkg/privilege/privileges/tidb_auth_token.go` loads legacy keys and verifies signatures. `privileges.go` checks claims/native users. `pkg/privilege/privilege.go` defines the authentication result. `pkg/session/session.go` publishes the authenticated native identity. `pkg/server/conn.go` negotiates plugins, dispatches commands and handles reset/change-user. `pkg/server/server.go` registers/kills connections. `pkg/executor/show.go` and `simple.go` implement session-state export/import. The companion design explains why each is in scope.

The current worktree is `/Users/siddon/eng/tidbcloud/kernel-worktrees/enterprise-sql-identity`; run repository commands there. No private runtime configuration, live Cloud password, existing customer table or signing key is a test fixture. Test keys must be generated for the experiment and destroyed with its owned resources.

## Plan of Work


### Milestone 1: Prove the native wire mechanism in isolation


First use `TestLegacyTokenVerifiedTLS` in `pkg/server/tests/tls`: one owned in-process TiDB server with mock storage, generated TLS certificate/key and test public JWKS, least-privilege token user, OS-assigned loopback SQL/status listeners, and the existing Go MySQL driver using `mysql_clear_password`. Verify certificate/hostname, observe the negotiated cipher, perform CRUD and reject access to another database and `mysql.user`. Wrong subject, expired token, bad signature and plaintext fail; wrong hostname fails certificate verification. The plaintext guard exercised here is global `require_secure_transport`, which rejects before the clear-password auth-switch. Do not infer that per-user REQUIRE SSL alone prevents a secret from being requested over plaintext. A valid token with an unrelated audience succeeds in the old profile: that is a documented gap, not Instance isolation. The fixture closes clients/server/store, removes its owned credential directory and verifies listener teardown and restored global state. No retained credentials or instances are used.

Before fleet integration, separately use an owned disposable deployed TiDB X/TiKV/keyspace and an external stock MySQL client supporting `mysql_clear_password` with certificate and hostname verification. Keep the Cloud password out of the SQL client. Repeat valid and negative attempts, record the actual pinned binary/configuration, and verify resource ownership/cleanup. The mock-storage Go fixture is not a substitute for this deployed-storage gate.

Start with existing `TestAuthTokenClaims` and `TestJWKSImpl` to understand baseline behavior. They validate legacy code only. A separate actual server/client fixture is necessary before calling this milestone complete. Before launching a disposable server, record a scoped resource owner, port allocation, resource budget and durable cleanup manifest in the execution Issue. Verify process/image and native user ownership before deletion.

### Milestone 2: Implement the versioned verifier and policy engine


Create a dependency-light identity type, proposed under `pkg/sessionctx/sessionauth`, and an isolated verifier in `pkg/privilege/privileges`. Use maintained JWS verification already present in go.mod. Implement the exact profile from the companion design, strict bounded parsing, explicit RS256/issuer/key selection, and checks against a trusted descriptor and active binding. Validate all claims on the receiving kernel; issuance alone is not enforcement.

The stateless increment defines `VerifyCloudToken(raw, request, policy, now)` and value-only `sessionauth.Identity`. Request and policy are trusted caller inputs, including the exact native user/host/plugin, TLS/account state and desired/applied grant revision. Snapshot maps/keys must not be mutated concurrently. The function does not verify policy provenance, native database records or SYSTEM classification, and cannot prevent a caller from presenting an old snapshot. It has no runtime caller. Returned session and initial policy-lease deadlines are bounded values, not active timers or revocation enforcement. Tests use deterministic clocks and existing synthetic RSA unit fixtures; wire keys are separately generated and removed.

Implement atomic immutable snapshots with bounded validity, anti-rollback revision handling, identical-content idempotency, per-binding/key retirement and restart watermarks. Keep the transport adapter separate from verification. Reject missing or invalid policy; never fall back to the old plugin. Provide an injectable clock for deterministic boundary tests, while production derives monotonic lease deadlines from validated wall-clock expiries. Do not add Global/KMS requests to the SQL path.

Tests must reject wrong Instance/storage-cluster/keyspace/principal/native user/org/project, including the same numeric keyspace ID in another storage cluster, old/revoked binding and policy revisions, missing/duplicate claims, invalid types and algorithm/key substitution. Cover expiry boundaries, clock skew, maximum token lifetime, malformed refresh, snapshot expiry, revision rollback, same-revision changed content, and removal/reintroduction of an old binding/key, and restart after expiry with a rolled-back wall clock. On restart require a fresh authority attestation bound to a new process incarnation/nonce and trusted time before token admission; persisted time/expiry/revocation watermarks alone do not prove freshness or authorize a new lease. Reject startup/readiness when skip-grant-table is combined with the new Cloud profile; its existing early return must not bypass authentication. Preserve legacy tests. This milestone's isolated verifier is not an enabled login path.

### Milestone 3: Publish identity and enforce connection lifetime


Add the verified immutable identity to `privilege.VerificationInfo`, then publish it in `session.Auth` after every existing native account validation succeeds. Carry a read-only reference in SessionVars for audit; do not serialize it as SQL session state. Add a server-owned connection guard with incarnation and auth generation.

Integrate policy publication and guard registration so a new connection cannot miss a concurrent revocation. Start enforcement before init-connect or other authenticated SQL executes, not only after the connection enters the public registry. Verify policy again at activation. Policy publication marks affected guards terminal under the same lock/order that serializes dispatch admission; cancellation and cleanup run outside the lock. Work admitted before publication remains in flight, with native cancellation limits. Use a central scheduler or registry suitable for many connections, rather than an unbounded timer goroutine for every token refresh. Check terminal guard state at command dispatch and `session.ExecuteStmt`/`PrepareStmt` entry, including each statement inside a multi-statement request, through a dependency-light `sessionauth.Authorization.Check()` interface. A Cloud identity without its guard fails closed; `InRestrictedSQL` does not bypass it. Prepared execution delegates to ExecuteStmt, but cursor FETCH directly consumes a result set and requires the dispatch/streaming cancellation paths. Cross-keyspace/internal helpers must neither discard a Cloud guard nor reuse it against another store; explicitly authorized internal work remains separate. Cancel active/idle connections through existing shutdown/deadline/cancel mechanisms. Keep the normal connection owner responsible for closing the Session and rolling back uncommitted work. Do not hold policy or server registry locks during cleanup.

A targeted server integration test must hold one idle connection, one running cancellable query and one uncommitted transaction, revoke that exact binding, and observe new login/dispatch denial, socket exit and transaction cleanup. Other users remain connected. Race tests must arrange revocation between verification and registration, concurrent key retirement, and an expired timer arriving after successful user replacement. Record cases with noninterruptible commits separately; do not assert rollback of acknowledged writes.

### Milestone 4: Close protocol escape paths


RESET CONNECTION preserves the old guard and original maximum deadline. Implement a typed restore path for Cloud users instead of `AuthWithoutVerification`: verify the exact original native `(user, host)`, expected plugin, current lock/account state, binding/native-grant readiness and storage scope, with native-record/policy generation revalidation before activation. A new host-specific record must not acquire different grants through reset. Fail closed if the identity cannot be safely restored. CHANGE USER stages a replacement guard and atomically swaps generations only after successful authentication. Failed attempts preserve the old guard's original expiry. Every path must retain terminal revocation state.

Block migration token issuance in SHOW SESSION_STATES for Cloud-bound users and reject `AuthTiDBSessionToken` for matched native records using the new profile. SET SESSION_STATES cannot install or change trusted identity, expiry or revisions. Test users cannot change their own auth mode through permitted account-management SQL to escape these requirements. The trusted managed-username classification applies before auth-plugin selection, so a mismatched plugin/host record fails rather than falling back to password authentication. Test this on new connections as well as reset. The grant reconciler must observe privileged native changes as drift.

Tests must exercise reset before/after revocation, native account lock/plugin change or addition of a competing host-specific record, failed/successful change-user in each auth-mode direction, prepared statements/cursors/long-data state, stale callbacks, and attempted migration bypass. A driver-triggered automatic reset must not renew a session. Network errors and client disconnects must not leak guards, sessions or locks.

### Milestone 5: Integrate Cloud issuance and fleet operations


Only after native gates pass, implement Regional token exchange and native-binding reconciliation. Freeze the matching public token profile and generated API contracts. Distribute descriptors, policy/key snapshots and anti-rollback watermarks through the authenticated Agent. Publish keys, collect per-incarnation acknowledgements, activate issuance, overlap and retire. Separate ordinary key retirement from emergency revocation of held sessions. Measure actual propagation/cutoff bounds under Global/Region/Agent outage and restart.

Validate Essential/Premium movement keeps Instance identity, mixed-version receivers reject unsupported profiles, TLS/CA rotation preserves security, and BYOC disconnection expires policy predictably. SQL grants stay distinct from Cloud management roles. Use the same authorization path for SQL Editor and external clients. Full fleet readiness requires these actual-system tests; it is outside the present design increment.

## Concrete Steps


All commands run at the kernel worktree root. Inspect ownership and uncommitted changes first. New source/test files or top-level test functions require Bazel metadata regeneration:

    git status --short
    make bazel_prepare

The inspected privilege package contains no direct failpoint/testfailpoint references. Recheck before tests whenever implementation changes the package. Run the existing baseline:

    rg -n 'failpoint\.|testfailpoint\.' pkg/privilege/privileges
    go test -run '^(TestAuthTokenClaims|TestJWKSImpl)$' -tags=intest,deadlock ./pkg/privilege/privileges

An rg exit status of 1 means no matches, not a failed unit test. Server/session/executor packages do use failpoints; use the repository wrapper for their new targeted tests, substituting the actual test name created by the implementation:

    ./tools/check/failpoint-go-test.sh pkg/server -run '<actual cloud identity test>' -count=1

The wrapper disables failpoints on exit. Use WIP verification while editing. Before a code change is called Ready, run its required scoped unit/integration tests, `make lint`, `git diff --check`, and include the generated Bazel changes. Do not run unrequested broad Bazel lint or RealTiKV suites. For a documentation-only increment, Ready requires design/link/diff review; existing-token baseline tests are supporting evidence, not native feature acceptance.

## Validation and Acceptance


A successful new-profile login must select the intended Instance, immutable principal and native grants. A token issued for another Instance must fail even when both Instances accept the same issuer/key and contain equivalent native users. Tokenless reset, stale migration tokens and user-controlled session-state JSON cannot bypass this check.

A binding/principal/key revoke must deny admission, mark affected guards terminal, stop new dispatch and observe targeted connection cleanup. This remains true for idle clients, streaming results, prepared statements, connection reuse, reset/change-user, policy refresh races and process restart. Preserve committed data; verify uncommitted test rows are absent after cleanup. Acknowledge noninterruptible in-flight outcomes separately rather than claiming a stronger transaction fence.

Test clock and policy expiry with deterministic clocks and include a real-server negative test. Record raw token/password absence from logs, errors, metrics and diagnostic artifacts. Record exact source/binary versions and test-only dependencies. No token is placed in a CLI argument or committed fixture. TLS verification is mandatory in the native client oracle.

## Idempotence and Recovery


A repeated identical snapshot install is safe; a same-revision different body or older revision fails. Persisted revision/time/expiry/revocation watermarks prevent accepted-state rollback, but admission after restart additionally requires fresh trusted authority/time evidence; cached state cannot acquire a new full lease. Failed parsing preserves the previous snapshot only within its existing validity, and no outage renews a lease. Revocation callbacks are idempotent and generation-fenced. Claim terminal connection shutdown for the expected incarnation/generation atomically under the same synchronization as auth replacement, then perform network cancellation outside locks. Test both orderings: replacement wins and stale callback does nothing; shutdown wins and replacement cannot activate. A generation check followed by an unfenced Kill call does not satisfy the contract.

Rollout is opt-in with distinct token users. Stop new issuance, drain or revoke token sessions, and reconcile only feature-owned users/keys before disabling the profile. Do not restore the whole metadata store, mutate retained password users or reuse old policy revisions. Test environments have their own durable cleanup records; unknown outcomes are observed before retry or deletion.

## Artifacts and Notes


The original design-only increment completed branch creation, source audit and the following baseline checks. These are not implementation acceptance for the new profile.

    make bazel_prepare
    # Exit 0; no generated Bazel/dependency metadata diff.
    go test -run '^(TestAuthTokenClaims|TestJWKSImpl)$' -tags=intest,deadlock ./pkg/privilege/privileges
    # ok github.com/pingcap/tidb/pkg/privilege/privileges 3.227s

The test ran on Go 1.25.12, macOS/arm64. No direct failpoint/testfailpoint references were found in the targeted privilege package, so failpoint instrumentation was not needed. No real MySQL token session or fleet policy was exercised. Independent reviewer `/root/sql_access_review` approved the design-only scope after resolving storage-cluster identity, typed reset, direct-session authorization, restart freshness, atomic generation-fenced shutdown, skip-grant-table and managed-username downgrade findings. Reviewed technical-content SHA-256 values: design `625cab0acb2f5b37c9bdcb4dfcbe53cc661e775657a11feed3e078003cdec3b5`; plan before this evidence-only finalization `181b8d3f19f5367c3e33b5a56484cd54f183d047b27d2abde5d50fac221d757a`. Local document links, whitespace and required ExecPlan structure passed. The Ready profile applies only to these documentation changes; `make lint` is not required because no code changed. This review does not authorize runtime enablement.

Second increment, Go 1.25.12/macOS arm64 on the same pinned kernel plus this branch: `make bazel_prepare` passed and generated the new source/dependency entries and test shard metadata. Initial verifier/legacy tests passed in 3.257s. Native-wire command:

    go test -run '^TestLegacyTokenVerifiedTLS$' -tags=intest,deadlock,nextgen -count=1 -timeout=10m ./pkg/server/tests/tls
    # ok github.com/pingcap/tidb/pkg/server/tests/tls 1.419s

The native-wire fixture uses mock storage and a standard Go MySQL client over real loopback TCP/TLS. TLS RSA keys and public JWKS are generated under an owned temporary directory; signing keys stay in process memory. Global JWKS is loaded without a refresh goroutine, and prior JWKS/transport/test-channel state is restored after server teardown. Client closures, server/store helper cleanup, closed SQL/status listeners and deleted credential directory are checked. No standalone deployed binary, TiKV, public endpoint, fleet movement, external CLI or held-session revoke was exercised. Error-surface checks and the strict verifier's fixed sentinels do not constitute captured-log/metrics redaction acceptance. Direct failpoint/testfailpoint references and failpoint BUILD dependencies were absent from both targeted packages; tests run without failpoint instrumentation under the repository's package-based decision rule.

The keyspace-range regression run failed both cases before the fix: the first ID above API-v2 range and the null/API-v1 sentinel were incorrectly accepted. The fix bounds IDs to `[1, 0xFFFFFF]`; zero DEFAULT and named SYSTEM are different concepts. The latter requires the future trusted adapter. Exact regression command:

    go test -run '^TestCloudTokenRejectsUntrustedInputs$/(keyspace_above_API-v2_range|null_keyspace)$' -tags=intest,deadlock -count=1 -timeout=10m ./pkg/privilege/privileges
    # Before fix: FAIL, both cases returned nil instead of the unavailable-policy error.

Final verifier validation includes the new cases and existing legacy compatibility tests:

    go test -run '^(TestCloudToken.*|TestAuthTokenClaims|TestJWKSImpl)$' -tags=intest,deadlock -count=1 -timeout=10m ./pkg/privilege/privileges
    # ok github.com/pingcap/tidb/pkg/privilege/privileges 3.545s
    make lint
    # Exit 0, including repository revive and dashboard checks.

The Ready profile applies to this bounded code increment only. No RealTiKV suite, broad Bazel lint sweep or new-profile native handshake has run. Local evidence logs remain under `/tmp/kernel-identity-{bazel-prepare,verifier-final,wire-final,range-before,lint}.log`; durable commands, observed outcomes and limits are retained in this plan and the private execution Issue. No new module dependency is introduced.

The exact native-wire command above was rerun against the final combined source after the verifier corrections and passed in 1.707s. Final `gofmt -l` produced no output; staged `git diff --check`, document links and ExecPlan structure checks passed. Independent agent `/root/identity_review` approved the bounded increment with no unresolved findings in [the hash-anchored review](https://github.com/siddontang/tidb-cloud-enterprise/issues/21#issuecomment-6597097267). All nine reviewed file hashes are recorded there; only this ExecPlan's evidence/completion text changed afterward. The original pinned build-source checkout remains clean. The kernel checkpoint stays local and does not publish private design context to public TiDB repositories.

## Interfaces and Dependencies


The proposed independent identity package contains immutable value types only. `privilege.VerificationInfo` gains a nullable `AuthenticatedIdentity`; existing plugins leave it nil and keep their behavior. SessionVars receives an immutable reference and a read-only `sessionauth.Authorization` checker after successful authentication; the checker references the shared guard state without importing server code into session. `clientConn` owns a guard that references identity plus terminal state, deadlines and an auth generation. Policy installation and guard activation use one explicit ordering protocol; implementation must document lock order and cleanup ownership.

The first version consumes authenticated local descriptor/policy files installed by the Cell Agent; no new public status endpoint and no per-query network service. The kernel uses existing native SQL grants and maintained cryptography. The control plane retains the authoritative identity/binding/policy state and KMS signing permissions. No Cloud password synchronization, Temporal dependency, or automatic SQL grant follows from this design.

Revision note (2026-11-03): initial plan written after native session, migration and cancellation audit; runtime implementation deliberately remains gated on the listed tests.

Revision note (2026-11-03): finalized baseline evidence and independent design review. All implementation milestones remain visibly pending; no native authentication behavior changed.

Revision note (2026-11-03): added the stateless verifier/value type and isolated native MySQL/TLS legacy fixture. Split mock-wire evidence from deployed TiKV acceptance, fixed keyspace-range/SYSTEM contract wording after independent review, and retained policy/session/Cloud integration as explicit pending milestones.
