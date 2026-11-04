# Implement Native SQL Token Identity

This ExecPlan is a living document maintained under `PLANS.md`. It accompanies `docs/design/2026-11-03-sql-token-identity.md`. Execution ownership and discussion live in [public Issue #4](https://github.com/siddontang/tidb/issues/4), and delivery is [PR #3](https://github.com/siddontang/tidb/pull/3); this plan records technical dependencies and evidence, not a second team assignment ledger.

## Purpose / Big Picture


A person or service authenticated by an external identity provider should connect to its authorized TiDB database with a short-lived token while ordinary MySQL SQL grants still limit data access. A token for another database audience must fail, including when keys and native accounts overlap. Removing authorization must stop new commands and close affected sessions without resetting unrelated users' credentials. This requires native token validation plus connection lifetime handling, not only an additional JWT field.

The current work extends the generic stateless-verifier/legacy-wire foundation into an opt-in native receiver. The opt-in native implementation has passed the local Ready profile, including targeted race and real MySQL/TLS tests with owned mock storage. Deployed TiKV, independent adapter and fleet acceptance remain separate gates. The branch merges pinned TiDB X revision `8935853b36c764a15ab7b6d5547446577f99844b` without rewriting history; the public fork's `nextgen` base advances to the same commit. Existing deployments remain untouched by experiments.

## Progress


- [x] Created an isolated working branch from the pinned kernel; preserved the original build-source checkout.
- [x] Audited authentication, trusted keyspace identity, reset/change-user, migration-token bypass and native cancellation paths.
- [x] Written the proposed strict token, policy, session and rollout contract in the companion design.
- [x] Repository preparation and existing-token baseline tests passed; generated Bazel/dependency metadata is unchanged.
- [x] Independent design review completed with all findings resolved; final local links, whitespace and ExecPlan structure checks passed (documentation-only Ready profile).
- [x] Verified the repeatable native MySQL/TLS legacy-token fixture with owned mock storage and synthetic credentials on the `nextgen` build; this is not deployed TiKV evidence.
- [ ] Run a separate deployed TiDB X/TiKV/keyspace experiment with an external stock MySQL client before fleet integration.
- [x] Verified the initial stateless v1 verifier and secret-free identity value, including API-v2 keyspace-bound regressions; that initial checkpoint had no runtime caller.
- [x] Final independent review and combined-source wire verification passed; the signed local implementation checkpoint and claim release are recorded in the execution Issue.
- [x] Separated token issuance, native user/grant provisioning and public-key distribution from the receiver contract.
- [x] Generalized verifier/value types and named protocol constants; added literal-wire and deployment-scope tests.
- [x] Completed generic-profile targeted verification, code lint and final independent review at commit `8607790`.
- [x] Established public Issue #4 as the feature/execution entry point and documented the external issuer integration contract.
- [x] Completed neutral-example verification and independent public-contract review; signed publication is tracked in Issue #4.
- [x] (2026-11-04) Merged exact upstream baseline `8935853b36c764a15ab7b6d5547446577f99844b` as signed merge `734d0b9`; fast-forwarded fork base and feature branch.
- [x] Frozen the file-based operator policy contract: a pinned Ed25519 authority, per-process challenge, strict signed full snapshot, and monotonic bounded freshness lease.
- [x] (2026-11-04) Completed the native receiver, independent security/source review and Ready checks: targeted ordinary/race suites, Bazel metadata, parser checks, lint and final source/document review. Signed publication is tracked in Issue #4.
- [x] Implemented and tested the bounded policy engine, trusted storage/account adapter, restart freshness and anti-rollback handling; final expanded-suite verification is recorded below.
- [x] Implemented typed identity publication, atomic connection activation and terminal session guards; targeted runtime and server tests passed.
- [x] Implemented and tested reset/change-user/migration protections, cancellation and pessimistic transaction cleanup, including targeted nextgen race runs.
- [ ] Integrate broker/operator issuance and snapshot distribution, then validate mixed versions and outage bounds before enablement; validate product-specific behavior only for the corresponding adapter.

## Surprises & Discoveries


The new rejected-bootstrap path initially started the existing AutoTLS renewal worker too early. A regression using an owned certificate directory and `goleak.IgnoreCurrent` reproduced the orphan worker; moving renewal startup after successful token configuration validation made the targeted server suite pass. This fixes failed startup only, not the preexisting lifetime of AutoTLS renewal after normal server shutdown.

Nextgen customer mock domains load SYSTEM metadata through the cross-keyspace manager, which requires etcd even though SQL data uses mock storage. Native integration fixtures own SYSTEM/customer stores and a one-member embedded etcd with repository-supported hooks; they do not rename SYSTEM or relax production target checks. Direct session SHOW returns a lazy recordset, so rejection assertions must drain and close it to observe the executor error.

`handleResetConnection` reconstructs TiDBContext and calls `AuthWithoutVerification`, so a field placed only in the old session would disappear. `handleChangeUser` restores the old session on authentication failure. Therefore guard replacement must be transactional with authentication and use a generation counter to ignore obsolete callbacks.

`onConn` authenticates before `registerConn` adds the connection to `server.clients`. A revocation scan can miss a connection in that interval unless policy publication and guard activation synchronize. Dispatch clears the query kill flag, so the flag alone cannot represent permanent authorization removal.

A matched native user can authenticate with `tidb_session_token` before the normal stored-plugin branch. SHOW SESSION_STATES can issue that credential, while SET SESSION_STATES consumes caller-controlled JSON. The first SQL-token implementation must reject migration credential issuance and acceptance for managed token users and never deserialize trusted authentication from user JSON.

The process's actual codec identifies its storage scope, not an operator-defined database audience; numeric keyspace IDs also repeat across storage clusters. An operator-provided audience/storage-cluster/scope/keyspace binding must be validated against `kv.Storage.GetClusterID()` and the actual codec. A client-supplied routing hint does not provide this trust.

The native liveness checker excludes noninterruptible transaction finalization and several long-running operation classes. Connection revocation is not an effect-level write fence and cannot undo a committed transaction or accepted durable job.

Keyspace ID zero is DEFAULT; SYSTEM is identified by its trusted name/classification and can have a positive ID. A numeric range check cannot enforce SYSTEM exclusion. The native `SQLTokenTargetForStore` adapter now performs this check before invoking the stateless verifier. Explicit scope distinguishes API-v1 cluster/zero from API-v2 DEFAULT/zero; omitted scope is invalid.

## Decision Log


- Decision: correlate immediate-issuance token `iat`/`nbf` with the exact acknowledged response `issued_at`, and conservatively bound `exp` by the fresh acknowledged lease and maximum token lifetime.
  Rationale: the receiver deliberately keeps lower/upper trusted-time bounds; a broker wall-clock second can be newer than the receiver lower bound after transport delay. Fresh signed time must never be fabricated or the not-before check weakened.
  Date/Author: 2026-11-04, native coordinator.
- Decision: native SQL administrators remain authoritative over SQL grants; signed desired/applied revisions acknowledge provisioning, not a cryptographic grant digest.
  Rationale: every observed relevant native-cache generation change terminalizes existing guards, including observed change-and-revert sequences. A future login may use current explicit native grants while its binding is enabled; revoke future admission through binding policy as well.
  Date/Author: 2026-11-04, native coordinator.
- Decision: use `[security.sql-token]` with explicit `enabled`, `audience`, `authority-key`, `policy-file`, and `state-dir` bootstrap options. No new HTTP endpoint is introduced.
  Rationale: the operator pins an Ed25519 authority separately from RSA SQL token keys and delivers atomic local files. The process publishes a random incarnation and rotating nonce/sequence; a signed response proves fresh authority participation. Challenges expire after ten seconds and leases after at most thirty seconds measured from challenge issuance. Cached signed policies cannot authorize a restarted process.
  Date/Author: 2026-11-04, native coordinator.
- Decision: persist revision/content, issuer-key fingerprints, revocation tombstones and managed usernames; removal of an RSA key revokes its sessions in v1 and permanently retires that issuer/key ID.
  Rationale: explicit terminal key retirement is implementable and testable without silently retaining sessions on removed keys. Operators overlap fresh IDs for ordinary rotation.
  Date/Author: 2026-11-04, native coordinator.
- Decision: separate candidate authentication from connection activation, with atomic runtime `Replace(old, next)` under the connection authentication mutex.
  Rationale: a failed change-user must retain the original guard/deadline; policy publication and replacement need a single ordering point. Policy callbacks execute outside engine locks and terminate only the exact current/pending guard.
  Date/Author: 2026-11-04, native coordinator.
- Decision: use public Issue #4 for feature discussion, execution and delivery links, and make external issuer integration self-contained in this repository.
  Rationale: independently operated systems must be able to implement the same fixed profile without another repository or product-specific terminology. Keep issuer trust provisioning explicit; a token cannot onboard its own issuer.
  Date/Author: 2026-11-03, coordinator `/root`.
- Decision: generalize the unregistered prototype as `tidb_sql_token_v1` / `tidb-sql+jwt`, with twelve fixed claims, opaque audience, issuer-scoped immutable principal and explicit storage-scope enum. Remove organization/project/principal-kind claims from the kernel.
  Rationale: source review of existing control-plane issuance/provisioning confirms that product identity and role mappings belong in adapters. The same native contract should work for hosted and self-managed deployments without importing their account model. The old draft was not enabled; legacy `tidb_auth_token` stays unchanged.
  Date/Author: 2026-11-03, coordinator `/root`, independently reviewed by `/root/identity_review`.
- Decision: use named custom protocol constants plus existing jwx/native constants, and independently literal signed wire fixtures.
  Rationale: protocol strings must have one production definition; tests must detect accidental wire changes rather than merely repeat implementation constants.
  Date/Author: 2026-11-03, coordinator `/root`.
- Decision: preserve legacy `tidb_auth_token` and introduce an opt-in `tidb_sql_token_v1` server profile.
  Rationale: changing the old profile's claims/identity requirements would break existing clients and still would not provide session enforcement.
  Date/Author: 2026-11-03, coordinator `/root`.
- Decision: separate verified identity data from the connection's revocable lifetime guard.
  Rationale: reset replaces session state; change-user has rollback semantics; timers must address an exact connection incarnation and auth generation.
  Date/Author: 2026-11-03, coordinator `/root`, with read-only session audit.
- Decision: initially deny migration for SQL-token users and expose no trusted auth fields in SessionStates JSON.
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


The 2026-11-04 native increment now registers the opt-in plugin and enforces authenticated signed policy, actual-store/native-account boundaries, one-use admission, terminal held-session revocation and reset/change-user/migration protections. All scoped Ready checks passed, including race detection for policy/native epochs, server, real TLS wire and session. The implementation remains disabled by default and has not changed a retained deployment. Native SQL grants are authoritative; policy acknowledgments do not attest an independent grant digest. Uncommitted pessimistic transaction cleanup is tested, while already-committed effects are never promised reversible.

Remaining acceptance concerns are deployed TiKV/stock-CLI and independently configured adapter operation, mixed-version/rolling upgrades, production load and full telemetry-redaction validation. The local command matrix covers prepared execution and common reset/change-user paths; a dedicated full streaming-cursor/long-data and audit-failure fault matrix remains follow-up integration coverage. Public Issue #4 stays open. Historical incremental outcomes below are retained to distinguish source evolution from current evidence.

The historical first committed increment contains independently reviewed kernel insertion points, protocol/lifetime contracts and required negative tests. The initial implementation increment added a stateless verifier, identity value and actual MySQL/TLS legacy-wire fixture, with passing verification evidence below. Neither that legacy fixture nor the initial isolated verifier enabled the new profile in a running server. The current native increment described below adds opt-in runtime wiring.

The previously committed native-wire fixture passed: verified TLS, native token login, CRUD, explicit SQL grant denials, expired/wrong-subject/bad-signature/plaintext/hostname negatives, expected legacy wrong-audience acceptance, and teardown checks. Strict-verifier and old claims/JWKS tests passed. Independent review identified an API-v2 numeric keyspace range omission and inaccurate SYSTEM wording; the new regression cases failed before the range fix and passed afterward. SYSTEM rejection was an explicit trusted-adapter obligation at that checkpoint; the current adapter enforces it from actual storage metadata. Required code lint, final independent review and combined-source wire verification passed. Policy/session/adapter integration milestones were still open at that checkpoint.

The generic-profile revision removes product-specific mandatory claims and naming, adds explicit cluster/keyspace scopes and centralizes wire constants. Generic verifier/legacy compatibility tests and the nextgen mock-wire fixture passed, as did Ready lint and independent review. The reviewed public scope covers independently configured self-managed and hosted adapters. That revision did not enable a native login path or complete session revocation.

## Context and Orientation


The current native implementation adds `pkg/privilege/privileges/sql_token_runtime.go` (admission/guards), `sql_token_runtime_wire.go` (public transport schema/constants), `sql_token_runtime_transport.go` (challenge/poll/status), `sql_token_runtime_storage.go` (strict policy/durable fences), `sql_token_storage.go` (actual-store target) and `sql_token_native.go` (native authorization generations). `pkg/server/sql_token.go` owns runtime and connection guard lifecycle; `pkg/session/sql_token_auth.go` carries typed authority through session/reset/internal execution. The operator guide contains complete file schemas and rollout instructions. No per-query network service or new Go module dependency is required.

A JWT is a signed token carrying identity and scope claims. Its audience identifies the logical database that may accept it. A key set contains public signature-verification keys. A policy snapshot is a versioned, expiring local copy of authoritative authorization bindings. A connection guard binds successful authentication to a particular connection lifetime; an authentication generation distinguishes successive identities on one TCP connection.

Read `AGENTS.md`, `pkg/server/AGENTS.md`, `docs/agents/testing-flow.md`, and the repository verification/test skills before editing. `pkg/privilege/privileges/tidb_auth_token.go` loads legacy keys and verifies signatures. `privileges.go` checks claims/native users. `pkg/privilege/privilege.go` defines the authentication result. `pkg/session/session.go` publishes the authenticated native identity. `pkg/server/conn.go` negotiates plugins, dispatches commands and handles reset/change-user. `pkg/server/server.go` registers/kills connections. `pkg/executor/show.go` and `simple.go` implement session-state export/import. The companion design explains why each is in scope.

Run repository commands from the root of the working checkout. No live runtime configuration, identity-provider password, existing customer table or signing key is a test fixture. Test keys must be generated for the experiment and destroyed with its owned resources.

## Plan of Work


### Milestone 1: Prove the native wire mechanism in isolation


First use `TestLegacyTokenVerifiedTLS` in `pkg/server/tests/tls`: one owned in-process TiDB server with mock storage, generated TLS certificate/key and test public JWKS, least-privilege token user, OS-assigned loopback SQL/status listeners, and the existing Go MySQL driver using `mysql_clear_password`. Verify certificate/hostname, observe the negotiated cipher, perform CRUD and reject access to another database and `mysql.user`. Wrong subject, expired token, bad signature and plaintext fail; wrong hostname fails certificate verification. The plaintext guard exercised here is global `require_secure_transport`, which rejects before the clear-password auth-switch. Do not infer that per-user REQUIRE SSL alone prevents a secret from being requested over plaintext. A valid token with an unrelated audience succeeds in the old profile: that is a documented gap, not audience isolation. The fixture closes clients/server/store, removes its owned credential directory and verifies listener teardown and restored global state. No retained credentials or instances are used.

Before fleet integration, separately use an owned disposable deployed TiDB X/TiKV/keyspace and an external stock MySQL client supporting `mysql_clear_password` with certificate and hostname verification. Keep the identity-provider password out of the SQL client. Repeat valid and negative attempts, record the actual pinned binary/configuration, and verify resource ownership/cleanup. The mock-storage Go fixture is not a substitute for this deployed-storage gate.

Start with existing `TestAuthTokenClaims` and `TestJWKSImpl` to understand baseline behavior. They validate legacy code only. A separate actual server/client fixture is necessary before calling this milestone complete. Before launching a disposable server, record a scoped resource owner, port allocation, resource budget and durable cleanup manifest in the execution Issue. Verify process/image and native user ownership before deletion.

### Milestone 2: Implement the versioned verifier and policy engine


Create a dependency-light identity type, proposed under `pkg/sessionctx/sessionauth`, and an isolated verifier in `pkg/privilege/privileges`. Use maintained JWS verification already present in go.mod. Implement the exact profile from the companion design, strict bounded parsing, explicit RS256/issuer/key selection, and checks against a trusted descriptor and active binding. Validate all claims on the receiving kernel; issuance alone is not enforcement.

The stateless increment defines `VerifySQLToken(raw, request, policy, now)` and value-only `sessionauth.Identity`. Request and policy are trusted caller inputs, including the exact native user/host/plugin, TLS/account state and desired/applied grant revision. Snapshot maps/keys must not be mutated concurrently. The function does not verify policy provenance, native database records or SYSTEM classification, and cannot prevent a caller from presenting an old snapshot. In isolation it does not manage a runtime. The current `SQLTokenRuntime.Authenticate` caller additionally supplies authenticated policy, bounded trusted time and one-use admission. Returned session and initial policy-lease deadlines are bounded values, not active timers or revocation enforcement. Tests use deterministic clocks and existing synthetic RSA unit fixtures; wire keys are separately generated and removed.

Current implementation files are `pkg/privilege/privileges/sql_token.go`, `sql_token_test.go` and `pkg/sessionctx/sessionauth/identity.go`. Trusted targets contain `Audience`, `StorageClusterID`, `StorageScope` and `KeyspaceID`; scope zero is invalid, cluster scope requires canonical keyspace zero, and keyspace scope accepts 0..0xFFFFFF. The adapter must attest codec/mode before normalizing API-v1 null IDs and must reject deployment-reserved keyspaces by trusted classification. Custom claim constants live beside the verifier; standard claim/header constants come from jwx. Keep the twelve-claim literal signed fixture independent of fixture-builder constants. Test self-managed cluster, API-v2 DEFAULT and external-issuer audiences, shared-key cross-audience denial and bidirectional cluster-vs-DEFAULT mismatch. Neither product org/project nor a principal-kind enum enters the generic identity.

Implement atomic immutable snapshots with bounded validity, anti-rollback revision handling, identical-content idempotency, per-binding/key retirement and restart watermarks. Keep the transport adapter separate from verification. Reject missing or invalid policy; never fall back to the old plugin. Provide an injectable clock for deterministic boundary tests, while production derives monotonic lease deadlines from signed authority time and challenge age; the receiver wall clock does not authorize freshness. Do not add identity-provider, broker or KMS requests to the SQL path.

Tests must reject wrong audience/storage-cluster/scope/keyspace/principal/native user, including the same numeric keyspace ID in another storage cluster, old/revoked binding and policy revisions, missing/duplicate claims, invalid types and algorithm/key substitution. Cover expiry boundaries, clock skew, maximum token lifetime, malformed refresh, snapshot expiry, revision rollback, same-revision changed content, and removal/reintroduction of an old binding/key, and restart after expiry with a rolled-back wall clock. On restart require a fresh authority attestation bound to a new process incarnation/nonce and trusted time before token admission; persisted time/expiry/revocation watermarks alone do not prove freshness or authorize a new lease. Reject startup/readiness when skip-grant-table is combined with the new SQL-token profile; its existing early return must not bypass authentication. Preserve legacy tests. This milestone's isolated verifier is not an enabled login path.

### Milestone 3: Publish identity and enforce connection lifetime


Add the verified immutable identity to `privilege.VerificationInfo`, then publish it in `session.Auth` after every existing native account validation succeeds. Carry a read-only reference in SessionVars for audit; do not serialize it as SQL session state. Add a server-owned connection guard with incarnation and auth generation.

Integrate policy publication and guard registration so a new connection cannot miss a concurrent revocation. Start enforcement before init-connect or other authenticated SQL executes, not only after the connection enters the public registry. Verify policy again at activation. Policy publication marks affected guards terminal under the same lock/order that serializes dispatch admission; cancellation and cleanup run outside the lock. Work admitted before publication remains in flight, with native cancellation limits. Use a central scheduler or registry suitable for many connections, rather than an unbounded timer goroutine for every token refresh. Check terminal guard state at command dispatch and `session.ExecuteStmt`/`PrepareStmt` entry, including each statement inside a multi-statement request, through a dependency-light `sessionauth.Authorization.Check()` interface. A token identity without its guard fails closed; `InRestrictedSQL` does not bypass it. Prepared execution delegates to ExecuteStmt, but cursor FETCH directly consumes a result set and requires the dispatch/streaming cancellation paths. Cross-keyspace/internal helpers must neither discard a token guard nor reuse it against another store; explicitly authorized internal work remains separate. Cancel active/idle connections through existing shutdown/deadline/cancel mechanisms. Keep the normal connection owner responsible for closing the Session and rolling back uncommitted work. Do not hold policy or server registry locks during cleanup.

A targeted server integration test must hold one idle connection, one running cancellable query and one uncommitted transaction, revoke that exact binding, and observe new login/dispatch denial, socket exit and transaction cleanup. Other users remain connected. Race tests must arrange revocation between verification and registration, concurrent key retirement, and an expired timer arriving after successful user replacement. Record cases with noninterruptible commits separately; do not assert rollback of acknowledged writes.

### Milestone 4: Close protocol escape paths


RESET CONNECTION preserves the old guard and original maximum deadline. Implement a typed restore path for token users instead of `AuthWithoutVerification`: verify the exact original native `(user, host)`, expected plugin, current lock/account state, binding/native-grant readiness and storage scope, with native-record/policy generation revalidation before activation. A new host-specific record must not acquire different grants through reset. Fail closed if the identity cannot be safely restored. CHANGE USER stages a replacement guard and atomically swaps generations only after successful authentication. Failed attempts preserve the old guard's original expiry. Every path must retain terminal revocation state.

Block migration token issuance in SHOW SESSION_STATES for token-bound users and reject `AuthTiDBSessionToken` for matched native records using the new profile. SET SESSION_STATES cannot install or change trusted identity, expiry or revisions. Test users cannot change their own auth mode through permitted account-management SQL to escape these requirements. The trusted managed-username classification applies before auth-plugin selection, so a mismatched plugin/host record fails rather than falling back to password authentication. Test this on new connections as well as reset. The grant reconciler must observe privileged native changes as drift.

Tests must exercise reset before/after revocation, native account lock/plugin change or addition of a competing host-specific record, failed/successful change-user in each auth-mode direction, prepared statements/cursors/long-data state, stale callbacks, and attempted migration bypass. A driver-triggered automatic reset must not renew a session. Network errors and client disconnects must not leak guards, sessions or locks.

### Milestone 5: Integrate broker adapters and fleet operations


Only after native gates pass, implement a broker adapter and native-binding reconciliation. Use an independently configured external issuer with its own resource and principal namespace, following the companion design's external issuer integration contract. Freeze the matching public token profile and generated API contracts. Distribute descriptors, policy/key snapshots and anti-rollback watermarks through authenticated operator installation or agent delivery. Publish keys, collect per-incarnation acknowledgements, activate issuance, overlap and retire. Separate ordinary key retirement from emergency revocation of held sessions. Measure actual propagation/cutoff bounds under broker/distribution outage and receiver restart.

For every adapter, validate that replacement processes preserve the logical database audience only for the same database, mixed-version receivers reject unsupported profiles, TLS/CA rotation preserves security, and distribution disconnection expires policy predictably. SQL grants stay distinct from identity-provider or management roles. All client entry points must use the same native authorization path. These deployed-system gates remain separate from the native mock-storage integration increment.

Verify at least one independently implemented issuer adapter against the same conformance cases. A correctly signed token from an unconfigured issuer must fail even if its key ID or native username overlaps a configured issuer. No provider-specific endpoint, identifier prefix or account attribute may be required. Simultaneous multi-issuer receiver selection remains a separately specified capability; one trusted issuer per policy snapshot is the current verifier model.

## Concrete Steps


All commands run at the kernel worktree root. Inspect ownership and uncommitted changes first. New source/test files or top-level test functions require Bazel metadata regeneration:

    git status --short
    make bazel_prepare

The inspected privilege package contains no direct failpoint/testfailpoint references. Recheck before tests whenever implementation changes the package. Run the existing baseline:

    rg -n 'failpoint\.|testfailpoint\.' pkg/privilege/privileges
    go test -run '^(TestAuthTokenClaims|TestJWKSImpl)$' -tags=intest,deadlock ./pkg/privilege/privileges

An rg exit status of 1 means no matches, not a failed unit test. Server/session/executor packages do use failpoints; use the repository wrapper for their new targeted tests, substituting the actual test name created by the implementation:

    ./tools/check/failpoint-go-test.sh pkg/server -run '<actual SQL token identity test>' -count=1

The wrapper disables failpoints on exit. Use WIP verification while editing. Before a code change is called Ready, run its required scoped unit/integration tests, `make lint`, `git diff --check`, and include the generated Bazel changes. Do not run unrequested broad Bazel lint or RealTiKV suites. For a documentation-only increment, Ready requires design/link/diff review; existing-token baseline tests are supporting evidence, not native feature acceptance.

## Validation and Acceptance


A successful new-profile login must select the intended opaque database audience, immutable issuer-scoped principal and native grants. A token issued for another audience must fail even when both databases accept the same issuer/key and contain equivalent native users. Cover both explicit cluster scope and API-v2 keyspace scope, including DEFAULT and cross-mode denial with keyspace zero. Tokenless reset, stale migration tokens and user-controlled session-state JSON cannot bypass this check.

A binding/principal/key revoke must deny admission, mark affected guards terminal, stop new dispatch and observe targeted connection cleanup. This remains true for idle clients, streaming results, prepared statements, connection reuse, reset/change-user, policy refresh races and process restart. Preserve committed data; verify uncommitted test rows are absent after cleanup. Acknowledge noninterruptible in-flight outcomes separately rather than claiming a stronger transaction fence.

Test clock and policy expiry with deterministic clocks and include a real-server negative test. Record raw token/password absence from logs, errors, metrics and diagnostic artifacts. Record exact source/binary versions and test-only dependencies. No token is placed in a CLI argument or committed fixture. TLS verification is mandatory in the native client oracle.

## Idempotence and Recovery


A repeated identical snapshot install is safe; a same-revision different body or older revision fails. Persisted revision/time/expiry/revocation watermarks prevent accepted-state rollback, but admission after restart additionally requires fresh trusted authority/time evidence; cached state cannot acquire a new full lease. Failed parsing preserves the previous snapshot only within its existing validity, and no outage renews a lease. Revocation callbacks are idempotent and generation-fenced. Claim terminal connection shutdown for the expected incarnation/generation atomically under the same synchronization as auth replacement, then perform network cancellation outside locks. Test both orderings: replacement wins and stale callback does nothing; shutdown wins and replacement cannot activate. A generation check followed by an unfenced Kill call does not satisfy the contract.

Rollout is opt-in with distinct token users. Stop new issuance, drain or revoke token sessions, and reconcile only feature-owned users/keys before disabling the profile. Do not restore the whole metadata store, mutate retained password users or reuse old policy revisions. Test environments have their own durable cleanup records; unknown outcomes are observed before retry or deletion.

## Artifacts and Notes


Current native increment, 2026-11-04, Go 1.25.12/macOS arm64, pinned upstream baseline `8935853b36c764a15ab7b6d5547446577f99844b`. Run from the worktree root with `GOMAXPROCS=2`, `GOMEMLIMIT=2GiB`, and `GOFLAGS=-p=2` to bound resource use. Final targeted ordinary/race checks, metadata generation, parser checks and lint passed as recorded below.

    GOMAXPROCS=2 GOMEMLIMIT=2GiB go test -race -p 1 -run '^(TestSQLToken.*|TestAuthTokenClaims|TestJWKSImpl)$' -tags=intest,deadlock -count=1 -timeout=10m ./pkg/privilege/privileges
    # PASS, 10.877s; verifier, real-codec target adapter, native epochs and policy runtime.
    ./tools/check/failpoint-go-test.sh pkg/server -run '^(TestSQLToken.*|TestChangeUserAuth|TestChangeUserAuthFailureRestoresOldSession|TestChangeUserAndResetSessionResourceGroupCounter)$' -tags=intest,deadlock,nextgen -count=1 -timeout=10m
    # PASS, 4.035s, including failed-bootstrap cleanup.
    ./tools/check/failpoint-go-test.sh pkg/session -run '^TestSQLToken' -tags=intest,deadlock,nextgen -count=1 -timeout=10m
    # PASS, 2.776s.
    ./tools/check/failpoint-go-test.sh pkg/server/tests/tls -run '^(TestSQLTokenNativeWire|TestLegacyTokenVerifiedTLS)$' -tags=intest,deadlock,nextgen -count=1 -timeout=10m
    # PASS, 14.069s, including pessimistic transaction cleanup.
    GOMAXPROCS=2 GOMEMLIMIT=2GiB GOFLAGS=-p=1 ./tools/check/failpoint-go-test.sh pkg/server -race -p 1 -run '^(TestSQLToken.*|TestLegacyTokenVerifiedTLS|TestChangeUserAuth|TestChangeUserAuthFailureRestoresOldSession|TestChangeUserAndResetSessionResourceGroupCounter)$' -tags=intest,deadlock,nextgen -count=1 -timeout=10m . ./tests/tls ../session
    # PASS: server 7.481s, verified-TLS wire 18.019s, session 7.070s.
    make bazel_prepare
    # Exit 0; generated metadata includes the new native source/tests and fixture dependencies.
    make parser parser_yacc parser_fmt parser_unit_test
    # Exit 0. Initial formatter run normalized existing parser.y field alignment;
    # the repeat run, generated-parser comparison and parser tests passed.
    make lint
    # Exit 0, including revive and dashboard checks.
    git diff --check
    # Exit 0; gofmt, neutral terminology, local links and ExecPlan structure passed.

The native wire fixture owns actual SYSTEM/customer mock stores and embedded etcd in nextgen, generated TLS/RSA/Ed25519 keys, state files, loopback SQL listeners and clients. It verifies locked-create/grant/unlock provisioning, exact native identity, permitted CRUD and denied unrelated data, missing/wrong-scope/expired/future credentials, TLS, native migration/self-auth-mode denial, reset/change-user, active/idle/prepared revocation and unrelated binding continuity. A held pessimistic transaction cannot commit after revocation; its row stays absent and another connection can insert the exact locked key within three seconds. A native grant change closes prior sessions, while a new policy-authorized login sees current explicit grants. An unrenewed policy lease closes a pinned connection and rejects new admission. These are native protocol and lifecycle tests, not deployed TiKV or external CLI evidence.

The separate pool for the pessimistic transaction is intentional: the fixture caps each stock driver pool at one connection, so attempting to acquire a second connection from the pinned idle pool would block before any server action. The test harness was corrected before recording the final passing result. Runtime constants preserve literal wire values, and independent source/security review found no unresolved blocker; this is agent review, not separate human approval.

Historical verifier-only checkpoints follow; their unwired-runtime limitations describe those commits, not the current implementation.

The prototype was built on TiDB X baseline `30d6b0e7e5a993e0b15a32e923a0a576ad70aa8e`. Public commit `86077904c7f768234aaf448f26a9ef77d41b0a63` established the generic verifier, explicit storage modes and independently literal wire fixture. Earlier source/design reviews identified activation, reset/migration, restart freshness and storage-scope gaps; the pending milestones above retain those gates. An earlier regression rejected IDs above the API-v2 range and its null sentinel after first reproducing their acceptance. The current contract additionally distinguishes cluster scope from API-v2 DEFAULT rather than inferring mode from a numeric ID.

Recorded Ready verification at `8607790`, Go 1.25.12/macOS arm64:

    make bazel_prepare
    # Exit 0; generated source/dependency metadata aligned.
    go test -run '^(TestSQLToken.*|TestAuthTokenClaims|TestJWKSImpl)$' -tags=intest,deadlock -count=1 -timeout=10m ./pkg/privilege/privileges
    # ok github.com/pingcap/tidb/pkg/privilege/privileges 3.500s
    go test -run '^TestLegacyTokenVerifiedTLS$' -tags=intest,deadlock,nextgen -count=1 -timeout=10m ./pkg/server/tests/tls
    # ok github.com/pingcap/tidb/pkg/server/tests/tls 3.778s
    make lint
    # Exit 0 after fixing exported-constant documentation comments.
    git diff --cached --check
    # Exit 0; gofmt, document links and ExecPlan structure passed.

The targeted packages have no direct failpoint/testfailpoint references or BUILD dependencies at this checkpoint, so tests need no instrumentation under the package-based rule. The native-wire fixture uses mock storage and a standard Go MySQL client over real loopback TCP/TLS. TLS keys are generated under an owned temporary directory; signing keys remain in process memory. Public JWKS is loaded without a refresh goroutine; prior process state is restored after teardown. Client/server/store cleanup, closed SQL/status listeners and removal of the credential directory are checked. No retained data or credentials are test fixtures.

Independent agent review found no unresolved issues in that bounded revision; it was not separate human approval. No deployed TiKV, external CLI, new-profile native handshake, held-session revocation, race-detector/load benchmark or complete captured-telemetry redaction acceptance is claimed. The public Issue tracks the unfinished feature; PR #3 references it without closing those gates. Public-contract follow-up verification also passed on Go 1.25.12/macOS arm64:

    go test -run '^(TestSQLToken.*|TestAuthTokenClaims|TestJWKSImpl)$' -tags=intest,deadlock -count=1 -timeout=10m ./pkg/privilege/privileges
    # ok github.com/pingcap/tidb/pkg/privilege/privileges 3.644s
    go test -run '^TestLegacyTokenVerifiedTLS$' -tags=intest,deadlock,nextgen -count=1 -timeout=10m ./pkg/server/tests/tls
    # ok github.com/pingcap/tidb/pkg/server/tests/tls 1.422s
    make lint
    # Exit 0.
    git diff --cached --check
    # Exit 0; gofmt, neutral terminology, local links and ExecPlan structure passed.

This follow-up changes documentation, comments and synthetic example strings. It adds no Go files, top-level test functions, imports, dependencies or build targets; `make bazel_prepare` is not required again. Independent agent review of the current public Issue, PR draft and seven-file diff found no unresolved issues. The final evidence-only plan update does not change the reviewed protocol. The deployment/runtime limits above remain unchanged.

## Interfaces and Dependencies


The independent identity package contains immutable value types and read-only authorization interfaces. `privilege.VerificationInfo` gains nullable `SQLTokenIdentity` and a snapshot-bound native check; existing plugins leave it nil and keep their behavior. SessionVars receives an immutable reference and a read-only `sessionauth.Authorization` checker after successful authentication; the checker references the shared guard state without importing server code into session. `clientConn` owns a guard that references identity plus terminal state, deadlines and an auth generation. Policy installation and guard activation use one explicit ordering protocol; implementation must document lock order and cleanup ownership.

The first version consumes authenticated local descriptor/policy files installed by a trusted operator/agent; no new public status endpoint and no per-query network service. The kernel uses existing native SQL grants and maintained cryptography. The operator/broker retains authoritative identity/binding/policy state and signing permissions. External account attributes and role mappings stay in adapters; the kernel accepts only the fixed profile. No identity-provider password synchronization, workflow-service dependency or automatic SQL grant follows from this design.

Revision note (2026-11-03): initial plan written after native session, migration and cancellation audit; runtime implementation deliberately remains gated on the listed tests.

Revision note (2026-11-03): finalized baseline evidence and independent design review. All implementation milestones remain visibly pending; no native authentication behavior changed.

Revision note (2026-11-03): added the stateless verifier/value type and isolated native MySQL/TLS legacy fixture. Split mock-wire evidence from deployed TiKV acceptance, fixed keyspace-range/SYSTEM contract wording after independent review, and retained policy/session/adapter integration as explicit pending milestones.

Revision note (2026-11-03): recorded the owner's explicit fork-publication request and pinned PR base. This publication-only revision changes no code or security contract.

Revision note (2026-11-03): generalized the unregistered profile and documentation after control-plane source review, retaining legacy compatibility and every pending activation/revocation gate. Current generic-profile validation and final independent review passed; the runtime and adapter milestones remain open.

Revision note (2026-11-03): moved current feature tracking to public Issue #4, removed product-specific terminology and inaccessible references, and specified provider-neutral issuer onboarding and conformance gates. Protocol behavior is unchanged.

Revision note (2026-11-04): merged the pinned baseline without rewriting history, implemented the signed local policy runtime and native connection/session integration, and added the concrete operator contract. Completed Ready verification, actual nextgen mock-store/MySQL/TLS transaction cleanup, race checks and the rejected-startup regression. Deployment and external adapter acceptance remain separate gates.
