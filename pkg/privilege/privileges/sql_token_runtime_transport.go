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

package privileges

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx/sessionauth"
)

func newSQLTokenRuntime(opts SQLTokenRuntimeOptions, now func() time.Time) (_ *SQLTokenRuntime, err error) {
	if !validSQLTarget(opts.Target) || opts.AuthorityKeyPath == "" || opts.PolicyPath == "" || opts.StateDir == "" || now == nil {
		return nil, ErrSQLTokenPolicyUnavailable
	}
	authority, err := readSQLAuthority(opts.AuthorityKeyPath)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(opts.StateDir, 0700); err != nil {
		return nil, ErrSQLTokenPolicyUnavailable
	}
	info, err := os.Lstat(opts.StateDir)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return nil, ErrSQLTokenPolicyUnavailable
	}
	lockFile, err := acquireSQLPolicyLock(opts.StateDir)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			releaseSQLPolicyLock(lockFile)
		}
	}()
	r := &SQLTokenRuntime{opts: opts, authority: authority, lockFile: lockFile, now: now, guards: map[*SQLTokenGuard]struct{}{}, admissions: map[sessionauth.Identity][]sqlTokenAdmission{}, callbackWake: make(chan struct{}, 1), stop: make(chan struct{})}
	raw, readErr := readSQLPolicyFile(filepath.Join(opts.StateDir, sqlPolicyWatermarkFile), true)
	if os.IsNotExist(readErr) {
		// An interrupted first boot can leave the initial watermark alone. Losing a
		// watermark from an established directory must never silently erase fences.
		for _, name := range []string{sqlPolicyChallengeFile, sqlPolicyStatusFile} {
			if _, e := os.Lstat(filepath.Join(opts.StateDir, name)); !os.IsNotExist(e) {
				return nil, ErrSQLTokenPolicyUnavailable
			}
		}
		r.watermark = sqlTokenWatermark{Version: sqlPolicyWireVersion, Target: sqlTargetDocument(opts.Target), AuthorityFingerprint: sqlPolicyHash(authority), Revision: "0", Keys: map[string]sqlTokenKeyWatermark{}, Bindings: map[string]sqlTokenBindingWatermark{}, ManagedUsernames: map[string]bool{}}
		if err = atomicSQLPolicyFile(opts.StateDir, sqlPolicyWatermarkFile, r.watermark); err != nil {
			return nil, err
		}
	} else if readErr != nil || decodeSQLPolicyJSON(raw, &r.watermark) != nil || !sqlWatermarkValid(r.watermark, opts.Target, authority) {
		return nil, ErrSQLTokenPolicyUnavailable
	}
	incarnation, err := randomSQLPolicyNonce()
	if err != nil {
		return nil, err
	}
	r.challenge = SQLTokenPolicyChallenge{Version: sqlPolicyWireVersion, Incarnation: incarnation, Sequence: "0", Target: sqlTargetDocument(opts.Target)}
	if err = r.rotateChallengeLocked(now()); err != nil {
		return nil, err
	}
	if err = r.writeStatusLocked(now()); err != nil {
		return nil, err
	}
	return r, nil
}

func acquireSQLPolicyLock(dir string) (*os.File, error) {
	path := filepath.Join(dir, sqlPolicyLockFile)
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
			return nil, ErrSQLTokenPolicyUnavailable
		}
	} else if !os.IsNotExist(err) {
		return nil, ErrSQLTokenPolicyUnavailable
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, ErrSQLTokenPolicyUnavailable
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, ErrSQLTokenPolicyUnavailable
	}
	return f, nil
}

func releaseSQLPolicyLock(f *os.File) {
	if f != nil {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}
}

func randomSQLPolicyNonce() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", ErrSQLTokenPolicyUnavailable
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func (r *SQLTokenRuntime) rotateChallengeLocked(now time.Time) error {
	sequence, err := strconv.ParseUint(r.challenge.Sequence, 10, 64)
	if err != nil || sequence == math.MaxUint64 {
		return ErrSQLTokenPolicyUnavailable
	}
	nonce, err := randomSQLPolicyNonce()
	if err != nil {
		return err
	}
	challenge := r.challenge
	challenge.Sequence = strconv.FormatUint(sequence+1, 10)
	challenge.Nonce = nonce
	challenge.MinimumIssuedAt = r.watermark.HighTime
	if !r.havePolicy {
		challenge.MinimumIssuedAt = max(challenge.MinimumIssuedAt, r.watermark.LowerTime)
	}
	if err = atomicSQLPolicyFile(r.opts.StateDir, sqlPolicyChallengeFile, challenge); err != nil {
		return err
	}
	r.challenge = challenge
	r.challengeAt = now
	return nil
}

func (r *SQLTokenRuntime) readResponse() (SQLTokenPolicyResponse, SQLTokenPolicy, string, error) {
	var response SQLTokenPolicyResponse
	var policy SQLTokenPolicy
	raw, err := readSQLPolicyFile(r.opts.PolicyPath, false)
	if err != nil {
		return response, policy, "", ErrSQLTokenPolicyUnavailable
	}
	hash := sqlPolicyHash(raw)
	r.mu.Lock()
	seen := hash == r.lastResponseHash
	r.mu.Unlock()
	if seen {
		return response, policy, hash, ErrSQLTokenPolicyUnavailable
	}
	var envelope SQLTokenPolicyEnvelope
	if decodeSQLPolicyJSON(raw, &envelope) != nil {
		return response, policy, hash, ErrSQLTokenPolicyUnavailable
	}
	payload, ok := sqlCanonicalBase64(envelope.Payload)
	if !ok {
		return response, policy, hash, ErrSQLTokenPolicyUnavailable
	}
	signature, ok := sqlCanonicalBase64(envelope.Signature)
	if !ok || len(signature) != ed25519.SignatureSize || !ed25519.Verify(r.authority, append([]byte(sqlPolicyDomain), payload...), signature) {
		return response, policy, hash, ErrSQLTokenPolicyUnavailable
	}
	if decodeSQLPolicyJSON(payload, &response) != nil {
		return response, policy, hash, ErrSQLTokenPolicyUnavailable
	}
	policy, err = response.Policy.policy()
	return response, policy, hash, err
}

func (r *SQLTokenRuntime) poll() {
	response, policy, hash, err := r.readResponse()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	now := r.now()
	// Expiration is terminal. A response arriving after the previous lease ended
	// may admit new sessions, but must not revive guards the old lease protected.
	r.checkGuardsLocked(now)
	if !r.fatal {
		if err == nil {
			r.acceptResponseLocked(response, policy, now)
		}
		// Remember rejected bytes as well; changed signed responses still get checked.
		// A rejected response cannot become valid merely by waiting or rotating nonce.
		if hash != "" {
			r.lastResponseHash = hash
		}
		r.persistTimeLocked(now)
		if !r.fatal && (now.Before(r.challengeAt) || !now.Before(r.challengeAt.Add(sqlPolicyChallengeLifetime))) {
			if r.rotateChallengeLocked(now) != nil {
				r.failLocked()
			}
		}
	}
	r.checkGuardsLocked(now)
	if r.writeStatusLocked(now) != nil {
		r.failLocked()
	}
}

func (r *SQLTokenRuntime) acceptResponseLocked(response SQLTokenPolicyResponse, policy SQLTokenPolicy, now time.Time) bool {
	c := r.challenge
	if response.Version != sqlPolicyWireVersion || response.Incarnation != c.Incarnation || response.Nonce != c.Nonce || response.Sequence != c.Sequence || response.Target != c.Target || response.Policy.Target != c.Target || now.Before(r.challengeAt) || !now.Before(r.challengeAt.Add(sqlPolicyChallengeLifetime)) {
		return false
	}
	if response.IssuedAt < 0 || response.IssuedAt < r.watermark.HighTime || !r.havePolicy && response.IssuedAt < r.watermark.LowerTime || response.ValidUntil > sqlTokenMaxUnixSeconds || response.ValidUntil <= response.IssuedAt || response.IssuedAt < response.Policy.ValidFrom || response.ValidUntil > response.Policy.ValidUntil {
		return false
	}
	// Signed timestamps never decrease. Effective clocks also never decrease, even
	// across same-second refreshes or an expired lease. Integer-second authority
	// timestamps have one second of uncertainty; transport adds challenge age.
	lower := time.Unix(response.IssuedAt, 0)
	upper := lower.Add(time.Second).Add(now.Sub(r.challengeAt))
	if r.havePolicy {
		if previous := r.authorityNowLocked(now); previous.After(lower) {
			lower = previous
		}
		if previous := r.authorityUpperNowLocked(now); previous.After(upper) {
			upper = previous
		}
	}
	if !r.havePolicy {
		if floor := time.Unix(r.watermark.LowerTime, 0); floor.After(lower) {
			lower = floor
		}
		if floor := time.Unix(r.watermark.UpperTime, 0); floor.After(upper) {
			upper = floor
		}
	}
	if !validSQLTime(lower) || !validSQLTime(upper) || lower.Before(policy.ValidFrom) || !upper.Before(policy.ValidUntil) || !upper.Before(time.Unix(response.ValidUntil, 0)) {
		return false
	}
	duration := response.ValidUntil - response.IssuedAt
	if duration > int64(sqlPolicyLeaseLifetime/time.Second) {
		duration = int64(sqlPolicyLeaseLifetime / time.Second)
	}
	deadline := r.challengeAt.Add(time.Duration(duration) * time.Second)
	if !now.Before(deadline) {
		return false
	}
	next, err := r.nextWatermarkLocked(response.Policy, policy, response.IssuedAt)
	if err != nil {
		return false
	}
	next.LowerTime = lower.Unix()
	next.UpperTime = sqlPolicyCeilSecond(upper)
	if next.UpperTime > sqlTokenMaxUnixSeconds {
		return false
	}
	if atomicSQLPolicyFile(r.opts.StateDir, sqlPolicyWatermarkFile, next) != nil {
		r.failLocked()
		return false
	}
	// Publication happens only after durable fencing. If challenge replacement
	// fails, the engine remains terminally closed rather than reuse an admission.
	acceptedNonce, acceptedSequence := c.Nonce, c.Sequence
	r.watermark = next
	if r.rotateChallengeLocked(now) != nil {
		r.failLocked()
		return false
	}
	r.policy = policy
	r.havePolicy = true
	r.authorityTime = lower
	r.authorityUpperTime = upper
	r.authorityAnchor = now
	r.leaseDeadline = deadline
	r.responseValidUntil = time.Unix(response.ValidUntil, 0)
	r.acceptedNonce = acceptedNonce
	r.acceptedSequence = acceptedSequence
	r.checkGuardsLocked(now)
	return true
}

func (r *SQLTokenRuntime) persistTimeLocked(now time.Time) {
	if r.fatal || !r.havePolicy {
		return
	}
	// Once a lease ends, its persisted fence stops advancing. In-process bounds
	// continue monotonically; a restart always requires a fresh authority response.
	if now.After(r.leaseDeadline) {
		now = r.leaseDeadline
	}
	if now.Before(r.authorityAnchor) {
		r.failLocked()
		return
	}
	lower := r.authorityNowLocked(now).Unix()
	upper := sqlPolicyCeilSecond(r.authorityUpperNowLocked(now))
	if lower <= r.watermark.LowerTime && upper <= r.watermark.UpperTime {
		return
	}
	if upper > sqlTokenMaxUnixSeconds {
		r.failLocked()
		return
	}
	next := r.watermark
	next.LowerTime = max(next.LowerTime, lower)
	next.UpperTime = max(next.UpperTime, upper)
	if atomicSQLPolicyFile(r.opts.StateDir, sqlPolicyWatermarkFile, next) != nil {
		r.failLocked()
		return
	}
	r.watermark = next
}

func sqlPolicyCeilSecond(value time.Time) int64 {
	second := value.Unix()
	if value.Nanosecond() != 0 {
		second++
	}
	return second
}

func (r *SQLTokenRuntime) writeStatusLocked(now time.Time) error {
	status := SQLTokenRuntimeStatus{Version: sqlPolicyWireVersion, Target: sqlTargetDocument(r.opts.Target), Incarnation: r.challenge.Incarnation, AcceptedNonce: r.acceptedNonce, AcceptedSequence: r.acceptedSequence, PolicyDigest: r.watermark.Digest, Revision: r.watermark.Revision, Ready: r.freshLocked(now)}
	if r.havePolicy {
		status.PolicyValidUntil = r.policy.ValidUntil.Unix()
		deadline := r.leaseDeadline
		upper := r.authorityUpperNowLocked(now)
		for _, until := range []time.Time{r.policy.ValidUntil, r.responseValidUntil} {
			if bound := now.Add(until.Sub(upper)); bound.Before(deadline) {
				deadline = bound
			}
		}
		status.LeaseValidUntil = r.authorityNowLocked(deadline).Unix()
	}
	switch {
	case r.closed:
		status.LastError = sqlPolicyStatusClosed
	case r.fatal || r.havePolicy && !status.Ready:
		status.LastError = sqlPolicyStatusUnavailable
	case !status.Ready:
		status.LastError = sqlPolicyStatusAwaiting
	}
	if status == r.status {
		return nil
	}
	if err := atomicSQLPolicyFile(r.opts.StateDir, sqlPolicyStatusFile, status); err != nil {
		return err
	}
	r.status = status
	return nil
}

func (r *SQLTokenRuntime) nextWatermarkLocked(doc SQLTokenPolicyDocument, policy SQLTokenPolicy, issuedAt int64) (sqlTokenWatermark, error) {
	prior := r.watermark
	oldRevision, _ := strconv.ParseUint(prior.Revision, 10, 64)
	raw, err := json.Marshal(doc)
	if err != nil {
		return prior, ErrSQLTokenPolicyUnavailable
	}
	digest := sqlPolicyHash(raw)
	if policy.SnapshotRevision < oldRevision || policy.SnapshotRevision == oldRevision && digest != prior.Digest {
		return prior, ErrSQLTokenPolicyUnavailable
	}
	next := sqlTokenWatermark{Version: sqlPolicyWireVersion, Target: prior.Target, AuthorityFingerprint: prior.AuthorityFingerprint, Revision: doc.Revision, Digest: digest, HighTime: issuedAt, Keys: map[string]sqlTokenKeyWatermark{}, Bindings: map[string]sqlTokenBindingWatermark{}, ManagedUsernames: map[string]bool{}}
	for id, value := range prior.Keys {
		value.Retired = true
		next.Keys[id] = value
	}
	for id, value := range prior.Bindings {
		value.Retired = true
		next.Bindings[id] = value
	}
	for username := range prior.ManagedUsernames {
		next.ManagedUsernames[username] = true
	}
	for id, key := range policy.Keys {
		encoded, err := x509.MarshalPKIXPublicKey(key)
		if err != nil {
			return prior, ErrSQLTokenPolicyUnavailable
		}
		namespaced := sqlPolicyNamespace(doc.Issuer, id)
		fingerprint := sqlPolicyHash(encoded)
		if old, ok := prior.Keys[namespaced]; ok && (old.Fingerprint != fingerprint || old.Retired) {
			return prior, ErrSQLTokenPolicyUnavailable
		}
		next.Keys[namespaced] = sqlTokenKeyWatermark{Fingerprint: fingerprint}
	}
	for id, binding := range doc.Bindings {
		namespaced := sqlPolicyNamespace(doc.Issuer, id)
		encoded, err := json.Marshal(binding)
		if err != nil {
			return prior, ErrSQLTokenPolicyUnavailable
		}
		bindingDigest := sqlPolicyHash(encoded)
		b := policy.Bindings[id]
		if old, ok := prior.Bindings[namespaced]; ok {
			oldBinding, _ := sqlTokenRevision(old.BindingRevision)
			oldPolicy, _ := sqlTokenRevision(old.PolicyRevision)
			if b.BindingRevision < oldBinding || b.PolicyRevision < oldPolicy || b.BindingRevision == oldBinding && b.PolicyRevision == oldPolicy && old.Digest != bindingDigest || old.Retired && binding.Enabled && (b.BindingRevision <= oldBinding || b.PolicyRevision <= oldPolicy) {
				return prior, ErrSQLTokenPolicyUnavailable
			}
		}
		next.Bindings[namespaced] = sqlTokenBindingWatermark{BindingRevision: binding.BindingRevision, PolicyRevision: binding.PolicyRevision, Digest: bindingDigest, Retired: !binding.Enabled}
		next.ManagedUsernames[binding.NativeUsername] = true
	}
	// The persisted representation has the same bounded map/parser limits. Refuse
	// new entries before they could create state that cannot be restored.
	if len(next.Keys) > sqlPolicyMaxBindings || len(next.Bindings) > sqlPolicyMaxBindings || len(next.ManagedUsernames) > sqlPolicyMaxBindings {
		return prior, ErrSQLTokenPolicyUnavailable
	}
	return next, nil
}
