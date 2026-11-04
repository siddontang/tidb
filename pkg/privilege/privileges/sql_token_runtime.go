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
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx/sessionauth"
)

const (
	sqlPolicyPollInterval      = 100 * time.Millisecond
	sqlPolicyChallengeLifetime = 10 * time.Second
	sqlPolicyLeaseLifetime     = 30 * time.Second
	sqlPolicyMaxAdmissions     = 65536
)

// SQLTokenRuntimeOptions pins one native database target and a separate policy
// authority. StateDir must be exclusively owned by this server (0700). Losing or
// restoring older state is not a supported recovery: it loses revocation fences.
// AuthorityKeyPath is a single PEM PKIX Ed25519 public key; PolicyPath contains a
// SQLTokenPolicyEnvelope supplied by a trusted, independently implemented broker.
type SQLTokenRuntimeOptions struct {
	Target           sessionauth.Target
	AuthorityKeyPath string
	PolicyPath       string
	StateDir         string
}

// SQLTokenRuntime serializes policy publication, login admission and session
// authorization. Its 100ms poll also revokes idle connections. Security decisions
// use signed authority time plus elapsed monotonic time, never the local wall clock.
// Close must not be called from a revocation callback, which must return promptly.
type SQLTokenRuntime struct {
	mu                 sync.Mutex
	opts               SQLTokenRuntimeOptions
	authority          ed25519.PublicKey
	lockFile           *os.File
	now                func() time.Time
	watermark          sqlTokenWatermark
	challenge          SQLTokenPolicyChallenge
	challengeAt        time.Time
	policy             SQLTokenPolicy
	havePolicy         bool
	authorityTime      time.Time
	authorityUpperTime time.Time
	authorityAnchor    time.Time
	leaseDeadline      time.Time
	responseValidUntil time.Time
	acceptedNonce      string
	acceptedSequence   string
	lastResponseHash   string
	status             SQLTokenRuntimeStatus
	fatal              bool
	closed             bool
	guards             map[*SQLTokenGuard]struct{}
	admissions         map[sessionauth.Identity][]sqlTokenAdmission
	admissionCount     int
	pendingCallbacks   []*SQLTokenGuard
	callbackWake       chan struct{}
	stop               chan struct{}
	closeOnce          sync.Once
	workers            sync.WaitGroup
}

type sqlTokenAdmission struct {
	sessionDeadline time.Time
	leaseDeadline   time.Time
}

// SQLTokenGuard is a terminal authorization guard for one physical SQL session.
// Identity returns immutable login evidence; refreshed leases do not alter it or
// extend the session's original maximum lifetime. Close unregisters normally and
// does not request transport closure.
type SQLTokenGuard struct {
	runtime     *SQLTokenRuntime
	identity    sessionauth.Identity
	deadline    time.Time
	nativeCheck func() bool
	onRevoke    func(*SQLTokenGuard)
	terminal    bool
	closed      bool
}

var activeSQLTokenRuntime atomic.Pointer[SQLTokenRuntime]

// CurrentSQLTokenRuntime returns the server-installed runtime, or nil if disabled.
func CurrentSQLTokenRuntime() *SQLTokenRuntime { return activeSQLTokenRuntime.Load() }

// SetSQLTokenRuntime swaps the process runtime. The caller closes the old value
// after connection shutdown; swapping never silently closes another server's state.
func SetSQLTokenRuntime(runtime *SQLTokenRuntime) *SQLTokenRuntime {
	return activeSQLTokenRuntime.Swap(runtime)
}

// NewSQLTokenRuntime validates pinned authority and persisted state, publishes a
// fresh boot challenge, and starts polling. It does not trust a restored policy or
// cached response. Missing, malformed and replayed responses leave login closed.
func NewSQLTokenRuntime(opts SQLTokenRuntimeOptions) (*SQLTokenRuntime, error) {
	r, err := newSQLTokenRuntime(opts, time.Now)
	if err != nil {
		return nil, err
	}
	r.workers.Add(2)
	go r.pollLoop()
	go r.callbackLoop()
	return r, nil
}

// Target returns the configured operator-attested target; the native adapter must
// independently compare it with the actual storage codec before every login.
func (r *SQLTokenRuntime) Target() sessionauth.Target { return r.opts.Target }

// IsManagedUsername retains the union of every admitted policy's usernames,
// including disabled and removed bindings, across expiration and restart. Native
// password fallback must not bypass these names when the policy is unavailable.
func (r *SQLTokenRuntime) IsManagedUsername(username string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.watermark.ManagedUsernames[username]
}

// Authenticate verifies a token against a fresh policy and creates a one-use
// admission ticket. Register must consume the exact returned identity, preventing
// callers from extending deadlines or manufacturing an authenticated identity.
func (r *SQLTokenRuntime) Authenticate(raw []byte, request SQLTokenRequest) (sessionauth.Identity, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	if !r.freshLocked(now) {
		return sessionauth.Identity{}, ErrSQLTokenPolicyUnavailable
	}
	r.pruneAdmissionsLocked(now)
	if r.admissionCount >= sqlPolicyMaxAdmissions {
		return sessionauth.Identity{}, ErrSQLTokenPolicyUnavailable
	}
	trustedNow := r.authorityNowLocked(now)
	identity, err := verifySQLTokenAtBounds(raw, request, r.policy, trustedNow, r.authorityUpperNowLocked(now))
	if err != nil {
		return sessionauth.Identity{}, err
	}
	admission := sqlTokenAdmission{sessionDeadline: now.Add(identity.SessionDeadline.Sub(trustedNow)), leaseDeadline: r.leaseDeadline}
	for _, remaining := range []time.Duration{identity.PolicyLeaseDeadline.Sub(trustedNow), r.responseValidUntil.Sub(r.authorityUpperNowLocked(now))} {
		if deadline := now.Add(remaining); deadline.Before(admission.leaseDeadline) {
			admission.leaseDeadline = deadline
		}
	}
	if admission.sessionDeadline.Before(admission.leaseDeadline) {
		admission.leaseDeadline = admission.sessionDeadline
	}
	identity.PolicyLeaseDeadline = trustedNow.Add(admission.leaseDeadline.Sub(now))
	r.admissions[identity] = append(r.admissions[identity], admission)
	r.admissionCount++
	return identity, nil
}

// Register atomically consumes an Authenticate admission and rechecks the current
// policy. NativeCheck must only read the privilege cache and must not perform I/O,
// acquire connection locks or access mutable session state. onRevoke executes on
// a separate worker outside the engine lock, at most once for this guard.
func (r *SQLTokenRuntime) Register(identity sessionauth.Identity, nativeCheck func() bool, onRevoke func(*SQLTokenGuard)) (*SQLTokenGuard, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	admissions := r.admissions[identity]
	if len(admissions) == 0 {
		return nil, ErrSQLTokenAccessDenied
	}
	admission := admissions[0]
	if len(admissions) == 1 {
		delete(r.admissions, identity)
	} else {
		r.admissions[identity] = admissions[1:]
	}
	r.admissionCount--
	now := r.now()
	if !now.Before(admission.sessionDeadline) || !now.Before(admission.leaseDeadline) || !r.identityAllowedLocked(identity, now) || nativeCheck == nil || !nativeCheck() {
		return nil, ErrSQLTokenAccessDenied
	}
	if len(r.guards)+len(r.pendingCallbacks) >= sqlPolicyMaxAdmissions {
		return nil, ErrSQLTokenPolicyUnavailable
	}
	guard := &SQLTokenGuard{runtime: r, identity: identity, deadline: admission.sessionDeadline, nativeCheck: nativeCheck, onRevoke: onRevoke}
	r.guards[guard] = struct{}{}
	return guard, nil
}

// Replace establishes one ordering point between CHANGE USER and policy
// revocation. nil old is an initial login; nil next is an unmanaged login. Failed
// replacements preserve any valid counterpart. An observed authorization failure
// terminally revokes that guard. Callers hold their connection auth mutex through
// this call and their pointer swap, and dispose failed candidates.
func (r *SQLTokenRuntime) Replace(old, next *SQLTokenGuard) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	for _, guard := range []*SQLTokenGuard{old, next} {
		if guard == nil {
			continue
		}
		if guard.runtime != r {
			return ErrSQLTokenAccessDenied
		}
		if !r.guardAllowedLocked(guard, now) {
			r.revokeLocked(guard)
			return ErrSQLTokenAccessDenied
		}
	}
	if old != nil && old != next {
		old.closed = true
		delete(r.guards, old)
	}
	return nil
}

// Check enforces current policy, original session lifetime and native privileges.
// An authorization failure is terminal, even if a later policy would allow login.
func (g *SQLTokenGuard) Check() error {
	r := g.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.guardAllowedLocked(g, r.now()) {
		r.revokeLocked(g)
		return ErrSQLTokenAccessDenied
	}
	return nil
}

// Identity returns a value copy containing neither credentials nor policy aliases.
func (g *SQLTokenGuard) Identity() sessionauth.Identity { return g.identity }

// Close unregisters the guard without scheduling a revocation callback. A callback
// already scheduled by an earlier revocation still runs exactly once.
func (g *SQLTokenGuard) Close() {
	r := g.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	g.closed = true
	delete(r.guards, g)
}

func (r *SQLTokenRuntime) freshLocked(now time.Time) bool {
	return !r.closed && !r.fatal && r.havePolicy && !now.Before(r.authorityAnchor) && now.Before(r.leaseDeadline) && !r.authorityNowLocked(now).Before(r.policy.ValidFrom) && r.authorityUpperNowLocked(now).Before(r.policy.ValidUntil) && r.authorityUpperNowLocked(now).Before(r.responseValidUntil)
}

func (r *SQLTokenRuntime) authorityNowLocked(now time.Time) time.Time {
	return r.authorityTime.Add(now.Sub(r.authorityAnchor))
}

func (r *SQLTokenRuntime) authorityUpperNowLocked(now time.Time) time.Time {
	return r.authorityUpperTime.Add(now.Sub(r.authorityAnchor))
}

func (r *SQLTokenRuntime) identityAllowedLocked(identity sessionauth.Identity, now time.Time) bool {
	if !r.freshLocked(now) || identity.Target != r.opts.Target || identity.Issuer != r.policy.Issuer || identity.Profile != SQLTokenPlugin {
		return false
	}
	if _, ok := r.policy.Keys[identity.KeyID]; !ok {
		return false
	}
	binding, ok := r.policy.Bindings[identity.BindingID]
	return ok && binding.Enabled && binding.PrincipalID == identity.PrincipalID && binding.NativeUsername == identity.NativeUsername && binding.NativeHost == identity.NativeHost && binding.BindingRevision == identity.BindingRevision && binding.PolicyRevision == identity.PolicyRevision && binding.DesiredGrantRevision > 0 && binding.DesiredGrantRevision == binding.AppliedGrantRevision && r.authorityUpperNowLocked(now).Before(binding.ExpiresAt)
}

func (r *SQLTokenRuntime) guardAllowedLocked(g *SQLTokenGuard, now time.Time) bool {
	if g.closed || g.terminal {
		return false
	}
	if _, ok := r.guards[g]; !ok {
		return false
	}
	return now.Before(g.deadline) && r.identityAllowedLocked(g.identity, now) && g.nativeCheck != nil && g.nativeCheck()
}

func (r *SQLTokenRuntime) revokeLocked(g *SQLTokenGuard) {
	if g.terminal || g.closed {
		return
	}
	g.terminal = true
	delete(r.guards, g)
	if g.onRevoke != nil {
		r.pendingCallbacks = append(r.pendingCallbacks, g)
		select {
		case r.callbackWake <- struct{}{}:
		default:
		}
	}
}

func (r *SQLTokenRuntime) checkGuardsLocked(now time.Time) {
	for g := range r.guards {
		if !r.guardAllowedLocked(g, now) {
			r.revokeLocked(g)
		}
	}
	r.pruneAdmissionsLocked(now)
}

func (r *SQLTokenRuntime) pruneAdmissionsLocked(now time.Time) {
	for identity, admissions := range r.admissions {
		kept := admissions[:0]
		for _, admission := range admissions {
			if now.Before(admission.sessionDeadline) && now.Before(admission.leaseDeadline) && r.identityAllowedLocked(identity, now) {
				kept = append(kept, admission)
			} else {
				r.admissionCount--
			}
		}
		if len(kept) == 0 {
			delete(r.admissions, identity)
		} else {
			r.admissions[identity] = kept
		}
	}
}

func (r *SQLTokenRuntime) failLocked() {
	r.fatal = true
	for g := range r.guards {
		r.revokeLocked(g)
	}
	clear(r.admissions)
	r.admissionCount = 0
}

func (r *SQLTokenRuntime) pollLoop() {
	defer r.workers.Done()
	ticker := time.NewTicker(sqlPolicyPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-ticker.C:
			r.poll()
		}
	}
}

func (r *SQLTokenRuntime) callbackLoop() {
	defer r.workers.Done()
	for {
		select {
		case <-r.callbackWake:
		case <-r.stop:
		}
		for {
			r.mu.Lock()
			if len(r.pendingCallbacks) == 0 {
				closed := r.closed
				r.mu.Unlock()
				if closed {
					return
				}
				break
			}
			callbacks := r.pendingCallbacks
			r.pendingCallbacks = nil
			r.mu.Unlock()
			for _, g := range callbacks {
				g.onRevoke(g)
			}
		}
	}
}

// Close terminally revokes guards, waits for poll and callback workers, and
// releases the state-directory lock. It is idempotent and safe concurrently.
func (r *SQLTokenRuntime) Close() {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.persistTimeLocked(r.now())
		r.closed = true
		for g := range r.guards {
			r.revokeLocked(g)
		}
		clear(r.admissions)
		r.admissionCount = 0
		_ = r.writeStatusLocked(r.now())
		close(r.stop)
		r.mu.Unlock()
		r.workers.Wait()
		releaseSQLPolicyLock(r.lockFile)
	})
}
