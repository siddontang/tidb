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
	"bytes"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx/sessionauth"
)

const (
	sqlPolicyMaxBytes    = 1 << 20
	sqlPolicyMaxKeys     = 64
	sqlPolicyMaxBindings = 4096
)

type sqlTokenKeyWatermark struct {
	Fingerprint string `json:"fingerprint"`
	Retired     bool   `json:"retired"`
}

type sqlTokenBindingWatermark struct {
	BindingRevision string `json:"binding_revision"`
	PolicyRevision  string `json:"policy_revision"`
	Digest          string `json:"digest"`
	Retired         bool   `json:"retired"`
}

type sqlTokenWatermark struct {
	Version              int                                 `json:"version"`
	Target               SQLTokenTargetDocument              `json:"target"`
	AuthorityFingerprint string                              `json:"authority_fingerprint"`
	Revision             string                              `json:"revision"`
	Digest               string                              `json:"digest"`
	HighTime             int64                               `json:"high_time"`
	LowerTime            int64                               `json:"lower_time"`
	UpperTime            int64                               `json:"upper_time"`
	Keys                 map[string]sqlTokenKeyWatermark     `json:"keys"`
	Bindings             map[string]sqlTokenBindingWatermark `json:"bindings"`
	ManagedUsernames     map[string]bool                     `json:"managed_usernames"`
}

func sqlTargetDocument(target sessionauth.Target) SQLTokenTargetDocument {
	scope := sqlPolicyScopeCluster
	if target.StorageScope == sessionauth.StorageScopeKeyspace {
		scope = sqlPolicyScopeKeyspace
	}
	return SQLTokenTargetDocument{Audience: target.Audience, StorageClusterID: strconv.FormatUint(target.StorageClusterID, 10), StorageScope: scope, KeyspaceID: target.KeyspaceID}
}

func (d SQLTokenTargetDocument) target() (sessionauth.Target, bool) {
	id, ok := sqlTokenRevision(d.StorageClusterID)
	target := sessionauth.Target{Audience: d.Audience, StorageClusterID: id, KeyspaceID: d.KeyspaceID}
	switch d.StorageScope {
	case sqlPolicyScopeCluster:
		target.StorageScope = sessionauth.StorageScopeCluster
	case sqlPolicyScopeKeyspace:
		target.StorageScope = sessionauth.StorageScopeKeyspace
	default:
		return target, false
	}
	return target, ok && validSQLTarget(target)
}

func sqlPolicyHash(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

func sqlCanonicalBase64(text string) ([]byte, bool) {
	b, err := base64.RawURLEncoding.Strict().DecodeString(text)
	return b, err == nil && len(b) > 0 && base64.RawURLEncoding.EncodeToString(b) == text
}

// Decode rejects duplicate keys (including escaped equivalents), null, unknown
// struct fields, omitted fields and noncanonical numeric syntax before conversion.
func decodeSQLPolicyJSON(raw []byte, out any) error {
	if len(raw) == 0 || len(raw) > sqlPolicyMaxBytes {
		return ErrSQLTokenPolicyUnavailable
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	value, err := readSQLPolicyValue(d, 0)
	if err != nil {
		return ErrSQLTokenPolicyUnavailable
	}
	if _, err = d.Token(); err != io.EOF {
		return ErrSQLTokenPolicyUnavailable
	}
	if !sqlPolicyShape(value, reflect.TypeOf(out).Elem()) {
		return ErrSQLTokenPolicyUnavailable
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil {
		return ErrSQLTokenPolicyUnavailable
	}
	return nil
}

func readSQLPolicyValue(d *json.Decoder, depth int) (any, error) {
	if depth > 8 {
		return nil, ErrSQLTokenPolicyUnavailable
	}
	token, err := d.Token()
	if err != nil {
		return nil, err
	}
	switch v := token.(type) {
	case json.Delim:
		switch v {
		case '{':
			m := map[string]any{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return nil, err
				}
				name, ok := key.(string)
				if !ok || len(m) >= sqlPolicyMaxBindings {
					return nil, ErrSQLTokenPolicyUnavailable
				}
				if _, ok = m[name]; ok {
					return nil, ErrSQLTokenPolicyUnavailable
				}
				value, err := readSQLPolicyValue(d, depth+1)
				if err != nil {
					return nil, err
				}
				m[name] = value
			}
			end, err := d.Token()
			if err != nil || end != json.Delim('}') {
				return nil, ErrSQLTokenPolicyUnavailable
			}
			return m, nil
		case '[':
			items := []any{}
			for d.More() {
				if len(items) >= sqlPolicyMaxBindings {
					return nil, ErrSQLTokenPolicyUnavailable
				}
				v, err := readSQLPolicyValue(d, depth+1)
				if err != nil {
					return nil, err
				}
				items = append(items, v)
			}
			end, err := d.Token()
			if err != nil || end != json.Delim(']') {
				return nil, ErrSQLTokenPolicyUnavailable
			}
			return items, nil
		}
	case json.Number:
		n, err := strconv.ParseUint(string(v), 10, 64)
		if err != nil || strconv.FormatUint(n, 10) != string(v) {
			return nil, ErrSQLTokenPolicyUnavailable
		}
		return v, nil
	case string, bool:
		return v, nil
	}
	return nil, ErrSQLTokenPolicyUnavailable
}

func sqlPolicyShape(value any, typ reflect.Type) bool {
	switch typ.Kind() {
	case reflect.Struct:
		m, ok := value.(map[string]any)
		if !ok || len(m) != typ.NumField() {
			return false
		}
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			v, ok := m[f.Tag.Get("json")]
			if !ok || !sqlPolicyShape(v, f.Type) {
				return false
			}
		}
		return true
	case reflect.Map:
		m, ok := value.(map[string]any)
		if !ok {
			return false
		}
		for _, v := range m {
			if !sqlPolicyShape(v, typ.Elem()) {
				return false
			}
		}
		return true
	case reflect.Slice:
		items, ok := value.([]any)
		if !ok {
			return false
		}
		for _, v := range items {
			if !sqlPolicyShape(v, typ.Elem()) {
				return false
			}
		}
		return true
	case reflect.String:
		_, ok := value.(string)
		return ok
	case reflect.Bool:
		_, ok := value.(bool)
		return ok
	default:
		_, ok := value.(json.Number)
		return ok
	}
}

func readSQLPolicyFile(path string, private bool) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || (private && info.Mode().Perm() != 0600) || info.Size() > sqlPolicyMaxBytes {
		return nil, ErrSQLTokenPolicyUnavailable
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, sqlPolicyMaxBytes+1))
	if err != nil || len(b) > sqlPolicyMaxBytes {
		return nil, ErrSQLTokenPolicyUnavailable
	}
	return b, nil
}

func atomicSQLPolicyFile(dir, name string, value any) error {
	b, err := json.Marshal(value)
	if err != nil || len(b) > sqlPolicyMaxBytes {
		return ErrSQLTokenPolicyUnavailable
	}
	f, err := os.CreateTemp(dir, "."+name+".")
	if err != nil {
		return ErrSQLTokenPolicyUnavailable
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(f.Name(), filepath.Join(dir, name))
	}
	if err != nil {
		return ErrSQLTokenPolicyUnavailable
	}
	d, err := os.Open(dir)
	if err != nil {
		return ErrSQLTokenPolicyUnavailable
	}
	defer d.Close()
	if d.Sync() != nil {
		return ErrSQLTokenPolicyUnavailable
	}
	return nil
}

func readSQLAuthority(path string) (ed25519.PublicKey, error) {
	b, err := readSQLPolicyFile(path, false)
	if err != nil {
		return nil, ErrSQLTokenPolicyUnavailable
	}
	block, rest := pem.Decode(b)
	if block == nil || block.Type != "PUBLIC KEY" || len(bytes.TrimSpace(rest)) != 0 {
		return nil, ErrSQLTokenPolicyUnavailable
	}
	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, ErrSQLTokenPolicyUnavailable
	}
	pub, ok := key.(ed25519.PublicKey)
	if !ok || len(pub) != ed25519.PublicKeySize {
		return nil, ErrSQLTokenPolicyUnavailable
	}
	return pub, nil
}

func (doc SQLTokenPolicyDocument) policy() (SQLTokenPolicy, error) {
	p := SQLTokenPolicy{Issuer: doc.Issuer, Keys: map[string]*rsa.PublicKey{}, Bindings: map[string]SQLTokenBinding{}}
	var ok bool
	p.Target, ok = doc.Target.target()
	if !ok {
		return p, ErrSQLTokenPolicyUnavailable
	}
	p.SnapshotRevision, ok = sqlTokenRevision(doc.Revision)
	if !ok {
		return p, ErrSQLTokenPolicyUnavailable
	}
	if !sqlTokenString(doc.Issuer, sqlTokenMaxIssuerBytes) || doc.ValidFrom < 0 || doc.ValidUntil > sqlTokenMaxUnixSeconds || doc.ValidUntil <= doc.ValidFrom || doc.MaxTokenLifetimeSeconds <= 0 || doc.MaxTokenLifetimeSeconds > 900 || doc.MaxSessionDurationSeconds <= 0 || doc.MaxSessionDurationSeconds > 86400 || len(doc.Keys) > sqlPolicyMaxKeys || len(doc.Bindings) > sqlPolicyMaxBindings {
		return p, ErrSQLTokenPolicyUnavailable
	}
	p.ValidFrom = time.Unix(doc.ValidFrom, 0)
	p.ValidUntil = time.Unix(doc.ValidUntil, 0)
	p.MaxTokenLifetime = time.Duration(doc.MaxTokenLifetimeSeconds) * time.Second
	p.MaxSessionDuration = time.Duration(doc.MaxSessionDurationSeconds) * time.Second
	for id, encoded := range doc.Keys {
		b, ok := sqlCanonicalBase64(encoded)
		if !ok || !sqlTokenString(id, sqlTokenMaxIDBytes) {
			return p, ErrSQLTokenPolicyUnavailable
		}
		key, err := x509.ParsePKIXPublicKey(b)
		if err != nil {
			return p, ErrSQLTokenPolicyUnavailable
		}
		pub, ok := key.(*rsa.PublicKey)
		if !ok || !validSQLRSAKey(pub) {
			return p, ErrSQLTokenPolicyUnavailable
		}
		p.Keys[id] = pub
	}
	users := map[string]bool{}
	for id, d := range doc.Bindings {
		b := SQLTokenBinding{PrincipalID: d.PrincipalID, NativeUsername: d.NativeUsername, NativeHost: d.NativeHost, Enabled: d.Enabled, ExpiresAt: time.Unix(d.ExpiresAt, 0)}
		if !sqlTokenString(id, sqlTokenMaxIDBytes) || !sqlTokenString(b.PrincipalID, sqlTokenMaxIDBytes) || !sqlTokenString(b.NativeUsername, sqlTokenMaxIDBytes) || !sqlTokenString(b.NativeHost, sqlTokenMaxIDBytes) || d.ExpiresAt < 0 || d.ExpiresAt > sqlTokenMaxUnixSeconds {
			return p, ErrSQLTokenPolicyUnavailable
		}
		if b.BindingRevision, ok = sqlTokenRevision(d.BindingRevision); !ok {
			return p, ErrSQLTokenPolicyUnavailable
		}
		if b.PolicyRevision, ok = sqlTokenRevision(d.PolicyRevision); !ok {
			return p, ErrSQLTokenPolicyUnavailable
		}
		if b.DesiredGrantRevision, ok = sqlTokenRevision(d.DesiredGrantRevision); !ok {
			return p, ErrSQLTokenPolicyUnavailable
		}
		if b.AppliedGrantRevision, ok = sqlTokenRevision(d.AppliedGrantRevision); !ok {
			return p, ErrSQLTokenPolicyUnavailable
		}
		// A username is managed before host matching. Competing host bindings would
		// make native-record selection ambiguous and are rejected in this version.
		if users[b.NativeUsername] {
			return p, ErrSQLTokenPolicyUnavailable
		}
		users[b.NativeUsername] = true
		p.Bindings[id] = b
	}
	return p, nil
}

func sqlPolicyNamespace(issuer, id string) string { return issuer + "\n" + id }

func sqlWatermarkValid(w sqlTokenWatermark, target sessionauth.Target, authority ed25519.PublicKey) bool {
	if w.Version != sqlPolicyWireVersion || w.Target != sqlTargetDocument(target) || w.AuthorityFingerprint != sqlPolicyHash(authority) || w.HighTime < 0 || w.HighTime > sqlTokenMaxUnixSeconds || w.LowerTime < w.HighTime || w.UpperTime < w.LowerTime || w.UpperTime > sqlTokenMaxUnixSeconds || w.Keys == nil || w.Bindings == nil || w.ManagedUsernames == nil {
		return false
	}
	if w.Revision != "0" {
		if _, ok := sqlTokenRevision(w.Revision); !ok || !sqlDigestValid(w.Digest) {
			return false
		}
	} else if w.Digest != "" || w.HighTime != 0 || w.LowerTime != 0 || w.UpperTime != 0 || len(w.Keys) != 0 || len(w.Bindings) != 0 || len(w.ManagedUsernames) != 0 {
		return false
	}
	for id, k := range w.Keys {
		if !sqlNamespaceValid(id) || !sqlDigestValid(k.Fingerprint) {
			return false
		}
	}
	for id, b := range w.Bindings {
		_, a := sqlTokenRevision(b.BindingRevision)
		_, c := sqlTokenRevision(b.PolicyRevision)
		if !sqlNamespaceValid(id) || !a || !c || !sqlDigestValid(b.Digest) {
			return false
		}
	}
	for u, managed := range w.ManagedUsernames {
		if !managed || !sqlTokenString(u, sqlTokenMaxIDBytes) {
			return false
		}
	}
	return true
}

func sqlDigestValid(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == 32 && hex.EncodeToString(b) == s
}
func sqlNamespaceValid(s string) bool {
	parts := strings.Split(s, "\n")
	return len(parts) == 2 && sqlTokenString(parts[0], sqlTokenMaxIssuerBytes) && sqlTokenString(parts[1], sqlTokenMaxIDBytes)
}
