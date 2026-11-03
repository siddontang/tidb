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

package tls

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jws"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/pingcap/tidb/pkg/errno"
	pmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/server/tests/servertestkit"
	tlsutil "github.com/pingcap/tidb/pkg/util/tls"
	"github.com/stretchr/testify/require"
)

// TestLegacyTokenVerifiedTLS exercises native authentication and SQL over actual
// MySQL/TLS connections. Storage is an owned mock store, not a deployed TiKV
// cluster. Accepting an unrelated audience is an intentional legacy limitation;
// this test is not evidence of Instance-scoped authentication or revocation.
func TestLegacyTokenVerifiedTLS(t *testing.T) {
	var sqlAddr, statusAddr, fixtureDir string
	originalJWKS := privileges.GlobalJWKS
	originalTransport := tlsutil.RequireSecureTransport.Load()
	originalTestChan := server.RunInGoTestChan
	// This fixture changes process-global state and must remain sequential.
	t.Run("wire", func(t *testing.T) {
		fixtureDir = t.TempDir()
		// Register before creating the server so its cleanup runs first. Loading
		// directly avoids starting a global JWKS refresh goroutine for this test.
		t.Cleanup(func() {
			privileges.GlobalJWKS = originalJWKS
			tlsutil.RequireSecureTransport.Store(originalTransport)
			server.RunInGoTestChan = originalTestChan
		})
		certFile, keyFile, roots := legacyTokenTLSCertificate(t, fixtureDir)
		for _, name := range []string{"token-verified", "token-wrong-host"} {
			serverName := "token.test"
			if name == "token-wrong-host" {
				serverName = "wrong.test"
			}
			require.NoError(t, mysql.RegisterTLSConfig(name, &tls.Config{
				RootCAs: roots, ServerName: serverName, MinVersion: tls.VersionTLS12,
			}))
			t.Cleanup(func() { mysql.DeregisterTLSConfig(name) })
		}

		signingKey, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)
		publicKey, err := jwk.FromRaw(&signingKey.PublicKey)
		require.NoError(t, err)
		require.NoError(t, publicKey.Set(jwk.AlgorithmKey, jwa.RS256))
		require.NoError(t, publicKey.Set(jwk.KeyIDKey, "wire-key"))
		require.NoError(t, publicKey.Set(jwk.KeyUsageKey, "sig"))
		keySet := jwk.NewSet()
		require.NoError(t, keySet.AddKey(publicKey))
		publicJSON, err := json.Marshal(keySet)
		require.NoError(t, err)
		jwksFile := filepath.Join(fixtureDir, "jwks.json")
		require.NoError(t, os.WriteFile(jwksFile, publicJSON, 0600))
		require.NoError(t, privileges.GlobalJWKS.LoadJWKS4AuthToken(nil, nil, jwksFile, time.Hour))

		cfg := util.NewTestConfig()
		cfg.Port, cfg.Status.StatusPort = 0, 0
		cfg.Status.ReportStatus = true
		cfg.Security.SSLCert, cfg.Security.SSLKey = certFile, keyFile
		suite := servertestkit.CreateTidbTestSuiteWithCfg(t, cfg)
		sqlAddr, statusAddr = suite.Server.ListenAddr().String(), suite.Server.StatusListenerAddr().String()
		openDB := func(t *testing.T, user, token, tlsName string) *sql.DB {
			t.Helper()
			client := mysql.NewConfig()
			client.Net, client.Addr, client.User, client.Passwd = "tcp", sqlAddr, user, token
			client.TLSConfig, client.AllowCleartextPasswords = tlsName, true
			client.Timeout, client.ReadTimeout, client.WriteTimeout = 5*time.Second, 5*time.Second, 5*time.Second
			connector, err := mysql.NewConnector(client)
			require.NoError(t, err)
			db := sql.OpenDB(connector)
			db.SetMaxOpenConns(1)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			return db
		}
		admin := openDB(t, "root", "", "token-verified")
		for _, statement := range []string{
			"SET GLOBAL require_secure_transport = ON",
			"CREATE DATABASE wire_allowed",
			"CREATE TABLE wire_allowed.items (id INT PRIMARY KEY, value INT)",
			"CREATE DATABASE wire_other",
			"CREATE TABLE wire_other.items (id INT PRIMARY KEY)",
			`CREATE USER 'wire_token'@'127.0.0.1' IDENTIFIED WITH '` + pmysql.AuthTiDBAuthToken + `' REQUIRE TOKEN_ISSUER 'wire-issuer' ATTRIBUTE '{"email":"wire@example.invalid"}'`,
			"GRANT SELECT, INSERT, UPDATE, DELETE ON wire_allowed.* TO 'wire_token'@'127.0.0.1'",
		} {
			_, err := admin.Exec(statement)
			require.NoError(t, err)
		}
		makeToken := func(t *testing.T, key *rsa.PrivateKey, mutate func(map[string]any)) string {
			t.Helper()
			now := time.Now().Unix()
			claims := map[string]any{
				jwt.SubjectKey: "wire_token", "email": "wire@example.invalid", jwt.IssuerKey: "wire-issuer",
				jwt.IssuedAtKey: now - 1, jwt.ExpirationKey: now + 300, jwt.AudienceKey: "tidb-cloud:instance:wire-instance",
			}
			if mutate != nil {
				mutate(claims)
			}
			payload, err := json.Marshal(claims)
			require.NoError(t, err)
			header := jws.NewHeaders()
			require.NoError(t, header.Set(jws.KeyIDKey, "wire-key"))
			token, err := jws.Sign(payload, jws.WithKey(jwa.RS256, key, jws.WithProtectedHeaders(header)))
			require.NoError(t, err)
			return string(token)
		}
		validToken := makeToken(t, signingKey, nil)
		t.Run("native_grants_and_CRUD", func(t *testing.T) {
			db := openDB(t, "wire_token", validToken, "token-verified")
			var currentUser, statusName, cipher string
			require.NoError(t, db.QueryRow("SELECT CURRENT_USER()").Scan(&currentUser))
			require.Equal(t, "wire_token@127.0.0.1", currentUser)
			require.NoError(t, db.QueryRow("SHOW SESSION STATUS LIKE 'Ssl_cipher'").Scan(&statusName, &cipher))
			require.NotEmpty(t, cipher)
			_, err := db.Exec("INSERT INTO wire_allowed.items VALUES (1, 10)")
			require.NoError(t, err)
			_, err = db.Exec("UPDATE wire_allowed.items SET value = 20 WHERE id = 1")
			require.NoError(t, err)
			var value int
			require.NoError(t, db.QueryRow("SELECT value FROM wire_allowed.items WHERE id = 1").Scan(&value))
			require.Equal(t, 20, value)
			_, err = db.Exec("DELETE FROM wire_allowed.items WHERE id = 1")
			require.NoError(t, err)
			require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM wire_allowed.items").Scan(&value))
			require.Zero(t, value)
			for _, statement := range []string{"SELECT * FROM wire_other.items", "SELECT * FROM mysql.user"} {
				_, err = db.Exec(statement)
				var mysqlErr *mysql.MySQLError
				require.ErrorAs(t, err, &mysqlErr)
				require.EqualValues(t, errno.ErrTableaccessDenied, mysqlErr.Number)
			}
		})
		wrongKey, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)
		for _, tc := range []struct {
			name, token, tlsName string
			code                 uint16
		}{
			{"wrong_subject", makeToken(t, signingKey, func(c map[string]any) { c[jwt.SubjectKey] = "other_user" }), "token-verified", errno.ErrAccessDenied},
			{"expired", makeToken(t, signingKey, func(c map[string]any) {
				c[jwt.IssuedAtKey] = time.Now().Add(-2 * time.Minute).Unix()
				c[jwt.ExpirationKey] = time.Now().Add(-time.Minute).Unix()
			}), "token-verified", errno.ErrAccessDenied},
			{"bad_signature", makeToken(t, wrongKey, nil), "token-verified", errno.ErrAccessDenied},
			// require_secure_transport rejects before the clear-password switch;
			// this is not a test of a per-account REQUIRE SSL transport boundary.
			{"plaintext", validToken, "false", errno.ErrSecureTransportRequired},
		} {
			t.Run(tc.name, func(t *testing.T) {
				db := openDB(t, "wire_token", tc.token, tc.tlsName)
				err := db.PingContext(context.Background())
				if err != nil && strings.Contains(err.Error(), tc.token) {
					t.Fatal("authentication error exposed the test credential")
				}
				var mysqlErr *mysql.MySQLError
				require.ErrorAs(t, err, &mysqlErr)
				require.Equal(t, tc.code, mysqlErr.Number)
			})
		}
		t.Run("hostname_verification", func(t *testing.T) {
			db := openDB(t, "wire_token", validToken, "token-wrong-host")
			var hostnameErr x509.HostnameError
			require.ErrorAs(t, db.PingContext(context.Background()), &hostnameErr)
		})
		t.Run("legacy_accepts_unrelated_audience", func(t *testing.T) {
			token := makeToken(t, signingKey, func(c map[string]any) { c[jwt.AudienceKey] = "tidb-cloud:instance:unrelated" })
			db := openDB(t, "wire_token", token, "token-verified")
			require.NoError(t, db.PingContext(context.Background()))
		})
	})
	// Child cleanups have already closed the clients, server, mock store and
	// removed owned files; verify observable teardown without deleting by name.
	for _, address := range []string{sqlAddr, statusAddr} {
		if address != "" {
			conn, err := net.DialTimeout("tcp", address, time.Second)
			if conn != nil {
				_ = conn.Close()
			}
			require.Error(t, err, "owned listener remained open after cleanup")
		}
	}
	if fixtureDir != "" {
		_, err := os.Stat(fixtureDir)
		require.True(t, os.IsNotExist(err), "owned credential directory remained after cleanup")
	}
	require.True(t, originalJWKS == privileges.GlobalJWKS)
	require.Equal(t, originalTransport, tlsutil.RequireSecureTransport.Load())
	require.True(t, originalTestChan == server.RunInGoTestChan)
}

func legacyTokenTLSCertificate(t *testing.T, dir string) (string, string, *x509.CertPool) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "token.test"}, DNSNames: []string{"token.test"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	certFile, keyFile := filepath.Join(dir, "server-cert.pem"), filepath.Join(dir, "server-key.pem")
	require.NoError(t, os.WriteFile(certFile, certPEM, 0600))
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0600))
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(certPEM))
	return certFile, keyFile, roots
}
