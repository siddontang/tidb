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
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jws"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	pmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/server/internal/util"
	kvstore "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

// TestSQLTokenNativeWire uses real verified TLS and stock driver SQL against
// owned mock storage. A small literal packet client exercises commands that the
// stock driver does not expose (COM_RESET_CONNECTION and COM_CHANGE_USER).
func TestSQLTokenNativeWire(t *testing.T) {
	originalTestChan := server.RunInGoTestChan
	t.Cleanup(func() { server.RunInGoTestChan = originalTestChan })
	dir := t.TempDir()
	certFile, keyFile, roots := legacyTokenTLSCertificate(t, dir)
	tlsConfig := &tls.Config{RootCAs: roots, ServerName: "token.test", MinVersion: tls.VersionTLS12}
	require.NoError(t, mysql.RegisterTLSConfig("sql-token-native", tlsConfig))
	t.Cleanup(func() { mysql.DeregisterTLSConfig("sql-token-native") })
	publicAuthority, authority, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	authorityDER, err := x509.MarshalPKIXPublicKey(publicAuthority)
	require.NoError(t, err)
	authorityPath := filepath.Join(dir, "authority.pem")
	require.NoError(t, os.WriteFile(authorityPath, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: authorityDER}), 0600))
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	require.NoError(t, err)

	cfg := util.NewTestConfig()
	cfg.Port, cfg.Status.StatusPort = 0, 0
	cfg.Security.SSLCert, cfg.Security.SSLKey = certFile, keyFile
	cfg.Security.SQLToken = config.SQLTokenSecurity{
		Enabled: true, Audience: "urn:example:database:wire", AuthorityKey: authorityPath,
		PolicyFile: filepath.Join(dir, "response.json"), StateDir: filepath.Join(dir, "state"),
	}
	var store kv.Storage
	var dom *domain.Domain
	if kerneltype.IsNextGen() {
		// Customer domains read SYSTEM metadata through the real cross-keyspace
		// manager, which needs an owned etcd client even with mock SQL storage.
		integration.BeforeTestExternal(t)
		cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
		t.Cleanup(func() { cluster.Terminate(t) })
		originalSystemStore := kvstore.GetSystemStorage()
		t.Cleanup(func() { kvstore.SetSystemStorage(originalSystemStore) })
		systemStore, _ := testkit.CreateMockStoreAndDomainForKS(t, keyspace.System)
		_, err := privileges.SQLTokenTargetForStore(systemStore, cfg.Security.SQLToken.Audience)
		require.Error(t, err, "the internal SYSTEM keyspace must never admit external tokens")
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/beforeGetStore", func(getStore *func(string) (kv.Storage, error)) {
			*getStore = func(name string) (kv.Storage, error) {
				require.Equal(t, keyspace.System, name)
				return systemStore, nil
			}
		})
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/skipCloseStore", func(closeStore *bool) { *closeStore = false })
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/injectETCDCli", func(client **clientv3.Client, name string) {
			require.Equal(t, keyspace.System, name)
			*client = cluster.Client(0)
			cluster.TakeClient(0)
			etcd.SetEtcdCliByNamespace(*client, keyspace.MakeKeyspaceEtcdNamespace(systemStore.GetCodec()))
		})
		store, dom = testkit.CreateMockStoreAndDomainForKS(t, "sql_token_wire")
	} else {
		store, dom = testkit.CreateMockStoreAndDomain(t)
	}
	srv, err := server.NewServer(cfg, server.NewTiDBDriver(store))
	require.NoError(t, err)
	srv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(srv)
	server.RunInGoTestChan = make(chan struct{})
	runDone := make(chan error, 1)
	go func() { runDone <- srv.Run(nil) }()
	<-server.RunInGoTestChan
	t.Cleanup(func() {
		srv.Close()
		select {
		case err := <-runDone:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Error("owned SQL server did not stop")
		}
	})
	address := srv.ListenAddr().String()
	openDB := func(user, token, tlsName string) *sql.DB {
		client := mysql.NewConfig()
		client.Net, client.Addr, client.User, client.Passwd = "tcp", address, user, token
		client.TLSConfig, client.AllowCleartextPasswords = tlsName, true
		client.Timeout, client.ReadTimeout, client.WriteTimeout = 3*time.Second, 5*time.Second, 3*time.Second
		connector, err := mysql.NewConnector(client)
		require.NoError(t, err)
		db := sql.OpenDB(connector)
		db.SetMaxOpenConns(1)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		return db
	}
	admin := openDB("root", "", "sql-token-native")
	for _, statement := range []string{
		"CREATE DATABASE scoped_data",
		"CREATE TABLE scoped_data.items (id INT PRIMARY KEY, v INT)",
		"CREATE DATABASE unrelated_data",
		"CREATE TABLE unrelated_data.items (id INT)",
		"CREATE USER 'native_a'@'%' IDENTIFIED WITH 'tidb_sql_token_v1' REQUIRE SSL ACCOUNT LOCK",
		"CREATE USER 'native_b'@'127.0.0.1' IDENTIFIED WITH 'tidb_sql_token_v1' REQUIRE SSL ACCOUNT LOCK",
		"CREATE USER 'ordinary'@'127.0.0.1' IDENTIFIED BY ''",
		"GRANT SELECT,INSERT,UPDATE,DELETE ON scoped_data.* TO 'native_a'@'%'",
		"GRANT SELECT ON scoped_data.* TO 'native_b'@'127.0.0.1'",
		"ALTER USER 'native_a'@'%' REQUIRE SSL ACCOUNT UNLOCK",
		"ALTER USER 'native_b'@'127.0.0.1' REQUIRE SSL ACCOUNT UNLOCK",
	} {
		_, err := admin.Exec(statement)
		require.NoError(t, err)
	}
	var challenge privileges.SQLTokenPolicyChallenge
	readSQLTokenJSON(t, filepath.Join(cfg.Security.SQLToken.StateDir, "challenge.json"), &challenge)
	now := time.Now().Unix()
	policy := privileges.SQLTokenPolicyDocument{
		Target: challenge.Target, Issuer: "wire-issuer", Revision: "1", ValidFrom: now - 10, ValidUntil: now + 600,
		MaxTokenLifetimeSeconds: 300, MaxSessionDurationSeconds: 120,
		Keys: map[string]string{"wire-key": base64.RawURLEncoding.EncodeToString(keyDER)},
		Bindings: map[string]privileges.SQLTokenBindingDocument{
			"binding-a": {PrincipalID: "principal-a", NativeUsername: "native_a", NativeHost: "%", BindingRevision: "1", PolicyRevision: "1", Enabled: true, ExpiresAt: now + 600, DesiredGrantRevision: "1", AppliedGrantRevision: "1"},
			"binding-b": {PrincipalID: "principal-b", NativeUsername: "native_b", NativeHost: "127.0.0.1", BindingRevision: "1", PolicyRevision: "1", Enabled: true, ExpiresAt: now + 600, DesiredGrantRevision: "1", AppliedGrantRevision: "1"},
		},
	}
	var acknowledgedIssuedAt int64
	publish := func(leaseSeconds int64) {
		t.Helper()
		readSQLTokenJSON(t, filepath.Join(cfg.Security.SQLToken.StateDir, "challenge.json"), &challenge)
		require.Eventually(t, func() bool { return time.Now().Unix() >= challenge.MinimumIssuedAt }, 12*time.Second, 20*time.Millisecond)
		issuedAt := time.Now().Unix()
		response := privileges.SQLTokenPolicyResponse{
			Version: 1, Incarnation: challenge.Incarnation, Nonce: challenge.Nonce, Sequence: challenge.Sequence,
			Target: challenge.Target, IssuedAt: issuedAt, ValidUntil: issuedAt + leaseSeconds, Policy: policy,
		}
		payload, err := json.Marshal(response)
		require.NoError(t, err)
		signature := ed25519.Sign(authority, append([]byte("tidb-sql-policy-v1\n"), payload...))
		envelope, err := json.Marshal(privileges.SQLTokenPolicyEnvelope{Payload: base64.RawURLEncoding.EncodeToString(payload), Signature: base64.RawURLEncoding.EncodeToString(signature)})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(cfg.Security.SQLToken.PolicyFile+".tmp", envelope, 0600))
		require.NoError(t, os.Rename(cfg.Security.SQLToken.PolicyFile+".tmp", cfg.Security.SQLToken.PolicyFile))
		require.Eventually(t, func() bool {
			var status privileges.SQLTokenRuntimeStatus
			data, err := os.ReadFile(filepath.Join(cfg.Security.SQLToken.StateDir, "status.json"))
			return err == nil && json.Unmarshal(data, &status) == nil && status.Ready && status.AcceptedSequence == challenge.Sequence && status.Revision == policy.Revision
		}, 3*time.Second, 20*time.Millisecond)
		acknowledgedIssuedAt = issuedAt
	}
	makeToken := func(user string, mutate func(map[string]any)) string {
		t.Helper()
		binding, principal := "binding-a", "principal-a"
		if user == "native_b" {
			binding, principal = "binding-b", "principal-b"
		}
		b := policy.Bindings[binding]
		now := time.Now().Unix()
		issuedAt := acknowledgedIssuedAt
		if issuedAt == 0 {
			issuedAt = now - 2 // Only the pre-policy rejection uses no acknowledgment.
		}
		claims := map[string]any{"ver": 1, "iss": "wire-issuer", "aud": cfg.Security.SQLToken.Audience, "sub": user,
			"principal_id": principal, "binding_id": binding, "binding_revision": b.BindingRevision, "policy_revision": b.PolicyRevision,
			"iat": issuedAt, "nbf": issuedAt, "exp": now + 120, "jti": "wire-token"}
		if mutate != nil {
			mutate(claims)
		}
		payload, err := json.Marshal(claims)
		require.NoError(t, err)
		header := jws.NewHeaders()
		require.NoError(t, header.Set("kid", "wire-key"))
		require.NoError(t, header.Set("typ", "tidb-sql+jwt"))
		signed, err := jws.Sign(payload, jws.WithKey(jwa.RS256, key, jws.WithProtectedHeaders(header)))
		require.NoError(t, err)
		return string(signed)
	}

	// A configured but freshly booted receiver cannot authenticate from no policy.
	require.Error(t, openDB("native_a", makeToken("native_a", nil), "sql-token-native").Ping())
	publish(30)
	db := openDB("native_a", makeToken("native_a", nil), "sql-token-native")
	var nativeUser string
	require.NoError(t, db.QueryRow("SELECT CURRENT_USER()").Scan(&nativeUser))
	require.Equal(t, "native_a@%", nativeUser)
	for _, statement := range []string{"INSERT INTO scoped_data.items VALUES (1,10)", "UPDATE scoped_data.items SET v=20 WHERE id=1", "DELETE FROM scoped_data.items WHERE id=1"} {
		_, err := db.Exec(statement)
		require.NoError(t, err)
	}
	_, err = db.Exec("SELECT * FROM unrelated_data.items")
	require.Error(t, err)
	_, err = db.Exec("SHOW SESSION_STATES")
	require.Error(t, err)
	_, err = db.Exec("ALTER USER USER() IDENTIFIED BY 'escape'")
	require.Error(t, err)
	for _, mutation := range []func(map[string]any){
		func(c map[string]any) { c["aud"] = "urn:example:database:other" },
		func(c map[string]any) { c["principal_id"] = "principal-b" },
		func(c map[string]any) { c["binding_revision"] = "2" },
		func(c map[string]any) { c["exp"] = time.Now().Unix() - 1 },
		func(c map[string]any) { c["nbf"] = time.Now().Unix() + 20 },
	} {
		require.Error(t, openDB("native_a", makeToken("native_a", mutation), "sql-token-native").Ping())
	}
	require.Error(t, openDB("native_a", makeToken("native_a", nil), "false").Ping(), "new profile requires TLS even without global transport requirement")

	// RESET preserves the original native host identity and its grants.
	wire := newSQLTokenWireClient(t, address, tlsConfig, "native_a", makeToken("native_a", nil))
	_, err = admin.Exec("CREATE USER 'native_a'@'127.0.0.1' IDENTIFIED WITH 'tidb_sql_token_v1'")
	require.NoError(t, err)
	require.NoError(t, wire.command(0x1f, nil))
	rows, err := wire.query("SELECT CURRENT_USER()")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"native_a@%"}}, rows)
	_, err = admin.Exec("DROP USER 'native_a'@'127.0.0.1'")
	require.NoError(t, err)
	// Failed change preserves the old guard/native identity; success replaces it.
	require.Error(t, wire.changeUser("native_b", "invalid", ""))
	rows, err = wire.query("SELECT CURRENT_USER()")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"native_a@%"}}, rows)
	require.Error(t, wire.changeUser("native_b", makeToken("native_b", nil), "missing_database"))
	rows, err = wire.query("SELECT CURRENT_USER()")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"native_a@%"}}, rows)
	require.NoError(t, wire.changeUser("native_b", makeToken("native_b", nil), ""))
	rows, err = wire.query("SELECT CURRENT_USER()")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"native_b@127.0.0.1"}}, rows)
	require.NoError(t, wire.changeUser("ordinary", "", ""))
	require.NoError(t, wire.changeUser("native_a", makeToken("native_a", nil), ""))

	// Revocation terminates idle/prepared and active work. A different binding
	// stays usable; database/sql connections are pinned so reconnect cannot mask it.
	held, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer held.Close()
	stmt, err := held.PrepareContext(context.Background(), "SELECT 1")
	require.NoError(t, err)
	defer stmt.Close()
	transactionDB := openDB("native_a", makeToken("native_a", nil), "sql-token-native")
	transactionConn, err := transactionDB.Conn(context.Background())
	require.NoError(t, err)
	defer transactionConn.Close()
	_, err = transactionConn.ExecContext(context.Background(), "BEGIN PESSIMISTIC")
	require.NoError(t, err)
	_, err = transactionConn.ExecContext(context.Background(), "INSERT INTO scoped_data.items VALUES (99,99)")
	require.NoError(t, err)
	active := newSQLTokenWireClient(t, address, tlsConfig, "native_a", makeToken("native_a", nil))
	activeRows, err := active.query("SELECT CONNECTION_ID()")
	require.NoError(t, err)
	activeID, err := strconv.ParseUint(activeRows[0][0], 10, 64)
	require.NoError(t, err)
	queryDone := make(chan error, 1)
	go func() { _, err := active.query("SELECT SLEEP(20)"); queryDone <- err }()
	require.Eventually(t, func() bool {
		var count int
		err := admin.QueryRow("SELECT COUNT(*) FROM information_schema.processlist WHERE ID=? AND INFO LIKE 'SELECT SLEEP%'", activeID).Scan(&count)
		return err == nil && count == 1
	}, 3*time.Second, 20*time.Millisecond)
	b := policy.Bindings["binding-a"]
	b.Enabled = false
	b.BindingRevision, b.PolicyRevision = "2", "2"
	policy.Bindings["binding-a"] = b
	policy.Revision = "2"
	publish(30)
	select {
	case err := <-queryDone:
		require.Error(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("revoked active SQL did not terminate")
	}
	require.Eventually(t, func() bool { return held.PingContext(context.Background()) != nil }, 3*time.Second, 20*time.Millisecond)
	_, err = stmt.QueryContext(context.Background())
	require.Error(t, err)
	require.Error(t, wire.command(0x1f, nil))
	require.Error(t, openDB("native_a", makeToken("native_a", nil), "sql-token-native").Ping())
	_, err = transactionConn.ExecContext(context.Background(), "COMMIT")
	require.Error(t, err)
	var uncommittedRows int
	require.NoError(t, admin.QueryRow("SELECT COUNT(*) FROM scoped_data.items WHERE id=99").Scan(&uncommittedRows))
	require.Zero(t, uncommittedRows)
	// Reusing the exact locked key also proves revoked transaction cleanup,
	// rather than merely observing MVCC's invisibility of uncommitted data.
	cleanupContext, cancelCleanup := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelCleanup()
	_, err = admin.ExecContext(cleanupContext, "INSERT INTO scoped_data.items VALUES (99,100)")
	require.NoError(t, err)
	unrelated := openDB("native_b", makeToken("native_b", nil), "sql-token-native")
	require.NoError(t, unrelated.Ping())

	// Native privilege changes terminate prior held sessions; native grants are
	// authoritative on the next explicitly authorized login (not a signed grant cap).
	otherHeld, err := unrelated.Conn(context.Background())
	require.NoError(t, err)
	defer otherHeld.Close()
	_, err = admin.Exec("REVOKE SELECT ON scoped_data.* FROM 'native_b'@'127.0.0.1'")
	require.NoError(t, err)
	require.Eventually(t, func() bool { return otherHeld.PingContext(context.Background()) != nil }, 3*time.Second, 20*time.Millisecond)
	fresh := openDB("native_b", makeToken("native_b", nil), "sql-token-native")
	require.NoError(t, fresh.Ping())
	_, err = fresh.Exec("SELECT * FROM scoped_data.items")
	require.Error(t, err)

	// With no fresh authority responses, a cached response cannot renew the lease.
	publish(30) // Obtain a newly issued challenge for the intentionally short lease.
	publish(12)
	leaseHeld, err := fresh.Conn(context.Background())
	require.NoError(t, err)
	defer leaseHeld.Close()
	require.Eventually(t, func() bool { return leaseHeld.PingContext(context.Background()) != nil }, 15*time.Second, 30*time.Millisecond)
	require.Error(t, openDB("native_b", makeToken("native_b", nil), "sql-token-native").Ping())
}

func readSQLTokenJSON(t *testing.T, path string, into any) {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, into))
}

// sqlTokenWireClient deliberately implements only the small test protocol subset;
// credentials never appear in assertion errors or packet dumps.
type sqlTokenWireClient struct {
	conn     net.Conn
	sequence byte
}

func newSQLTokenWireClient(t *testing.T, address string, tlsConfig *tls.Config, user, token string) *sqlTokenWireClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, 3*time.Second)
	require.NoError(t, err)
	c := &sqlTokenWireClient{conn: conn}
	t.Cleanup(func() { _ = c.conn.Close() })
	_, err = c.read()
	require.NoError(t, err)
	capabilities := uint32(pmysql.ClientProtocol41 | pmysql.ClientSSL | pmysql.ClientSecureConnection | pmysql.ClientPluginAuth | pmysql.ClientTransactions)
	request := make([]byte, 32)
	binary.LittleEndian.PutUint32(request, capabilities)
	request[8] = pmysql.DefaultCollationID
	require.NoError(t, c.write(request))
	tlsConn := tls.Client(conn, tlsConfig.Clone())
	require.NoError(t, tlsConn.Handshake())
	c.conn = tlsConn
	response := append(append([]byte{}, request...), []byte(user)...)
	response = append(response, 0, 0)
	response = append(response, []byte("mysql_native_password")...)
	response = append(response, 0)
	require.NoError(t, c.write(response))
	require.NoError(t, c.finishAuth(token))
	return c
}

func (c *sqlTokenWireClient) read() ([]byte, error) {
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var header [4]byte
	if _, err := io.ReadFull(c.conn, header[:]); err != nil {
		return nil, err
	}
	if header[3] != c.sequence {
		return nil, errors.New("unexpected MySQL packet sequence")
	}
	c.sequence++
	size := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	if size > 1<<20 {
		return nil, errors.New("oversized MySQL test response")
	}
	data := make([]byte, size)
	_, err := io.ReadFull(c.conn, data)
	return data, err
}
func (c *sqlTokenWireClient) write(data []byte) error {
	_ = c.conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	packet := []byte{byte(len(data)), byte(len(data) >> 8), byte(len(data) >> 16), c.sequence}
	c.sequence++
	_, err := c.conn.Write(append(packet, data...))
	return err
}
func (c *sqlTokenWireClient) finishAuth(token string) error {
	response, err := c.read()
	if err != nil {
		return err
	}
	if len(response) == 0 {
		return errors.New("empty MySQL authentication response")
	}
	if response[0] == 0xfe {
		plugin := string(response[1:])
		var answer []byte
		if len(plugin) >= len("mysql_clear_password") && plugin[:len("mysql_clear_password")] == "mysql_clear_password" {
			answer = append([]byte(token), 0)
		} else if token != "" {
			return errors.New("unexpected authentication plugin")
		}
		if err := c.write(answer); err != nil {
			return err
		}
		response, err = c.read()
		if err != nil {
			return err
		}
	}
	if len(response) == 0 || response[0] != 0 {
		return errors.New("MySQL authentication rejected")
	}
	return nil
}
func (c *sqlTokenWireClient) changeUser(user, token, database string) error {
	c.sequence = 0
	packet := append([]byte{0x11}, []byte(user)...)
	packet = append(packet, 0, 0)
	packet = append(packet, []byte(database)...)
	packet = append(packet, 0, pmysql.DefaultCollationID, 0)
	packet = append(packet, []byte("mysql_native_password")...)
	packet = append(packet, 0)
	if err := c.write(packet); err != nil {
		return err
	}
	return c.finishAuth(token)
}
func (c *sqlTokenWireClient) command(command byte, data []byte) error {
	c.sequence = 0
	if err := c.write(append([]byte{command}, data...)); err != nil {
		return err
	}
	response, err := c.read()
	if err != nil {
		return err
	}
	if len(response) == 0 || response[0] != 0 {
		return errors.New("MySQL command rejected")
	}
	return nil
}
func (c *sqlTokenWireClient) query(query string) ([][]string, error) {
	c.sequence = 0
	if err := c.write(append([]byte{0x03}, []byte(query)...)); err != nil {
		return nil, err
	}
	header, err := c.read()
	if err != nil {
		return nil, err
	}
	if len(header) != 1 || header[0] > 64 {
		return nil, errors.New("unexpected MySQL query response")
	}
	count := int(header[0])
	for range count + 1 {
		if _, err := c.read(); err != nil {
			return nil, err
		}
	}
	var rows [][]string
	for {
		packet, err := c.read()
		if err != nil {
			return nil, err
		}
		if len(packet) > 0 && packet[0] == 0xfe && len(packet) < 9 {
			return rows, nil
		}
		row := make([]string, 0, count)
		for range count {
			if len(packet) == 0 || int(packet[0])+1 > len(packet) {
				return nil, errors.New("invalid MySQL test row")
			}
			n := int(packet[0])
			row = append(row, string(packet[1:n+1]))
			packet = packet[n+1:]
		}
		rows = append(rows, row)
	}
}
