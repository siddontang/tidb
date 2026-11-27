#!/usr/bin/env python3
"""Live regression suite for two tenant accounts on one development TiDB listener.

Run directly with PyMySQL installed. Set TIDB_MULTI_KEYSPACE_DEV_ROOT_PASSWORD
to the test password for both accounts. TIDB_TEST_HOST / TIDB_TEST_PORT select
the listener. TIDB_TEST_TENANT_A / TIDB_TEST_TENANT_B select the keyspaces.
TIDB_TEST_PD selects the PD whose keyspace-qualified objects the PD cases read.
The PD cases run DDL in both tenants and need each tenant's DDL owner to make
the PD writes; they compare PD's rule ranges byte for byte. `--self-test` runs
the offline checks of those guards over recorded responses.
Each case creates and removes its own databases. Use dedicated development
tenants with similar DDL histories for the physical table-ID collision case.
"""

import json
import os
import sys
import time
import urllib.parse
import urllib.request
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

try:
    import pymysql
except ImportError:  # --self-test needs no driver
    pymysql = None


class TenantSQLTests(unittest.TestCase):
    def connect(self, tenant, account="root", password=None):
        db = pymysql.connect(
            host=os.getenv("TIDB_TEST_HOST", "127.0.0.1"),
            port=int(os.getenv("TIDB_TEST_PORT", "4000")),
            user=tenant + "." + account,
            password=os.environ["TIDB_MULTI_KEYSPACE_DEV_ROOT_PASSWORD"] if password is None else password,
            autocommit=True, connect_timeout=10, read_timeout=40, write_timeout=15,
        )
        self.addCleanup(db.close)
        return db

    def setUp(self):
        self.tenants = (os.getenv("TIDB_TEST_TENANT_A", "keyspace1"),
                        os.getenv("TIDB_TEST_TENANT_B", "keyspace2"))
        self.assertNotEqual(*self.tenants)
        self.schema = "tenant_test_" + uuid.uuid4().hex
        self.clients = []
        for index, tenant in enumerate(self.tenants):
            cursor = self.connect(tenant).cursor()
            self.addCleanup(cursor.close)
            cursor.execute("CREATE DATABASE " + self.schema)
            self.addCleanup(cursor.execute, "DROP DATABASE " + self.schema)
            cursor.execute("USE " + self.schema)
            cursor.execute("CREATE TABLE t (id INT PRIMARY KEY, marker INT)")
            cursor.execute("INSERT INTO t VALUES (1, %s)", (index + 1,))
            self.clients.append(cursor)

    def marker(self, cursor):
        cursor.execute("SELECT marker FROM t WHERE id = 1")
        return cursor.fetchall()

    def test_new_sessions_read_their_own_committed_rows(self):
        for index in (1, 0):
            with self.connect(self.tenants[index]).cursor() as cursor:
                cursor.execute("USE " + self.schema)
                self.assertEqual(self.marker(cursor), ((index + 1,),))

    def test_rollback_and_commit_on_overlapping_keys(self):
        a, b = self.clients
        for cursor in self.clients:
            cursor.execute("BEGIN")
        a.execute("UPDATE t SET marker = 10 WHERE id = 1")
        b.execute("UPDATE t SET marker = 20 WHERE id = 1")
        self.assertEqual(self.marker(a), ((10,),))
        self.assertEqual(self.marker(b), ((20,),))
        b.execute("COMMIT")
        a.execute("ROLLBACK")
        self.assertEqual(self.marker(a), ((1,),))
        self.assertEqual(self.marker(b), ((20,),))

    def test_ddl_in_one_tenant_leaves_peer_table_readable(self):
        a, b = self.clients
        a.execute("DROP TABLE t")
        self.assertEqual(self.marker(b), ((2,),))
        a.execute("CREATE TABLE t (description VARCHAR(32))")
        a.execute("INSERT INTO t VALUES ('new schema')")
        a.execute("SELECT description FROM t")
        self.assertEqual(a.fetchall(), (("new schema",),))
        self.assertEqual(self.marker(b), ((2,),))

    def test_session_variables_and_prepared_names_are_local(self):
        for index, cursor in enumerate(self.clients):
            cursor.execute("SET @increment = %s", ((index + 1) * 10,))
            cursor.execute("PREPARE read_marker FROM "
                           "'SELECT marker + @increment FROM t WHERE id = 1'")
        for index in (1, 0):
            cursor = self.clients[index]
            cursor.execute("EXECUTE read_marker")
            self.assertEqual(cursor.fetchall(), (((index + 1) * 11,),))
        self.clients[0].execute("DEALLOCATE PREPARE read_marker")
        self.clients[1].execute("EXECUTE read_marker")
        self.assertEqual(self.clients[1].fetchall(), ((22,),))

    def test_deleting_rows_in_one_tenant_keeps_peer_rows(self):
        a, b = self.clients
        a.execute("DELETE FROM t WHERE id = 1")
        self.assertEqual(self.marker(a), ())
        self.assertEqual(self.marker(b), ((2,),))
        a.execute("INSERT INTO t VALUES (1, 101)")
        b.execute("UPDATE t SET marker = 202 WHERE id = 1")
        self.assertEqual(self.marker(a), ((101,),))
        self.assertEqual(self.marker(b), ((202,),))

    def test_read_snapshot_and_later_commit_in_same_tenant(self):
        a, b = self.clients
        a.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        self.assertEqual(self.marker(a), ((1,),))
        with self.connect(self.tenants[0]).cursor() as writer:
            writer.execute("USE " + self.schema)
            writer.execute("UPDATE t SET marker = 99 WHERE id = 1")
        self.assertEqual(self.marker(a), ((1,),))
        self.assertEqual(self.marker(b), ((2,),))
        a.execute("COMMIT")
        self.assertEqual(self.marker(a), ((99,),))

    def test_table_and_column_catalogs_only_show_local_objects(self):
        for name, cursor in zip(("alpha", "beta"), self.clients):
            cursor.execute(f"CREATE TABLE {name} (col_{name} INT)")
        for name, cursor in zip(("alpha", "beta"), self.clients):
            cursor.execute("SHOW TABLES")
            self.assertEqual({row[0] for row in cursor}, {"t", name})
            cursor.execute("SELECT table_name, column_name FROM information_schema.columns "
                           "WHERE table_schema = %s", (self.schema,))
            self.assertCountEqual(cursor.fetchall(), [
                ("t", "id"), ("t", "marker"), (name, "col_" + name),
            ])

    def test_private_database_is_visible_only_to_its_owner(self):
        private = self.schema + "_private"
        a, b = self.clients
        a.execute("CREATE DATABASE " + private)
        self.addCleanup(a.execute, "DROP DATABASE " + private)
        a.execute(f"CREATE TABLE {private}.only_here (id INT PRIMARY KEY)")
        a.execute(f"INSERT INTO {private}.only_here VALUES (37)")
        a.execute(f"SELECT id FROM {private}.only_here")
        self.assertEqual(a.fetchall(), ((37,),))
        for cursor, expected in ((a, ((private,),)), (b, ())):
            cursor.execute("SELECT schema_name FROM information_schema.schemata "
                           "WHERE schema_name = %s", (private,))
            self.assertEqual(cursor.fetchall(), expected)
        with self.assertRaises(pymysql.MySQLError) as missing:
            b.execute("USE " + private)
        self.assertEqual(missing.exception.args[0], 1049)
        self.assertEqual(self.marker(b), ((2,),))

    def test_unique_constraint_and_failed_statement_are_tenant_local(self):
        for cursor in self.clients:
            cursor.execute("CREATE TABLE tickets (id INT PRIMARY KEY, tag INT UNIQUE)")
            cursor.execute("INSERT INTO tickets VALUES (1, 100)")
        a, b = self.clients
        with self.assertRaises(pymysql.MySQLError) as duplicate:
            a.execute("INSERT INTO tickets VALUES (2, 100)")
        self.assertEqual(duplicate.exception.args[0], 1062)
        b.execute("UPDATE tickets SET tag = 200 WHERE id = 1")
        b.execute("INSERT INTO tickets VALUES (2, 100)")
        for cursor, expected in ((a, ((1, 100),)), (b, ((1, 200), (2, 100)))):
            cursor.execute("SELECT id, tag FROM tickets ORDER BY id")
            self.assertEqual(cursor.fetchall(), expected)

    def test_secondary_index_ranges_and_indexed_updates(self):
        for tenant_id, cursor in enumerate(self.clients, start=1):
            cursor.execute("CREATE TABLE indexed_rows "
                           "(id INT PRIMARY KEY, bucket INT, payload INT, INDEX by_bucket (bucket))")
            cursor.executemany("INSERT INTO indexed_rows VALUES (%s, %s, %s)",
                               [(i, i // 2, 100 * tenant_id + i) for i in range(8)])
        self.clients[0].execute("UPDATE indexed_rows SET bucket = 9 WHERE id = 3")
        for tenant_id, cursor in enumerate(self.clients, start=1):
            query = ("SELECT id, payload FROM indexed_rows FORCE INDEX (by_bucket) "
                     "WHERE bucket BETWEEN 1 AND 2 ORDER BY id")
            cursor.execute("EXPLAIN " + query)
            self.assertTrue(any("IndexRangeScan" in row[0] and "by_bucket" in row[3]
                                for row in cursor.fetchall()))
            cursor.execute(query)
            ids = [2, 4, 5] if tenant_id == 1 else [2, 3, 4, 5]
            self.assertEqual(cursor.fetchall(), tuple((i, 100 * tenant_id + i) for i in ids))

    def test_join_results_after_truncate_and_reinsert(self):
        for cursor in self.clients:
            cursor.execute("CREATE TABLE detail (id INT PRIMARY KEY, qty INT)")
        a, b = self.clients
        a.execute("INSERT INTO detail VALUES (1, 10)")
        b.execute("INSERT INTO detail VALUES (1, 20)")
        query = "SELECT t.marker, detail.qty FROM t JOIN detail ON t.id = detail.id"
        a.execute("TRUNCATE TABLE detail")
        a.execute(query)
        self.assertEqual(a.fetchall(), ())
        b.execute(query)
        self.assertEqual(b.fetchall(), ((2, 20),))
        a.execute("INSERT INTO detail VALUES (1, 30)")
        for cursor, expected in ((a, (1, 30)), (b, (2, 20))):
            cursor.execute(query)
            self.assertEqual(cursor.fetchall(), (expected,))

    def test_tenant_accounts_with_independent_table_grants(self):
        # The logical account name and schema match. Credentials and grants differ.
        account = "u" + uuid.uuid4().hex[:12]
        passwords = [uuid.uuid4().hex for _ in self.tenants]
        for index, (tenant, admin) in enumerate(zip(self.tenants, self.clients)):
            user = tenant + "." + account
            admin.execute("CREATE USER %s@'%%' IDENTIFIED BY %s", (user, passwords[index]))
            self.addCleanup(admin.execute, "DROP USER %s@'%%'", (user,))
            privileges = "SELECT" if index == 0 else "SELECT, UPDATE"
            admin.execute(f"GRANT {privileges} ON {self.schema}.t TO %s@'%%'", (user,))

        for index, tenant in enumerate(self.tenants):
            with self.assertRaises(pymysql.MySQLError) as denied:
                self.connect(tenant, account, passwords[1 - index])
            self.assertEqual(denied.exception.args[0], 1045)
            client = self.connect(tenant, account, passwords[index]).cursor()
            self.addCleanup(client.close)
            client.execute("USE " + self.schema)
            self.assertEqual(self.marker(client), ((index + 1,),))
            if index == 0:
                with self.assertRaises(pymysql.MySQLError) as denied:
                    client.execute("UPDATE t SET marker = 19 WHERE id = 1")
                self.assertEqual(denied.exception.args[0], 8121)
            else:
                client.execute("UPDATE t SET marker = 29 WHERE id = 1")
        self.assertEqual(self.marker(self.clients[0]), ((1,),))
        self.assertEqual(self.marker(self.clients[1]), ((29,),))

    def test_identical_physical_table_ids_keep_row_and_index_keys_separate(self):
        def table_id(cursor, name):
            cursor.execute("SELECT tidb_table_id FROM information_schema.tables "
                           "WHERE table_schema = %s AND table_name = %s", (self.schema, name))
            value, = cursor.fetchone()
            return int(value)

        # Exercise a genuine collision using only DDL inside our scratch schemas.
        # Each CREATE normally consumes a table ID and job ID. A DROP consumes
        # a job ID and lets the two sequences meet if their parities differ.
        names = ["t", "t"]
        ids = [table_id(cursor, "t") for cursor in self.clients]
        for step in range(24):
            if ids[0] == ids[1]:
                break
            lag = 0 if ids[0] < ids[1] else 1
            cursor = self.clients[lag]
            if abs(ids[0] - ids[1]) % 2:
                cursor.execute("DROP TABLE " + names[lag])
            names[lag] = "collision_" + str(step)
            cursor.execute(f"CREATE TABLE {names[lag]} (id INT PRIMARY KEY, marker INT)")
            ids[lag] = table_id(cursor, names[lag])
        self.assertEqual(*ids, msg="Use dedicated tenants with similar DDL histories for this fixture")

        index_ids = []
        for index, (cursor, name) in enumerate(zip(self.clients, names)):
            cursor.execute(f"DELETE FROM {name}")
            cursor.execute(f"CREATE UNIQUE INDEX by_marker ON {name} (marker)")
            cursor.execute("SELECT index_id FROM information_schema.tidb_indexes "
                           "WHERE table_schema = %s AND table_name = %s AND key_name = 'by_marker'",
                           (self.schema, name))
            index_id, = cursor.fetchone()
            index_ids.append(int(index_id))
            cursor.execute(f"INSERT INTO {name} VALUES (1, 70), (2, %s)", (80 + index,))
        self.assertEqual(*index_ids)
        for index, (cursor, name) in enumerate(zip(self.clients, names)):
            cursor.execute(f"SELECT id, marker FROM {name} ORDER BY id")
            self.assertEqual(cursor.fetchall(), ((1, 70), (2, 80 + index)))
        a, b = self.clients
        a.execute(f"DELETE FROM {names[0]} WHERE marker = 70")
        for cursor, name, expected in ((a, names[0], ()), (b, names[1], ((1,),))):
            cursor.execute(f"SELECT id FROM {name} FORCE INDEX (by_marker) WHERE marker = 70")
            self.assertEqual(cursor.fetchall(), expected)

    def test_concurrent_updates_use_independent_rows(self):
        ready = Barrier(2)

        def increment(cursor, step):
            ready.wait(timeout=10)
            for _ in range(4):
                cursor.execute("UPDATE t SET marker = marker + %s WHERE id = 1", (step,))

        with ThreadPoolExecutor(max_workers=2) as pool:
            jobs = [pool.submit(increment, cursor, step)
                    for cursor, step in zip(self.clients, (1, 10))]
            for job in jobs:
                job.result()
        self.assertEqual(self.marker(self.clients[0]), ((5,),))
        self.assertEqual(self.marker(self.clients[1]), ((42,),))

    # Phase 3e (design §6.8): diagnostics from process-wide owners.

    def scalar(self, cursor, sql, args=None):
        cursor.execute(sql, args)
        row = cursor.fetchone()
        return row[0] if row else None

    def test_statement_summary_rows_are_tenant_local(self):
        a, b = self.clients
        alias = "probe_" + uuid.uuid4().hex[:12]
        for cursor, runs in ((a, 2), (b, 1)):
            for _ in range(runs):
                cursor.execute(f"SELECT 1 AS {alias}")
                cursor.fetchall()
        for cursor, want in ((a, 2), (b, 1)):
            for table in ("statements_summary", "statements_summary_history",
                          "cluster_statements_summary"):
                got = self.scalar(cursor, f"SELECT SUM(exec_count) FROM information_schema.{table} "
                                          "WHERE digest_text LIKE %s", ("%" + alias + "%",))
                self.assertEqual(int(got), want, table)
            self.assertEqual(self.scalar(cursor, "SELECT COUNT(*) FROM information_schema."
                                                 "statements_summary_evicted"), 0)

    def test_slow_query_entries_are_tenant_local(self):
        markers = ["slow_" + uuid.uuid4().hex[:12] for _ in self.clients]
        for cursor, marker in zip(self.clients, markers):
            cursor.execute(f"SELECT SLEEP(0.4), '{marker}'")
            cursor.fetchall()
        for cursor, own, other in zip(self.clients, markers, reversed(markers)):
            for table in ("slow_query", "cluster_slow_query"):
                # The probe passes the marker reversed, so a probe that is itself slow does not
                # add the marker to this tenant's slow log.
                count = lambda marker: self.scalar(
                    cursor, f"SELECT COUNT(*) FROM information_schema.{table} "
                            "WHERE query LIKE CONCAT('%%', REVERSE(%s), '%%')",
                    (marker[::-1],))
                self.assertEqual(count(own), 1, table)
                self.assertEqual(count(other), 0, table)
            cursor.execute("ADMIN SHOW SLOW RECENT 200")
            texts = " ".join(str(row[0]) for row in cursor.fetchall())
            self.assertIn(own, texts)
            self.assertNotIn(other, texts)

    def test_show_status_is_tenant_local(self):
        a, b = self.clients
        status = lambda cursor: (cursor.execute("SHOW STATUS LIKE 'ddl_schema_version'"),
                                 cursor.fetchall()[0][1])[1]
        before = status(a), status(b)
        a.execute("CREATE TABLE status_probe (id INT)")
        self.assertNotEqual(status(a), before[0])
        self.assertEqual(status(b), before[1])

    def test_lock_waits_deadlocks_and_transactions_are_tenant_local(self):
        a, b = self.clients
        a.execute("INSERT INTO t VALUES (2, 1)")
        holder = self.connect(self.tenants[0]).cursor()
        waiter = self.connect(self.tenants[0]).cursor()
        for cursor in (holder, waiter):
            cursor.execute("USE " + self.schema)
            cursor.execute("SET innodb_lock_wait_timeout = 20")
        holder.execute("BEGIN PESSIMISTIC")
        holder.execute("SELECT 1 AS trx_probe")
        holder.execute("UPDATE t SET marker = 3 WHERE id = 1")
        holder_ts = self.scalar(holder, "SELECT @@tidb_current_ts")
        waiter.execute("BEGIN PESSIMISTIC")
        waiter.execute("UPDATE t SET marker = 4 WHERE id = 2")
        with ThreadPoolExecutor(max_workers=1) as pool:
            blocked = pool.submit(waiter.execute, "UPDATE t SET marker = 4 WHERE id = 1")
            waits = 0
            for _ in range(50):
                waits = self.scalar(a, "SELECT COUNT(*) FROM information_schema.data_lock_waits "
                                       "WHERE current_holding_trx_id = %s", (holder_ts,))
                if waits:
                    break
                time.sleep(0.1)
            self.assertGreater(waits, 0, "the tenant sees its own lock wait")
            self.assertEqual(self.scalar(b, "SELECT COUNT(*) FROM information_schema.data_lock_waits"), 0)
            self.assertEqual(self.scalar(b, "SELECT COUNT(*) FROM information_schema.cluster_tidb_trx "
                                            "WHERE id = %s", (holder_ts,)), 0)
            # Close the cycle: the holder now waits for the waiter's row 2.
            with self.assertRaises(pymysql.err.OperationalError) as raised:
                holder.execute("UPDATE t SET marker = 5 WHERE id = 2")
                blocked.result(timeout=30)
            self.assertEqual(raised.exception.args[0], 1213)
            try:
                blocked.result(timeout=30)
            except pymysql.err.MySQLError:
                pass
        for cursor in (holder, waiter):
            cursor.execute("ROLLBACK")
        deadlock = lambda cursor: self.scalar(
            cursor, "SELECT COUNT(*) FROM information_schema.deadlocks "
                    "WHERE try_lock_trx_id = %s OR trx_holding_lock = %s", (holder_ts, holder_ts))
        self.assertGreater(deadlock(a), 0, "the tenant sees its own deadlock")
        self.assertEqual(deadlock(b), 0)

    def test_concurrent_ddl_keeps_each_tenants_schema(self):
        """Phase 3d's gate, live: both tenants alter their same-named table
        at once and plan with asynchronous statistics loads (job-ID and
        table-ID keyed state on a node), and each keeps only its own
        schema, rows and job history. ADD INDEX and ANALYZE are left to the
        serial test `two_keyspaces_run_ddl_with_colliding_ids_at_once`: a
        tenant's DDL owner and peers may be other nodes of the fleet (an
        older playground build runs neither this node's reorganization nor
        its ANALYZE broadcast)."""
        def alter(index):
            cursor = self.connect(self.tenants[index]).cursor()
            cursor.execute("USE " + self.schema)
            cursor.execute("SET tidb_stats_load_sync_wait = 0")
            cursor.execute("INSERT INTO t VALUES " + ", ".join("(%d, %d)" % (10 + i, index + 1) for i in range(index + 2)))
            for round_ in range(2):
                cursor.execute("ALTER TABLE t ADD COLUMN c_t%d_%d INT" % (index, round_))
                cursor.execute("SELECT * FROM t WHERE marker = %d AND id > 0" % (index + 1))
                cursor.fetchall()

        with ThreadPoolExecutor(2) as pool:
            list(pool.map(alter, (0, 1)))
        for index, cursor in enumerate(self.clients):
            other = 1 - index
            cursor.execute("SHOW COLUMNS FROM t")
            columns = [row[0] for row in cursor.fetchall()]
            self.assertEqual(columns, ["id", "marker", "c_t%d_0" % index, "c_t%d_1" % index])
            rows = 1 + index + 2
            self.assertEqual(self.scalar(cursor, "SELECT COUNT(*) FROM t"), rows)
            cursor.execute("SELECT query FROM information_schema.ddl_jobs WHERE db_name = %s AND table_name = 't' "
                           "AND query LIKE 'ALTER TABLE t ADD%%'", (self.schema,))
            queries = [row[0] for row in cursor.fetchall()]
            self.assertEqual(len(queries), 2, queries)
            self.assertTrue(all("_t%d_" % index in query and "_t%d_" % other not in query for query in queries),
                            queries)

    def test_process_wide_sources_are_refused_or_empty(self):
        for cursor in self.clients:
            for sql in ("SELECT * FROM metrics_schema.up",
                        "SELECT * FROM information_schema.cluster_log "
                        "WHERE time > '2026-01-01 00:00:00' AND time < '2030-01-01 00:00:00'"):
                with self.assertRaises(pymysql.err.MySQLError) as raised:
                    cursor.execute(sql)
                self.assertEqual(raised.exception.args[0], 1235, sql)
            for table in ("client_errors_summary_global", "memory_usage_ops_history"):
                self.assertEqual(self.scalar(cursor, f"SELECT COUNT(*) FROM information_schema.{table}"), 0)


    def pd_get(self, path):
        url = os.getenv("TIDB_TEST_PD", "http://127.0.0.1:2379") + path
        with urllib.request.urlopen(url, timeout=10) as response:
            return json.load(response)

    def keyspace_id(self, tenant):
        return self.pd_get("/pd/api/v2/keyspaces/" + urllib.parse.quote(tenant))["id"]

    def ddl_owner(self, cursor):
        """The address of the tenant's DDL owner (ADMIN SHOW DDL)."""
        cursor.execute("ADMIN SHOW DDL")
        return cursor.fetchone()[2]

    def run_pd_ddl(self, index, sql, owners):
        """Runs DDL that writes PD in tenant `index` until it succeeds, and
        records the tenant's DDL owner, whose job step made the PD write."""
        cursor = self.clients[index]
        owners[self.tenants[index]] = self.ddl_owner(cursor)
        run_ddl_until_success(cursor.execute, sql)

    def test_resource_groups_are_tenant_local(self):
        """Phase 3b: both tenants create a group of the same name, each on
        its own DDL owner; PD keeps it under each one's keyspace, and no
        other tenant reads it."""
        name = "rg_" + uuid.uuid4().hex[:12]
        owners = {}
        for index, cursor in enumerate(self.clients):
            self.addCleanup(cursor.execute, "DROP RESOURCE GROUP IF EXISTS " + name)
            self.run_pd_ddl(index, "CREATE RESOURCE GROUP IF NOT EXISTS %s RU_PER_SEC = %d"
                            % (name, 100 * (index + 1)), owners)
        require_owner_coverage(self.tenants, owners)
        for index, (tenant, cursor) in enumerate(zip(self.tenants, self.clients)):
            want = [100 * (index + 1)]
            cursor.execute("SELECT ru_per_sec FROM information_schema.resource_groups WHERE name = %s", (name,))
            self.assertEqual([int(row[0]) for row in cursor.fetchall()], want, tenant)
            groups = self.pd_get("/resource-manager/api/v1/config/groups?keyspace_name="
                                 + urllib.parse.quote(tenant))
            pd_ru = [group["r_u_settings"]["r_u"]["settings"]["fill_rate"]
                     for group in groups if group["name"] == name]
            self.assertEqual(pd_ru, want, tenant)

    def test_attributes_are_tenant_local(self):
        """Phase 3b: the same table gets a label rule in PD per tenant, named
        by its keyspace and ranged over exactly its keyspace's table bytes,
        and each tenant's ATTRIBUTES lists only its own."""
        options = ("deny", "allow")
        owners = {}
        for index, cursor in enumerate(self.clients):
            self.addCleanup(cursor.execute, "ALTER TABLE t ATTRIBUTES = DEFAULT")
            self.run_pd_ddl(index, "ALTER TABLE t ATTRIBUTES = 'merge_option=%s'" % options[index], owners)
        keyspace_ids = [self.keyspace_id(tenant) for tenant in self.tenants]
        table_ids = [self.scalar(cursor, "SELECT tidb_table_id FROM information_schema.tables "
                                         "WHERE table_schema = %s AND table_name = 't'", (self.schema,))
                     for cursor in self.clients]
        for index, cursor in enumerate(self.clients):
            cursor.execute("SELECT id, attributes FROM information_schema.attributes WHERE id LIKE %s",
                           ("%" + self.schema + "%",))
            self.assertEqual(cursor.fetchall(),
                             (("schema/%s/t" % self.schema, '"merge_option=%s"' % options[index]),),
                             self.tenants[index])
        rules = [rule for rule in self.pd_get("/pd/api/v1/config/region-label/rules") if self.schema in rule["id"]]
        verify_label_rules(self.tenants, owners, keyspace_ids, table_ids, self.schema, options, rules)

    def test_placement_policies_are_tenant_local(self):
        """Phase 3b: both tenants create a policy of the same name and read
        only their own, and attach it to a table on their own DDL owner,
        whose bundle PD keeps as keyspace-<id>-TiDB_DDL_<table id> over the
        table's keyspace bytes. ALTER RANGE global puts each tenant's range
        bundle under its keyspace, keyspace-<id>-TiDB_GLOBAL, over its whole
        keyspace; each tenant's previous global policy is put back."""
        name = "pp_" + uuid.uuid4().hex[:12]
        bundle = lambda group: self.pd_get("/pd/api/v1/config/placement-rule/" + group)
        ids = [self.keyspace_id(tenant) for tenant in self.tenants]
        owners = {}
        for index, cursor in enumerate(self.clients):
            cursor.execute("CREATE PLACEMENT POLICY %s FOLLOWERS = %d" % (name, index + 1))
            self.addCleanup(cursor.execute, "DROP PLACEMENT POLICY IF EXISTS " + name)
        # Every tenant's previous global policy goes back, whichever tenant
        # fails, before any of them changes it.
        for index, cursor in enumerate(self.clients):
            previous = range_policy(bundle("keyspace-%d-TiDB_GLOBAL" % ids[index]))
            self.addCleanup(cursor.execute, "ALTER RANGE global PLACEMENT POLICY " + previous)
        for index, cursor in enumerate(self.clients):
            self.addCleanup(cursor.execute, "DROP TABLE IF EXISTS tp")
            self.run_pd_ddl(index, "CREATE TABLE IF NOT EXISTS tp (id INT) PLACEMENT POLICY = " + name, owners)
        require_owner_coverage(self.tenants, owners)
        for index, cursor in enumerate(self.clients):
            table_id = self.scalar(cursor, "SELECT tidb_table_id FROM information_schema.tables "
                                           "WHERE table_schema = %s AND table_name = 'tp'", (self.schema,))
            group = "keyspace-%d-TiDB_DDL_%d" % (ids[index], table_id)
            verify_bundle(bundle(group), group, index + 2, table_range(ids[index], table_id))
        for index, cursor in enumerate(self.clients):
            self.assertEqual(self.scalar(cursor, "SELECT followers FROM information_schema.placement_policies "
                                                 "WHERE policy_name = %s", (name,)), index + 1)
            cursor.execute("ALTER RANGE global PLACEMENT POLICY " + name)
        for index, cursor in enumerate(self.clients):
            group = "keyspace-%d-TiDB_GLOBAL" % ids[index]
            verify_bundle(bundle(group), group, index + 2, keyspace_range(ids[index]))
            cursor.execute("SHOW PLACEMENT")
            self.assertIn(("RANGE TiDB_GLOBAL", "FOLLOWERS=%d" % (index + 1)),
                          [row[:2] for row in cursor.fetchall()])

    def test_pd_objects_without_a_keyspace_are_refused(self):
        """TiFlash rules and PD's schedulers and affinity groups are keyed by
        bare table IDs or keyspace-less ranges: refused per tenant."""
        for cursor in self.clients:
            for sql in ("ALTER TABLE t SET TIFLASH REPLICA 1",
                        "DISTRIBUTE TABLE t RULE = 'leader-scatter' ENGINE = 'tikv'",
                        "SHOW DISTRIBUTION JOBS",
                        "SHOW TABLE t DISTRIBUTIONS",
                        "SHOW AFFINITY"):
                with self.assertRaises(pymysql.err.MySQLError) as raised:
                    cursor.execute(sql)
                self.assertEqual(raised.exception.args[0], 1235, sql)


def is_split_decode_error(error):
    """The client-visible error of a read that met a region split whose new
    bounds it could not decode ("invalid marker byte")."""
    args = getattr(error, "args", ())
    return len(args) >= 2 and args[0] == 1105 and "invalid marker byte" in str(args[1])


def run_ddl_until_success(execute, sql, attempts=5, interval=1.0):
    """Runs `sql` (idempotent DDL) until it answers success. A statement that
    failed with the split decode error is retried; it must still end in a
    successful response: catalog state that appears after a failed response
    is not success."""
    for attempt in range(attempts):
        try:
            execute(sql)
            return
        except Exception as error:  # pymysql's OperationalError
            if not is_split_decode_error(error) or attempt == attempts - 1:
                raise AssertionError("%s did not succeed: %r" % (sql, error))
            time.sleep(interval)


def require_owner_coverage(tenants, owners):
    """Every tenant's DDL ran on its own DDL owner (`owners`: tenant to the
    owner that ran it): a case that exercised one tenant's owner only proves
    nothing about the other's."""
    missing = [tenant for tenant in tenants if not owners.get(tenant)]
    if missing:
        raise AssertionError("no DDL ran on the DDL owner of %s (exercised: %r)" % (missing, owners))


def encode_bytes(key):
    """Go `codec.EncodeBytes`: memcomparable groups of eight bytes."""
    out = bytearray()
    for index in range(0, len(key) + 1, 8):
        group = key[index:index + 8]
        pad = 8 - len(group)
        out += group + b"\x00" * pad + bytes([0xFF - pad])
    return bytes(out)


def table_prefix(table_id):
    """Go `tablecodec.GenTablePrefix`."""
    return b"t" + ((table_id ^ (1 << 63)) & ((1 << 64) - 1)).to_bytes(8, "big")


def keyspace_prefix(keyspace_id):
    """The API-V2 transaction prefix of a keyspace."""
    return b"x" + keyspace_id.to_bytes(3, "big")


def table_range(keyspace_id, table_id):
    """The hex region range a keyspace's label rule or table bundle covers
    (`codec.EncodeRegionRange` of the table prefix up to the next table's)."""
    prefix = keyspace_prefix(keyspace_id)
    return (encode_bytes(prefix + table_prefix(table_id)).hex(),
            encode_bytes(prefix + table_prefix(table_id + 1)).hex())


def keyspace_range(keyspace_id):
    """The hex region range of a whole keyspace (the global range)."""
    return (encode_bytes(keyspace_prefix(keyspace_id)).hex(),
            encode_bytes(keyspace_prefix(keyspace_id + 1)).hex())


def range_policy(bundle):
    """The policy a range bundle's first rule names (`<policy>_rule_...`),
    `default` without one."""
    rules = (bundle or {}).get("rules") or []
    rule_id = rules[0]["id"] if rules else ""
    position = rule_id.rfind("_rule_")
    return rule_id[:position] if position > 0 else "default"


def verify_label_rules(tenants, owners, keyspace_ids, table_ids, schema, options, rules):
    """PD's label rules of the case: one per tenant, ran on its owner,
    `keyspace/<id>/schema/<db>/t`, labelled with the tenant's option and
    ranged over exactly the tenant's table bytes."""
    require_owner_coverage(tenants, owners)
    by_id = {rule["id"]: rule for rule in rules}
    want = {"keyspace/%d/schema/%s/t" % (keyspace_id, schema): index
            for index, keyspace_id in enumerate(keyspace_ids)}
    if sorted(by_id) != sorted(want):
        raise AssertionError("PD's label rules %r, want %r" % (sorted(by_id), sorted(want)))
    for rule_id, index in want.items():
        rule = by_id[rule_id]
        if {"key": "merge_option", "value": options[index]} not in rule.get("labels", []):
            raise AssertionError("%s labels %r" % (rule_id, rule.get("labels")))
        ranges = [(item["start_key"], item["end_key"]) for item in rule.get("data") or []]
        expected = [table_range(keyspace_ids[index], table_ids[index])]
        if ranges != expected:
            raise AssertionError("%s ranges %r, want %r" % (rule_id, ranges, expected))


def verify_bundle(bundle, group, peers, expected_range):
    """A bundle of PD's: its rules are the group's, place `peers` peers and
    range over exactly `expected_range`."""
    rules = (bundle or {}).get("rules") or []
    if not rules:
        raise AssertionError("PD holds no rules under %s" % group)
    if {rule["group_id"] for rule in rules} != {group}:
        raise AssertionError("%s rules of groups %r" % (group, {rule["group_id"] for rule in rules}))
    if sum(rule["count"] for rule in rules) != peers:
        raise AssertionError("%s places %d peers, want %d" % (group, sum(rule["count"] for rule in rules), peers))
    for rule in rules:
        if (rule["start_key"], rule["end_key"]) != expected_range:
            raise AssertionError("%s rule %s ranges %r, want %r"
                                 % (group, rule["id"], (rule["start_key"], rule["end_key"]), expected_range))


class SelfTests(unittest.TestCase):
    """Offline checks that the guards reject what must not pass, over
    recorded responses (`--self-test`; no listener or PD needed)."""

    tenants = ("keyspace1", "keyspace2")

    def test_one_owner_coverage_is_rejected(self):
        with self.assertRaises(AssertionError):
            require_owner_coverage(self.tenants, {"keyspace2": "127.0.0.1:4000"})
        require_owner_coverage(self.tenants, {"keyspace1": "127.0.0.1:4001", "keyspace2": "127.0.0.1:4000"})

    def test_an_accepted_1105_is_rejected(self):
        class SplitError(Exception):
            pass
        def failing(sql):
            raise SplitError(1105, "invalid marker byte, group bytes \"\\x00\"")
        with self.assertRaises(AssertionError):
            run_ddl_until_success(failing, "ALTER TABLE t ATTRIBUTES = 'merge_option=deny'", interval=0)
        calls = []
        def flaky(sql):
            calls.append(sql)
            if len(calls) == 1:
                failing(sql)
        run_ddl_until_success(flaky, "ALTER TABLE t ATTRIBUTES = 'merge_option=deny'", interval=0)
        self.assertEqual(len(calls), 2)

    def test_a_range_mismatch_is_rejected(self):
        group = "keyspace-2-TiDB_DDL_120"
        good = {"rules": [{"id": "table_rule_120_0", "group_id": group, "count": 2,
                           "start_key": table_range(2, 120)[0], "end_key": table_range(2, 120)[1]}]}
        verify_bundle(good, group, 2, table_range(2, 120))
        keyspace_less = {"rules": [dict(good["rules"][0], start_key=encode_bytes(table_prefix(120)).hex(),
                                        end_key=encode_bytes(table_prefix(121)).hex())]}
        other_keyspace = {"rules": [dict(good["rules"][0], start_key=table_range(1, 120)[0],
                                         end_key=table_range(1, 120)[1])]}
        for bundle in (keyspace_less, other_keyspace, {"rules": []}):
            with self.assertRaises(AssertionError):
                verify_bundle(bundle, group, 2, table_range(2, 120))

    def test_the_recorded_vacuous_attributes_run_is_rejected(self):
        """The run that passed before: one DDL owner exercised (keyspace2's),
        a client-visible 1105 on keyspace1's ALTER, and PD's rule with an
        empty range list. Each guard rejects it."""
        owners = {"keyspace2": "127.0.0.1:4000"}
        rules = [{"id": "keyspace/2/schema/s/t", "labels": [{"key": "merge_option", "value": "allow"}],
                  "data": []}]
        with self.assertRaises(AssertionError):
            verify_label_rules(self.tenants, owners, [1, 2], [110, 110], "s", ("deny", "allow"), rules)
        owners["keyspace1"] = "127.0.0.1:4001"
        with self.assertRaises(AssertionError):
            verify_label_rules(self.tenants, owners, [1, 2], [110, 110], "s", ("deny", "allow"), rules)
        def recorded(sql):
            raise RuntimeError(1105, "invalid marker byte, group bytes")
        with self.assertRaises(AssertionError):
            run_ddl_until_success(recorded, "ALTER TABLE t ATTRIBUTES = 'merge_option=deny'", interval=0)
        complete = [
            {"id": "keyspace/%d/schema/s/t" % keyspace_id,
             "labels": [{"key": "merge_option", "value": option}],
             "data": [dict(zip(("start_key", "end_key"), table_range(keyspace_id, 110)))]}
            for keyspace_id, option in ((1, "deny"), (2, "allow"))
        ]
        verify_label_rules(self.tenants, owners, [1, 2], [110, 110], "s", ("deny", "allow"), complete)

    def test_the_encoding_matches_the_keyspace_codec(self):
        # tidb-ddl-label's encode_region_range of keyspace 1's table 100.
        self.assertEqual(encode_bytes(b""), bytes([0] * 8 + [0xF7]))
        # x 000001 t 8000000000000064, memcomparable in groups of eight.
        self.assertEqual(table_range(1, 100), ("7800000174800000ff0000000064000000fc",
                                               "7800000174800000ff0000000065000000fc"))
        self.assertEqual(keyspace_range(1), (encode_bytes(b"x\x00\x00\x01").hex(), encode_bytes(b"x\x00\x00\x02").hex()))


if __name__ == "__main__":
    if "--self-test" in sys.argv:
        sys.argv.remove("--self-test")
        unittest.main(defaultTest="SelfTests", verbosity=2)
    unittest.main(verbosity=2)
