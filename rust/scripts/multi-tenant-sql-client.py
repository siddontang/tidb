#!/usr/bin/env python3
"""Live regression suite for two tenant accounts on one development TiDB listener.

Run directly with PyMySQL installed. Set TIDB_MULTI_KEYSPACE_DEV_ROOT_PASSWORD
to the test password for both accounts. TIDB_TEST_HOST / TIDB_TEST_PORT select
the listener. TIDB_TEST_TENANT_A / TIDB_TEST_TENANT_B select the keyspaces.
TIDB_TEST_PD selects the PD whose keyspace-qualified objects the PD cases read.
Each case creates and removes its own databases. Use dedicated development
tenants with similar DDL histories for the physical table-ID collision case.
"""

import json
import os
import time
import urllib.parse
import urllib.request
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pymysql


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
                count = lambda marker: self.scalar(
                    cursor, f"SELECT COUNT(*) FROM information_schema.{table} WHERE query LIKE %s",
                    ("%" + marker + "%",))
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

    def execute_through_split(self, cursor, sql, done):
        """Runs DDL that puts a PD rule over a tenant's table. PD then splits
        the tenant's regions at the rule's bounds, and a read that meets the
        split can fail to decode the new region's bounds ("invalid marker
        byte"; the client and the playground's TiKV, not the rule): the
        statement fails while its job finishes, which `done` observes."""
        try:
            cursor.execute(sql)
        except pymysql.err.OperationalError as error:
            if error.args[0] != 1105 or "invalid marker byte" not in error.args[1]:
                raise
            for _ in range(50):
                if done():
                    return
                time.sleep(0.2)
            raise

    def ddl_owned_here(self):
        """The tenants whose DDL owner is this node. A tenant's job steps
        write PD on its DDL owner, which may be another node of the fleet
        (the playground's older multi-keyspace node refuses them)."""
        here = "%s:%s" % (os.getenv("TIDB_TEST_HOST", "127.0.0.1"), os.getenv("TIDB_TEST_PORT", "4000"))
        owned = []
        for index, cursor in enumerate(self.clients):
            cursor.execute("ADMIN SHOW DDL")
            if cursor.fetchone()[2] == here:
                owned.append(index)
        if not owned:
            self.skipTest("this node owns neither tenant's DDL")
        return owned

    def test_resource_groups_are_tenant_local(self):
        """Phase 3b: the tenants whose DDL this node owns create a group of
        the same name; PD keeps it under each one's keyspace, and no other
        tenant reads it."""
        owned = self.ddl_owned_here()
        name = "rg_" + uuid.uuid4().hex[:12]
        for index in owned:
            self.clients[index].execute("CREATE RESOURCE GROUP %s RU_PER_SEC = %d" % (name, 100 * (index + 1)))
            self.addCleanup(self.clients[index].execute, "DROP RESOURCE GROUP IF EXISTS " + name)
        for index, (tenant, cursor) in enumerate(zip(self.tenants, self.clients)):
            want = [100 * (index + 1)] if index in owned else []
            cursor.execute("SELECT ru_per_sec FROM information_schema.resource_groups WHERE name = %s", (name,))
            self.assertEqual([int(row[0]) for row in cursor.fetchall()], want, tenant)
            groups = self.pd_get("/resource-manager/api/v1/config/groups?keyspace_name="
                                 + urllib.parse.quote(tenant))
            pd_ru = [group["r_u_settings"]["r_u"]["settings"]["fill_rate"]
                     for group in groups if group["name"] == name]
            self.assertEqual(pd_ru, want, tenant)

    def test_attributes_are_tenant_local(self):
        """Phase 3b: the same table gets a label rule in PD per tenant, named
        and ranged by its keyspace, and each tenant's ATTRIBUTES lists only
        its own."""
        owned = self.ddl_owned_here()
        options = ("deny", "allow")
        for index in owned:
            cursor = self.clients[index]
            self.execute_through_split(
                cursor, "ALTER TABLE t ATTRIBUTES = 'merge_option=%s'" % options[index],
                lambda: self.scalar(cursor, "SELECT COUNT(*) FROM information_schema.attributes WHERE id LIKE %s",
                                    ("%" + self.schema + "%",)))
        rule_ids = ["keyspace/%d/schema/%s/t" % (self.keyspace_id(tenant), self.schema) for tenant in self.tenants]
        try:
            for index, cursor in enumerate(self.clients):
                cursor.execute("SELECT id, attributes FROM information_schema.attributes WHERE id LIKE %s",
                               ("%" + self.schema + "%",))
                want = (("schema/%s/t" % self.schema, '"merge_option=%s"' % options[index]),) if index in owned else ()
                self.assertEqual(cursor.fetchall(), want, self.tenants[index])
            rules = {rule["id"]: rule for rule in self.pd_get("/pd/api/v1/config/region-label/rules")
                     if self.schema in rule["id"]}
            self.assertEqual(sorted(rules), sorted(rule_ids[index] for index in owned))
            for index in owned:
                labels = rules[rule_ids[index]]["labels"]
                self.assertIn({"key": "merge_option", "value": options[index]}, labels)
        finally:
            for index in owned:
                self.clients[index].execute("ALTER TABLE t ATTRIBUTES = DEFAULT")
        remaining = [rule["id"] for rule in self.pd_get("/pd/api/v1/config/region-label/rules")
                     if self.schema in rule["id"]]
        self.assertEqual(remaining, [])

    def test_placement_policies_are_tenant_local(self):
        """Phase 3b: both tenants create a policy of the same name and read
        only their own; ALTER RANGE runs on this node and puts each tenant's
        range bundle under its keyspace, keyspace-<id>-TiDB_GLOBAL. The
        tenants whose DDL this node owns attach the policy to a table, whose
        bundle PD keeps as keyspace-<id>-TiDB_DDL_<table id>."""
        name = "pp_" + uuid.uuid4().hex[:12]
        owned = self.ddl_owned_here()
        bundle = lambda group: self.pd_get("/pd/api/v1/config/placement-rule/" + group)
        ids = [self.keyspace_id(tenant) for tenant in self.tenants]
        for index, cursor in enumerate(self.clients):
            cursor.execute("CREATE PLACEMENT POLICY %s FOLLOWERS = %d" % (name, index + 1))
            self.addCleanup(cursor.execute, "DROP PLACEMENT POLICY IF EXISTS " + name)
        for index in owned:
            cursor = self.clients[index]
            self.addCleanup(cursor.execute, "DROP TABLE IF EXISTS tp")
            table_id = lambda: self.scalar(cursor, "SELECT tidb_table_id FROM information_schema.tables "
                                                   "WHERE table_schema = %s AND table_name = 'tp'", (self.schema,))
            self.execute_through_split(cursor, "CREATE TABLE tp (id INT) PLACEMENT POLICY = " + name, table_id)
            table_id = table_id()
            group = "keyspace-%d-TiDB_DDL_%d" % (ids[index], table_id)
            rules = bundle(group)["rules"]
            self.assertEqual({rule["group_id"] for rule in rules}, {group})
            self.assertEqual(sum(rule["count"] for rule in rules), index + 2)
        for index, cursor in enumerate(self.clients):
            self.assertEqual(self.scalar(cursor, "SELECT followers FROM information_schema.placement_policies "
                                                 "WHERE policy_name = %s", (name,)), index + 1)
            cursor.execute("ALTER RANGE global PLACEMENT POLICY " + name)
        try:
            for index, cursor in enumerate(self.clients):
                rules = bundle("keyspace-%d-TiDB_GLOBAL" % ids[index])["rules"]
                self.assertEqual(sum(rule["count"] for rule in rules), index + 2)
                cursor.execute("SHOW PLACEMENT")
                self.assertIn(("RANGE TiDB_GLOBAL", "FOLLOWERS=%d" % (index + 1)),
                              [row[:2] for row in cursor.fetchall()])
        finally:
            for cursor in self.clients:
                cursor.execute("ALTER RANGE global PLACEMENT POLICY default")
        for keyspace_id in ids:
            self.assertFalse(bundle("keyspace-%d-TiDB_GLOBAL" % keyspace_id).get("rules"))

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


if __name__ == "__main__":
    unittest.main(verbosity=2)
