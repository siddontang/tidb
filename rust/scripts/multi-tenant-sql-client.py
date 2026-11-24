#!/usr/bin/env python3
"""Live regression suite for two tenant accounts on one development TiDB listener.

Run directly with PyMySQL installed. Set TIDB_MULTI_KEYSPACE_DEV_ROOT_PASSWORD
to the test password for both accounts. TIDB_TEST_HOST / TIDB_TEST_PORT select
the listener. TIDB_TEST_TENANT_A / TIDB_TEST_TENANT_B select the keyspaces.
Each case creates and removes its own databases.
"""

import os
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pymysql


class TenantSQLTests(unittest.TestCase):
    def connect(self, tenant):
        db = pymysql.connect(
            host=os.getenv("TIDB_TEST_HOST", "127.0.0.1"),
            port=int(os.getenv("TIDB_TEST_PORT", "4000")),
            user=tenant + ".root",
            password=os.environ["TIDB_MULTI_KEYSPACE_DEV_ROOT_PASSWORD"],
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


if __name__ == "__main__":
    unittest.main(verbosity=2)
