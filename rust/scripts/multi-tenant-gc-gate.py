#!/usr/bin/env python3
"""Live GC gate for two tenants of one multi-keyspace node (design §9, §6.5).

A read transaction held open in tenant A must hold back A's GC safe point
and only A's:

1. A and B each get a table with original rows. A's reader begins a
   transaction and reads them; its start TS is recorded.
2. Both tenants then overwrite and delete those rows (old versions for GC
   to remove) and run a DDL, so no other node's report is held back by
   a schema older than the reader.
3. The script waits until GC has run in both keyspaces at least
   `tidb_gc_life_time` after the reader began (keyspace-level GC; the
   minimum life time and run interval are 10m, so this takes about 25
   minutes), and checks:
   - B's GC safe point passed the reader's start TS;
   - A's GC ran when its life time alone would have passed the reader,
     and A's safe point did not pass the reader's start TS;
   - the reader still reads the original rows.

Run with PyMySQL against a multi-keyspace node serving both tenants.
Environment: TIDB_MULTI_KEYSPACE_DEV_ROOT_PASSWORD (required, never
printed); TIDB_TEST_HOST / TIDB_TEST_PORT (default 127.0.0.1:4002);
TIDB_TEST_TENANT_A / TIDB_TEST_TENANT_B (default keyspace1 / keyspace2);
GC_GATE_TIMEOUT seconds (default 2400). Each run uses and drops its own
databases.
"""

import datetime
import os
import sys
import time
import uuid

import pymysql

HOST = os.getenv("TIDB_TEST_HOST", "127.0.0.1")
PORT = int(os.getenv("TIDB_TEST_PORT", "4002"))
TENANT_A = os.getenv("TIDB_TEST_TENANT_A", "keyspace1")
TENANT_B = os.getenv("TIDB_TEST_TENANT_B", "keyspace2")
TIMEOUT = int(os.getenv("GC_GATE_TIMEOUT", "2400"))


def connect(tenant):
    return pymysql.connect(host=HOST, port=PORT, user=tenant + ".root",
                           password=os.environ["TIDB_MULTI_KEYSPACE_DEV_ROOT_PASSWORD"],
                           autocommit=True, connect_timeout=30, read_timeout=120)


def one(cursor, sql):
    cursor.execute(sql)
    row = cursor.fetchone()
    return row[0] if row else None


def physical_ms(ts):
    return ts >> 18


def gc_time_ms(value):
    """mysql.tidb's GC times, Go's `20060102-15:04:05.000 -0700`."""
    if not value:
        return None
    return int(datetime.datetime.strptime(value, "%Y%m%d-%H:%M:%S.%f %z").timestamp() * 1000)


def gc_state(cursor):
    cursor.execute("SELECT variable_name, variable_value FROM mysql.tidb WHERE variable_name IN "
                   "('tikv_gc_safe_point', 'tikv_gc_last_run_time', 'tikv_gc_life_time', 'tikv_gc_enable')")
    return dict(cursor.fetchall())


def stamp(ms):
    return datetime.datetime.fromtimestamp(ms / 1000).strftime("%H:%M:%S.%f")[:-3] if ms else "-"


def main():
    if not os.getenv("TIDB_MULTI_KEYSPACE_DEV_ROOT_PASSWORD"):
        sys.exit("TIDB_MULTI_KEYSPACE_DEV_ROOT_PASSWORD is required")
    schema = "gc_gate_" + uuid.uuid4().hex[:12]
    admin = {tenant: connect(tenant).cursor() for tenant in (TENANT_A, TENANT_B)}
    for tenant, cursor in admin.items():
        state = gc_state(cursor)
        print("%s: GC enable %s, life time %s" % (tenant, state.get("tikv_gc_enable"), state.get("tikv_gc_life_time")))
        cursor.execute("CREATE DATABASE " + schema)
        cursor.execute("CREATE TABLE %s.t (id INT PRIMARY KEY, v VARCHAR(16))" % schema)
        cursor.execute("INSERT INTO %s.t VALUES (1, 'original'), (2, 'original')" % schema)
    reader = connect(TENANT_A).cursor()
    reader.execute("BEGIN")
    reader.execute("SELECT id, v FROM %s.t ORDER BY id" % schema)
    original = reader.fetchall()
    start_ts = int(one(reader, "SELECT @@tidb_current_ts"))
    start_ms = physical_ms(start_ts)
    print("A's reader start ts %d (%s), reads %s" % (start_ts, stamp(start_ms), original))
    for tenant, cursor in admin.items():
        cursor.execute("UPDATE %s.t SET v = 'overwritten' WHERE id = 1" % schema)
        cursor.execute("DELETE FROM %s.t WHERE id = 2" % schema)
        cursor.execute("CREATE TABLE %s.after_reader (a INT)" % schema)
    failures = []
    deadline = time.time() + TIMEOUT
    last = None
    try:
        while time.time() < deadline:
            states = {tenant: gc_state(cursor) for tenant, cursor in admin.items()}
            view = tuple((states[t].get("tikv_gc_safe_point"), states[t].get("tikv_gc_last_run_time")) for t in admin)
            if view != last:
                for tenant in admin:
                    print("  %s: safe point %s, last run %s" % (tenant, states[tenant].get("tikv_gc_safe_point"),
                                                              states[tenant].get("tikv_gc_last_run_time")), flush=True)
                last = view
            b_safe = gc_time_ms(states[TENANT_B].get("tikv_gc_safe_point"))
            a_run = gc_time_ms(states[TENANT_A].get("tikv_gc_last_run_time"))
            # A's GC ran when a ten-minute life time alone would have put
            # its safe point past the reader.
            a_ran_past = a_run is not None and a_run > start_ms + 10 * 60 * 1000 + 30_000
            if b_safe is not None and b_safe > start_ms and a_ran_past:
                break
            # The reader keeps its transaction alive.
            reader.execute("SELECT 1")
            reader.fetchall()
            time.sleep(30)
        else:
            failures.append("GC did not run past the reader in both keyspaces within %ds" % TIMEOUT)
        states = {tenant: gc_state(cursor) for tenant, cursor in admin.items()}
        a_safe = gc_time_ms(states[TENANT_A].get("tikv_gc_safe_point"))
        b_safe = gc_time_ms(states[TENANT_B].get("tikv_gc_safe_point"))
        print("reader start %s; A's safe point %s; B's safe point %s" % (stamp(start_ms), stamp(a_safe), stamp(b_safe)))
        if not (b_safe and b_safe > start_ms):
            failures.append("B's safe point did not pass A's reader")
        if not (a_safe and a_safe <= start_ms):
            failures.append("A's safe point passed its open reader")
        reader.execute("SELECT id, v FROM %s.t ORDER BY id" % schema)
        still = reader.fetchall()
        print("A's reader reads %s after both GCs" % (still,))
        if still != original:
            failures.append("A's reader lost its rows: %s" % (still,))
    finally:
        try:
            reader.execute("ROLLBACK")
        except pymysql.err.MySQLError:
            pass
        for cursor in admin.values():
            cursor.execute("DROP DATABASE IF EXISTS " + schema)
    for failure in failures:
        print("FAIL " + failure)
    print("PASS" if not failures else "FAILED")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
