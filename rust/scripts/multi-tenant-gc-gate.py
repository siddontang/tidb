#!/usr/bin/env python3
"""Live GC gate for two tenants of one multi-keyspace node (design §9, §6.5).

A read transaction held open in tenant A must hold back A's GC safe point
and only A's. The evidence is the GC safe point PD stores for each
keyspace (keyspace-level GC: what the GC job's AdvanceGCSafePoint
publishes, `/pd/<cluster>/keyspaces/gc_safe_point/<keyspace id>` in PD's
etcd), compared with the reader's full start TSO. The `mysql.tidb` GC rows
are written before a job runs, so they prove nothing.

1. A and B each get a table with original rows. A's reader begins a
   transaction, reads them, and records its start TSO.
2. Both tenants overwrite and delete those rows (old versions for GC to
   remove). Every two minutes each tenant runs a DDL, so no node's report
   (an older node's stays at its keyspace's last DDL) holds a keyspace
   back before the reader.
3. While the reader is open, on every poll A's PD safe point must not pass
   the reader's TSO. The gate waits until:
   - B's PD safe point moved past the reader's TSO (a B job completed);
   - A's PD safe point moved in a poll that began after A's TSO minus its
     configured `tidb_gc_life_time` (plus a margin) had passed the reader:
     a job completed when the life time alone would have passed the reader,
     so only the reader held A back.
   Then the reader must still read its original rows.
4. The reader rolls back; A's PD safe point must pass the same TSO within
   `tidb_gc_run_interval` plus a margin.

Each wait fails the gate with the step that timed out. Run with PyMySQL
against a multi-keyspace node serving both tenants. Environment:
TIDB_MULTI_KEYSPACE_DEV_ROOT_PASSWORD (required, never printed);
TIDB_TEST_HOST / TIDB_TEST_PORT (default 127.0.0.1:4002);
TIDB_TEST_TENANT_A / TIDB_TEST_TENANT_B (default keyspace1 / keyspace2);
PD_ADDR (default 127.0.0.1:2379). With the minimum 10m life time and run
interval it takes about 35 minutes. Each run uses and drops its own
databases.
"""

import base64
import json
import os
import re
import sys
import time
import urllib.request
import uuid

import pymysql

HOST = os.getenv("TIDB_TEST_HOST", "127.0.0.1")
PORT = int(os.getenv("TIDB_TEST_PORT", "4002"))
TENANT_A = os.getenv("TIDB_TEST_TENANT_A", "keyspace1")
TENANT_B = os.getenv("TIDB_TEST_TENANT_B", "keyspace2")
PD = os.getenv("PD_ADDR", "127.0.0.1:2379")
POLL = 30
DDL_EVERY = 120
# A GC job computes its safe point at its start and publishes it at its
# end; a poll that began this long after the life time passed the reader
# only sees jobs that computed after it.
JOB_MARGIN_MS = 60_000


class GateFailed(Exception):
    pass


def http_json(url, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.load(resp)


def pd_safe_point(cluster_id, keyspace_id):
    """The keyspace's GC safe point as PD stores it (0 before any)."""
    key = "/pd/%d/keyspaces/gc_safe_point/%08d" % (cluster_id, keyspace_id)
    resp = http_json("http://%s/v3/kv/range" % PD, {"key": base64.b64encode(key.encode()).decode()})
    for kv in resp.get("kvs", []):
        if base64.b64decode(kv["key"]).decode() == key:
            return int(json.loads(base64.b64decode(kv["value"]))["safe_point"])
    return 0


def go_duration_ms(text):
    units = {"h": 3_600_000, "m": 60_000, "s": 1000, "ms": 1}
    parts = re.findall(r"(\d+(?:\.\d+)?)(ms|h|m|s)", text)
    if not parts:
        raise ValueError("not a Go duration: %r" % text)
    return int(sum(float(value) * units[unit] for value, unit in parts))


def connect(tenant):
    return pymysql.connect(host=HOST, port=PORT, user=tenant + ".root",
                           password=os.environ["TIDB_MULTI_KEYSPACE_DEV_ROOT_PASSWORD"],
                           autocommit=True, connect_timeout=30, read_timeout=120)


def one(cursor, sql):
    cursor.execute(sql)
    row = cursor.fetchone()
    return row[0] if row else None


def current_tso(cursor):
    cursor.execute("BEGIN")
    try:
        return int(one(cursor, "SELECT @@tidb_current_ts"))
    finally:
        cursor.execute("ROLLBACK")


def ms(ts):
    return ts >> 18


def clock(ts):
    """A TSO's physical part as a local time: a timestamp's VALUE."""
    return time.strftime("%H:%M:%S", time.localtime(ms(ts) / 1000)) + ".%03d" % (ms(ts) % 1000)


def value(ts):
    return "%d (physical time %s)" % (ts, clock(ts))


def observed():
    """The local time an event is OBSERVED at."""
    now = time.time()
    return time.strftime("%H:%M:%S", time.localtime(now)) + ".%03d" % (int(now * 1000) % 1000)


class Gate:
    def __init__(self):
        self.schema = "gc_gate_" + uuid.uuid4().hex[:12]
        self.cluster = int(http_json("http://%s/pd/api/v1/cluster" % PD)["id"])
        self.ids = {t: int(http_json("http://%s/pd/api/v2/keyspaces/%s" % (PD, t))["id"]) for t in (TENANT_A, TENANT_B)}
        self.admin = {t: connect(t).cursor() for t in (TENANT_A, TENANT_B)}
        self.life_ms = go_duration_ms(one(self.admin[TENANT_A], "SELECT @@global.tidb_gc_life_time"))
        self.interval_ms = go_duration_ms(one(self.admin[TENANT_A], "SELECT @@global.tidb_gc_run_interval"))
        self.last_ddl = 0
        self.ddl_seq = 0
        self.reader = None

    def safe_points(self):
        return {t: pd_safe_point(self.cluster, self.ids[t]) for t in (TENANT_A, TENANT_B)}

    def ddl_tick(self):
        if time.time() - self.last_ddl < DDL_EVERY:
            return
        self.last_ddl = time.time()
        self.ddl_seq += 1
        for cursor in self.admin.values():
            cursor.execute("CREATE TABLE %s.tick_%d (a INT)" % (self.schema, self.ddl_seq))
            if self.ddl_seq > 1:
                cursor.execute("DROP TABLE %s.tick_%d" % (self.schema, self.ddl_seq - 1))

    def setup(self):
        for cursor in self.admin.values():
            cursor.execute("CREATE DATABASE " + self.schema)
            cursor.execute("CREATE TABLE %s.t (id INT PRIMARY KEY, v VARCHAR(16))" % self.schema)
            cursor.execute("INSERT INTO %s.t VALUES (1, 'original'), (2, 'original')" % self.schema)
        self.reader = connect(TENANT_A).cursor()
        self.reader.execute("BEGIN")
        self.reader.execute("SELECT id, v FROM %s.t ORDER BY id" % self.schema)
        self.original = self.reader.fetchall()
        self.start = int(one(self.reader, "SELECT @@tidb_current_ts"))
        for cursor in self.admin.values():
            cursor.execute("UPDATE %s.t SET v = 'overwritten' WHERE id = 1" % self.schema)
            cursor.execute("DELETE FROM %s.t WHERE id = 2" % self.schema)
        self.ddl_tick()

    def hold(self):
        """Phase 3: the reader is open; returns the evidence for both."""
        deadline = time.time() + (self.life_ms + 2 * self.interval_ms) / 1000 + 300
        points = self.safe_points()
        print("PD safe points at the reader's start: A %s, B %s; observed at %s"
              % (value(points[TENANT_A]), value(points[TENANT_B]), observed()), flush=True)
        a_prev, b_done, a_done = points[TENANT_A], None, None
        poll_started = current_tso(self.admin[TENANT_A])
        while time.time() < deadline:
            time.sleep(POLL)
            self.reader.execute("SELECT 1")
            self.reader.fetchall()
            self.ddl_tick()
            points = self.safe_points()
            a_now = current_tso(self.admin[TENANT_A])
            a, b = points[TENANT_A], points[TENANT_B]
            if a > self.start:
                raise GateFailed("A's PD safe point %d passed its open reader %d" % (a, self.start))
            if b > self.start and b_done is None:
                b_done = (b, observed())
                print("  B's PD safe point %s passed the reader; observed at %s" % (value(b), b_done[1]), flush=True)
            # The poll began after A's TSO minus its life time passed the
            # reader by the job margin: a job seen completing now computed
            # its safe point when the life time alone would have passed
            # the reader.
            life_passed = ms(poll_started) - self.life_ms - JOB_MARGIN_MS > ms(self.start)
            if a != a_prev:
                seen = observed()
                print("  A's PD safe point moved %d -> %s; observed at %s, in a poll that began at A's TSO %s; "
                      "life time passed the reader before this poll: %s"
                      % (a_prev, value(a), seen, value(poll_started), life_passed), flush=True)
                if life_passed and a_done is None:
                    a_done = (a, poll_started, seen)
            a_prev = a
            if b_done and a_done:
                return b_done, a_done
            poll_started = a_now
        missing = [what for what, done in (("B's GC completing past the reader", b_done),
                                           ("A's GC completing after its life time passed the reader", a_done)) if not done]
        raise GateFailed("timed out waiting for " + " and ".join(missing))

    def release(self):
        """Phase 4: after the reader rolls back, A's PD safe point passes it."""
        self.reader.execute("ROLLBACK")
        self.reader = None
        print("A's reader rolled back; observed at %s" % observed(), flush=True)
        deadline = time.time() + self.interval_ms / 1000 + 300
        while time.time() < deadline:
            time.sleep(POLL)
            self.ddl_tick()
            a = self.safe_points()[TENANT_A]
            if a > self.start:
                return a, observed()
        raise GateFailed("A's PD safe point did not pass the released reader within the run interval")

    def cleanup(self):
        if self.reader is not None:
            try:
                self.reader.execute("ROLLBACK")
            except pymysql.err.MySQLError:
                pass
        for cursor in self.admin.values():
            try:
                cursor.execute("DROP DATABASE IF EXISTS " + self.schema)
            except pymysql.err.MySQLError:
                pass


def main():
    if not os.getenv("TIDB_MULTI_KEYSPACE_DEV_ROOT_PASSWORD"):
        sys.exit("TIDB_MULTI_KEYSPACE_DEV_ROOT_PASSWORD is required")
    gate = Gate()
    print("A=%s (keyspace %d), B=%s (keyspace %d); A's GC life time %dms, run interval %dms"
          % (TENANT_A, gate.ids[TENANT_A], TENANT_B, gate.ids[TENANT_B], gate.life_ms, gate.interval_ms), flush=True)
    try:
        gate.setup()
        print("A's reader start TSO %s, reads %s; observed at %s" % (value(gate.start), gate.original, observed()),
              flush=True)
        (b, b_seen), (a, a_poll, a_seen) = gate.hold()
        print("PASS B's PD safe point %s > reader %d; observed at %s" % (value(b), gate.start, b_seen))
        print("PASS A's PD safe point %s <= reader %d; observed at %s, in a poll that began at A's TSO %s, when "
              "that TSO minus A's %dms life time was already %dms past the reader"
              % (value(a), gate.start, a_seen, value(a_poll), gate.life_ms,
                 ms(a_poll) - gate.life_ms - ms(gate.start)))
        gate.reader.execute("SELECT id, v FROM %s.t ORDER BY id" % gate.schema)
        still = gate.reader.fetchall()
        if still != gate.original:
            raise GateFailed("A's reader lost its rows after both GCs: %s" % (still,))
        print("PASS A's reader still reads %s after both GCs; observed at %s" % (still, observed()))
        released, released_seen = gate.release()
        print("PASS after the reader rolled back, A's PD safe point %s > reader %d; observed at %s"
              % (value(released), gate.start, released_seen))
    except GateFailed as failure:
        print("FAIL " + str(failure))
        print("FAILED")
        sys.exit(1)
    finally:
        gate.cleanup()
    print("PASS")


if __name__ == "__main__":
    main()
