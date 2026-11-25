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
   - B's PD safe point exceeds both its own PD point at the reader's start
     and the reader's TSO (a B job completed past the reader);
   - A's PD safe point equals the reader's TSO exactly: a completed A job
     stopped at the reader and nowhere else (a point merely below it could
     come from a job that picked its target earlier).
   Then the reader must still read its original rows.
4. The reader rolls back; A's PD safe point must pass the same TSO within
   `tidb_gc_run_interval` plus a margin.

Each wait fails the gate with the step that timed out. `Judge` holds these
assertions; `--replay <log>` feeds a saved run's output through it, and
`--self-test` replays the recorded passing run and a trace that must fail. Run with PyMySQL
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


class Judge:
    """The gate's assertions over the PD safe points it observes, shared by
    a live run and a replay."""

    def __init__(self, start, initial_a, initial_b):
        self.start, self.initial_b = start, initial_b
        self.a_prev = initial_a
        self.b_done = self.a_done = None

    def hold_poll(self, a, b, seen):
        """One poll while the reader is open; True once both hold."""
        if a > self.start:
            raise GateFailed("A's PD safe point %d passed its open reader %d" % (a, self.start))
        if self.b_done is None and b > self.start and b > self.initial_b:
            self.b_done = (b, seen)
            print("  B's PD safe point %s passed the reader and its own start %d; observed at %s"
                  % (value(b), self.initial_b, seen), flush=True)
        if a != self.a_prev:
            print("  A's PD safe point moved %d -> %s; observed at %s" % (self.a_prev, value(a), seen), flush=True)
        if self.a_done is None and a == self.start:
            self.a_done = (a, seen)
        self.a_prev = a
        return bool(self.b_done and self.a_done)

    def hold_missing(self):
        return [what for what, done in (("B's GC completing past the reader and its own start", self.b_done),
                                        ("A's GC stopping exactly at the reader", self.a_done)) if not done]

    def released(self, a):
        """One poll after the reader rolled back; True once A passed it."""
        return a > self.start


def replay(path):
    """Feeds a saved run's output through `Judge`: the reader's TSO, the
    PD safe points at its start, every move the run printed (each a poll),
    and A's point after the rollback. Raises GateFailed as a live run would."""
    with open(path) as source:
        text = source.read()
    start = int(re.search(r"reader start TSO (\d+)", text).group(1))
    initial = re.search(r"PD safe points at the reader's start: A (\d+)[^,]*, B (\d+)", text)
    judge = Judge(start, int(initial.group(1)), int(initial.group(2)))
    a, b = judge.a_prev, judge.initial_b
    done = False
    for line in text.splitlines():
        moved = re.search(r"A's PD safe point moved \d+ -> (\d+)", line)
        passed = re.search(r"^  B's PD safe point (\d+)", line)
        if moved:
            a = int(moved.group(1))
        elif passed:
            b = int(passed.group(1))
        else:
            continue
        done = judge.hold_poll(a, b, "replayed")
        if done:
            break
    if not done:
        raise GateFailed("the trace ended before " + " and ".join(judge.hold_missing()))
    after = re.search(r"after the reader rolled back, A's PD safe point (\d+)", text)
    if not (after and judge.released(int(after.group(1)))):
        raise GateFailed("the trace shows no A safe point past the released reader")
    return judge


RECORDED_RUN = """A's reader start TSO 470704065376157703 (15:58:31.109), reads ((1, 'original'), (2, 'original'))
PD safe points at the reader's start: A 470703262919557125, B 470703263470059528
  A's PD safe point moved 470703262919557125 -> 470703937528791040 (15:50:23.410); life time passed the reader before this poll: False
  B's PD safe point 470704097214857216 (16:00:32.564) passed the reader
  A's PD safe point moved 470703937528791040 -> 470704065376157703 (15:58:31.109); life time passed the reader before this poll: True
PASS after the reader rolled back, A's PD safe point 470704252523642880 (16:10:25.020) > reader 470704065376157703
"""


def self_test():
    """The recorded live run (2026-11-25, the design's §6.5 record) passes;
    a review's trace in which B started above the reader and never moved,
    and A rose only to 20s below the reader, fails."""
    import tempfile
    reader = 470704065376157703
    scale = 1 << 18
    false_pass = "\n".join([
        "A's reader start TSO %d" % reader,
        "PD safe points at the reader's start: A %d, B %d" % (reader - 90000 * scale, reader + 10 * scale),
        "  B's PD safe point %d passed the reader" % (reader + 10 * scale),
        "  A's PD safe point moved %d -> %d" % (reader - 90000 * scale, reader - 20000 * scale),
        "PASS after the reader rolled back, A's PD safe point %d > reader %d" % (reader + 10000 * scale, reader),
    ]) + "\n"
    with tempfile.TemporaryDirectory() as directory:
        for name, text, passes in (("recorded", RECORDED_RUN, True), ("false-pass", false_pass, False)):
            path = os.path.join(directory, name)
            with open(path, "w") as out:
                out.write(text)
            try:
                replay(path)
                outcome = True
            except GateFailed as failure:
                outcome = False
                print("  %s trace fails: %s" % (name, failure))
            assert outcome == passes, "%s trace: expected %s" % (name, "PASS" if passes else "FAIL")
    print("self-test passed")


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
        judge = Judge(self.start, points[TENANT_A], points[TENANT_B])
        while time.time() < deadline:
            time.sleep(POLL)
            self.reader.execute("SELECT 1")
            self.reader.fetchall()
            self.ddl_tick()
            points = self.safe_points()
            if judge.hold_poll(points[TENANT_A], points[TENANT_B], observed()):
                return judge
        raise GateFailed("timed out waiting for " + " and ".join(judge.hold_missing()))

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
            if Judge(self.start, a, a).released(a):
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
    if sys.argv[1:2] == ["--self-test"]:
        self_test()
        return
    if sys.argv[1:2] == ["--replay"] and len(sys.argv) == 3:
        try:
            replay(sys.argv[2])
        except GateFailed as failure:
            print("FAIL " + str(failure))
            sys.exit(1)
        print("PASS")
        return
    if not os.getenv("TIDB_MULTI_KEYSPACE_DEV_ROOT_PASSWORD"):
        sys.exit("TIDB_MULTI_KEYSPACE_DEV_ROOT_PASSWORD is required")
    gate = Gate()
    print("A=%s (keyspace %d), B=%s (keyspace %d); A's GC life time %dms, run interval %dms"
          % (TENANT_A, gate.ids[TENANT_A], TENANT_B, gate.ids[TENANT_B], gate.life_ms, gate.interval_ms), flush=True)
    try:
        gate.setup()
        print("A's reader start TSO %s, reads %s; observed at %s" % (value(gate.start), gate.original, observed()),
              flush=True)
        judge = gate.hold()
        (b, b_seen), (a, a_seen) = judge.b_done, judge.a_done
        print("PASS B's PD safe point %s > reader %d and > its own start %d; observed at %s"
              % (value(b), gate.start, judge.initial_b, b_seen))
        print("PASS A's PD safe point %s == reader %d while the reader was open; observed at %s"
              % (value(a), gate.start, a_seen))
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
