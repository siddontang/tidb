#!/usr/bin/env python3
"""Live mixed-fleet check of server-ID reservation and remote KILL.

Design: rust/docs/design/2026-11-24-multi-keyspace-server.md, section 6.6.
One multi-keyspace node (M) and one single-keyspace nextgen node (S) serve
the same tenant over an existing TiDB X cluster (PD, TiKV). Both run a
`failpoints` + `nextgen` tidb-server with GO_FAILPOINTS forcing the same
first server-ID candidate. The script checks:

1. S opens the tenant first: M's open of the tenant fails with the
   collision error (1045 to the client), and M never publishes the ID there.
2. M opens it first: S moves off the ID (its collision retry), and M keeps it.
3. With distinct IDs: KILL and KILL QUERY from M into S work; from S into M
   they are refused (design 6.8, until peer requests name their keyspace);
   a KILL from another tenant of M does not reach S.
4. Unless --skip-lease: every lease is renewed by its keeper's refresh;
   S's lease expiry drains S and S reserves an ID again; M's tenant-lease
   loss drains only that tenant, which then reserves the same ID again.

Environment:
  TIDB_SERVER   the failpoints + nextgen tidb-server binary (required)
  TIDB_MULTI_KEYSPACE_DEV_ROOT_PASSWORD  the `<keyspace>.root` password
                (required; never printed)
  PD_ADDR       PD client address (default 127.0.0.1:2379)
  MF_PRIMARY / MF_TENANT  M's configured keyspace and the shared tenant
                (default keyspace1 / keyspace2; both must exist in PD)
  MF_MULTI_PORT / MF_MULTI_STATUS / MF_SINGLE_PORT / MF_SINGLE_STATUS
                (default 4002 / 10082 / 4003 / 10083)
  MF_WORKDIR    logs and configs (default: a new temporary directory)

The nodes join the cluster as ordinary members of their keyspaces; they
are stopped with SIGTERM (which releases their leases) at the end.
"""

import argparse
import base64
import json
import os
import random
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request

import pymysql

FAILPOINT = "github.com/pingcap/tidb/pkg/domain/forceServerIDCandidate"
PD = os.getenv("PD_ADDR", "127.0.0.1:2379")
PRIMARY = os.getenv("MF_PRIMARY", "keyspace1")
TENANT = os.getenv("MF_TENANT", "keyspace2")
MULTI_PORT = int(os.getenv("MF_MULTI_PORT", "4002"))
MULTI_STATUS = int(os.getenv("MF_MULTI_STATUS", "10082"))
SINGLE_PORT = int(os.getenv("MF_SINGLE_PORT", "4003"))
SINGLE_STATUS = int(os.getenv("MF_SINGLE_STATUS", "10083"))
LEASE_TTL = 12 * 3600
REFRESH = 300

RESULTS = []


def check(name, ok, detail=""):
    RESULTS.append((name, bool(ok), detail))
    print("%s %s%s" % ("PASS" if ok else "FAIL", name, (": " + detail) if detail else ""), flush=True)
    return ok


# --- PD and etcd ---------------------------------------------------------

def http_json(url, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.load(resp)


def etcd(path, body):
    return http_json("http://%s/v3/%s" % (PD, path), body)


def b64(text):
    return base64.b64encode(text.encode()).decode()


def etcd_range(prefix):
    end = bytearray(prefix.encode())
    end[-1] += 1
    resp = etcd("kv/range", {"key": b64(prefix), "range_end": base64.b64encode(bytes(end)).decode()})
    return [(base64.b64decode(kv["key"]).decode(), base64.b64decode(kv.get("value", "")),
             int(kv.get("lease", "0"))) for kv in resp.get("kvs", [])]


def keyspace_id(name):
    return int(http_json("http://%s/pd/api/v2/keyspaces/%s" % (PD, name))["id"])


def id_key(ks, server_id):
    return "/keyspaces/tidb/%d/tidb/server_id/%d" % (ks, server_id)


def id_lease(ks, server_id):
    found = etcd_range(id_key(ks, server_id))
    return found[0][2] if found else None


def server_infos(ks):
    infos = []
    for _, value, _ in etcd_range("/keyspaces/tidb/%d/tidb/server/info/" % ks):
        try:
            info = json.loads(value)
        except ValueError:
            continue
        infos.append((info.get("server_id"), info.get("listening_port") or info.get("port")))
    return infos


def lease_ttl(lease):
    return int(etcd("lease/timetolive", {"ID": str(lease)}).get("TTL", "-1"))


def revoke(lease):
    etcd("lease/revoke", {"ID": str(lease)})


# --- nodes ---------------------------------------------------------------

class Node:
    def __init__(self, name, port, status, keyspace, multi, workdir):
        self.name, self.port, self.status, self.keyspace, self.multi = name, port, status, keyspace, multi
        self.dir = os.path.join(workdir, name)
        os.makedirs(os.path.join(self.dir, "tmp"), exist_ok=True)
        self.log = os.path.join(self.dir, "tidb.log")
        self.proc = None

    def config(self):
        lines = [
            'disaggregated-tiflash = true', 'enable-safe-point-v2 = true',
            'keyspace-name = "%s"' % self.keyspace, 'split-table = false', 'use-autoscaler = false',
            'temp-dir = "%s"' % os.path.join(self.dir, "tmp"),
        ]
        if self.multi:
            lines.append('enable-32bits-connection-id = false')
        lines += ['', '[log]', 'slow-query-file = "%s"' % os.path.join(self.dir, "tidb-slow.log"),
                  '', '[performance]', 'server-memory-quota = 2147483648']
        path = os.path.join(self.dir, "tidb.toml")
        with open(path, "w") as out:
            out.write("\n".join(lines) + "\n")
        return path

    def start(self, candidate):
        env = dict(os.environ)
        env["GO_FAILPOINTS"] = "%s=return(%d)" % (FAILPOINT, candidate)
        env.pop("TIDB_MULTI_KEYSPACE_DEV", None)
        if self.multi:
            env["TIDB_MULTI_KEYSPACE_DEV"] = TENANT
        if os.path.exists(self.log):
            os.rename(self.log, self.log + ".%d" % int(time.time()))
        self.proc = subprocess.Popen(
            [os.environ["TIDB_SERVER"], "-P", str(self.port), "--store=tikv", "--host=127.0.0.1",
             "--status=%d" % self.status, "--path=%s" % PD, "--log-file=%s" % self.log,
             "--config=%s" % self.config()],
            env=env, stdout=open(os.path.join(self.dir, "stdout.log"), "ab"), stderr=subprocess.STDOUT)
        deadline = time.time() + 400
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError("%s exited with %s; see %s" % (self.name, self.proc.returncode, self.dir))
            try:
                connect(self, self.keyspace, timeout=5).close()
                return
            except Exception:
                time.sleep(2)
        raise RuntimeError("%s not ready; see %s" % (self.name, self.log))

    def stop(self):
        if self.proc is None or self.proc.poll() is not None:
            return
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(90)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()

    def log_text(self):
        try:
            with open(self.log, errors="replace") as source:
                return source.read()
        except OSError:
            return ""


def connect(node, tenant, timeout=20):
    return pymysql.connect(host="127.0.0.1", port=node.port, user=tenant + ".root",
                           password=os.environ["TIDB_MULTI_KEYSPACE_DEV_ROOT_PASSWORD"], autocommit=True,
                           connect_timeout=timeout, read_timeout=60, write_timeout=15)


def login_error(node, tenant):
    try:
        connect(node, tenant).close()
        return None
    except pymysql.err.OperationalError as error:
        return error.args[0]


def conn_id(db):
    with db.cursor() as cursor:
        cursor.execute("SELECT CONNECTION_ID()")
        return cursor.fetchone()[0]


def server_id_of(cid):
    """globalconn: a 64-bit ID (bit 0 set) carries 22 bits of server ID at
    bit 41; a 32-bit ID carries 11 bits at bit 21."""
    if cid & 1:
        return (cid >> 41) & ((1 << 22) - 1)
    return (cid >> 21) & ((1 << 11) - 1)


def alive(db):
    try:
        with db.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchall()
        return True
    except Exception:
        return False


def run(db, sql):
    try:
        with db.cursor() as cursor:
            cursor.execute(sql)
            cursor.fetchall()
        return "ok"
    except pymysql.err.MySQLError as error:
        return "error %s" % (error.args[0],)


def run_warned(db, sql):
    """`run`, with the statement's warnings: a remote KILL that fails
    answers OK with a warning, as Go's does."""
    result = run(db, sql)
    if result != "ok":
        return result
    with db.cursor() as cursor:
        cursor.execute("SHOW WARNINGS")
        warnings = ["%s %s" % (row[1], row[2]) for row in cursor.fetchall()]
    return "ok" + ("; warnings: " + " | ".join(warnings) if warnings else "")


def kill_query_takes(source, target, statement):
    """Runs SELECT SLEEP(8) on `target` and `statement` (with the target's ID)
    from `source` after a second; returns (statement result, sleep seconds)."""
    out = {}

    def sleeper():
        start = time.time()
        out["sleep"] = run(target, "SELECT SLEEP(8)")
        out["elapsed"] = time.time() - start

    thread = threading.Thread(target=sleeper)
    thread.start()
    time.sleep(1.5)
    out["kill"] = run_warned(source, statement % conn_id_cache[id(target)])
    thread.join()
    return out["kill"], out["elapsed"]


conn_id_cache = {}


def tracked(db):
    conn_id_cache[id(db)] = conn_id(db)
    return db


def dead_within(db, seconds):
    deadline = time.time() + seconds
    while time.time() < deadline:
        if not alive(db):
            return True
        time.sleep(0.5)
    return False


def free_candidate(namespaces):
    taken = set()
    for ks in namespaces:
        for key, _, _ in etcd_range("/keyspaces/tidb/%d/tidb/server_id/" % ks):
            taken.add(int(key.rsplit("/", 1)[1]))
        taken.update(server_id for server_id, _ in server_infos(ks) if server_id)
    # At most 2047, so a 32-bit single-keyspace node can hold it too.
    return random.choice([candidate for candidate in range(1024, 2048) if candidate not in taken])


# --- phases --------------------------------------------------------------

def single_first(multi, single, ids):
    candidate = free_candidate(ids.values())
    print("== S opens %s first; both forced onto server ID %d" % (TENANT, candidate), flush=True)
    single.start(candidate)
    s = tracked(connect(single, TENANT))
    check("S holds the forced ID", server_id_of(conn_id_cache[id(s)]) == candidate)
    s_lease = id_lease(ids["tenant"], candidate)
    multi.start(candidate)
    m1 = tracked(connect(multi, PRIMARY))
    check("M greets with the forced ID", server_id_of(conn_id_cache[id(m1)]) == candidate)
    check("M reserved it in SYSTEM and %s" % PRIMARY,
          id_lease(ids["system"], candidate) is not None and id_lease(ids["primary"], candidate) is not None)
    codes = [login_error(multi, TENANT) for _ in range(2)]
    check("M refuses %s logins with 1045" % TENANT, codes == [1045, 1045], str(codes))
    text = multi.log_text()
    collision = "server ID %d is already leased in this keyspace by" % candidate
    check("M logs the collision naming S", collision in text and (":%d" % SINGLE_PORT) in text.split(collision, 1)[-1][:300])
    check("M publishes no server info in %s" % TENANT,
          all(port != MULTI_PORT for _, port in server_infos(ids["tenant"])), str(server_infos(ids["tenant"])))
    check("S's claim is untouched", id_lease(ids["tenant"], candidate) == s_lease)
    check("S still serves", alive(s))
    check("M still serves %s" % PRIMARY, alive(m1))
    for db in (s, m1):
        db.close()
    multi.stop()
    single.stop()


def multi_first(multi, single, ids, skip_lease):
    candidate = free_candidate(ids.values())
    print("== M opens %s first; both forced onto server ID %d" % (TENANT, candidate), flush=True)
    started = time.time()
    multi.start(candidate)
    m2 = tracked(connect(multi, TENANT))
    check("M serves %s with the forced ID" % TENANT, server_id_of(conn_id_cache[id(m2)]) == candidate)
    m_lease = id_lease(ids["tenant"], candidate)
    check("M publishes the ID in %s" % TENANT, (candidate, MULTI_PORT) in server_infos(ids["tenant"]))
    single.start(candidate)
    s = tracked(connect(single, TENANT))
    s_id = server_id_of(conn_id_cache[id(s)])
    check("S moved off the forced ID", s_id != candidate, "S holds %d" % s_id)
    check("S publishes its own ID", (s_id, SINGLE_PORT) in server_infos(ids["tenant"]))
    check("M's claim is untouched", id_lease(ids["tenant"], candidate) == m_lease)

    print("== remote KILL between M (%d) and S (%d)" % (candidate, s_id), flush=True)
    m2b = tracked(connect(multi, TENANT))
    m1 = tracked(connect(multi, PRIMARY))
    result, elapsed = kill_query_takes(m2b, s, "KILL QUERY %d")
    check("KILL QUERY from M into S stops the query", result == "ok" and elapsed < 6,
          "%s, the query ran %.1fs" % (result, elapsed))
    check("S's connection survives KILL QUERY", alive(s))
    result = run_warned(m1, "KILL %d" % conn_id_cache[id(s)])
    check("KILL from M's other tenant does not reach S", alive(s) and not dead_within(s, 3), result)
    result = run_warned(m2b, "KILL %d" % conn_id_cache[id(s)])
    check("KILL from M into S closes the connection", result == "ok" and dead_within(s, 10), result)
    s = tracked(connect(single, TENANT))
    result, elapsed = kill_query_takes(s, m2, "KILL QUERY %d")
    check("KILL QUERY from S into M is refused (design 6.8, until 3f)", elapsed > 7,
          "%s, the query ran %.1fs" % (result, elapsed))
    result = run_warned(s, "KILL %d" % conn_id_cache[id(m2)])
    check("KILL from S into M is refused (design 6.8, until 3f)", not dead_within(m2, 5), result)
    for db in (m2b,):
        db.close()
    if skip_lease:
        return

    leases = {"S %s" % TENANT: id_lease(ids["tenant"], s_id), "M %s" % TENANT: id_lease(ids["tenant"], candidate),
              "M %s" % PRIMARY: id_lease(ids["primary"], candidate), "M SYSTEM": id_lease(ids["system"], candidate)}
    wait = started + REFRESH + 40 - time.time()
    print("== lease renewal: waiting %ds for the keepers' refresh" % max(0, wait), flush=True)
    time.sleep(max(0, wait))
    for name, lease in leases.items():
        ttl = lease_ttl(lease)
        check("%s lease renewed" % name, ttl > LEASE_TTL - 120, "TTL %d" % ttl)

    print("== lease expiry: revoking S's and M's %s leases" % TENANT, flush=True)
    s, m1, m2 = tracked(connect(single, TENANT)), tracked(connect(multi, PRIMARY)), tracked(connect(multi, TENANT))
    revoke(leases["S %s" % TENANT])
    revoke(leases["M %s" % TENANT])
    revoked = time.time()
    seen = {"s_dead": None, "m2_dead": None, "m1_dead": None, "m2_refused": None, "m2_back": None, "s_back": None}
    while time.time() - revoked < REFRESH + 90:
        now = time.time() - revoked
        if seen["s_dead"] is None and not alive(s):
            seen["s_dead"] = now
        if seen["m2_dead"] is None and not alive(m2):
            seen["m2_dead"] = now
        if seen["m1_dead"] is None and not alive(m1):
            seen["m1_dead"] = now
        if seen["m2_dead"] is not None and seen["m2_back"] is None:
            code = login_error(multi, TENANT)
            if code is not None and seen["m2_refused"] is None:
                seen["m2_refused"] = (now, code)
            if code is None:
                seen["m2_back"] = now
        if seen["s_dead"] is not None and seen["s_back"] is None and login_error(single, TENANT) is None:
            seen["s_back"] = now
        if seen["s_back"] is not None and seen["m2_back"] is not None:
            break
        time.sleep(1)
    print("   observed (seconds after the revoke): %s" % seen, flush=True)
    check("S drains on its lease expiry", seen["s_dead"] is not None)
    check("S reserves an ID again", seen["s_back"] is not None)
    check("M drains %s on its tenant-lease loss" % TENANT, seen["m2_dead"] is not None)
    check("M keeps serving %s" % PRIMARY, seen["m1_dead"] is None and alive(m1))
    check("M serves %s again" % TENANT, seen["m2_back"] is not None)
    if seen["m2_back"] is not None:
        m2 = tracked(connect(multi, TENANT))
        check("M re-reserved the same ID in %s" % TENANT, server_id_of(conn_id_cache[id(m2)]) == candidate
              and id_lease(ids["tenant"], candidate) not in (None, leases["M %s" % TENANT]))
    if seen["s_back"] is not None:
        s = tracked(connect(single, TENANT))
        new_id = server_id_of(conn_id_cache[id(s)])
        check("S publishes its new ID", (new_id, SINGLE_PORT) in server_infos(ids["tenant"]), "S holds %d" % new_id)
    check("M's ID is not S's after S's reacquisition", server_id_of(conn_id_cache[id(s)]) != candidate)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--skip-lease", action="store_true", help="skip the renewal and expiry checks (about 11 minutes)")
    args = parser.parse_args()
    for name in ("TIDB_SERVER", "TIDB_MULTI_KEYSPACE_DEV_ROOT_PASSWORD"):
        if not os.getenv(name):
            sys.exit("%s is required" % name)
    workdir = os.getenv("MF_WORKDIR") or tempfile.mkdtemp(prefix="mixed-fleet-")
    print("work directory: %s" % workdir, flush=True)
    ids = {"system": keyspace_id("SYSTEM"), "primary": keyspace_id(PRIMARY), "tenant": keyspace_id(TENANT)}
    multi = Node("multi", MULTI_PORT, MULTI_STATUS, PRIMARY, True, workdir)
    single = Node("single", SINGLE_PORT, SINGLE_STATUS, TENANT, False, workdir)
    try:
        single_first(multi, single, ids)
        multi_first(multi, single, ids, args.skip_lease)
    finally:
        multi.stop()
        single.stop()
    failed = [name for name, ok, _ in RESULTS if not ok]
    print("%d checks, %d failed%s" % (len(RESULTS), len(failed), (": " + "; ".join(failed)) if failed else ""))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
