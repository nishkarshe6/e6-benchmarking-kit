#!/usr/bin/env python3
"""Run a CSV of SQL queries on e6data and write the timings to a CSV.

    pip install e6data-python-connector==3.0.0
    python run_queries.py queries.csv                              # one query at a time
    python run_queries.py queries.csv --runs 3
    python run_queries.py queries.csv --pattern qps_pattern.json   # queries arrive on a fixed load pattern
    python run_queries.py queries.csv --engine databricks          # on a Databricks SQL warehouse
    python run_queries.py queries.csv --engine both                # on e6data and Databricks together

--engine both sends every query to e6data and to a Databricks SQL warehouse on the same
schedule, starting together (pip install databricks-sql-connector). Databricks details come
from --dbr-* or DATABRICKS_HOST / _HTTP_PATH / _CATALOG / _SCHEMA; the token from
DATABRICKS_TOKEN, or it is asked for (hidden). The Databricks result cache is turned off.

Input CSV: any CSV with a column of SQL (found by content, or --sql-column) and an
optional column of query names (header containing name/id, or --name-column).
Comma, semicolon or tab separated.
Table names can be unqualified (store_sales); they resolve in CATALOG.<schema>.

Output CSV: one row per execution with client time and e6data's own server-side
timings. A per-query summary and overall throughput / latency are printed at the end.
Exit code: 0 if every execution succeeded, 1 otherwise.
"""
import argparse
import csv
import getpass
import io
import json
import logging
import math
import os
import queue
import re
import statistics
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from e6data_python_connector import Connection

# ---- e6data connection ---------------------------------------------------
HOST = "e64145f73b-ue1a.e6.run"
PORT = 443
# You log in as yourself: set E6_USER to your e6data login email and E6_TOKEN to your
# personal access token (e6pat_..., see the README Quick start).
USERNAME = os.environ.get("E6_USER", "").strip()
TOKEN = os.environ.get("E6_TOKEN", "").strip()
if not (USERNAME and TOKEN) and not {"-h", "--help"} & set(sys.argv[1:]):
    sys.exit("set E6_USER (your e6data login email) and E6_TOKEN (your e6pat_ access token); see the README Quick start")
CLUSTER = "sandbox"
CATALOG = "demo"
SCHEMA = "tpcds_sf1000_delta"
SCHEMAS = ["tpcds_sf1000_delta"]
RESUMED_LOGIN_S = 10  # a login slower than this means the cluster was paused and has just been resumed
LOGIN_TIMEOUT_S = 420  # covers resuming a paused cluster (the connector waits up to 5 min)
INTERRUPT_WAIT_S = 30
DBR_LOGIN_TIMEOUT_S = 600  # a stopped SQL warehouse can take minutes to start
# ---------------------------------------------------------------------------

FIELDS = ["name", "run", "status", "client_seconds", "rows", "e6_planning_ms", "e6_queue_ms",
          "e6_execution_ms", "e6_total_ms", "query_id", "error"]
# Column headers are matched by pattern, not exact name. The SQL column is found by its
# content (the column whose cells read as SQL); these patterns only break ties.
SQL_HINT = re.compile(r"sql|query|statement|text|engine", re.I)
NAME_HINT = re.compile(r"name|id|label|key|title", re.I)
SQL_START = re.compile(r"\(|(select|with|show|describe|desc|explain|values|set|use|insert|create|drop|alter)\b")
SUSPENDED_HINT = "the cluster is suspended and could not be resumed from here: ask the e6data team to resume it, then rerun"

try:  # never let an odd character in an error message crash the console output
    sys.stdout.reconfigure(errors="replace")
except Exception:
    pass


def now_iso():
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


# ---------------------------------------------------------------- input

def _is_comment_only(sql):
    lines = [l.strip() for l in sql.splitlines() if l.strip()]
    return all(l.startswith("--") for l in lines)


def _looks_like_sql(cell):
    """True if the cell's first non-comment line starts like a SQL statement."""
    for line in cell.strip().lstrip('"').splitlines():
        line = line.strip().lower()
        if line and not line.startswith("--"):
            return bool(SQL_START.match(line))
    return False


def _pick_column(header, wanted, flag):
    """Resolve a --sql-column / --name-column value: a header (any case) or a 1-based number."""
    low = [h.strip().lower() for h in header]
    if wanted.strip().lower() in low:
        return low.index(wanted.strip().lower())
    if wanted.isdigit() and 1 <= int(wanted) <= len(header):
        return int(wanted) - 1
    sys.exit(f"{flag} '{wanted}' is not a column of this CSV. Columns: {', '.join(header)}")


def read_queries(path, sql_column=None, name_column=None):
    try:
        raw = open(path, "rb").read()
    except OSError as exc:
        sys.exit(f"cannot open {path}: {exc.strerror}")
    for enc in ("utf-8-sig", "cp1252"):
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        sys.exit(f"cannot read {path}: save it as UTF-8 CSV")

    first = next((l for l in text.splitlines() if l.strip()), "")
    if not first:
        sys.exit("the CSV has no queries")
    # The separator is taken from the first line only (column names never contain
    # commas), so commas inside SQL can never be mistaken for column breaks.
    counts = {d: first.count(d) for d in (",", ";", "\t")}
    delim = max(counts, key=counts.get) if max(counts.values()) else None
    if delim is None or _looks_like_sql(first):
        # One column: every row is a whole SQL statement (quoted multi-line SQL still works).
        has_header = not _looks_like_sql(first)
        delim = None
        rows = [r for r in csv.reader(io.StringIO(text), delimiter="\x1f") if r and r[0].strip()]
        if has_header:
            rows = rows[1:]
        sql_col, name_col, ncols = 0, None, 1
    else:
        rows = [r for r in csv.reader(io.StringIO(text), delimiter=delim) if any(c.strip() for c in r)]
        has_header = not any(_looks_like_sql(c) for c in rows[0])
        # Without a header, the narrowest row gives the column count; wider rows had
        # unquoted separators inside their SQL and are rejoined below.
        ncols = len(rows[0]) if has_header else min(len(r) for r in rows)
        header = [h.strip() for h in rows[0]] if has_header else [f"column {i + 1}" for i in range(ncols)]
        data = rows[1:] if has_header else rows
        if not data:
            sys.exit("the CSV has no queries")
        sample = data[:500]

        def cells(i):
            return [r[i] for r in sample if len(r) > i and r[i].strip()]

        if sql_column:
            sql_col = _pick_column(header, sql_column, "--sql-column")
            sql_like = [sql_col]
        else:
            score = {i: (sum(map(_looks_like_sql, cells(i))) / len(cells(i)) if cells(i) else 0.0)
                     for i in range(len(header))}
            sql_like = [i for i in range(len(header)) if score[i] >= 0.5]
            if not sql_like:
                sys.exit("could not find a column of SQL queries (cells starting with SELECT, WITH, ...). "
                         f"Columns: {', '.join(header)}. Name it with --sql-column")
            sql_like.sort(key=lambda i: (-score[i], not SQL_HINT.search(header[i]), i))
            sql_col = sql_like[0]
        if name_column:
            name_col = _pick_column(header, name_column, "--name-column")
        else:
            others = [i for i in range(len(header)) if i not in sql_like]
            hinted = [i for i in others if NAME_HINT.search(header[i])] if has_header else []
            short = [i for i in others if cells(i) and all(len(c) <= 100 for c in cells(i))
                     and len(set(cells(i))) >= 0.9 * len(cells(i))]
            name_col = (hinted or short or [None])[0]
        note = f"reading SQL from column '{header[sql_col]}'"
        note += f", query names from '{header[name_col]}'" if name_col is not None else ", no name column"
        extra = [header[i] for i in sql_like[1:]]
        if extra:
            note += f" (also SQL-like: {', '.join(extra)}; pick another with --sql-column)"
        print(note)
        rows = data

    out, used = [], {}
    for line_no, r in enumerate(rows, 2 if has_header else 1):
        if len(r) > ncols:
            # An unquoted separator inside the SQL split it into extra fields.
            if sql_col == ncols - 1:
                r = r[:sql_col] + [delim.join(r[sql_col:])]
            else:
                sys.exit(f"row {line_no}: more fields than the header. Put the SQL in double quotes.")
        if len(r) <= sql_col or not r[sql_col].strip():
            continue
        sql = r[sql_col].strip().rstrip(";").strip()
        if not sql or _is_comment_only(sql):
            continue
        name = r[name_col].strip() if name_col is not None and len(r) > name_col else ""
        name = name or f"q{len(out) + 1:03d}"
        if name in used:
            n = used[name] + 1
            while f"{name}#{n}" in used:
                n += 1
            used[name] = n
            new = f"{name}#{n}"
            used[new] = 1
            print(f"note: query name '{name}' is used more than once; this one is reported as '{new}'")
            name = new
        else:
            used[name] = 1
        out.append((name, sql))
    if not out:
        sys.exit("the CSV has no queries")
    return out


# ---------------------------------------------------------------- e6data

def connect(schema, cluster=CLUSTER):
    return Connection(host=HOST, port=PORT, username=USERNAME, password=TOKEN,
                      catalog=CATALOG, database=schema, cluster_name=cluster,
                      secure=True, auto_resume=True)


def error_text(exc):
    details = getattr(exc, "details", None)
    try:
        msg = details() if callable(details) else None
    except Exception:
        msg = None
    return " ".join(str(msg or exc).split())[:1000]


def login(conn):
    """Log in now, outside any timed query, with a deadline (a blocked network
    otherwise hangs silently inside the connector). Returns an error message or None."""
    box = {}

    def go():
        try:
            box["sid"] = conn.get_session_id
        except Exception as exc:
            box["err"] = exc
    t = threading.Thread(target=go, daemon=True)
    t.start()
    t.join(LOGIN_TIMEOUT_S)
    if t.is_alive():
        return (f"could not reach {HOST}:{PORT} within {LOGIN_TIMEOUT_S}s. Check that outbound HTTPS (port 443) "
                "is allowed from this machine; behind a proxy, set the https_proxy environment variable.")
    if "err" in box:
        msg = error_text(box["err"])
        return f"login failed: {msg}" + (f"\n{SUSPENDED_HINT}" if "suspended" in msg.lower() else "")
    return None


def engine_stats(cur):
    """e6data's own timings for the query just run (one extra call, not in client_seconds)."""
    try:
        ea = cur.explain_analyse()
        plan = json.loads(ea.get("planner") or "{}")
        return {"e6_cached": ea.get("is_cached"),
                "e6_planning_ms": ea.get("parsing_time"),
                "e6_queue_ms": ea.get("queuing_time"),
                "e6_execution_ms": plan.get("total_query_time"),
                "e6_total_ms": plan.get("totalClientQueryTime")}
    except Exception:
        return None


def _close(c):
    try:
        c.close()
    except Exception:
        pass


def e6_open(schema, cluster):
    """A logged-in e6data connection, or (None, error)."""
    c = connect(schema, cluster)
    err = login(c)
    if err:
        _close(c)
        return None, err
    return c, None


def e6_cancel(cur):
    qid = getattr(cur, "query_id", None)
    if qid:
        cur.cancel(qid)


# ---------------------------------------------------------------- e6data usage and cost

USAGE_VIEW = "system.usage.core_usage"


def _utc_epoch(v):
    """A core_usage timestamp (datetime or ISO text) as epoch seconds; naive values are UTC."""
    if isinstance(v, (int, float)):
        return float(v)
    if not isinstance(v, datetime):
        v = datetime.fromisoformat(str(v).strip().replace("Z", "+00:00").replace(" ", "T", 1))
    if v.tzinfo is None:
        v = v.replace(tzinfo=timezone.utc)
    return v.timestamp()


def e6_usage(schema, cluster, t0, t1, attempts=9, wait_s=30.0):
    """e6data's own usage for this cluster between t0 and t1 (epoch seconds), from the
    system.usage.core_usage view: core-hours and cost, each usage interval counted only
    for the part that falls inside the run. Returns (dict, None) or (None, error)."""
    since = datetime.fromtimestamp(t0 - 86400, timezone.utc).strftime("%Y-%m-%d 00:00:00")
    name = cluster.replace("'", "''")
    sql = (f"SELECT active_start, active_end, total_cores, core_hours, total_cost FROM {USAGE_VIEW} "
           f"WHERE cluster_name = '{name}' AND active_end >= TIMESTAMP '{since}'")
    c, err = e6_open(schema, cluster)
    if err:
        return None, err
    try:
        for attempt in range(attempts):
            cur = c.cursor()
            try:
                cur.execute(sql)
                rows = cur.fetchall()
            finally:
                _close(cur)
            spans = [(_utc_epoch(r[0]), _utc_epoch(r[1]), float(r[2] or 0), float(r[3] or 0), float(r[4] or 0))
                     for r in rows if r[0] is not None and r[1] is not None]
            latest = max((e for _, e, *_ in spans), default=0.0)
            if latest >= t1 - 30 or attempt == attempts - 1:
                break
            time.sleep(wait_s)  # the newest usage can take a short while to appear
    except Exception as exc:
        return None, error_text(exc)
    finally:
        _close(c)
    hours = cost = 0.0
    cores = []
    for start, end, n, ch, usd in spans:
        overlap = min(end, t1) - max(start, t0)
        if overlap <= 0 or end <= start:
            continue
        share = overlap / (end - start)
        hours += ch * share
        cost += usd * share
        cores.append(n)
    if not cores:
        return None, f"no usage rows for cluster {cluster} during the run in {USAGE_VIEW}"
    return {"core_hours": hours, "cost": cost, "cores_min": min(cores), "cores_max": max(cores),
            "complete": latest >= t1 - 30, "recorded_to": latest}, None


def e6_cluster_ready(schema, cluster, settle_s=0, max_wait_s=300, poll_s=15):
    """Before the clock starts: wake the cluster with one small untimed query, then wait
    (up to max_wait_s) until e6data's usage view shows it at its usual size, so a cluster
    that is still starting up does not slow the first timed queries.
    Returns (usual cores or None, warning or None)."""
    c, err = e6_open(schema, cluster)
    if err:
        return None, err
    name = cluster.replace("'", "''")
    since = datetime.fromtimestamp(time.time() - 7 * 86400, timezone.utc).strftime("%Y-%m-%d 00:00:00")

    def q(sql):
        cur = c.cursor()
        try:
            cur.execute(sql)
            return cur.fetchall()
        finally:
            _close(cur)
    try:
        q("SELECT count(*) FROM date_dim")  # wakes the executors; not timed
        if settle_s:  # just resumed: give it a minute to finish starting before checking its size
            print(f"  the cluster was just resumed; waiting {settle_s:g} s for it to finish starting ...", flush=True)
            time.sleep(settle_s)
        weight = {}  # cores -> seconds at that size over the last week
        for n, sec in q(f"SELECT total_cores, active_seconds FROM {USAGE_VIEW} "
                        f"WHERE cluster_name = '{name}' AND active_end >= TIMESTAMP '{since}'"):
            weight[float(n or 0)] = weight.get(float(n or 0), 0.0) + float(sec or 0)
        usual = max(weight, key=weight.get) if weight else None
        if not usual:
            return None, None
        deadline = time.time() + max_wait_s
        while True:
            last = q(f"SELECT total_cores, active_end FROM {USAGE_VIEW} WHERE cluster_name = '{name}' "
                     "ORDER BY active_end DESC LIMIT 1")
            fresh = bool(last) and _utc_epoch(last[0][1]) >= time.time() - 120
            cores = float(last[0][0] or 0) if last else 0.0
            if not fresh or cores >= usual or time.time() >= deadline:
                break
            print(f"  cluster is at {cores:g} of its usual {usual:g} cores; waiting for it to finish starting ...",
                  flush=True)
            time.sleep(poll_s)
        if fresh and cores < usual:
            return usual, (f"the cluster was still at {cores:g} of its usual {usual:g} cores after "
                           f"{max_wait_s // 60} min, so timings may be slower")
        return usual, None
    except Exception as exc:
        return None, f"warm-up skipped: {error_text(exc)}"
    finally:
        _close(c)


# ---------------------------------------------------------------- Databricks
# Result cache off; Databricks' own timings come from the Query History API.

def dbr_settings(a):
    """Databricks connection details from the options or DATABRICKS_* variables; asks for any
    that are missing. The token is never printed."""
    def ask(label, value, env):
        value = (value or os.environ.get(env, "")).strip()
        if value:
            return value
        if not sys.stdin.isatty():
            sys.exit(f"Databricks {label} is missing: set {env}")
        value = input(f"Databricks {label}: ").strip()
        if not value:
            sys.exit(f"Databricks {label} is needed for --engine {a.engine}")
        return value
    cfg = {"host": re.sub(r"^https?://", "", ask("host (e.g. dbc-1234.cloud.databricks.com)",
                                                     a.dbr_host, "DATABRICKS_HOST")).rstrip("/"),
           "http_path": ask("HTTP path (/sql/1.0/warehouses/<id>)", a.dbr_http_path, "DATABRICKS_HTTP_PATH"),
           "catalog": ask("catalog", a.dbr_catalog, "DATABRICKS_CATALOG"),
           "schema": ask("schema", a.dbr_schema, "DATABRICKS_SCHEMA")}
    token = os.environ.get("DATABRICKS_TOKEN", "").strip()
    if not token:
        try:
            token = getpass.getpass("Databricks personal access token (input hidden): ").strip()
        except (EOFError, KeyboardInterrupt):
            token = ""
    if not token:
        sys.exit("a Databricks token is needed: set DATABRICKS_TOKEN or type it when asked")
    cfg["token"] = token
    return cfg


def dbr_open(cfg):
    """A Databricks connection with the result cache off, or (None, error). The first one
    can take minutes while a stopped SQL warehouse starts."""
    box = {}

    def go():
        try:
            from databricks import sql as dbsql
            # The connector logs its own retries and telemetry to the terminal; errors still reach
            # the results file through the exception.
            logging.getLogger("databricks").setLevel(logging.CRITICAL)
            logging.getLogger("urllib3").setLevel(logging.CRITICAL)
        except ImportError:
            box["err"] = "the Databricks connector is not installed: pip install databricks-sql-connector"
            return
        try:
            c = dbsql.connect(server_hostname=cfg["host"], http_path=cfg["http_path"],
                              access_token=cfg["token"], catalog=cfg["catalog"], schema=cfg["schema"])
        except Exception as exc:
            box["err"] = f"Databricks login failed: {error_text(exc)}"
            return
        try:  # a cached answer would not be a real run
            cur = c.cursor()
            try:
                cur.execute("SET use_cached_result = false")
            finally:
                cur.close()
        except Exception as exc:
            _close(c)
            box["err"] = f"could not turn off the Databricks result cache: {error_text(exc)}"
            return
        box["conn"] = c
    t = threading.Thread(target=go, daemon=True)
    t.start()
    t.join(DBR_LOGIN_TIMEOUT_S)
    if t.is_alive():
        return None, (f"no answer from Databricks {cfg['host']} within {DBR_LOGIN_TIMEOUT_S}s: check the host, "
                      "the HTTP path and that the SQL warehouse can start")
    return box.get("conn"), box.get("err")


def dbr_cancel(cur):
    cur.cancel()


def dbr_history(cfg, qids, attempts=8, wait_s=3.0):
    """Databricks' own timings per statement id from the query history API. The dotted
    filter_by form is required (the JSON form is a 400); FINISHED is recorded before the
    metrics are, so unfilled entries are asked again. Returns ({qid: timings}, warning)."""
    def one(qid):
        params = urllib.parse.urlencode({"filter_by.statement_ids": qid, "include_metrics": "true"})
        req = urllib.request.Request(f"https://{cfg['host']}/api/2.0/sql/history/queries?{params}",
                                     headers={"Authorization": f"Bearer {cfg['token']}"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.load(resp)
        for q in payload.get("res") or []:
            if q.get("query_id") != qid or q.get("status") != "FINISHED":
                continue
            m = q.get("metrics") or {}
            if m.get("execution_time_ms") is None:
                return None
            return {"dbr_compile_ms": m.get("compilation_time_ms"), "dbr_execution_ms": m.get("execution_time_ms"),
                    "dbr_total_ms": q.get("duration"), "dbr_rows_read": m.get("rows_read_count"),
                    "dbr_cached": m.get("result_from_cache")}
        return None

    found, pending, warning, last_err = {}, list(dict.fromkeys(qids)), None, ""
    for attempt in range(attempts):
        def safe(qid):
            try:
                return qid, one(qid), None
            except urllib.error.HTTPError as exc:
                return qid, None, f"HTTP {exc.code}"
            except Exception as exc:
                return qid, None, " ".join(str(exc).split())[:150]
        with ThreadPoolExecutor(4) as ex:  # few at a time: the API rate-limits bursts (HTTP 429)
            got = list(ex.map(safe, pending))
        if any(err in ("HTTP 401", "HTTP 403") for _, _, err in got):
            return found, "the Databricks token cannot read query history, so Databricks server timings are blank"
        found.update({qid: t for qid, t, _ in got if t})
        pending = [qid for qid, t, _ in got if not t]
        errs = [err for _, t, err in got if err]
        last_err = errs[-1] if errs else last_err
        if not pending:
            break
        if attempt < attempts - 1:
            time.sleep(wait_s * (attempt + 1))  # wait longer each round
    if pending:
        warning = (f"Databricks server timings not found for {len(pending)} execution(s)"
                   + (f" (last error: {last_err})" if last_err else " (not in query history yet)"))
    return found, warning


# ---------------------------------------------------------------- one query

def run_one(conn, eng, name, run, worker, sql, inflight, stop):
    rec = {"engine": eng.label, "name": name, "run": run, "worker": worker, "sql": sql, "query_id": "",
           "error": "", "rows": 0, "status": "FAILED", "client_seconds": 0.0}
    rec["started"] = now_iso()
    t0 = time.perf_counter()
    cur = None
    try:
        cur = conn.cursor()
        inflight[(eng.key, worker)] = (eng, cur)
        cur.execute(sql)
        rows = cur.fetchall() or []
        rec["client_seconds"] = round(time.perf_counter() - t0, 3)
        rec["finished"] = now_iso()
        rec.update(status="OK", rows=len(rows))
        if eng.stats:
            stats = eng.stats(cur)
            rec.update(stats if stats is not None else {"_stats_missing": True})
    except Exception as exc:
        rec["client_seconds"] = round(time.perf_counter() - t0, 3)
        rec["finished"] = now_iso()
        rec["error"] = error_text(exc)
        if stop.is_set():
            rec["status"] = "CANCELLED"
    finally:
        inflight.pop((eng.key, worker), None)
        if cur is not None:
            try:
                rec["query_id"] = str(getattr(cur, "query_id", "") or "")
            except Exception:
                pass
            try:
                cur.close()
            except Exception:
                pass
    if not rec["query_id"]:  # failed e6data queries carry their id at the start of the error
        m = re.match(r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})", rec["error"])
        rec["query_id"] = m.group(1) if m else ""
    return rec


class Engine:
    """One engine to send the queries to, with its own connections and counters."""

    def __init__(self, key, label, open_conn, cancel, stats=None, where=""):
        self.key, self.label, self.open_conn, self.cancel, self.stats, self.where = \
            key, label, open_conn, cancel, stats, where
        self.conns, self.pool, self.jobs = [], queue.Queue(), queue.Queue()
        self.arr = {"lags": [], "open": 0, "running": 0, "peak": 0, "open_at_end": None,
                    "running_at_end": None, "extra_logins": 0, "login_errors": 0}

    def open_many(self, n):
        """n ready connections before the clock starts: one first (it may wake a stopped
        warehouse or cluster), then the rest at once. Returns an error or None."""
        c, err = self.open_conn()
        if err:
            return err
        self.conns.append(c)
        box = [None] * (n - 1)

        def go(i):
            box[i] = self.open_conn()
        threads = [threading.Thread(target=go, args=(i,), daemon=True) for i in range(n - 1)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        for res in box:
            if res is None:
                return f"{self.label}: a login did not finish"
            c, err = res
            if err:
                return err
            self.conns.append(c)
        return None

    def close_all(self):
        for c in self.conns:
            _close(c)


# ---------------------------------------------------------------- stats

def quantile(values, q):
    """Linear interpolation between closest ranks (same as Excel PERCENTILE.INC)."""
    v = sorted(values)
    if len(v) == 1:
        return v[0]
    pos = q * (len(v) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (pos - lo)


def write_summary(a, results, total, mode="", extra_facts=(), engines=("e6data",)):
    """Run summary next to the results file, in a percentile layout:
    one row per percentile, one column per time (ms), over successful executions; the
    databricks_ms columns are filled when Databricks ran too (else left to paste into);
    counts underneath. Returns its path, or None."""
    base = a.out[:-4] if a.out.lower().endswith(".csv") else a.out
    path = f"{base}_summary.csv"

    def ms(r, key):
        v = r.get(key)
        return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None
    e6_ok = [r for r in results if r["status"] == "OK" and r.get("engine", "e6data") == "e6data"]
    dbr_ok = [r for r in results if r["status"] == "OK" and r.get("engine") == "databricks"]
    cols = []
    if "e6data" in engines:
        cols.append(("e6_client_ms", [r["client_seconds"] * 1000 for r in e6_ok]))
        for key in ("e6_total_ms", "e6_execution_ms", "e6_queue_ms", "e6_planning_ms"):
            cols.append((key, [x for x in (ms(r, key) for r in e6_ok) if x is not None]))
    if "databricks" in engines:
        cols.append(("databricks_client_ms", [r["client_seconds"] * 1000 for r in dbr_ok]))
        for key, label in (("dbr_total_ms", "databricks_ms"), ("dbr_execution_ms", "databricks_execution_ms"),
                           ("dbr_compile_ms", "databricks_compile_ms")):
            cols.append((label, [x for x in (ms(r, key) for r in dbr_ok) if x is not None]))
    else:
        cols.append(("databricks_ms", []))

    stats = [(f"p{q}", lambda v, q=q: quantile(v, q / 100)) for q in (50, 75, 90, 99)] + \
            [("max", max), ("mean", statistics.mean), ("total", sum)]
    width = 1 + len(cols)
    rows = [["percentile"] + [name for name, _ in cols]]
    for label, fn in stats:
        rows.append([label] + [round(fn(v)) if v else "" for _, v in cols])

    def counts(eng, prefix):
        mine = [r for r in results if r.get("engine", "e6data") == eng]
        return [(f"{prefix}SUCCESS", sum(1 for r in mine if r["status"] == "OK")),
                (f"{prefix}FAILED", sum(1 for r in mine if r["status"] == "FAILED")),
                (f"{prefix}CANCELLED", sum(1 for r in mine if r["status"] == "CANCELLED")),
                (f"{prefix}NOT_RUN", total - len(mine))]
    facts = []
    if "e6data" in engines:
        facts += counts("e6data", "") + [("dataset", f"{CATALOG}.{a.schema}"), ("cluster", a.cluster)]
    if "databricks" in engines:
        facts += counts("databricks", "databricks_")
    facts += [("mode", mode)] + list(extra_facts)
    rows += [[k, v] + [""] * (width - 2) for k, v in facts]
    try:
        with open(path, "w", newline="", encoding="utf-8-sig") as sf:
            csv.writer(sf).writerows(rows)
        return path
    except OSError as exc:
        print(f"warning: could not write {path}: {exc.strerror}")
        return None


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("csv", help="input CSV of queries")
    ap.add_argument("--runs", type=int, default=1, help="times each query is run (default 1)")
    ap.add_argument("--pattern",
                    help='arrival-rate mode with changing rates: a JSON file (or JSON text) listing steps, e.g. '
                         '[{"Seconds": 10, "QPS": 2}, {"Seconds": 8, "QPS": 5}], run in order, cycling through the CSV')
    ap.add_argument("--max-inflight", type=int, default=64,
                    help="with --pattern: most queries open at once from this machine, per engine (default 64)")
    ap.add_argument("--cluster", default=CLUSTER, help=f"e6data cluster to run on (default {CLUSTER})")
    ap.add_argument("--schema", default=SCHEMA, choices=SCHEMAS, help=f"dataset in catalog {CATALOG} (default {SCHEMA})")
    ap.add_argument("--engine", default="e6data", choices=["e6data", "databricks", "both"],
                    help="where to run: e6data (default), databricks, or both at the same time on the same schedule")
    ap.add_argument("--dbr-host", help="Databricks workspace host (default: DATABRICKS_HOST)")
    ap.add_argument("--dbr-http-path", help="SQL warehouse HTTP path, /sql/1.0/warehouses/<id> (default: DATABRICKS_HTTP_PATH)")
    ap.add_argument("--dbr-catalog", help="Databricks catalog holding the same tables (default: DATABRICKS_CATALOG)")
    ap.add_argument("--dbr-schema", help="Databricks schema holding the same tables (default: DATABRICKS_SCHEMA)")
    ap.add_argument("--sql-column", help="column holding the SQL, by header or 1-based number (default: found automatically)")
    ap.add_argument("--name-column", help="column holding query names, by header or 1-based number (default: found automatically)")
    ap.add_argument("--no-engine-stats", action="store_true",
                    help="skip fetching e6data's server-side timings (use for throughput runs)")
    ap.add_argument("--out", default=f"e6_results_{time.strftime('%Y%m%d_%H%M%S')}.csv",
                    help="results CSV (default e6_results_<date>_<time>.csv; an existing file is overwritten)")
    a = ap.parse_args()
    a.concurrency, a.qps, a.duration = 1, None, None  # one at a time unless --pattern sets the arrival rate
    if a.runs < 1:
        sys.exit("--runs must be 1 or more")
    if a.max_inflight < 1:
        sys.exit("--max-inflight must be 1 or more")
    offsets = None  # start time of each query, in seconds from the start (arrival-rate modes)
    if a.pattern is not None:
        if a.runs != 1:
            sys.exit("--runs does not apply with --pattern (the pattern sets how many queries start)")
        try:
            text = open(a.pattern, encoding="utf-8").read() if os.path.isfile(a.pattern) else a.pattern
            steps = [(float(st["Seconds"]), float(st["QPS"])) for st in json.loads(text)]
        except (OSError, ValueError, KeyError, TypeError) as exc:
            sys.exit(f"cannot read --pattern: {exc}")
        if not steps or any(sec <= 0 or rate <= 0 for sec, rate in steps):
            sys.exit("--pattern needs at least one step, each with Seconds and QPS above 0")
        offsets, t0 = [], 0.0
        for sec, rate in steps:
            offsets += [t0 + k / rate for k in range(max(1, round(sec * rate)))]
            t0 += sec
        a.qps = len(offsets) / t0  # average rate, used for the pool size and the report
        a.duration = t0
        pattern_desc = ", ".join(f"{rate:g}/s x {sec:g}s" for sec, rate in steps)

    queries = read_queries(a.csv, a.sql_column, a.name_column)
    if os.path.abspath(a.out) == os.path.abspath(a.csv):
        sys.exit("--out is the same file as the input CSV; choose another name")
    use_e6 = a.engine in ("e6data", "both")
    use_dbr = a.engine in ("databricks", "both")
    dbr = dbr_settings(a) if use_dbr else None
    engines = []
    if use_e6:
        engines.append(Engine("e6", "e6data", lambda: e6_open(a.schema, a.cluster), e6_cancel,
                              None if a.no_engine_stats else engine_stats,
                              f"cluster {a.cluster}, dataset {CATALOG}.{a.schema}, as {USERNAME}"))
    if use_dbr:
        engines.append(Engine("dbr", "databricks", lambda: dbr_open(dbr), dbr_cancel, None,
                              f"warehouse {dbr['http_path'].rsplit('/', 1)[-1]}, "
                              f"dataset {dbr['catalog']}.{dbr['schema']}, result cache off"))
    both = len(engines) == 2
    fields = (["engine"] if use_dbr else []) + FIELDS + (["started"] if use_dbr else []) + \
             (["dbr_compile_ms", "dbr_execution_ms", "dbr_total_ms", "dbr_rows_read", "dbr_cached"] if use_dbr else [])
    try:
        f = open(a.out, "w", newline="", encoding="utf-8-sig")
    except OSError as exc:
        sys.exit(f"cannot write {a.out}: {exc.strerror}")
    writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()

    joblist = []
    if a.qps is not None and a.duration is not None:
        n = len(offsets) if offsets is not None else max(1, math.ceil(a.qps * a.duration))
        joblist = [(queries[i % len(queries)][0], i // len(queries) + 1, queries[i % len(queries)][1]) for i in range(n)]
    else:
        joblist = [(name, run, sql) for run in range(1, a.runs + 1) for name, sql in queries]
    for eng in engines:
        for j in joblist:
            eng.jobs.put(j)
    total = len(joblist)
    w = max(14, max(len(n) for n, _ in queries))
    where = "; ".join(f"{eng.label}: {eng.where}" for eng in engines)
    if a.qps is not None:
        peak_rate = max(r for _, r in steps) if offsets is not None else a.qps
        workers = min(total, a.max_inflight, max(2, math.ceil(peak_rate * 4)))  # pre-logged-in pool; grows on demand
        if offsets is not None:
            mode = f"pattern {pattern_desc} ({a.duration:g} s)"
            print(f"{len(queries)} queries, {total} executions over {a.duration:g}s in steps ({pattern_desc})")
        else:
            mode = f"{a.qps:g} qps x {total / a.qps:g} s"
            print(f"{len(queries)} queries, {total} executions started at {a.qps:g} per second "
                  f"(about {total / a.qps:.0f}s)")
    else:
        workers = min(a.concurrency, total)
        mode = f"{a.runs} run(s), one at a time"
        print(f"{len(queries)} queries x {a.runs} run(s) = {total} executions, one at a time")
    for eng in engines:
        print(f"  {eng.label}: {eng.where}")
    if both:
        print("  both engines get the same queries on the same schedule, starting together")

    # Connections are logged in before the clock starts.
    def close_all():
        for eng in engines:
            eng.close_all()
    for eng in engines:
        print(f"logging in to {eng.label} ({workers} connection(s)) ..."
              + (" if the cluster is paused it is resumed first, which can take a few minutes" if eng.key == "e6" else ""),
              flush=True)
        login_t0 = time.time()
        try:
            err = eng.open_many(workers)
        except KeyboardInterrupt:
            err = "interrupted while logging in; no queries were run"
        if err:
            close_all()
            f.close()
            sys.exit(err)
        if eng.key == "e6":
            e6_login_s = time.time() - login_t0
        for i, c in enumerate(eng.conns):
            eng.pool.put((i + 1, c))

    if use_e6:
        print("warming up e6data (not timed) ...", flush=True)
        _, warm_msg = e6_cluster_ready(a.schema, a.cluster, settle_s=60 if e6_login_s > RESUMED_LOGIN_S else 0)
        if warm_msg:
            print(f"  note: {warm_msg}")

    lock = threading.Lock()
    stop = threading.Event()
    inflight = {}
    results = []
    state = {"suspended_warned": False, "closed": False, "stats_missing": 0}
    grand_total = total * len(engines)

    def record(rec, eng, wid, name, run):
        with lock:
            if state["closed"]:
                return
            if rec.pop("_stats_missing", False):
                state["stats_missing"] += 1
            results.append(rec)
            writer.writerow(rec)
            f.flush()
            extra = f"  server {rec['e6_total_ms']} ms" if rec.get("e6_total_ms") is not None else ""
            tag = f"{eng.label:<10} " if both else ""
            print(f"  [{len(results):>4}/{grand_total}] {tag}w{wid} {name:<{w}} run {run}  {rec['status']:<9} "
                  f"{rec['client_seconds']:>8.2f}s  rows={rec['rows']}{extra}"
                  + (f"  {rec['error'][:150]}" if rec["error"] else ""), flush=True)
            if (eng.key == "e6" and rec["error"] and "suspended" in rec["error"].lower()
                    and not state["suspended_warned"]):
                state["suspended_warned"] = True
                print("  " + SUSPENDED_HINT)

    def worker(eng, wid, conn):
        while not stop.is_set():
            try:
                name, run, sql = eng.jobs.get_nowait()
            except queue.Empty:
                return
            record(run_one(conn, eng, name, run, wid, sql, inflight, stop), eng, wid, name, run)

    # Arrival-rate mode: a pool of logged-in connections per engine, one thread per started query.
    def get_conn(eng):
        try:
            return eng.pool.get_nowait()
        except queue.Empty:
            pass
        with lock:
            grow = len(eng.conns) < a.max_inflight
            if grow:
                eng.conns.append(None)  # hold the slot while logging in
                cid = len(eng.conns)
        if grow:
            c, err = eng.open_conn()
            if not err:
                with lock:
                    eng.conns[cid - 1] = c
                eng.arr["extra_logins"] += 1
                return cid, c
            eng.arr["login_errors"] += 1
        return eng.pool.get()  # at --max-inflight: wait for a connection to come back

    def arrival(eng, name, run, sql, due):
        cid, conn = get_conn(eng)
        if stop.is_set():  # Ctrl+C while waiting for a connection: don't start it
            eng.pool.put((cid, conn))
            with lock:
                eng.arr["open"] -= 1
            return
        lag = time.perf_counter() - due
        with lock:  # sent to the engine now; before this it was waiting for a connection here
            eng.arr["running"] += 1
            eng.arr["peak"] = max(eng.arr["peak"], eng.arr["running"])
        rec = run_one(conn, eng, name, run, cid, sql, inflight, stop)
        with lock:  # count it finished before its connection is handed to the next query
            eng.arr["lags"].append(lag)
            eng.arr["open"] -= 1
            eng.arr["running"] -= 1
        eng.pool.put((cid, conn))
        record(rec, eng, cid, name, run)

    wall0 = time.perf_counter()
    run_t0 = time.time()
    interrupted = False
    threads = []
    try:
        if a.qps is None:
            threads = [threading.Thread(target=worker, args=(eng, i + 1, c), daemon=True)
                       for i in range(workers) for eng in engines for c in [eng.conns[i]]]
            for t in threads:
                t.start()
        else:
            for i, (name, run, sql) in enumerate(joblist):
                due = wall0 + (offsets[i] if offsets is not None else i / a.qps)
                while not stop.is_set() and time.perf_counter() < due:
                    time.sleep(min(0.05, max(0.0, due - time.perf_counter())))
                for eng in engines:  # same query, same moment, on every engine
                    with lock:
                        eng.arr["open"] += 1
                    t = threading.Thread(target=arrival, args=(eng, name, run, sql, due), daemon=True)
                    threads.append(t)
                    t.start()
            with lock:
                for eng in engines:
                    eng.arr["open_at_end"] = eng.arr["open"]
                    eng.arr["running_at_end"] = eng.arr["running"]
                    eng.arr["send_s"] = time.perf_counter() - wall0
        while any(t.is_alive() for t in threads):
            for t in threads:
                t.join(0.5)
    except KeyboardInterrupt:
        interrupted = True
        stop.set()
        print(f"\ninterrupted: cancelling {len(inflight)} running queries (waiting up to {INTERRUPT_WAIT_S}s; "
              "press Ctrl+C again to quit at once) ...", flush=True)

        def cancel(eng, cur):
            try:
                eng.cancel(cur)
            except Exception:
                pass
        # All cancels at once, in the background: a busy engine can be slow to answer each one.
        for eng, cur in list(inflight.values()):
            threading.Thread(target=cancel, args=(eng, cur), daemon=True).start()
        deadline = time.monotonic() + INTERRUPT_WAIT_S
        try:
            for t in threads:
                t.join(max(0.0, deadline - time.monotonic()))
        except KeyboardInterrupt:
            print("quitting now; queries still running on the engines finish or time out there", flush=True)
            with lock:
                state["closed"] = True
                f.close()
            os._exit(1)
    wall = time.perf_counter() - wall0
    run_t1 = time.time()
    with lock:
        state["closed"] = True
        f.close()
    closer = threading.Thread(target=close_all, daemon=True)
    closer.start()
    closer.join(10)

    notes = []
    if use_dbr:
        qids = [r["query_id"] for r in results if r.get("engine") == "databricks" and r["status"] == "OK"
                and r["query_id"]]
        if qids:
            print(f"\nfetching Databricks server timings for {len(qids)} execution(s) from query history ...", flush=True)
            found, warning = dbr_history(dbr, qids)
            for r in results:
                if r.get("engine") == "databricks" and r["query_id"] in found:
                    r.update(found[r["query_id"]])
            if warning:
                notes.append(warning)
            cached = sum(1 for r in results if r.get("dbr_cached"))
            if cached:
                notes.append(f"{cached} Databricks execution(s) were answered from its result cache")
    if use_dbr:  # rewrite the results with the Databricks timings added after the run
        try:
            with open(a.out, "w", newline="", encoding="utf-8-sig") as f2:
                wr = csv.DictWriter(f2, fieldnames=fields, extrasaction="ignore")
                wr.writeheader()
                wr.writerows(results)
        except OSError as exc:
            print(f"warning: could not rewrite {a.out}: {exc.strerror}")

    failed = sum(1 for r in results if r["status"] != "OK")
    not_run = grand_total - len(results)
    extra = []
    usage = None
    if use_e6 and results:
        print(f"\nreading e6data usage and cost for this run from {USAGE_VIEW} ...", flush=True)
        usage, usage_err = e6_usage(a.schema, a.cluster, run_t0, run_t1)
        if usage_err:
            notes.append(f"could not read e6data usage and cost: {usage_err}")
        else:
            extra += [("e6_core_hours", round(usage["core_hours"], 4)), ("e6_cost_usd", round(usage["cost"], 4))]
    if a.qps is not None:
        for eng in engines:
            arr = eng.arr
            end = arr["open_at_end"] is not None
            p = "" if eng.key == "e6" else "databricks_"
            extra += [(f"{p}peak_running", arr["peak"]),
                      (f"{p}running_when_arrivals_ended", arr["running_at_end"] if end else ""),
                      (f"{p}waiting_when_arrivals_ended", arr["open_at_end"] - arr["running_at_end"] if end else "")]
    if use_dbr:
        extra += [("databricks_dataset", f"{dbr['catalog']}.{dbr['schema']}"),
                  ("databricks_warehouse", dbr["http_path"].rsplit("/", 1)[-1])]
    summary_path = write_summary(a, results, total, mode, extra, [eng.label for eng in engines])
    print(f"\n{len(results) - failed} ok, {failed} failed/cancelled" + (f", {not_run} not run" if not_run else "")
          + f", wall time {wall:.1f}s -> {a.out}" + (f" and {summary_path}" if summary_path else ""))

    for eng in engines:
        ok = [r for r in results if r["status"] == "OK" and r.get("engine", "e6data") == eng.label]
        if not ok:
            continue
        srv_key = "e6_total_ms" if eng.key == "e6" else "dbr_total_ms"
        print(f"\n{eng.label}\n{'query':<{w}} {'ok runs':>7} {'median s':>9} {'min s':>7} {'max s':>7} {'server median s':>16}")
        for name, _ in queries:
            mine = [r for r in ok if r["name"] == name]
            if not mine:
                print(f"{name:<{w}} {0:>7}")
                continue
            secs = [r["client_seconds"] for r in mine]
            srv = [r[srv_key] / 1000 for r in mine if isinstance(r.get(srv_key), (int, float))]
            print(f"{name:<{w}} {len(mine):>7} {statistics.median(secs):>9.2f} {min(secs):>7.2f} {max(secs):>7.2f} "
                  f"{(f'{statistics.median(srv):.2f}' if srv else '-'):>16}")
        best = [min(r["client_seconds"] for r in ok if r["name"] == name) for name, _ in queries
                if any(r["name"] == name for r in ok)]
        if a.qps is None:
            print(f"{eng.label} suite total (sum of each query's fastest run): {sum(best):.2f}s "
                  f"over {len(best)} of {len(queries)} queries")
        secs = [r["client_seconds"] for r in ok]
        print(f"{eng.label} successful executions: throughput {len(ok) / wall:.2f} queries/s, "
              f"client latency p50 {quantile(secs, .5):.2f}s  p95 {quantile(secs, .95):.2f}s  "
              f"max {max(secs):.2f}s  mean {statistics.mean(secs):.2f}s")
        arr = eng.arr
        if a.qps is not None and arr["lags"]:
            lags = arr["lags"]
            print(f"{eng.label} arrivals: target {('average ' if offsets is not None else '')}{a.qps:.3g} queries/s, "
                  f"{len(lags)} started over {arr.get('send_s', wall):.1f}s; "
                  f"start delay p50 {quantile(lags, .5) * 1000:.0f} ms, max {max(lags) * 1000:.0f} ms; "
                  f"peak running {arr['peak']} (--max-inflight {a.max_inflight})"
                  + ("" if arr["open_at_end"] is None else
                     f"; when arrivals ended: {arr['running_at_end']} running, "
                     f"{arr['open_at_end'] - arr['running_at_end']} waiting for a connection here"))
            if arr["extra_logins"]:
                print(f"note: {arr['extra_logins']} extra {eng.label} connection(s) were opened during the run "
                      "(login time is not counted in client_seconds)")
            if max(lags) > 1:
                print(f"note: some {eng.label} queries started more than 1 s late, so the target rate was not fully "
                      f"met: all {a.max_inflight} connections were busy, so it saw at most {a.max_inflight} "
                      "queries at once. Raise --max-inflight for a true arrival-rate test")
    e6_ok = [r for r in results if r["status"] == "OK" and r.get("engine", "e6data") == "e6data"]
    if e6_ok and not a.no_engine_stats:
        print("note: fetching e6data server timings adds a short call after each query, which lowers throughput; "
              "use --no-engine-stats for throughput runs")
    cached = sum(1 for r in e6_ok if r.get("e6_cached"))
    if cached:
        print(f"note: {cached} execution(s) were answered from e6data's result cache")
    if state["stats_missing"]:
        print(f"note: e6data server timings could not be fetched for {state['stats_missing']} execution(s)")
    if usage:
        span = (f"{usage['cores_min']:g} cores" if usage["cores_min"] == usage["cores_max"]
                else f"{usage['cores_min']:g} to {usage['cores_max']:g} cores")
        print(f"\ne6data usage during this run (cluster {a.cluster}, {span}): "
              f"{usage['core_hours']:.2f} core-hours, cost ${usage['cost']:.2f}")
        print(f"  from {USAGE_VIEW}; this is the whole cluster's usage while the run was going, "
              "including anyone else's queries on it at the same time"
              "")
        if not usage["complete"]:
            gap = max(1, round((run_t1 - usage["recorded_to"]) / 60))
            print(f"  warning: usage for the last ~{gap} min of the run is not recorded yet, so this "
                  f"figure is low. Rerun the SQL in the README (Usage and cost) in about 10 minutes "
                  f"for the full number.")
    for n in notes:
        print(f"note: {n}")
    sys.exit(0 if failed == 0 and not_run == 0 and not interrupted else 1)


if __name__ == "__main__":
    main()
