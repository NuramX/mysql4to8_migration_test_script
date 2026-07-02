#!/usr/bin/env python3
"""
Flask web server for the prs Migration Validator dashboard.
Serves the web UI and streams validation results via Server-Sent Events (SSE).

Key design:
  - All subprocess output is collected into an in-memory buffer.
  - Any number of browser tabs can subscribe to /api/all-tables/stream and will
    receive all past events (replay from position 0) plus new events as they arrive.
  - This prevents the "first-reader consumes stdout" problem.
"""

import subprocess
import sys
import os
import json
import math
import threading
import time
from flask import Flask, Response, jsonify, send_from_directory, request
import datetime as _dt

app = Flask(__name__, static_folder="static")

# ─── Global state ─────────────────────────────────────────────────────────────
_process: subprocess.Popen | None = None
_process_lock = threading.Lock()

_output_buffer: list[str] = []
_buffer_lock = threading.Lock()
_run_done = threading.Event()

# Merged snapshot of all runs — persists across page refreshes.
# Full run replaces it entirely; sync-only run updates only selected tables.
_snapshot: list[str] = []
_snapshot_lock = threading.Lock()
_current_sync_tables: list[str] = []   # set before each run


def _reset_state(sync_tables: list | None = None):
    global _output_buffer
    with _buffer_lock:
        _output_buffer = []
    _run_done.clear()
    reports_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "reports")
    if not os.path.isdir(reports_dir):
        return
    for fname in os.listdir(reports_dir):
        if not fname.endswith((".csv", ".ndjson", ".json")):
            continue
        # Full run: delete everything.  Sync-only run: delete only files that
        # belong to the tables being re-scanned so other tables keep their data.
        if sync_tables:
            if not any(fname.startswith(t + "_") for t in sync_tables):
                continue
        try:
            os.remove(os.path.join(reports_dir, fname))
        except Exception:
            pass


def _merge_snapshot(new_events: list[str], sync_tables: list[str]) -> list[str]:
    """Return updated snapshot: full run replaces all; sync-only updates selected tables.

    For sync runs we do a fine-grained merge: keep every old event for
    selected tables EXCEPT the ones the new run re-emits.  This preserves
    row_count / schema / indexes / dup_pk results from a prior validation
    run when only full_sync is re-run.
    """
    non_done = [e for e in new_events if '"type": "done"' not in e]
    done_events = [e for e in new_events if '"type": "done"' in e]
    if not sync_tables:
        return non_done + done_events

    sync_set = set(sync_tables)

    # Collect which (table, check) pairs and which table_starts the new run emits
    new_checks: set[tuple[str, str]] = set()
    new_table_starts: set[str] = set()
    for e in non_done:
        try:
            ev = json.loads(e)
            t = ev.get("table")
            if not t or t not in sync_set:
                continue
            if ev.get("type") == "table_start":
                new_table_starts.add(t)
            elif ev.get("type") == "result":
                new_checks.add((t, ev.get("check", "")))
        except Exception:
            pass

    # Keep old snapshot events, skipping only the specific events the new run replaces
    kept = []
    for line in _snapshot:
        if '"type": "done"' in line:
            continue
        try:
            ev = json.loads(line)
            t = ev.get("table")
            if t in sync_set:
                etype = ev.get("type")
                if etype == "table_start" and t in new_table_starts:
                    continue  # replaced by new table_start
                if etype == "result" and (t, ev.get("check", "")) in new_checks:
                    continue  # replaced by new result event
        except Exception:
            pass
        kept.append(line)

    return kept + non_done + done_events


def _reader_thread(proc: subprocess.Popen):
    for raw in proc.stdout:
        text = raw.decode("utf-8", errors="replace").strip()
        if text:
            with _buffer_lock:
                _output_buffer.append(text)
    proc.wait()
    exit_code = proc.returncode
    done_line = json.dumps({"type": "done", "exit_code": exit_code})
    with _buffer_lock:
        _output_buffer.append(done_line)
    with _snapshot_lock:
        global _snapshot
        _snapshot = _merge_snapshot(list(_output_buffer), _current_sync_tables)
    _run_done.set()


def _get_data_window(year_from, year_to, month_from, month_to, data_window_years=0,
                     day_from=None, day_to=None):
    import datetime as _dt
    def _int(v, lo=None, hi=None):
        try:
            n = int(v)
            if lo is not None and n < lo: return None
            if hi is not None and n > hi: return None
            return n
        except (ValueError, TypeError):
            return None

    yfrom = _int(year_from)
    yto   = _int(year_to)
    mfrom = _int(month_from, 1, 12)
    mto   = _int(month_to,   1, 12)
    dfrom = _int(day_from,   1, 31)
    dto   = _int(day_to,     1, 31)

    base_year = _dt.date.today().year
    if yfrom is not None:
        _yfrom = yfrom
    elif data_window_years > 0:
        _yfrom = base_year - data_window_years
    else:
        _yfrom = None

    if _yfrom is not None:
        _dm = mfrom if mfrom else 1
        _dd = dfrom if dfrom else 1
        win_start = f"{_yfrom}-{_dm:02d}-{_dd:02d}"
    else:
        win_start = None

    today_str = _dt.date.today().isoformat()
    if yto is not None:
        _mto = mto if mto else 12
        # day_to only applies when year+month both explicit
        if dto is not None and mto is not None:
            try:
                d_end = _dt.date(yto, _mto, dto) + _dt.timedelta(days=1)
                calc_end = d_end.isoformat()
            except ValueError:
                calc_end = f"{yto}-{_mto + 1:02d}-01" if _mto < 12 else f"{yto + 1}-01-01"
        elif _mto == 12:
            calc_end = f"{yto + 1}-01-01"
        else:
            calc_end = f"{yto}-{_mto + 1:02d}-01"
        win_end = min(calc_end, today_str)
    else:
        win_end = today_str

    return win_start, win_end


def _get_window_label(win_start, win_end):
    if win_start and win_end:
        import datetime as _dt
        try:
            _end_excl = _dt.date.fromisoformat(win_end)
            _last_day = (_end_excl - _dt.timedelta(days=1)).isoformat()
            return f"{win_start} → {_last_day}"
        except Exception:
            pass
    if win_start:
        return f"≥{win_start}"
    return None


# ─── Routes ───────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/all-tables/status")
def at_status():
    with _process_lock:
        running = _process is not None and _process.poll() is None
    done = _run_done.is_set()
    with _buffer_lock:
        n_events = len(_output_buffer)
    with _snapshot_lock:
        n_snapshot = len(_snapshot)
    return jsonify({"running": running, "done": done, "events": n_events, "snapshot": n_snapshot})


@app.route("/api/all-tables/snapshot")
def at_snapshot():
    """Return merged results from all runs as a JSON array (for page-load replay)."""
    with _snapshot_lock:
        events = list(_snapshot)
    return jsonify({"events": events})


@app.route("/api/all-tables/run", methods=["POST"])
def at_run_validation():
    global _process, _current_sync_tables
    data       = request.json if request.is_json else {}
    db         = data.get("db", "prs")
    skip_sync  = data.get("skip_sync", True)
    sync_limit = data.get("sync_limit", 0)
    sync_tables   = data.get("sync_tables", [])    # list of table names for full sync pass
    tables_filter = data.get("tables", [])         # scan only these tables (all checks)
    year_from  = data.get("year_from")          # int or None → config default window
    year_to    = data.get("year_to")
    month_from      = data.get("month_from")         # int 1-12 or None
    month_to        = data.get("month_to")
    day_from        = data.get("day_from")
    day_to          = data.get("day_to")

    with _process_lock:
        if _process is not None and _process.poll() is None:
            return jsonify({"error": "Validation already running"}), 409
        _current_sync_tables = list(sync_tables)
        _reset_state(sync_tables=sync_tables or None)
        script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "validate_all_tables.py")
        cmd = [sys.executable, script, "--json", "--db", db]
        if sync_tables:
            cmd += ["--sync-tables", ",".join(sync_tables)]
        elif skip_sync:
            cmd.append("--skip-sync")
        if sync_limit:
            cmd += ["--sync-limit", str(sync_limit)]
        if year_from:
            cmd += ["--year-from", str(int(year_from))]
        if year_to:
            cmd += ["--year-to", str(int(year_to))]
        if month_from:
            cmd += ["--month-from", str(int(month_from))]
        if month_to:
            cmd += ["--month-to", str(int(month_to))]
        if day_from:
            cmd += ["--day-from", str(int(day_from))]
        if day_to:
            cmd += ["--day-to", str(int(day_to))]
        if tables_filter:
            cmd += ["--tables", ",".join(tables_filter)]
        _process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )
    t = threading.Thread(target=_reader_thread, args=(_process,), daemon=True)
    t.start()
    return jsonify({"started": True, "db": db})


@app.route("/api/list-tables", methods=["POST"])
def list_tables():
    """Return all table names from source DB — no comparison, just names."""
    data = request.json if request.is_json else {}
    db   = data.get("db", "prs")
    try:
        from mysql40 import MySQL40Connection
        cfg     = _load_config()
        src_cfg = cfg.get("source", DEFAULT_CONFIG["source"])
        src_conn = MySQL40Connection(
            host=src_cfg["host"], port=int(src_cfg["port"]),
            user=src_cfg["user"], password=src_cfg["password"],
            database=db, timeout=30, charset="tis620",
        )
        rows = src_conn.query("SHOW TABLES")
        src_conn.close()
        return jsonify({"tables": [r[0] for r in rows]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/all-tables/stop", methods=["POST"])
def at_stop():
    """Terminate the running validation/sync process and wait until it is dead."""
    global _process
    stopped = False
    with _process_lock:
        if _process is not None and _process.poll() is None:
            _process.terminate()
            stopped = True
    if stopped:
        # Block until reader_thread has finished and _snapshot has been updated.
        # This ensures that a /run call immediately after /stop won't hit 409.
        _run_done.wait(timeout=15)
    return jsonify({"stopped": stopped})


@app.route("/api/all-tables/stream")
def at_stream():
    def generate():
        pos = 0
        while True:
            with _buffer_lock:
                snapshot_len = len(_output_buffer)
            if pos < snapshot_len:
                with _buffer_lock:
                    chunk = _output_buffer[pos:snapshot_len]
                for line in chunk:
                    yield f"data: {line}\n\n"
                pos = snapshot_len
                if chunk and '"type": "done"' in chunk[-1]:
                    return
            else:
                if _run_done.is_set() and pos >= snapshot_len:
                    return
                time.sleep(0.15)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ─── Configuration & Database Discovery Endpoints ──────────────────────────────
try:
    import MySQLdb
    import MySQLdb.cursors
except ModuleNotFoundError:
    import pymysql
    pymysql.install_as_MySQLdb()
    import MySQLdb          # noqa: F811  (now resolves via pymysql shim)
    import MySQLdb.cursors  # noqa: F811

def _connect_target(tgt_cfg, db):
    if int(tgt_cfg.get("version", 8)) == 4:
        from mysql40 import MySQL40Connection
        return MySQL40Connection(
            host=tgt_cfg["host"],
            port=int(tgt_cfg["port"]),
            user=tgt_cfg["user"],
            password=tgt_cfg["password"],
            database=db,
            timeout=30,
            charset="tis620",
        )

    from MySQLdb.constants import FIELD_TYPE
    from MySQLdb.converters import conversions
    my_conv = conversions.copy()
    str_decoder = lambda val: val.decode("utf-8") if isinstance(val, bytes) else str(val)
    my_conv[FIELD_TYPE.DATE] = str_decoder
    my_conv[FIELD_TYPE.DATETIME] = str_decoder
    my_conv[FIELD_TYPE.TIMESTAMP] = str_decoder
    return MySQLdb.connect(
        host=tgt_cfg["host"],
        port=int(tgt_cfg["port"]),
        user=tgt_cfg["user"],
        passwd=tgt_cfg["password"],
        db=db,
        charset="utf8mb4",
        conv=my_conv
    )


def _get_tgt_pk_cols(tgt_conn, tgt_version, db, table):
    if tgt_version == 4:
        tc = tgt_conn.cursor()
        tc.execute(f"SHOW INDEX FROM `{table}`")
        rows = tc.fetchall()
        pk = [(int(r[3]), r[4]) for r in rows if r[2] == "PRIMARY"]
        tc.close()
        return [col for _, col in sorted(pk)]
    tc = tgt_conn.cursor()
    tc.execute("""
        SELECT column_name FROM information_schema.key_column_usage
        WHERE table_schema = %s AND table_name = %s AND constraint_name = 'PRIMARY'
        ORDER BY ordinal_position
    """, (db, table))
    pk_cols = [r[0] for r in tc.fetchall()]
    tc.close()
    return pk_cols


def _get_tgt_all_cols(tgt_conn, tgt_version, db, table):
    if tgt_version == 4:
        tc = tgt_conn.cursor()
        tc.execute(f"SHOW COLUMNS FROM `{table}`")
        rows = tc.fetchall()
        tc.close()
        return [r[0] for r in rows]
    tc = tgt_conn.cursor()
    tc.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (db, table))
    all_cols = [r[0] for r in tc.fetchall()]
    tc.close()
    return all_cols



CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")

DEFAULT_CONFIG = {
    "source": {"host": "", "port": 3306, "user": "", "password": ""},
    "target": {"host": "", "port": 3306, "user": "", "password": ""},
    "default_db": "prs",
}


def _load_config():
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return DEFAULT_CONFIG.copy()


def _get_target_config(cfg):
    t = cfg.get("target")
    if not t:
        _tgts = cfg.get("targets", [])
        if isinstance(_tgts, list) and len(_tgts) > 0:
            t = _tgts[0]
    return t or DEFAULT_CONFIG["target"]



def _save_config(cfg):
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2, ensure_ascii=False)
        return True
    except Exception:
        return False


@app.route("/api/config", methods=["GET"])
def get_config():
    cfg = _load_config()
    return jsonify(cfg)


@app.route("/api/config", methods=["POST"])
def post_config():
    data = request.json or {}
    source = data.get("source", {})
    target = data.get("target", {})
    
    # Validation
    for k in ["host", "port", "user", "password"]:
        if k not in source or k not in target:
            return jsonify({"error": f"Missing field '{k}' in configuration"}), 400
    
    # Test target connection
    try:
        if int(target.get("version", 8)) == 4:
            from mysql40 import MySQL40Connection
            t_conn = MySQL40Connection(
                host=target["host"],
                port=int(target["port"]),
                user=target["user"],
                password=target["password"],
                database=data.get("default_db", "prs"),
                timeout=10
            )
            t_conn.close()
        else:
            t_conn = MySQLdb.connect(
                host=target["host"],
                port=int(target["port"]),
                user=target["user"],
                passwd=target["password"],
                connect_timeout=10
            )
            t_conn.close()
    except Exception as e:
        import socket as _sock
        if isinstance(e, _sock.timeout):
            return jsonify({"error": f"Target DB Connection Error: connection timed out ({target.get('host')}:{target.get('port')})"}), 400
        return jsonify({"error": f"Target DB Connection Error: {str(e)}"}), 400

    # Test source connection
    try:
        from mysql40 import MySQL40Connection
        s_conn = MySQL40Connection(
            host=source["host"],
            port=int(source["port"]),
            user=source["user"],
            password=source["password"],
            database=data.get("default_db", "prs"),
            timeout=10
        )
        s_conn.close()
    except Exception as e:
        import socket as _sock
        if isinstance(e, _sock.timeout):
            return jsonify({"error": f"Source DB Connection Error: connection timed out ({source.get('host')}:{source.get('port')})"}), 400
        return jsonify({"error": f"Source DB Connection Error: {str(e)}"}), 400

    # Save configuration — merge over existing file so extra sections (tuning) survive
    cfg = _load_config()
    cfg.update({
        "source": {
            "host": source["host"],
            "port": int(source["port"]),
            "user": source["user"],
            "password": source["password"],
            "version": int(source.get("version", 4))
        },
        "target": {
            "host": target["host"],
            "port": int(target["port"]),
            "user": target["user"],
            "password": target["password"],
            "version": int(target.get("version", 8))
        },
        "default_db": data.get("default_db", "prs")
    })


    # Optional numeric-comparison tuning from the Settings UI
    if "tuning" in data and isinstance(data["tuning"], dict):
        tuning = cfg.setdefault("tuning", {})
        t_in = data["tuning"]
        if "decimal_round" in t_in:
            dr = t_in["decimal_round"]
            if dr is None or str(dr).strip() == "":
                tuning.pop("decimal_round", None)   # blank = off, fall back to tolerance
            else:
                try:
                    tuning["decimal_round"] = max(0, int(dr))
                except (TypeError, ValueError):
                    return jsonify({"error": "decimal_round must be an integer"}), 400
        if "decimal_tol" in t_in and str(t_in["decimal_tol"]).strip() != "":
            try:
                Decimal(str(t_in["decimal_tol"]))
            except InvalidOperation:
                return jsonify({"error": "decimal_tol must be a number"}), 400
            tuning["decimal_tol"] = str(t_in["decimal_tol"])

    if _save_config(cfg):
        return jsonify({"success": True})
    else:
        return jsonify({"error": "Failed to write configuration file"}), 500


@app.route("/api/config/ignore-fields", methods=["POST"])
def post_ignore_fields():
    data = request.json or {}
    db = data.get("db")
    table = data.get("table")
    column = data.get("column")
    ignore = data.get("ignore", False)

    if not db or not table or not column:
        return jsonify({"error": "Missing db, table, or column parameter"}), 400

    cfg = _load_config()
    ignore_fields = cfg.setdefault("ignore_fields", {})
    db_ignores = ignore_fields.setdefault(db, {})
    table_ignores = db_ignores.setdefault(table, [])

    if ignore:
        if column not in table_ignores:
            table_ignores.append(column)
    else:
        if column in table_ignores:
            table_ignores.remove(column)

    if not table_ignores:
        db_ignores.pop(table, None)
    if not db_ignores:
        ignore_fields.pop(db, None)

    if _save_config(cfg):
        return jsonify({"success": True, "ignore_fields": cfg.get("ignore_fields", {})})
    else:
        return jsonify({"error": "Failed to write configuration file"}), 500


@app.route("/api/databases", methods=["GET"])
def get_databases():
    cfg = _load_config()
    target = _get_target_config(cfg)
    
    try:
        t_conn = _connect_target(target, cfg.get("default_db", "prs"))
        cursor = t_conn.cursor()
        cursor.execute("SHOW DATABASES")
        dbs = [r[0] for r in cursor.fetchall()]
        cursor.close()
        t_conn.close()
        
        # Filter out system databases
        sys_dbs = {"information_schema", "performance_schema", "mysql", "sys"}
        user_dbs = [db for db in dbs if db.lower() not in sys_dbs]
        
        return jsonify({"databases": user_dbs, "default": cfg.get("default_db", "prs")})
    except Exception as e:
        fallback_db = cfg.get("default_db", "prs")
        return jsonify({
            "databases": [fallback_db, "prs"],
            "default": fallback_db,
            "warning": f"Unable to fetch databases: {str(e)}"
        })





# ─── Random Field-by-Field Comparison ──────────────────────────────────────────
import random
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP


def _numeric_cmp_params(cfg):
    """(tolerance, round_places) from config tuning — same semantics as
    validate_all_tables: decimal_round set → round-and-compare wins,
    otherwise abs-diff tolerance. Read per request so Settings saves apply
    without a server restart."""
    tuning = cfg.get("tuning") or {}
    tol = Decimal(str(tuning.get("decimal_tol", "0.0001")))
    dr = tuning.get("decimal_round")
    rnd = int(dr) if dr is not None and str(dr).strip() != "" else None
    return tol, rnd


def _norm_val_str(v) -> str:
    """Normalize date/datetime to consistent string for cross-DB comparison."""
    if isinstance(v, _dt.datetime):
        return v.strftime("%Y-%m-%d") if (v.hour == v.minute == v.second == v.microsecond == 0) else v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, _dt.date):
        return v.strftime("%Y-%m-%d")
    return str(v)


def _close_vals(a, b, tol=Decimal("0.0001"), rnd=None):
    """Check if two values are 'close enough' (handles decimals, nulls, strings)."""
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    sa, sb = _norm_val_str(a).strip(), _norm_val_str(b).strip()
    if sa == sb:
        return True
    try:
        da, db = Decimal(sa), Decimal(sb)
        if rnd is not None:
            q = Decimal(1).scaleb(-rnd)   # 10^-N
            return da.quantize(q, rounding=ROUND_HALF_UP) == db.quantize(q, rounding=ROUND_HALF_UP)
        return abs(da - db) <= tol
    except (InvalidOperation, ValueError):
        return False


@app.route("/api/random-compare", methods=["POST"])
def random_compare():
    """
    Randomly sample ~1000 rows from a table, compare field-by-field between
    source (MySQL 4) and target (MySQL 8).
    Accepts: { table, db, seed: int|null, sample_offset: int }
    Returns: { seed, sample_offset, table, total_compared, matches, mismatches, pk_cols, columns, rows: [...] }
    """
    data = request.json or {}
    table = data.get("table")
    db = data.get("db", "prs")
    sample_size = min(int(data.get("sample_size", 1000)), 5000)
    # seed: fixed per table session (frontend sends same seed every time)
    # sample_offset: advances by sample_size each call to guarantee no repeated rows
    seed = data.get("seed") or random.randint(1, 999999)
    # int for single-numeric-PK range mode, list of PK values (continuation
    # anchor) for the general key-anchor mode, 0/None for a fresh start
    sample_offset = data.get("sample_offset", 0) or 0

    if not table:
        return jsonify({"error": "Missing 'table' parameter"}), 400

    src_conn = None
    tgt_conn = None
    try:
        from mysql40 import MySQL40Connection

        cfg = _load_config()
        src_cfg = cfg.get("source", DEFAULT_CONFIG["source"])
        tgt_cfg = _get_target_config(cfg)

        src_conn = MySQL40Connection(
            host=src_cfg["host"],
            port=int(src_cfg["port"]),
            user=src_cfg["user"],
            password=src_cfg["password"],
            database=db,
            timeout=120,
            charset="tis620",
        )
        tgt_conn = _connect_target(tgt_cfg, db)

        tgt_version = int(tgt_cfg.get("version", 8))
        pk_cols = _get_tgt_pk_cols(tgt_conn, tgt_version, db, table)

        if not pk_cols:
            return jsonify({"error": f"Table '{table}' has no primary key"}), 400

        all_cols = _get_tgt_all_cols(tgt_conn, tgt_version, db, table)


        # Also get source column names for intersection
        src_raw_cols = src_conn.query(f"SHOW COLUMNS FROM `{table}`")
        src_col_names = [r[0] for r in src_raw_cols]

        # Data window: same rule as validate_all_tables.py. Request year_from/
        # year_to (inclusive years) override the config default of the last
        # data_window_years calendar years. Uses first date-typed PK column.
        win_years = int((cfg.get("tuning") or {}).get("data_window_years", 0))
        win_start, win_end = _get_data_window(
            data.get("year_from"), data.get("year_to"),
            data.get("month_from"), data.get("month_to"),
            win_years,
            data.get("day_from"), data.get("day_to"),
        )

        win_cond = ""
        cutoff = None
        if win_start or win_end:
            src_col_types = {r[0]: str(r[1]).lower() for r in src_raw_cols}
            for c in pk_cols:
                t = src_col_types.get(c, "")
                if "timestamp" in t or "datetime" in t or t.startswith("date"):
                    parts = []
                    if win_start:
                        parts.append(f"`{c}` >= '{win_start}'")
                    if win_end:
                        parts.append(f"`{c}` < '{win_end}'")
                    win_cond = " AND ".join(parts)
                    cutoff = _get_window_label(win_start, win_end)
                    break

        # Use intersection of columns (in target order)
        common_cols = [c for c in all_cols if c in set(src_col_names)]

        # Exclude ignored columns, preserving primary keys
        ignored = cfg.get("ignore_fields", {}).get(db, {}).get(table, [])
        common_cols = [c for c in common_cols if c not in ignored or c in pk_cols]

        if not common_cols:
            return jsonify({"error": "No common columns found between source and target"}), 400

        col_list = ", ".join(f"`{c}`" for c in common_cols)
        pk_concat = "CONCAT_WS('|', " + ", ".join(f"IFNULL(`{c}`,'')" for c in pk_cols) + ")"

        # ── Sampling strategy ──────────────────────────────────────────────────
        # ORDER BY RAND() on MySQL 4 sorts every row in memory — O(n), very slow
        # for large tables.  Use index-based range sampling instead:
        #   • single numeric PK → WHERE pk >= X ORDER BY pk LIMIT N  (O(log n))
        #   • otherwise         → LIMIT N OFFSET random_start         (O(offset))
        # sample_offset encodes continuation state returned by previous call.

        _NUMERIC_TYPES = ('int', 'bigint', 'smallint', 'mediumint', 'tinyint')
        use_range = False
        range_pk = None
        if len(pk_cols) == 1:
            _ci = src_conn.query(f"SHOW COLUMNS FROM `{table}` LIKE '{pk_cols[0]}'")
            if _ci and any(t in _ci[0][1].lower() for t in _NUMERIC_TYPES):
                use_range = True
                range_pk = pk_cols[0]

        _win_where = f" WHERE {win_cond}" if win_cond else ""
        _win_and   = f" AND {win_cond}" if win_cond else ""

        _tol, _rnd = _numeric_cmp_params(cfg)

        if use_range:
            if not sample_offset:
                _mm = src_conn.query(f"SELECT MIN(`{range_pk}`), MAX(`{range_pk}`) FROM `{table}`{_win_where}")
                _min = int(_mm[0][0]) if _mm and _mm[0][0] is not None else 0
                _max = int(_mm[0][1]) if _mm and _mm[0][1] is not None else 0
                _span = max(0, _max - _min - sample_size)
                start_pk = _min + int((seed % 999983) / 999983.0 * _span)
            else:
                start_pk = int(sample_offset)   # continuation: last judged pk + 1
            src_sql = (
                f"SELECT {col_list} FROM `{table}` "
                f"WHERE `{range_pk}` >= {start_pk}{_win_and} "
                f"ORDER BY `{range_pk}` LIMIT {sample_size}"
            )
        else:
            # Key-anchor mode: pick a random anchor ROW from source, then read
            # the same PK position onward on BOTH sides. OFFSET on each side
            # separately would desync the windows the moment row counts differ.
            _order_by = ", ".join(f"`{c}`" for c in pk_cols)

            def _esc(v):
                return str(v).replace("\\", "\\\\").replace("'", "''")

            def _pk_after(vals, inclusive):
                """Lexicographic (pk1,pk2,..) > vals (>= when inclusive) — expanded
                OR-AND form, works on MySQL 4 (no row-constructor comparison)."""
                ors = []
                for i in range(len(pk_cols)):
                    ands = [f"`{pk_cols[j]}` = '{_esc(vals[j])}'" for j in range(i)]
                    op = ">=" if (inclusive and i == len(pk_cols) - 1) else ">"
                    ands.append(f"`{pk_cols[i]}` {op} '{_esc(vals[i])}'")
                    ors.append("(" + " AND ".join(ands) + ")")
                return "(" + " OR ".join(ors) + ")"

            if not sample_offset:
                _cnt = src_conn.query(f"SELECT COUNT(*) FROM `{table}`{_win_where}")
                _total = int(_cnt[0][0]) if _cnt else 0
                _max_off = max(0, _total - sample_size)
                _off = int((seed % 999983) / 999983.0 * _max_off) if _max_off > 0 else 0
                _pk_list = ", ".join(f"`{c}`" for c in pk_cols)
                _anchor = src_conn.query(
                    f"SELECT {_pk_list} FROM `{table}`{_win_where} "
                    f"ORDER BY {_order_by} LIMIT 1 OFFSET {_off}")
                anchor_vals = ["" if v is None else str(v) for v in _anchor[0]] if _anchor else None
                _inclusive = True
            else:
                anchor_vals = [str(v) for v in sample_offset]   # last judged key
                _inclusive = False

            pk_pred = _pk_after(anchor_vals, _inclusive) if anchor_vals else "1=0"
            src_sql = (f"SELECT {col_list} FROM `{table}` WHERE {pk_pred}{_win_and} "
                       f"ORDER BY {_order_by} LIMIT {sample_size}")

        src_rows_raw = src_conn.query(src_sql)

        # Build a dict of source rows keyed by PK. Numeric components are
        # normalized so '4702745699' (MySQL 4 string) and 4702745699.0
        # (MySQL 8 float) key identically — float/double PKs would otherwise
        # report every row as missing+extra.
        from decimal import Decimal as _Dec, InvalidOperation as _DecErr
        pk_indices = [common_cols.index(c) for c in pk_cols]
        def _pk_norm(v):
            if v is None:
                return (0, "")
            s = str(v)
            try:
                return (1, _Dec(s))
            except _DecErr:
                return (2, s)
        def pk_of(row):
            return tuple(_pk_norm(row[i]) for i in pk_indices)
        def _pk_raw(k, j):
            """Raw value of key component j (for SQL params / display)."""
            return str(k[j][1])

        # Python-side anchor guard: the SQL predicate compares float PKs against
        # decimal literals, so a boundary row (stored 1593.0700683 vs anchor
        # '1593.07') can slip back into the next batch. Re-check the anchor
        # against exact normalized keys to guarantee no repeated rows.
        _anchor_key = None
        if not use_range and anchor_vals is not None:
            _anchor_key = tuple(_pk_norm(v) for v in anchor_vals)
        def _past_anchor(key):
            if _anchor_key is None:
                return True
            return key >= _anchor_key if _inclusive else key > _anchor_key

        src_by_pk = {}
        for row in src_rows_raw:
            pk = pk_of(row)
            if _past_anchor(pk):
                src_by_pk[pk] = row

        # Query target for matching PKs
        result_rows = []
        total_matches = 0
        total_mismatches = 0
        total_missing = 0
        total_extra = 0

        # ── Fetch the SAME ordered slice from target, merge in Python ────────
        # Full-sync style: no SQL lookup by PK value (WHERE pk = <literal>
        # can't match float/double PKs reliably — float precision). Both sides
        # run the identical windowed/ordered query, then rows pair up by
        # normalized key exactly like _stream_compare in validate_all_tables.
        if use_range:
            tgt_sql = (
                f"SELECT {col_list} FROM `{table}` "
                f"WHERE `{range_pk}` >= {start_pk}{_win_and} "
                f"ORDER BY `{range_pk}` LIMIT {sample_size}"
            )
        else:
            tgt_sql = (f"SELECT {col_list} FROM `{table}` WHERE {pk_pred}{_win_and} "
                       f"ORDER BY {_order_by} LIMIT {sample_size}")
        tc = tgt_conn.cursor()
        tc.execute(tgt_sql)
        tgt_rows_raw = tc.fetchall()
        tc.close()

        tgt_by_pk = {}
        for row in tgt_rows_raw:
            pk = pk_of(row)
            if _past_anchor(pk):
                tgt_by_pk[pk] = row

        # Judge missing/extra only inside the key range BOTH windows fully
        # cover. A row absent on one side shifts that side's LIMIT window, so
        # keys outside the overlap may exist just beyond the fetched slice —
        # counting them would be a false positive.
        _INF_KEY = tuple([(3, "")] * len(pk_cols))   # above any (rank, value)
        def _hi(rows_raw, by_pk):
            # window exhausted the table → covers everything upward
            return _INF_KEY if len(rows_raw) < sample_size or not by_pk else max(by_pk)
        hi_bound = min(_hi(src_rows_raw, src_by_pk), _hi(tgt_rows_raw, tgt_by_pk))
        if src_by_pk and tgt_by_pk:
            lo_bound = max(min(src_by_pk), min(tgt_by_pk))
        elif src_by_pk:
            # target empty = it has nothing ≥ anchor at all → every source row
            # in the window is genuinely missing
            lo_bound = min(src_by_pk)
        elif tgt_by_pk:
            lo_bound = min(tgt_by_pk)
        else:
            lo_bound = hi_bound  # both empty

        for key in sorted(set(src_by_pk) | set(tgt_by_pk)):
            if not (lo_bound <= key <= hi_bound):
                continue   # outside the overlap — re-fetched and judged next batch
            src_row = src_by_pk.get(key)
            tgt_row = tgt_by_pk.get(key)
            pk_dict = {pk_cols[i]: _pk_raw(key, i) for i in range(len(pk_cols))}

            if src_row is not None and tgt_row is not None:
                # Both exist — compare each field
                fields = []
                row_has_diff = False
                for ci, col in enumerate(common_cols):
                    sv = src_row[ci]
                    tv = tgt_row[ci]
                    is_match = _close_vals(sv, tv, _tol, _rnd)
                    if not is_match:
                        row_has_diff = True
                    fields.append({
                        "column": col,
                        "source": str(sv) if sv is not None else None,
                        "target": str(tv) if tv is not None else None,
                        "match": is_match,
                    })
                if row_has_diff:
                    total_mismatches += 1
                else:
                    total_matches += 1
                result_rows.append({
                    "pk": pk_dict,
                    "status": "mismatch" if row_has_diff else "match",
                    "fields": fields,
                })
                continue

            if src_row is not None:
                # Missing in target
                fields = [{
                    "column": col,
                    "source": str(src_row[ci]) if src_row[ci] is not None else None,
                    "target": None,
                    "match": False,
                } for ci, col in enumerate(common_cols)]
                result_rows.append({"pk": pk_dict, "status": "missing", "fields": fields})
                total_missing += 1
            else:
                # Extra in target
                fields = [{
                    "column": col,
                    "source": None,
                    "target": str(tgt_row[ci]) if tgt_row[ci] is not None else None,
                    "match": False,
                } for ci, col in enumerate(common_cols)]
                result_rows.append({"pk": pk_dict, "status": "extra", "fields": fields})
                total_extra += 1

        # next_offset: continuation = last JUDGED key (hi_bound). Keys beyond it
        # were skipped this batch and will be re-fetched next call — no gaps,
        # no repeats.
        _all_keys = sorted(set(src_by_pk) | set(tgt_by_pk))
        _judged_hi = hi_bound if hi_bound != _INF_KEY else (_all_keys[-1] if _all_keys else None)
        if use_range:
            next_offset = (int(float(_pk_raw(_judged_hi, 0))) + 1) if _judged_hi else (start_pk if isinstance(start_pk, int) else 0)
        elif _judged_hi is not None:
            next_offset = [_pk_raw(_judged_hi, j) for j in range(len(pk_cols))]
        else:
            next_offset = sample_offset or 0

        return jsonify({
            "seed": seed,
            "sample_offset": sample_offset,
            "next_offset": next_offset,
            "table": table,
            "db": db,
            "total_compared": len(result_rows),
            "matches": total_matches,
            "mismatches": total_mismatches,
            "missing": total_missing,
            "extra": total_extra,
            "pk_cols": pk_cols,
            "columns": common_cols,
            "rows": result_rows,
            "window": cutoff if win_cond else None,
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# ─── Table Timestamp Info ─────────────────────────────────────────────────────
@app.route("/api/table-timestamp-info", methods=["POST"])
def table_timestamp_info():
    """
    For a given table:
    - If any PK column is TIMESTAMP/DATETIME: return MIN/MAX of that column from source & target
    - Otherwise: return COUNT(*) from source & target and whether they match
    Accepts: { table, db }
    Returns: { has_ts_pk, ts_col, source_min, source_max, target_min, target_max,
               source_count, target_count, counts_match }
    """
    data = request.json or {}
    table = data.get("table")
    db = data.get("db", "prs")
    pk_cols_hint = data.get("pk_cols", [])   # pre-known PK cols from validation run
    year_from = data.get("year_from")
    year_to = data.get("year_to")

    if not table:
        return jsonify({"error": "Missing 'table' parameter"}), 400

    cfg = _load_config()

    # Year window: explicit request range, else config default (last N years)
    win_years = int((cfg.get("tuning") or {}).get("data_window_years", 0))
    win_start, win_end = _get_data_window(
        data.get("year_from"), data.get("year_to"),
        data.get("month_from"), data.get("month_to"),
        win_years,
        data.get("day_from"), data.get("day_to"),
    )

    def _win_where(col):
        parts = []
        if win_start: parts.append(f"`{col}` >= '{win_start}'")
        if win_end:   parts.append(f"`{col}` < '{win_end}'")
        return f" WHERE {' AND '.join(parts)}" if parts else ""
    src_cfg = cfg.get("source", DEFAULT_CONFIG["source"])
    tgt_cfg = _get_target_config(cfg)

    src_conn = None
    tgt_conn = None
    try:
        from mysql40 import MySQL40Connection

        src_conn = MySQL40Connection(
            host=src_cfg["host"],
            port=int(src_cfg["port"]),
            user=src_cfg["user"],
            password=src_cfg["password"],
            database=db,
            timeout=60,
            charset="tis620",
        )
        tgt_conn = _connect_target(tgt_cfg, db)

        # Look up the designated MIN/MAX column from ts_field_config.json.
        # This config is generated from the Numbers condition file where each
        # table's primary date/time field is manually curated (orange = skip).
        # Tables not in the config fall back to row-count comparison.
        _TS_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ts_field_config.json")
        try:
            with open(_TS_CONFIG_PATH, "r", encoding="utf-8") as _f:
                _ts_config = json.load(_f)
        except Exception:
            _ts_config = {}

        ts_col = _ts_config.get(db, {}).get(table)

        result = {"has_ts_pk": ts_col is not None, "ts_col": ts_col}

        # window label only when a date-typed PK column exists to filter on —
        # the COUNT(*) fallback path has no such column and counts the full table
        result["window"] = (_get_window_label(win_start, win_end)
                            if (ts_col and (win_start or win_end)) else None)

        if ts_col:
            # MIN/MAX via ORDER BY + LIMIT 1 instead of MIN()/MAX() with a
            # range WHERE — aggregate over a windowed range scans every index
            # entry in the window (30M+ rows on big tables), while an ordered
            # LIMIT 1 is a single index seek per end when ts_col is indexed.
            # Without an index it degrades to the same scan as before.
            def _edge_sql(direction):
                return (f"SELECT `{ts_col}` FROM `{table}`{_win_where(ts_col)} "
                        f"ORDER BY `{ts_col}` {direction} LIMIT 1")

            def _one(rows):
                return str(rows[0][0]) if rows and rows[0][0] is not None else None

            src_min = _one(src_conn.query(_edge_sql("ASC")))
            src_max = _one(src_conn.query(_edge_sql("DESC")))

            tc = tgt_conn.cursor()
            tc.execute(_edge_sql("ASC"))
            r_min = tc.fetchone()
            tc.execute(_edge_sql("DESC"))
            r_max = tc.fetchone()
            tc.close()
            tgt_min = str(r_min[0]) if r_min and r_min[0] is not None else None
            tgt_max = str(r_max[0]) if r_max and r_max[0] is not None else None

            # If both sides have no data at all → fall back to row count
            if src_min is None and src_max is None and tgt_min is None and tgt_max is None:
                result["has_ts_pk"] = False
                result["ts_col"] = None
                result["window"] = None
                src_cnt_rows = src_conn.query(f"SELECT COUNT(*) FROM `{table}`")
                src_count = int(src_cnt_rows[0][0]) if src_cnt_rows else 0
                tc2 = tgt_conn.cursor()
                tc2.execute(f"SELECT COUNT(*) FROM `{table}`")
                tgt_count = int(tc2.fetchone()[0])
                tc2.close()
                result.update({
                    "source_count": src_count,
                    "target_count": tgt_count,
                    "counts_match": src_count == tgt_count,
                })
            else:
                result.update({
                    "source_min": src_min, "source_max": src_max,
                    "target_min": tgt_min, "target_max": tgt_max,
                })
        else:
            # Query COUNT(*) from both
            src_cnt_rows = src_conn.query(f"SELECT COUNT(*) FROM `{table}`")
            src_count = int(src_cnt_rows[0][0]) if src_cnt_rows else 0

            tc = tgt_conn.cursor()
            tc.execute(f"SELECT COUNT(*) FROM `{table}`")
            tgt_count = int(tc.fetchone()[0])
            tc.close()

            result.update({
                "source_count": src_count,
                "target_count": tgt_count,
                "counts_match": src_count == tgt_count,
            })

        return jsonify(result)

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        if src_conn:
            try: src_conn.close()
            except Exception: pass
        if tgt_conn:
            try: tgt_conn.close()
            except Exception: pass


# ─── Custom SQL Query Runner ───────────────────────────────────────────────────
@app.route("/api/custom-query", methods=["POST"])
def custom_query():
    """
    Run a read-only SQL query against source, target, or both databases.
    Accepts: { sql, db, target: "source"|"target"|"both", limit }
    Returns: { source?: { columns, rows, count }, target?: { columns, rows, count },
               source_error?: str, target_error?: str }
    """
    data      = request.json or {}
    sql       = (data.get("sql") or "").strip()
    db        = data.get("db", "prs")
    target    = data.get("target", "both")
    row_limit = min(int(data.get("limit", 500)), 2000)

    if not sql:
        return jsonify({"error": "Missing 'sql' parameter"}), 400

    sql_upper = sql.upper().lstrip()
    allowed   = ("SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN")
    if not any(sql_upper.startswith(k) for k in allowed):
        return jsonify({"error": "Only SELECT, SHOW, DESCRIBE, and EXPLAIN queries are allowed"}), 400

    cfg     = _load_config()
    src_cfg = cfg.get("source", DEFAULT_CONFIG["source"])
    tgt_cfg = _get_target_config(cfg)

    result = {}

    if target in ("source", "both"):
        try:
            from mysql40 import MySQL40Connection
            src_conn = MySQL40Connection(
                host=src_cfg["host"], port=int(src_cfg["port"]),
                user=src_cfg["user"], password=src_cfg["password"],
                database=db, timeout=30, charset="tis620",
            )
            exec_sql = sql
            if sql_upper.startswith("SELECT") and "LIMIT" not in sql_upper:
                exec_sql = f"{sql} LIMIT {row_limit}"
            columns, rows_raw = src_conn.query_with_cols(exec_sql)
            src_conn.close()
            result["source"] = {
                "columns": columns,
                "rows": [[str(v) if v is not None else None for v in row] for row in rows_raw],
                "count": len(rows_raw),
            }
        except Exception as e:
            result["source_error"] = str(e)

    if target in ("target", "both"):
        try:
            tgt_conn = _connect_target(tgt_cfg, db)
            exec_sql = sql
            if sql_upper.startswith("SELECT") and "LIMIT" not in sql_upper:
                exec_sql = f"{sql} LIMIT {row_limit}"
            if int(tgt_cfg.get("version", 8)) == 4:
                columns, rows_raw = tgt_conn.query_with_cols(exec_sql)
            else:
                tc = tgt_conn.cursor()
                tc.execute(exec_sql)
                columns  = [d[0] for d in tc.description] if tc.description else []
                rows_raw = tc.fetchall()
                tc.close()
            tgt_conn.close()
            result["target"] = {
                "columns": columns,
                "rows": [[str(v) if v is not None else None for v in row] for row in rows_raw],
                "count": len(rows_raw),
            }
        except Exception as e:
            result["target_error"] = str(e)

    return jsonify(result)


def _read_compare_meta(table, reports_dir):
    """Read sidecar meta JSON for fast type_counts. Returns None if missing."""
    meta_path = os.path.join(reports_dir, f"{table}_compare_meta.json")
    if os.path.exists(meta_path):
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return None


def _stream_discrepancy_page(csv_path, filter_type, start, page_size):
    """Stream CSV, group on-the-fly, return one page. O(start+page_size) memory."""
    import csv as csv_mod

    def make_pk(pk_str):
        pk_dict = {}
        for part in pk_str.split(", "):
            kv = part.split("=", 1)
            if len(kv) == 2:
                pk_dict[kv[0]] = kv[1]
            elif part:
                pk_dict["PK"] = part
        return pk_dict

    groups = []
    skipped = 0
    cur_key = cur_type = cur_pk = None
    cur_fields = []

    def flush():
        nonlocal skipped
        if cur_key is None:
            return
        if filter_type != "all" and cur_type != filter_type:
            return
        if skipped < start:
            skipped += 1
        elif len(groups) < page_size:
            groups.append({"type": cur_type, "status": cur_type, "pk": cur_pk, "fields": cur_fields[:]})

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv_mod.reader(f)
        next(reader, None)
        for row in reader:
            if len(row) < 5:
                continue
            rtype, pk_str, col, src_val, tgt_val = row[0], row[1], row[2], row[3], row[4]
            key = f"{rtype}|{pk_str}"
            if key != cur_key:
                flush()
                if len(groups) >= page_size:
                    break
                cur_key, cur_type, cur_pk, cur_fields = key, rtype, make_pk(pk_str), []
            if col and col != "ALL":
                cur_fields.append({
                    "column": col,
                    "source": src_val if src_val != "NULL" else None,
                    "target": tgt_val if tgt_val != "NULL" else None,
                    "match": False,
                })
        else:
            flush()

    return groups


@app.route("/api/reports/discrepancies/paged", methods=["POST"])
def get_discrepancies_paged():
    """
    Streaming paginated discrepancies. No full in-memory load.
    Type counts from sidecar meta file; fallback to fast count-only scan.
    """
    data = request.json or {}
    table = data.get("table")
    page = int(data.get("page", 0))
    page_size = max(1, int(data.get("page_size", 50)))
    filter_type = data.get("filter_type", "all").lower()

    if not table:
        return jsonify({"error": "Missing 'table'"}), 400

    reports_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "reports")
    csv_path = os.path.join(reports_dir, f"{table}_mismatches.csv")
    if not os.path.exists(csv_path):
        return jsonify({"table": table, "total": 0, "total_pages": 0, "page": 0,
                        "type_counts": {"all": 0, "mismatch": 0, "missing": 0, "extra": 0},
                        "samples": []})

    try:
        meta = _read_compare_meta(table, reports_dir)
        if meta:
            disc_counts = {
                "all":      meta.get("mismatch", 0) + meta.get("missing", 0) + meta.get("extra", 0),
                "mismatch": meta.get("mismatch", 0),
                "missing":  meta.get("missing", 0),
                "extra":    meta.get("extra", 0),
            }
        else:
            import csv as csv_mod
            counts = {}
            last_key = None
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv_mod.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) < 5:
                        continue
                    key = f"{row[0]}|{row[1]}"
                    if key != last_key:
                        counts[row[0]] = counts.get(row[0], 0) + 1
                        last_key = key
            disc_counts = {
                "all": sum(counts.values()),
                "mismatch": counts.get("mismatch", 0),
                "missing":  counts.get("missing", 0),
                "extra":    counts.get("extra", 0),
            }

        total = disc_counts.get(filter_type, 0) if filter_type != "all" else disc_counts["all"]
        total_pages = max(1, math.ceil(total / page_size))
        page = min(max(page, 0), total_pages - 1)
        start = page * page_size

        samples = _stream_discrepancy_page(csv_path, filter_type, start, page_size)

        return jsonify({
            "table": table, "filter_type": filter_type,
            "total": total, "total_pages": total_pages,
            "page": page, "page_size": page_size,
            "type_counts": disc_counts, "samples": samples,
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


_full_compare_cache = {}  # table -> {type_counts, mtime}


@app.route("/api/reports/full-compare/paged", methods=["POST"])
def get_full_compare_paged():
    """
    Server-side paginated full comparison (match + mismatch + missing + extra).
    Accepts: { table, page, page_size, filter_type }
    Returns: { total, total_pages, page, type_counts, samples: [...] }
    Reads from {table}_full_compare.ndjson written by _stream_compare.
    """
    data = request.json or {}
    table = data.get("table")
    page = int(data.get("page", 0))
    page_size = max(1, int(data.get("page_size", 50)))
    filter_type = data.get("filter_type", "all").lower()

    if not table:
        return jsonify({"error": "Missing 'table'"}), 400

    ndjson_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "reports", f"{table}_full_compare.ndjson")
    if not os.path.exists(ndjson_path):
        return jsonify({"table": table, "total": 0, "total_pages": 0, "page": 0,
                        "type_counts": {"all": 0, "match": 0, "mismatch": 0, "missing": 0, "extra": 0},
                        "samples": []})

    try:
        mtime = os.path.getmtime(ndjson_path)
        cache = _full_compare_cache.get(table)
        if not cache or cache["mtime"] != mtime:
            reports_dir_fc = os.path.dirname(ndjson_path)
            meta = _read_compare_meta(table, reports_dir_fc)
            if meta:
                tc = meta
            else:
                tc = {"all": 0, "match": 0, "mismatch": 0, "missing": 0, "extra": 0}
                with open(ndjson_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        idx = line.find('"type": "')
                        if idx == -1:
                            idx = line.find('"type":"')
                            if idx == -1:
                                continue
                            idx += 8
                        else:
                            idx += 9
                        end = line.find('"', idx)
                        rtype = line[idx:end]
                        tc["all"] += 1
                        tc[rtype] = tc.get(rtype, 0) + 1
            _full_compare_cache[table] = {"type_counts": tc, "mtime": mtime}
            cache = _full_compare_cache[table]

        type_counts = cache["type_counts"]
        _DISCREPANCY = {"mismatch", "missing", "extra"}
        if filter_type == "discrepancy":
            total = sum(type_counts.get(t, 0) for t in _DISCREPANCY)
        elif filter_type != "all":
            total = type_counts.get(filter_type, 0)
        else:
            total = type_counts["all"]
        total_pages = max(1, math.ceil(total / page_size))
        page = min(max(page, 0), total_pages - 1)
        start = page * page_size

        samples = []
        count = 0
        with open(ndjson_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                rtype = row.get("type")
                if filter_type == "discrepancy" and rtype not in _DISCREPANCY:
                    continue
                if filter_type not in ("all", "discrepancy") and rtype != filter_type:
                    continue
                count += 1
                if count <= start:
                    continue
                if len(samples) >= page_size:
                    break

                pk_dict = {}
                for part in (row.get("pk") or "").split(", "):
                    kv = part.split("=", 1)
                    if len(kv) == 2:
                        pk_dict[kv[0]] = kv[1]
                    elif part:
                        pk_dict["PK"] = part

                sample = {"type": row["type"], "pk": pk_dict, "fields": []}
                cols = row.get("cols", [])
                src_vals = row.get("src", [])
                tgt_vals = row.get("tgt", [])
                diff_flags = row.get("diffs", [])

                for i, col in enumerate(cols):
                    sv = src_vals[i] if i < len(src_vals) else None
                    tv = tgt_vals[i] if i < len(tgt_vals) else None
                    is_diff = diff_flags[i] if i < len(diff_flags) else (row["type"] != "match")
                    sample["fields"].append({
                        "column": col, "source": sv, "target": tv, "match": not is_diff,
                    })

                samples.append(sample)

        return jsonify({
            "table": table, "total": total, "total_pages": total_pages,
            "page": page, "page_size": page_size,
            "type_counts": type_counts, "samples": samples,
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/reports/download-excel/<table>")
def download_excel(table):
    """4-sheet formatted Excel: Summary / Mismatch / Missing / Extra."""
    import csv as csv_mod
    import io
    import xlsxwriter
    from flask import send_file

    reports_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "reports")
    csv_path = os.path.join(reports_dir, f"{table}_mismatches.csv")
    if not os.path.exists(csv_path):
        return jsonify({"error": "Report not found"}), 404

    meta = _read_compare_meta(table, reports_dir) or {}

    cfg = _load_config()
    src_cfg = cfg.get("source", DEFAULT_CONFIG["source"])
    tgt_cfg = cfg.get("target", DEFAULT_CONFIG["target"])

    src_host = src_cfg.get("host", "unknown")
    src_ver = src_cfg.get("version", 4)
    tgt_host = tgt_cfg.get("host", "unknown")
    tgt_ver = tgt_cfg.get("version", 8)

    src_label = f"Source ({src_host}, MySQL {src_ver})"
    tgt_label = f"Target ({tgt_host}, MySQL {tgt_ver})"

    # ── Pass 1: discover PK columns + collect all grouped data ───────────
    pk_cols = []
    mm_groups = []   # [{pk_dict, fields: [(col,src,tgt)]}]
    ms_groups = []   # [{pk_dict, fields: [(col,src,tgt)]}]
    ex_groups = []   # [{pk_dict, fields: [(col,src,tgt)]}]

    _cur = {
        "mismatch": {"key": None, "pk": None, "fields": [], "out": mm_groups},
        "missing":  {"key": None, "pk": None, "fields": [], "out": ms_groups},
        "extra":    {"key": None, "pk": None, "fields": [], "out": ex_groups},
    }

    def parse_pk(pk_str):
        d = {}
        for part in pk_str.split(", "):
            kv = part.split("=", 1)
            if len(kv) == 2:
                d[kv[0]] = kv[1]
                if kv[0] not in pk_cols:
                    pk_cols.append(kv[0])
        return d

    def flush(rtype):
        c = _cur[rtype]
        if c["key"] and c["fields"]:
            c["out"].append({"pk": c["pk"], "fields": c["fields"][:]})

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv_mod.reader(f)
        next(reader, None)
        for row in reader:
            if len(row) < 5:
                continue
            rtype, pk_str, col, src_val, tgt_val = row[0], row[1], row[2], row[3], row[4]
            pk_dict = parse_pk(pk_str)
            if rtype not in _cur:
                continue
            c = _cur[rtype]
            key = pk_str
            if key != c["key"]:
                flush(rtype)
                c["key"], c["pk"], c["fields"] = key, pk_dict, []
            if col != "ALL":
                c["fields"].append((col, src_val, tgt_val))
    flush("mismatch"); flush("missing"); flush("extra")

    # ── Build workbook ────────────────────────────────────────────────────
    buf = io.BytesIO()
    wb  = xlsxwriter.Workbook(buf, {"in_memory": True, "remove_timezone": True})

    def fmt(**kw):
        return wb.add_format(kw)

    HDR  = fmt(bold=True, font_color="#FFFFFF", bg_color="#1F2937", border=1,
               border_color="#374151", align="center", valign="vcenter", text_wrap=True)
    TITLE = fmt(bold=True, font_size=14, font_color="#111827", valign="vcenter")

    # row backgrounds (data cells)
    MM_A = fmt(bg_color="#FEE2E2", border=1, border_color="#FECACA", valign="vcenter")
    MM_B = fmt(bg_color="#FECACA", border=1, border_color="#FCA5A5", valign="vcenter")
    MS   = fmt(bg_color="#FEF9C3", border=1, border_color="#FDE68A", valign="vcenter")
    EX   = fmt(bg_color="#DBEAFE", border=1, border_color="#BFDBFE", valign="vcenter")

    # PK cells — purple accent, bold, สีเดียวกันทุก sheet
    PK   = fmt(bold=True, font_color="#4C1D95", bg_color="#EDE9FE",
               border=2, border_color="#7C3AED", valign="vcenter")

    MM_BADGE = fmt(bold=True, font_color="#FFFFFF", bg_color="#EF4444", border=1,
                   border_color="#DC2626", align="center", valign="vcenter")
    MS_BADGE = fmt(bold=True, font_color="#FFFFFF", bg_color="#D97706", border=1,
                   border_color="#B45309", align="center", valign="vcenter")
    EX_BADGE = fmt(bold=True, font_color="#FFFFFF", bg_color="#2563EB", border=1,
                   border_color="#1D4ED8", align="center", valign="vcenter")

    NUM  = fmt(font_color="#9CA3AF", font_size=9, border=1, border_color="#E5E7EB",
               align="center", valign="vcenter")

    # ── Sheet 1: Summary ─────────────────────────────────────────────────
    ws = wb.add_worksheet("📊 Summary")
    ws.hide_gridlines(2)
    ws.set_column(0, 0, 28); ws.set_column(1, 1, 18)
    ws.set_row(0, 32); ws.set_row(1, 8); ws.set_row(2, 22)
    ws.merge_range(0, 0, 0, 1, f"Migration Audit Report — {table}", TITLE)
    if meta.get("window"):
        ws.set_row(1, 18)
        ws.merge_range(1, 0, 1, 1, f"Data window: {meta['window']}",
                       fmt(font_color="#6B7280", italic=True, valign="vcenter"))
    ws.write_row(2, 0, ["Category", "Count"], HDR)

    for ri, (label, val, bg, vbg) in enumerate([
        ("Total Discrepancies", meta.get("mismatch",0)+meta.get("missing",0)+meta.get("extra",0), "#F3F4F6","#E5E7EB"),
        ("✓  Matched Rows",     meta.get("match", 0),    "#D1FAE5","#A7F3D0"),
        ("✗  Mismatch",         meta.get("mismatch", 0), "#FEE2E2","#FECACA"),
        ("⚠  Missing in Target",meta.get("missing", 0),  "#FEF9C3","#FDE68A"),
        ("➕  Extra in Target",  meta.get("extra", 0),    "#DBEAFE","#BFDBFE"),
    ]):
        ws.write(ri+3, 0, label, fmt(bg_color=bg,  border=1, border_color="#D1D5DB"))
        ws.write(ri+3, 1, val,   fmt(bold=True, font_size=12, bg_color=vbg,
                                     border=1, border_color="#D1D5DB", align="center"))

    # ── Sheet 2: Mismatch (merged PK cells) ──────────────────────────────
    pk_n  = len(pk_cols)
    pk_w  = [max(14, len(c)+2) for c in pk_cols]
    ws2   = wb.add_worksheet("🔴 Mismatch")
    ws2.hide_gridlines(2); ws2.freeze_panes(1, 0)
    hdr2  = ["#"] + pk_cols + ["Column Name", src_label, tgt_label]
    ws2.write_row(0, 0, hdr2, HDR); ws2.set_row(0, 24)
    ws2.set_column(0, 0, 6)
    for ci, w in enumerate(pk_w): ws2.set_column(ci+1, ci+1, min(w, 36))
    ws2.set_column(pk_n+1, pk_n+1, 22)
    ws2.set_column(pk_n+2, pk_n+2, 28); ws2.set_column(pk_n+3, pk_n+3, 28)

    r = 1
    for grp_i, grp in enumerate(mm_groups):
        pk_vals = [grp["pk"].get(c, "") for c in pk_cols]
        nrows   = len(grp["fields"]) or 1
        bg      = MM_A if grp_i % 2 == 0 else MM_B

        # # column + PK columns: merge if multiple diff fields
        if nrows > 1:
            ws2.merge_range(r, 0, r+nrows-1, 0, grp_i+1, NUM)
            for ci, v in enumerate(pk_vals):
                ws2.merge_range(r, ci+1, r+nrows-1, ci+1, v, PK)
        else:
            ws2.write(r, 0, grp_i+1, NUM)
            for ci, v in enumerate(pk_vals):
                ws2.write(r, ci+1, v, PK)

        for fi, (col, src, tgt) in enumerate(grp["fields"]):
            ws2.write(r+fi, pk_n+1, col, bg)
            ws2.write(r+fi, pk_n+2, src, bg)
            ws2.write(r+fi, pk_n+3, tgt, bg)

        r += nrows

    # ── Sheet 3: Missing (merged PK cells, mismatch-style grid) ──────────
    ws3 = wb.add_worksheet("🟡 Missing")
    ws3.hide_gridlines(2); ws3.freeze_panes(1, 0)
    hdr3 = ["#"] + pk_cols + ["Column Name", src_label, tgt_label]
    ws3.write_row(0, 0, hdr3, HDR); ws3.set_row(0, 24)
    ws3.set_column(0, 0, 6)
    for ci, w in enumerate(pk_w): ws3.set_column(ci+1, ci+1, min(w, 36))
    ws3.set_column(pk_n+1, pk_n+1, 22)
    ws3.set_column(pk_n+2, pk_n+2, 28); ws3.set_column(pk_n+3, pk_n+3, 28)

    r = 1
    for grp_i, grp in enumerate(ms_groups):
        pk_vals = [grp["pk"].get(c, "") for c in pk_cols]
        nrows   = len(grp["fields"]) or 1

        if nrows > 1:
            ws3.merge_range(r, 0, r+nrows-1, 0, grp_i+1, NUM)
            for ci, v in enumerate(pk_vals):
                ws3.merge_range(r, ci+1, r+nrows-1, ci+1, v, PK)
        else:
            ws3.write(r, 0, grp_i+1, NUM)
            for ci, v in enumerate(pk_vals):
                ws3.write(r, ci+1, v, PK)

        for fi, (col, src, tgt) in enumerate(grp["fields"]):
            ws3.write(r+fi, pk_n+1, col, MS)
            ws3.write(r+fi, pk_n+2, src or "-", MS)
            ws3.write(r+fi, pk_n+3, tgt or "-", MS)

        r += nrows

    # ── Sheet 4: Extra (merged PK cells, mismatch-style grid) ────────────
    ws4 = wb.add_worksheet("🔵 Extra")
    ws4.hide_gridlines(2); ws4.freeze_panes(1, 0)
    hdr4 = ["#"] + pk_cols + ["Column Name", src_label, tgt_label]
    ws4.write_row(0, 0, hdr4, HDR); ws4.set_row(0, 24)
    ws4.set_column(0, 0, 6)
    for ci, w in enumerate(pk_w): ws4.set_column(ci+1, ci+1, min(w, 36))
    ws4.set_column(pk_n+1, pk_n+1, 22)
    ws4.set_column(pk_n+2, pk_n+2, 28); ws4.set_column(pk_n+3, pk_n+3, 28)

    r = 1
    for grp_i, grp in enumerate(ex_groups):
        pk_vals = [grp["pk"].get(c, "") for c in pk_cols]
        nrows   = len(grp["fields"]) or 1

        if nrows > 1:
            ws4.merge_range(r, 0, r+nrows-1, 0, grp_i+1, NUM)
            for ci, v in enumerate(pk_vals):
                ws4.merge_range(r, ci+1, r+nrows-1, ci+1, v, PK)
        else:
            ws4.write(r, 0, grp_i+1, NUM)
            for ci, v in enumerate(pk_vals):
                ws4.write(r, ci+1, v, PK)

        for fi, (col, src, tgt) in enumerate(grp["fields"]):
            ws4.write(r+fi, pk_n+1, col, EX)
            ws4.write(r+fi, pk_n+2, src or "-", EX)
            ws4.write(r+fi, pk_n+3, tgt or "-", EX)

        r += nrows

    wb.close()
    buf.seek(0)
    return send_file(buf,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=f"{table}_discrepancies.xlsx",
    )


@app.route("/api/reports/mismatches", methods=["POST"])
def get_report_mismatches():
    """
    Paginated read of the generated mismatches CSV report.
    Accepts: { table, offset, limit, filter_type }
    Returns: { table, offset, limit, returned, total, samples: [...] }
    """
    data = request.json or {}
    table = data.get("table")
    offset = int(data.get("offset", 0))
    limit = int(data.get("limit", 50))
    filter_type = data.get("filter_type", "all").lower()

    if not table:
        return jsonify({"error": "Missing 'table' parameter"}), 400

    csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "reports", f"{table}_mismatches.csv")
    if not os.path.exists(csv_path):
        return jsonify({"table": table, "offset": offset, "limit": limit, "returned": 0, "total": 0, "samples": []})

    import csv
    samples = []
    total = 0
    
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None) # skip header row
            
            for row in reader:
                if len(row) < 5:
                    continue
                rtype, pk, col, src_val, tgt_val = row
                if filter_type != "all" and rtype.lower() != filter_type:
                    continue
                
                total += 1
                
                # Apply offset and limit
                if total > offset and len(samples) < limit:
                    # Unpack the Primary Key back into a structured dictionary for the UI
                    # E.g. "id=105, key_col=abc" -> {"id": "105", "key_col": "abc"}
                    pk_dict = {}
                    if pk and pk != "—":
                        parts = pk.split(", ")
                        for p in parts:
                            kv = p.split("=")
                            if len(kv) == 2:
                                pk_dict[kv[0]] = kv[1]
                            else:
                                pk_dict["PK"] = p
                    else:
                        pk_dict["PK"] = "—"

                    samples.append({
                        "type": rtype,
                        "status": rtype,
                        "pk": pk_dict,
                        "column": col,
                        "source": src_val if src_val != "NULL" else None,
                        "target": tgt_val if tgt_val != "NULL" else None
                    })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

    return jsonify({
        "table": table,
        "offset": offset,
        "limit": limit,
        "returned": len(samples),
        "total": total,
        "samples": samples
    })


# ─── SQL Compare Streaming ────────────────────────────────────────────────────
import re as _re

_sc_thread: threading.Thread | None = None
_sc_thread_lock = threading.Lock()
_sc_buffer: list[str] = []
_sc_buffer_lock = threading.Lock()
_sc_done = threading.Event()
_sc_stop = threading.Event()
SC_REPORT = "_sql_compare"


def _sc_reset():
    global _sc_buffer
    with _sc_buffer_lock:
        _sc_buffer = []
    _sc_done.clear()
    _sc_stop.clear()
    reports_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "reports")
    for suffix in ("_full_compare.ndjson", "_compare_meta.json", "_mismatches.csv"):
        try:
            os.remove(os.path.join(reports_dir, SC_REPORT + suffix))
        except Exception:
            pass


def _sc_emit(obj: dict):
    line = json.dumps(obj, ensure_ascii=False)
    with _sc_buffer_lock:
        _sc_buffer.append(line)


def _strip_order_limit(sql: str) -> str:
    s = _re.sub(r'\s+ORDER\s+BY\b[^;]*$', '', sql, flags=_re.IGNORECASE).strip()
    s = _re.sub(r'\s+LIMIT\s+\d+(\s*,\s*\d+)?\s*$', '', s, flags=_re.IGNORECASE).strip()
    return s


def _sc_worker(sql: str, table: str, db: str):
    from mysql40 import MySQL40Connection
    from decimal import Decimal as _Dec, InvalidOperation as _DecErr

    cfg = _load_config()
    src_cfg = cfg.get("source", DEFAULT_CONFIG["source"])
    tgt_cfg = cfg.get("target", DEFAULT_CONFIG["target"])
    _tol, _rnd = _numeric_cmp_params(cfg)

    reports_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "reports")
    os.makedirs(reports_dir, exist_ok=True)
    ndjson_path = os.path.join(reports_dir, f"{SC_REPORT}_full_compare.ndjson")
    meta_path   = os.path.join(reports_dir, f"{SC_REPORT}_compare_meta.json")

    src_conn = tgt_conn = ndjson_f = csv_f = None
    try:
        # Preserve user LIMIT before stripping
        _lm = _re.search(r'\bLIMIT\s+(\d+)\s*$', sql, flags=_re.IGNORECASE)
        user_limit = int(_lm.group(1)) if _lm else None
        strip_sql = _strip_order_limit(sql)

        # ── Discover columns & PK ─────────────────────────────────────────────
        tgt_conn = _connect_target(tgt_cfg, db)

        tgt_version = int(tgt_cfg.get("version", 8))
        pk_cols = []
        if table:
            pk_cols = _get_tgt_pk_cols(tgt_conn, tgt_version, db, table)


        src_conn = MySQL40Connection(
            host=src_cfg["host"], port=int(src_cfg["port"]),
            user=src_cfg["user"], password=src_cfg["password"],
            database=db, timeout=3600, charset="tis620",
        )
        src_cols, _ = src_conn.query_with_cols(f"{strip_sql} LIMIT 1")
        src_col_type_map = {}
        _type_table = table
        if not _type_table:
            # No table selected — infer from SQL so BINARY is applied for string PKs
            _m = _re.search(r'\bFROM\s+`?(\w+)`?', strip_sql, _re.IGNORECASE)
            if _m:
                _type_table = _m.group(1)
        if _type_table:
            try:
                src_col_rows = src_conn.query(f"SHOW COLUMNS FROM `{_type_table}`")
                src_col_type_map = {r[0]: r[1].lower() for r in src_col_rows}
            except Exception:
                pass
        src_conn.close(); src_conn = None

        if tgt_version == 4:
            tgt_cols, _ = tgt_conn.query_with_cols(f"{strip_sql} LIMIT 1")
        else:
            tc = tgt_conn.cursor()
            tc.execute(f"{strip_sql} LIMIT 1")
            tgt_cols = [d[0] for d in tc.description] if tc.description else []
            tc.close()

        tgt_col_set = set(tgt_cols)
        common_cols = [c for c in src_cols if c in tgt_col_set]

        # Filter out ignored columns if a table is specified, but ALWAYS preserve PK columns
        if table:
            cfg = _load_config()
            ignored = cfg.get("ignore_fields", {}).get(db, {}).get(table, [])
            common_cols = [c for c in common_cols if c not in ignored or c in pk_cols]

        if not common_cols:
            _sc_emit({"type": "error", "message": "No common columns between source and target"})
            _sc_emit({"type": "done", "exit_code": 1}); return

        if not pk_cols:
            pk_cols = [common_cols[0]]

        missing_pk = [c for c in pk_cols if c not in set(common_cols)]
        if missing_pk:
            _sc_emit({"type": "error",
                      "message": f"PK column(s) {missing_pk} not in SELECT — add them to your query"})
            _sc_emit({"type": "done", "exit_code": 1}); return

        # ── Build streaming SQL ───────────────────────────────────────────────
        _str_type_keywords = ("char", "text", "enum", "set", "varchar", "binary", "blob")
        pk_str_mask = [
            any(kw in src_col_type_map.get(c, "") for kw in _str_type_keywords)
            for c in pk_cols
        ]
        pk_order = ", ".join(
            f"BINARY `{c}`" if pk_str_mask[i] else f"`{c}`"
            for i, c in enumerate(pk_cols)
        )
        limit_clause = f" LIMIT {user_limit}" if user_limit else ""
        stream_sql = f"{strip_sql} ORDER BY {pk_order}{limit_clause}"

        _sc_emit({"type": "sc_start", "pk_cols": pk_cols, "columns": common_cols,
                  "db": db, "table": table, "sql": stream_sql})

        # ── Open streams ──────────────────────────────────────────────────────
        src_conn = MySQL40Connection(
            host=src_cfg["host"], port=int(src_cfg["port"]),
            user=src_cfg["user"], password=src_cfg["password"],
            database=db, timeout=3600, charset="tis620",
        )

        if tgt_version != 4:
            tc_setup = tgt_conn.cursor()
            try:
                tc_setup.execute("SET SESSION net_write_timeout=3600")
                tc_setup.execute("SET SESSION net_read_timeout=3600")
            except Exception:
                pass
            tc_setup.close()

        src_idx = {c: i for i, c in enumerate(src_cols)}
        tgt_idx = {c: i for i, c in enumerate(tgt_cols)}
        c_src = [src_idx[c] for c in common_cols]
        c_tgt = [tgt_idx[c] for c in common_cols]
        pk_si  = [src_idx[c] for c in pk_cols]
        pk_ti  = [tgt_idx[c] for c in pk_cols]

        def _norm(v):
            if v is None: return (0, "")
            # Normalize date/datetime to consistent string so MySQL 4.0
            # (returns datetime.datetime for DATE cols) matches MySQL 8
            # (returns datetime.date). Strip zero time to "YYYY-MM-DD".
            if isinstance(v, _dt.datetime):
                s = v.strftime("%Y-%m-%d") if (v.hour == v.minute == v.second == v.microsecond == 0) else v.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(v, _dt.date):
                s = v.strftime("%Y-%m-%d")
            else:
                s = str(v)
            try: return (1, _Dec(s))
            except _DecErr: return (2, s)

        def pk_s(row): return tuple(_norm(row[i]) for i in pk_si)
        def pk_t(row): return tuple(_norm(row[i]) for i in pk_ti)
        def pk_d(key): return {pk_cols[j]: str(key[j][1]) for j in range(len(pk_cols))}
        def pk_str(d): return ", ".join(f"{k}={v}" for k, v in d.items())

        src_gen = src_conn.query_stream(stream_sql)

        tgt_cur = tgt_conn.cursor() if tgt_version == 4 else tgt_conn.cursor(MySQLdb.cursors.SSCursor)
        tgt_cur.execute(stream_sql)
        def tgt_gen():
            while not _sc_stop.is_set():
                r = tgt_cur.fetchone()
                if r is None: break
                yield r

        # ── Two-pointer merge ─────────────────────────────────────────────────
        csv_path = os.path.join(reports_dir, f"{SC_REPORT}_mismatches.csv")
        ndjson_f = open(ndjson_path, "w", encoding="utf-8")
        csv_f    = open(csv_path, "w", newline="", encoding="utf-8-sig")
        import csv as _csv
        csv_w    = _csv.writer(csv_f)
        csv_w.writerow(["Type", "Primary Key", "Column Name", "Source Value", "Target Value"])
        PROG = 10000; MAX_S = 2000

        total = matches = mismatches = missing = extra = 0
        last_prog = 0

        s_row = next(src_gen, None)
        t_iter = tgt_gen(); t_row = next(t_iter, None)

        while (s_row is not None or t_row is not None) and not _sc_stop.is_set():
            if s_row is not None and t_row is not None:
                sk, tk = pk_s(s_row), pk_t(t_row)
                if sk == tk:
                    total += 1
                    diffs_flag = []
                    row_diff = False
                    src_vals = [str(s_row[c_src[i]]) if s_row[c_src[i]] is not None else None for i in range(len(common_cols))]
                    tgt_vals = [str(t_row[c_tgt[i]]) if t_row[c_tgt[i]] is not None else None for i in range(len(common_cols))]
                    for i in range(len(common_cols)):
                        ok = _close_vals(s_row[c_src[i]], t_row[c_tgt[i]], _tol, _rnd)
                        diffs_flag.append(not ok)
                        if not ok: row_diff = True
                    status = "mismatch" if row_diff else "match"
                    if row_diff: mismatches += 1
                    else: matches += 1
                    d = pk_d(sk)
                    ndjson_f.write(json.dumps({"type": status, "pk": pk_str(d),
                        "cols": common_cols, "src": src_vals, "tgt": tgt_vals,
                        "diffs": diffs_flag}, ensure_ascii=False) + "\n")
                    if row_diff:
                        ps = pk_str(d)
                        for i, col in enumerate(common_cols):
                            if diffs_flag[i]:
                                csv_w.writerow(["mismatch", ps, col,
                                                src_vals[i] if src_vals[i] is not None else "NULL",
                                                tgt_vals[i] if tgt_vals[i] is not None else "NULL"])
                    s_row = next(src_gen, None); t_row = next(t_iter, None)

                elif sk < tk:
                    missing += 1; total += 1
                    d = pk_d(sk)
                    _src = [str(s_row[c_src[i]]) if s_row[c_src[i]] is not None else None for i in range(len(common_cols))]
                    ndjson_f.write(json.dumps({"type": "missing", "pk": pk_str(d),
                        "cols": common_cols, "src": _src}, ensure_ascii=False) + "\n")
                    csv_w.writerow(["missing", pk_str(d), "ALL", "Present in Source", "Missing in Target"])
                    s_row = next(src_gen, None)
                else:
                    extra += 1; total += 1
                    d = pk_d(tk)
                    _tgt = [str(t_row[c_tgt[i]]) if t_row[c_tgt[i]] is not None else None for i in range(len(common_cols))]
                    ndjson_f.write(json.dumps({"type": "extra", "pk": pk_str(d),
                        "cols": common_cols, "tgt": _tgt}, ensure_ascii=False) + "\n")
                    csv_w.writerow(["extra", pk_str(d), "ALL", "Missing in Source", "Extra in Target"])
                    t_row = next(t_iter, None)

            elif s_row is not None:
                missing += 1; total += 1
                d = pk_d(pk_s(s_row))
                _src = [str(s_row[c_src[i]]) if s_row[c_src[i]] is not None else None for i in range(len(common_cols))]
                ndjson_f.write(json.dumps({"type": "missing", "pk": pk_str(d),
                    "cols": common_cols, "src": _src}, ensure_ascii=False) + "\n")
                csv_w.writerow(["missing", pk_str(d), "ALL", "Present in Source", "Missing in Target"])
                s_row = next(src_gen, None)
            else:
                extra += 1; total += 1
                d = pk_d(pk_t(t_row))
                _tgt = [str(t_row[c_tgt[i]]) if t_row[c_tgt[i]] is not None else None for i in range(len(common_cols))]
                ndjson_f.write(json.dumps({"type": "extra", "pk": pk_str(d),
                    "cols": common_cols, "tgt": _tgt}, ensure_ascii=False) + "\n")
                csv_w.writerow(["extra", pk_str(d), "ALL", "Missing in Source", "Extra in Target"])
                t_row = next(t_iter, None)

            if total - last_prog >= PROG:
                last_prog = total
                _sc_emit({"type": "sc_progress", "total": total, "matches": matches,
                          "mismatches": mismatches, "missing": missing, "extra": extra})

        # ── Finish ────────────────────────────────────────────────────────────
        ndjson_f.close(); ndjson_f = None
        csv_f.close(); csv_f = None
        meta = {"all": total, "match": matches, "mismatch": mismatches,
                "missing": missing, "extra": extra}
        with open(meta_path, "w", encoding="utf-8") as mf:
            json.dump(meta, mf)

        stopped = _sc_stop.is_set()
        _sc_emit({"type": "sc_done", "total": total, "matches": matches,
                  "mismatches": mismatches, "missing": missing, "extra": extra,
                  "stopped": stopped})
        _sc_emit({"type": "done", "exit_code": 0 if not stopped else 1})

    except Exception as e:
        import traceback; traceback.print_exc()
        _sc_emit({"type": "error", "message": str(e)})
        _sc_emit({"type": "done", "exit_code": 1})
    finally:
        if ndjson_f:
            try: ndjson_f.close()
            except Exception: pass
        if csv_f:
            try: csv_f.close()
            except Exception: pass
        if src_conn:
            try: src_conn.close()
            except Exception: pass
        if tgt_conn:
            try: tgt_conn.close()
            except Exception: pass
        _sc_done.set()


@app.route("/api/sql-compare/run", methods=["POST"])
def sc_run_stream():
    global _sc_thread
    data  = request.json or {}
    sql   = (data.get("sql") or "").strip()
    table = (data.get("table") or "").strip()
    db    = data.get("db", "prs")

    if not sql:
        return jsonify({"error": "Missing sql"}), 400
    if not sql.upper().lstrip().startswith("SELECT"):
        return jsonify({"error": "Only SELECT allowed"}), 400

    with _sc_thread_lock:
        if _sc_thread and _sc_thread.is_alive():
            return jsonify({"error": "Already running"}), 409
        _sc_reset()
        _sc_thread = threading.Thread(target=_sc_worker, args=(sql, table, db), daemon=True)
        _sc_thread.start()
    return jsonify({"started": True})


@app.route("/api/sql-compare/stop", methods=["POST"])
def sc_stop_stream():
    _sc_stop.set()
    _sc_done.wait(timeout=15)
    return jsonify({"stopped": True})


@app.route("/api/sql-compare/status")
def sc_status():
    with _sc_thread_lock:
        running = _sc_thread is not None and _sc_thread.is_alive()
    return jsonify({"running": running, "done": _sc_done.is_set(),
                    "buffered": len(_sc_buffer)})


@app.route("/api/sql-compare/stream")
def sc_stream():
    def generate():
        pos = 0
        while True:
            with _sc_buffer_lock:
                snap = len(_sc_buffer)
            if pos < snap:
                with _sc_buffer_lock:
                    chunk = _sc_buffer[pos:snap]
                for line in chunk:
                    yield f"data: {line}\n\n"
                pos = snap
                if chunk and '"type": "done"' in chunk[-1]:
                    return
            else:
                if _sc_done.is_set() and pos >= snap:
                    return
                time.sleep(0.2)
    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ─── SQL Compare (one-shot, legacy) ───────────────────────────────────────────
@app.route("/api/sql-compare", methods=["POST"])
def sql_compare():
    """
    Run a user-written SELECT on both source and target, compare field-by-field.
    Accepts: { sql, pk_cols: [], db, limit }
    Returns same row shape as /api/random-compare.
    """
    data = request.json or {}
    sql = (data.get("sql") or "").strip()
    db = data.get("db", "prs")
    table = (data.get("table") or "").strip()
    row_limit = min(int(data.get("limit", 1000)), 50000)

    if not sql:
        return jsonify({"error": "Missing 'sql' parameter"}), 400

    sql_upper = sql.upper().lstrip()
    if not any(sql_upper.startswith(k) for k in ("SELECT", "SHOW", "DESCRIBE", "DESC")):
        return jsonify({"error": "Only SELECT queries are allowed"}), 400

    exec_sql = sql
    if sql_upper.startswith("SELECT") and "LIMIT" not in sql_upper:
        exec_sql = f"{sql} LIMIT {row_limit}"

    cfg = _load_config()
    src_cfg = cfg.get("source", DEFAULT_CONFIG["source"])
    tgt_cfg = cfg.get("target", DEFAULT_CONFIG["target"])
    _tol, _rnd = _numeric_cmp_params(cfg)

    src_conn = None
    tgt_conn = None
    try:
        from mysql40 import MySQL40Connection
        from decimal import Decimal as _Dec, InvalidOperation as _DecErr

        src_conn = MySQL40Connection(
            host=src_cfg["host"], port=int(src_cfg["port"]),
            user=src_cfg["user"], password=src_cfg["password"],
            database=db, timeout=60, charset="tis620",
        )
        tgt_conn = _connect_target(tgt_cfg, db)

        src_cols, src_rows_raw = src_conn.query_with_cols(exec_sql)

        tgt_version = int(tgt_cfg.get("version", 8))
        if tgt_version == 4:
            tgt_cols, tgt_rows_raw = tgt_conn.query_with_cols(exec_sql)
        else:
            tc = tgt_conn.cursor()
            tc.execute(exec_sql)
            tgt_cols = [d[0] for d in tc.description] if tc.description else []
            tgt_rows_raw = tc.fetchall()
            tc.close()

        tgt_col_set = set(tgt_cols)
        common_cols = [c for c in src_cols if c in tgt_col_set]
        if not common_cols:
            return jsonify({"error": "No common columns between source and target result sets"}), 400
        pk_cols = []
        if table:
            pk_cols = _get_tgt_pk_cols(tgt_conn, tgt_version, db, table)

            missing_pk = [c for c in pk_cols if c not in set(common_cols)]
            if missing_pk:
                return jsonify({"error": f"PK column(s) {missing_pk} not in SELECT result — add them to your query"}), 400

        if not pk_cols:
            pk_cols = [common_cols[0]]

        src_col_idx = {c: i for i, c in enumerate(src_cols)}
        tgt_col_idx = {c: i for i, c in enumerate(tgt_cols)}
        common_src_idx = [src_col_idx[c] for c in common_cols]
        common_tgt_idx = [tgt_col_idx[c] for c in common_cols]
        pk_src_idx = [src_col_idx[c] for c in pk_cols]
        pk_tgt_idx = [tgt_col_idx[c] for c in pk_cols]

        def _pk_norm(v):
            if v is None:
                return (0, "")
            s = str(v)
            try:
                return (1, _Dec(s))
            except _DecErr:
                return (2, s)

        src_by_pk = {}
        for row in src_rows_raw:
            src_by_pk[tuple(_pk_norm(row[i]) for i in pk_src_idx)] = row

        tgt_by_pk = {}
        for row in tgt_rows_raw:
            tgt_by_pk[tuple(_pk_norm(row[i]) for i in pk_tgt_idx)] = row

        result_rows = []
        total_matches = total_mismatches = total_missing = total_extra = 0

        for key in sorted(set(src_by_pk) | set(tgt_by_pk)):
            src_row = src_by_pk.get(key)
            tgt_row = tgt_by_pk.get(key)
            pk_dict = {pk_cols[j]: str(key[j][1]) for j in range(len(pk_cols))}

            if src_row is not None and tgt_row is not None:
                fields = []
                row_has_diff = False
                for ci, col in enumerate(common_cols):
                    sv = src_row[common_src_idx[ci]]
                    tv = tgt_row[common_tgt_idx[ci]]
                    is_match = _close_vals(sv, tv, _tol, _rnd)
                    if not is_match:
                        row_has_diff = True
                    fields.append({
                        "column": col,
                        "source": str(sv) if sv is not None else None,
                        "target": str(tv) if tv is not None else None,
                        "match": is_match,
                    })
                if row_has_diff:
                    total_mismatches += 1
                else:
                    total_matches += 1
                result_rows.append({"pk": pk_dict, "status": "mismatch" if row_has_diff else "match", "fields": fields})

            elif src_row is not None:
                fields = [{"column": col, "source": str(src_row[common_src_idx[ci]]) if src_row[common_src_idx[ci]] is not None else None,
                           "target": None, "match": False} for ci, col in enumerate(common_cols)]
                result_rows.append({"pk": pk_dict, "status": "missing", "fields": fields})
                total_missing += 1
            else:
                fields = [{"column": col, "source": None,
                           "target": str(tgt_row[common_tgt_idx[ci]]) if tgt_row[common_tgt_idx[ci]] is not None else None,
                           "match": False} for ci, col in enumerate(common_cols)]
                result_rows.append({"pk": pk_dict, "status": "extra", "fields": fields})
                total_extra += 1

        return jsonify({
            "sql": sql,
            "table": table,
            "pk_cols": pk_cols,
            "db": db,
            "total_compared": len(result_rows),
            "matches": total_matches,
            "mismatches": total_mismatches,
            "missing": total_missing,
            "extra": total_extra,
            "columns": common_cols,
            "rows": result_rows,
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        if src_conn:
            try: src_conn.close()
            except Exception: pass
        if tgt_conn:
            try: tgt_conn.close()
            except Exception: pass


if __name__ == "__main__":
    os.makedirs(os.path.join(os.path.dirname(os.path.abspath(__file__)), "static"), exist_ok=True)
    print("  ✓  Migration Validator Dashboard running at http://localhost:5001")
    app.run(host="0.0.0.0", port=5001, debug=False, threaded=True)
