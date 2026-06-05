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
    """Return updated snapshot: full run replaces all; sync-only updates selected tables."""
    non_done = [e for e in new_events if '"type": "done"' not in e]
    done_events = [e for e in new_events if '"type": "done"' in e]
    if not sync_tables:
        return non_done + done_events
    # Keep existing events for tables NOT in this sync run
    kept = []
    for line in _snapshot:
        if '"type": "done"' in line:
            continue
        try:
            ev = json.loads(line)
            if ev.get("table") in sync_tables:
                continue   # will be replaced by new events
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
    sync_tables = data.get("sync_tables", [])   # list of table names for full sync pass

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
        _process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )
    t = threading.Thread(target=_reader_thread, args=(_process,), daemon=True)
    t.start()
    return jsonify({"started": True, "db": db})


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
import MySQLdb

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
        t_conn = MySQLdb.connect(
            host=target["host"],
            port=int(target["port"]),
            user=target["user"],
            passwd=target["password"],
            connect_timeout=3
        )
        t_conn.close()
    except Exception as e:
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
            timeout=3
        )
        s_conn.close()
    except Exception as e:
        return jsonify({"error": f"Source DB Connection Error: {str(e)}"}), 400

    # Save configuration — merge over existing file so extra sections (tuning) survive
    cfg = _load_config()
    cfg.update({
        "source": {
            "host": source["host"],
            "port": int(source["port"]),
            "user": source["user"],
            "password": source["password"]
        },
        "target": {
            "host": target["host"],
            "port": int(target["port"]),
            "user": target["user"],
            "password": target["password"]
        },
        "default_db": data.get("default_db", "prs")
    })

    if _save_config(cfg):
        return jsonify({"success": True})
    else:
        return jsonify({"error": "Failed to write configuration file"}), 500


@app.route("/api/databases", methods=["GET"])
def get_databases():
    cfg = _load_config()
    target = cfg.get("target", DEFAULT_CONFIG["target"])
    
    try:
        t_conn = MySQLdb.connect(
            host=target["host"],
            port=int(target["port"]),
            user=target["user"],
            passwd=target["password"],
            connect_timeout=3
        )
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
from decimal import Decimal, InvalidOperation

DECIMAL_TOL = Decimal("0.0001")

def _close_vals(a, b):
    """Check if two values are 'close enough' (handles decimals, nulls, strings)."""
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    sa, sb = str(a).strip(), str(b).strip()
    if sa == sb:
        return True
    try:
        return abs(Decimal(sa) - Decimal(sb)) <= DECIMAL_TOL
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
    sample_offset = int(data.get("sample_offset", 0))

    if not table:
        return jsonify({"error": "Missing 'table' parameter"}), 400

    src_conn = None
    tgt_conn = None
    try:
        from mysql40 import MySQL40Connection

        cfg = _load_config()
        src_cfg = cfg.get("source", DEFAULT_CONFIG["source"])
        tgt_cfg = cfg.get("target", DEFAULT_CONFIG["target"])

        src_conn = MySQL40Connection(
            host=src_cfg["host"],
            port=int(src_cfg["port"]),
            user=src_cfg["user"],
            password=src_cfg["password"],
            database=db,
            timeout=120,
            charset="tis620",
        )
        tgt_conn = MySQLdb.connect(
            host=tgt_cfg["host"],
            port=int(tgt_cfg["port"]),
            user=tgt_cfg["user"],
            passwd=tgt_cfg["password"],
            db=db,
            charset="utf8mb4",
        )

        # Get PK columns from target (information_schema)
        tc = tgt_conn.cursor()
        tc.execute("""
            SELECT column_name FROM information_schema.key_column_usage
            WHERE table_schema = %s AND table_name = %s AND constraint_name = 'PRIMARY'
            ORDER BY ordinal_position
        """, (db, table))
        pk_cols = [r[0] for r in tc.fetchall()]
        tc.close()

        if not pk_cols:
            return jsonify({"error": f"Table '{table}' has no primary key"}), 400

        # Get all column names from target
        tc = tgt_conn.cursor()
        tc.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (db, table))
        all_cols = [r[0] for r in tc.fetchall()]
        tc.close()

        # Also get source column names for intersection
        src_raw_cols = src_conn.query(f"SHOW COLUMNS FROM `{table}`")
        src_col_names = [r[0] for r in src_raw_cols]

        # Use intersection of columns (in target order)
        common_cols = [c for c in all_cols if c in set(src_col_names)]
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

        if use_range:
            if sample_offset == 0:
                _mm = src_conn.query(f"SELECT MIN(`{range_pk}`), MAX(`{range_pk}`) FROM `{table}`")
                _min = int(_mm[0][0]) if _mm and _mm[0][0] is not None else 0
                _max = int(_mm[0][1]) if _mm and _mm[0][1] is not None else 0
                _span = max(0, _max - _min - sample_size)
                start_pk = _min + int((seed % 999983) / 999983.0 * _span)
            else:
                start_pk = sample_offset   # continuation: last batch max_pk + 1
            src_sql = (
                f"SELECT {col_list} FROM `{table}` "
                f"WHERE `{range_pk}` >= {start_pk} "
                f"ORDER BY `{range_pk}` LIMIT {sample_size}"
            )
        else:
            if sample_offset == 0:
                _cnt = src_conn.query(f"SELECT COUNT(*) FROM `{table}`")
                _total = int(_cnt[0][0]) if _cnt else 0
                _max_off = max(0, _total - sample_size)
                start_pk = int((seed % 999983) / 999983.0 * _max_off) if _max_off > 0 else 0
            else:
                start_pk = sample_offset
            src_sql = f"SELECT {col_list} FROM `{table}` LIMIT {sample_size} OFFSET {start_pk}"

        src_rows_raw = src_conn.query(src_sql)

        # Build a dict of source rows keyed by PK
        pk_indices = [common_cols.index(c) for c in pk_cols]
        def pk_of(row):
            return tuple(str(row[i]) if row[i] is not None else "" for i in pk_indices)

        src_by_pk = {}
        for row in src_rows_raw:
            pk = pk_of(row)
            src_by_pk[pk] = row

        # Query target for matching PKs
        result_rows = []
        total_matches = 0
        total_mismatches = 0
        total_missing = 0
        total_extra = 0

        # Batch fetch from target using IN clause on PK
        if len(pk_cols) == 1:
            pk_values = [pk[0] for pk in src_by_pk.keys()]
            # Process in batches of 500
            batch_size = 500
            tgt_by_pk = {}
            for i in range(0, len(pk_values), batch_size):
                batch = pk_values[i:i + batch_size]
                placeholders = ", ".join(["%s"] * len(batch))
                tgt_sql = (
                    f"SELECT {col_list} FROM `{table}` "
                    f"WHERE `{pk_cols[0]}` IN ({placeholders})"
                )
                tc = tgt_conn.cursor()
                tc.execute(tgt_sql, batch)
                for row in tc.fetchall():
                    pk = pk_of(row)
                    tgt_by_pk[pk] = row
                tc.close()
        else:
            # Multi-column PK: query each row individually (or use OR conditions)
            tgt_by_pk = {}
            pk_values_list = list(src_by_pk.keys())
            batch_size = 200
            for i in range(0, len(pk_values_list), batch_size):
                batch = pk_values_list[i:i + batch_size]
                conditions = []
                params = []
                for pk_vals in batch:
                    cond = " AND ".join(f"`{pk_cols[j]}` = %s" for j in range(len(pk_cols)))
                    conditions.append(f"({cond})")
                    params.extend(pk_vals)
                where = " OR ".join(conditions)
                tgt_sql = f"SELECT {col_list} FROM `{table}` WHERE {where}"
                tc = tgt_conn.cursor()
                tc.execute(tgt_sql, params)
                for row in tc.fetchall():
                    pk = pk_of(row)
                    tgt_by_pk[pk] = row
                tc.close()

        # Compare field-by-field: source rows vs target
        for pk, src_row in src_by_pk.items():
            tgt_row = tgt_by_pk.get(pk)
            pk_dict = {pk_cols[i]: pk[i] for i in range(len(pk_cols))}

            if tgt_row is None:
                # Missing in target
                fields = []
                for ci, col in enumerate(common_cols):
                    fields.append({
                        "column": col,
                        "source": str(src_row[ci]) if src_row[ci] is not None else None,
                        "target": None,
                        "match": False,
                    })
                result_rows.append({
                    "pk": pk_dict,
                    "status": "missing",
                    "fields": fields,
                })
                total_missing += 1
                continue

            # Both exist — compare each field
            fields = []
            row_has_diff = False
            for ci, col in enumerate(common_cols):
                sv = src_row[ci]
                tv = tgt_row[ci]
                is_match = _close_vals(sv, tv)
                if not is_match:
                    row_has_diff = True
                fields.append({
                    "column": col,
                    "source": str(sv) if sv is not None else None,
                    "target": str(tv) if tv is not None else None,
                    "match": is_match,
                })

            status = "mismatch" if row_has_diff else "match"
            if row_has_diff:
                total_mismatches += 1
            else:
                total_matches += 1

            result_rows.append({
                "pk": pk_dict,
                "status": status,
                "fields": fields,
            })

        # Detect extra rows: sample TARGET with same range/offset, find rows not in source
        try:
            if use_range:
                tgt_sample_sql = (
                    f"SELECT {col_list} FROM `{table}` "
                    f"WHERE `{range_pk}` >= {start_pk} "
                    f"ORDER BY `{range_pk}` LIMIT {sample_size}"
                )
            else:
                tgt_sample_sql = (
                    f"SELECT {col_list} FROM `{table}` "
                    f"LIMIT {sample_size} OFFSET {start_pk}"
                )
            tc = tgt_conn.cursor()
            tc.execute(tgt_sample_sql)
            tgt_sample_rows = tc.fetchall()
            tc.close()

            tgt_sample_pks = set()
            for row in tgt_sample_rows:
                pk = pk_of(row)
                tgt_sample_pks.add(pk)

            # Find PKs in target sample that aren't in source sample
            extra_pks = tgt_sample_pks - set(src_by_pk.keys())

            # Verify these extras really don't exist in source
            # (they might exist in source but just weren't in the random sample)
            # We do a quick lookup in source to confirm
            for epk in list(extra_pks)[:200]:  # limit to avoid overwhelming source
                pk_dict = {pk_cols[i]: epk[i] for i in range(len(pk_cols))}

                # Find the target row data
                tgt_row = None
                for row in tgt_sample_rows:
                    if pk_of(row) == epk:
                        tgt_row = row
                        break

                if tgt_row is None:
                    continue

                # Check if this PK exists in source
                if len(pk_cols) == 1:
                    check_sql = f"SELECT COUNT(*) FROM `{table}` WHERE `{pk_cols[0]}` = '{epk[0]}'"
                else:
                    where_parts = " AND ".join(
                        f"`{pk_cols[j]}` = '{epk[j]}'" for j in range(len(pk_cols))
                    )
                    check_sql = f"SELECT COUNT(*) FROM `{table}` WHERE {where_parts}"

                try:
                    src_check = src_conn.query(check_sql)
                    exists_in_src = int(src_check[0][0]) > 0
                except Exception:
                    exists_in_src = True  # assume exists if query fails

                if not exists_in_src:
                    fields = []
                    for ci, col in enumerate(common_cols):
                        fields.append({
                            "column": col,
                            "source": None,
                            "target": str(tgt_row[ci]) if tgt_row[ci] is not None else None,
                            "match": False,
                        })
                    result_rows.append({
                        "pk": pk_dict,
                        "status": "extra",
                        "fields": fields,
                    })
                    total_extra += 1
        except Exception:
            pass  # extra detection is best-effort

        # next_offset: for range mode = max PK in batch + 1; for offset mode = start + size
        if use_range and src_rows_raw:
            _pk_idx = common_cols.index(range_pk)
            next_offset = int(src_rows_raw[-1][_pk_idx]) + 1
        else:
            next_offset = start_pk + sample_size

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

    if not table:
        return jsonify({"error": "Missing 'table' parameter"}), 400

    cfg = _load_config()
    src_cfg = cfg.get("source", DEFAULT_CONFIG["source"])
    tgt_cfg = cfg.get("target", DEFAULT_CONFIG["target"])

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
        tgt_conn = MySQLdb.connect(
            host=tgt_cfg["host"],
            port=int(tgt_cfg["port"]),
            user=tgt_cfg["user"],
            passwd=tgt_cfg["password"],
            db=db,
            charset="utf8mb4",
        )

        # Determine which PK columns are TIMESTAMP/DATETIME.
        # Use pk_cols_hint from the frontend (derived from SHOW INDEX on source)
        # then check column types via SHOW COLUMNS on source — more reliable than
        # querying information_schema on target which may have different types.
        if pk_cols_hint:
            col_rows = src_conn.query(f"SHOW COLUMNS FROM `{table}`")
            # SHOW COLUMNS: Field, Type, Null, Key, Default, Extra
            src_col_types = {r[0]: r[1].lower() for r in col_rows}
            ts_col = None
            for col in pk_cols_hint:
                ctype = src_col_types.get(col, "")
                if "timestamp" in ctype or "datetime" in ctype or ctype.strip().startswith("date"):
                    ts_col = col
                    break
        else:
            # Fallback: ask target information_schema (original behaviour)
            tc = tgt_conn.cursor()
            tc.execute("""
                SELECT kcu.column_name, c.data_type
                FROM information_schema.key_column_usage kcu
                JOIN information_schema.columns c
                  ON c.table_schema = kcu.table_schema
                 AND c.table_name   = kcu.table_name
                 AND c.column_name  = kcu.column_name
                WHERE kcu.table_schema = %s
                  AND kcu.table_name   = %s
                  AND kcu.constraint_name = 'PRIMARY'
                ORDER BY kcu.ordinal_position
            """, (db, table))
            pk_info = tc.fetchall()
            tc.close()
            ts_col = None
            for col_name, data_type in pk_info:
                if data_type.lower() in ("timestamp", "datetime", "date"):
                    ts_col = col_name
                    break

        result = {"has_ts_pk": ts_col is not None, "ts_col": ts_col}

        if ts_col:
            # Query MIN/MAX from source
            src_rows = src_conn.query(
                f"SELECT MIN(`{ts_col}`), MAX(`{ts_col}`) FROM `{table}`"
            )
            src_min = str(src_rows[0][0]) if src_rows and src_rows[0][0] is not None else None
            src_max = str(src_rows[0][1]) if src_rows and src_rows[0][1] is not None else None

            # Query MIN/MAX from target
            tc = tgt_conn.cursor()
            tc.execute(f"SELECT MIN(`{ts_col}`), MAX(`{ts_col}`) FROM `{table}`")
            tgt_row = tc.fetchone()
            tc.close()
            tgt_min = str(tgt_row[0]) if tgt_row and tgt_row[0] is not None else None
            tgt_max = str(tgt_row[1]) if tgt_row and tgt_row[1] is not None else None

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
    tgt_cfg = cfg.get("target", DEFAULT_CONFIG["target"])

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
            tgt_conn = MySQLdb.connect(
                host=tgt_cfg["host"], port=int(tgt_cfg["port"]),
                user=tgt_cfg["user"], passwd=tgt_cfg["password"],
                db=db, charset="utf8mb4",
            )
            exec_sql = sql
            if sql_upper.startswith("SELECT") and "LIMIT" not in sql_upper:
                exec_sql = f"{sql} LIMIT {row_limit}"
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
                        idx = line.find('"type":"')
                        if idx == -1:
                            continue
                        idx += 8
                        end = line.find('"', idx)
                        rtype = line[idx:end]
                        tc["all"] += 1
                        tc[rtype] = tc.get(rtype, 0) + 1
            _full_compare_cache[table] = {"type_counts": tc, "mtime": mtime}
            cache = _full_compare_cache[table]

        type_counts = cache["type_counts"]
        total = type_counts.get(filter_type, 0) if filter_type != "all" else type_counts["all"]
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
                if filter_type != "all" and row.get("type") != filter_type:
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

    # ── Pass 1: discover PK columns + collect all grouped data ───────────
    pk_cols = []
    mm_groups = []   # [{pk_dict, fields: [(col,src,tgt)]}]
    ms_rows   = []   # [pk_dict]
    ex_rows   = []   # [pk_dict]

    cur_key = None
    cur_pk  = None
    cur_fields = []

    def parse_pk(pk_str):
        d = {}
        for part in pk_str.split(", "):
            kv = part.split("=", 1)
            if len(kv) == 2:
                d[kv[0]] = kv[1]
                if kv[0] not in pk_cols:
                    pk_cols.append(kv[0])
        return d

    def flush_mm():
        if cur_key and cur_fields:
            mm_groups.append({"pk": cur_pk, "fields": cur_fields[:]})

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv_mod.reader(f)
        next(reader, None)
        for row in reader:
            if len(row) < 5:
                continue
            rtype, pk_str, col, src_val, tgt_val = row[0], row[1], row[2], row[3], row[4]
            pk_dict = parse_pk(pk_str)
            if rtype == "mismatch":
                key = pk_str
                if key != cur_key:
                    flush_mm()
                    cur_key, cur_pk, cur_fields = key, pk_dict, []
                if col != "ALL":
                    cur_fields.append((col, src_val, tgt_val))
            elif rtype == "missing":
                ms_rows.append(pk_dict)
            elif rtype == "extra":
                ex_rows.append(pk_dict)
    flush_mm()

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
    NOTE = fmt(font_color="#6B7280", italic=True, border=1, border_color="#E5E7EB", valign="vcenter")

    # ── Sheet 1: Summary ─────────────────────────────────────────────────
    ws = wb.add_worksheet("📊 Summary")
    ws.hide_gridlines(2)
    ws.set_column(0, 0, 28); ws.set_column(1, 1, 18)
    ws.set_row(0, 32); ws.set_row(1, 8); ws.set_row(2, 22)
    ws.merge_range(0, 0, 0, 1, f"Migration Audit Report — {table}", TITLE)
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
    hdr2  = ["#"] + pk_cols + ["Column Name", "Source (MySQL 4)", "Target (MySQL 8)"]
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

    # ── Sheet 3: Missing ─────────────────────────────────────────────────
    ws3 = wb.add_worksheet("🟡 Missing")
    ws3.hide_gridlines(2); ws3.freeze_panes(1, 0)
    hdr3 = ["#"] + pk_cols + ["หมายเหตุ"]
    ws3.write_row(0, 0, hdr3, HDR); ws3.set_row(0, 24)
    ws3.set_column(0, 0, 6)
    for ci, w in enumerate(pk_w): ws3.set_column(ci+1, ci+1, min(w, 36))
    ws3.set_column(pk_n+1, pk_n+1, 32)
    for i, pk_dict in enumerate(ms_rows):
        ws3.write(i+1, 0, i+1, NUM)
        for ci, c in enumerate(pk_cols):
            ws3.write(i+1, ci+1, pk_dict.get(c, ""), PK)
        ws3.write(i+1, pk_n+1, "มีใน Source (MySQL 4) แต่ไม่มีใน Target (MySQL 8)", NOTE)

    # ── Sheet 4: Extra ───────────────────────────────────────────────────
    ws4 = wb.add_worksheet("🔵 Extra")
    ws4.hide_gridlines(2); ws4.freeze_panes(1, 0)
    hdr4 = ["#"] + pk_cols + ["หมายเหตุ"]
    ws4.write_row(0, 0, hdr4, HDR); ws4.set_row(0, 24)
    ws4.set_column(0, 0, 6)
    for ci, w in enumerate(pk_w): ws4.set_column(ci+1, ci+1, min(w, 36))
    ws4.set_column(pk_n+1, pk_n+1, 32)
    for i, pk_dict in enumerate(ex_rows):
        ws4.write(i+1, 0, i+1, NUM)
        for ci, c in enumerate(pk_cols):
            ws4.write(i+1, ci+1, pk_dict.get(c, ""), PK)
        ws4.write(i+1, pk_n+1, "ไม่มีใน Source (MySQL 4) แต่มีใน Target (MySQL 8)", NOTE)

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


if __name__ == "__main__":
    os.makedirs(os.path.join(os.path.dirname(os.path.abspath(__file__)), "static"), exist_ok=True)
    print("  ✓  Migration Validator Dashboard running at http://localhost:5001")
    app.run(host="0.0.0.0", port=5001, debug=False, threaded=True)
