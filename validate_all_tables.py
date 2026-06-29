#!/usr/bin/env python3
"""
validate_all_tables.py — Generic multi-table validator for MySQL 4 → MySQL 8 migration.
Streams JSONL results compatible with the web dashboard.

Usage:
    python validate_all_tables.py --json [--db prs] [--skip-sync] [--sync-limit N]
"""

import sys
import json
import re
import threading
import MySQLdb
import MySQLdb.cursors
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

# ─── Config ───────────────────────────────────────────────────────────────────
import os as _os
_cfg_path = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "config.json")
try:
    with open(_cfg_path, "r", encoding="utf-8") as _f:
        _cfg = json.load(_f)
except Exception as _e:
    print(f"ERROR: cannot load config.json: {_e}", file=sys.stderr)
    sys.exit(1)

SOURCE_CFG: dict = _cfg.get("source", {})
TARGET_CFG: dict = _cfg.get("target", {})
if not TARGET_CFG:
    _tgts = _cfg.get("targets", [])
    if isinstance(_tgts, list) and len(_tgts) > 0:
        TARGET_CFG = _tgts[0]

# --target-name: select a specific target from targets[] array by name field
if "--target-name" in sys.argv:
    _tn_idx = sys.argv.index("--target-name")
    if _tn_idx + 1 < len(sys.argv):
        _tn = sys.argv[_tn_idx + 1]
        for _t in _cfg.get("targets", []):
            if _t.get("name") == _tn:
                TARGET_CFG = _t
                break

_tuning     = _cfg.get("tuning", {})
import os

JSON_MODE  = "--json"       in sys.argv
SKIP_SYNC       = "--skip-sync"       in sys.argv


DATABASE = "prs"
if "--db" in sys.argv:
    idx = sys.argv.index("--db")
    if idx + 1 < len(sys.argv):
        DATABASE = sys.argv[idx + 1]

SYNC_LIMIT = 0
if "--sync-limit" in sys.argv:
    idx = sys.argv.index("--sync-limit")
    if idx + 1 < len(sys.argv):
        SYNC_LIMIT = int(sys.argv[idx + 1])

# --tables TABLE1,TABLE2 — process only these tables (all checks including sync)
# --sync-tables TABLE1,TABLE2 — run full sync ONLY on these (skip fast checks for others)
TABLES_FILTER: set[str] = set()
SYNC_ONLY_MODE = False
if "--tables" in sys.argv:
    idx = sys.argv.index("--tables")
    if idx + 1 < len(sys.argv):
        TABLES_FILTER = set(sys.argv[idx + 1].split(","))
elif "--sync-tables" in sys.argv:
    idx = sys.argv.index("--sync-tables")
    if idx + 1 < len(sys.argv):
        TABLES_FILTER = set(sys.argv[idx + 1].split(","))
        SYNC_ONLY_MODE = True

DECIMAL_TOL   = Decimal(str(_tuning.get("decimal_tol", "0.0001")))
# decimal_round: round both sides to N decimal places before comparing
# (takes precedence over decimal_tol). None/absent = use tolerance.
_dr = _tuning.get("decimal_round")
DECIMAL_ROUND = int(_dr) if _dr is not None and str(_dr).strip() != "" else None
MAX_ERRORS    = int(_tuning.get("max_errors", 2000))
SRC_TIMEOUT   = int(_tuning.get("src_timeout", 300))
WORKERS       = int(_tuning.get("workers", 8))

# Data window: only validate rows inside a calendar-year range.
# Default: last N years from config (data_window_years, 0 = full table),
# i.e. rows >= Jan 1 of (current_year - N). Overridable per run with
# --year-from YYYY / --year-to YYYY (inclusive years).
import datetime as _datetime
DATA_WINDOW_YEARS = int(_tuning.get("data_window_years", 0))

def _cli_int(flag: str) -> int | None:
    if flag in sys.argv:
        idx = sys.argv.index(flag)
        if idx + 1 < len(sys.argv):
            try:
                return int(sys.argv[idx + 1])
            except ValueError:
                pass
    return None

_year_from  = _cli_int("--year-from")
_year_to    = _cli_int("--year-to")
_month_from = _cli_int("--month-from")
_month_to   = _cli_int("--month-to")
_day_from   = _cli_int("--day-from")
_day_to     = _cli_int("--day-to")

# Validate month range (1-12)
for _mflag, _mval in (("--month-from", _month_from), ("--month-to", _month_to)):
    if _mval is not None and not (1 <= _mval <= 12):
        print(f"Warning: {_mflag} {_mval} out of range (1-12), ignoring", file=sys.stderr)
        if _mflag == "--month-from":
            _month_from = None
        else:
            _month_to = None

# Validate day range (1-31); day only meaningful when matching month is set
for _dflag, _dval, _mval in (("--day-from", _day_from, _month_from), ("--day-to", _day_to, _month_to)):
    if _dval is not None and (not (1 <= _dval <= 31) or _mval is None):
        if _dflag == "--day-from":
            _day_from = None
        else:
            _day_to = None

def _month_start(year: int, month: int, day: int = 1) -> str:
    return f"{year}-{month:02d}-{day:02d}"

def _month_end_exclusive(year: int, month: int, day: int | None = None) -> str:
    """Exclusive upper bound: next day if day given, else first day of next month."""
    if day is not None:
        try:
            d = _datetime.date(year, month, day) + _datetime.timedelta(days=1)
            return d.isoformat()
        except ValueError:
            pass
    if month == 12:
        return f"{year + 1}-01-01"
    return f"{year}-{month + 1:02d}-01"

_today      = _datetime.date.today()
_base_year  = _today.year

if _year_from is not None:
    _yfrom = _year_from
elif DATA_WINDOW_YEARS > 0:
    _yfrom = _base_year - DATA_WINDOW_YEARS
else:
    _yfrom = None

if _yfrom is not None:
    WINDOW_START = _month_start(_yfrom, _month_from if _month_from else 1,
                                _day_from if _day_from else 1)
else:
    WINDOW_START = None

# Upper bound: always cap at today (exclusive) so today's incomplete data is
# never included. If user specified a past year/month the calculated end is
# already before today and the min() leaves it unchanged.
if _year_to is not None:
    _calc_end = _month_end_exclusive(_year_to, _month_to if _month_to else 12, _day_to)
    WINDOW_END = min(_calc_end, _today.isoformat())
else:
    # No explicit upper bound → default cap is today (exclude today's data).
    WINDOW_END = _today.isoformat()

def _window_label() -> str:
    """Human-readable range for summaries, e.g. '2024-03-01 → 2024-06-30'."""
    if WINDOW_START and WINDOW_END:
        # compute last day of window for display (WINDOW_END is exclusive)
        import datetime as _dt
        _end_excl = _dt.date.fromisoformat(WINDOW_END)
        _last_day = (_end_excl - _dt.timedelta(days=1)).isoformat()
        return f"{WINDOW_START} → {_last_day}"
    if WINDOW_START:
        return f"≥{WINDOW_START}"
    if WINDOW_END:
        import datetime as _dt
        _end_excl = _dt.date.fromisoformat(WINDOW_END)
        _last_day = (_end_excl - _dt.timedelta(days=1)).isoformat()
        return f"≤{_last_day}"
    return ""

_TS_TYPE_RE = re.compile(r"timestamp|datetime|^date")

# Load curated ts column config (same source as web's table-timestamp-info).
# Keys: db -> table -> column_name.  Generated from condition Numbers file.
_TS_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ts_field_config.json")
try:
    with open(_TS_CONFIG_PATH, "r", encoding="utf-8") as _f:
        _TS_FIELD_CONFIG: dict = json.load(_f)
except Exception:
    _TS_FIELD_CONFIG = {}

def _window_ts_col(entry: dict) -> str | None:
    """Return the column used to apply the year-range window for this table.

    Lookup order:
      1. ts_field_config.json (curated, matches web's MIN/MAX column)
      2. Fallback: first PK column with a date/datetime/timestamp type
    Returns None when no window is configured or no suitable column found.
    """
    if not (WINDOW_START or WINDOW_END):
        return None
    db    = entry.get("db", DATABASE)
    table = entry.get("table", "")
    # 1. curated config
    col = _TS_FIELD_CONFIG.get(db, {}).get(table)
    if col:
        return col
    # 2. fallback: first PK col with date/time type
    types = {r[0]: str(r[1]).lower() for r in entry.get("columns", [])}
    for c in entry.get("pk_cols", []):
        if _TS_TYPE_RE.search(types.get(c, "")):
            return c
    return None

def _window_cond(ts_col: str | None) -> str:
    """Bare condition (no WHERE keyword) limiting rows to the data window."""
    if not ts_col:
        return ""
    conds = []
    if WINDOW_START:
        conds.append(f"`{ts_col}` >= '{WINDOW_START}'")
    if WINDOW_END:
        conds.append(f"`{ts_col}` < '{WINDOW_END}'")
    return " AND ".join(conds)
PASS = "PASS"; FAIL = "FAIL"; WARN = "WARN"; SKIP = "SKIP"
_print_lock   = threading.Lock()   # serialise print() across worker threads


# ─── Emit helpers ────────────────────────────────────────────────────────────
def _emit(table: str, check: str, name: str, status: str, data: dict):
    if JSON_MODE:
        line = json.dumps({
            "type": "result", "table": table, "check": check,
            "name": name, "status": status, "data": data,
        })
    else:
        icon = {"PASS": "✓", "FAIL": "✗", "WARN": "!", "SKIP": "–"}.get(status, "?")
        line = f"  [{icon}] [{table}] {name}: {data.get('summary', '')}"
    with _print_lock:
        print(line, flush=True)


def _emit_table_start(table: str, src_rows: int, has_pk: bool, pk_cols: list[str]):
    if JSON_MODE:
        line = json.dumps({"type": "table_start", "table": table,
                           "src_rows": src_rows, "has_pk": has_pk, "pk_cols": pk_cols})
    else:
        pk_tag = f"PK={pk_cols if has_pk else 'no'}"
        line = f"\n  ── {table} ({src_rows:,} rows, {pk_tag})"
    with _print_lock:
        print(line, flush=True)


def _emit_progress(table: str, done: int, total: int):
    if JSON_MODE:
        with _print_lock:
            print(json.dumps({"type": "progress", "table": table,
                              "done": done, "total": total}), flush=True)


def _emit_meta(database: str, total: int, only_src: list, only_tgt: list):
    if JSON_MODE:
        print(json.dumps({
            "type": "meta",
            "database": database,
            "total_tables": total,
            "only_in_src": only_src,
            "only_in_tgt": only_tgt,
        }), flush=True)
    else:
        print(f"\n=== Validating database: {database} ({total} common tables) ===")
        if only_src:
            print(f"  Tables only in source: {only_src}")
        if only_tgt:
            print(f"  Tables only in target: {only_tgt}")


def _emit_done():
    if JSON_MODE:
        print(json.dumps({"type": "done", "exit_code": 0}), flush=True)


# ─── Connections ──────────────────────────────────────────────────────────────
def _connect_source(db: str):
    from mysql40 import MySQL40Connection
    return MySQL40Connection(
        host=SOURCE_CFG["host"],
        port=SOURCE_CFG["port"],
        user=SOURCE_CFG["user"],
        password=SOURCE_CFG["password"],
        database=db,
        timeout=SRC_TIMEOUT,
        charset="tis620",
    )


def _connect_target(db: str):
    if TARGET_CFG.get("version") == 4:
        from mysql40 import MySQL40Connection
        return MySQL40Connection(
            host=TARGET_CFG["host"],
            port=TARGET_CFG["port"],
            user=TARGET_CFG["user"],
            password=TARGET_CFG["password"],
            database=db,
            timeout=SRC_TIMEOUT,
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
        host=TARGET_CFG["host"],
        port=TARGET_CFG["port"],
        user=TARGET_CFG["user"],
        passwd=TARGET_CFG["password"],
        db=db,
        charset="utf8mb4",
        conv=my_conv,
    )


# ─── Discovery helpers ───────────────────────────────────────────────────────
def _src_tables(src) -> list[str]:
    return [r[0] for r in src.query("SHOW TABLES")]


def _tgt_tables(tgt) -> list[str]:
    tc = tgt.cursor()
    tc.execute("SHOW TABLES")
    result = [r[0] for r in tc.fetchall()]
    tc.close()
    return result


def _src_pk_cols(src, table: str) -> list[str]:
    rows = src.query(f"SHOW INDEX FROM `{table}`")
    pk = [(int(r[3]), r[4]) for r in rows if r[2] == "PRIMARY"]
    return [col for _, col in sorted(pk)]


def _tgt_pk_cols(tgt, table: str) -> list[str]:
    if TARGET_CFG.get("version") == 4:
        tc = tgt.cursor()
        tc.execute(f"SHOW INDEX FROM `{table}`")
        rows = tc.fetchall()
        pk = [(int(r[3]), r[4]) for r in rows if r[2] == "PRIMARY"]
        tc.close()
        return [col for _, col in sorted(pk)]

    tc = tgt.cursor()
    tc.execute(f"""
        SELECT column_name FROM information_schema.key_column_usage
        WHERE table_schema = %s AND table_name = %s AND constraint_name = 'PRIMARY'
        ORDER BY ordinal_position
    """, (DATABASE, table))
    result = [r[0] for r in tc.fetchall()]
    tc.close()
    return result


def _src_all_cols(src, table: str) -> list[str]:
    rows = src.query(f"SHOW COLUMNS FROM `{table}`")
    return [r[0] for r in rows]


def _tgt_all_cols(tgt, table: str) -> list[str]:
    if TARGET_CFG.get("version") == 4:
        tc = tgt.cursor()
        tc.execute(f"SHOW COLUMNS FROM `{table}`")
        rows = tc.fetchall()
        tc.close()
        return [r[0] for r in rows]

    tc = tgt.cursor()
    tc.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (DATABASE, table))
    result = [r[0] for r in tc.fetchall()]
    tc.close()
    return result


def _tgt_row_count(tgt, table: str, cond: str = "") -> int:
    tc = tgt.cursor()
    where = f" WHERE {cond}" if cond else ""
    tc.execute(f"SELECT COUNT(*) FROM `{table}`{where}")
    result = int(tc.fetchone()[0])
    tc.close()
    return result


def _src_row_count(src, table: str, cond: str = "") -> int:
    where = f" WHERE {cond}" if cond else ""
    rows = src.query(f"SELECT COUNT(*) FROM `{table}`{where}")
    return int(rows[0][0])


def _normalize_type(t: str) -> str:
    """Normalize MySQL type for loose comparison — MySQL 8 drops precision from double/float/int."""
    t = t.lower().strip()
    t = re.sub(r'\bdouble\(\d+,\d+\)\s*(unsigned)?', lambda m: 'double' + (' unsigned' if m.group(1) else ''), t)
    t = re.sub(r'\bfloat\(\d+,\d+\)\s*(unsigned)?', lambda m: 'float' + (' unsigned' if m.group(1) else ''), t)
    t = re.sub(r'\b(tinyint|smallint|mediumint|int|bigint)\(\d+\)', r'\1', t)
    return t


def _date_str(v) -> str:
    """Normalize date/datetime to consistent string for cross-DB comparison."""
    if isinstance(v, _datetime.datetime):
        return v.strftime("%Y-%m-%d") if (v.hour == v.minute == v.second == v.microsecond == 0) else v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, _datetime.date):
        return v.strftime("%Y-%m-%d")
    return str(v)


def _close(a, b) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    try:
        da, db = Decimal(_date_str(a)), Decimal(_date_str(b))
        if DECIMAL_ROUND is not None:
            q = Decimal(1).scaleb(-DECIMAL_ROUND)   # 10^-N, e.g. N=2 -> 0.01
            return da.quantize(q, rounding=ROUND_HALF_UP) == db.quantize(q, rounding=ROUND_HALF_UP)
        return abs(da - db) <= DECIMAL_TOL
    except InvalidOperation:
        return _date_str(a) == _date_str(b)


# ─── Schema cache (Option 2: batch pre-fetch) ────────────────────────────────
def _load_src_schema_cache(src, tables: list[str]) -> dict:
    """Pre-fetch SHOW COLUMNS + SHOW INDEX for all source tables sequentially."""
    cache: dict = {}
    for table in tables:
        try:
            col_rows = src.query(f"SHOW COLUMNS FROM `{table}`")
            idx_rows = src.query(f"SHOW INDEX FROM `{table}`")
        except Exception:
            col_rows, idx_rows = [], []
        pk = [(int(r[3]), r[4]) for r in idx_rows if r[2] == "PRIMARY"]
        cache[table] = {
            "table":    table,
            "columns":  col_rows,
            "indexes":  idx_rows,
            "pk_cols":  [c for _, c in sorted(pk)],
            "all_cols": [r[0] for r in col_rows],
        }
    return cache


def _load_tgt_schema_cache(tgt, db: str) -> tuple[dict, dict]:
    """Batch-load all columns + indexes from target in 2 queries (vs 94×3 before)."""
    if TARGET_CFG.get("version") == 4:
        col_cache = {}
        idx_cache = {}
        tc = tgt.cursor()
        tc.execute("SHOW TABLES")
        tables = [r[0] for r in tc.fetchall()]
        for table in tables:
            tc.execute(f"SHOW COLUMNS FROM `{table}`")
            col_cache[table] = [(r[0], r[1], r[2], r[3]) for r in tc.fetchall()]
            
            tc.execute(f"SHOW INDEX FROM `{table}`")
            idx_cache[table] = [(None, int(r[1]), r[2], int(r[3]), r[4]) for r in tc.fetchall()]
        tc.close()
        return col_cache, idx_cache

    tc = tgt.cursor()

    tc.execute("""
        SELECT table_name, column_name, column_type, is_nullable, column_key
        FROM information_schema.columns
        WHERE table_schema = %s
        ORDER BY table_name, ordinal_position
    """, (db,))
    col_cache: dict[str, list] = {}
    for tname, col_name, col_type, nullable, col_key in tc.fetchall():
        col_cache.setdefault(tname, []).append(
            (col_name, col_type, nullable, col_key))

    tc.execute("""
        SELECT table_name, non_unique, index_name, seq_in_index, column_name
        FROM information_schema.statistics
        WHERE table_schema = %s
        ORDER BY table_name, index_name, seq_in_index
    """, (db,))
    idx_cache: dict[str, list] = {}
    for tname, non_uniq, idx_name, seq, col_name in tc.fetchall():
        # store as (_, _, index_name, seq, col_name) — same shape as SHOW INDEX r[2..4]
        idx_cache.setdefault(tname, []).append(
            (None, non_uniq, idx_name, int(seq), col_name))

    tc.close()
    return col_cache, idx_cache


# ─── CHECK: Row Count ─────────────────────────────────────────────────────────
def check_row_count(table: str, src, tgt, ts_col: str | None = None):
    try:
        cond = _window_cond(ts_col)
        s = _src_row_count(src, table, cond)
        t = _tgt_row_count(tgt, table, cond)
        diff = s - t
        status = PASS if s == t else FAIL
        note = f"  (window: `{ts_col}` {_window_label()})" if cond else ""
        _emit(table, "row_count", "Row Count", status, {
            "summary": f"source={s:,}  target={t:,}  diff={diff:+,}{note}",
            "source": s, "target": t, "diff": diff,
            "window": _window_label() if cond else None,
        })
        return s
    except Exception as e:
        _emit(table, "row_count", "Row Count", SKIP, {"summary": str(e)})
        return 0


# ─── CHECK: Schema Diff ───────────────────────────────────────────────────────
def check_schema(table: str, src, tgt,
                 src_cache: dict | None = None,
                 tgt_col_cache: list | None = None):
    try:
        if src_cache is not None:
            raw = src_cache.get(table, {}).get("columns", [])
        else:
            raw = src.query(f"SHOW COLUMNS FROM `{table}`")
        src_cols = {r[0]: {"type": r[1], "null": r[2], "key": r[3]} for r in raw}

        if tgt_col_cache is not None:
            tgt_cols = {r[0]: {"type": r[1], "null": r[2], "key": r[3]}
                        for r in tgt_col_cache}
        else:
            tc = tgt.cursor()
            if TARGET_CFG.get("version") == 4:
                tc.execute(f"SHOW COLUMNS FROM `{table}`")
                tgt_cols = {r[0]: {"type": r[1], "null": r[2], "key": r[3]}
                            for r in tc.fetchall()}
            else:
                tc.execute("""
                    SELECT column_name, column_type, is_nullable, column_key
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (DATABASE, table))
                tgt_cols = {r[0]: {"type": r[1], "null": r[2], "key": r[3]}
                            for r in tc.fetchall()}
            tc.close()


        diffs = []
        warnings = []
        only_src = set(src_cols) - set(tgt_cols)
        only_tgt = set(tgt_cols) - set(src_cols)

        for col in only_src:
            diffs.append(f"{col}: missing in target")
        for col in only_tgt:
            diffs.append(f"{col}: extra in target")

        for col in set(src_cols) & set(tgt_cols):
            s_type = src_cols[col]["type"]
            t_type = tgt_cols[col]["type"]
            if _normalize_type(s_type) != _normalize_type(t_type):
                if s_type.lower().startswith("double") or s_type.lower().startswith("float"):
                    warnings.append(f"{col}: {s_type} → {t_type} (precision dropped, expected)")
                else:
                    diffs.append(f"{col}: type {s_type} vs {t_type}")

        if diffs:
            status = FAIL
            summary = f"{len(diffs)} diff(s): " + "; ".join(diffs[:3])
        elif warnings:
            status = WARN
            summary = f"{len(warnings)} type change(s) (double precision drift): " + "; ".join(warnings[:2])
        else:
            status = PASS
            summary = f"{len(src_cols)} columns all match"

        # Build ordered column list for full display
        all_col_names = list(src_cols.keys()) + [c for c in tgt_cols if c not in src_cols]
        diff_set = set()
        for d in diffs:
            col = d.split(":")[0].strip()
            diff_set.add(col)
        warn_set = set()
        for w in warnings:
            col = w.split(":")[0].strip()
            warn_set.add(col)

        columns = []
        for col in all_col_names:
            in_src = col in src_cols
            in_tgt = col in tgt_cols
            if not in_src:
                st = "extra"
            elif not in_tgt:
                st = "missing"
            elif col in diff_set:
                st = "mismatch"
            elif col in warn_set:
                st = "warning"
            else:
                st = "match"
            columns.append({
                "name": col,
                "src_type": src_cols[col]["type"] if in_src else None,
                "tgt_type": tgt_cols[col]["type"] if in_tgt else None,
                "status": st,
            })

        _emit(table, "schema", "Schema", status, {
            "summary": summary,
            "diffs": diffs,
            "warnings": warnings,
            "src_col_count": len(src_cols),
            "tgt_col_count": len(tgt_cols),
            "columns": columns,
        })
    except Exception as e:
        _emit(table, "schema", "Schema", SKIP, {"summary": str(e)})


# ─── CHECK: Index Coverage ────────────────────────────────────────────────────
def check_indexes(table: str, src, tgt,
                  src_cache: dict | None = None,
                  tgt_idx_cache: list | None = None):
    try:
        if src_cache is not None:
            src_raw = src_cache.get(table, {}).get("indexes", [])
        else:
            src_raw = src.query(f"SHOW INDEX FROM `{table}`")
        src_idx: dict[str, list] = {}
        for r in src_raw:
            name = r[2]; col = r[4]
            src_idx.setdefault(name, []).append((int(r[3]), col))
        src_idx = {k: [c for _, c in sorted(v)] for k, v in src_idx.items()}

        if tgt_idx_cache is not None:
            tgt_raw = tgt_idx_cache
        else:
            tc = tgt.cursor()
            tc.execute(f"SHOW INDEX FROM `{table}`")
            tgt_raw = tc.fetchall()
            tc.close()
        tgt_idx: dict[str, list] = {}
        for r in tgt_raw:
            name = r[2]; col = r[4]
            tgt_idx.setdefault(name, []).append((int(r[3]), col))
        tgt_idx = {k: [c for _, c in sorted(v)] for k, v in tgt_idx.items()}

        missing = [n for n in src_idx if n not in tgt_idx]
        mismatched = [
            n for n in src_idx
            if n in tgt_idx and src_idx[n] != tgt_idx[n]
        ]

        if missing or mismatched:
            status = FAIL
            parts = []
            if missing:
                parts.append(f"missing: {missing}")
            if mismatched:
                parts.append(f"column mismatch: {mismatched}")
            summary = "; ".join(parts)
        else:
            status = PASS
            summary = f"{len(src_idx)} index(es) all present"

        _emit(table, "indexes", "Index Coverage", status, {
            "summary": summary,
            "src_indexes": {k: v for k, v in src_idx.items()},
            "tgt_indexes": {k: v for k, v in tgt_idx.items()},
            "missing": missing,
            "mismatched": mismatched,
        })
    except Exception as e:
        _emit(table, "indexes", "Index Coverage", SKIP, {"summary": str(e)})


# ─── CHECK: Duplicate PK ──────────────────────────────────────────────────────
def check_duplicate_pk(table: str, pk_cols: list[str], src, tgt, src_rows: int = 0):
    # Since pk_cols represents the PRIMARY KEY, uniqueness is strictly enforced by the database constraint.
    # We can report PASS instantly without running slow, timeout-prone COUNT(DISTINCT) queries.
    _emit(table, "dup_pk", "Duplicate PK", PASS, {
        "summary": "enforced by PRIMARY KEY constraint",
        "src_dups": 0,
        "tgt_dups": 0,
    })


# ─── CHECK: Full Field Sync ───────────────────────────────────────────────────


# Keyset (seek) pagination chunk size. Each page is fetched with
# `ORDER BY pk LIMIT N`, which lets the DB walk the PK index and stop early
# instead of sorting the whole filtered set on disk (avoids tmpdir filesort).
KEYSET_CHUNK = int(_tuning.get("keyset_chunk_size", 50000))


def _sql_lit(v) -> str:
    """Quote a value as a MySQL string literal. PK columns are NOT NULL, so
    the column type drives comparison semantics (numeric col → numeric compare,
    char col → string compare), matching the DB's own ORDER BY."""
    if v is None:
        return "NULL"
    s = str(v)
    return "'" + s.replace("\\", "\\\\").replace("'", "''") + "'"


def _keyset_cond(pk_cols: list[str], last_vals: tuple,
                 pk_str_mask: list[bool] | None = None) -> str:
    """Row-value boundary for keyset pagination over a (possibly composite) PK.
    For PK (a,b,c) after (la,lb,lc):
      (a>la) OR (a=la AND b>lb) OR (a=la AND b=lb AND c>lc)
    pk_str_mask: True for string-type PK cols → use BINARY for byte-value ordering
    that matches Python str comparison (fixes collation divergence between MySQL 4
    latin1 byte order and MySQL 8 utf8mb4_0900_ai_ci Unicode DUCET order).
    """
    mask = pk_str_mask or [False] * len(pk_cols)

    def _col_expr(j):
        c = pk_cols[j]
        return f"BINARY `{c}`" if mask[j] else f"`{c}`"

    def _val_expr(j):
        lit = _sql_lit(last_vals[j])
        return f"BINARY {lit}" if mask[j] else lit

    ors = []
    for i in range(len(pk_cols)):
        eqs = [f"{_col_expr(j)} = {_val_expr(j)}" for j in range(i)]
        eqs.append(f"{_col_expr(i)} > {_val_expr(i)}")
        ors.append("(" + " AND ".join(eqs) + ")")
    return "(" + " OR ".join(ors) + ")"


_PREFIX_BATCH = 50000  # rows per prefix-discovery page


def _prefix_keyset_stream(fetch, table: str, pk_cols: list[str], all_cols: list[str],
                           prefix_cols: list[str], ts_cond: str,
                           chunk_size: int = KEYSET_CHUNK,
                           pk_str_mask: list[bool] | None = None):
    """Keyset stream partitioned by prefix columns.

    Used when ts_col is not the leading PK column (e.g. PK=(share,timestamp,symboltypeid)).
    Discovers distinct prefix values via keyset pagination (guaranteed O(distinct_values),
    not O(total_rows)), then for each prefix pins those columns and does a normal keyset
    stream — letting MySQL use the PK index for both the prefix equality seek AND the
    timestamp range scan.

    prefix_cols: pk_cols[:ts_col_idx] — the columns that appear before ts_col in PK.
    ts_cond: bare WHERE condition (no WHERE keyword) for the timestamp window.
    """
    mask = pk_str_mask or [False] * len(pk_cols)
    prefix_len = len(prefix_cols)
    prefix_mask = mask[:prefix_len]

    order_by = ", ".join(
        f"BINARY `{c}`" if mask[i] else f"`{c}`"
        for i, c in enumerate(prefix_cols)
    )
    col_list = ", ".join(f"`{c}`" for c in prefix_cols)

    # Keyset-paginate over distinct prefix values — walks the B-tree index directly,
    # guaranteed O(distinct_values) regardless of total row count.
    last_prefix = None
    while True:
        seek_conds = []
        if last_prefix is not None:
            seek_conds.append(_keyset_cond(prefix_cols, last_prefix, prefix_mask))
        where = f"WHERE {seek_conds[0]}" if seek_conds else ""
        batch = fetch(
            f"SELECT DISTINCT {col_list} FROM `{table}` {where} "
            f"ORDER BY {order_by} LIMIT {_PREFIX_BATCH}"
        )
        if not batch:
            break
        for prow in batch:
            conds = [f"`{prefix_cols[i]}` = {_sql_lit(prow[i])}" for i in range(prefix_len)]
            if ts_cond:
                conds.append(ts_cond)
            full_cond = " AND ".join(conds)
            yield from _keyset_stream(fetch, table, pk_cols, all_cols, full_cond,
                                       chunk_size=chunk_size, pk_str_mask=pk_str_mask)
        if len(batch) < _PREFIX_BATCH:
            break
        last_prefix = batch[-1]


def _keyset_stream(fetch, table: str, pk_cols: list[str], all_cols: list[str],
                   base_cond: str, chunk_size: int = KEYSET_CHUNK,
                   pk_str_mask: list[bool] | None = None):
    """Yield rows in PK order using keyset pagination, so the DB never has to
    filesort the whole result set to disk. `fetch(sql)` runs one page and
    returns a list of row tuples. `base_cond` is a bare WHERE condition
    (no WHERE keyword) or empty string.
    """
    mask = pk_str_mask or [False] * len(pk_cols)
    pk_indices = [all_cols.index(c) for c in pk_cols]
    order_by = ", ".join(
        f"BINARY `{c}`" if mask[i] else f"`{c}`"
        for i, c in enumerate(pk_cols)
    )
    col_list = ", ".join(f"`{c}`" for c in all_cols)
    last = None
    while True:
        conds = []
        if base_cond:
            conds.append(base_cond)
        if last is not None:
            conds.append(_keyset_cond(pk_cols, last, mask))
        where = ("WHERE " + " AND ".join(conds)) if conds else ""
        sql = (f"SELECT {col_list} FROM `{table}` {where} "
               f"ORDER BY {order_by} LIMIT {chunk_size}")
        rows = fetch(sql)
        if not rows:
            break
        for r in rows:
            yield r
        if len(rows) < chunk_size:
            break
        last = tuple(rows[-1][i] for i in pk_indices)


def _stream_compare(table: str, pk_cols: list[str], all_cols: list[str],
                     src, where: str, src_rows_est: int,
                     window_label: str | None = None,
                     ts_col: str | None = None,
                     tgt_conn=None) -> dict:
    """Two-pointer merge on filtered rows. Returns result dict.

    Accepts an optional `tgt_conn` so the caller can pass the connection it
    already has open — this avoids opening a redundant 3rd concurrent
    connection to MySQL 4, which blocks when max_user_connections is limited.
    When `tgt_conn` is None a fresh target connection is opened here and
    closed in the finally block; when provided the caller owns its lifecycle.

    Both sides are read with keyset pagination (ORDER BY pk LIMIT N per page)
    so neither DB filesorts the whole set to its tmpdir.
    Also appends all discrepancies to a dynamic local CSV report.
    """
    import csv
    import os

    reports_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "reports")
    os.makedirs(reports_dir, exist_ok=True)
    _tname_sfx = f"_{TARGET_CFG.get('name')}" if TARGET_CFG.get("name") else ""
    csv_path  = os.path.join(reports_dir, f"{table}{_tname_sfx}_mismatches.csv")
    ndjson_path = os.path.join(reports_dir, f"{table}{_tname_sfx}_full_compare.ndjson")
    csv_file = None
    csv_writer = None
    ndjson_file = None

    try:
        csv_file = open(csv_path, "w", encoding="utf-8", newline="")
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["Type", "Primary Key", "Column Name", "Source Value", "Target Value"])
    except Exception as e:
        print(f"Warning: Failed to create mismatch report file: {e}", file=sys.stderr)

    try:
        ndjson_file = open(ndjson_path, "w", encoding="utf-8")
    except Exception as e:
        print(f"Warning: Failed to create full compare file: {e}", file=sys.stderr)

    # Bare condition (strip the WHERE keyword) so keyset can AND it with the
    # seek boundary on each page.
    base_cond = where[6:].strip() if where[:5].upper() == "WHERE" else where

    # Reuse the caller's tgt connection if provided (avoids a 3rd concurrent
    # connection to MySQL 4 which blocks when max_user_connections is limited).
    _owns_tgt = tgt_conn is None

    if TARGET_CFG.get("version") == 4:
        # ── MySQL 4 target: fresh-connection-per-chunk fetch functions ──────────
        # Python generators are single-threaded: src chunk is fetched, connection
        # closed, then tgt chunk is fetched, connection closed.  This guarantees
        # we never hold two MySQL 4 connections open simultaneously.
        def _src_fetch(sql):
            c = _connect_source(DATABASE)
            try:
                return c.query(sql)
            finally:
                c.close()

        def _tgt_fetch(sql):
            c = _connect_target(DATABASE)
            try:
                cur = c.cursor()
                cur.execute(sql)
                result = cur.fetchall()
                cur.close()
                return result
            finally:
                c.close()

        # No BINARY for 4vs4: same charset/collation on both sides, and BINARY
        # prevents MySQL 4 from using the B-tree PK index (see comment below).
        pk_str_mask = [False] * len(pk_cols)
    else:
        # ── Normal mode: use persistent tgt_conn (MySQL 8 target) ─────────────
        if _owns_tgt:
            tgt_conn = _connect_target(DATABASE)

        try:
            setup = tgt_conn.cursor()
            try:
                setup.execute("SET SESSION net_write_timeout = 3600")
                setup.execute("SET SESSION net_read_timeout  = 3600")
                setup.execute("SET SESSION sort_buffer_size = 268435456")
            except Exception:
                pass
            setup.close()
        except Exception:
            pass

        def _tgt_fetch(sql):
            cur = tgt_conn.cursor()
            try:
                cur.execute(sql)
                return cur.fetchall()
            finally:
                cur.close()

        # Detect string-type PK columns so both streams use BINARY ORDER BY,
        # matching Python's byte-value str comparison. Fixes collation divergence:
        # MySQL 4 latin1 byte order vs MySQL 8 utf8mb4_0900_ai_ci DUCET order
        # (e.g. '-' sorts before '&' in MySQL 8 but after in MySQL 4 / Python).
        _str_type_keywords = ("char", "text", "enum", "set", "varchar", "binary", "blob")
        try:
            src_col_rows = src.query(f"SHOW COLUMNS FROM `{table}`")
            src_col_type_map = {r[0]: r[1].lower() for r in src_col_rows}
        except Exception:
            src_col_type_map = {}
        pk_str_mask = [
            any(kw in src_col_type_map.get(c, "") for kw in _str_type_keywords)
            for c in pk_cols
        ]
        def _src_fetch(sql):
            return src.query(sql)

    try:
        # If ts_col is not the leading PK column and there is a window condition,
        # use prefix-partitioned streaming: pin the columns before ts_col via
        # DISTINCT lookup so MySQL can use the PK index for the timestamp range.
        ts_col_idx = pk_cols.index(ts_col) if (ts_col and ts_col in pk_cols and base_cond) else -1
        if ts_col_idx > 0:
            prefix_cols = pk_cols[:ts_col_idx]
            src_gen = _prefix_keyset_stream(_src_fetch, table, pk_cols, all_cols,
                                             prefix_cols, base_cond,
                                             pk_str_mask=pk_str_mask)
            tgt_gen = _prefix_keyset_stream(_tgt_fetch, table, pk_cols, all_cols,
                                             prefix_cols, base_cond,
                                             pk_str_mask=pk_str_mask)
        else:
            src_gen = _keyset_stream(_src_fetch, table, pk_cols, all_cols, base_cond,
                                     pk_str_mask=pk_str_mask)
            tgt_gen = _keyset_stream(_tgt_fetch, table, pk_cols, all_cols, base_cond,
                                     pk_str_mask=pk_str_mask)

        pk_n = len(pk_cols)
        col_idx = {col: i for i, col in enumerate(all_cols)}
        pk_indices = [col_idx[c] for c in pk_cols]  # actual positions of PK cols in SELECT result
        pk_idx_set = set(pk_indices)
        def pk_of(row):
            return tuple("" if row[i] is None else str(row[i]) for i in pk_indices)

        # Comparison key: numerics compared as numbers so '4702745699' (MySQL 4
        # string) == '4702745699.0' (MySQL 8 float) and ordering matches the
        # DB's ORDER BY ('9' < '10' numerically, not as strings). Rank prefix
        # keeps tuple comparison type-safe: NULL < numeric < string.
        def _pk_norm(v):
            if v is None:
                return (0, "")
            # Normalize date/datetime to consistent string so MySQL 4.0
            # (returns datetime.datetime for DATE cols) matches MySQL 8
            # (returns datetime.date). Strip zero time to "YYYY-MM-DD".
            if isinstance(v, _datetime.datetime):
                s = v.strftime("%Y-%m-%d") if (v.hour == v.minute == v.second == v.microsecond == 0) else v.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(v, _datetime.date):
                s = v.strftime("%Y-%m-%d")
            else:
                s = str(v)
            try:
                return (1, Decimal(s))
            except InvalidOperation:
                return (2, s)
        def pk_key(row):
            return tuple(_pk_norm(row[i]) for i in pk_indices)

        total_src = total_tgt = mismatches = missing = extra = 0
        samples: list[dict] = []
        last_emit = 0

        s_row = next(src_gen, None)
        t_row = next(tgt_gen, None)

        while s_row is not None or t_row is not None:
            if s_row is not None and t_row is not None:
                s_pk, t_pk = pk_of(s_row), pk_of(t_row)
                s_key, t_key = pk_key(s_row), pk_key(t_row)
                if s_key == t_key:
                    total_src += 1; total_tgt += 1
                    diffs = [f"{all_cols[i]}: {s_row[i]} vs {t_row[i]}"
                             for i in range(len(all_cols))
                             if i not in pk_idx_set and not _close(s_row[i], t_row[i])]
                    pk_dict = {pk_cols[j]: s_pk[j] for j in range(pk_n)}
                    pk_str = ", ".join(f"{k}={v}" for k, v in pk_dict.items())
                    if diffs:
                        mismatches += 1
                        fields = []
                        for i, col in enumerate(all_cols):
                            sv = s_row[i]
                            tv = t_row[i]
                            is_match = _close(sv, tv)
                            fields.append({
                                "column": col,
                                "source": str(sv) if sv is not None else None,
                                "target": str(tv) if tv is not None else None,
                                "match": is_match,
                            })
                            if not is_match and csv_writer:
                                csv_writer.writerow([
                                    "mismatch",
                                    pk_str,
                                    col,
                                    str(sv) if sv is not None else "NULL",
                                    str(tv) if tv is not None else "NULL"
                                ])
                        if ndjson_file:
                            ndjson_file.write(json.dumps({
                                "type": "mismatch", "pk": pk_str,
                                "cols": all_cols,
                                "src": [str(s_row[i]) if s_row[i] is not None else None for i in range(len(all_cols))],
                                "tgt": [str(t_row[i]) if t_row[i] is not None else None for i in range(len(all_cols))],
                                "diffs": [i not in pk_idx_set and not _close(s_row[i], t_row[i]) for i in range(len(all_cols))],
                            }, ensure_ascii=False) + "\n")
                        if len(samples) < MAX_ERRORS:
                            samples.append({
                                "type": "mismatch",
                                "status": "mismatch",
                                "pk": pk_dict,
                                "fields": fields,
                            })
                    else:
                        if ndjson_file:
                            ndjson_file.write(json.dumps({
                                "type": "match", "pk": pk_str,
                                "cols": all_cols,
                                "src": [str(s_row[i]) if s_row[i] is not None else None for i in range(len(all_cols))],
                                "tgt": [str(t_row[i]) if t_row[i] is not None else None for i in range(len(all_cols))],
                                "diffs": [False] * len(all_cols),
                            }, ensure_ascii=False) + "\n")
                    s_row = next(src_gen, None)
                    t_row = next(tgt_gen, None)
                elif s_key < t_key:
                    total_src += 1; missing += 1
                    pk_dict = {pk_cols[j]: s_pk[j] for j in range(pk_n)}
                    pk_str = ", ".join(f"{k}={v}" for k, v in pk_dict.items())
                    if csv_writer:
                        csv_writer.writerow(["missing", pk_str, "ALL", "Present in Source", "Missing in Target"])
                    if ndjson_file:
                        ndjson_file.write(json.dumps({
                            "type": "missing", "pk": pk_str,
                            "cols": all_cols,
                            "src": [str(s_row[i]) if s_row[i] is not None else None for i in range(len(all_cols))],
                        }, ensure_ascii=False) + "\n")
                    if len(samples) < MAX_ERRORS:
                        fields = []
                        for i, col in enumerate(all_cols):
                            sv = s_row[i]
                            fields.append({
                                "column": col,
                                "source": str(sv) if sv is not None else None,
                                "target": None,
                                "match": False,
                            })
                        samples.append({"type": "missing", "status": "missing", "pk": pk_dict, "fields": fields})
                    s_row = next(src_gen, None)
                else:
                    total_tgt += 1; extra += 1
                    pk_dict = {pk_cols[j]: t_pk[j] for j in range(pk_n)}
                    pk_str = ", ".join(f"{k}={v}" for k, v in pk_dict.items())
                    if csv_writer:
                        csv_writer.writerow(["extra", pk_str, "ALL", "Missing in Source", "Extra in Target"])
                    if ndjson_file:
                        ndjson_file.write(json.dumps({
                            "type": "extra", "pk": pk_str,
                            "cols": all_cols,
                            "tgt": [str(t_row[i]) if t_row[i] is not None else None for i in range(len(all_cols))],
                        }, ensure_ascii=False) + "\n")
                    if len(samples) < MAX_ERRORS:
                        fields = []
                        for i, col in enumerate(all_cols):
                            tv = t_row[i]
                            fields.append({
                                "column": col,
                                "source": None,
                                "target": str(tv) if tv is not None else None,
                                "match": False,
                            })
                        samples.append({"type": "extra", "status": "extra", "pk": pk_dict, "fields": fields})
                    t_row = next(tgt_gen, None)
            elif s_row is not None:
                total_src += 1; missing += 1
                s_pk_val = pk_of(s_row)
                pk_dict = {pk_cols[j]: s_pk_val[j] for j in range(pk_n)}
                pk_str = ", ".join(f"{k}={v}" for k, v in pk_dict.items())
                if csv_writer:
                    csv_writer.writerow(["missing", pk_str, "ALL", "Present in Source", "Missing in Target"])
                if ndjson_file:
                    ndjson_file.write(json.dumps({
                        "type": "missing", "pk": pk_str,
                        "cols": all_cols,
                        "src": [str(s_row[i]) if s_row[i] is not None else None for i in range(len(all_cols))],
                    }, ensure_ascii=False) + "\n")
                if len(samples) < MAX_ERRORS:
                    fields = []
                    for i, col in enumerate(all_cols):
                        sv = s_row[i]
                        fields.append({
                            "column": col,
                            "source": str(sv) if sv is not None else None,
                            "target": None,
                            "match": False,
                        })
                    samples.append({"type": "missing", "status": "missing", "pk": pk_dict, "fields": fields})
                s_row = next(src_gen, None)
            else:
                total_tgt += 1; extra += 1
                t_pk_val = pk_of(t_row)
                pk_dict = {pk_cols[j]: t_pk_val[j] for j in range(pk_n)}
                pk_str = ", ".join(f"{k}={v}" for k, v in pk_dict.items())
                if csv_writer:
                    csv_writer.writerow(["extra", pk_str, "ALL", "Missing in Source", "Extra in Target"])
                if ndjson_file:
                    ndjson_file.write(json.dumps({
                        "type": "extra", "pk": pk_str,
                        "cols": all_cols,
                        "tgt": [str(t_row[i]) if t_row[i] is not None else None for i in range(len(all_cols))],
                    }, ensure_ascii=False) + "\n")
                if len(samples) < MAX_ERRORS:
                    fields = []
                    for i, col in enumerate(all_cols):
                        tv = t_row[i]
                        fields.append({
                            "column": col,
                            "source": None,
                            "target": str(tv) if tv is not None else None,
                            "match": False,
                        })
                    samples.append({
                        "type": "extra",
                        "status": "extra",
                        "pk": pk_dict,
                        "fields": fields,
                    })
                t_row = next(tgt_gen, None)

            matched = total_src + total_tgt - (missing + extra)
            if matched - last_emit >= 1_000_000:
                last_emit = matched
                _emit_progress(table, matched, src_rows_est)

        matched = total_src - missing - mismatches
        meta = {
            "all": matched + mismatches + missing + extra,
            "match": matched,
            "mismatch": mismatches,
            "missing": missing,
            "extra": extra,
            "window": window_label,
        }
        try:
            meta_path = os.path.join(reports_dir, f"{table}{_tname_sfx}_compare_meta.json")
            with open(meta_path, "w", encoding="utf-8") as mf:
                json.dump(meta, mf)
        except Exception:
            pass
        return {
            "src_rows": total_src, "tgt_rows": total_tgt,
            "matched": matched, "mismatches": mismatches,
            "missing_in_tgt": missing, "extra_in_tgt": extra,
            "samples": samples[:MAX_ERRORS],
        }
    finally:
        if csv_file:
            try: csv_file.close()
            except: pass
        if ndjson_file:
            try: ndjson_file.close()
            except: pass
        # Only close tgt_conn if we opened it; caller manages its own connection.
        if _owns_tgt:
            try: tgt_conn.close()
            except Exception: pass


def check_full_sync(table: str, pk_cols: list[str], all_cols: list[str],
                    src, tgt, src_rows_est: int, ts_col: str | None = None):
    if SKIP_SYNC:
        _emit(table, "full_sync", "Full Field Sync", SKIP, {"summary": "skipped (--skip-sync)"})
        return
    if SYNC_LIMIT and src_rows_est > SYNC_LIMIT:
        _emit(table, "full_sync", "Full Field Sync", SKIP, {
            "summary": f"skipped: {src_rows_est:,} rows > limit {SYNC_LIMIT:,}",
        })
        return

    try:
        window   = _window_cond(ts_col)
        win_note = f", window {_window_label()}" if window else ""
        where    = f"WHERE {window}" if window else ""

        result = _stream_compare(table, pk_cols, all_cols, src, where, src_rows_est,
                                 window_label=_window_label() if window else None,
                                 ts_col=ts_col,
                                 tgt_conn=tgt)

        has_errors = (result["mismatches"] > 0 or
                      result["missing_in_tgt"] > 0 or
                      result["extra_in_tgt"] > 0)
        status = FAIL if has_errors else PASS

        matched = result["matched"]
        _emit(table, "full_sync", "Full Field Sync", status, {
            "summary": (
                f"src={result['src_rows']:,}  tgt={result['tgt_rows']:,}  "
                f"matched={matched:,}  mismatch={result['mismatches']:,}  "
                f"missing={result['missing_in_tgt']:,}  extra={result['extra_in_tgt']:,}"
                f"{win_note}"
            ),
            **result, "method": "stream",
            "window": _window_label() if window else None,
        })

    except Exception as e:
        _emit(table, "full_sync", "Full Field Sync", SKIP, {"summary": str(e)})


# ─── Main ─────────────────────────────────────────────────────────────────────
def _process_table(table: str, src_cache: dict,
                    tgt_col_cache: dict, tgt_idx_cache: dict):
    """Worker: process one table with its own DB connections."""
    entry   = src_cache.get(table, {})
    pk_cols = entry.get("pk_cols", [])
    all_cols= entry.get("all_cols", [])

    # Filter out ignored columns, but ALWAYS preserve PK columns
    ignored = _cfg.get("ignore_fields", {}).get(DATABASE, {}).get(table, [])
    all_cols = [c for c in all_cols if c not in ignored or c in pk_cols]

    has_pk  = bool(pk_cols)
    ts_col  = _window_ts_col(entry)   # None → no window, check full table

    if TARGET_CFG.get("version") == 4:
        # ── Sequential mode for MySQL 4 target ──────────────────────────────
        # Never hold src and tgt connections open at the same time; MySQL 4
        # enforces per-user connection limits and the 2nd connection blocks
        # indefinitely when the 1st is still active on the same server/user.
        cond = _window_cond(ts_col)

        # Phase 1: source row count
        src = _connect_source(DATABASE)
        src_rows = 0
        try:
            src_rows = _src_row_count(src, table, cond)
        except Exception:
            pass
        finally:
            src.close()

        _emit_table_start(table, src_rows, has_pk, pk_cols)

        if SYNC_ONLY_MODE:
            if has_pk:
                check_full_sync(table, pk_cols, all_cols, None, None, src_rows, ts_col)
            else:
                _emit(table, "full_sync", "Full Field Sync", SKIP,
                      {"summary": "no primary key"})
            return

        # Phase 2: target row count
        tgt_rows = 0
        tgt = _connect_target(DATABASE)
        try:
            tgt_rows = _tgt_row_count(tgt, table, cond)
        except Exception:
            pass
        finally:
            tgt.close()

        # Emit row count result
        diff = src_rows - tgt_rows
        status = PASS if src_rows == tgt_rows else FAIL
        win_note = f"  (window: `{ts_col}` {_window_label()})" if cond else ""
        _emit(table, "row_count", "Row Count", status, {
            "summary": f"source={src_rows:,}  target={tgt_rows:,}  diff={diff:+,}{win_note}",
            "source": src_rows, "target": tgt_rows, "diff": diff,
            "window": _window_label() if cond else None,
        })
        actual = src_rows

        # Schema and index checks use the pre-fetched caches — no connection needed
        check_schema(table, None, None,
                     src_cache=src_cache,
                     tgt_col_cache=tgt_col_cache.get(table))
        check_indexes(table, None, None,
                      src_cache=src_cache,
                      tgt_idx_cache=tgt_idx_cache.get(table))

        if has_pk:
            # dup_pk: instant PASS (enforced by DB constraint), no connection needed
            check_duplicate_pk(table, pk_cols, None, None, actual)
            # Phase 3: full sync with fresh-per-chunk connections
            check_full_sync(table, pk_cols, all_cols, None, None, actual, ts_col)
        else:
            _emit(table, "dup_pk",    "Duplicate PK",    SKIP,
                  {"summary": "no primary key"})
            _emit(table, "full_sync", "Full Field Sync", SKIP,
                  {"summary": "no primary key"})
        return

    # ── Normal mode (target is MySQL 8 or non-MySQL-4) ───────────────────────
    # Count(*) still needs a live connection — fast for MyISAM
    src = _connect_source(DATABASE)
    tgt = _connect_target(DATABASE)
    try:
        src_rows = 0
        try:
            src_rows = _src_row_count(src, table, _window_cond(ts_col))
        except Exception:
            pass

        _emit_table_start(table, src_rows, has_pk, pk_cols)

        if SYNC_ONLY_MODE:
            if has_pk:
                check_full_sync(table, pk_cols, all_cols, src, tgt, src_rows, ts_col)
            else:
                _emit(table, "full_sync", "Full Field Sync", SKIP,
                      {"summary": "no primary key"})
        else:
            actual = check_row_count(table, src, tgt, ts_col)
            check_schema(table, src, tgt,
                         src_cache=src_cache,
                         tgt_col_cache=tgt_col_cache.get(table))
            check_indexes(table, src, tgt,
                          src_cache=src_cache,
                          tgt_idx_cache=tgt_idx_cache.get(table))
            if has_pk:
                check_duplicate_pk(table, pk_cols, src, tgt, actual or src_rows)
                check_full_sync(table, pk_cols, all_cols, src, tgt, actual or src_rows, ts_col)
            else:
                _emit(table, "dup_pk",    "Duplicate PK",    SKIP,
                      {"summary": "no primary key"})
                _emit(table, "full_sync", "Full Field Sync", SKIP,
                      {"summary": "no primary key"})
    finally:
        try: src.close()
        except Exception: pass
        try: tgt.close()
        except Exception: pass


def main():
    if TARGET_CFG.get("version") == 4:
        # ── Sequential connection mode for MySQL 4 target ─────────────────────
        # Never hold src and tgt connections open at the same time; MySQL 4
        # blocks the 2nd connection when max_user_connections is hit.
        # Phase A: source table list
        src = _connect_source(DATABASE)
        src_table_set = set(_src_tables(src))
        src.close()

        # Phase B: target table list + schema cache
        tgt = _connect_target(DATABASE)
        tgt_table_set = set(_tgt_tables(tgt))
        only_src = sorted(src_table_set - tgt_table_set)
        only_tgt = sorted(tgt_table_set - src_table_set)
        common   = sorted(src_table_set & tgt_table_set)
        if TABLES_FILTER:
            common = [t for t in common if t in TABLES_FILTER]
        _emit_meta(DATABASE, len(common), only_src, only_tgt)
        tgt_col_cache, tgt_idx_cache = _load_tgt_schema_cache(tgt, DATABASE)
        tgt.close()

        # Phase C: source schema cache
        src = _connect_source(DATABASE)
        src_cache = _load_src_schema_cache(src, common)
        src.close()
    else:
        # ── Normal mode (target is MySQL 8) ───────────────────────────────────
        src = _connect_source(DATABASE)
        tgt = _connect_target(DATABASE)

        src_table_set = set(_src_tables(src))
        tgt_table_set = set(_tgt_tables(tgt))
        only_src = sorted(src_table_set - tgt_table_set)
        only_tgt = sorted(tgt_table_set - src_table_set)
        common   = sorted(src_table_set & tgt_table_set)

        if TABLES_FILTER:
            common = [t for t in common if t in TABLES_FILTER]

        _emit_meta(DATABASE, len(common), only_src, only_tgt)

        # ── Option 2: batch pre-fetch target schema (2 queries total) ──────────────
        tgt_col_cache, tgt_idx_cache = _load_tgt_schema_cache(tgt, DATABASE)

        # ── Option 2: pre-fetch source schema sequentially (SHOW COLUMNS/INDEX × N) ─
        src_cache = _load_src_schema_cache(src, common)

        src.close()
        tgt.close()

    # ── Option 1: parallel table processing ───────────────────────────────────
    # Use multiple workers for fast-check mode; sequential for sync (avoids
    # overwhelming MySQL 4 with parallel streaming cursors).
    workers = 1 if (SYNC_ONLY_MODE or not SKIP_SYNC) else WORKERS

    if workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {
                ex.submit(_process_table, t, src_cache,
                          tgt_col_cache, tgt_idx_cache): t
                for t in common
            }
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    table = futures[future]
                    for chk, nm in [("row_count","Row Count"),("schema","Schema"),
                                    ("indexes","Index Coverage"),("dup_pk","Duplicate PK"),
                                    ("full_sync","Full Field Sync")]:
                        _emit(table, chk, nm, SKIP, {"summary": str(e)})
    else:
        for table in common:
            try:
                _process_table(table, src_cache, tgt_col_cache, tgt_idx_cache)
            except Exception as e:
                for chk, nm in [("row_count","Row Count"),("schema","Schema"),
                                ("indexes","Index Coverage"),("dup_pk","Duplicate PK"),
                                ("full_sync","Full Field Sync")]:
                    _emit(table, chk, nm, SKIP, {"summary": str(e)})

    _emit_done()


if __name__ == "__main__":
    main()
