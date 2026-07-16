#!/usr/bin/env python3
"""
Analyze which tables will show MIN/MAX vs row count in Timestamp Range & Row Count.
Rules:
  - MIN/MAX  : table has >= 1 date/time field whose type matches on both src & tgt (not orange)
  - ROW COUNT: table has no date/time fields at all, OR all date/time fields are type-mismatched
"""

import json, os, sys
import MySQLdb

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
with open(CONFIG_PATH) as f:
    cfg = json.load(f)

SRC = cfg["source"]
TGT = cfg["target"]
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mysql40 import MySQL40Connection

SYS_DBS  = {"information_schema", "performance_schema", "mysql", "sys"}
DATE_TYPES = ("timestamp", "datetime", "date")
TARGET_DBS = ["prs", "psims", "reference", "technical"]


def is_date_type(t):
    t = t.lower()
    return any(dt in t for dt in DATE_TYPES)


def get_schema_src(db):
    conn = MySQL40Connection(host=SRC["host"], port=int(SRC["port"]),
                             user=SRC["user"], password=SRC["password"],
                             database=db, timeout=60, charset="tis620")
    schema = {}
    for row in conn.query("SHOW TABLES"):
        tbl = row[0]
        schema[tbl] = {col[0]: col[1] for col in conn.query(f"SHOW COLUMNS FROM `{tbl}`")}
    return schema


def get_schema_tgt(db):
    conn = MySQLdb.connect(host=TGT["host"], port=int(TGT["port"]),
                           user=TGT["user"], passwd=TGT["password"],
                           db=db, charset="utf8mb4", connect_timeout=10)
    cur = conn.cursor()
    cur.execute("SHOW TABLES")
    tables = [r[0] for r in cur.fetchall()]
    schema = {}
    for tbl in tables:
        cur.execute(f"SHOW COLUMNS FROM `{tbl}`")
        schema[tbl] = {col[0]: col[1] for col in cur.fetchall()}
    cur.close(); conn.close()
    return schema


print(f"{'DB':<12} {'Table':<40} {'Mode':<12} {'Field(s) used'}")
print("-" * 100)

summary = {"minmax": 0, "rowcount": 0}

for db in TARGET_DBS:
    print(f"\n=== {db} ===")
    try:
        src = get_schema_src(db)
    except Exception as e:
        print(f"  src ERROR: {e}"); continue
    try:
        tgt = get_schema_tgt(db)
    except Exception as e:
        print(f"  tgt ERROR: {e}"); continue

    all_tables = sorted(set(list(src.keys()) + list(tgt.keys())))

    for tbl in all_tables:
        sf = src.get(tbl, {})
        tf = tgt.get(tbl, {})
        all_fields = list(dict.fromkeys(list(sf.keys()) + list(tf.keys())))

        matched_date_fields = []   # type matches on both sides
        mismatched_date_fields = []

        for field in all_fields:
            st = sf.get(field, "")
            tt = tf.get(field, "")
            if not is_date_type(st) and not is_date_type(tt):
                continue
            if st == tt:
                matched_date_fields.append(f"{field}({st})")
            else:
                mismatched_date_fields.append(f"{field}(src:{st}/tgt:{tt})")

        if matched_date_fields:
            mode = "MIN/MAX"
            detail = ", ".join(matched_date_fields[:3])
            summary["minmax"] += 1
        elif mismatched_date_fields:
            mode = "ROW COUNT*"  # has date fields but all mismatched
            detail = "all mismatch: " + ", ".join(mismatched_date_fields[:3])
            summary["rowcount"] += 1
        else:
            mode = "ROW COUNT"
            detail = "no date/time fields"
            summary["rowcount"] += 1

        print(f"  {tbl:<40} {mode:<12} {detail}")

print(f"\n{'='*100}")
print(f"Summary: MIN/MAX={summary['minmax']}  ROW COUNT={summary['rowcount']}")
