#!/usr/bin/env bash
set -euo pipefail

# --- Locate repo root (this file lives in scripts/) ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# --- Optional: activate venv if present ---
if [[ -d "$ROOT/.venv" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT/.venv/bin/activate"
fi

# --- Paths (override via env if you like) ---
BRONZE="${BRONZE:-$ROOT/data/bronze/crypto_trades}"
SILVER="${SILVER:-$ROOT/data/silver/trades}"
GOLD="${GOLD:-$ROOT/data/gold/bars_1m}"

# Output mode: set JSON=1 for JSON output
JSON="${JSON:-0}"

# --- Run a tiny Python helper with Spark to count rows ---
python - "$BRONZE" "$SILVER" "$GOLD" "$JSON" <<'PY'
import sys, json
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

bronze, silver, gold, json_flag = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
spark = SparkSession.builder.appName("count-rows").getOrCreate()

def count(path, name):
    p = Path(path)
    if not p.exists():
        return {"name": name, "path": str(p), "rows": 0, "exists": False}
    df = spark.read.parquet(str(p))
    out = {"name": name, "path": str(p), "rows": df.count(), "exists": True}
    if name == "gold" and "bar_start" in df.columns and out["rows"] > 0:
        mn, mx = df.agg(F.min("bar_start"), F.max("bar_start")).first()
        out["start"] = str(mn); out["end"] = str(mx)
    return out

res = [count(bronze, "bronze"), count(silver, "silver"), count(gold, "gold")]

if json_flag in ("1","true","True"):
    print(json.dumps(res, indent=2))
else:
    for r in res:
        line = f"{r['name']:6} rows: {r['rows']:>6}"
        if not r["exists"]:
            line += "   [MISSING]"
        if r["name"] == "gold" and r.get("start"):
            line += f"   [{r['start']} \u2192 {r['end']}]"
        print(line)

spark.stop()
PY
