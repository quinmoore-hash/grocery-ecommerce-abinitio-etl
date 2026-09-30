#!/usr/bin/env bash
# run_daily_online_sales.sh - PySpark replacement for run/daily_online_sales.ksh
# Usage: run_daily_online_sales.sh <BUSINESS_DATE:YYYYMMDD>

set -euo pipefail

usage() { echo "usage: $(basename "$0") <YYYYMMDD>" >&2; exit 2; }

[ $# -eq 1 ] || usage
BUSINESS_DATE=$1
[[ $BUSINESS_DATE =~ ^[0-9]{8}$ ]] || usage
date -d "$BUSINESS_DATE" +%Y%m%d >/dev/null 2>&1 || { echo "invalid date: $BUSINESS_DATE" >&2; exit 2; }

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
export PROJECT_DIR=${PROJECT_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}
SPARK_SUBMIT=${SPARK_SUBMIT:-spark-submit}

IN=$PROJECT_DIR/data/in/online_orders_${BUSINESS_DATE}.dat
[ -f "$IN" ] || { echo "input not found: $IN" >&2; exit 1; }
mkdir -p "$PROJECT_DIR/data/out"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] START daily_online_sales BUSINESS_DATE=$BUSINESS_DATE"

"$SPARK_SUBMIT" ${SPARK_SUBMIT_OPTS:-} "$SCRIPT_DIR/daily_online_sales.py" \
  --business_date "$BUSINESS_DATE" \
  --project_dir "$PROJECT_DIR"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] wrote $PROJECT_DIR/data/out/daily_online_sales_${BUSINESS_DATE}.dat"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] END   daily_online_sales BUSINESS_DATE=$BUSINESS_DATE"
