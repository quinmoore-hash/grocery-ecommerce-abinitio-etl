#!/usr/bin/env bash
# run_daily_online_sales.sh - spark-submit wrapper for pyspark/daily_online_sales.py
# Replaces run/daily_online_sales.ksh (Ab Initio). Usage: run_daily_online_sales.sh <YYYYMMDD>
#
# Env:
#   PROJECT_DIR         repo/data root (default: parent of this script's dir)
#   SPARK_SUBMIT        spark-submit binary (default: spark-submit on PATH)
#   SPARK_MASTER        --master value (default: local[*])
#   SPARK_SUBMIT_ARGS   extra spark-submit args, e.g. "--deploy-mode client --conf k=v"
#   PYSPARK_PYTHON      python used by driver/executors (Spark default otherwise)

set -euo pipefail

BUSINESS_DATE=${1:?"usage: run_daily_online_sales.sh <YYYYMMDD>"}
shift

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export PROJECT_DIR=${PROJECT_DIR:-$(dirname "$SCRIPT_DIR")}
SPARK_SUBMIT=${SPARK_SUBMIT:-spark-submit}
SPARK_MASTER=${SPARK_MASTER:-local[*]}

# shellcheck disable=SC2086
exec "$SPARK_SUBMIT" \
  --master "$SPARK_MASTER" \
  --name "daily_online_sales_${BUSINESS_DATE}" \
  ${SPARK_SUBMIT_ARGS:-} \
  "$SCRIPT_DIR/daily_online_sales.py" \
  --business-date "$BUSINESS_DATE" \
  --project-dir "$PROJECT_DIR" \
  "$@"
