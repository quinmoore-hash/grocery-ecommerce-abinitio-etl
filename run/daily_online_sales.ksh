#!/bin/ksh
# daily_online_sales.ksh - deployed script for mp/daily_online_sales.mp
# Usage: daily_online_sales.ksh <BUSINESS_DATE:YYYYMMDD>

set -e
set -o pipefail

: ${AB_HOME:?"AB_HOME not set"}
: ${PROJECT_DIR:?"PROJECT_DIR not set - source sand/project.pset first"}
export PATH=$AB_HOME/bin:$PATH

BUSINESS_DATE=${1:?"usage: daily_online_sales.ksh <YYYYMMDD>"}
export BUSINESS_DATE

IN=$PROJECT_DIR/data/in/online_orders_${BUSINESS_DATE}.dat
DIM=$PROJECT_DIR/data/serial/product_dim.dat
OUT=$PROJECT_DIR/data/out/daily_online_sales_${BUSINESS_DATE}.dat
REJECT=$PROJECT_DIR/data/out/reject_${BUSINESS_DATE}.dat
TMP=$PROJECT_DIR/data/serial
mkdir -p $PROJECT_DIR/data/out

echo "[$(date '+%Y-%m-%d %H:%M:%S')] START daily_online_sales BUSINESS_DATE=$BUSINESS_DATE"

# 1. REFORMAT: cleanse + select is_valid_order
m_dump $PROJECT_DIR/dml/online_order.dml $IN \
  | m_eval "$PROJECT_DIR/xfr/order_cleanse.xfr" \
  > $TMP/cleansed_${BUSINESS_DATE}.dat

# 2. SORT on sku, JOIN to product_dim (unmatched -> reject)
m_sort -key sku $TMP/cleansed_${BUSINESS_DATE}.dat > $TMP/cleansed_sorted_${BUSINESS_DATE}.dat

m_join \
  -input0 $TMP/cleansed_sorted_${BUSINESS_DATE}.dat -key0 sku \
  -input1 $DIM                                      -key1 sku \
  -join-type inner \
  -transform $PROJECT_DIR/xfr/product_join.xfr \
  -unused0 $REJECT \
  > $TMP/joined_${BUSINESS_DATE}.dat

# 3. SORT on rollup key, ROLLUP to daily grain
m_sort -key "business_date store_id fulfillment_type department order_id" \
  $TMP/joined_${BUSINESS_DATE}.dat > $TMP/joined_sorted_${BUSINESS_DATE}.dat

m_rollup \
  -key "business_date store_id fulfillment_type department" \
  -transform $PROJECT_DIR/xfr/daily_rollup.xfr \
  $TMP/joined_sorted_${BUSINESS_DATE}.dat \
  > $OUT

echo "[$(date '+%Y-%m-%d %H:%M:%S')] wrote $OUT"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] END   daily_online_sales BUSINESS_DATE=$BUSINESS_DATE"
