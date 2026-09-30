;;; ============================================================
;;; daily_online_sales.mp
;;; Ab Initio graph (checked-in text form).
;;; Nightly rollup of online grocery orders into a daily sales fact.
;;;
;;;   INPUT FILE (online_orders) -> REFORMAT (cleanse) -> SORT (sku)
;;;                                                           |
;;;   INPUT FILE (product_dim) -----------------------------> JOIN (product_join)
;;;                                                           |   unused0 -> OUTPUT FILE (reject)
;;;                                                        SORT (rollup key)
;;;                                                           |
;;;                                                        ROLLUP (daily_rollup)
;;;                                                           |
;;;                                            OUTPUT FILE (daily_online_sales)
;;; ============================================================

graph daily_online_sales
  parameter BUSINESS_DATE     = "${BUSINESS_DATE}"
  parameter ORDERS_INPUT_FILE = "$PROJECT_DIR/data/in/online_orders_${BUSINESS_DATE}.dat"
  parameter PRODUCT_DIM_FILE  = "$PROJECT_DIR/data/serial/product_dim.dat"
  parameter OUTPUT_FILE       = "$PROJECT_DIR/data/out/daily_online_sales_${BUSINESS_DATE}.dat"
  parameter REJECT_FILE       = "$PROJECT_DIR/data/out/reject_${BUSINESS_DATE}.dat"

  component INPUT_FILE read_orders
    layout        = serial
    file          = "${ORDERS_INPUT_FILE}"
    record_format = "$PROJECT_DIR/dml/online_order.dml"

  component INPUT_FILE read_product_dim
    layout        = serial
    file          = "${PRODUCT_DIM_FILE}"
    record_format = "$PROJECT_DIR/dml/product_dim.dml"

  component REFORMAT cleanse
    transform = "$PROJECT_DIR/xfr/order_cleanse.xfr"
    select    = is_valid_order

  component SORT sort_by_sku
    key = { sku }

  component JOIN product_join
    join-type   = inner
    key         = in0.sku = in1.sku
    transform   = "$PROJECT_DIR/xfr/product_join.xfr"
    unused-port = enabled

  component SORT sort_for_rollup
    key = { business_date; store_id; fulfillment_type; department; order_id }

  component ROLLUP daily_rollup
    key       = { business_date; store_id; fulfillment_type; department }
    transform = "$PROJECT_DIR/xfr/daily_rollup.xfr"

  component OUTPUT_FILE write_summary
    layout        = serial
    file          = "${OUTPUT_FILE}"
    record_format = "$PROJECT_DIR/dml/daily_online_sales.dml"

  component OUTPUT_FILE write_reject
    layout        = serial
    file          = "${REJECT_FILE}"

  flow read_orders.out        -> cleanse.in
  flow cleanse.out            -> sort_by_sku.in
  flow sort_by_sku.out        -> product_join.in0
  flow read_product_dim.out   -> product_join.in1
  flow product_join.unused0   -> write_reject.in
  flow product_join.out       -> sort_for_rollup.in
  flow sort_for_rollup.out    -> daily_rollup.in
  flow daily_rollup.out       -> write_summary.in
end graph
