# grocery-ecommerce-abinitio-etl

> **Status:** the pipeline has been ported to **PySpark** (`pyspark/`). The Ab
> Initio artifacts (`mp/`, `dml/`, `xfr/`, `plan/`, `run/`, `sand/`) are
> **retained for reference and deprecated** - do not change them; see
> [PySpark job](#pyspark-job) below.

A simple **Ab Initio** batch pipeline for a grocery chain's online (ecommerce)
orders. One graph, run nightly: it reads the day's online order lines,
cleanses them, enriches them with the product dimension, and rolls them up
into a daily online-sales fact by store, fulfillment type and department.

```
INPUT FILE            REFORMAT         JOIN                 SORT      ROLLUP           OUTPUT FILE
online_orders_*.dat -> cleanse ------> product_dim ------> key ----> daily grain ---> daily_online_sales_*.dat
                       (drop           (sku)                          (orders, units,
                        cancelled,        |                            gross, discount,
                        trim, line     unused -> reject                net)
                        total)
```

## Layout

| Path    | Artifact                  | Contents                                            |
|---------|---------------------------|-----------------------------------------------------|
| `mp/`   | Graph (`.mp`)             | `daily_online_sales.mp` – the pipeline              |
| `dml/`  | Record formats (`.dml`)   | input orders, product dimension, output fact        |
| `xfr/`  | Transforms (`.xfr`)       | cleanse (REFORMAT), product join (JOIN), rollup     |
| `plan/` | Conduct>It plan           | `daily_ecommerce.plan` – nightly schedule           |
| `run/`  | Deployed script (`.ksh`)  | `daily_online_sales.ksh` – Co>Op entry point        |
| `sand/` | Sandbox params            | `project.pset` – `PROJECT_DIR`, `AB_HOME`           |
| `data/` | Staging                   | `in/` order feed, `serial/` product dim, `out/` results |

## Running (Ab Initio, deprecated)

On a host with the Co>Operating System installed:

```sh
export PROJECT_DIR=$(pwd)
. sand/project.pset
run/daily_online_sales.ksh 20260929
```

Sample data for business date `20260929` is included. The feed contains one
cancelled order (dropped by the cleanse step) and one unknown SKU
(`99999`) that lands in the reject file. `20260930` exercises cleanse edge
cases (whitespace/lowercase codes, empty prices/discounts, zero/negative qty,
half-up rounding, sub-cent discounts, two rejects); `20261001` has no rejects.

## Output grain

`daily_online_sales`: `business_date x store_id x fulfillment_type x department`
with `order_count`, `unit_qty`, `gross_sales`, `discount_total`, `net_sales`.

## PySpark job

`pyspark/daily_online_sales.py` replaces `mp/daily_online_sales.mp` /
`run/daily_online_sales.ksh` and reads/writes the same files:

| Graph component            | PySpark                                                                 |
|----------------------------|-------------------------------------------------------------------------|
| INPUT_FILE + `*.dml`       | `ONLINE_ORDER_SCHEMA`, `PRODUCT_DIM_SCHEMA` (`StructType`) + `parse_delimited` |
| REFORMAT `order_cleanse`   | `cleanse()` - `is_valid_order` filter, trims, upcase, null -> 0, `line_total` |
| SORT + JOIN `product_join` | `join_products()` - broadcast inner join on `sku`; `left_anti` = `unused0` reject |
| SORT + ROLLUP `daily_rollup` | `rollup()` - `groupBy(business_date, store_id, fulfillment_type, department)` |
| OUTPUT_FILE                | `write_single_file()` - one delimited file, `DAILY_ONLINE_SALES_SCHEMA` column order |

### Requirements

- Spark / PySpark **4.0.1** (`pip install -r pyspark/requirements.txt` installs
  `pyspark` incl. `spark-submit`, plus `pytest`), Python 3.10+
- Java 17 or 21

### Running

```sh
pyspark/run_daily_online_sales.sh 20260929
# equivalent to:
spark-submit --master 'local[*]' pyspark/daily_online_sales.py \
  --business-date 20260929 --project-dir "$PWD"
```

Writes `data/out/daily_online_sales_<date>.dat` and `data/out/reject_<date>.dat`.

Runner environment variables:

| Variable            | Default                   | Purpose                                         |
|---------------------|---------------------------|-------------------------------------------------|
| `PROJECT_DIR`       | repo root                 | root of `data/in`, `data/serial`, `data/out`    |
| `SPARK_SUBMIT`      | `spark-submit` on `PATH`  | spark-submit binary                             |
| `SPARK_MASTER`      | `local[*]`                | `--master` (e.g. `yarn`, `k8s://...`)           |
| `SPARK_SUBMIT_ARGS` | *(empty)*                 | extra spark-submit flags (`--conf`, `--executor-memory`, ...) |
| `PYSPARK_PYTHON`    | Spark default             | Python for driver/executors                     |

Job flags (mirror the graph parameters): `--business-date YYYYMMDD` (required),
`--project-dir`, `--orders-file`, `--product-dim-file`, `--output-dir`,
`--output-file`, `--reject-file`.

Scheduling: replace the Conduct>It plan `plan/daily_ecommerce.plan` with the
same trigger (03:00 daily) calling the runner, e.g. cron
`0 3 * * * cd /path/to/repo && pyspark/run_daily_online_sales.sh $(date -d yesterday +\%Y\%m\%d)`
or an Airflow `BashOperator` running `pyspark/run_daily_online_sales.sh {{ ds_nodash }}`.
Adjust the date expression to however `BUSINESS_DATE` is chosen today.

### Tests and validation

```sh
python -m pytest pyspark/tests
```

The tests diff the PySpark output against a golden file for `20260929` and
against `pyspark/abinitio_reference.py`, a pure-Python, line-by-line emulation of
the graph (stream select/reformat, SORT, sorted-merge JOIN with `unused0`,
SORT, ROLLUP with the `last_order_id` change counter), for every
`data/in/online_orders_*.dat` plus targeted edge cases.

To diff against real Ab Initio output on a Co>Op host (order-insensitive,
row-for-row):

```sh
export PROJECT_DIR=$(pwd); . sand/project.pset
for d in 20260929 20260930 20261001; do
  run/daily_online_sales.ksh $d
  pyspark/run_daily_online_sales.sh $d --output-dir /tmp/spark_out
done
python pyspark/validate_outputs.py --expected-dir data/out --actual-dir /tmp/spark_out \
  --business-date 20260929 --business-date 20260930 --business-date 20261001
# add --numeric to compare numbers by value and separate formatting diffs
```

### Validation results and discrepancies

- **Ab Initio not run here.** The Co>Operating System (`m_dump`, `m_join`,
  `m_rollup`, ...) is not available on the migration host, so the ksh driver
  could not be executed. The PySpark job was validated against the
  hand-computed golden output for `20260929` and against the graph emulation
  for `20260929`, `20260930` and `20261001`: output and reject files match
  row-for-row (the fact file byte-for-byte). Run `validate_outputs.py` above on
  a Co>Op host before cut-over.
- **Number formatting** of `decimal(",")` fields is an assumption to confirm in
  that diff: ids/counts as integers; `qty`/`unit_qty` without trailing zeros
  (`9`, `6.5`); money fields with at least 2 decimals (`0.00`, `5.99`). If
  Ab Initio renders differently, only `format_field()` needs to change;
  `--numeric` shows whether values agree.
- **Rounding** uses decimal arithmetic, half away from zero (`3 x 0.335 = 1.01`),
  assumed to match `decimal_round`.
- **`net_sales`** is `round(sum(line_total) - sum(discount_amt), 2)` from the
  unrounded sums, as in `daily_rollup.xfr` `finalize()` - not the difference of
  the rounded `gross_sales`/`discount_total` (they differ when discounts have
  sub-cent precision; see `20260930`).
- **`order_count`** is `countDistinct(order_id)` excluding empty ids: the
  original counter starts at `last_order_id = ""`, so an empty `order_id` is
  never counted.
- **Reject layout** is the cleansed record (the `unused0` port of the JOIN):
  `order_id, business_date, customer_id, store_id, fulfillment_type, sku, qty,
  unit_price, discount_amt, line_total` (`order_status` is not part of the
  cleanse output).
- **Record parsing** follows the DML: fields split on `,`, the last field runs
  to `\n` (may contain commas), no quoting, strings kept verbatim, an empty
  decimal is null. Records with too few fields fail the job, as the graph would.
- **Null / whitespace edge cases** (not in the sample data): an empty `qty` is
  treated as null and the row is dropped by `qty > 0`; trimming removes spaces
  only (Spark `trim`). Confirm against Ab Initio if such data can occur.
- **Row order**: the fact file is written sorted by the rollup key (as the
  graph's output); rejects are sorted by `sku, order_id`. Compare order-insensitively.
