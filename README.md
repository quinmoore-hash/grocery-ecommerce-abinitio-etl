# grocery-ecommerce-abinitio-etl

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

## Running (Ab Initio)

On a host with the Co>Operating System installed:

```sh
export PROJECT_DIR=$(pwd)
. sand/project.pset
run/daily_online_sales.ksh 20260929
```

Sample data for business date `20260929` is included. The feed contains one
cancelled order (dropped by the cleanse step) and one unknown SKU
(`99999`) that lands in the reject file.

## Running (PySpark)

`pyspark/` is an additive PySpark port of `mp/daily_online_sales.mp`; the Ab
Initio artifacts stay in place until decommission is confirmed.

| Ab Initio                         | PySpark                                        |
|-----------------------------------|------------------------------------------------|
| `run/daily_online_sales.ksh`      | `pyspark/run_daily_online_sales.sh`            |
| `mp/` + `xfr/` + `dml/`           | `pyspark/daily_online_sales.py`                |
| JOIN `unused0` -> reject file     | left-anti join on `sku` -> same reject file    |

Requires Java 17+ and Spark 3.5 (`pip install -r pyspark/requirements.txt`
for a local run):

```sh
pyspark/run_daily_online_sales.sh 20260929
# or directly:
spark-submit pyspark/daily_online_sales.py --business_date 20260929
```

Inputs/outputs are the same paths the graph uses, resolved under
`$PROJECT_DIR` (default: repo root):

- `data/in/online_orders_<date>.dat` + `data/serial/product_dim.dat`
- `data/out/daily_online_sales_<date>.dat` (single file, sorted by the rollup key)
- `data/out/reject_<date>.dat` (cleansed order lines with no matching SKU)

Extra `spark-submit` flags (e.g. `--master yarn`) can be passed via
`SPARK_SUBMIT_OPTS`; set `SPARK_SUBMIT` to use a non-default binary.

Tests (sample data vs. golden files in `pyspark/tests/expected/`, plus
cleanse/distinct-order-count edge cases):

```sh
python -m pytest pyspark/tests
```

## Output grain

`daily_online_sales`: `business_date x store_id x fulfillment_type x department`
with `order_count`, `unit_qty`, `gross_sales`, `discount_total`, `net_sales`.
