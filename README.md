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

## Running

On a host with the Co>Operating System installed:

```sh
export PROJECT_DIR=$(pwd)
. sand/project.pset
run/daily_online_sales.ksh 20260929
```

Sample data for business date `20260929` is included. The feed contains one
cancelled order (dropped by the cleanse step) and one unknown SKU
(`99999`) that lands in the reject file.

## Output grain

`daily_online_sales`: `business_date x store_id x fulfillment_type x department`
with `order_count`, `unit_qty`, `gross_sales`, `discount_total`, `net_sales`.
