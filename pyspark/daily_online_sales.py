"""PySpark port of mp/daily_online_sales.mp (Ab Initio).

    online_orders -> cleanse (xfr/order_cleanse.xfr) -> inner join product_dim on sku
      (unmatched -> reject) -> rollup (xfr/daily_rollup.xfr) -> daily_online_sales

Usage:
    spark-submit pyspark/daily_online_sales.py --business-date YYYYMMDD [--project-dir DIR]
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)

ID_TYPE = DecimalType(18, 0)
AMOUNT_TYPE = DecimalType(18, 4)

# dml/online_order.dml
ONLINE_ORDER_SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("business_date", DateType()),
        StructField("customer_id", StringType()),
        StructField("store_id", ID_TYPE),
        StructField("fulfillment_type", StringType()),
        StructField("sku", ID_TYPE),
        StructField("qty", AMOUNT_TYPE),
        StructField("unit_price", AMOUNT_TYPE),
        StructField("discount_amt", AMOUNT_TYPE),
        StructField("order_status", StringType()),
    ]
)

# dml/product_dim.dml
PRODUCT_DIM_SCHEMA = StructType(
    [
        StructField("sku", ID_TYPE),
        StructField("product_name", StringType()),
        StructField("department", StringType()),
        StructField("category", StringType()),
    ]
)

# dml/daily_online_sales.dml
DAILY_ONLINE_SALES_SCHEMA = StructType(
    [
        StructField("business_date", DateType()),
        StructField("store_id", ID_TYPE),
        StructField("fulfillment_type", StringType()),
        StructField("department", StringType()),
        StructField("order_count", LongType()),
        StructField("unit_qty", AMOUNT_TYPE),
        StructField("gross_sales", DecimalType(38, 2)),
        StructField("discount_total", DecimalType(38, 2)),
        StructField("net_sales", DecimalType(38, 2)),
    ]
)

# Output record of the cleanse REFORMAT (xfr/order_cleanse.xfr); the JOIN's
# unused0 port (reject file) carries records in this layout.
CLEANSED_ORDER_COLUMNS = [
    "order_id",
    "business_date",
    "customer_id",
    "store_id",
    "fulfillment_type",
    "sku",
    "qty",
    "unit_price",
    "discount_amt",
    "line_total",
]

ROLLUP_KEY = ["business_date", "store_id", "fulfillment_type", "department"]

QTY_COLUMNS = {"qty", "unit_qty"}
MONEY_COLUMNS = {
    "unit_price",
    "discount_amt",
    "line_total",
    "gross_sales",
    "discount_total",
    "net_sales",
}

DELIMITER = ","
DATE_FORMAT = "yyyy-MM-dd"


def parse_delimited(lines: DataFrame, schema: StructType) -> DataFrame:
    """Parse ``value`` lines of an Ab Initio delimited record.

    Fields 1..n-1 end at ``,`` and the last field runs to end of line, so the
    last field may itself contain commas. No quoting, strings are kept verbatim
    (empty stays empty), empty decimals are null. Malformed records fail the job.
    """
    n = len(schema.fields)
    parts = F.split(F.col("value"), DELIMITER, n)
    checked = F.when(
        F.size(parts) == n, parts
    ).otherwise(
        F.raise_error(
            F.concat(
                F.lit(f"malformed record (expected {n} fields): "), F.col("value")
            )
        )
    )
    columns = []
    for i, field in enumerate(schema.fields):
        raw = checked.getItem(i)
        if isinstance(field.dataType, StringType):
            col = raw
        elif isinstance(field.dataType, DateType):
            col = F.to_date(raw, DATE_FORMAT)
        else:
            trimmed = F.trim(raw)
            col = F.when(trimmed == "", F.lit(None)).otherwise(trimmed).cast(
                field.dataType
            )
        columns.append(col.cast(field.dataType).alias(field.name))
    return lines.select(*columns)


def read_delimited(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    lines = spark.read.option("lineSep", "\n").text(path)
    return parse_delimited(lines, schema)


def cleanse(orders: DataFrame) -> DataFrame:
    """xfr/order_cleanse.xfr: select is_valid_order, then reformat."""
    unit_price = F.coalesce(F.col("unit_price"), F.lit(0).cast(AMOUNT_TYPE))
    discount_amt = F.coalesce(F.col("discount_amt"), F.lit(0).cast(AMOUNT_TYPE))
    is_valid_order = (F.upper(F.trim(F.col("order_status"))) != "CANCELLED") & (
        F.col("qty") > 0
    )
    return orders.where(is_valid_order).select(
        F.trim("order_id").alias("order_id"),
        "business_date",
        F.trim("customer_id").alias("customer_id"),
        "store_id",
        F.upper(F.trim("fulfillment_type")).alias("fulfillment_type"),
        "sku",
        "qty",
        unit_price.alias("unit_price"),
        discount_amt.alias("discount_amt"),
        F.round(F.col("qty") * unit_price, 2).alias("line_total"),
    )


def join_products(cleansed: DataFrame, product_dim: DataFrame):
    """xfr/product_join.xfr: inner join on sku. Returns (joined, unused0)."""
    dim = F.broadcast(product_dim.select("sku", "department", "category"))
    joined = cleansed.join(dim, on="sku", how="inner").select(
        "order_id",
        "business_date",
        "store_id",
        "fulfillment_type",
        "sku",
        "department",
        "category",
        "qty",
        "line_total",
        "discount_amt",
    )
    rejects = cleansed.join(dim, on="sku", how="left_anti").select(
        *CLEANSED_ORDER_COLUMNS
    )
    return joined, rejects


def rollup(joined: DataFrame) -> DataFrame:
    """xfr/daily_rollup.xfr.

    order_count mirrors the sorted-stream "order_id changed" counter, which
    starts from last_order_id = "": a distinct count that ignores empty ids.
    net_sales is rounded from the unrounded sums, as in finalize().
    """
    order_id = F.col("order_id")
    agg = joined.groupBy(*ROLLUP_KEY).agg(
        F.countDistinct(F.when(order_id != "", order_id)).alias("order_count"),
        F.sum("qty").alias("unit_qty"),
        F.sum("line_total").alias("gross_raw"),
        F.sum("discount_amt").alias("discount_raw"),
    )
    return agg.select(
        *ROLLUP_KEY,
        "order_count",
        "unit_qty",
        F.round("gross_raw", 2).alias("gross_sales"),
        F.round("discount_raw", 2).alias("discount_total"),
        F.round(F.col("gross_raw") - F.col("discount_raw"), 2).alias("net_sales"),
    ).select(
        *[
            F.col(f.name).cast(f.dataType).alias(f.name)
            for f in DAILY_ONLINE_SALES_SCHEMA.fields
        ]
    )


def format_field(name: str, col: Column) -> Column:
    """Render a column as the text written for its DML field."""
    if name == "business_date":
        text = F.date_format(col, DATE_FORMAT)
    elif name in MONEY_COLUMNS:
        # at least 2 decimal places, extra trailing zeros dropped
        text = F.regexp_replace(col.cast("string"), r"(\.\d\d\d*?)0+$", "$1")
    elif name in QTY_COLUMNS:
        text = F.regexp_replace(
            F.regexp_replace(col.cast("string"), r"(\.\d*?)0+$", "$1"), r"\.$", ""
        )
    else:
        text = col.cast("string")
    return F.coalesce(text, F.lit(""))


def to_lines(df: DataFrame, columns, sort_key) -> DataFrame:
    return (
        df.coalesce(1)
        .sortWithinPartitions(*sort_key)
        .select(
            F.concat_ws(DELIMITER, *[format_field(c, F.col(c)) for c in columns]).alias(
                "value"
            )
        )
    )


def write_single_file(spark: SparkSession, lines: DataFrame, path: str) -> None:
    """Write a one-partition text DataFrame as exactly one file at ``path``."""
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    target = jvm.org.apache.hadoop.fs.Path(path)
    fs = target.getFileSystem(conf)
    tmp_dir = jvm.org.apache.hadoop.fs.Path(path + "._spark_tmp")

    lines.write.mode("overwrite").option("lineSep", "\n").text(path + "._spark_tmp")

    parts = [
        s.getPath()
        for s in fs.listStatus(tmp_dir)
        if s.getPath().getName().startswith("part-")
    ]
    if len(parts) > 1:
        raise RuntimeError(f"expected one part file in {tmp_dir}, got {len(parts)}")
    if fs.exists(target):
        fs.delete(target, False)
    if parts:
        fs.rename(parts[0], target)
    else:
        fs.create(target, True).close()
    fs.delete(tmp_dir, True)
    checksum = jvm.org.apache.hadoop.fs.Path(target.getParent(), f".{target.getName()}.crc")
    if fs.exists(checksum):
        fs.delete(checksum, False)


def run(
    spark: SparkSession,
    orders_file: str,
    product_dim_file: str,
    output_file: str,
    reject_file: str,
) -> None:
    orders = read_delimited(spark, orders_file, ONLINE_ORDER_SCHEMA)
    product_dim = read_delimited(spark, product_dim_file, PRODUCT_DIM_SCHEMA)

    cleansed = cleanse(orders).persist()
    joined, rejects = join_products(cleansed, product_dim)
    summary = rollup(joined)

    write_single_file(
        spark, to_lines(rejects, CLEANSED_ORDER_COLUMNS, ["sku", "order_id"]), reject_file
    )
    write_single_file(
        spark,
        to_lines(summary, [f.name for f in DAILY_ONLINE_SALES_SCHEMA.fields], ROLLUP_KEY),
        output_file,
    )
    cleansed.unpersist()


def business_date_arg(value: str) -> str:
    try:
        datetime.strptime(value, "%Y%m%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected YYYYMMDD, got {value!r}") from exc
    return value


def parse_args(argv):
    default_project_dir = os.environ.get(
        "PROJECT_DIR", str(Path(__file__).resolve().parent.parent)
    )
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--business-date", required=True, type=business_date_arg)
    p.add_argument("--project-dir", default=default_project_dir)
    p.add_argument("--orders-file")
    p.add_argument("--product-dim-file")
    p.add_argument("--output-dir", help="default: <project-dir>/data/out")
    p.add_argument("--output-file")
    p.add_argument("--reject-file")
    args = p.parse_args(argv)

    d = args.business_date
    proj = args.project_dir
    out_dir = args.output_dir or os.path.join(proj, "data", "out")
    args.orders_file = args.orders_file or os.path.join(
        proj, "data", "in", f"online_orders_{d}.dat"
    )
    args.product_dim_file = args.product_dim_file or os.path.join(
        proj, "data", "serial", "product_dim.dat"
    )
    args.output_file = args.output_file or os.path.join(
        out_dir, f"daily_online_sales_{d}.dat"
    )
    args.reject_file = args.reject_file or os.path.join(out_dir, f"reject_{d}.dat")
    return args


def log(msg: str) -> None:
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def main(argv=None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    spark = (
        SparkSession.builder.appName(f"daily_online_sales_{args.business_date}")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        log(f"START daily_online_sales BUSINESS_DATE={args.business_date}")
        run(
            spark,
            args.orders_file,
            args.product_dim_file,
            args.output_file,
            args.reject_file,
        )
        log(f"wrote {args.output_file}")
        log(f"wrote {args.reject_file}")
        log(f"END   daily_online_sales BUSINESS_DATE={args.business_date}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
