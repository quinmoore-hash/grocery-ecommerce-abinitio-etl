"""PySpark port of mp/daily_online_sales.mp.

Reads the day's online order lines, cleanses them (xfr/order_cleanse.xfr),
joins to the product dimension (xfr/product_join.xfr), writes unmatched SKUs
to the reject file (JOIN unused0 port) and rolls up to the daily grain
(xfr/daily_rollup.xfr).

Usage:
    spark-submit pyspark/daily_online_sales.py --business_date YYYYMMDD
"""

import argparse
import os
import sys
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)

MONEY = DecimalType(18, 2)

# dml/online_order.dml
ONLINE_ORDER_SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("business_date", DateType()),
        StructField("customer_id", StringType()),
        StructField("store_id", LongType()),
        StructField("fulfillment_type", StringType()),
        StructField("sku", LongType()),
        StructField("qty", LongType()),
        StructField("unit_price", MONEY),
        StructField("discount_amt", MONEY),
        StructField("order_status", StringType()),
    ]
)

# dml/product_dim.dml
PRODUCT_DIM_SCHEMA = StructType(
    [
        StructField("sku", LongType()),
        StructField("product_name", StringType()),
        StructField("department", StringType()),
        StructField("category", StringType()),
    ]
)

# Output of order_cleanse.xfr reformat(); also the record format of the reject file.
CLEANSED_COLUMNS = [
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

# Output of product_join.xfr join().
JOINED_COLUMNS = [
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
]

ROLLUP_KEY = ["business_date", "store_id", "fulfillment_type", "department"]

# dml/daily_online_sales.dml
OUTPUT_COLUMNS = ROLLUP_KEY + [
    "order_count",
    "unit_qty",
    "gross_sales",
    "discount_total",
    "net_sales",
]

CSV_READ_OPTIONS = {
    "header": "false",
    "sep": ",",
    "dateFormat": "yyyy-MM-dd",
    "mode": "FAILFAST",
    "ignoreLeadingWhiteSpace": "false",
    "ignoreTrailingWhiteSpace": "false",
}

CSV_WRITE_OPTIONS = {
    "header": "false",
    "sep": ",",
    "dateFormat": "yyyy-MM-dd",
    "lineSep": "\n",
    "nullValue": "",
    "emptyValue": "",
    "ignoreLeadingWhiteSpace": "false",
    "ignoreTrailingWhiteSpace": "false",
}


def read_csv(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    return spark.read.options(**CSV_READ_OPTIONS).schema(schema).csv(path)


def cleanse(orders: DataFrame) -> DataFrame:
    """order_cleanse.xfr: is_valid_order select + reformat."""
    unit_price = F.coalesce(F.col("unit_price"), F.lit(0).cast(MONEY))
    discount_amt = F.coalesce(F.col("discount_amt"), F.lit(0).cast(MONEY))
    is_valid_order = (F.upper(F.trim(F.col("order_status"))) != F.lit("CANCELLED")) & (
        F.col("qty") > 0
    )
    return orders.where(is_valid_order).select(
        F.trim("order_id").alias("order_id"),
        F.col("business_date"),
        F.trim("customer_id").alias("customer_id"),
        F.col("store_id"),
        F.upper(F.trim("fulfillment_type")).alias("fulfillment_type"),
        F.col("sku"),
        F.col("qty"),
        unit_price.alias("unit_price"),
        discount_amt.alias("discount_amt"),
        F.round(F.col("qty") * unit_price, 2).cast(MONEY).alias("line_total"),
    )


def product_join(cleansed: DataFrame, product_dim: DataFrame) -> DataFrame:
    """product_join.xfr: inner join on sku."""
    o = cleansed.alias("in0")
    p = product_dim.alias("in1")
    return o.join(p, F.col("in0.sku") == F.col("in1.sku"), "inner").select(
        F.col("in0.order_id"),
        F.col("in0.business_date"),
        F.col("in0.store_id"),
        F.col("in0.fulfillment_type"),
        F.col("in0.sku"),
        F.col("in1.department"),
        F.col("in1.category"),
        F.col("in0.qty"),
        F.col("in0.line_total"),
        F.col("in0.discount_amt"),
    )


def rejects(cleansed: DataFrame, product_dim: DataFrame) -> DataFrame:
    """JOIN unused0 port: cleansed order lines with no matching sku."""
    return (
        cleansed.join(product_dim.select("sku"), on="sku", how="left_anti")
        .select(*CLEANSED_COLUMNS)
        .orderBy("sku", "order_id")
    )


def daily_rollup(joined: DataFrame) -> DataFrame:
    """daily_rollup.xfr. order_count is distinct order_id per group."""
    return (
        joined.groupBy(*ROLLUP_KEY)
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("qty").alias("unit_qty"),
            F.round(F.sum("line_total"), 2).alias("gross_sales"),
            F.round(F.sum("discount_amt"), 2).alias("discount_total"),
        )
        .withColumn(
            "net_sales", F.round(F.col("gross_sales") - F.col("discount_total"), 2)
        )
        .select(*OUTPUT_COLUMNS)
        .orderBy(*ROLLUP_KEY)
    )


def write_single_file(df: DataFrame, path: str) -> None:
    """Write df as one delimited file at exactly `path` (serial OUTPUT FILE)."""
    spark = df.sparkSession
    tmp_dir = f"{path}._tmp"
    df.coalesce(1).write.mode("overwrite").options(**CSV_WRITE_OPTIONS).csv(tmp_dir)

    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    tmp_path = jvm.org.apache.hadoop.fs.Path(tmp_dir)
    dst_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = tmp_path.getFileSystem(conf)
    parts = [
        s.getPath()
        for s in fs.listStatus(tmp_path)
        if s.getPath().getName().startswith("part-")
    ]
    if len(parts) != 1:
        raise RuntimeError(f"expected 1 part file in {tmp_dir}, found {len(parts)}")
    backup_path = jvm.org.apache.hadoop.fs.Path(f"{path}._prev")
    had_previous = fs.exists(dst_path)
    if had_previous:
        fs.delete(backup_path, False)
        if not fs.rename(dst_path, backup_path):
            raise RuntimeError(f"failed to move existing {path} aside")
    if not fs.rename(parts[0], dst_path):
        if had_previous:
            fs.rename(backup_path, dst_path)
        raise RuntimeError(f"failed to move {parts[0]} to {path}")
    if had_previous:
        fs.delete(backup_path, False)
    fs.delete(tmp_path, True)
    crc_path = jvm.org.apache.hadoop.fs.Path(
        dst_path.getParent(), f".{dst_path.getName()}.crc"
    )
    if fs.exists(crc_path):
        fs.delete(crc_path, False)


def run(spark: SparkSession, business_date: str, project_dir: str) -> None:
    orders_file = os.path.join(
        project_dir, "data", "in", f"online_orders_{business_date}.dat"
    )
    product_dim_file = os.path.join(project_dir, "data", "serial", "product_dim.dat")
    out_dir = os.path.join(project_dir, "data", "out")
    output_file = os.path.join(out_dir, f"daily_online_sales_{business_date}.dat")
    reject_file = os.path.join(out_dir, f"reject_{business_date}.dat")

    orders = read_csv(spark, orders_file, ONLINE_ORDER_SCHEMA)
    product_dim = read_csv(spark, product_dim_file, PRODUCT_DIM_SCHEMA)

    cleansed = cleanse(orders).cache()
    try:
        write_single_file(rejects(cleansed, product_dim), reject_file)
        write_single_file(
            daily_rollup(product_join(cleansed, product_dim)), output_file
        )
    finally:
        cleansed.unpersist()


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--business_date", required=True, help="YYYYMMDD")
    parser.add_argument(
        "--project_dir",
        default=os.environ.get("PROJECT_DIR")
        or os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        help="project root containing data/ (default: $PROJECT_DIR or repo root)",
    )
    args = parser.parse_args(argv)
    try:
        datetime.strptime(args.business_date, "%Y%m%d")
    except ValueError:
        parser.error(f"--business_date must be YYYYMMDD, got {args.business_date!r}")
    if len(args.business_date) != 8:
        parser.error(f"--business_date must be YYYYMMDD, got {args.business_date!r}")
    return args


def main(argv=None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    spark = SparkSession.builder.appName(
        f"daily_online_sales_{args.business_date}"
    ).getOrCreate()
    try:
        run(spark, args.business_date, args.project_dir)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
