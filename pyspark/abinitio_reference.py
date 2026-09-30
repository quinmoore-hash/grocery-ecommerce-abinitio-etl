"""Pure-Python emulation of mp/daily_online_sales.mp, used as a validation oracle.

Follows the graph literally (stream REFORMAT/select, SORT, sorted-merge JOIN
with unused0, SORT, ROLLUP with the last_order_id change counter) so the
PySpark job can be diffed against it on hosts without the Co>Operating System.

Usage:
    python pyspark/abinitio_reference.py --business-date YYYYMMDD --output-dir DIR
"""

import argparse
import os
import sys
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

ORDER_FIELDS = [
    ("order_id", "string"),
    ("business_date", "date"),
    ("customer_id", "string"),
    ("store_id", "decimal"),
    ("fulfillment_type", "string"),
    ("sku", "decimal"),
    ("qty", "decimal"),
    ("unit_price", "decimal"),
    ("discount_amt", "decimal"),
    ("order_status", "string"),
]
PRODUCT_FIELDS = [
    ("sku", "decimal"),
    ("product_name", "string"),
    ("department", "string"),
    ("category", "string"),
]
CLEANSED_FIELDS = [
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
SUMMARY_FIELDS = [
    "business_date",
    "store_id",
    "fulfillment_type",
    "department",
    "order_count",
    "unit_qty",
    "gross_sales",
    "discount_total",
    "net_sales",
]
ROLLUP_KEY = ["business_date", "store_id", "fulfillment_type", "department"]
MONEY = {"unit_price", "discount_amt", "line_total", "gross_sales", "discount_total", "net_sales"}
QTY = {"qty", "unit_qty"}
CENT = Decimal("0.01")


def read_records(path, fields):
    records = []
    with open(path, "rb") as fh:
        data = fh.read().decode("utf-8")
    lines = data.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    for line in lines:
        parts = line.split(",", len(fields) - 1)
        if len(parts) != len(fields):
            raise ValueError(f"malformed record (expected {len(fields)} fields): {line}")
        rec = {}
        for (name, kind), raw in zip(fields, parts):
            if kind == "string":
                rec[name] = raw
            elif kind == "date":
                rec[name] = datetime.strptime(raw, "%Y-%m-%d").date()
            else:
                rec[name] = Decimal(raw.strip()) if raw.strip() else None
        records.append(rec)
    return records


def decimal_round(value, places=2):
    return value.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)


def lrtrim(s):
    return s.strip(" ")


def is_valid_order(rec):
    return (
        lrtrim(rec["order_status"]).upper() != "CANCELLED"
        and rec["qty"] is not None
        and rec["qty"] > 0
    )


def reformat(rec):
    unit_price = Decimal(0) if rec["unit_price"] is None else rec["unit_price"]
    discount = Decimal(0) if rec["discount_amt"] is None else rec["discount_amt"]
    return {
        "order_id": lrtrim(rec["order_id"]),
        "business_date": rec["business_date"],
        "customer_id": lrtrim(rec["customer_id"]),
        "store_id": rec["store_id"],
        "fulfillment_type": lrtrim(rec["fulfillment_type"]).upper(),
        "sku": rec["sku"],
        "qty": rec["qty"],
        "unit_price": unit_price,
        "discount_amt": discount,
        "line_total": decimal_round(rec["qty"] * unit_price),
    }


def merge_join(in0, in1):
    """Sorted inner JOIN on sku; returns (out, unused0)."""
    in0 = sorted(in0, key=lambda r: r["sku"])
    in1 = sorted(in1, key=lambda r: r["sku"])
    out, unused0 = [], []
    j = 0
    for rec in in0:
        while j < len(in1) and in1[j]["sku"] < rec["sku"]:
            j += 1
        k = j
        matched = False
        while k < len(in1) and in1[k]["sku"] == rec["sku"]:
            dim = in1[k]
            out.append(
                {
                    "order_id": rec["order_id"],
                    "business_date": rec["business_date"],
                    "store_id": rec["store_id"],
                    "fulfillment_type": rec["fulfillment_type"],
                    "sku": rec["sku"],
                    "department": dim["department"],
                    "category": dim["category"],
                    "qty": rec["qty"],
                    "line_total": rec["line_total"],
                    "discount_amt": rec["discount_amt"],
                }
            )
            matched = True
            k += 1
        if not matched:
            unused0.append(rec)
    return out, unused0


def string_key(s):
    return s.encode("utf-8")


def rollup_key(rec):
    return (
        rec["business_date"],
        rec["store_id"],
        string_key(rec["fulfillment_type"]),
        string_key(rec["department"]),
    )


def rollup(joined):
    joined = sorted(joined, key=lambda r: rollup_key(r) + (string_key(r["order_id"]),))
    results = []
    temp = None
    last = None
    for rec in joined + [None]:
        key = None if rec is None else rollup_key(rec)
        if temp is not None and key != rollup_key(last):
            results.append(
                {
                    "business_date": last["business_date"],
                    "store_id": last["store_id"],
                    "fulfillment_type": last["fulfillment_type"],
                    "department": last["department"],
                    "order_count": temp["order_count"],
                    "unit_qty": temp["unit_qty"],
                    "gross_sales": decimal_round(temp["gross_sales"]),
                    "discount_total": decimal_round(temp["discount_total"]),
                    "net_sales": decimal_round(temp["gross_sales"] - temp["discount_total"]),
                }
            )
            temp = None
        if rec is None:
            break
        if temp is None:
            temp = {
                "order_count": 0,
                "unit_qty": Decimal(0),
                "gross_sales": Decimal(0),
                "discount_total": Decimal(0),
                "last_order_id": "",
            }
        temp["order_count"] += 1 if rec["order_id"] != temp["last_order_id"] else 0
        temp["last_order_id"] = rec["order_id"]
        temp["unit_qty"] += rec["qty"]
        temp["gross_sales"] += rec["line_total"]
        temp["discount_total"] += rec["discount_amt"]
        last = rec
    return results


def fmt(name, value):
    if value is None:
        return ""
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, Decimal):
        if name in MONEY:
            text = format(value, "f")
            if "." not in text:
                text += ".00"
            whole, frac = text.split(".")
            frac = frac.rstrip("0")
            return f"{whole}.{frac.ljust(2, '0')}"
        if name in QTY:
            text = format(value, "f")
            return text.rstrip("0").rstrip(".") if "." in text else text
        return format(value, "f")
    return str(value)


def write_records(path, records, fields):
    with open(path, "w", newline="") as fh:
        for rec in records:
            fh.write(",".join(fmt(f, rec[f]) for f in fields) + "\n")


def run(orders_file, product_dim_file, output_file, reject_file):
    orders = read_records(orders_file, ORDER_FIELDS)
    dim = read_records(product_dim_file, PRODUCT_FIELDS)
    cleansed = [reformat(r) for r in orders if is_valid_order(r)]
    joined, unused0 = merge_join(cleansed, dim)
    write_records(reject_file, unused0, CLEANSED_FIELDS)
    write_records(output_file, rollup(joined), SUMMARY_FIELDS)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--business-date", required=True)
    p.add_argument(
        "--project-dir",
        default=os.environ.get("PROJECT_DIR", str(Path(__file__).resolve().parent.parent)),
    )
    p.add_argument("--output-dir", required=True)
    a = p.parse_args(argv)
    d = a.business_date
    os.makedirs(a.output_dir, exist_ok=True)
    run(
        os.path.join(a.project_dir, "data", "in", f"online_orders_{d}.dat"),
        os.path.join(a.project_dir, "data", "serial", "product_dim.dat"),
        os.path.join(a.output_dir, f"daily_online_sales_{d}.dat"),
        os.path.join(a.output_dir, f"reject_{d}.dat"),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
