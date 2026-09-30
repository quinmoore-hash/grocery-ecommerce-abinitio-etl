from pathlib import Path

import pytest

import abinitio_reference
import daily_online_sales as job

PROJECT_DIR = Path(__file__).resolve().parents[2]
PRODUCT_DIM = PROJECT_DIR / "data" / "serial" / "product_dim.dat"
SAMPLE_DATES = sorted(
    p.stem.removeprefix("online_orders_")
    for p in (PROJECT_DIR / "data" / "in").glob("online_orders_*.dat")
)


def run_both(spark, tmp_path, orders_file, dim_file=PRODUCT_DIM):
    spark_dir, ref_dir = tmp_path / "spark", tmp_path / "ref"
    spark_dir.mkdir()
    ref_dir.mkdir()
    job.run(
        spark,
        str(orders_file),
        str(dim_file),
        str(spark_dir / "out.dat"),
        str(spark_dir / "reject.dat"),
    )
    abinitio_reference.run(
        str(orders_file), str(dim_file), str(ref_dir / "out.dat"), str(ref_dir / "reject.dat")
    )
    return spark_dir, ref_dir


def read(path):
    return Path(path).read_text()


def sorted_lines(path):
    return sorted(read(path).splitlines())


def write_orders(tmp_path, *lines):
    path = tmp_path / "orders.dat"
    path.write_text("".join(line + "\n" for line in lines))
    return path


def test_sample_date_golden_output(spark, tmp_path):
    spark_dir, _ = run_both(
        spark, tmp_path, PROJECT_DIR / "data" / "in" / "online_orders_20260929.dat"
    )
    assert read(spark_dir / "out.dat") == (
        "2026-09-29,101,DELIVERY,BAKERY,1,1,5.99,0.00,5.99\n"
        "2026-09-29,101,DELIVERY,DAIRY,1,1,4.29,0.50,3.79\n"
        "2026-09-29,101,DELIVERY,PRODUCE,2,9,5.31,0.25,5.06\n"
        "2026-09-29,101,PICKUP,DAIRY,1,2,6.98,0.00,6.98\n"
        "2026-09-29,101,PICKUP,MEAT,1,2,13.98,1.00,12.98\n"
        "2026-09-29,102,DELIVERY,GROCERY,1,1,7.49,0.00,7.49\n"
        "2026-09-29,102,DELIVERY,PRODUCE,1,4,5.16,0.00,5.16\n"
    )
    assert read(spark_dir / "reject.dat") == (
        "W1005,2026-09-29,C945,102,PICKUP,99999,1,2.00,0.00,2.00\n"
    )


@pytest.mark.parametrize("business_date", SAMPLE_DATES)
def test_matches_graph_emulation_on_sample_data(spark, tmp_path, business_date):
    orders = PROJECT_DIR / "data" / "in" / f"online_orders_{business_date}.dat"
    spark_dir, ref_dir = run_both(spark, tmp_path, orders)
    assert read(spark_dir / "out.dat") == read(ref_dir / "out.dat")
    assert sorted_lines(spark_dir / "reject.dat") == sorted_lines(ref_dir / "reject.dat")


def test_cleanse_rules(spark, tmp_path):
    orders = write_orders(
        tmp_path,
        "A1,2026-09-30, C1 ,101, pickup ,10001,3,0.335,,placed",
        "A2,2026-09-30,C2,101,PICKUP,10001,2,0.59,0.10, Cancelled ",
        "A3,2026-09-30,C3,101,PICKUP,10001,0,0.59,0.00,FULFILLED",
        "A4,2026-09-30,C4,101,PICKUP,10001,-2,0.59,0.00,FULFILLED",
        "A5,2026-09-30,C5,101,PICKUP,99999,1,,,FULFILLED",
    )
    spark_dir, ref_dir = run_both(spark, tmp_path, orders)
    # 3 * 0.335 = 1.005 rounds half-up; null unit_price/discount_amt -> 0
    assert read(spark_dir / "out.dat") == "2026-09-30,101,PICKUP,PRODUCE,1,3,1.01,0.00,1.01\n"
    assert read(spark_dir / "reject.dat") == "A5,2026-09-30,C5,101,PICKUP,99999,1,0.00,0.00,0.00\n"
    assert read(spark_dir / "out.dat") == read(ref_dir / "out.dat")
    assert read(spark_dir / "reject.dat") == read(ref_dir / "reject.dat")


def test_net_sales_rounds_unrounded_sums(spark, tmp_path):
    orders = write_orders(
        tmp_path,
        "A1,2026-09-30,C1,101,PICKUP,40001,1,6.99,0.002,FULFILLED",
        "A2,2026-09-30,C2,101,PICKUP,40001,1,6.99,0.003,FULFILLED",
    )
    spark_dir, ref_dir = run_both(spark, tmp_path, orders)
    # round(13.98 - 0.005) = 13.98, not round(13.98) - round(0.005) = 13.97
    assert read(spark_dir / "out.dat") == "2026-09-30,101,PICKUP,MEAT,2,2,13.98,0.01,13.98\n"
    assert read(spark_dir / "out.dat") == read(ref_dir / "out.dat")


def test_order_count_is_distinct_and_ignores_empty_order_id(spark, tmp_path):
    orders = write_orders(
        tmp_path,
        "B1,2026-09-30,C1,101,PICKUP,10001,1,1.00,0.00,FULFILLED",
        "B1,2026-09-30,C1,101,PICKUP,10002,1,1.00,0.00,FULFILLED",
        "B2,2026-09-30,C2,101,PICKUP,10001,1,1.00,0.00,FULFILLED",
        "  ,2026-09-30,C3,101,PICKUP,10001,1,1.00,0.00,FULFILLED",
    )
    spark_dir, ref_dir = run_both(spark, tmp_path, orders)
    assert read(spark_dir / "out.dat") == "2026-09-30,101,PICKUP,PRODUCE,2,4,4.00,0.00,4.00\n"
    assert read(spark_dir / "out.dat") == read(ref_dir / "out.dat")


def test_duplicate_dim_sku_fans_out_like_inner_join(spark, tmp_path):
    dim = tmp_path / "dim.dat"
    dim.write_text("10001,BANANAS,PRODUCE,FRUIT\n10001,BANANAS,GROCERY,FRUIT\n")
    orders = write_orders(tmp_path, "C1,2026-09-30,C1,101,PICKUP,10001,1,1.00,0.00,FULFILLED")
    spark_dir, ref_dir = run_both(spark, tmp_path, orders, dim)
    assert sorted_lines(spark_dir / "out.dat") == [
        "2026-09-30,101,PICKUP,GROCERY,1,1,1.00,0.00,1.00",
        "2026-09-30,101,PICKUP,PRODUCE,1,1,1.00,0.00,1.00",
    ]
    assert read(spark_dir / "out.dat") == read(ref_dir / "out.dat")


def test_last_field_keeps_embedded_delimiters(spark, tmp_path):
    orders = write_orders(tmp_path, "D1,2026-09-30,C1,101,PICKUP,10001,1,1.00,0.00,CANCELLED,X")
    spark_dir, ref_dir = run_both(spark, tmp_path, orders)
    assert read(spark_dir / "out.dat") == "2026-09-30,101,PICKUP,PRODUCE,1,1,1.00,0.00,1.00\n"
    assert read(spark_dir / "out.dat") == read(ref_dir / "out.dat")


def test_no_rejects_writes_empty_reject_file(spark, tmp_path):
    orders = write_orders(tmp_path, "E1,2026-09-30,C1,101,PICKUP,10001,1,1.00,0.00,FULFILLED")
    spark_dir, _ = run_both(spark, tmp_path, orders)
    assert read(spark_dir / "reject.dat") == ""
    assert sorted(p.name for p in spark_dir.iterdir()) == ["out.dat", "reject.dat"]


def test_malformed_record_fails(spark, tmp_path):
    orders = write_orders(tmp_path, "F1,2026-09-30,C1,101,PICKUP,10001,1,1.00")
    with pytest.raises(Exception, match="malformed record"):
        job.run(
            spark,
            str(orders),
            str(PRODUCT_DIM),
            str(tmp_path / "out.dat"),
            str(tmp_path / "reject.dat"),
        )


def test_default_paths_follow_graph_parameters(tmp_path):
    args = job.parse_args(["--business-date", "20260929", "--project-dir", str(tmp_path)])
    assert args.orders_file == str(tmp_path / "data/in/online_orders_20260929.dat")
    assert args.product_dim_file == str(tmp_path / "data/serial/product_dim.dat")
    assert args.output_file == str(tmp_path / "data/out/daily_online_sales_20260929.dat")
    assert args.reject_file == str(tmp_path / "data/out/reject_20260929.dat")


def test_rejects_bad_business_date():
    with pytest.raises(SystemExit):
        job.parse_args(["--business-date", "2026-09-29"])
