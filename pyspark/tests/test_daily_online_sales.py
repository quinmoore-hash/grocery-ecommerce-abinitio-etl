import os
import shutil

import daily_online_sales as job
import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
EXPECTED_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "expected")


def _project(tmp_path, business_date, orders_lines):
    for sub in ("in", "serial", "out"):
        (tmp_path / "data" / sub).mkdir(parents=True)
    shutil.copy(
        os.path.join(REPO_ROOT, "data", "serial", "product_dim.dat"),
        tmp_path / "data" / "serial" / "product_dim.dat",
    )
    (tmp_path / "data" / "in" / f"online_orders_{business_date}.dat").write_text(
        "".join(line + "\n" for line in orders_lines)
    )
    return tmp_path


def _read_bytes(path):
    with open(path, "rb") as f:
        return f.read()


def test_sample_matches_expected(spark, tmp_path):
    with open(os.path.join(REPO_ROOT, "data", "in", "online_orders_20260929.dat")) as f:
        lines = f.read().splitlines()
    project = _project(tmp_path, "20260929", lines)

    job.run(spark, "20260929", str(project))

    out = project / "data" / "out"
    assert sorted(os.listdir(out)) == [
        "daily_online_sales_20260929.dat",
        "reject_20260929.dat",
    ]
    for name in os.listdir(out):
        assert _read_bytes(out / name) == _read_bytes(
            os.path.join(EXPECTED_DIR, name)
        ), name


def test_cleanse_and_distinct_order_count(spark, tmp_path):
    project = _project(
        tmp_path,
        "20260930",
        [
            "E1,2026-09-30, C1 ,101, delivery ,10001,2,0.59,0.10,FULFILLED",
            "E1,2026-09-30,C1,101,DELIVERY,10002,1,1.29,,FULFILLED",
            "  E2,2026-09-30,C2,101,DELIVERY,10001,1,,0.00,PLACED",
            "E3,2026-09-30,C3,101,DELIVERY,10001,0,0.59,0.00,FULFILLED",
            "E4,2026-09-30,C4,101,DELIVERY,10001,1,0.59,0.00, cancelled ",
            "E5,2026-09-30, C5,102,pickup,88888,2,1.25,0.00,FULFILLED",
        ],
    )

    job.run(spark, "20260930", str(project))

    out = project / "data" / "out"
    # 3 joined rows, 2 distinct orders (E1 twice) -> order_count 2, not 3.
    assert (out / "daily_online_sales_20260930.dat").read_text() == (
        "2026-09-30,101,DELIVERY,PRODUCE,2,4,2.47,0.10,2.37\n"
    )
    assert (out / "reject_20260930.dat").read_text() == (
        "E5,2026-09-30,C5,102,PICKUP,88888,2,1.25,0.00,2.50\n"
    )


@pytest.mark.parametrize("bad", ["2026-09-29", "2026093", "20261301", "abcdefgh"])
def test_rejects_bad_business_date(bad):
    with pytest.raises(SystemExit):
        job.parse_args(["--business_date", bad])
