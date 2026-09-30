"""Order-insensitive, row-for-row diff of daily_online_sales and reject files.

Usage:
    python pyspark/validate_outputs.py --expected-dir ABINITIO_OUT --actual-dir SPARK_OUT \
        --business-date 20260929 [--business-date ...] [--numeric]

--numeric compares numeric-looking fields by value (e.g. 0.5 == 0.50), to
separate formatting differences from real data differences.
"""

import argparse
import os
import sys
from collections import Counter
from decimal import Decimal, InvalidOperation

FILES = ["daily_online_sales_{d}.dat", "reject_{d}.dat"]


def normalize(line, numeric):
    if not numeric:
        return line
    out = []
    for field in line.split(","):
        try:
            out.append(str(Decimal(field).normalize()) if field.strip() else field)
        except InvalidOperation:
            out.append(field)
    return ",".join(out)


def read_rows(path, numeric):
    with open(path, newline="") as fh:
        return Counter(normalize(line.rstrip("\n"), numeric) for line in fh)


def diff_file(expected, actual, numeric):
    missing = [p for p in (expected, actual) if not os.path.exists(p)]
    if missing:
        return [f"missing file: {p}" for p in missing]
    exp, act = read_rows(expected, numeric), read_rows(actual, numeric)
    problems = []
    for row, n in sorted((exp - act).items()):
        problems.append(f"  only in expected (x{n}): {row}")
    for row, n in sorted((act - exp).items()):
        problems.append(f"  only in actual   (x{n}): {row}")
    return problems


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--expected-dir", required=True)
    p.add_argument("--actual-dir", required=True)
    p.add_argument("--business-date", action="append", required=True)
    p.add_argument("--numeric", action="store_true")
    a = p.parse_args(argv)

    failed = False
    for d in a.business_date:
        for pattern in FILES:
            name = pattern.format(d=d)
            problems = diff_file(
                os.path.join(a.expected_dir, name), os.path.join(a.actual_dir, name), a.numeric
            )
            print(f"{'DIFF' if problems else 'OK  '} {name}")
            for line in problems:
                print(line)
            failed = failed or bool(problems)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
