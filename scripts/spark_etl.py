#!/usr/bin/env python
import json
import os
from dataclasses import asdict, dataclass
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit


@dataclass
class Point:
    x: float
    y: float


def compute_regression_points(xs: List[float], ys: List[float]):
    n = float(len(xs))
    sx = sum(xs)
    sy = sum(ys)
    sxy = sum(x * y for x, y in zip(xs, ys))
    sx2 = sum(x * x for x in xs)
    denom = (n * sx2 - sx * sx) or 1.0
    slope = (n * sxy - sx * sy) / denom
    intercept = (sy - slope * sx) / n
    reg_points = [Point(x=float(x), y=float(slope * x + intercept)) for x in xs]
    return slope, intercept, reg_points


def main():
    import argparse

    parser = argparse.ArgumentParser(description="ETL with PySpark -> JSON for charts")
    parser.add_argument("--input", default="data/coffee_sales.csv", help="Path to input CSV")
    parser.add_argument(
        "--output",
        default="docs/assets/data.json",
        help="Output JSON path (served by GitHub Pages)",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("coffee_sales_etl").getOrCreate()

    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(args.input)
        .withColumn("total_sales", col("online_sales") + col("in_store_sales"))
    )

    # Order by month just in case
    df = df.orderBy(col("month"))

    rows = df.collect()
    # Ensure months are JSON-serializable strings
    months = [str(r["month"]) for r in rows]
    marketing = [float(r["marketing_spend"]) for r in rows]
    online = [float(r["online_sales"]) for r in rows]
    instore = [float(r["in_store_sales"]) for r in rows]
    totals = [float(r["total_sales"]) for r in rows]

    # Scatter and regression (marketing -> total_sales)
    scatter = [asdict(Point(x=m, y=t)) for m, t in zip(marketing, totals)]
    _, _, reg_points = compute_regression_points(marketing, totals)
    regression = [asdict(p) for p in reg_points]

    payload = {
        "months": months,
        "total_sales": totals,
        "online_sales": online,
        "in_store_sales": instore,
        "marketing_spend": marketing,
        "scatter_points": scatter,
        "regression_points": regression,
    }

    out_path = args.output
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f)

    print(f"Wrote chart data -> {out_path}")

    spark.stop()


if __name__ == "__main__":
    main()
