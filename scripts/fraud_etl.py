#!/usr/bin/env python
import json
import os
from datetime import datetime, timedelta
from random import Random

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    expr,
    lit,
    sum as spark_sum,
    to_date,
    hour,
)


def ensure_df(spark, path):
    try:
        return (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(path)
        )
    except Exception:
        return None


def generate_synthetic(spark, n_days=60, seed=7):
    rnd = Random(seed)
    start = datetime(2023, 1, 1)
    merchants = [f"M{m:02d}" for m in range(1, 9)]
    channels = ["ecom", "pos"]
    countries = ["US", "GB", "CA", "DE", "FR"]
    rows = []
    tx_id = 1
    for d in range(n_days):
        date = start + timedelta(days=d)
        day_vol = rnd.randint(80, 160)
        for _ in range(day_vol):
            ts = date + timedelta(hours=rnd.randint(0, 23), minutes=rnd.randint(0, 59))
            amount = round(rnd.uniform(5, 900), 2)
            merchant = rnd.choice(merchants)
            channel = rnd.choices(channels, weights=[0.65, 0.35])[0]
            country = rnd.choices(countries, weights=[0.6, 0.15, 0.1, 0.1, 0.05])[0]
            card_present = 1 if channel == "pos" and rnd.random() > 0.15 else 0
            is_international = 1 if country != "US" else 0
            # Simple fraud propensity: higher for high amount, ecom, international, not-present
            p = 0.005
            if amount > 400:
                p += 0.02
            if channel == "ecom":
                p += 0.015
            if is_international:
                p += 0.015
            if card_present == 0:
                p += 0.01
            is_fraud = 1 if rnd.random() < p else 0
            rows.append(
                (
                    tx_id,
                    ts.strftime("%Y-%m-%d %H:%M:%S"),
                    float(amount),
                    merchant,
                    channel,
                    country,
                    int(card_present),
                    int(is_international),
                    int(is_fraud),
                )
            )
            tx_id += 1

    schema = "tx_id int, timestamp string, amount double, merchant string, channel string, country string, card_present int, is_international int, is_fraud int"
    return spark.createDataFrame(rows, schema=schema)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Fraud ETL with PySpark -> JSON for charts")
    parser.add_argument("--input", default="data/fraud_transactions.csv", help="Input CSV path (optional; synthetic data used if missing)")
    parser.add_argument("--output", default="docs/assets/fraud_data.json", help="Output JSON for dashboard")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("fraud_etl").getOrCreate()

    df = ensure_df(spark, args.input)
    if df is None or df.rdd.isEmpty():
        df = generate_synthetic(spark)

    # Basic types
    df = df.withColumn("amount", col("amount").cast("double")).withColumn("is_fraud", col("is_fraud").cast("int"))
    df = df.withColumn("ts", col("timestamp").cast("timestamp"))
    df = df.withColumn("date", to_date(col("ts")))

    # Time series aggregates
    daily = (
        df.groupBy("date")
        .agg(
            count(lit(1)).alias("tx_count"),
            spark_sum(expr("CASE WHEN is_fraud=1 THEN 1 ELSE 0 END")).alias("fraud_count"),
            spark_sum(col("amount")).alias("total_amount"),
            spark_sum(expr("CASE WHEN is_fraud=1 THEN amount ELSE 0 END")).alias("fraud_amount"),
        )
        .orderBy("date")
    )

    daily_rows = daily.collect()
    dates = [str(r["date"]) for r in daily_rows]
    tx_count = [int(r["tx_count"]) for r in daily_rows]
    fraud_count = [int(r["fraud_count"]) for r in daily_rows]
    fraud_rate = [round((r["fraud_count"] / r["tx_count"]) * 100.0, 2) if r["tx_count"] else 0.0 for r in daily_rows]
    total_amount = [float(r["total_amount"]) for r in daily_rows]
    fraud_amount = [float(r["fraud_amount"]) for r in daily_rows]

    # Top merchants by fraud_amount
    merch = (
        df.groupBy("merchant")
        .agg(
            count(lit(1)).alias("tx_count"),
            spark_sum(expr("CASE WHEN is_fraud=1 THEN 1 ELSE 0 END")).alias("fraud_count"),
            spark_sum(expr("CASE WHEN is_fraud=1 THEN amount ELSE 0 END")).alias("fraud_amount"),
        )
        .withColumn("fraud_rate", expr("ROUND((fraud_count/tx_count)*100.0, 2)"))
        .orderBy(col("fraud_amount").desc())
    )
    top_merchants = [
        {
            "merchant": r["merchant"],
            "tx_count": int(r["tx_count"] or 0),
            "fraud_count": int(r["fraud_count"] or 0),
            "fraud_amount": float(r["fraud_amount"] or 0.0),
            "fraud_rate": float(r["fraud_rate"] or 0.0),
        }
        for r in merch.limit(12).collect()
    ]

    # Channel breakdown (e.g., ecom vs pos)
    chan = (
        df.groupBy("channel")
        .agg(
            spark_sum(expr("CASE WHEN is_fraud=1 THEN amount ELSE 0 END")).alias("fraud_amount"),
            spark_sum(expr("CASE WHEN is_fraud=0 THEN amount ELSE 0 END")).alias("legit_amount"),
            count(lit(1)).alias("tx_count"),
            spark_sum(expr("CASE WHEN is_fraud=1 THEN 1 ELSE 0 END")).alias("fraud_count"),
        )
        .withColumn("fraud_rate", expr("ROUND((fraud_count/tx_count)*100.0, 2)"))
        .orderBy(col("fraud_amount").desc())
    )
    channels = [
        {
            "channel": r["channel"],
            "fraud_amount": float(r["fraud_amount"] or 0.0),
            "legit_amount": float(r["legit_amount"] or 0.0),
            "fraud_rate": float(r["fraud_rate"] or 0.0),
            "tx_count": int(r["tx_count"] or 0),
        }
        for r in chan.collect()
    ]

    # Hour-of-day overview
    hour_df = (
        df.withColumn("hour", hour(col("ts")))
        .groupBy("hour")
        .agg(
            count(lit(1)).alias("tx_count"),
            spark_sum(expr("CASE WHEN is_fraud=1 THEN 1 ELSE 0 END")).alias("fraud_count"),
        )
        .orderBy("hour")
    )
    hours = []
    for r in hour_df.collect():
        txc = int(r["tx_count"]) if r["tx_count"] is not None else 0
        frc = int(r["fraud_count"]) if r["fraud_count"] is not None else 0
        fr = round((frc / txc) * 100.0, 2) if txc else 0.0
        hours.append({"hour": int(r["hour"]), "tx_count": txc, "fraud_rate": fr})

    payload = {
        "dates": dates,
        "tx_count": tx_count,
        "fraud_count": fraud_count,
        "fraud_rate": fraud_rate,  # percentage
        "total_amount": total_amount,
        "fraud_amount": fraud_amount,
        "top_merchants": top_merchants,
        "channels": channels,
        "hours": hours,
    }

    out_path = args.output
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    print(f"Wrote fraud dashboard data -> {out_path}")

    spark.stop()


if __name__ == "__main__":
    main()

