#!/usr/bin/env python
import json
import math
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from random import Random
from typing import List, Tuple

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


@dataclass
class Txn:
    tx_id: int
    ts: str
    account_id: str
    customer_id: str
    channel: str
    mcc: str
    merchant_id: str
    amount: float
    balance: float
    currency: str
    lat: float
    lon: float
    home_lat: float
    home_lon: float
    device_id: str
    ip_country: str
    merchant_country: str
    card_present: int
    is_international: int
    is_fraud: int
    risk_score: float


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def synth_bank_data(days=60, n_customers=80, seed=17) -> List[Txn]:
    rnd = Random(seed)
    start = datetime(2023, 3, 1)
    # Simple US home base cities (lat, lon)
    cities = [
        (40.71, -74.00),  # NYC
        (34.05, -118.24),  # LA
        (41.88, -87.63),  # Chicago
        (29.76, -95.36),  # Houston
        (33.45, -112.07),  # Phoenix
        (32.78, -96.80),  # Dallas
        (39.95, -75.16),  # Philly
        (29.42, -98.49),  # San Antonio
        (32.71, -117.16),  # San Diego
        (37.77, -122.41),  # SF
    ]
    channels = ["pos", "ecom", "atm", "wire"]
    mccs = ["5411", "5812", "5999", "5722", "4111", "6011"]  # grocery, restaurant, misc, electronics, transit, atm
    countries = ["US", "GB", "CA", "DE", "FR", "CN", "JP", "BR", "MX"]

    def jitter_home(lat, lon) -> Tuple[float, float]:
        return lat + rnd.uniform(-0.3, 0.3), lon + rnd.uniform(-0.3, 0.3)

    txns: List[Txn] = []
    tx_id = 1
    for cust in range(1, n_customers + 1):
        base_lat, base_lon = cities[rnd.randrange(len(cities))]
        home_lat, home_lon = jitter_home(base_lat, base_lon)
        n_accounts = rnd.choice([1, 1, 2, 2, 3])
        for a in range(n_accounts):
            account_id = f"A{cust:03d}{a:01d}"
            balance = rnd.uniform(2_000, 15_000)
            device_id = f"D{cust:03d}"
            for d in range(days):
                day = start + timedelta(days=d)
                # typical daily tx volume per account
                n_tx = max(0, int(rnd.gauss(3.5, 1.8)))
                # Occasional travel/international bursts
                traveling = rnd.random() < 0.04
                travel_lat, travel_lon = home_lat, home_lon
                travel_country = "US"
                if traveling:
                    # Far location
                    far_places = [(48.85, 2.35, "FR"), (51.50, -0.12, "GB"), (35.68, 139.69, "JP"), (19.43, -99.13, "MX")]
                    travel_lat, travel_lon, travel_country = rnd.choice(far_places)
                for _ in range(n_tx):
                    hour_of_day = int(min(23, max(0, rnd.gauss(13, 5))))
                    ts = day + timedelta(hours=hour_of_day, minutes=rnd.randint(0, 59), seconds=rnd.randint(0, 59))
                    channel = rnd.choices(channels, weights=[0.55, 0.30, 0.10, 0.05])[0]
                    mcc = rnd.choice(mccs)
                    merchant_id = f"M{rnd.randint(1000, 9999)}"
                    amount_base = {
                        "pos": rnd.uniform(5, 120),
                        "ecom": rnd.uniform(5, 300),
                        "atm": rnd.uniform(40, 400),
                        "wire": rnd.uniform(200, 5000),
                    }[channel]
                    # Rare high value spikes
                    if rnd.random() < 0.02:
                        amount_base *= rnd.uniform(3, 8)
                    amount = round(amount_base, 2)

                    # Location: mostly near home unless traveling
                    if traveling and rnd.random() < 0.85:
                        lat, lon = travel_lat + rnd.uniform(-0.1, 0.1), travel_lon + rnd.uniform(-0.1, 0.1)
                        merchant_country = travel_country
                    else:
                        lat, lon = home_lat + rnd.uniform(-0.15, 0.15), home_lon + rnd.uniform(-0.15, 0.15)
                        merchant_country = "US"
                    ip_country = merchant_country if rnd.random() < 0.9 else rnd.choice(countries)

                    # Card presence
                    card_present = 1 if channel in ("pos", "atm") else 0
                    is_international = 1 if merchant_country != "US" else 0

                    # Heuristic risk signals
                    night = 1 if hour_of_day <= 5 else 0
                    far_km = haversine_km(home_lat, home_lon, lat, lon)
                    far_from_home = 1 if far_km > 500 else 0
                    high_amount = 1 if amount > 1500 or (channel == "wire" and amount > 3000) else 0
                    device_change = 1 if rnd.random() < 0.01 else 0
                    if device_change:
                        device_id = f"D{cust:03d}x{rnd.randint(1,9)}"

                    # Combine into risk score [0,1]
                    risk = (
                        0.28 * is_international +
                        0.22 * high_amount +
                        0.18 * (1 - card_present) +
                        0.14 * far_from_home +
                        0.10 * night +
                        0.08 * device_change
                    )
                    # Small random noise
                    risk = min(1.0, max(0.0, risk + rnd.uniform(-0.05, 0.05)))
                    # Label as fraud with probability increasing with risk
                    fraud_prob = max(0.01, risk * 0.85)
                    is_fraud = 1 if rnd.random() < fraud_prob and amount > 10 else 0

                    balance = max(0.0, round(balance - amount, 2))
                    txns.append(Txn(
                        tx_id=tx_id,
                        ts=ts.strftime("%Y-%m-%d %H:%M:%S"),
                        account_id=account_id,
                        customer_id=f"C{cust:03d}",
                        channel=channel,
                        mcc=mcc,
                        merchant_id=merchant_id,
                        amount=float(amount),
                        balance=float(balance),
                        currency="USD",
                        lat=float(lat),
                        lon=float(lon),
                        home_lat=float(home_lat),
                        home_lon=float(home_lon),
                        device_id=device_id,
                        ip_country=ip_country,
                        merchant_country=merchant_country,
                        card_present=int(card_present),
                        is_international=int(is_international),
                        is_fraud=int(is_fraud),
                        risk_score=float(round(risk, 4)),
                    ))
                    tx_id += 1

    return txns


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Bank anti‑fraud ETL (PySpark) → JSON for Pages dashboard")
    parser.add_argument("--output", default="docs/assets/bank_data.json", help="Output JSON path")
    parser.add_argument("--days", type=int, default=60, help="Number of days synthetic data")
    parser.add_argument("--customers", type=int, default=80, help="Number of customers synthetic data")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("bank_fraud_etl").getOrCreate()

    # Generate synthetic bank transactions
    rows = [asdict(t) for t in synth_bank_data(days=args.days, n_customers=args.customers)]
    schema = (
        "tx_id int, ts string, account_id string, customer_id string, channel string, mcc string, merchant_id string, "
        "amount double, balance double, currency string, lat double, lon double, home_lat double, home_lon double, "
        "device_id string, ip_country string, merchant_country string, card_present int, is_international int, is_fraud int, risk_score double"
    )
    df = spark.createDataFrame([tuple(r.values()) for r in rows], schema=schema)
    df = df.withColumn("date", to_date(col("ts")))

    # Daily aggregates
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
    drows = daily.collect()
    dates = [str(r["date"]) for r in drows]
    tx_count = [int(r["tx_count"]) for r in drows]
    fraud_count = [int(r["fraud_count"]) for r in drows]
    fraud_rate = [round((r["fraud_count"]/r["tx_count"]) * 100.0, 2) if r["tx_count"] else 0.0 for r in drows]
    total_amount = [float(r["total_amount"]) for r in drows]
    fraud_amount = [float(r["fraud_amount"]) for r in drows]

    # Top accounts by fraud amount
    top_acc_df = (
        df.groupBy("account_id")
        .agg(
            spark_sum(expr("CASE WHEN is_fraud=1 THEN amount ELSE 0 END")).alias("fraud_amount"),
            spark_sum(expr("CASE WHEN is_fraud=1 THEN 1 ELSE 0 END")).alias("fraud_count"),
            count(lit(1)).alias("tx_count"),
        )
        .orderBy(col("fraud_amount").desc())
    )
    top_accounts = [
        {
            "account_id": r["account_id"],
            "tx_count": int(r["tx_count"] or 0),
            "fraud_count": int(r["fraud_count"] or 0),
            "fraud_amount": float(r["fraud_amount"] or 0.0),
        }
        for r in top_acc_df.limit(15).collect()
    ]

    # Channel breakdown
    chan = (
        df.groupBy("channel")
        .agg(
            spark_sum(expr("CASE WHEN is_fraud=1 THEN amount ELSE 0 END")).alias("fraud_amount"),
            spark_sum(expr("CASE WHEN is_fraud=0 THEN amount ELSE 0 END")).alias("legit_amount"),
            count(lit(1)).alias("tx_count"),
            spark_sum(expr("CASE WHEN is_fraud=1 THEN 1 ELSE 0 END")).alias("fraud_count"),
        )
        .orderBy(col("fraud_amount").desc())
    )
    channels = [
        {
            "channel": r["channel"],
            "fraud_amount": float(r["fraud_amount"] or 0.0),
            "legit_amount": float(r["legit_amount"] or 0.0),
            "fraud_rate": float(round(((r["fraud_count"] or 0) / (r["tx_count"] or 1)) * 100.0, 2)),
            "tx_count": int(r["tx_count"] or 0),
        }
        for r in chan.collect()
    ]

    # Hour-of-day pattern
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

    # Geo points for fraud (cap to 500)
    fraud_pts = (
        df.filter(col("is_fraud") == 1)
        .select("ts", "amount", "lat", "lon", "account_id", "channel")
        .orderBy(col("ts").desc())
        .limit(500)
        .collect()
    )
    geo_points = [
        {
            "ts": r["ts"],
            "amount": float(r["amount"]),
            "lat": float(r["lat"]),
            "lon": float(r["lon"]),
            "account_id": r["account_id"],
            "channel": r["channel"],
        }
        for r in fraud_pts
    ]

    # MCC summary (top by fraud amount)
    mcc_df = (
        df.groupBy("mcc")
        .agg(
            spark_sum(expr("CASE WHEN is_fraud=1 THEN amount ELSE 0 END")).alias("fraud_amount"),
            spark_sum(expr("CASE WHEN is_fraud=1 THEN 1 ELSE 0 END")).alias("fraud_count"),
        )
        .orderBy(col("fraud_amount").desc())
    )
    mcc_top = [
        {"mcc": r["mcc"], "fraud_amount": float(r["fraud_amount"] or 0.0), "fraud_count": int(r["fraud_count"] or 0)}
        for r in mcc_df.limit(10).collect()
    ]

    payload = {
        "dates": dates,
        "tx_count": tx_count,
        "fraud_count": fraud_count,
        "fraud_rate": fraud_rate,
        "total_amount": total_amount,
        "fraud_amount": fraud_amount,
        "top_accounts": top_accounts,
        "channels": channels,
        "hours": hours,
        "geo_points": geo_points,
        "mcc_top": mcc_top,
    }

    out_path = args.output
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    print(f"Wrote bank fraud dashboard data -> {out_path}")

    spark.stop()


if __name__ == "__main__":
    main()

