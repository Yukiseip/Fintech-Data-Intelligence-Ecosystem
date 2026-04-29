"""
Anomaly Detection Job.

Implements 3 fraud detection rules on Silver data:
1. VELOCITY_ATTACK: >= 10 transactions from same user in 60 seconds
2. HIGH_AMOUNT:     > 500% of user's 30-day average
3. GEO_IMPOSSIBLE:  Physically impossible travel (> 800 km/h)

Writes all alerts to gold.alerts_log via JDBC.
Updates fact_transactions.is_flagged_fraud via a subsequent
PostgresOperator in the Airflow DAG.

Usage:
    spark-submit jobs/anomaly_detection.py --date 2026-04-24
"""

from __future__ import annotations
import argparse
import logging
import math
import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window

from processing.utils.spark_session import get_spark_session
from processing.utils.audit_logger import AuditLogger

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


# ── Rule 1: Velocity Attack ────────────────────────────────────────────

def detect_velocity_attack(df: DataFrame) -> DataFrame:
    """Detect users with >= 10 transactions in a 60-second rolling window.

    Uses a range-based window ordered by unix epoch seconds to count
    concurrent transactions per user.

    Args:
        df: Silver DataFrame with user_id and timestamp columns.

    Returns:
        DataFrame with columns: transaction_id, reason_code,
        severity_score, description.
    """
    window = (
        Window.partitionBy("user_id")
        .orderBy(F.col("timestamp").cast("long"))
        .rangeBetween(-60, 0)
    )

    velocity_df = df.withColumn(
        "txn_count_60s", F.count("transaction_id").over(window)
    )

    return (
        velocity_df
        .filter(F.col("txn_count_60s") >= 10)
        .select(
            "transaction_id",
            F.lit("VELOCITY_ATTACK").alias("reason_code"),
            F.least(
                F.col("txn_count_60s") / 20.0, F.lit(1.0)
            ).cast(FloatType()).alias("severity_score"),
            F.concat(
                F.lit("User had "),
                F.col("txn_count_60s").cast("string"),
                F.lit(" transactions in 60 seconds"),
            ).alias("description"),
        )
    )


# ── Rule 2: High Amount Deviation ──────────────────────────────────────

def detect_high_amount(df: DataFrame, lookback_days: int = 30) -> DataFrame:
    """Detect transactions > 500% of the user's rolling 30-day average.

    Only includes transactions within lookback_days when computing the
    user average to avoid stale baselines.

    Args:
        df: Silver DataFrame with user_id, amount, and timestamp columns.
        lookback_days: Number of days to include in the rolling average.

    Returns:
        DataFrame with columns: transaction_id, reason_code,
        severity_score, description.
    """
    cutoff = F.current_date() - F.expr(f"INTERVAL {lookback_days} DAYS")

    user_stats = (
        df
        .filter(F.col("timestamp") >= cutoff)
        .groupBy("user_id")
        .agg(
            F.avg("amount").alias("avg_amount"),
            F.stddev("amount").alias("stddev_amount"),
        )
    )

    return (
        df.join(user_stats, on="user_id", how="inner")
        .filter(
            (F.col("amount") > F.col("avg_amount") * 5.0) &
            (F.col("avg_amount") > 0)
        )
        .select(
            "transaction_id",
            F.lit("HIGH_AMOUNT").alias("reason_code"),
            F.least(
                (F.col("amount") - F.col("avg_amount")) / (F.col("avg_amount") * 5.0),
                F.lit(1.0),
            ).cast(FloatType()).alias("severity_score"),
            F.concat(
                F.lit("Amount $"),
                F.col("amount").cast("string"),
                F.lit(" vs user avg $"),
                F.round(F.col("avg_amount"), 2).cast("string"),
            ).alias("description"),
        )
    )


# ── Rule 3: Geographic Impossibility ───────────────────────────────────

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance in km between two coordinates using the Haversine formula.

    Args:
        lat1: Latitude of point 1 (degrees).
        lon1: Longitude of point 1 (degrees).
        lat2: Latitude of point 2 (degrees).
        lon2: Longitude of point 2 (degrees).

    Returns:
        Distance in kilometres as a float.
    """
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    return 2 * R * math.asin(math.sqrt(a))


def detect_geo_impossible(df: DataFrame) -> DataFrame:
    """Detect impossible travel: same user, different countries, implied speed > 800 km/h.

    Uses lag() window to compare consecutive transactions per user ordered
    by timestamp.

    Args:
        df: Silver DataFrame with user_id, latitude, longitude,
            country_code, and timestamp columns.

    Returns:
        DataFrame with columns: transaction_id, reason_code,
        severity_score, description.
    """
    window = Window.partitionBy("user_id").orderBy("timestamp")

    df_with_prev = (
        df
        .withColumn("prev_lat",       F.lag("latitude").over(window))
        .withColumn("prev_lon",       F.lag("longitude").over(window))
        .withColumn("prev_timestamp", F.lag("timestamp").over(window))
        .withColumn("prev_country",   F.lag("country_code").over(window))
    )

    # Native Spark SQL Haversine calculation to avoid Python UDFs
    lat1 = F.radians(F.col("prev_lat"))
    lon1 = F.radians(F.col("prev_lon"))
    lat2 = F.radians(F.col("latitude"))
    lon2 = F.radians(F.col("longitude"))
    
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = F.pow(F.sin(dlat / 2), 2) + F.cos(lat1) * F.cos(lat2) * F.pow(F.sin(dlon / 2), 2)
    c = 2 * F.asin(F.sqrt(a))
    distance_expr = 6371.0 * c

    return (
        df_with_prev
        .filter(
            F.col("prev_lat").isNotNull() &
            (F.col("country_code") != F.col("prev_country"))
        )
        .withColumn("distance_km", distance_expr)
        .withColumn(
            "time_diff_hours",
            (F.unix_timestamp("timestamp") - F.unix_timestamp("prev_timestamp")) / 3600.0,
        )
        .withColumn(
            "implied_speed_kmh",
            F.col("distance_km") / F.when(
                F.col("time_diff_hours") > 0, F.col("time_diff_hours")
            ).otherwise(F.lit(0.001)),
        )
        .filter(F.col("implied_speed_kmh") > 800)
        .select(
            "transaction_id",
            F.lit("GEO_IMPOSSIBLE").alias("reason_code"),
            F.least(
                F.col("implied_speed_kmh") / 2000.0, F.lit(1.0)
            ).cast(FloatType()).alias("severity_score"),
            F.concat(
                F.lit("Impossible travel: "),
                F.round(F.col("distance_km"), 0).cast("string"),
                F.lit("km in "),
                F.round(F.col("time_diff_hours"), 2).cast("string"),
                F.lit("h ("),
                F.round(F.col("implied_speed_kmh"), 0).cast("string"),
                F.lit(" km/h)"),
            ).alias("description"),
        )
    )


# ── Main orchestrator ──────────────────────────────────────────────────

def run_anomaly_detection(date: str, s3_bucket: str) -> dict:
    """Execute anomaly detection for a given partition date.

    Runs all 3 rules, unions results, writes to gold.alerts_log via JDBC.

    Args:
        date: Processing date in YYYY-MM-DD format.
        s3_bucket: S3 bucket name (e.g., 'fintech-raw-data').

    Returns:
        dict with total_alerts and per-rule breakdown.
    """
    audit = AuditLogger("anomaly_detection")
    start_time = datetime.now(timezone.utc)

    spark = get_spark_session("AnomalyDetection")

    base_path = f"s3a://{s3_bucket}/silver/transactions/"
    silver_path = f"s3a://{s3_bucket}/silver/transactions/partition_date={date}/"
    
    try:
        silver_df = spark.read.option("basePath", base_path).parquet(silver_path)
    except Exception as e:
        if "Path does not exist" in str(e):
            logger.warning("No Silver data found for date=%s. Skipping anomaly detection.", date)
            spark.stop()
            return {"total_alerts": 0}
        raise e

    input_rows = silver_df.count()
    logger.info("Anomaly detection input rows: %d", input_rows)

    if input_rows == 0:
        logger.warning("No Silver data for date=%s. Exiting.", date)
        spark.stop()
        return {"total_alerts": 0}

    # Run all rules
    velocity_alerts = detect_velocity_attack(silver_df)
    amount_alerts   = detect_high_amount(silver_df)
    geo_alerts      = detect_geo_impossible(silver_df)

    velocity_count = velocity_alerts.count()
    amount_count   = amount_alerts.count()
    geo_count      = geo_alerts.count()

    logger.info(
        "Alerts — VELOCITY_ATTACK: %d | HIGH_AMOUNT: %d | GEO_IMPOSSIBLE: %d",
        velocity_count, amount_count, geo_count,
    )

    # Combine all alerts with audit metadata
    all_alerts = (
        velocity_alerts
        .union(amount_alerts)
        .union(geo_alerts)
        .withColumn("alert_timestamp", F.current_timestamp())
        .withColumn("status",          F.lit("open"))
        .withColumn("ml_score",        F.lit(None).cast(FloatType()))
        .withColumn("rule_score",      F.col("severity_score"))
    )

    total_alerts = velocity_count + amount_count + geo_count

    if total_alerts > 0:
        jdbc_url = (
            f"jdbc:postgresql://"
            f"{os.environ['POSTGRES_HOST']}:"
            f"{os.environ.get('POSTGRES_PORT', '5432')}/"
            f"{os.environ['POSTGRES_DB']}"
        )
        props = {
            "user":       os.environ["POSTGRES_USER"],
            "password":   os.environ["POSTGRES_PASSWORD"],
            "driver":     "org.postgresql.Driver",
            "stringtype": "unspecified",
        }

        all_alerts.write.jdbc(
            url=jdbc_url,
            table="gold.alerts_log",
            mode="append",
            properties=props,
        )
        logger.info("Written %d alerts to gold.alerts_log", total_alerts)

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()

    audit.log(
        run_id=f"anomaly-{date}",
        input_rows=input_rows,
        total_alerts=total_alerts,
        velocity_alerts=velocity_count,
        amount_alerts=amount_count,
        geo_alerts=geo_count,
        duration_seconds=round(duration, 2),
    )

    spark.stop()
    return {
        "total_alerts":     total_alerts,
        "velocity_alerts":  velocity_count,
        "amount_alerts":    amount_count,
        "geo_alerts":       geo_count,
        "duration_seconds": duration,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Anomaly Detection Spark Job")
    parser.add_argument("--date",   required=True, help="Processing date YYYY-MM-DD")
    parser.add_argument("--bucket", default="fintech-raw-data", help="S3 bucket name")
    args = parser.parse_args()

    result = run_anomaly_detection(args.date, args.bucket)
    logger.info("Result: %s", result)
