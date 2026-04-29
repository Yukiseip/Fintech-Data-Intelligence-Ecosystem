"""Unit tests for anomaly detection rules (pytest + local Spark)."""

from __future__ import annotations
import pytest
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

from processing.jobs.anomaly_detection import (
    detect_velocity_attack,
    detect_high_amount,
    detect_geo_impossible,
)


@pytest.fixture(scope="module")
def spark():
    """Local SparkSession for unit tests."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test-anomaly")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


# ── Velocity Attack ─────────────────────────────────────────────────

class TestVelocityAttack:

    def test_alert_triggered_at_10_transactions(self, spark):
        """12 txns from same user within 60s must trigger VELOCITY_ATTACK."""
        base = datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc)
        records = [
            {
                "transaction_id": f"t{i}",
                "user_id": "u1",
                "timestamp": base + timedelta(seconds=i * 5),  # 12 txns in 55s
                "amount": 10.0,
            }
            for i in range(12)
        ]
        df = spark.createDataFrame(records)
        alerts = detect_velocity_attack(df)

        assert alerts.count() > 0
        first = alerts.collect()[0]
        assert first["reason_code"] == "VELOCITY_ATTACK"
        assert first["severity_score"] > 0.5

    def test_no_alert_below_threshold(self, spark):
        """5 txns in 60s must NOT trigger VELOCITY_ATTACK."""
        base = datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc)
        records = [
            {
                "transaction_id": f"t{i}",
                "user_id": "u1",
                "timestamp": base + timedelta(seconds=i * 10),
                "amount": 10.0,
            }
            for i in range(5)
        ]
        df = spark.createDataFrame(records)
        alerts = detect_velocity_attack(df)
        assert alerts.count() == 0

    def test_different_users_isolated(self, spark):
        """High velocity for user A must not flag user B."""
        base = datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc)
        # 12 txns for u1, 3 txns for u2
        records = (
            [{"transaction_id": f"a{i}", "user_id": "u1",
              "timestamp": base + timedelta(seconds=i), "amount": 10.0}
             for i in range(12)]
            +
            [{"transaction_id": f"b{i}", "user_id": "u2",
              "timestamp": base + timedelta(seconds=i * 30), "amount": 10.0}
             for i in range(3)]
        )
        df = spark.createDataFrame(records)
        alerts = detect_velocity_attack(df)

        flagged_users = {row["transaction_id"][0] for row in alerts.collect()}
        assert "a" in flagged_users  # u1 txns flagged
        # u2 txns must not appear (b-prefixed)
        assert not any(r.startswith("b") for r in flagged_users)


# ── High Amount ──────────────────────────────────────────────────────

class TestHighAmount:

    def test_alert_triggered_when_exceeds_500pct(self, spark):
        """Transaction > 500% of user avg must trigger HIGH_AMOUNT."""
        base = datetime(2026, 4, 1, tzinfo=timezone.utc)
        # 30 normal txns ($10), 1 huge txn ($1000 = 100× avg)
        records = [
            {"transaction_id": f"n{i}", "user_id": "u1",
             "amount": 10.0, "timestamp": base + timedelta(days=i)}
            for i in range(30)
        ]
        records.append({
            "transaction_id": "fraud-big",
            "user_id": "u1",
            "amount": 1000.0,
            "timestamp": datetime(2026, 4, 24, tzinfo=timezone.utc),
        })
        df = spark.createDataFrame(records)
        alerts = detect_high_amount(df)

        fraud_alert = alerts.filter(alerts.transaction_id == "fraud-big")
        assert fraud_alert.count() == 1
        assert fraud_alert.collect()[0]["reason_code"] == "HIGH_AMOUNT"

    def test_no_alert_for_normal_amount(self, spark):
        """Transactions within 4× user avg must NOT trigger HIGH_AMOUNT."""
        base = datetime(2026, 4, 1, tzinfo=timezone.utc)
        records = [
            {"transaction_id": f"n{i}", "user_id": "u1",
             "amount": 100.0, "timestamp": base + timedelta(days=i)}
            for i in range(30)
        ]
        records.append({
            "transaction_id": "normal-big",
            "user_id": "u1",
            "amount": 350.0,  # 3.5× avg — below threshold
            "timestamp": datetime(2026, 4, 24, tzinfo=timezone.utc),
        })
        df = spark.createDataFrame(records)
        alerts = detect_high_amount(df)
        assert alerts.filter(alerts.transaction_id == "normal-big").count() == 0


# ── Geo Impossible ──────────────────────────────────────────────────

class TestGeoImpossible:

    def test_alert_triggered_for_impossible_travel(self, spark):
        """Mexico City → Tokyo in 30 minutes must trigger GEO_IMPOSSIBLE."""
        records = [
            {
                "transaction_id": "t1",
                "user_id": "u1",
                "timestamp": datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
                "latitude":  19.4326,
                "longitude": -99.1332,
                "country_code": "MX",
                "amount": 50.0,
            },
            {
                "transaction_id": "t2",
                "user_id": "u1",
                "timestamp": datetime(2026, 4, 24, 10, 30, tzinfo=timezone.utc),
                "latitude":  35.6762,
                "longitude": 139.6503,
                "country_code": "JP",
                "amount": 50.0,
            },
        ]
        df = spark.createDataFrame(records)
        alerts = detect_geo_impossible(df)

        assert alerts.count() == 1
        row = alerts.collect()[0]
        assert row["reason_code"] == "GEO_IMPOSSIBLE"
        assert row["severity_score"] > 0.9

    def test_no_alert_for_nearby_cities(self, spark):
        """Transactions in same country must NOT trigger GEO_IMPOSSIBLE."""
        records = [
            {
                "transaction_id": "t1",
                "user_id": "u1",
                "timestamp": datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
                "latitude":  19.4326,
                "longitude": -99.1332,
                "country_code": "MX",
                "amount": 50.0,
            },
            {
                "transaction_id": "t2",
                "user_id": "u1",
                "timestamp": datetime(2026, 4, 24, 11, 0, tzinfo=timezone.utc),
                "latitude":  20.6534,
                "longitude": -103.3484,
                "country_code": "MX",  # same country
                "amount": 50.0,
            },
        ]
        df = spark.createDataFrame(records)
        alerts = detect_geo_impossible(df)
        assert alerts.count() == 0
