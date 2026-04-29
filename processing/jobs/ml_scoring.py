"""
ML Anomaly Scoring Job — Isolation Forest.

Stretch goal (S3-D2): trains an Isolation Forest model on Silver transaction
features and produces ml_score for each transaction.

The scores are written to gold.alerts_log (ml_score column) for records
that already have a rule_score alert. For new detections, a new row is
inserted with reason_code='ML_ANOMALY'.

Usage:
    spark-submit jobs/ml_scoring.py --date 2026-04-24 [--train]
"""

from __future__ import annotations
import argparse
import logging
import os
import pickle
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from processing.utils.audit_logger import AuditLogger

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────
MODEL_DIR = Path(os.environ.get("ML_MODEL_DIR", "/tmp/fintech_models"))
MODEL_PATH = MODEL_DIR / "isolation_forest.pkl"
SCALER_PATH = MODEL_DIR / "scaler.pkl"

# Features used for anomaly scoring
FEATURE_COLS = [
    "amount",
    "latitude",
    "longitude",
]


# ── Helpers ────────────────────────────────────────────────────────────

def get_pg_conn():
    """Build psycopg2 connection from environment variables.

    Returns:
        psycopg2 connection object.

    Raises:
        KeyError: If required PostgreSQL env vars are missing.
    """
    import psycopg2
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def load_silver_to_pandas(date: str, s3_bucket: str) -> pd.DataFrame:
    """Load Silver Parquet for a given date into a pandas DataFrame.

    Args:
        date: Processing date in YYYY-MM-DD format.
        s3_bucket: S3 bucket name.

    Returns:
        pandas DataFrame with Silver transactions.
    """
    from processing.utils.spark_session import get_spark_session
    spark = get_spark_session("MLScoring-Load")
    path = f"s3a://{s3_bucket}/silver/transactions/partition_date={date}/"
    df = spark.read.parquet(path).select(
        "transaction_id", "user_id", *FEATURE_COLS
    ).toPandas()
    spark.stop()
    return df


# ── Training ───────────────────────────────────────────────────────────

def train_model(df: pd.DataFrame) -> tuple:
    """Train an Isolation Forest model on transaction features.

    Args:
        df: pandas DataFrame with FEATURE_COLS columns.

    Returns:
        Tuple of (trained IsolationForest, fitted StandardScaler).
    """
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    df_clean = df[FEATURE_COLS].dropna()
    X = df_clean.values.astype(float)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = IsolationForest(
        n_estimators=200,
        contamination=float(os.environ.get("GENERATOR_FRAUD_RATE", "0.02")),
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_scaled)

    with open(MODEL_PATH, "wb") as f:
        pickle.dump(model, f)
    with open(SCALER_PATH, "wb") as f:
        pickle.dump(scaler, f)

    logger.info(
        "Isolation Forest trained on %d samples. Model saved to %s",
        len(X), MODEL_PATH,
    )
    return model, scaler


def load_model() -> tuple:
    """Load pre-trained Isolation Forest and scaler from disk.

    Returns:
        Tuple of (IsolationForest, StandardScaler).

    Raises:
        FileNotFoundError: If model files do not exist.
    """
    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"No trained model at {MODEL_PATH}. Run with --train flag first."
        )
    with open(MODEL_PATH, "rb") as f:
        model = pickle.load(f)
    with open(SCALER_PATH, "rb") as f:
        scaler = pickle.load(f)
    logger.info("Loaded Isolation Forest model from %s", MODEL_PATH)
    return model, scaler


# ── Scoring ────────────────────────────────────────────────────────────

def score_transactions(df: pd.DataFrame, model, scaler) -> pd.DataFrame:
    """Apply Isolation Forest to produce anomaly scores.

    Isolation Forest returns +1 (normal) or -1 (anomaly).
    We convert the decision function output to a [0, 1] score where
    1.0 = most anomalous.

    Args:
        df: pandas DataFrame with FEATURE_COLS.
        model: Fitted IsolationForest instance.
        scaler: Fitted StandardScaler instance.

    Returns:
        DataFrame with added 'ml_score' column in [0, 1].
    """
    df_copy = df.copy()
    features = df_copy[FEATURE_COLS].fillna(0).values.astype(float)
    X_scaled = scaler.transform(features)

    # decision_function: more negative = more anomalous
    decision = model.decision_function(X_scaled)

    # Normalize to [0, 1] where 1 = most anomalous
    d_min, d_max = decision.min(), decision.max()
    if d_max > d_min:
        ml_score = 1.0 - (decision - d_min) / (d_max - d_min)
    else:
        ml_score = np.zeros(len(decision))

    df_copy["ml_score"] = np.round(ml_score, 4)
    return df_copy


def write_ml_scores(df_scored: pd.DataFrame, date: str) -> int:
    """Write ML scores to gold.alerts_log for highly anomalous transactions.

    Only writes transactions with ml_score > 0.7 that do NOT already
    have a rule-based alert (to avoid duplicates).

    Args:
        df_scored: Scored pandas DataFrame with ml_score column.
        date: Processing date string.

    Returns:
        Number of ML alerts written.
    """
    threshold = float(os.environ.get("ML_SCORE_THRESHOLD", "0.7"))
    high_anomaly = df_scored[df_scored["ml_score"] > threshold].copy()

    if high_anomaly.empty:
        logger.info("No high-anomaly transactions above threshold %.2f", threshold)
        return 0

    conn = get_pg_conn()
    cur = conn.cursor()

    # Check existing alerts to avoid duplicates
    existing_ids = set()
    if not high_anomaly.empty:
        placeholders = ",".join(["%s"] * len(high_anomaly))
        cur.execute(
            f"SELECT transaction_id FROM gold.alerts_log WHERE transaction_id::text IN ({placeholders})",
            list(high_anomaly["transaction_id"].astype(str)),
        )
        existing_ids = {str(row[0]) for row in cur.fetchall()}

    new_alerts = high_anomaly[
        ~high_anomaly["transaction_id"].astype(str).isin(existing_ids)
    ]

    if new_alerts.empty:
        logger.info("All high-anomaly transactions already have alerts.")
        conn.close()
        return 0

    now = datetime.now(timezone.utc)
    insert_data = [
        (
            str(row["transaction_id"]),
            now,
            "ML_ANOMALY",
            float(row["ml_score"]),
            f"Isolation Forest score: {row['ml_score']:.4f}",
            "open",
            float(row["ml_score"]),  # ml_score
            None,                    # rule_score
        )
        for _, row in new_alerts.iterrows()
    ]

    cur.executemany(
        """
        INSERT INTO gold.alerts_log
            (transaction_id, alert_timestamp, reason_code, severity_score,
             description, status, ml_score, rule_score)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        insert_data,
    )
    conn.commit()
    conn.close()

    written = len(insert_data)
    logger.info("Written %d ML anomaly alerts to gold.alerts_log", written)
    return written


# ── Main orchestrator ──────────────────────────────────────────────────

def run_ml_scoring(date: str, s3_bucket: str, train: bool = False) -> dict:
    """Execute ML scoring pipeline for a given partition date.

    Args:
        date: Processing date in YYYY-MM-DD format.
        s3_bucket: S3 bucket name.
        train: If True, trains a new model before scoring.

    Returns:
        dict with ml_alerts and duration_seconds.
    """
    audit = AuditLogger("ml_scoring")
    start_time = datetime.now(timezone.utc)

    logger.info("Loading Silver data for date=%s...", date)
    df = load_silver_to_pandas(date, s3_bucket)

    if df.empty:
        logger.warning("No Silver data for date=%s. Exiting.", date)
        return {"ml_alerts": 0, "duration_seconds": 0}

    if train or not MODEL_PATH.exists():
        logger.info("Training Isolation Forest on %d records...", len(df))
        model, scaler = train_model(df)
    else:
        model, scaler = load_model()

    df_scored = score_transactions(df, model, scaler)
    ml_alerts = write_ml_scores(df_scored, date)

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    audit.log(
        run_id=f"ml-{date}",
        input_rows=len(df),
        ml_alerts=ml_alerts,
        duration_seconds=round(duration, 2),
    )

    logger.info(
        "ML scoring complete. Input: %d, Alerts: %d, Duration: %.1fs",
        len(df), ml_alerts, duration,
    )
    return {"ml_alerts": ml_alerts, "duration_seconds": duration}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ML Anomaly Scoring Job")
    parser.add_argument("--date",   required=True,      help="Processing date YYYY-MM-DD")
    parser.add_argument("--bucket", default="fintech-raw-data")
    parser.add_argument("--train",  action="store_true", help="Train a fresh model")
    args = parser.parse_args()

    result = run_ml_scoring(args.date, args.bucket, train=args.train)
    logger.info("Result: %s", result)
