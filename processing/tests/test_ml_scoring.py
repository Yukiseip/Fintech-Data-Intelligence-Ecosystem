"""Unit tests for ML Scoring (Isolation Forest)."""

from __future__ import annotations
import numpy as np
import pandas as pd
import pytest

from processing.jobs.ml_scoring import score_transactions, train_model, FEATURE_COLS


def make_sample_df(n: int = 200, fraud_pct: float = 0.05) -> pd.DataFrame:
    """Generate a synthetic transaction DataFrame for testing.

    Args:
        n: Number of rows.
        fraud_pct: Fraction of extreme outlier rows.

    Returns:
        pandas DataFrame with FEATURE_COLS + transaction_id.
    """
    rng = np.random.default_rng(42)
    normal_n = int(n * (1 - fraud_pct))
    fraud_n = n - normal_n

    normal = pd.DataFrame({
        "transaction_id": [f"n{i}" for i in range(normal_n)],
        "user_id":        [f"u{i}" for i in range(normal_n)],
        "amount":         rng.uniform(10, 500, normal_n),
        "latitude":       rng.uniform(-60, 60, normal_n),
        "longitude":      rng.uniform(-100, 100, normal_n),
    })
    # Extreme outliers that should score high
    fraud = pd.DataFrame({
        "transaction_id": [f"f{i}" for i in range(fraud_n)],
        "user_id":        [f"uf{i}" for i in range(fraud_n)],
        "amount":         rng.uniform(50_000, 100_000, fraud_n),
        "latitude":       rng.uniform(85, 90, fraud_n),
        "longitude":      rng.uniform(170, 180, fraud_n),
    })
    return pd.concat([normal, fraud], ignore_index=True)


class TestMLScoring:

    def test_score_transactions_adds_ml_score_column(self):
        """score_transactions must add 'ml_score' column."""
        df = make_sample_df(200)
        model, scaler = train_model(df)
        result = score_transactions(df, model, scaler)
        assert "ml_score" in result.columns

    def test_ml_scores_in_valid_range(self):
        """All ml_score values must be in [0, 1]."""
        df = make_sample_df(200)
        model, scaler = train_model(df)
        result = score_transactions(df, model, scaler)
        assert result["ml_score"].between(0, 1).all(), "ml_score out of [0,1] range"

    def test_fraud_outliers_score_higher_than_normal(self):
        """Extreme outlier transactions must have higher ml_score than normal."""
        df = make_sample_df(200, fraud_pct=0.1)
        model, scaler = train_model(df)
        result = score_transactions(df, model, scaler)

        normal_avg = result[result["transaction_id"].str.startswith("n")]["ml_score"].mean()
        fraud_avg  = result[result["transaction_id"].str.startswith("f")]["ml_score"].mean()

        assert fraud_avg > normal_avg, (
            f"Fraud avg score ({fraud_avg:.3f}) not > normal avg score ({normal_avg:.3f})"
        )

    def test_row_count_preserved(self):
        """Output row count must equal input row count."""
        df = make_sample_df(100)
        model, scaler = train_model(df)
        result = score_transactions(df, model, scaler)
        assert len(result) == len(df)

    def test_handles_missing_feature_values(self):
        """score_transactions must handle NaN feature values without crashing."""
        df = make_sample_df(50)
        df.loc[0, "latitude"] = None  # Introduce NaN
        model, scaler = train_model(df.dropna())
        result = score_transactions(df, model, scaler)  # Should not raise
        assert result["ml_score"].notna().all()
