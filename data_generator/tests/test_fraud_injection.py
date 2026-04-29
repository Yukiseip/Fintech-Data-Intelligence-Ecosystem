"""Unit tests for fraud pattern injection."""

from __future__ import annotations
import pytest
from datetime import datetime, timezone
from decimal import Decimal

from data_generator.models.transaction import Transaction
from data_generator.fraud_patterns.velocity_attack import VelocityAttack
from data_generator.fraud_patterns.high_amount import HighAmount
from data_generator.fraud_patterns.geographic_impossible import GeographicImpossible


def make_txn(**kwargs) -> Transaction:
    """Create a valid Transaction with default values, overridable via kwargs."""
    defaults = dict(
        user_id="user-001",
        merchant_id="merchant-001",
        amount=Decimal("100.00"),
        currency="USD",
        timestamp=datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc),
        transaction_type="purchase",
        status="completed",
        payment_method="card",
        ip_address="192.168.1.1",
        latitude=19.43,
        longitude=-99.13,
        country_code="MX",
        mcc_code="5411",
    )
    defaults.update(kwargs)
    return Transaction(**defaults)


# ── VelocityAttack tests ──────────────────────────────────────────────

class TestVelocityAttack:

    def test_injects_burst_transactions(self):
        """Injected burst must contain at least min_txns transactions."""
        txns = [make_txn() for _ in range(5)]
        pattern = VelocityAttack(min_txns=10, max_txns=12, window_seconds=60)
        result = pattern.inject(txns)

        new_txns = [t for t in result if t.is_fraud]
        assert len(new_txns) >= 10

    def test_fraud_flag_set_on_burst(self):
        """All injected burst transactions must have is_fraud=True."""
        txns = [make_txn() for _ in range(5)]
        pattern = VelocityAttack(min_txns=10, max_txns=10)
        result = pattern.inject(txns)

        fraud_txns = [t for t in result if t.is_fraud]
        for txn in fraud_txns:
            assert txn.is_fraud is True
            assert txn.fraud_pattern == "VELOCITY_ATTACK"

    def test_pattern_name(self):
        """pattern_name must return 'VELOCITY_ATTACK'."""
        assert VelocityAttack().pattern_name == "VELOCITY_ATTACK"

    def test_empty_list_returns_unchanged(self):
        """Empty input should return empty list unchanged."""
        result = VelocityAttack().inject([])
        assert result == []

    def test_burst_same_user_id(self):
        """Burst transactions must share the victim's user_id."""
        victim_user = "user-victim-001"
        txns = [make_txn(user_id=victim_user)]
        pattern = VelocityAttack(min_txns=10, max_txns=10)
        result = pattern.inject(txns)

        burst = [t for t in result if t.is_fraud]
        for txn in burst:
            assert txn.user_id == victim_user


# ── HighAmount tests ──────────────────────────────────────────────────

class TestHighAmount:

    def test_injects_high_amount_transaction(self):
        """A transaction with amount >> 5x avg must be injected."""
        txns = [make_txn(amount=Decimal("100.00")) for _ in range(10)]
        pattern = HighAmount(multiplier_min=5.0, multiplier_max=6.0)
        result = pattern.inject(txns)

        fraud_txns = [t for t in result if t.is_fraud]
        assert len(fraud_txns) == 1
        assert float(fraud_txns[0].amount) >= 500.0

    def test_fraud_flag_and_pattern(self):
        """Injected transaction must have is_fraud=True and correct pattern."""
        txns = [make_txn() for _ in range(5)]
        result = HighAmount().inject(txns)
        fraud_txns = [t for t in result if t.is_fraud]
        for txn in fraud_txns:
            assert txn.fraud_pattern == "HIGH_AMOUNT"

    def test_pattern_name(self):
        """pattern_name must return 'HIGH_AMOUNT'."""
        assert HighAmount().pattern_name == "HIGH_AMOUNT"


# ── GeographicImpossible tests ────────────────────────────────────────

class TestGeographicImpossible:

    def test_injects_two_transactions_in_distant_locations(self):
        """Must inject exactly 2 transactions in distant locations."""
        txns = [make_txn()]
        pattern = GeographicImpossible(max_time_hours=0.5)
        result = pattern.inject(txns)

        fraud_txns = [t for t in result if t.is_fraud]
        assert len(fraud_txns) == 2

    def test_locations_are_different_countries(self):
        """The two injected transactions must be in different countries."""
        txns = [make_txn()]
        result = GeographicImpossible().inject(txns)
        fraud_txns = [t for t in result if t.is_fraud]
        countries = {t.country_code for t in fraud_txns}
        assert len(countries) == 2

    def test_pattern_name(self):
        """pattern_name must return 'GEO_IMPOSSIBLE'."""
        assert GeographicImpossible().pattern_name == "GEO_IMPOSSIBLE"

    def test_same_user_id_in_both_transactions(self):
        """Both injected transactions must have the same user_id."""
        txns = [make_txn(user_id="user-geo-001")]
        result = GeographicImpossible().inject(txns)
        fraud_txns = [t for t in result if t.is_fraud]
        user_ids = {t.user_id for t in fraud_txns}
        assert len(user_ids) == 1
        assert "user-geo-001" in user_ids
