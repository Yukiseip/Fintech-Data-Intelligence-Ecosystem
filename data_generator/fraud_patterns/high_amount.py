"""High amount fraud pattern: transaction > 500% of user's average."""

from __future__ import annotations
import random
import uuid
from decimal import Decimal
from typing import List

from data_generator.fraud_patterns.base import FraudPattern
from data_generator.models.transaction import Transaction


class HighAmount(FraudPattern):
    """Injects transactions with amounts far exceeding user's normal spending.

    Simulates account takeover or large unauthorized purchases.
    Triggers the HIGH_AMOUNT anomaly rule (> 500% of user's 30-day average).
    """

    def __init__(
        self,
        multiplier_min: float = 5.0,
        multiplier_max: float = 10.0,
    ) -> None:
        """Initialize HighAmount pattern.

        Args:
            multiplier_min: Minimum multiplier over user's average amount.
            multiplier_max: Maximum multiplier over user's average amount.
        """
        self.multiplier_min = multiplier_min
        self.multiplier_max = multiplier_max

    @property
    def pattern_name(self) -> str:
        """Return the pattern identifier.

        Returns:
            String 'HIGH_AMOUNT'.
        """
        return "HIGH_AMOUNT"

    def inject(self, transactions: List[Transaction]) -> List[Transaction]:
        """Inject a high-amount transaction for a random victim user.

        Calculates the victim's average amount from existing transactions,
        then creates one transaction with multiplier_min..multiplier_max times that average.

        Args:
            transactions: List of clean transactions.

        Returns:
            Original list plus one high-amount fraud transaction.
        """
        if len(transactions) < 2:
            return transactions

        victim = random.choice(transactions)
        user_txns = [t for t in transactions if t.user_id == victim.user_id]

        if not user_txns:
            return transactions

        avg_amount = float(sum(float(t.amount) for t in user_txns)) / len(user_txns)
        multiplier = random.uniform(self.multiplier_min, self.multiplier_max)
        fraud_amount = round(avg_amount * multiplier, 2)

        fraud_txn = victim.copy(deep=True)
        fraud_txn.transaction_id = str(uuid.uuid4())
        fraud_txn.amount = Decimal(str(fraud_amount))
        fraud_txn.status = "completed"
        fraud_txn.is_fraud = True
        fraud_txn.fraud_pattern = self.pattern_name

        return transactions + [fraud_txn]
