"""Velocity attack fraud pattern: burst of transactions in a short time window."""

from __future__ import annotations
import random
import uuid
from datetime import timedelta
from typing import List

from data_generator.fraud_patterns.base import FraudPattern
from data_generator.models.transaction import Transaction


class VelocityAttack(FraudPattern):
    """Injects a rapid burst of transactions from the same user within a 60-second window.

    Simulates card-testing attacks or automated fraud bots.
    Triggers the VELOCITY_ATTACK anomaly rule (>= 10 txns in 60s).
    """

    def __init__(
        self,
        min_txns: int = 10,
        max_txns: int = 15,
        window_seconds: int = 60,
    ) -> None:
        """Initialize VelocityAttack pattern.

        Args:
            min_txns: Minimum number of burst transactions to inject.
            max_txns: Maximum number of burst transactions to inject.
            window_seconds: Time window in seconds for the burst.
        """
        self.min_txns = min_txns
        self.max_txns = max_txns
        self.window_seconds = window_seconds

    @property
    def pattern_name(self) -> str:
        """Return the pattern identifier.

        Returns:
            String 'VELOCITY_ATTACK'.
        """
        return "VELOCITY_ATTACK"

    def inject(self, transactions: List[Transaction]) -> List[Transaction]:
        """Inject a velocity attack burst into the transaction list.

        Picks a random victim user and creates min_txns..max_txns transactions
        all within window_seconds of the victim's original timestamp.

        Args:
            transactions: List of clean transactions.

        Returns:
            Original list plus burst fraud transactions appended.
        """
        if not transactions:
            return transactions

        victim = random.choice(transactions)
        burst_count = random.randint(self.min_txns, self.max_txns)
        base_time = victim.timestamp

        burst: List[Transaction] = []
        for _ in range(burst_count):
            fraud_txn = victim.copy(deep=True)
            fraud_txn.transaction_id = str(uuid.uuid4())
            fraud_txn.timestamp = base_time + timedelta(
                seconds=random.randint(0, self.window_seconds)
            )
            fraud_txn.amount = round(random.uniform(1.0, 50.0), 2)  # type: ignore
            fraud_txn.is_fraud = True
            fraud_txn.fraud_pattern = self.pattern_name
            burst.append(fraud_txn)

        return transactions + burst
