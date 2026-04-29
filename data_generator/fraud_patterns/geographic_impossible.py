"""Geographic impossibility fraud pattern: impossible travel between locations."""

from __future__ import annotations
import random
import uuid
from datetime import timedelta
from typing import List, Tuple

from data_generator.fraud_patterns.base import FraudPattern
from data_generator.models.transaction import Transaction

# Geographic pairs: (lat, lon, country_code) far apart
DISTANT_LOCATION_PAIRS: List[Tuple[Tuple[float, float, str], Tuple[float, float, str]]] = [
    ((19.4326, -99.1332, "MX"), (35.6762, 139.6503, "JP")),   # Mexico City → Tokyo
    ((40.7128, -74.0060, "US"), (51.5074, -0.1278, "GB")),     # New York → London
    ((48.8566, 2.3522, "FR"), (-33.8688, 151.2093, "AU")),     # Paris → Sydney
    ((55.7558, 37.6173, "RU"), (-23.5505, -46.6333, "BR")),    # Moscow → São Paulo
    ((1.3521, 103.8198, "SG"), (52.5200, 13.4050, "DE")),      # Singapore → Berlin
]


class GeographicImpossible(FraudPattern):
    """Injects consecutive transactions in locations impossible to travel between in the time elapsed.

    Simulates card cloning or stolen credential use across continents.
    Triggers the GEO_IMPOSSIBLE anomaly rule (> 800 km/h implied travel speed).
    """

    def __init__(self, max_time_hours: float = 0.5) -> None:
        """Initialize GeographicImpossible pattern.

        Args:
            max_time_hours: Maximum time in hours between the two transactions.
                            Defaults to 0.5 hours (30 minutes).
        """
        self.max_time_hours = max_time_hours

    @property
    def pattern_name(self) -> str:
        """Return the pattern identifier.

        Returns:
            String 'GEO_IMPOSSIBLE'.
        """
        return "GEO_IMPOSSIBLE"

    def inject(self, transactions: List[Transaction]) -> List[Transaction]:
        """Inject a pair of geographically impossible transactions.

        Picks a random victim, places them in Location A, then creates a
        second transaction in Location B (far away) within max_time_hours.

        Args:
            transactions: List of clean transactions.

        Returns:
            Original list plus two fraud transactions in distant locations.
        """
        if not transactions:
            return transactions

        victim = random.choice(transactions)
        loc_pair = random.choice(DISTANT_LOCATION_PAIRS)
        loc_a, loc_b = loc_pair

        # First transaction: victim at location A
        txn_a = victim.copy(deep=True)
        txn_a.transaction_id = str(uuid.uuid4())
        txn_a.latitude = loc_a[0]
        txn_a.longitude = loc_a[1]
        txn_a.country_code = loc_a[2]
        txn_a.is_fraud = True
        txn_a.fraud_pattern = self.pattern_name

        # Second transaction: same user at location B, short time later
        minutes_later = random.randint(15, int(self.max_time_hours * 60))
        txn_b = victim.copy(deep=True)
        txn_b.transaction_id = str(uuid.uuid4())
        txn_b.timestamp = txn_a.timestamp + timedelta(minutes=minutes_later)
        txn_b.latitude = loc_b[0]
        txn_b.longitude = loc_b[1]
        txn_b.country_code = loc_b[2]
        txn_b.is_fraud = True
        txn_b.fraud_pattern = self.pattern_name

        return transactions + [txn_a, txn_b]
