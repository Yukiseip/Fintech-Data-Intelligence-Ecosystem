"""Abstract base class for fraud pattern injectors."""

from abc import ABC, abstractmethod
from typing import List

from data_generator.models.transaction import Transaction


class FraudPattern(ABC):
    """Base class for all fraud pattern implementations.

    Subclasses must implement inject() and the pattern_name property.
    Each pattern is responsible for:
    1. Selecting victim transactions from the input list
    2. Creating fraudulent transactions based on the pattern
    3. Setting is_fraud=True and fraud_pattern on modified records
    """

    @abstractmethod
    def inject(self, transactions: List[Transaction]) -> List[Transaction]:
        """Inject fraud pattern into a list of transactions.

        Args:
            transactions: List of clean transactions to potentially modify.

        Returns:
            Modified list with fraud pattern injected. May contain additional
            fraudulent transactions appended to the original list.
        """
        pass

    @property
    @abstractmethod
    def pattern_name(self) -> str:
        """Human-readable name of this fraud pattern.

        Returns:
            String identifier used in alerts_log reason_code.
        """
        pass
