"""Transaction Pydantic model for synthetic data generation."""

from __future__ import annotations
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field, validator


VALID_CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD", "MXN"]
VALID_TXN_TYPES = ["purchase", "refund", "transfer", "withdrawal"]
VALID_STATUSES = ["completed", "pending", "failed", "disputed"]
VALID_PAYMENT_METHODS = ["card", "bank_transfer", "wallet", "crypto"]


class Transaction(BaseModel):
    """Represents a financial transaction record."""

    transaction_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    merchant_id: str
    amount: Decimal = Field(gt=Decimal("0.01"), decimal_places=2)
    currency: str = Field(min_length=3, max_length=3)
    timestamp: datetime
    transaction_type: str
    status: str
    payment_method: str
    device_id: str = Field(default_factory=lambda: f"dev-{uuid.uuid4().hex[:8]}")
    ip_address: str
    latitude: float = Field(ge=-90.0, le=90.0)
    longitude: float = Field(ge=-180.0, le=180.0)
    country_code: str = Field(min_length=2, max_length=2)
    mcc_code: str = Field(min_length=4, max_length=4)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    # Internal-only fields: NOT exported to JSONL (exclude=True)
    is_fraud: bool = Field(default=False, exclude=True)
    fraud_pattern: Optional[str] = Field(default=None, exclude=True)

    @validator("currency")
    def validate_currency(cls, v: str) -> str:
        """Validate currency is in allowed set.

        Args:
            v: Currency code string.

        Returns:
            Validated currency string.

        Raises:
            ValueError: If currency is not in VALID_CURRENCIES.
        """
        if v not in VALID_CURRENCIES:
            raise ValueError(f"currency must be one of {VALID_CURRENCIES}")
        return v

    @validator("transaction_type")
    def validate_txn_type(cls, v: str) -> str:
        """Validate transaction type.

        Args:
            v: Transaction type string.

        Returns:
            Validated transaction type.

        Raises:
            ValueError: If transaction_type not in VALID_TXN_TYPES.
        """
        if v not in VALID_TXN_TYPES:
            raise ValueError(f"transaction_type must be one of {VALID_TXN_TYPES}")
        return v

    @validator("status")
    def validate_status(cls, v: str) -> str:
        """Validate transaction status.

        Args:
            v: Status string.

        Returns:
            Validated status.

        Raises:
            ValueError: If status not in VALID_STATUSES.
        """
        if v not in VALID_STATUSES:
            raise ValueError(f"status must be one of {VALID_STATUSES}")
        return v

    @validator("payment_method")
    def validate_payment_method(cls, v: str) -> str:
        """Validate payment method.

        Args:
            v: Payment method string.

        Returns:
            Validated payment method.

        Raises:
            ValueError: If payment_method not in VALID_PAYMENT_METHODS.
        """
        if v not in VALID_PAYMENT_METHODS:
            raise ValueError(f"payment_method must be one of {VALID_PAYMENT_METHODS}")
        return v

    def to_jsonl_dict(self) -> Dict[str, Any]:
        """Return dict suitable for JSONL export (excludes internal fraud flags).

        Returns:
            Dictionary with all fields except is_fraud and fraud_pattern.
        """
        data = self.dict(exclude={"is_fraud", "fraud_pattern"})
        # Serialize non-JSON-native types
        data["amount"] = float(data["amount"])
        data["timestamp"] = data["timestamp"].isoformat()
        return data
