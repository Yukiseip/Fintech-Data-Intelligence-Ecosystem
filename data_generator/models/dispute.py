"""Dispute Pydantic model for synthetic data generation."""

from __future__ import annotations
import uuid
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, validator


VALID_DISPUTE_REASONS = [
    "unauthorized_transaction",
    "item_not_received",
    "duplicate_charge",
    "amount_discrepancy",
    "fraud",
]

VALID_DISPUTE_STATUSES = ["open", "under_review", "resolved", "closed"]


class Dispute(BaseModel):
    """Represents a transaction dispute record."""

    dispute_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    transaction_id: str
    user_id: str
    reason: str
    status: str = Field(default="open")
    opened_at: datetime
    resolved_at: Optional[datetime] = None
    resolution_notes: Optional[str] = None

    @validator("reason")
    def validate_reason(cls, v: str) -> str:
        """Validate dispute reason is in allowed set.

        Args:
            v: Dispute reason string.

        Returns:
            Validated reason string.

        Raises:
            ValueError: If reason not in VALID_DISPUTE_REASONS.
        """
        if v not in VALID_DISPUTE_REASONS:
            raise ValueError(f"reason must be one of {VALID_DISPUTE_REASONS}")
        return v

    @validator("status")
    def validate_status(cls, v: str) -> str:
        """Validate dispute status.

        Args:
            v: Status string.

        Returns:
            Validated status.

        Raises:
            ValueError: If status not in VALID_DISPUTE_STATUSES.
        """
        if v not in VALID_DISPUTE_STATUSES:
            raise ValueError(f"status must be one of {VALID_DISPUTE_STATUSES}")
        return v

    def to_dict(self) -> dict:
        """Return dict for S3 JSONL export.

        Returns:
            Dictionary representation suitable for JSONL serialization.
        """
        data = self.dict()
        data["opened_at"] = data["opened_at"].isoformat()
        if data.get("resolved_at"):
            data["resolved_at"] = data["resolved_at"].isoformat()
        return data
