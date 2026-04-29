"""Merchant Pydantic model for synthetic data generation."""

from __future__ import annotations
import uuid
from decimal import Decimal
from typing import Optional
from pydantic import BaseModel, Field, validator


class Merchant(BaseModel):
    """Represents a Fintech platform merchant."""

    merchant_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    merchant_name: str
    mcc_code: str = Field(min_length=4, max_length=4)
    country_code: str = Field(min_length=2, max_length=2)
    risk_score: Decimal = Field(default=Decimal("0.5"), ge=0, le=1)
    is_active: bool = Field(default=True)

    @validator("mcc_code")
    def validate_mcc_code(cls, v: str) -> str:
        """Validate MCC code is exactly 4 digits.

        Args:
            v: MCC code string.

        Returns:
            Validated MCC code string.

        Raises:
            ValueError: If mcc_code is not 4 numeric digits.
        """
        if not v.isdigit() or len(v) != 4:
            raise ValueError(f"mcc_code must be exactly 4 digits, got '{v}'")
        return v

    def to_dict(self) -> dict:
        """Return dict for S3 JSONL export.

        Returns:
            Dictionary representation suitable for JSONL serialization.
        """
        data = self.dict()
        data["risk_score"] = float(data["risk_score"])
        return data
