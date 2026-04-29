"""User Pydantic model for synthetic data generation."""

from __future__ import annotations
import uuid
from datetime import date
from typing import Optional
from pydantic import BaseModel, Field, validator


class User(BaseModel):
    """Represents a Fintech platform user."""

    user_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    email: str
    email_hash: Optional[str] = None  # Populated after SHA-256 hashing
    registration_date: date
    risk_tier: str = Field(default="low")
    country_code: str = Field(min_length=2, max_length=2)
    is_active: bool = Field(default=True)

    @validator("risk_tier")
    def validate_risk_tier(cls, v: str) -> str:
        """Validate risk tier is one of the allowed values.

        Args:
            v: Risk tier string value.

        Returns:
            Validated risk tier string.

        Raises:
            ValueError: If risk_tier is not in allowed values.
        """
        allowed = {"low", "medium", "high"}
        if v not in allowed:
            raise ValueError(f"risk_tier must be one of {allowed}, got '{v}'")
        return v

    def to_dict(self) -> dict:
        """Return dict for S3 JSONL export (excludes email plain text in downstream layers).

        Returns:
            Dictionary representation suitable for JSONL serialization.
        """
        return self.dict()
