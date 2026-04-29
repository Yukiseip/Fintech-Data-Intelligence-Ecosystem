"""Fraud pattern modules for data generation."""

from data_generator.fraud_patterns.base import FraudPattern
from data_generator.fraud_patterns.velocity_attack import VelocityAttack
from data_generator.fraud_patterns.high_amount import HighAmount
from data_generator.fraud_patterns.geographic_impossible import GeographicImpossible

__all__ = ["FraudPattern", "VelocityAttack", "HighAmount", "GeographicImpossible"]
