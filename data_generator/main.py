"""Main CLI entrypoint for the Fintech synthetic data generator.

Usage:
    python main.py generate users
    python main.py generate merchants
    python main.py generate transactions
    python main.py generate all
"""

from __future__ import annotations
import argparse
import hashlib
import json
import logging
import os
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import List

import boto3
import yaml
from botocore.config import Config
from botocore.exceptions import ClientError
from faker import Faker

from data_generator.models.dispute import Dispute
from data_generator.models.merchant import Merchant
from data_generator.models.transaction import (
    VALID_CURRENCIES, VALID_PAYMENT_METHODS, VALID_STATUSES, VALID_TXN_TYPES, Transaction,
)
from data_generator.models.user import User
from data_generator.fraud_patterns.velocity_attack import VelocityAttack
from data_generator.fraud_patterns.high_amount import HighAmount
from data_generator.fraud_patterns.geographic_impossible import GeographicImpossible

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

fake = Faker()
Faker.seed(42)


# ── S3 Client ─────────────────────────────────────────────────────────

def get_s3_client(endpoint_url: str) -> boto3.client:
    """Create S3 client configured for LocalStack.

    Args:
        endpoint_url: LocalStack S3 endpoint URL.

    Returns:
        Configured boto3 S3 client.
    """
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )


def upload_to_s3(
    s3_client: boto3.client,
    bucket: str,
    data: List[dict],
    entity: str,
    base_time: datetime = None,
) -> str:
    """Upload JSONL data to S3 with the canonical path structure.

    Args:
        s3_client: Configured boto3 S3 client.
        bucket: Target S3 bucket name.
        data: List of dicts to serialize as JSONL.
        entity: Entity type (users, merchants, transactions, disputes).

    Returns:
        S3 key of the uploaded file.

    Raises:
        ClientError: If S3 upload fails.
    """
    now = base_time or datetime.now(timezone.utc)
    file_key = (
        f"raw/{entity}/{now.year}/{now.month:02d}/{now.day:02d}"
        f"/{now.hour:02d}/{uuid.uuid4()}.jsonl"
    )
    success_key = (
        f"raw/{entity}/{now.year}/{now.month:02d}/{now.day:02d}"
        f"/{now.hour:02d}/_SUCCESS"
    )

    body = "\n".join(json.dumps(record, default=str) for record in data)

    s3_client.put_object(Bucket=bucket, Key=file_key, Body=body.encode("utf-8"))
    s3_client.put_object(Bucket=bucket, Key=success_key, Body=b"")

    logger.info("✅ Uploaded %d %s records → s3://%s/%s", len(data), entity, bucket, file_key)
    return file_key


# ── Generators ────────────────────────────────────────────────────────

def generate_users(count: int) -> List[User]:
    """Generate synthetic User records.

    Args:
        count: Number of users to generate.

    Returns:
        List of User instances.
    """
    users = []
    for _ in range(count):
        user = User(
            email=fake.email(),
            registration_date=fake.date_between(start_date="-3y", end_date="today"),
            risk_tier=random.choices(["low", "medium", "high"], weights=[70, 25, 5])[0],
            country_code=fake.country_code(representation="alpha-2")[:2].upper(),
        )
        user.email_hash = hashlib.sha256(user.email.encode()).hexdigest()
        users.append(user)
    logger.info("Generated %d users", len(users))
    return users


def generate_merchants(count: int) -> List[Merchant]:
    """Generate synthetic Merchant records.

    Args:
        count: Number of merchants to generate.

    Returns:
        List of Merchant instances.
    """
    mcc_codes = ["5411", "5812", "5999", "7011", "4121", "5912", "5732", "5045"]
    merchants = []
    for _ in range(count):
        merchant = Merchant(
            merchant_name=fake.company(),
            mcc_code=random.choice(mcc_codes),
            country_code=fake.country_code(representation="alpha-2")[:2].upper(),
            risk_score=Decimal(str(round(random.uniform(0.1, 0.9), 4))),
        )
        merchants.append(merchant)
    logger.info("Generated %d merchants", len(merchants))
    return merchants


def generate_transactions(
    users: List[User],
    merchants: List[Merchant],
    config: dict,
    base_time: datetime = None,
) -> List[Transaction]:
    """Generate synthetic Transaction records with fraud injection.

    Args:
        users: List of User instances to sample from.
        merchants: List of Merchant instances to sample from.
        config: Generator configuration dict from YAML.

    Returns:
        List of Transaction instances (including injected fraud).
    """
    count = config["volumes"].get("transactions_per_hour", 1000)
    fraud_rate = float(config["fraud"].get("injection_rate", 0.02))

    now = base_time or datetime.now(timezone.utc)
    transactions = []

    for _ in range(count):
        user = random.choice(users)
        merchant = random.choice(merchants)
        lat = float(fake.latitude())
        lon = float(fake.longitude())

        txn = Transaction(
            user_id=user.user_id,
            merchant_id=merchant.merchant_id,
            amount=Decimal(str(round(random.uniform(1.0, 500.0), 2))),
            currency=random.choice(VALID_CURRENCIES),
            timestamp=now - timedelta(seconds=random.randint(0, 3600)),
            transaction_type=random.choices(
                VALID_TXN_TYPES, weights=[70, 10, 15, 5]
            )[0],
            status=random.choices(
                VALID_STATUSES, weights=[80, 10, 7, 3]
            )[0],
            payment_method=random.choices(
                VALID_PAYMENT_METHODS, weights=[60, 20, 15, 5]
            )[0],
            ip_address=fake.ipv4(),
            latitude=lat,
            longitude=lon,
            country_code=user.country_code,
            mcc_code=merchant.mcc_code,
        )
        transactions.append(txn)

    # ── Fraud injection ────────────────────────────────────────────
    fraud_cfg = config.get("fraud", {}).get("patterns", {})

    if fraud_cfg.get("velocity_attack", {}).get("enabled", True):
        vc = fraud_cfg["velocity_attack"]
        pattern = VelocityAttack(
            min_txns=vc.get("min_txns", 10),
            max_txns=vc.get("max_txns", 15),
            window_seconds=vc.get("window_seconds", 60),
        )
        transactions = pattern.inject(transactions)
        logger.info("Injected %s pattern", pattern.pattern_name)

    if fraud_cfg.get("high_amount", {}).get("enabled", True):
        ha = fraud_cfg["high_amount"]
        pattern = HighAmount(
            multiplier_min=ha.get("multiplier_min", 5.0),
            multiplier_max=ha.get("multiplier_max", 10.0),
        )
        transactions = pattern.inject(transactions)
        logger.info("Injected %s pattern", pattern.pattern_name)

    if fraud_cfg.get("geographic_impossible", {}).get("enabled", True):
        gi = fraud_cfg["geographic_impossible"]
        pattern = GeographicImpossible(
            max_time_hours=gi.get("max_time_hours", 0.5),
        )
        transactions = pattern.inject(transactions)
        logger.info("Injected %s pattern", pattern.pattern_name)

    fraud_count = sum(1 for t in transactions if t.is_fraud)
    logger.info(
        "Generated %d transactions (%d fraudulent, %.1f%%)",
        len(transactions), fraud_count, 100.0 * fraud_count / len(transactions),
    )
    return transactions


def generate_disputes(
    transactions: List[Transaction],
    config: dict,
    base_time: datetime = None,
) -> List[Dispute]:
    """Generate Dispute records for a fraction of transactions.

    Args:
        transactions: Source transactions to create disputes from.
        config: Generator configuration dict from YAML.

    Returns:
        List of Dispute instances.
    """
    dispute_rate = config["volumes"].get("disputes_rate", 0.005)
    reasons = [
        "unauthorized_transaction", "item_not_received",
        "duplicate_charge", "amount_discrepancy", "fraud",
    ]
    disputes = []
    now = base_time or datetime.now(timezone.utc)

    for txn in transactions:
        if random.random() < dispute_rate:
            dispute = Dispute(
                transaction_id=txn.transaction_id,
                user_id=txn.user_id,
                reason=random.choice(reasons),
                opened_at=now - timedelta(hours=random.randint(1, 72)),
            )
            disputes.append(dispute)

    logger.info("Generated %d disputes", len(disputes))
    return disputes


# ── CLI Entrypoint ────────────────────────────────────────────────────

def main() -> None:
    """CLI entrypoint for the data generator."""
    parser = argparse.ArgumentParser(description="Fintech Synthetic Data Generator")
    parser.add_argument("command", choices=["generate"], help="Command to run")
    parser.add_argument(
        "entity",
        choices=["users", "merchants", "transactions", "all"],
        help="Entity type to generate",
    )
    parser.add_argument(
        "--config",
        default=str(Path(__file__).parent / "config" / "generator_config.yaml"),
        help="Path to generator config YAML",
    )
    parser.add_argument(
        "--execution-date",
        default=None,
        help="Base datetime in ISO 8601 format to use for data generation timestamps.",
    )
    args = parser.parse_args()

    # Parse base_time
    base_time = None
    if args.execution_date:
        base_time = datetime.fromisoformat(args.execution_date)
        if base_time.tzinfo is None:
            base_time = base_time.replace(tzinfo=timezone.utc)

    # Load config
    with open(args.config, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    endpoint_url = os.environ["LOCALSTACK_ENDPOINT"]
    bucket = os.environ["S3_BUCKET"]
    s3_client = get_s3_client(endpoint_url)

    logger.info("🚀 Starting data generation (entity=%s)", args.entity)

    users: List[User] = []
    merchants: List[Merchant] = []

    if args.entity in ("users", "all"):
        users = generate_users(config["volumes"]["users"])
        upload_to_s3(s3_client, bucket, [u.to_dict() for u in users], "users", base_time=base_time)

    if args.entity in ("merchants", "all"):
        merchants = generate_merchants(config["volumes"]["merchants"])
        upload_to_s3(s3_client, bucket, [m.to_dict() for m in merchants], "merchants", base_time=base_time)

    if args.entity in ("transactions", "all"):
        # Need users & merchants for transaction generation
        if not users:
            # Generate minimal set for foreign keys
            users = generate_users(min(1000, config["volumes"]["users"]))
        if not merchants:
            merchants = generate_merchants(min(500, config["volumes"]["merchants"]))

        transactions = generate_transactions(users, merchants, config, base_time=base_time)
        upload_to_s3(
            s3_client, bucket,
            [t.to_jsonl_dict() for t in transactions],
            "transactions",
            base_time=base_time,
        )

        # Generate disputes for this batch
        disputes = generate_disputes(transactions, config, base_time=base_time)
        if disputes:
            upload_to_s3(
                s3_client, bucket,
                [d.to_dict() for d in disputes],
                "disputes",
                base_time=base_time,
            )

    logger.info("✅ Data generation complete for entity: %s", args.entity)


if __name__ == "__main__":
    main()
