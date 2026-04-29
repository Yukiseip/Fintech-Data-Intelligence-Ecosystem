"""Schema definition for the Silver (cleansed) layer."""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SILVER_SCHEMA = StructType([
    StructField("transaction_id",     StringType(),       False),
    StructField("user_id",            StringType(),       False),
    StructField("merchant_id",        StringType(),       False),
    StructField("amount",             DecimalType(18, 2), False),
    StructField("currency",           StringType(),       False),
    StructField("timestamp",          TimestampType(),    False),
    StructField("transaction_type",   StringType(),       False),
    StructField("status",             StringType(),       False),
    StructField("payment_method",     StringType(),       False),
    StructField("device_id",          StringType(),       True),
    StructField("ip_address_hash",    StringType(),       True),   # SHA-256, PII free
    StructField("latitude",           DecimalType(9, 6),  True),
    StructField("longitude",          DecimalType(9, 6),  True),
    StructField("country_code",       StringType(),       True),
    StructField("mcc_code",           StringType(),       True),
    # Audit / metadata columns
    StructField("ingestion_timestamp", TimestampType(),   True),
    StructField("source_file",         StringType(),      True),
    StructField("partition_date",      DateType(),        True),
    StructField("_silver_processed_at", TimestampType(),  False),
])
