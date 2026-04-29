"""Schema definitions for Bronze layer (raw ingested data)."""

from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    DecimalType,
    FloatType,
    BooleanType,
)

# Bronze layer output schema: minimal metadata wrapper around raw JSON
BRONZE_SCHEMA = StructType([
    StructField("raw_data",            StringType(),    False),
    StructField("ingestion_timestamp", TimestampType(), False),
    StructField("source_file",         StringType(),    False),
    StructField("partition_date",      DateType(),      False),
    StructField("bronze_batch_id",     StringType(),    False),
])

# Inline schema for parsing raw_data JSON in Silver job
RAW_TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",   StringType(),       True),
    StructField("user_id",          StringType(),       True),
    StructField("merchant_id",      StringType(),       True),
    StructField("amount",           DecimalType(18, 2), True),
    StructField("currency",         StringType(),       True),
    StructField("timestamp",        StringType(),       True),   # parsed as string, cast later
    StructField("transaction_type", StringType(),       True),
    StructField("status",           StringType(),       True),
    StructField("payment_method",   StringType(),       True),
    StructField("device_id",        StringType(),       True),
    StructField("ip_address",       StringType(),       True),
    StructField("latitude",         FloatType(),        True),
    StructField("longitude",        FloatType(),        True),
    StructField("country_code",     StringType(),       True),
    StructField("mcc_code",         StringType(),       True),
])
