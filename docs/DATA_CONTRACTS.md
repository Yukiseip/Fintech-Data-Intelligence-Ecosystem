# Data Contracts — Fintech Data Intelligence Platform

> All schemas are enforced via Pydantic (generation), Spark StructType (processing),
> and dbt schema tests (warehouse). Changes require a version bump.

---

## Bronze Layer — Raw Transaction Schema

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `raw_data` | STRING | No | Full JSON payload from JSONL |
| `ingestion_timestamp` | TIMESTAMP | No | Job start time (UTC) |
| `source_file` | STRING | No | S3 URI of source file |
| `partition_date` | DATE | No | Processing date for partitioning |
| `bronze_batch_id` | STRING | No | UUID4 per job run |

---

## Silver Layer — Cleaned Transaction Schema

| Column | Type | Nullable | Constraint |
|--------|------|----------|------------|
| `transaction_id` | STRING (UUID) | No | Unique per batch |
| `user_id` | STRING (UUID) | No | — |
| `merchant_id` | STRING (UUID) | No | — |
| `amount` | DECIMAL(18,2) | No | > 0 |
| `currency` | STRING (3) | No | USD, EUR, GBP, JPY, CAD, MXN, BRL |
| `timestamp` | TIMESTAMP | No | ≤ NOW() (no future dates) |
| `transaction_type` | STRING | No | purchase, refund, transfer, withdrawal |
| `status` | STRING | No | completed, pending, failed, disputed |
| `payment_method` | STRING | No | card, bank_transfer, wallet, crypto |
| `device_id` | STRING | Yes | — |
| `ip_address_hash` | STRING (64) | Yes | SHA-256 of original ip_address |
| `latitude` | DECIMAL(9,6) | Yes | [-90, 90] |
| `longitude` | DECIMAL(9,6) | Yes | [-180, 180] |
| `country_code` | STRING (2) | Yes | ISO 3166-1 alpha-2 |
| `mcc_code` | STRING (4) | Yes | Merchant Category Code |
| `ingestion_timestamp` | TIMESTAMP | Yes | From Bronze layer |
| `source_file` | STRING | Yes | From Bronze layer |
| `partition_date` | DATE | Yes | Processing date |
| `_silver_processed_at` | TIMESTAMP | No | Silver job execution time |

> ⚠️ **PII Policy**: `ip_address` is NEVER written to Silver or Gold layers.
> Only `ip_address_hash` (SHA-256) is persisted.

---

## Gold Layer — Star Schema

### `gold.fact_transactions`

| Column | Type | Notes |
|--------|------|-------|
| `transaction_sk` | BIGSERIAL PK | Surrogate key |
| `transaction_id` | UUID UNIQUE | Natural key |
| `user_sk` | BIGINT FK | → dim_users |
| `merchant_sk` | BIGINT FK | → dim_merchants |
| `time_sk` | INT FK | → dim_time (YYYYMMDD) |
| `amount` | DECIMAL(18,2) | > 0 |
| `currency` | CHAR(3) | — |
| `transaction_type` | VARCHAR(20) | — |
| `status` | VARCHAR(20) | — |
| `payment_method` | VARCHAR(20) | — |
| `is_flagged_fraud` | BOOLEAN | Updated by anomaly job |
| `fraud_score` | DECIMAL(5,4) | [0, 1] |
| `latitude` | DECIMAL(9,6) | — |
| `longitude` | DECIMAL(9,6) | — |
| `country_code` | CHAR(2) | — |
| `created_at` | TIMESTAMP | Row insertion time |

### `gold.dim_users` (SCD Type 2)

| Column | Type | Notes |
|--------|------|-------|
| `user_sk` | BIGSERIAL PK | — |
| `user_id` | UUID | Natural key |
| `email_hash` | VARCHAR(64) | SHA-256 |
| `registration_date` | DATE | — |
| `risk_tier` | VARCHAR(10) | low / medium / high |
| `country_code` | CHAR(2) | — |
| `valid_from` | TIMESTAMP | SCD2 validity start |
| `valid_to` | TIMESTAMP | `9999-12-31` if current |
| `is_current` | BOOLEAN | Unique index when TRUE |
| `dbt_scd_id` | VARCHAR(64) | dbt snapshot hash |

### `gold.alerts_log`

| Column | Type | Notes |
|--------|------|-------|
| `alert_id` | BIGSERIAL PK | — |
| `transaction_id` | UUID | FK to fact_transactions |
| `alert_timestamp` | TIMESTAMP | Alert creation time |
| `reason_code` | VARCHAR(50) | VELOCITY_ATTACK / HIGH_AMOUNT / GEO_IMPOSSIBLE / ML_ANOMALY |
| `severity_score` | DECIMAL(5,4) | [0, 1]; higher = more severe |
| `description` | TEXT | Human-readable explanation |
| `status` | VARCHAR(40) | open / investigating / closed_false_positive / confirmed_fraud |
| `ml_score` | DECIMAL(5,4) | Isolation Forest score (nullable if rule-only) |
| `rule_score` | DECIMAL(5,4) | Rule-based score (nullable if ML-only) |

---

## Data Quality Gates

### Great Expectations (Silver Layer — 15 expectations)
- Column existence: 11 required columns
- PII check: `ip_address` must NOT exist
- Nullability: transaction_id, user_id, merchant_id, amount, timestamp
- Uniqueness: transaction_id
- Range: amount [0.01, 1,000,000], latitude [-90,90], longitude [-180,180]
- Accepted values: currency, transaction_type, status, payment_method
- ip_address_hash length = 64 chars

### dbt Tests (Gold Layer)
| Test | What it checks |
|------|---------------|
| `assert_positive_revenue` | No Gold transaction has amount ≤ 0 |
| `assert_user_uniqueness` | No user_id has > 1 `is_current=TRUE` record |
| `assert_no_orphan_facts` | All fact_transactions have valid user_sk |
| `assert_fraud_score_range` | fraud_score in [0, 1] |
| Schema tests | not_null, unique, accepted_values on all mart models |

---

## Schema Evolution Policy

1. **Backward-compatible changes** (add nullable column): no version bump required; update schema files.
2. **Breaking changes** (remove column, change type): bump Silver/Gold schema version, coordinate Spark + dbt migration.
3. **PII additions**: any new field containing PII must be hashed before Silver layer. Update `cast_and_validate()` and GE expectations.
