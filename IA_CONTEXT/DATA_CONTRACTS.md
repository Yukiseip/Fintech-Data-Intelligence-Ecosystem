# Data Contracts & Schemas — Fintech Data Intelligence Ecosystem

## Schema 0: Raw Input (S3 JSONL)

**Path Pattern:**
```
s3://fintech-raw-data/raw/{entity}/{year}/{month}/{day}/{hour}/{uuid}.jsonl
```

**Entity Types:** users, merchants, transactions, disputes

### Transaction Raw Schema
```json
{
  "transaction_id": "uuid-v4",
  "user_id": "uuid-v4",
  "merchant_id": "uuid-v4",
  "amount": 123.45,
  "currency": "USD",
  "timestamp": "2026-04-24T15:30:00Z",
  "transaction_type": "purchase | refund | transfer | withdrawal",
  "status": "completed | pending | failed | disputed",
  "payment_method": "card | bank_transfer | wallet | crypto",
  "device_id": "dev-abc123",
  "ip_address": "192.168.1.1",
  "latitude": 19.4326,
  "longitude": -99.1332,
  "country_code": "MX",
  "mcc_code": "5411",
  "metadata": {}
}
```

---

## Schema 1: Bronze Layer (Parquet on S3)

**Path Pattern:**
```
s3://fintech-raw-data/bronze/transactions/partition_date=YYYY-MM-DD/
```

**Format:** Parquet with Snappy compression  
**Properties:** Append-only, immutable

| Column | Spark Type | Nullable | Description |
|--------|-----------|----------|-------------|
| raw_data | StringType | NO | Original JSON as string |
| ingestion_timestamp | TimestampType | NO | When Spark wrote the record |
| source_file | StringType | NO | Source S3 path |
| partition_date | DateType | NO | Derived from event timestamp |
| bronze_batch_id | StringType | NO | UUID of ingestion run |

---

## Schema 2: Silver Layer (Parquet on S3)

**Path Pattern:**
```
s3://fintech-raw-data/silver/transactions/partition_date=YYYY-MM-DD/
```

**Format:** Parquet with Snappy compression  
**Quality:** Quality-gated (GE suite must pass)

| Column | Spark Type | Constraints |
|--------|-----------|-------------|
| transaction_id | StringType (UUID) | NOT NULL, UNIQUE per day |
| user_id | StringType (UUID) | NOT NULL |
| merchant_id | StringType (UUID) | NOT NULL |
| amount | DecimalType(18,2) | > 0.01 |
| currency | StringType | IN [USD, EUR, GBP, JPY, CAD, MXN] |
| timestamp | TimestampType | NOT NULL, NOT future, within 90 days |
| transaction_type | StringType | IN [purchase, refund, transfer, withdrawal] |
| status | StringType | IN [completed, pending, failed, disputed] |
| payment_method | StringType | IN [card, bank_transfer, wallet, crypto] |
| latitude | DecimalType(9,6) | -90 to 90 |
| longitude | DecimalType(9,6) | -180 to 180 |
| country_code | StringType | ISO 3166-1 alpha-2 valid |
| mcc_code | StringType | 4 digits |
| ip_address_hash | StringType | SHA-256 of original IP |
| ingestion_timestamp | TimestampType | Metadata |
| _silver_processed_at | TimestampType | Audit trail |

### Deduplication Rule
```python
ROW_NUMBER() OVER (
    PARTITION BY transaction_id 
    ORDER BY ingestion_timestamp DESC
) = 1
```

### Validation Rules
- transaction_id, user_id, merchant_id: NOT NULL
- amount: > 0.01
- timestamp: NOT NULL, NOT future, within 90 days
- currency: IN [USD, EUR, GBP, JPY, CAD, MXN]
- transaction_type: IN [purchase, refund, transfer, withdrawal]
- status: IN [completed, pending, failed, disputed]
- payment_method: IN [card, bank_transfer, wallet, crypto]
- latitude: BETWEEN -90 AND 90
- longitude: BETWEEN -180 AND 180

---

## Schema 3: Gold Layer (PostgreSQL)

### Table: `gold.fact_transactions`

```sql
CREATE TABLE gold.fact_transactions (
    transaction_sk   BIGSERIAL PRIMARY KEY,
    transaction_id   UUID         NOT NULL UNIQUE,
    user_sk          BIGINT       NOT NULL REFERENCES gold.dim_users(user_sk),
    merchant_sk      BIGINT       NOT NULL REFERENCES gold.dim_merchants(merchant_sk),
    time_sk          INT          NOT NULL REFERENCES gold.dim_time(time_sk),
    amount           DECIMAL(18,2) NOT NULL,
    currency         CHAR(3)      NOT NULL,
    transaction_type VARCHAR(20)  NOT NULL,
    status           VARCHAR(20)  NOT NULL,
    payment_method   VARCHAR(20)  NOT NULL,
    is_flagged_fraud BOOLEAN      DEFAULT FALSE,
    fraud_score      DECIMAL(5,4) DEFAULT 0.0,
    latitude         DECIMAL(9,6),
    longitude        DECIMAL(9,6),
    country_code     CHAR(2),
    created_at       TIMESTAMP    DEFAULT NOW()
);

CREATE INDEX idx_fact_txn_user ON gold.fact_transactions(user_sk);
CREATE INDEX idx_fact_txn_time ON gold.fact_transactions(time_sk);
CREATE INDEX idx_fact_txn_fraud ON gold.fact_transactions(is_flagged_fraud) WHERE is_flagged_fraud = TRUE;
```

### Table: `gold.dim_users` (SCD Type 2)

```sql
CREATE TABLE gold.dim_users (
    user_sk          BIGSERIAL PRIMARY KEY,
    user_id          UUID         NOT NULL,
    email_hash       VARCHAR(64)  NOT NULL,   -- SHA-256
    registration_date DATE,
    risk_tier        VARCHAR(10)  CHECK (risk_tier IN ('low', 'medium', 'high')),
    country_code     CHAR(2),
    valid_from       TIMESTAMP    NOT NULL DEFAULT NOW(),
    valid_to         TIMESTAMP    DEFAULT '9999-12-31'::TIMESTAMP,
    is_current       BOOLEAN      DEFAULT TRUE,
    dbt_scd_id       VARCHAR(64)  -- dbt snapshot hash
);

CREATE UNIQUE INDEX idx_dim_users_current ON gold.dim_users(user_id) WHERE is_current = TRUE;
```

### Table: `gold.dim_merchants`

```sql
CREATE TABLE gold.dim_merchants (
    merchant_sk      BIGSERIAL PRIMARY KEY,
    merchant_id      UUID         NOT NULL UNIQUE,
    merchant_name    VARCHAR(255),
    mcc_code         CHAR(4),
    country_code     CHAR(2),
    risk_score       DECIMAL(5,4) DEFAULT 0.5,
    created_at       TIMESTAMP    DEFAULT NOW()
);
```

### Table: `gold.dim_time`

```sql
CREATE TABLE gold.dim_time (
    time_sk     INT PRIMARY KEY,       -- YYYYMMDD
    date        DATE        NOT NULL UNIQUE,
    year        SMALLINT    NOT NULL,
    month       SMALLINT    NOT NULL,
    day         SMALLINT    NOT NULL,
    quarter     SMALLINT    NOT NULL,
    day_of_week SMALLINT    NOT NULL,  -- 1=Mon, 7=Sun
    is_weekend  BOOLEAN     NOT NULL,
    is_holiday  BOOLEAN     DEFAULT FALSE,
    month_name  VARCHAR(10),
    week_of_year SMALLINT
);
-- Pre-populate: 2020-01-01 to 2030-12-31
```

### Table: `gold.alerts_log`

```sql
CREATE TABLE gold.alerts_log (
    alert_id         BIGSERIAL PRIMARY KEY,
    transaction_id   UUID         NOT NULL,
    alert_timestamp  TIMESTAMP    NOT NULL DEFAULT NOW(),
    reason_code      VARCHAR(50)  NOT NULL,
    -- Values: VELOCITY_ATTACK, HIGH_AMOUNT, GEO_IMPOSSIBLE, ML_ANOMALY
    severity_score   DECIMAL(5,4) NOT NULL CHECK (severity_score BETWEEN 0 AND 1),
    description      TEXT,
    status           VARCHAR(40)  DEFAULT 'open',
    -- Values: open, investigating, closed_false_positive, confirmed_fraud
    ml_score         DECIMAL(5,4),
    rule_score       DECIMAL(5,4),
    created_at       TIMESTAMP    DEFAULT NOW()
);

CREATE INDEX idx_alerts_txn ON gold.alerts_log(transaction_id);
CREATE INDEX idx_alerts_status ON gold.alerts_log(status) WHERE status = 'open';
CREATE INDEX idx_alerts_timestamp ON gold.alerts_log(alert_timestamp DESC);
```

---

## Data Quality Expectations

### Silver Layer (Great Expectations)
- transaction_id: not_null, unique
- user_id: not_null
- merchant_id: not_null
- amount: not_null, greater_than(0)
- timestamp: not_null, not_future, within_90_days
- currency: value_in_set([USD, EUR, GBP, JPY, CAD, MXN])
- latitude: between(-90, 90)
- longitude: between(-180, 180)

### Gold Layer (dbt Tests)
- `assert_positive_revenue`: No negative amounts in fact_transactions
- `assert_user_uniqueness`: No duplicate current users in dim_users
- `assert_referential_integrity`: All foreign keys resolve

---

## PII Handling

| Field | Raw | Bronze | Silver | Gold | Method |
|-------|-----|--------|--------|------|--------|
| ip_address | ✅ | ✅ | ❌ (dropped) | ❌ | SHA-256 hash → ip_address_hash |
| email | ✅ | ✅ | ❌ | ❌ (hash only) | SHA-256 hash → email_hash |
| device_id | ✅ | ✅ | ✅ | ❌ | Excluded from Gold |
| metadata | ✅ | ✅ | ✅ | ❌ | Excluded from Gold |
