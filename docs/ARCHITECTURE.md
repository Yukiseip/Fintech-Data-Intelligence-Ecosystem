# Architecture Guide — Fintech Data Intelligence Platform

> Version: 3.0 | Sprint 3 Final

---

## System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Fintech Analytics Ecosystem                      │
│                                                                       │
│  ┌──────────────┐    ┌──────────────────────────────────────────┐   │
│  │ Data         │    │  Object Storage (S3 via LocalStack)       │   │
│  │ Generator    │───▶│  raw/transactions/ (JSONL)                │   │
│  │ (Python)     │    │  bronze/transactions/ (Parquet)           │   │
│  └──────────────┘    │  silver/transactions/ (Parquet, clean)    │   │
│                       └──────────────────────┬───────────────────┘   │
│                                              │                        │
│  ┌───────────────────────────────────────────▼───────────────────┐   │
│  │  Apache Airflow (CeleryExecutor + Redis)                       │   │
│  │  fintech_master_pipeline @hourly                               │   │
│  │                                                                 │   │
│  │  [generate] → [s3 sensor] → [bronze] → [silver] → [gold]     │   │
│  │                                           ↓           ↓        │   │
│  │                                       [GE + dbt]  [anomaly]   │   │
│  └───────────────────────────────────────────────────────────────┘   │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │  Apache Spark 3.5 (Standalone: 1 Master + 1 Worker)             │ │
│  │                                                                   │ │
│  │  bronze_ingest.py    — JSONL → Parquet + metadata               │ │
│  │  silver_cleanse.py   — type cast + PII hash + dedup             │ │
│  │  gold_modeling.py    — star schema → staging → PostgreSQL       │ │
│  │  anomaly_detection.py — 3 rules → alerts_log                   │ │
│  │  ml_scoring.py       — Isolation Forest → ML_ANOMALY alerts    │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│                                                                       │
│  ┌───────────────────────────────────────────────────────────────┐   │
│  │  PostgreSQL 15 (Gold Layer — Kimball Star Schema)              │   │
│  │                                                                 │   │
│  │  gold.fact_transactions ──┬── gold.dim_users (SCD2)           │   │
│  │                            ├── gold.dim_merchants               │   │
│  │                            └── gold.dim_time                   │   │
│  │  gold.alerts_log (fraud + ML anomaly alerts)                   │   │
│  │  gold.mart_executive_kpis (precomputed via dbt)                │   │
│  │  gold.mart_fraud_summary  (precomputed via dbt)                │   │
│  └───────────────────────────────────────────────────────────────┘   │
│                                                                       │
│  ┌─────────────────────────────────────────┐                        │
│  │  Streamlit Dashboard (Port 8501)         │                        │
│  │  Page 1: Executive KPIs                  │                        │
│  │  Page 2: Fraud Monitoring               │                        │
│  │  Page 3: Operational Health             │                        │
│  └─────────────────────────────────────────┘                        │
│                                                                       │
│  ┌───────────────────────────────────────────────────────────┐       │
│  │  Monitoring                                                │       │
│  │  Prometheus (scrapes: Airflow, Spark, PostgreSQL, LS)     │       │
│  │  Grafana (2 provisioned dashboards, auto-refreshing)      │       │
│  └───────────────────────────────────────────────────────────┘       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Medallion Architecture

| Layer | Storage | Format | Purpose |
|-------|---------|--------|---------|
| **Raw** | S3 `raw/transactions/YYYY/MM/DD/HH/` | JSONL | Immutable source of truth |
| **Bronze** | S3 `bronze/transactions/partition_date=YYYY-MM-DD/` | Parquet | Exactly as raw + metadata (batch_id, ingestion_timestamp) |
| **Silver** | S3 `silver/transactions/partition_date=YYYY-MM-DD/` | Parquet | Cleaned, typed, PII-hashed, deduplicated |
| **Gold** | PostgreSQL `gold.*` | Tables | Star schema for analytics + BI |

---

## Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Orchestration | Apache Airflow (CeleryExecutor) | 2.9.0 |
| Processing | Apache Spark (Standalone) | 3.5.1 |
| Storage | S3 via LocalStack | latest |
| Warehouse | PostgreSQL | 15 |
| Transformation | dbt-core | 1.7 |
| Quality | Great Expectations | 0.18 |
| ML Scoring | scikit-learn Isolation Forest | 1.4 |
| Dashboard | Streamlit + Plotly | 1.32 / 5.20 |
| Monitoring | Prometheus + Grafana | 2.48 / 10.2 |
| Message Queue | Redis | 7 |
| IaC | Terraform | 1.7+ |
| Container | Docker Compose | 25.x |

---

## Data Flow Detail

### Bronze Ingestion (`bronze_ingest.py`)
1. Reads JSONL files from `s3a://fintech-raw-data/raw/transactions/`
2. Adds metadata: `ingestion_timestamp`, `source_file`, `partition_date`, `bronze_batch_id` (UUID)
3. Writes Parquet partitioned by `partition_date` to `s3a://fintech-raw-data/bronze/`
4. Emits JSON audit log entry

### Silver Cleansing (`silver_cleanse.py`)
1. Reads Bronze Parquet for the target date
2. Parses `raw_data` JSON string via StructType schema
3. Applies type casts (DecimalType, TimestampType)
4. **PII hash**: `ip_address → ip_address_hash` (SHA-256), original dropped
5. Filters invalid rows (NULL keys, amount ≤ 0, future timestamps, invalid enums, out-of-range coordinates)
6. Deduplicates by `transaction_id` keeping the latest `ingestion_timestamp`
7. Writes Parquet partitioned by `partition_date` to `s3a://fintech-raw-data/silver/`

### Gold Modeling (`gold_modeling.py`)
1. Reads Silver Parquet
2. Derives `time_sk` (YYYYMMDD int) for `dim_time` join
3. Stages `fact_transactions_staging`, `dim_users_staging`, `dim_merchants_staging` via JDBC
4. dbt runs `users_snapshot` (SCD2) + core mart models to merge staging → Gold

### Anomaly Detection (`anomaly_detection.py`)
| Rule | Condition | Severity Formula |
|------|-----------|-----------------|
| `VELOCITY_ATTACK` | ≥ 10 txns per user in 60s (rolling window) | min(count/20, 1.0) |
| `HIGH_AMOUNT` | > 500% of user's 30-day rolling average | min(delta/avg, 1.0) |
| `GEO_IMPOSSIBLE` | > 800 km/h implied travel speed (Haversine) | min(speed/2000, 1.0) |

### ML Scoring (`ml_scoring.py`)
- Features: `amount`, `latitude`, `longitude`
- Algorithm: Isolation Forest (n_estimators=200, contamination=fraud_rate)
- Scoring: normalized decision_function → [0, 1], higher = more anomalous
- Threshold: `ML_SCORE_THRESHOLD` (default 0.7) → `ML_ANOMALY` alert

---

## Security Design

| Control | Implementation |
|---------|---------------|
| Credentials | `os.environ["KEY"]` — never hardcoded |
| PII protection | SHA-256 hash at Silver layer |
| Docker users | Non-root (`fintech:1000`) in all images |
| Airflow RBAC | Auth_DB + 4 roles (Admin/User/Op/Viewer) |
| Git protection | `.env` in `.gitignore` |
| S3 encryption | AES-256 (simulated in LocalStack) |
| Connection pooling | SQLAlchemy QueuePool (max 10 connections) |

---

## Fraud Detection Precision Benchmark

Run `make test-fraud` to validate:

| Scenario | Expected Detection |
|----------|--------------------|
| 12 txns in 60s (velocity) | ✅ Detected |
| $50,000 vs $10 avg (amount) | ✅ Detected |
| MX → JP in 30 min (geo) | ✅ Detected |
| 5 normal txns | ❌ Not flagged |
| $350 vs $100 avg (4× not 5×) | ❌ Not flagged |
| Same country travel | ❌ Not flagged |
