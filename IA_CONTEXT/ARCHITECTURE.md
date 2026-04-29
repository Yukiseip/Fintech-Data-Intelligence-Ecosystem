# Architecture Overview — Fintech Data Intelligence Ecosystem

## System Architecture Diagram

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────────┐
│  Data Generator  │────▶│  S3 LocalStack   │────▶│  Spark: Bronze Job   │
│  Python + Faker  │     │  Raw JSONL files │     │  JSONL → Parquet     │
└──────────────────┘     └──────────────────┘     └──────────────────────┘
         │                                                    │
         │                                                    ▼
         │                                       ┌──────────────────────┐
         │                                       │  Spark: Silver Job   │
         │                                       │  Dedup + Validate    │
         │                                       └──────────────────────┘
         │                                                    │
         │                                                    ▼
         │                                       ┌──────────────────────┐
         │                                       │  Spark: Gold Job     │
         │                                       │  Star Schema → PG    │
         │                                       └──────────────────────┘
         │                                                    │
         ▼                                                    ▼
┌──────────────────┐                            ┌──────────────────────┐
│  Airflow DAG     │──── Orquesta todo ────────▶│  PostgreSQL DWH      │
│  @hourly         │                            │  Gold + Alerts       │
└──────────────────┘                            └──────────────────────┘
         │                                                    │
         ▼                                                    ▼
┌──────────────────┐                            ┌──────────────────────┐
│  Anomaly Engine  │                            │  Streamlit Dashboard │
│  Rules + ML      │                            │  KPIs + Fraud BI     │
└──────────────────┘                            └──────────────────────┘
         │                                                    ▲
         ▼                                                    │
┌──────────────────┐                            ┌──────────────────────┐
│  dbt Transforms  │                            │  Prometheus + Grafana│
│  Gold modeling   │                            │  Observabilidad      │
└──────────────────┘                            └──────────────────────┘
```

## Service Dependency Graph

```
postgres (healthcheck: pg_isready)
    └── airflow-webserver (depends_on: postgres, redis)
    └── airflow-scheduler (depends_on: postgres, redis)
    └── airflow-worker ×2 (depends_on: postgres, redis)

redis (healthcheck: redis-cli ping)
    └── airflow-worker (celery broker)

localstack (healthcheck: /health endpoint)
    └── (terraform init runs after healthcheck)

spark-master (healthcheck: /json endpoint)
    └── spark-worker ×2

data-generator → localstack (S3 boto3)
airflow → spark-master (SparkSubmitOperator)
airflow → postgres (PostgresOperator, metadata)
streamlit → postgres (SQLAlchemy read-only)
prometheus → airflow-webserver, postgres-exporter, spark-master
grafana → prometheus
```

## Medallion Data Flow

### Bronze Layer (Raw)
- **Source:** S3 JSONL files from data generator
- **Format:** Parquet with Snappy compression
- **Properties:** Append-only, immutable
- **Path:** `s3://fintech-raw-data/bronze/transactions/partition_date=YYYY-MM-DD/`
- **Key Columns:** raw_data, ingestion_timestamp, source_file, partition_date, bronze_batch_id

### Silver Layer (Cleaned)
- **Source:** Bronze Parquet
- **Format:** Parquet with Snappy compression
- **Properties:** Quality-gated (Great Expectations suite must pass)
- **Path:** `s3://fintech-raw-data/silver/transactions/partition_date=YYYY-MM-DD/`
- **Transformations:**
  - JSON parsing of raw_data
  - Type casting and validation
  - Deduplication by transaction_id (keep latest)
  - PII hashing (ip_address → ip_address_hash SHA-256)
  - Invalid row filtering

### Gold Layer (Modeled)
- **Source:** Silver Parquet
- **Target:** PostgreSQL DWH
- **Schema:** Kimball Star Schema
- **Tables:**
  - `gold.fact_transactions` — Fact table with foreign keys
  - `gold.dim_users` — SCD Type 2 dimension
  - `gold.dim_merchants` — Merchant dimension
  - `gold.dim_time` — Time dimension (pre-populated 2020-2030)
  - `gold.alerts_log` — Fraud alerts and anomalies

## Airflow DAG Structure

```
fintech_master_pipeline (@hourly)
│
├── TaskGroup: data_generation
│   └── generate_transactions (BashOperator)
│
├── TaskGroup: ingestion
│   ├── wait_for_s3_data (S3KeySensor)
│   └── spark_bronze_job (SparkSubmitOperator)
│
├── TaskGroup: transformation
│   ├── spark_silver_job (SparkSubmitOperator)
│   └── spark_gold_job (SparkSubmitOperator)
│
├── TaskGroup: quality_and_modeling
│   ├── dbt_run (BashOperator)
│   ├── dbt_test (BashOperator)
│   └── great_expectations_validate (PythonOperator)
│
└── TaskGroup: anomaly_detection
    ├── spark_anomaly_detection (SparkSubmitOperator)
    └── update_fraud_flags (PostgresOperator)

Dependencies:
data_generation >> ingestion >> transformation >> quality_and_modeling >> anomaly_detection
```

## Fraud Detection Architecture

### Rule-Based Engine (3 Patterns)
1. **VELOCITY_ATTACK:** ≥10 transactions from same user in 60 seconds
2. **HIGH_AMOUNT:** Transaction >500% of user's 30-day average amount
3. **GEO_IMPOSSIBLE:** Physically impossible travel (>800 km/h between transactions)

### Output
- Writes to `gold.alerts_log` table
- Updates `gold.fact_transactions.is_flagged_fraud` and `fraud_score`

## Observability Stack

### Metrics (Prometheus)
- Airflow task metrics
- PostgreSQL exporter metrics
- Spark master metrics
- Custom pipeline metrics

### Visualization (Grafana)
- Pipeline health dashboard
- Data freshness monitoring
- Fraud detection KPIs

### Data Quality
- Great Expectations: Silver layer expectations
- dbt tests: Gold layer assertions
- Custom quality gates that block pipeline on failure

## Security Architecture

```
┌─────────────────────────────────────────┐
│           Application Layer              │
│  Streamlit (read-only) | Airflow (RBAC) │
└─────────────────────────────────────────┘
                   │
┌─────────────────────────────────────────┐
│           Processing Layer               │
│  Spark (no hardcoded credentials)        │
│  dbt (SQL transforms)                    │
└─────────────────────────────────────────┘
                   │
┌─────────────────────────────────────────┐
│           Storage Layer                  │
│  S3/LocalStack (AES-256 encryption)      │
│  PostgreSQL (schema-separated, RBAC)     │
└─────────────────────────────────────────┘
                   │
┌─────────────────────────────────────────┐
│           Secrets Management             │
│  .env file (gitignored)                  │
│  Environment variables only              │
└─────────────────────────────────────────┘
```

## Key Architectural Decisions

| ID | Decision | Alternatives | Rationale |
|----|----------|-------------|-----------|
| AD-01 | Spark over Pandas | Pandas, Dask | Scales to TBs; unified batch/streaming API |
| AD-02 | Airflow over Prefect/Dagster | Prefect, Dagster | Mature ecosystem; Celery scales; native backfill |
| AD-03 | PostgreSQL over ClickHouse | ClickHouse, Snowflake | Simple local dev; dbt-native; sufficient for <1TB |
| AD-04 | LocalStack over real AWS | Real AWS, MinIO | Zero cost; reproducible; ideal for CI/CD |
| AD-05 | Parquet over Delta Lake | Delta Lake, Iceberg | Simpler setup; sufficient for immutable Bronze/Silver |
| AD-06 | Streamlit over Tableau | Tableau, PowerBI, Metabase | Code-native; free; integrates with Python stack |
| AD-07 | CeleryExecutor over LocalExecutor | LocalExecutor, K8s | Horizontal scaling; production-ready |
| AD-08 | dbt Core over Spark SQL | Pure Spark SQL, SQLMesh | Versioned SQL; native tests; auto lineage/docs |
