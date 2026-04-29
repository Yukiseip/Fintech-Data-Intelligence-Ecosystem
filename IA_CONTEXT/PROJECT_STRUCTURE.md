# Project Structure — Fintech Data Intelligence Ecosystem

## Complete Directory Tree

```
fintech-analytics-engine/
│
├── Makefile                              # ← SINGLE ENTRYPOINT
├── docker-compose.yml
├── docker-compose.override.yml           # Dev overrides (volumes, extra ports)
├── .env.example
├── .env                                  # ← DO NOT COMMIT (in .gitignore)
├── .gitignore
├── README.md
│
├── infrastructure/
│   ├── terraform/
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   ├── outputs.tf
│   │   └── modules/
│   │       ├── s3/
│   │       │   ├── main.tf               # Bucket fintech-raw-data + lifecycle
│   │       │   ├── variables.tf
│   │       │   └── outputs.tf
│   │       └── iam/
│   │           ├── main.tf               # Least-privilege roles
│   │           └── outputs.tf
│   └── scripts/
│       ├── init-localstack.sh            # wait-for-localstack + terraform apply
│       └── wait-for-it.sh                # Healthcheck helper
│
├── orchestration/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── dags/
│   │   ├── fintech_master_dag.py         # Main DAG with TaskGroups
│   │   ├── utils/
│   │   │   ├── __init__.py
│   │   │   ├── s3_utils.py
│   │   │   ├── spark_submit.py
│   │   │   └── notifications.py          # Slack/email alerting
│   │   └── tests/
│   │       ├── __init__.py
│   │       └── test_dag_integrity.py
│   └── config/
│       ├── airflow.cfg
│       └── webserver_config.py
│
├── processing/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── jobs/
│   │   ├── bronze_ingest.py
│   │   ├── silver_cleanse.py
│   │   ├── gold_modeling.py
│   │   └── anomaly_detection.py
│   ├── schemas/
│   │   ├── __init__.py
│   │   ├── bronze_schema.py              # StructType definitions
│   │   ├── silver_schema.py
│   │   └── gold_schema.py
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── spark_session.py              # Reusable builder
│   │   ├── s3_client.py                  # LocalStack-aware
│   │   ├── quality_checks.py             # Great Expectations wrappers
│   │   └── audit_logger.py               # JSON structured logging
│   └── tests/
│       ├── __init__.py
│       ├── conftest.py                   # Spark + test data fixtures
│       ├── test_bronze_ingest.py
│       ├── test_silver_cleanse.py
│       ├── test_gold_modeling.py
│       └── test_anomaly_rules.py
│
├── data_generator/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── config/
│   │   └── generator_config.yaml
│   ├── models/
│   │   ├── __init__.py
│   │   ├── user.py                       # Pydantic model
│   │   ├── merchant.py
│   │   ├── transaction.py
│   │   └── dispute.py
│   ├── fraud_patterns/
│   │   ├── __init__.py
│   │   ├── base.py                       # FraudPattern ABC
│   │   ├── velocity_attack.py
│   │   ├── high_amount.py
│   │   └── geographic_impossible.py
│   ├── main.py                           # CLI: generate [users|merchants|transactions|all]
│   └── tests/
│       ├── __init__.py
│       └── test_fraud_injection.py
│
├── warehouse/
│   ├── init-scripts/
│   │   ├── 01_create_roles.sql
│   │   ├── 02_create_schemas.sql         # bronze, silver, gold, analytics, staging
│   │   └── 03_create_extensions.sql      # uuid-ossp, pg_stat_statements
│   └── dbt/
│       ├── dbt_project.yml
│       ├── profiles.yml
│       ├── packages.yml
│       ├── models/
│       │   ├── staging/
│       │   │   ├── stg_transactions.sql
│       │   │   ├── stg_users.sql
│       │   │   └── stg_merchants.sql
│       │   ├── marts/
│       │   │   ├── core/
│       │   │   │   ├── fct_transactions.sql
│       │   │   │   ├── dim_users.sql     # SCD Type 2
│       │   │   │   ├── dim_merchants.sql
│       │   │   │   └── dim_time.sql
│       │   │   └── risk/
│       │   │       ├── alerts_log.sql
│       │   │       └── fraud_summary.sql
│       │   └── sources.yml
│       ├── snapshots/
│       │   └── users_snapshot.sql        # dbt snapshot for SCD2
│       ├── tests/
│       │   ├── assert_positive_revenue.sql
│       │   ├── assert_user_uniqueness.sql
│       │   └── assert_referential_integrity.sql
│       ├── macros/
│       │   ├── generate_surrogate_key.sql
│       │   └── scd_type_2.sql
│       └── docs/
│           └── overview.md
│
├── analytics/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── app.py                            # Entry point + sidebar nav
│   ├── pages/
│   │   ├── 01_executive_kpis.py
│   │   ├── 02_fraud_monitoring.py
│   │   └── 03_operational_health.py
│   ├── queries/
│   │   ├── kpi_queries.sql
│   │   └── fraud_queries.sql
│   └── utils/
│       ├── __init__.py
│       └── db_connection.py              # SQLAlchemy pool + retry logic
│
├── monitoring/
│   ├── prometheus/
│   │   └── prometheus.yml
│   ├── grafana/
│   │   ├── provisioning/
│   │   │   ├── datasources/
│   │   │   │   └── prometheus.yml
│   │   │   └── dashboards/
│   │   │       └── dashboard.yml
│   │   └── dashboards/
│   │       └── fintech_overview.json
│   └── great_expectations/
│       ├── great_expectations.yml
│       └── expectations/
│           ├── bronze_suite.json
│           └── silver_suite.json
│
├── notebooks/
│   ├── exploration/
│   │   └── eda_transactions.ipynb
│   └── prototyping/
│       └── anomaly_ml_prototype.ipynb
│
├── tests/
│   ├── integration/
│   │   ├── test_end_to_end_pipeline.py
│   │   ├── test_data_quality_gates.py
│   │   └── test_dashboard_queries.py
│   └── performance/
│       └── test_throughput.py
│
└── docs/
    ├── ARCHITECTURE.md
    ├── TECH_STACK.md
    ├── DATA_CONTRACTS.md
    ├── RUNBOOK.md
    └── API_SPECS.md
```

---

## File Purposes

### Root Level

| File | Purpose |
|------|---------|
| `Makefile` | Single entrypoint for all operations (`up`, `down`, `test`, `logs`, `clean`, `infra`, `generate`) |
| `docker-compose.yml` | Complete service definitions (12+ services) |
| `docker-compose.override.yml` | Development overrides (volume mounts, extra ports) |
| `.env.example` | Template for all environment variables |
| `.env` | Actual secrets (gitignored) |
| `.gitignore` | Excludes: `.env`, `__pycache__`, `target/`, `dbt_packages/`, etc. |
| `README.md` | Project overview and quickstart guide |

### Infrastructure (`infrastructure/`)

| File | Purpose |
|------|---------|
| `terraform/modules/s3/main.tf` | S3 bucket, lifecycle policies, encryption |
| `terraform/modules/s3/variables.tf` | S3 module variables |
| `terraform/modules/s3/outputs.tf` | S3 module outputs |
| `terraform/modules/iam/main.tf` | Least-privilege IAM roles |
| `terraform/main.tf` | Root Terraform configuration with LocalStack provider |
| `scripts/init-localstack.sh` | Wait for LocalStack + run terraform apply |
| `scripts/wait-for-it.sh` | Generic healthcheck helper |

### Orchestration (`orchestration/`)

| File | Purpose |
|------|---------|
| `dags/fintech_master_dag.py` | Main Airflow DAG with 5 TaskGroups |
| `dags/utils/s3_utils.py` | S3 operations for DAG tasks |
| `dags/utils/spark_submit.py` | Spark job submission helpers |
| `dags/utils/notifications.py` | Slack/email alerting |
| `dags/tests/test_dag_integrity.py` | DAG cycle and import tests |
| `config/airflow.cfg` | Airflow configuration |
| `config/webserver_config.py` | RBAC and webserver settings |

### Processing (`processing/`)

| File | Purpose |
|------|---------|
| `jobs/bronze_ingest.py` | Raw JSONL → Parquet (Bronze) |
| `jobs/silver_cleanse.py` | Parquet → Cleaned Parquet (Silver) |
| `jobs/gold_modeling.py` | Parquet → PostgreSQL Star Schema (Gold) |
| `jobs/anomaly_detection.py` | Fraud detection rules engine |
| `schemas/bronze_schema.py` | Spark StructType for Bronze |
| `schemas/silver_schema.py` | Spark StructType for Silver |
| `schemas/gold_schema.py` | Spark StructType for Gold |
| `utils/spark_session.py` | Reusable SparkSession builder |
| `utils/s3_client.py` | LocalStack-aware S3 client |
| `utils/quality_checks.py` | Great Expectations wrappers |
| `utils/audit_logger.py` | JSON structured logging |
| `tests/conftest.py` | Pytest fixtures (Spark session, test data) |
| `tests/test_bronze_ingest.py` | Bronze job unit tests |
| `tests/test_silver_cleanse.py` | Silver job unit tests |
| `tests/test_gold_modeling.py` | Gold job unit tests |
| `tests/test_anomaly_rules.py` | Fraud detection unit tests |

### Data Generator (`data_generator/`)

| File | Purpose |
|------|---------|
| `config/generator_config.yaml` | Generator volumes, fraud rates, output settings |
| `models/user.py` | Pydantic User model |
| `models/merchant.py` | Pydantic Merchant model |
| `models/transaction.py` | Pydantic Transaction model |
| `models/dispute.py` | Pydantic Dispute model |
| `fraud_patterns/base.py` | Abstract base class for fraud patterns |
| `fraud_patterns/velocity_attack.py` | Rapid burst fraud injection |
| `fraud_patterns/high_amount.py` | High amount deviation injection |
| `fraud_patterns/geographic_impossible.py` | Impossible travel injection |
| `main.py` | CLI entrypoint |
| `tests/test_fraud_injection.py` | Fraud pattern unit tests |

### Warehouse (`warehouse/`)

| File | Purpose |
|------|---------|
| `init-scripts/01_create_roles.sql` | PostgreSQL roles |
| `init-scripts/02_create_schemas.sql` | Schema creation (bronze, silver, gold, etc.) |
| `init-scripts/03_create_extensions.sql` | Extensions: uuid-ossp, pg_stat_statements |
| `dbt/dbt_project.yml` | dbt project configuration |
| `dbt/profiles.yml` | dbt connection profiles |
| `dbt/packages.yml` | dbt package dependencies |
| `dbt/models/staging/stg_transactions.sql` | Staging transaction model |
| `dbt/models/staging/stg_users.sql` | Staging user model |
| `dbt/models/staging/stg_merchants.sql` | Staging merchant model |
| `dbt/models/marts/core/fct_transactions.sql` | Fact transactions |
| `dbt/models/marts/core/dim_users.sql` | User dimension (SCD Type 2) |
| `dbt/models/marts/core/dim_merchants.sql` | Merchant dimension |
| `dbt/models/marts/core/dim_time.sql` | Time dimension |
| `dbt/models/marts/risk/alerts_log.sql` | Alerts mart |
| `dbt/models/marts/risk/fraud_summary.sql` | Fraud summary mart |
| `dbt/models/sources.yml` | Source definitions |
| `dbt/snapshots/users_snapshot.sql` | SCD Type 2 snapshot |
| `dbt/tests/assert_positive_revenue.sql` | Revenue positivity test |
| `dbt/tests/assert_user_uniqueness.sql` | User uniqueness test |
| `dbt/tests/assert_referential_integrity.sql` | FK integrity test |
| `dbt/macros/generate_surrogate_key.sql` | Surrogate key macro |
| `dbt/macros/scd_type_2.sql` | SCD Type 2 macro |
| `dbt/docs/overview.md` | dbt documentation |

### Analytics (`analytics/`)

| File | Purpose |
|------|---------|
| `app.py` | Streamlit entry point with sidebar navigation |
| `pages/01_executive_kpis.py` | Executive KPIs page |
| `pages/02_fraud_monitoring.py` | Fraud monitoring page |
| `pages/03_operational_health.py` | Operational health page |
| `queries/kpi_queries.sql` | Reusable KPI SQL queries |
| `queries/fraud_queries.sql` | Reusable fraud SQL queries |
| `utils/db_connection.py` | SQLAlchemy pool with retry logic |

### Monitoring (`monitoring/`)

| File | Purpose |
|------|---------|
| `prometheus/prometheus.yml` | Prometheus scrape configuration |
| `grafana/provisioning/datasources/prometheus.yml` | Grafana datasource provisioning |
| `grafana/provisioning/dashboards/dashboard.yml` | Grafana dashboard provisioning |
| `grafana/dashboards/fintech_overview.json` | Main dashboard JSON |
| `great_expectations/great_expectations.yml` | GE configuration |
| `great_expectations/expectations/bronze_suite.json` | Bronze expectations |
| `great_expectations/expectations/silver_suite.json` | Silver expectations |

### Tests (`tests/`)

| File | Purpose |
|------|---------|
| `integration/test_end_to_end_pipeline.py` | Full pipeline E2E test |
| `integration/test_data_quality_gates.py` | Quality gate integration test |
| `integration/test_dashboard_queries.py` | Dashboard query integration test |
| `performance/test_throughput.py` | Throughput performance test |

### Notebooks (`notebooks/`)

| File | Purpose |
|------|---------|
| `exploration/eda_transactions.ipynb` | Transaction EDA |
| `prototyping/anomaly_ml_prototype.ipynb` | ML anomaly detection prototype |

### Documentation (`docs/`)

| File | Purpose |
|------|---------|
| `ARCHITECTURE.md` | System architecture |
| `TECH_STACK.md` | Technology choices and justifications |
| `DATA_CONTRACTS.md` | Data schemas and contracts |
| `RUNBOOK.md` | Operational runbook |
| `API_SPECS.md` | API specifications |

---

## Service-to-File Mapping

| Service | Dockerfile | Key Files |
|---------|-----------|-----------|
| postgres | `postgres:16-alpine` (official) | `warehouse/init-scripts/*.sql` |
| redis | `redis:7-alpine` (official) | — |
| localstack | `localstack/localstack:3.0` (official) | `infrastructure/scripts/*.sh`, `infrastructure/terraform/**` |
| airflow-webserver | `orchestration/Dockerfile` | `orchestration/dags/**`, `orchestration/config/**` |
| airflow-scheduler | `orchestration/Dockerfile` | `orchestration/dags/**`, `orchestration/config/**` |
| airflow-worker | `orchestration/Dockerfile` | `orchestration/dags/**`, `orchestration/config/**` |
| spark-master | `bitnami/spark:3.5` (official) | — |
| spark-worker | `bitnami/spark:3.5` (official) | — |
| data-generator | `data_generator/Dockerfile` | `data_generator/**` |
| processing | `processing/Dockerfile` | `processing/**` |
| streamlit | `analytics/Dockerfile` | `analytics/**` |
| prometheus | `prom/prometheus:latest` (official) | `monitoring/prometheus/**` |
| grafana | `grafana/grafana:latest` (official) | `monitoring/grafana/**` |
| jupyter | `jupyter/pyspark-notebook` (official) | `notebooks/**` |
