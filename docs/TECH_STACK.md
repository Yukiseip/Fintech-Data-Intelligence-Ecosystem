# Tech Stack — Fintech Data Intelligence Platform

> Rationale for each technology choice, version pins, and upgrade paths.

---

## Core Services

| Technology | Version | Role | Rationale |
|-----------|---------|------|-----------|
| **Apache Spark** | 3.5.1 | Distributed ETL | Native Parquet/S3A, Structured Streaming ready, Python UDFs |
| **Apache Airflow** | 2.9.0 | Orchestration | CeleryExecutor for horizontal scaling; S3, Spark, Postgres providers |
| **Redis** | 7.x | Celery broker | Low-latency queue; required by CeleryExecutor |
| **PostgreSQL** | 15 | Gold DWH | JSONB, window functions, `pg_stat_statements`, ACID compliance |
| **LocalStack** | latest | AWS S3 simulation | Reproduces production S3 without cloud costs; supports Terraform |
| **Terraform** | 1.7+ | IaC | Declarative S3 bucket/IAM provisioning; reproducible infra |

---

## Processing Libraries

| Library | Version | Purpose |
|---------|---------|---------|
| `pyspark` | 3.5.1 | Spark Python API |
| `boto3` | 1.34.69 | S3 client |
| `pyarrow` | 15.0.2 | Parquet read/write |
| `pydantic` | 2.x | Data model validation (generator) |
| `scikit-learn` | 1.4 | Isolation Forest ML scoring |
| `great-expectations` | 0.18.12 | Data quality validation |
| `psycopg2-binary` | 2.9.9 | PostgreSQL JDBC driver wrapper |
| `sqlalchemy` | 1.4.52 | ORM / connection pooling |

---

## Analytics / Dashboard

| Library | Version | Purpose |
|---------|---------|---------|
| `streamlit` | 1.32.0 | Interactive dashboard |
| `plotly` | 5.20.0 | Charts (area, pie, bar, gauge) |
| `pandas` | 2.2.1 | In-memory data manipulation |

---

## dbt Project

| Plugin | Version | Purpose |
|--------|---------|---------|
| `dbt-core` | 1.7.x | SQL transformation engine |
| `dbt-postgres` | 1.7.x | PostgreSQL adapter |
| `dbt-utils` | 1.1.x | Expression tests, macros |

---

## Monitoring Stack

| Tool | Version | Role |
|------|---------|------|
| Prometheus | 2.48 | Metrics collection (30s scrape interval) |
| Grafana | 10.2 | Dashboard visualization + alerting |
| `postgres-exporter` | 0.15 | PostgreSQL `pg_stat_statements` → Prometheus |

---

## Development Tools

| Tool | Version | Role |
|------|---------|------|
| `pytest` | 8.1.1 | Unit + integration testing |
| `pytest-cov` | 5.0.0 | Coverage reporting (≥ 70% enforced) |
| `chispa` | 0.9.4 | Spark DataFrame equality assertions |
| `flake8` | 7.0.0 | PEP 8 linting |
| `mypy` | 1.9.0 | Static type checking |
| `Docker Compose` | 25.x | Local multi-service orchestration |

---

## Upgrade Policy

1. **Airflow**: Follow [provider compatibility matrix](https://airflow.apache.org/docs/apache-airflow-providers/).
2. **Spark**: Test bronze/silver/gold jobs in local mode before upgrading.
3. **dbt**: Check [dbt release notes](https://github.com/dbt-labs/dbt-core/releases) for breaking changes in `dbt_project.yml` format.
4. **scikit-learn**: Retrain Isolation Forest after any upgrade; pickle may be incompatible.
5. **Great Expectations**: v1.0 introduced breaking API changes; stay on 0.18.x until GA.
