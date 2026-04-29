# Implementation Guide for AI Assistants — Fintech Data Intelligence Ecosystem

## 🎯 How to Use This Guide

This document provides step-by-step implementation instructions for AI coding assistants (Claude Code, Cursor, GitHub Copilot, Windsurf, etc.) working on the Fintech Data Intelligence Ecosystem.

**ALWAYS read the master TSD first** before writing any code.

---

## 📋 Pre-Implementation Checklist

Before generating any code, verify you have read and understood:
- [ ] `AI_CONTEXT.md` — Project context and constraints
- [ ] `ARCHITECTURE.md` — System architecture and data flow
- [ ] `DATA_CONTRACTS.md` — All schemas and validation rules
- [ ] `SPRINT_PLAN.md` — Current sprint deliverables and dependencies
- [ ] `FINTECH_TSD_v3_0_MASTER.md` — Complete technical specification

---

## 🔧 Implementation Protocol

### Rule 1: Sprint Order is Sacred
- **NEVER** start Sprint 2 before Sprint 1 is fully functional
- **NEVER** skip deliverables within a sprint
- Each deliverable must pass its acceptance criteria before moving on

### Rule 2: Tests First
- Write tests BEFORE or ALONGSIDE production code
- No code is accepted without corresponding tests
- Target coverage: Bronze 70%, Silver 80%, Anomaly Rules 90%

### Rule 3: One Commit Per Deliverable
```
feat(S1-D1): Docker Compose environment with healthchecks
feat(S1-D2): Terraform S3 bucket and lifecycle policies
feat(S2-D1): Spark Silver cleansing job with deduplication
```

### Rule 4: No Hardcoding
- **NEVER** hardcode endpoints, credentials, or paths
- **ALWAYS** use environment variables via `.env`
- **ALWAYS** provide `.env.example` with dummy values

### Rule 5: Type Hints & Docstrings
Every Python function MUST have:
- Type hints for all parameters and return values
- Google-style docstrings
- Proper error handling

Example:
```python
def run_bronze_ingest(date: str, hour: str, s3_bucket: str) -> dict:
    """
    Execute Bronze ingestion for a given date partition.

    Args:
        date: Processing date in YYYY-MM-DD format.
        hour: Processing hour in HH format (00-23).
        s3_bucket: S3 bucket name.

    Returns:
        dict with run metrics: input_rows, output_rows, duration_seconds.

    Raises:
        ValueError: If date or hour format is invalid.
    """
```

### Rule 6: Stop on Ambiguity
If a requirement is ambiguous, **STOP and ask** — do not assume.

---

## 🏗️ Implementation Order

### Phase 1: Project Skeleton (Sprint 1, Day 1)

1. **Create directory structure**
   ```bash
   mkdir -p fintech-analytics-engine/{infrastructure/{terraform/modules/{s3,iam},scripts},orchestration/{dags/{utils,tests},config},processing/{jobs,schemas,utils,tests},data_generator/{config,models,fraud_patterns,tests},warehouse/{init-scripts,dbt/{models/{staging,marts/{core,risk}},snapshots,tests,macros,docs}},analytics/{pages,queries,utils},monitoring/{prometheus,grafana/{provisioning/{datasources,dashboards},dashboards},great_expectations/expectations},notebooks/{exploration,prototyping},tests/{integration,performance},docs}
   ```

2. **Create base files**
   - `Makefile`
   - `.env.example`
   - `.gitignore`
   - `README.md` (placeholder)
   - `docker-compose.yml`
   - `docker-compose.override.yml`

3. **Create Dockerfiles**
   - `orchestration/Dockerfile`
   - `processing/Dockerfile`
   - `data_generator/Dockerfile`
   - `analytics/Dockerfile`

### Phase 2: Infrastructure (Sprint 1, Days 1-3)

1. **Terraform S3 Module**
   - `infrastructure/terraform/modules/s3/main.tf`
   - `infrastructure/terraform/modules/s3/variables.tf`
   - `infrastructure/terraform/modules/s3/outputs.tf`
   - `infrastructure/terraform/main.tf`
   - `infrastructure/terraform/variables.tf`
   - `infrastructure/terraform/outputs.tf`

2. **Init Scripts**
   - `infrastructure/scripts/init-localstack.sh`
   - `infrastructure/scripts/wait-for-it.sh`

3. **Test Infrastructure**
   ```bash
   make up
   make infra
   # Verify: curl http://localhost:4566/_localstack/health
   ```

### Phase 3: Data Generator (Sprint 1, Days 3-5)

1. **Configuration**
   - `data_generator/config/generator_config.yaml`

2. **Models**
   - `data_generator/models/__init__.py`
   - `data_generator/models/user.py` (Pydantic)
   - `data_generator/models/merchant.py` (Pydantic)
   - `data_generator/models/transaction.py` (Pydantic)
   - `data_generator/models/dispute.py` (Pydantic)

3. **Fraud Patterns**
   - `data_generator/fraud_patterns/__init__.py`
   - `data_generator/fraud_patterns/base.py` (ABC)
   - `data_generator/fraud_patterns/velocity_attack.py`
   - `data_generator/fraud_patterns/high_amount.py`
   - `data_generator/fraud_patterns/geographic_impossible.py`

4. **Main Entrypoint**
   - `data_generator/main.py`

5. **Tests**
   - `data_generator/tests/__init__.py`
   - `data_generator/tests/test_fraud_injection.py`

6. **Test Generator**
   ```bash
   make generate
   # Verify: aws s3 ls s3://fintech-raw-data/raw/ --endpoint-url=http://localhost:4566
   ```

### Phase 4: Spark Processing (Sprint 1-2)

1. **Utilities**
   - `processing/utils/__init__.py`
   - `processing/utils/spark_session.py`
   - `processing/utils/s3_client.py`
   - `processing/utils/audit_logger.py`
   - `processing/utils/quality_checks.py`

2. **Schemas**
   - `processing/schemas/__init__.py`
   - `processing/schemas/bronze_schema.py`
   - `processing/schemas/silver_schema.py`
   - `processing/schemas/gold_schema.py`

3. **Jobs**
   - `processing/jobs/bronze_ingest.py`
   - `processing/jobs/silver_cleanse.py`
   - `processing/jobs/gold_modeling.py`
   - `processing/jobs/anomaly_detection.py`

4. **Tests**
   - `processing/tests/__init__.py`
   - `processing/tests/conftest.py`
   - `processing/tests/test_bronze_ingest.py`
   - `processing/tests/test_silver_cleanse.py`
   - `processing/tests/test_gold_modeling.py`
   - `processing/tests/test_anomaly_rules.py`

### Phase 5: Airflow Orchestration (Sprint 1-2)

1. **Configuration**
   - `orchestration/config/airflow.cfg`
   - `orchestration/config/webserver_config.py`

2. **Utilities**
   - `orchestration/dags/utils/__init__.py`
   - `orchestration/dags/utils/s3_utils.py`
   - `orchestration/dags/utils/spark_submit.py`
   - `orchestration/dags/utils/notifications.py`

3. **DAG**
   - `orchestration/dags/fintech_master_dag.py`

4. **Tests**
   - `orchestration/dags/tests/__init__.py`
   - `orchestration/dags/tests/test_dag_integrity.py`

### Phase 6: Warehouse & dbt (Sprint 2)

1. **PostgreSQL Init**
   - `warehouse/init-scripts/01_create_roles.sql`
   - `warehouse/init-scripts/02_create_schemas.sql`
   - `warehouse/init-scripts/03_create_extensions.sql`

2. **dbt Project**
   - `warehouse/dbt/dbt_project.yml`
   - `warehouse/dbt/profiles.yml`
   - `warehouse/dbt/packages.yml`

3. **Models**
   - `warehouse/dbt/models/sources.yml`
   - `warehouse/dbt/models/staging/stg_transactions.sql`
   - `warehouse/dbt/models/staging/stg_users.sql`
   - `warehouse/dbt/models/staging/stg_merchants.sql`
   - `warehouse/dbt/models/marts/core/fct_transactions.sql`
   - `warehouse/dbt/models/marts/core/dim_users.sql`
   - `warehouse/dbt/models/marts/core/dim_merchants.sql`
   - `warehouse/dbt/models/marts/core/dim_time.sql`
   - `warehouse/dbt/models/marts/risk/alerts_log.sql`
   - `warehouse/dbt/models/marts/risk/fraud_summary.sql`

4. **Snapshots**
   - `warehouse/dbt/snapshots/users_snapshot.sql`

5. **Tests**
   - `warehouse/dbt/tests/assert_positive_revenue.sql`
   - `warehouse/dbt/tests/assert_user_uniqueness.sql`
   - `warehouse/dbt/tests/assert_referential_integrity.sql`

6. **Macros**
   - `warehouse/dbt/macros/generate_surrogate_key.sql`
   - `warehouse/dbt/macros/scd_type_2.sql`

### Phase 7: Analytics Dashboard (Sprint 3)

1. **Application**
   - `analytics/app.py`
   - `analytics/requirements.txt`
   - `analytics/Dockerfile`

2. **Pages**
   - `analytics/pages/01_executive_kpis.py`
   - `analytics/pages/02_fraud_monitoring.py`
   - `analytics/pages/03_operational_health.py`

3. **Queries**
   - `analytics/queries/kpi_queries.sql`
   - `analytics/queries/fraud_queries.sql`

4. **Utilities**
   - `analytics/utils/__init__.py`
   - `analytics/utils/db_connection.py`

### Phase 8: Monitoring (Sprint 3)

1. **Prometheus**
   - `monitoring/prometheus/prometheus.yml`

2. **Grafana**
   - `monitoring/grafana/provisioning/datasources/prometheus.yml`
   - `monitoring/grafana/provisioning/dashboards/dashboard.yml`
   - `monitoring/grafana/dashboards/fintech_overview.json`

3. **Great Expectations**
   - `monitoring/great_expectations/great_expectations.yml`
   - `monitoring/great_expectations/expectations/bronze_suite.json`
   - `monitoring/great_expectations/expectations/silver_suite.json`

### Phase 9: Integration Tests (Sprint 3)

1. **E2E Tests**
   - `tests/integration/test_end_to_end_pipeline.py`
   - `tests/integration/test_data_quality_gates.py`
   - `tests/integration/test_dashboard_queries.py`

2. **Performance Tests**
   - `tests/performance/test_throughput.py`

### Phase 10: Documentation (Sprint 3)

1. **Docs**
   - `docs/ARCHITECTURE.md`
   - `docs/TECH_STACK.md`
   - `docs/DATA_CONTRACTS.md`
   - `docs/RUNBOOK.md`
   - `docs/API_SPECS.md`

---

## 🧪 Testing Strategy

### Unit Tests
```bash
# Data Generator
docker compose run --rm data-generator pytest tests/ -v

# Processing (Spark)
docker compose run --rm processing pytest tests/ -v --tb=short

# Airflow DAG integrity
docker compose exec airflow-scheduler python -m pytest /opt/airflow/dags/tests/ -v

# dbt
docker compose exec warehouse-dbt dbt test --target dev
```

### Integration Tests
```bash
make test-integration
```

### Performance Tests
```bash
make test-perf    # Throughput: 10K txns/min
make test-fraud   # Fraud detection: >95% precision
make test-dash    # Dashboard queries: <2s p95
```

---

## 🔐 Security Checklist

For every file you create, verify:

- [ ] No hardcoded passwords, API keys, or tokens
- [ ] No hardcoded endpoints or connection strings
- [ ] All secrets referenced via `os.environ` or `os.getenv`
- [ ] PII fields hashed (SHA-256) before Silver layer
- [ ] `.env` listed in `.gitignore`
- [ ] Non-root user in Dockerfile (`USER` directive)
- [ ] No sensitive data in logs or error messages

---

## 🐛 Common Issues & Solutions

### Issue: Spark cannot connect to LocalStack S3
**Solution:** Verify `fs.s3a.endpoint` points to `http://localstack:4566` and `path.style.access` is `true`.

### Issue: Airflow tasks fail with connection errors
**Solution:** Check service healthchecks in docker-compose.yml. Ensure postgres and redis are healthy before airflow starts.

### Issue: dbt tests fail on foreign key constraints
**Solution:** Ensure Gold job ran successfully and populated dimension tables before dbt models run.

### Issue: Streamlit cannot connect to PostgreSQL
**Solution:** Verify `STREAMLIT_DB_CONNECTION` env var and PostgreSQL healthcheck.

---

## 📚 Reference Materials

| Topic | Reference |
|-------|-----------|
| Spark S3A Config | https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html |
| Airflow SparkSubmitOperator | https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/operators.html |
| dbt Best Practices | https://docs.getdbt.com/best-practices |
| Great Expectations | https://docs.greatexpectations.io/docs/ |
| Streamlit Docs | https://docs.streamlit.io/ |
| LocalStack S3 | https://docs.localstack.cloud/user-guide/aws/s3/ |
