# Sprint Plan — Fintech Data Intelligence Ecosystem

## Overview
- **Total Duration:** 3 Sprints / 6 Weeks
- **Sprint Length:** 2 weeks
- **Team:** Senior Staff Data Engineer
- **Status:** Approved for Development

---

## Sprint 1: Foundation & Infrastructure (Weeks 1-2)

**Objective:** Fully functional environment with synthetic data flowing to Bronze layer.

### Deliverables

| ID | Deliverable | Acceptance Criteria | Commit Message |
|----|-------------|---------------------|----------------|
| S1-D1 | Docker Compose with all services | `make up` starts all services; healthchecks pass in <5 min | `feat(S1-D1): Docker Compose environment with healthchecks` |
| S1-D2 | Terraform IaC in LocalStack | `make infra` creates S3 bucket; LocalStack responds 200 | `feat(S1-D2): Terraform S3 bucket and lifecycle policies` |
| S1-D3 | Data Generator (4 entities) | Generates users/merchants/transactions/disputes; fraud injection works; writes to S3 | `feat(S1-D3): Synthetic data generator with fraud patterns` |
| S1-D4 | Bronze Ingestion Job (Spark) | Reads JSONL from S3 → writes Parquet; schema enforced; partitioning correct | `feat(S1-D4): Spark Bronze ingestion job` |
| S1-D5 | Airflow DAG skeleton | DAG loads without errors; tasks visible in UI; TaskGroups correct | `feat(S1-D5): Airflow master DAG skeleton` |
| S1-D6 | Complete Makefile | Targets: up, down, test, logs, clean, infra, generate | `feat(S1-D6): Makefile with all operational targets` |

### Sprint 1 Tasks Detail

#### S1-TASK-01: Makefile and Project Base
- Create `Makefile` with all operational targets
- Create `.env.example` with all required variables
- Create `.gitignore` excluding `.env`, `__pycache__`, `target/`, etc.

#### S1-TASK-02: Docker Compose
- Define all 12+ services with proper healthchecks
- Configure `x-airflow-common` anchor for DRY configuration
- Set up volume mounts for persistence
- Configure service dependencies and startup order

#### S1-TASK-03: Terraform S3 Module
- Create S3 bucket with lifecycle policies
- Set up folder prefixes: raw/, bronze/, silver/, gold/
- Configure server-side encryption (AES-256)
- Create init script for LocalStack

#### S1-TASK-04: Data Generator
- Pydantic models: User, Merchant, Transaction, Dispute
- Fraud pattern injectors (ABC base class)
- VelocityAttack, HighAmount, GeographicImpossible patterns
- CLI: `generate [users|merchants|transactions|all]`
- S3 upload with proper path structure and _SUCCESS marker

#### S1-TASK-05: Spark Bronze Job
- Reusable SparkSession builder with LocalStack S3 config
- Read JSONL from S3, add metadata columns
- Write Parquet with partition_date partitioning
- Audit logging with run metrics

#### S1-TASK-06: Airflow DAG Skeleton
- Master DAG with 5 TaskGroups
- Proper schedule (@hourly), retries, timeouts
- SparkSubmitOperator configuration
- S3KeySensor for data arrival

---

## Sprint 2: Transformation, Quality & Warehouse (Weeks 3-4)

**Objective:** Complete Bronze → Silver → Gold pipeline with quality gates and dbt models.

### Deliverables

| ID | Deliverable | Acceptance Criteria |
|----|-------------|---------------------|
| S2-D1 | Silver Cleansing Job (Spark) | Dedup, casting, null filtering; GE suite passes; bad records rejected |
| S2-D2 | Gold Modeling Job (Spark) | Star schema in Postgres; referential integrity validated |
| S2-D3 | Complete dbt Project | All mart models run; `dbt test` 100% green |
| S2-D4 | SCD Type 2 in dim_users | Historical changes tracked correctly |
| S2-D5 | Data Quality Gates | GE + dbt tests block bad data; pipeline stops on failure |
| S2-D6 | End-to-end DAG functional | DAG runs without manual intervention; 7-day backfill OK |

### Sprint 2 Tasks Detail

#### S2-TASK-01: Silver Cleansing Job
- Parse raw_data JSON into typed columns
- Cast types (Decimal, Timestamp, etc.)
- Hash PII (ip_address → SHA-256)
- Validate against allowed values and ranges
- Deduplicate by transaction_id (keep latest)
- Write quality-gated Parquet

#### S2-TASK-02: Gold Modeling Job
- Read Silver Parquet
- Build fact_transactions with surrogate keys
- Stage dim_merchants and dim_users
- Pre-populate dim_time (2020-2030)
- Write to PostgreSQL via JDBC

#### S2-TASK-03: dbt Project
- dbt_project.yml with proper paths and configs
- Staging models: stg_transactions, stg_users, stg_merchants
- Mart models: fct_transactions, dim_users, dim_merchants, dim_time
- Risk models: alerts_log, fraud_summary
- Macros: generate_surrogate_key, scd_type_2

#### S2-TASK-04: SCD Type 2
- dbt snapshot for dim_users
- Track changes in: risk_tier, country_code, email_hash
- valid_from/valid_to dates
- is_current flag

#### S2-TASK-05: Data Quality Gates
- Great Expectations suite for Silver (≥10 expectations)
- dbt tests for Gold (positive revenue, user uniqueness, referential integrity)
- Pipeline blocking on quality failure

#### S2-TASK-06: DAG End-to-End
- Wire all TaskGroups with proper dependencies
- Test with 7-day backfill
- Verify all tasks complete successfully

---

## Sprint 3: Anomaly Detection, BI & Hardening (Weeks 5-6)

**Objective:** Operational fraud engine, Streamlit dashboard, monitoring, and documentation.

### Deliverables

| ID | Deliverable | Acceptance Criteria |
|----|-------------|---------------------|
| S3-D1 | Anomaly Detection Engine | 3 rule types; alerts_log populated; >95% precision on test set |
| S3-D2 | ML Scoring (Stretch) | Isolation Forest trained; scores in alerts_log |
| S3-D3 | Streamlit Dashboard | 3 functional pages; KPIs match SQL queries |
| S3-D4 | Monitoring Stack | Grafana shows pipeline health; Prometheus scraping active |
| S3-D5 | Security Hardening | No credentials in code; RBAC in Airflow; PII hashed |
| S3-D6 | Final Documentation | README complete; RUNBOOK; all `make` commands documented |

### Sprint 3 Tasks Detail

#### S3-TASK-01: Anomaly Detection Engine
- **Rule 1 — VELOCITY_ATTACK:** ≥10 txns in 60s window
- **Rule 2 — HIGH_AMOUNT:** >500% of user's 30-day average
- **Rule 3 — GEO_IMPOSSIBLE:** >800 km/h implied speed
- Haversine formula for distance calculation
- Write alerts to gold.alerts_log
- Update fact_transactions fraud flags

#### S3-TASK-02: ML Scoring (Stretch Goal)
- Isolation Forest on transaction features
- Feature engineering: amount, velocity, geo-deviation
- Store ml_score in alerts_log

#### S3-TASK-03: Streamlit Dashboard
- **Page 1 — Executive KPIs:** TPV, volume, fraud rate, trends
- **Page 2 — Fraud Monitoring:** Real-time alerts, heatmaps, severity
- **Page 3 — Operational Health:** Pipeline status, data freshness
- SQLAlchemy connection pool with retry logic
- Plotly visualizations

#### S3-TASK-04: Monitoring Stack
- Prometheus configuration for all services
- Grafana dashboards provisioned
- Pipeline health metrics
- Data freshness alerts

#### S3-TASK-05: Security Hardening
- Verify zero credentials in codebase
- Airflow RBAC configuration
- PII hashing verification
- Schema permissions in PostgreSQL
- Non-root Docker users

#### S3-TASK-06: Documentation
- README.md with quickstart
- ARCHITECTURE.md
- DATA_CONTRACTS.md
- RUNBOOK.md with troubleshooting
- API_SPECS.md
- TECH_STACK.md

---

## Sprint Dependencies

```
Sprint 1 (Foundation)
    ├── S1-D1: Docker Compose
    ├── S1-D2: Terraform
    ├── S1-D3: Data Generator
    ├── S1-D4: Bronze Job
    ├── S1-D5: Airflow DAG
    └── S1-D6: Makefile
         │
         ▼
Sprint 2 (Transformation)
    ├── S2-D1: Silver Job (needs S1-D4)
    ├── S2-D2: Gold Job (needs S2-D1)
    ├── S2-D3: dbt Project (needs S2-D2)
    ├── S2-D4: SCD Type 2 (needs S2-D3)
    ├── S2-D5: Quality Gates (needs S2-D1, S2-D3)
    └── S2-D6: E2E DAG (needs all above)
         │
         ▼
Sprint 3 (Analytics & Hardening)
    ├── S3-D1: Anomaly Detection (needs S2-D2)
    ├── S3-D2: ML Scoring (needs S3-D1)
    ├── S3-D3: Streamlit (needs S2-D3)
    ├── S3-D4: Monitoring (needs all)
    ├── S3-D5: Security (needs all)
    └── S3-D6: Docs (needs all)
```

---

## Definition of Done (Project Level)

Before declaring the project complete, ALL of the following must be ✅:

- [ ] `make up` brings up functional environment in <10 min on fresh clone
- [ ] `make test` passes all unit + integration + data quality tests
- [ ] `make down` destroys everything without orphan resources or dangling volumes
- [ ] DAG runs successfully for 7 consecutive days (simulated with backfill)
- [ ] Dashboard displays all 6 required KPIs with query time <2s
- [ ] Fraud detection identifies all 3 patterns with >95% accuracy on labeled test set
- [ ] Documentation complete (README, ARCHITECTURE, RUNBOOK, DATA_CONTRACTS)
- [ ] Zero credentials committed to repository
- [ ] Code review checklist completed (type hints, docstrings, error handling)
- [ ] All Dockerfiles use non-root users and multi-stage builds where applicable
- [ ] Great Expectations suite has ≥10 expectations in Silver layer
- [ ] dbt `test` 100% green before merge
