# FINTECH DATA INTELLIGENCE ECOSYSTEM
## Technical Specification Document (TSD) v3.0 — Production Blueprint

**Document Type:** Technical Specification / AI-Ready Development Blueprint  
**Target Audience:** AI Coding Assistants (Claude Code, Cursor, GitHub Copilot, Windsurf, etc.)  
**Complexity Level:** Senior Staff Data Engineer  
**Effort Estimado:** 3 Sprints / 6 semanas  
**Última Revisión:** 2026-04-24  
**Status:** ✅ APROBADO PARA DESARROLLO

---

## ⚡ INSTRUCCIONES PARA LA IA IMPLEMENTADORA

> **LEE ESTO PRIMERO ANTES DE ESCRIBIR UNA SOLA LÍNEA DE CÓDIGO.**

### Protocolo de Trabajo
1. **Lee el documento completo** antes de empezar cualquier sprint.
2. **Sigue el orden de sprints estrictamente.** Sprint 1 debe estar completamente funcional antes de tocar Sprint 2.
3. **Escribe tests antes o junto al código.** No se acepta código sin tests.
4. **Un commit por entregable (Deliverable ID).** Mensaje de commit: `feat(S1-D1): Docker Compose environment with healthchecks`.
5. **Valida cada componente con su criterio de aceptación** antes de marcarlo como hecho.
6. **Si un requerimiento es ambiguo, detente y pregunta.** No asumas.
7. **Nunca hardcodees endpoints, credenciales, ni rutas.** Todo va a `.env`.
8. **Cada función Python debe tener type hints y docstring (Google style).**

### Principios de Arquitectura
- **Medallion Architecture:** Bronze (raw) → Silver (cleaned) → Gold (modeled)
- **Clean Architecture:** Separación estricta entre capas
- **Infrastructure as Code:** Toda la infra reproducible via `make up`
- **Observability First:** Logging, métricas y data quality en cada capa
- **Security by Design:** PII hasheado, secretos en env vars, least-privilege

---

## 📋 RESUMEN EJECUTIVO

Construir una plataforma de inteligencia de datos **containerizada y production-grade** para una Fintech simulada. El sistema:

- **Ingiere** flujos de transacciones sintéticas de alta velocidad (10K txns/min)
- **Detecta** anomalías financieras en near-real-time (3 patrones de fraude)
- **Expone** KPIs agregados a través de un dashboard BI en Streamlit
- **Orquesta** todo mediante Apache Airflow con CeleryExecutor
- **Almacena** datos en arquitectura Medallion (S3/LocalStack + PostgreSQL)

Todo el stack arranca desde cero con un solo comando: `make up`

---

## 🎯 OBJETIVOS Y CRITERIOS DE ÉXITO

| ID | Objetivo | Métrica de Éxito | Cómo Verificar |
|----|----------|------------------|----------------|
| O1 | Ingestión de 10K+ txns/min con latencia <5s a Bronze | Throughput test pasa | `make test-perf` |
| O2 | Detectar 3 patrones de fraude con >95% precision | Benchmark en labeled test set | `make test-fraud` |
| O3 | Queries Gold <2s p95 para dashboard | p95 latency check | `make test-dash` |
| O4 | Infra 100% reproducible | Clone fresco → running en <10 min | `make up` en máquina limpia |
| O5 | Data quality gates bloquean datos malos en Silver | Zero revenue negativo en Gold | `dbt test` |

---

## 🏛️ ARQUITECTURA DEL SISTEMA

### Flujo de Datos de Alto Nivel

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

### Diagrama de Dependencias entre Servicios

```
postgres (healthcheck: pg_isready)
    └── airflow-webserver (depends_on: postgres, redis)
    └── airflow-scheduler (depends_on: postgres, redis)
    └── airflow-worker ×2 (depends_on: postgres, redis)

redis (healthcheck: redis-cli ping)
    └── airflow-worker (celery broker)

localstack (healthcheck: /health endpoint)
    └── (terraform init se ejecuta después del healthcheck)

spark-master (healthcheck: /json endpoint)
    └── spark-worker ×2

data-generator → localstack (S3 boto3)
airflow → spark-master (SparkSubmitOperator)
airflow → postgres (PostgresOperator, metadata)
streamlit → postgres (SQLAlchemy read-only)
prometheus → airflow-webserver, postgres-exporter, spark-master
grafana → prometheus
```

---

## 📁 ESTRUCTURA DE PROYECTO (COMPLETA)

```
fintech-analytics-engine/
│
├── Makefile                              # ← ENTRYPOINT ÚNICO
├── docker-compose.yml
├── docker-compose.override.yml           # dev overrides (volumes, ports extra)
├── .env.example
├── .env                                  # ← NO COMMITEAR (en .gitignore)
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
│   │   ├── fintech_master_dag.py         # DAG principal con TaskGroups
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
│   │   ├── spark_session.py              # Builder reutilizable
│   │   ├── s3_client.py                  # LocalStack-aware
│   │   ├── quality_checks.py             # Great Expectations wrappers
│   │   └── audit_logger.py               # JSON structured logging
│   └── tests/
│       ├── __init__.py
│       ├── conftest.py                   # Fixtures Spark + test data
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
│       │   └── users_snapshot.sql        # dbt snapshot para SCD2
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

## 🔧 TECH STACK

### Infraestructura Core

| Componente | Tecnología | Versión | Justificación |
|------------|-----------|---------|---------------|
| Containerización | Docker + Docker Compose | 25.x / 2.24.x | Universal, reproducible |
| Mock Cloud | LocalStack | 3.x | AWS-compatible, costo cero |
| IaC | Terraform | 1.7.x | Declarativo, state-managed |
| Orquestación | Apache Airflow | 2.9.x (CeleryExecutor) | Estándar industria, backfill nativo |
| Message Broker | Redis | 7.x | Celery backend + cache |
| Base de Datos | PostgreSQL | 16.x | DWH + metadata Airflow (dual-use) |

### Procesamiento

| Componente | Tecnología | Versión | Justificación |
|------------|-----------|---------|---------------|
| Motor distribuido | Apache Spark | 3.5.x | Escala a TBs, Parquet/S3 nativo |
| Modo Spark | Standalone (1 master + 2 workers) | — | Más simple que K8s para dev local |
| Lenguaje | Python | 3.11 | Type hints, ecosystem maduro |
| Data Quality | Great Expectations | 0.18.x | Expectations declarativas, integra con Airflow |

### Warehouse y Modelado

| Componente | Tecnología | Versión | Justificación |
|------------|-----------|---------|---------------|
| DWH Engine | PostgreSQL | 16.x | OLAP con índices, dbt-native |
| Transformaciones | dbt Core | 1.8.x | SQL versionado, tests, docs automáticos |
| Modelo de datos | Kimball Star Schema | — | Performance BI probado |

### Presentación

| Componente | Tecnología | Versión | Justificación |
|------------|-----------|---------|---------------|
| Dashboard | Streamlit | 1.35.x | Python-native BI, rápido |
| Visualización | Plotly | 5.x | Interactivo, web-native |

### Observabilidad

| Componente | Tecnología | Propósito |
|------------|-----------|-----------|
| Métricas | Prometheus + Grafana | Health de infra y pipelines |
| Logging | Python logging → JSON stdout | Centralizado y parseable |
| Data Quality | GE + dbt tests | Pipeline gating y alertas |

---

## 📊 CONTRATOS DE DATOS Y SCHEMAS

### Schema 0: Raw (S3 JSONL Input)

**Path:** `s3://fintech-raw-data/raw/{entity}/{year}/{month}/{day}/{hour}/{uuid}.jsonl`

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

### Schema 1: Bronze Layer (Parquet en S3)

**Path:** `s3://fintech-raw-data/bronze/transactions/year=YYYY/month=MM/day=DD/hour=HH/`  
**Formato:** Parquet con compresión Snappy  
**Propiedad:** Append-only, inmutable

| Columna | Tipo Spark | Nullable | Descripción |
|---------|-----------|----------|-------------|
| raw_data | StringType | NO | JSON original como string |
| ingestion_timestamp | TimestampType | NO | Cuando Spark escribió el registro |
| source_file | StringType | NO | Path S3 de origen |
| partition_date | DateType | NO | Derivado del event timestamp |
| bronze_batch_id | StringType | NO | UUID del run de ingesta |

### Schema 2: Silver Layer (Parquet en S3)

**Path:** `s3://fintech-raw-data/silver/transactions/date=YYYY-MM-DD/`  
**Formato:** Parquet con compresión Snappy  
**Calidad:** Quality-gated (GE suite debe pasar)

| Columna | Tipo Spark | Constraints |
|---------|-----------|-------------|
| transaction_id | StringType (UUID) | NOT NULL, UNIQUE por día |
| user_id | StringType (UUID) | NOT NULL |
| merchant_id | StringType (UUID) | NOT NULL |
| amount | DecimalType(18,2) | > 0.01 |
| currency | StringType | IN [USD, EUR, GBP, JPY, CAD, MXN] |
| timestamp | TimestampType | NOT NULL, NOT future, dentro 90 días |
| transaction_type | StringType | IN enum válido |
| status | StringType | IN enum válido |
| payment_method | StringType | IN enum válido |
| latitude | DecimalType(9,6) | -90 a 90 |
| longitude | DecimalType(9,6) | -180 a 180 |
| country_code | StringType | ISO 3166-1 alpha-2 válido |
| mcc_code | StringType | 4 dígitos |
| ip_address_hash | StringType | SHA-256 del IP original |
| ingestion_timestamp | TimestampType | Metadata |
| _silver_processed_at | TimestampType | Audit trail |

**Regla de deduplicación:**
```python
# Mantener último registro por transaction_id
ROW_NUMBER() OVER (
    PARTITION BY transaction_id 
    ORDER BY ingestion_timestamp DESC
) = 1
```

### Schema 3: Gold Layer (PostgreSQL)

#### Tabla: `gold.fact_transactions`

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

#### Tabla: `gold.dim_users` (SCD Type 2)

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

#### Tabla: `gold.dim_merchants`

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

#### Tabla: `gold.dim_time`

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
-- Pre-populate: 2020-01-01 a 2030-12-31
```

#### Tabla: `gold.alerts_log`

```sql
CREATE TABLE gold.alerts_log (
    alert_id         BIGSERIAL PRIMARY KEY,
    transaction_id   UUID         NOT NULL,
    alert_timestamp  TIMESTAMP    NOT NULL DEFAULT NOW(),
    reason_code      VARCHAR(50)  NOT NULL,
    -- Valores: VELOCITY_ATTACK, HIGH_AMOUNT, GEO_IMPOSSIBLE, ML_ANOMALY
    severity_score   DECIMAL(5,4) NOT NULL CHECK (severity_score BETWEEN 0 AND 1),
    description      TEXT,
    status           VARCHAR(40)  DEFAULT 'open',
    -- Valores: open, investigating, closed_false_positive, confirmed_fraud
    ml_score         DECIMAL(5,4),
    rule_score       DECIMAL(5,4),
    created_at       TIMESTAMP    DEFAULT NOW()
);

CREATE INDEX idx_alerts_txn ON gold.alerts_log(transaction_id);
CREATE INDEX idx_alerts_status ON gold.alerts_log(status) WHERE status = 'open';
CREATE INDEX idx_alerts_timestamp ON gold.alerts_log(alert_timestamp DESC);
```

---

## 🚀 PLAN DE DESARROLLO — SPRINTS DETALLADOS

---

## SPRINT 1: Fundación e Infraestructura (Semanas 1-2)

**Objetivo:** Ambiente completamente funcional con datos sintéticos fluyendo hasta Bronze layer.

### Entregables Sprint 1

| ID | Entregable | Criterio de Aceptación |
|----|------------|----------------------|
| S1-D1 | Docker Compose con todos los servicios | `make up` arranca todos los servicios; todos los healthchecks pasan en <5 min |
| S1-D2 | Terraform IaC en LocalStack | `make infra` crea bucket S3; LocalStack responde con 200 |
| S1-D3 | Data Generator (4 entidades) | Genera users/merchants/transactions/disputes; fraud injection funciona; escribe a S3 |
| S1-D4 | Bronze Ingestion Job (Spark) | Lee JSONL de S3 → escribe Parquet; schema enforced; partitioning correcto |
| S1-D5 | Airflow DAG skeleton | DAG carga sin errores; tasks visibles en UI; TaskGroups correctos |
| S1-D6 | Makefile completo | Targets: `up`, `down`, `test`, `logs`, `clean`, `infra`, `generate` |

---

### S1-TASK-01: Makefile y Proyecto Base

**Archivo:** `Makefile`

```makefile
.PHONY: up down test logs clean infra generate lint

# Levantar todo el stack
up:
	cp -n .env.example .env || true
	docker compose up -d --build
	@echo "⏳ Esperando healthchecks..."
	@sleep 15
	$(MAKE) infra

# Infraestructura (Terraform + LocalStack)
infra:
	docker compose exec localstack bash /scripts/init-localstack.sh

# Bajar todo (limpio)
down:
	docker compose down -v --remove-orphans

# Ejecutar todos los tests
test:
	docker compose run --rm processing pytest tests/ -v --tb=short
	docker compose exec airflow-scheduler python -m pytest /opt/airflow/dags/tests/ -v
	docker compose exec warehouse-dbt dbt test --target dev

# Tests de integración
test-integration:
	docker compose run --rm processing pytest tests/integration/ -v

# Generar datos sintéticos
generate:
	docker compose run --rm data-generator python main.py generate all

# Logs de todos los servicios
logs:
	docker compose logs -f --tail=100

# Limpiar todo (incluyendo imágenes)
clean:
	docker compose down -v --remove-orphans --rmi local
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true

# Lint
lint:
	docker compose run --rm processing flake8 jobs/ utils/ --max-line-length=100
	docker compose run --rm processing mypy jobs/ utils/ --ignore-missing-imports
```

---

### S1-TASK-02: Docker Compose

**Archivo:** `docker-compose.yml`

**Servicios requeridos y configuración:**

```yaml
version: '3.9'

x-airflow-common: &airflow-common
  image: apache/airflow:2.9.0-python3.11
  environment: &airflow-env
    AIRFLOW__CORE__EXECUTOR: CeleryExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/airflow
    AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/airflow
    AIRFLOW__CELERY__BROKER_URL: redis://redis:6379/0
    AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW_FERNET_KEY}
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'false'
    AIRFLOW__WEBSERVER__RBAC: 'true'
    AWS_ACCESS_KEY_ID: test
    AWS_SECRET_ACCESS_KEY: test
    AWS_DEFAULT_REGION: us-east-1
  volumes:
    - ./orchestration/dags:/opt/airflow/dags
    - ./orchestration/config/airflow.cfg:/opt/airflow/airflow.cfg
    - airflow-logs:/opt/airflow/logs
  depends_on:
    postgres:
      condition: service_healthy
    redis:
      condition: service_healthy

services:
  # ─── BASE DE DATOS ─────────────────────────────────────────────
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./warehouse/init-scripts:/docker-entrypoint-initdb.d
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}"]
      interval: 10s
      timeout: 5s
      retries: 5

  # ─── REDIS ─────────────────────────────────────────────────────
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 3s
      retries: 5

  # ─── LOCALSTACK (S3) ───────────────────────────────────────────
  localstack:
    image: localstack/localstack:3.0
    ports:
      - "4566:4566"
    environment:
      SERVICES: s3
      DEFAULT_REGION: us-east-1
      DATA_DIR: /var/lib/localstack
    volumes:
      - localstack-data:/var/lib/localstack
      - ./infrastructure/scripts:/scripts
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:4566/_localstack/health"]
      interval: 15s
      timeout: 5s
      retries: 10

  # ─── AIRFLOW ───────────────────────────────────────────────────
  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 5

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler

  airflow-worker:
    <<: *airflow-common
    command: celery worker
    deploy:
      replicas: 2

  airflow-init:
    <<: *airflow-common
    command: >
      bash -c "airflow db migrate &&
               airflow users create --username admin --password admin
               --firstname Admin --lastname User --role Admin --email admin@fintech.local"
    restart: "no"

  # ─── SPARK ─────────────────────────────────────────────────────
  spark-master:
    image: bitnami/spark:3.5
    environment:
      SPARK_MODE: master
      SPARK_MASTER_WEBUI_PORT: 8081
    ports:
      - "8081:8081"
      - "7077:7077"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/json"]
      interval: 20s
      timeout: 5s
      retries: 5

  spark-worker:
    image: bitnami/spark:3.5
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
      SPARK_WORKER_MEMORY: ${SPARK_EXECUTOR_MEMORY:-2G}
      SPARK_WORKER_CORES: ${SPARK_EXECUTOR_CORES:-2}
    depends_on:
      spark-master:
        condition: service_healthy
    deploy:
      replicas: 2

  # ─── DATA GENERATOR ────────────────────────────────────────────
  data-generator:
    build: ./data_generator
    environment:
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
      LOCALSTACK_ENDPOINT: http://localstack:4566
      S3_BUCKET: ${S3_BUCKET}
      GENERATOR_BATCH_SIZE: ${GENERATOR_BATCH_SIZE:-1000}
      GENERATOR_FRAUD_RATE: ${GENERATOR_FRAUD_RATE:-0.02}
    depends_on:
      localstack:
        condition: service_healthy
    profiles:
      - generate

  # ─── PROCESSING (SPARK JOBS) ───────────────────────────────────
  processing:
    build: ./processing
    environment:
      SPARK_MASTER_URL: spark://spark-master:7077
      LOCALSTACK_ENDPOINT: http://localstack:4566
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
      POSTGRES_HOST: postgres
      POSTGRES_PORT: 5432
      POSTGRES_DB: ${POSTGRES_DB}
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    depends_on:
      - spark-master
      - postgres
      - localstack
    profiles:
      - processing

  # ─── STREAMLIT DASHBOARD ───────────────────────────────────────
  streamlit:
    build: ./analytics
    ports:
      - "8501:8501"
    environment:
      STREAMLIT_DB_CONNECTION: postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}
    depends_on:
      postgres:
        condition: service_healthy

  # ─── OBSERVABILIDAD ────────────────────────────────────────────
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.retention.time=7d'

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_PASSWORD:-admin}
      GF_USERS_ALLOW_SIGN_UP: 'false'
    volumes:
      - grafana-data:/var/lib/grafana
      - ./monitoring/grafana/provisioning:/etc/grafana/provisioning
      - ./monitoring/grafana/dashboards:/var/lib/grafana/dashboards
    depends_on:
      - prometheus

  # ─── JUPYTER ───────────────────────────────────────────────────
  jupyter:
    image: jupyter/pyspark-notebook:spark-3.5.0
    ports:
      - "8888:8888"
    environment:
      JUPYTER_ENABLE_LAB: "yes"
    volumes:
      - ./notebooks:/home/jovyan/work
    profiles:
      - dev

volumes:
  postgres-data:
  localstack-data:
  airflow-logs:
  grafana-data:
```

---

### S1-TASK-03: Terraform — Módulo S3

**Archivo:** `infrastructure/terraform/modules/s3/main.tf`

```hcl
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

resource "aws_s3_bucket" "fintech_raw_data" {
  bucket = var.bucket_name

  tags = {
    Environment = var.environment
    Project     = "fintech-analytics"
    ManagedBy   = "terraform"
  }
}

# Estructura de "carpetas" (prefijos)
resource "aws_s3_object" "folders" {
  for_each = toset(["raw/", "bronze/", "silver/", "gold/"])

  bucket = aws_s3_bucket.fintech_raw_data.id
  key    = each.value
  source = "/dev/null"
}

# Lifecycle: expirar raw/ después de 30 días
resource "aws_s3_bucket_lifecycle_configuration" "lifecycle" {
  bucket = aws_s3_bucket.fintech_raw_data.id

  rule {
    id     = "expire-raw-data"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    expiration {
      days = var.raw_retention_days
    }
  }

  rule {
    id     = "expire-bronze-data"
    status = "Enabled"

    filter {
      prefix = "bronze/"
    }

    expiration {
      days = var.bronze_retention_days
    }
  }
}

# Server-side encryption (simulado en LocalStack)
resource "aws_s3_bucket_server_side_encryption_configuration" "sse" {
  bucket = aws_s3_bucket.fintech_raw_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
```

**Archivo:** `infrastructure/terraform/modules/s3/variables.tf`

```hcl
variable "bucket_name"          { default = "fintech-raw-data" }
variable "environment"          { default = "local" }
variable "raw_retention_days"   { default = 30 }
variable "bronze_retention_days" { default = 90 }
```

**Archivo:** `infrastructure/terraform/main.tf`

```hcl
terraform {
  required_version = ">= 1.7"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    s3 = "http://localstack:4566"
  }
}

module "s3" {
  source = "./modules/s3"
}

module "iam" {
  source = "./modules/iam"
}
```

**Archivo:** `infrastructure/scripts/init-localstack.sh`

```bash
#!/bin/bash
set -e

echo "⏳ Esperando LocalStack..."
until curl -s http://localstack:4566/_localstack/health | grep -q '"s3": "available"'; do
  sleep 3
done

echo "✅ LocalStack listo. Aplicando Terraform..."
cd /infrastructure/terraform
terraform init -upgrade
terraform apply -auto-approve

echo "✅ Infraestructura lista. Bucket creado: $(terraform output -raw bucket_name)"
```

---

### S1-TASK-04: Data Generator

**Archivo:** `data_generator/config/generator_config.yaml`

```yaml
volumes:
  users: 10000
  merchants: 5000
  transactions_per_hour: 10000
  disputes_rate: 0.005

fraud:
  injection_rate: 0.02
  patterns:
    velocity_attack:
      enabled: true
      min_txns: 10
      max_txns: 15
      window_seconds: 60
      affected_users_pct: 0.001
    high_amount:
      enabled: true
      multiplier_min: 5
      multiplier_max: 10
    geographic_impossible:
      enabled: true
      min_distance_km: 1000
      max_time_hours: 1

output:
  format: jsonl
  batch_size: 1000
  s3_bucket: fintech-raw-data
  success_marker: _SUCCESS

currencies: [USD, EUR, GBP, JPY, CAD, MXN]
transaction_types: [purchase, refund, transfer, withdrawal]
statuses: [completed, pending, failed, disputed]
payment_methods: [card, bank_transfer, wallet, crypto]
```

**Archivo:** `data_generator/models/transaction.py`

```python
"""Transaction model using Pydantic for validation."""

from __future__ import annotations
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, validator


class Transaction(BaseModel):
    """Represents a financial transaction."""

    transaction_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    merchant_id: str
    amount: Decimal = Field(gt=0, decimal_places=2)
    currency: str = Field(min_length=3, max_length=3)
    timestamp: datetime
    transaction_type: str
    status: str
    payment_method: str
    device_id: str = Field(default_factory=lambda: f"dev-{uuid.uuid4().hex[:8]}")
    ip_address: str
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    country_code: str = Field(min_length=2, max_length=2)
    mcc_code: str = Field(min_length=4, max_length=4)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    is_fraud: bool = Field(default=False, exclude=True)  # Internal flag, NOT exported
    fraud_pattern: Optional[str] = Field(default=None, exclude=True)

    def to_jsonl_dict(self) -> Dict[str, Any]:
        """Return dict suitable for JSONL export (excludes internal fields)."""
        return self.dict(exclude={"is_fraud", "fraud_pattern"})
```

**Archivo:** `data_generator/fraud_patterns/base.py`

```python
"""Abstract base class for fraud pattern injectors."""

from abc import ABC, abstractmethod
from typing import List
from data_generator.models.transaction import Transaction


class FraudPattern(ABC):
    """Base class for all fraud pattern implementations."""

    @abstractmethod
    def inject(self, transactions: List[Transaction]) -> List[Transaction]:
        """
        Inject fraud pattern into a list of transactions.
        
        Args:
            transactions: List of clean transactions to potentially modify.
            
        Returns:
            Modified list with fraud pattern injected.
        """
        pass

    @property
    @abstractmethod
    def pattern_name(self) -> str:
        """Human-readable name of this fraud pattern."""
        pass
```

**Archivo:** `data_generator/fraud_patterns/velocity_attack.py`

```python
"""Velocity attack fraud pattern: burst of transactions in short window."""

import random
from datetime import timedelta
from typing import List
from data_generator.fraud_patterns.base import FraudPattern
from data_generator.models.transaction import Transaction


class VelocityAttack(FraudPattern):
    """Injects rapid burst of transactions from same user in 60s window."""

    def __init__(self, min_txns: int = 10, max_txns: int = 15, window_seconds: int = 60):
        self.min_txns = min_txns
        self.max_txns = max_txns
        self.window_seconds = window_seconds

    @property
    def pattern_name(self) -> str:
        return "VELOCITY_ATTACK"

    def inject(self, transactions: List[Transaction]) -> List[Transaction]:
        if not transactions:
            return transactions

        # Pick a random victim user
        victim = random.choice(transactions)
        burst_count = random.randint(self.min_txns, self.max_txns)
        base_time = victim.timestamp

        burst = []
        for i in range(burst_count):
            txn = victim.copy()
            txn.transaction_id = __import__('uuid').uuid4().__str__()
            txn.timestamp = base_time + timedelta(seconds=random.randint(0, self.window_seconds))
            txn.is_fraud = True
            txn.fraud_pattern = self.pattern_name
            burst.append(txn)

        return transactions + burst
```

**Archivo:** `data_generator/main.py`

```python
"""Main CLI entrypoint for the data generator."""

import argparse
import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import boto3
import yaml
from faker import Faker
from botocore.config import Config

from data_generator.models.transaction import Transaction
from data_generator.models.user import User
from data_generator.models.merchant import Merchant
from data_generator.fraud_patterns.velocity_attack import VelocityAttack
from data_generator.fraud_patterns.high_amount import HighAmount
from data_generator.fraud_patterns.geographic_impossible import GeographicImpossible

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

fake = Faker()


def get_s3_client(endpoint_url: str) -> boto3.client:
    """Create S3 client configured for LocalStack."""
    return boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1',
        config=Config(signature_version='s3v4')
    )


def upload_to_s3(s3_client, bucket: str, data: List[dict], entity: str) -> str:
    """Upload JSONL data to S3 with proper path structure."""
    now = datetime.now(timezone.utc)
    key = f"raw/{entity}/{now.year}/{now.month:02d}/{now.day:02d}/{now.hour:02d}/{uuid.uuid4()}.jsonl"
    
    body = '\n'.join(json.dumps(record) for record in data)
    s3_client.put_object(Bucket=bucket, Key=key, Body=body)
    
    # Upload _SUCCESS marker
    success_key = f"raw/{entity}/{now.year}/{now.month:02d}/{now.day:02d}/{now.hour:02d}/_SUCCESS"
    s3_client.put_object(Bucket=bucket, Key=success_key, Body='')
    
    logger.info(f"✅ Uploaded {len(data)} {entity} records to s3://{bucket}/{key}")
    return key


def main():
    parser = argparse.ArgumentParser(description='Fintech Data Generator')
    parser.add_argument('command', choices=['generate'])
    parser.add_argument('entity', choices=['users', 'merchants', 'transactions', 'all'])
    parser.add_argument('--config', default='config/generator_config.yaml')
    args = parser.parse_args()

    # Load config
    with open(args.config) as f:
        config = yaml.safe_load(f)

    import os
    s3_client = get_s3_client(os.environ['LOCALSTACK_ENDPOINT'])
    bucket = os.environ['S3_BUCKET']

    if args.entity in ('users', 'all'):
        users = generate_users(config['volumes']['users'])
        upload_to_s3(s3_client, bucket, [u.dict() for u in users], 'users')

    if args.entity in ('merchants', 'all'):
        merchants = generate_merchants(config['volumes']['merchants'])
        upload_to_s3(s3_client, bucket, [m.dict() for m in merchants], 'merchants')

    if args.entity in ('transactions', 'all'):
        transactions = generate_transactions(config)
        upload_to_s3(s3_client, bucket, [t.to_jsonl_dict() for t in transactions], 'transactions')


if __name__ == '__main__':
    main()
```

---

### S1-TASK-05: Spark Bronze Job

**Archivo:** `processing/utils/spark_session.py`

```python
"""Reusable Spark session builder with LocalStack S3 configuration."""

from __future__ import annotations
import os
from pyspark.sql import SparkSession


def get_spark_session(app_name: str, mode: str = "local[*]") -> SparkSession:
    """
    Create or retrieve a configured SparkSession.
    
    Args:
        app_name: Name of the Spark application.
        mode: Spark master URL or 'local[*]' for tests.
        
    Returns:
        Configured SparkSession instance.
    """
    localstack_endpoint = os.environ.get("LOCALSTACK_ENDPOINT", "http://localstack:4566")
    master_url = os.environ.get("SPARK_MASTER_URL", mode)

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master_url)
        .config("spark.hadoop.fs.s3a.endpoint", localstack_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.261,"
                "org.postgresql:postgresql:42.6.0")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark
```

**Archivo:** `processing/schemas/bronze_schema.py`

```python
"""Schema definitions for Bronze layer."""

from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DateType
)

BRONZE_SCHEMA = StructType([
    StructField("raw_data",             StringType(),    False),
    StructField("ingestion_timestamp",  TimestampType(), False),
    StructField("source_file",          StringType(),    False),
    StructField("partition_date",       DateType(),      False),
    StructField("bronze_batch_id",      StringType(),    False),
])
```

**Archivo:** `processing/jobs/bronze_ingest.py`

```python
"""
Bronze Ingestion Job.

Reads raw JSONL files from S3 and writes them as Parquet to the Bronze layer.
Adds metadata columns: ingestion_timestamp, source_file, partition_date, bronze_batch_id.

Usage:
    spark-submit jobs/bronze_ingest.py --date 2026-04-24 --hour 15
"""

from __future__ import annotations
import argparse
import uuid
import logging
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from processing.utils.spark_session import get_spark_session
from processing.utils.audit_logger import AuditLogger

logger = logging.getLogger(__name__)


def run_bronze_ingest(date: str, hour: str, s3_bucket: str) -> dict:
    """
    Execute Bronze ingestion for a given date partition.
    
    Args:
        date: Processing date in YYYY-MM-DD format.
        hour: Processing hour in HH format (00-23).
        s3_bucket: S3 bucket name.
        
    Returns:
        dict with run metrics: input_rows, output_rows, duration_seconds.
    """
    audit = AuditLogger("bronze_ingest")
    start_time = datetime.now(timezone.utc)
    batch_id = str(uuid.uuid4())

    spark = get_spark_session("BronzeIngestion")

    year, month, day = date.split('-')
    input_path = f"s3a://{s3_bucket}/raw/transactions/{year}/{month}/{day}/{hour}/"
    output_path = f"s3a://{s3_bucket}/bronze/transactions/"

    logger.info(f"[{batch_id}] Reading from: {input_path}")

    # Read raw JSONL
    raw_df = (
        spark.read
        .option("multiline", "false")
        .text(input_path)
        .select(
            F.col("value").alias("raw_data"),
            F.input_file_name().alias("source_file"),
        )
    )

    input_rows = raw_df.count()
    logger.info(f"[{batch_id}] Input rows: {input_rows}")

    if input_rows == 0:
        logger.warning(f"[{batch_id}] No data found at {input_path}. Exiting.")
        return {"input_rows": 0, "output_rows": 0, "batch_id": batch_id}

    # Add metadata columns
    bronze_df = (
        raw_df
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("partition_date", F.lit(date).cast("date"))
        .withColumn("bronze_batch_id", F.lit(batch_id))
    )

    # Write Parquet, partitioned by year/month/day/hour
    (
        bronze_df.write
        .mode("append")
        .partitionBy("partition_date")
        .parquet(output_path)
    )

    output_rows = bronze_df.count()
    duration = (datetime.now(timezone.utc) - start_time).total_seconds()

    audit.log(
        run_id=batch_id,
        input_rows=input_rows,
        output_rows=output_rows,
        duration_seconds=duration,
    )

    logger.info(f"[{batch_id}] Bronze complete. Output rows: {output_rows}. Duration: {duration:.1f}s")
    spark.stop()

    return {"input_rows": input_rows, "output_rows": output_rows, "batch_id": batch_id}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--hour", required=True, help="HH")
    parser.add_argument("--bucket", default="fintech-raw-data")
    args = parser.parse_args()

    run_bronze_ingest(args.date, args.hour, args.bucket)
```

---

### S1-TASK-06: Airflow DAG Skeleton

**Archivo:** `orchestration/dags/fintech_master_dag.py`

```python
"""
Fintech Master Pipeline DAG.

Schedule: Hourly
Orchestrates: Data Generation → Bronze → Silver → Gold → dbt → GE → Anomaly Detection

Author: data-engineering
SLA: 45 minutes from DAG start
"""

from __future__ import annotations
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.utils.task_group import TaskGroup

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

BUCKET = os.environ.get("S3_BUCKET", "fintech-raw-data")
SPARK_CONF = {
    "spark.executor.memory": os.environ.get("SPARK_EXECUTOR_MEMORY", "2g"),
    "spark.executor.cores": os.environ.get("SPARK_EXECUTOR_CORES", "2"),
    "spark.hadoop.fs.s3a.endpoint": os.environ.get("LOCALSTACK_ENDPOINT", "http://localstack:4566"),
    "spark.hadoop.fs.s3a.access.key": "test",
    "spark.hadoop.fs.s3a.secret.key": "test",
    "spark.hadoop.fs.s3a.path.style.access": "true",
}


with DAG(
    dag_id="fintech_master_pipeline",
    default_args=DEFAULT_ARGS,
    description="Fintech ETL: Raw → Bronze → Silver → Gold → Anomaly Detection",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["fintech", "etl", "critical"],
    sla_miss_callback=None,  # TODO Sprint 3: Implement Slack callback
) as dag:

    # ── TASK GROUP 1: Data Generation ──────────────────────────────
    with TaskGroup("data_generation", tooltip="Generate synthetic transaction data") as tg_gen:
        generate_all = BashOperator(
            task_id="generate_transactions",
            bash_command=(
                "docker run --rm "
                "--env-file /opt/airflow/.env "
                "data-generator python main.py generate all"
            ),
            execution_timeout=timedelta(minutes=10),
        )

    # ── TASK GROUP 2: Bronze Ingestion ─────────────────────────────
    with TaskGroup("ingestion", tooltip="S3 sensor + Spark Bronze job") as tg_ingest:
        s3_sensor = S3KeySensor(
            task_id="wait_for_s3_data",
            bucket_name=BUCKET,
            bucket_key="raw/transactions/{{ execution_date.strftime('%Y/%m/%d/%H') }}/_SUCCESS",
            aws_conn_id="aws_localstack",
            timeout=600,
            poke_interval=30,
        )

        bronze_job = SparkSubmitOperator(
            task_id="spark_bronze_job",
            application="/opt/spark/jobs/bronze_ingest.py",
            application_args=[
                "--date", "{{ ds }}",
                "--hour", "{{ execution_date.strftime('%H') }}",
                "--bucket", BUCKET,
            ],
            conf=SPARK_CONF,
            conn_id="spark_default",
            execution_timeout=timedelta(minutes=15),
        )

        s3_sensor >> bronze_job

    # ── TASK GROUP 3: Transformation ───────────────────────────────
    with TaskGroup("transformation", tooltip="Silver cleanse + Gold modeling") as tg_transform:
        silver_job = SparkSubmitOperator(
            task_id="spark_silver_job",
            application="/opt/spark/jobs/silver_cleanse.py",
            application_args=["--date", "{{ ds }}"],
            conf=SPARK_CONF,
            conn_id="spark_default",
            execution_timeout=timedelta(minutes=20),
        )

        gold_job = SparkSubmitOperator(
            task_id="spark_gold_job",
            application="/opt/spark/jobs/gold_modeling.py",
            application_args=["--date", "{{ ds }}"],
            conf=SPARK_CONF,
            conn_id="spark_default",
            execution_timeout=timedelta(minutes=15),
        )

        silver_job >> gold_job

    # ── TASK GROUP 4: Quality & Modeling ───────────────────────────
    with TaskGroup("quality_and_modeling", tooltip="dbt + Great Expectations") as tg_quality:
        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command="cd /opt/dbt && dbt run --target prod --no-write-json",
            execution_timeout=timedelta(minutes=10),
        )

        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command="cd /opt/dbt && dbt test --target prod",
            execution_timeout=timedelta(minutes=10),
        )

        gx_validate = PythonOperator(
            task_id="great_expectations_validate",
            python_callable=lambda: __import__(
                "processing.utils.quality_checks", fromlist=["run_silver_suite"]
            ).run_silver_suite(),
            execution_timeout=timedelta(minutes=10),
        )

        dbt_run >> [dbt_test, gx_validate]

    # ── TASK GROUP 5: Anomaly Detection ────────────────────────────
    with TaskGroup("anomaly_detection", tooltip="Fraud detection engine") as tg_anomaly:
        detect_fraud = SparkSubmitOperator(
            task_id="spark_anomaly_detection",
            application="/opt/spark/jobs/anomaly_detection.py",
            application_args=["--date", "{{ ds }}"],
            conf=SPARK_CONF,
            conn_id="spark_default",
            execution_timeout=timedelta(minutes=15),
        )

        update_fraud_flags = PostgresOperator(
            task_id="update_fraud_flags",
            postgres_conn_id="postgres_fintech",
            sql="""
                UPDATE gold.fact_transactions ft
                SET is_flagged_fraud = TRUE,
                    fraud_score = al.severity_score
                FROM gold.alerts_log al
                WHERE ft.transaction_id = al.transaction_id
                  AND al.created_at::date = '{{ ds }}'::date;
            """,
        )

        detect_fraud >> update_fraud_flags

    # ── PIPELINE DEPENDENCIES ──────────────────────────────────────
    tg_gen >> tg_ingest >> tg_transform >> tg_quality >> tg_anomaly
```

---

## SPRINT 2: Transformación, Calidad y Warehouse (Semanas 3-4)

**Objetivo:** Pipeline completo Bronze → Silver → Gold con quality gates y dbt models.

### Entregables Sprint 2

| ID | Entregable | Criterio de Aceptación |
|----|------------|----------------------|
| S2-D1 | Silver Cleansing Job (Spark) | Dedup, casting, null filtering; GE suite pasa; records malos rechazados |
| S2-D2 | Gold Modeling Job (Spark) | Star schema en Postgres; integridad referencial validada |
| S2-D3 | dbt Project completo | Todos los mart models corren; `dbt test` 100% verde |
| S2-D4 | SCD Type 2 en dim_users | Cambios históricos trackeados correctamente |
| S2-D5 | Data Quality Gates | GE + dbt tests bloquean datos malos; pipeline se detiene |
| S2-D6 | DAG end-to-end funcional | DAG corre sin intervención manual; backfill de 7 días OK |

---

### S2-TASK-01: Silver Cleansing Job

**Archivo:** `processing/jobs/silver_cleanse.py`

```python
"""
Silver Cleansing Job.

Transforms Bronze Parquet → Silver Parquet with:
- JSON parsing of raw_data column
- Type casting and validation
- Deduplication by transaction_id
- PII hashing (ip_address → ip_address_hash)
- Data quality enforcement

Usage:
    spark-submit jobs/silver_cleanse.py --date 2026-04-24
"""

from __future__ import annotations
import argparse
import hashlib
import logging
from datetime import datetime, timezone

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DecimalType, TimestampType

from processing.utils.spark_session import get_spark_session
from processing.utils.audit_logger import AuditLogger
from processing.schemas.silver_schema import SILVER_SCHEMA

logger = logging.getLogger(__name__)

VALID_CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD", "MXN", "BRL"]
VALID_TXN_TYPES = ["purchase", "refund", "transfer", "withdrawal"]
VALID_STATUSES = ["completed", "pending", "failed", "disputed"]
VALID_METHODS = ["card", "bank_transfer", "wallet", "crypto"]


def parse_raw_json(df: DataFrame) -> DataFrame:
    """Parse raw_data JSON string into typed columns."""
    from pyspark.sql.functions import from_json, col
    from processing.schemas.bronze_schema import RAW_TRANSACTION_SCHEMA  # inline StructType

    return df.select(
        from_json(col("raw_data"), RAW_TRANSACTION_SCHEMA).alias("data"),
        col("ingestion_timestamp"),
        col("source_file"),
        col("partition_date"),
        col("bronze_batch_id"),
    ).select("data.*", "ingestion_timestamp", "source_file", "partition_date")


def cast_and_validate(df: DataFrame) -> DataFrame:
    """Apply type casts and drop invalid rows."""
    hash_ip_udf = F.udf(
        lambda ip: hashlib.sha256(ip.encode()).hexdigest() if ip else None
    )

    df = (
        df
        # Type casts
        .withColumn("amount", F.col("amount").cast(DecimalType(18, 2)))
        .withColumn("timestamp", F.col("timestamp").cast(TimestampType()))
        .withColumn("latitude", F.col("latitude").cast(DecimalType(9, 6)))
        .withColumn("longitude", F.col("longitude").cast(DecimalType(9, 6)))
        # PII hashing
        .withColumn("ip_address_hash", hash_ip_udf(F.col("ip_address")))
        .drop("ip_address")
        # Audit column
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

    # Filter invalid rows
    df = df.filter(
        F.col("transaction_id").isNotNull() &
        F.col("user_id").isNotNull() &
        F.col("merchant_id").isNotNull() &
        F.col("amount").isNotNull() &
        (F.col("amount") > 0) &
        F.col("timestamp").isNotNull() &
        (F.col("timestamp") <= F.current_timestamp()) &  # no future dates
        F.col("currency").isin(VALID_CURRENCIES) &
        F.col("transaction_type").isin(VALID_TXN_TYPES) &
        F.col("status").isin(VALID_STATUSES) &
        F.col("payment_method").isin(VALID_METHODS) &
        F.col("latitude").between(-90, 90) &
        F.col("longitude").between(-180, 180)
    )

    return df


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep latest record per transaction_id."""
    from pyspark.sql.window import Window

    window = Window.partitionBy("transaction_id").orderBy(F.col("ingestion_timestamp").desc())
    return (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def run_silver_cleanse(date: str, s3_bucket: str) -> dict:
    """Execute Silver cleansing pipeline for a given date."""
    audit = AuditLogger("silver_cleanse")
    start_time = datetime.now(timezone.utc)

    spark = get_spark_session("SilverCleanse")

    input_path = f"s3a://{s3_bucket}/bronze/transactions/partition_date={date}/"
    output_path = f"s3a://{s3_bucket}/silver/transactions/"

    raw_df = spark.read.parquet(input_path)
    input_rows = raw_df.count()
    logger.info(f"Silver input rows: {input_rows}")

    parsed_df = parse_raw_json(raw_df)
    validated_df = cast_and_validate(parsed_df)
    deduped_df = deduplicate(validated_df)

    (
        deduped_df.write
        .mode("overwrite")
        .partitionBy("partition_date")
        .parquet(output_path)
    )

    output_rows = deduped_df.count()
    rejected_rows = input_rows - output_rows
    duration = (datetime.now(timezone.utc) - start_time).total_seconds()

    logger.info(f"Silver complete. Input: {input_rows}, Output: {output_rows}, Rejected: {rejected_rows}")
    audit.log(
        run_id=f"silver-{date}",
        input_rows=input_rows,
        output_rows=output_rows,
        rejected_rows=rejected_rows,
        duration_seconds=duration,
    )

    spark.stop()
    return {"input_rows": input_rows, "output_rows": output_rows, "rejected_rows": rejected_rows}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--bucket", default="fintech-raw-data")
    args = parser.parse_args()
    run_silver_cleanse(args.date, args.bucket)
```

---

### S2-TASK-02: Gold Modeling Job

**Archivo:** `processing/jobs/gold_modeling.py`

```python
"""
Gold Modeling Job.

Transforms Silver Parquet → PostgreSQL Gold layer (Star Schema):
- fact_transactions
- dim_users (with SCD Type 2 merge logic)
- dim_merchants
- dim_time (pre-populated calendar)

Usage:
    spark-submit jobs/gold_modeling.py --date 2026-04-24
"""

from __future__ import annotations
import argparse
import os
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame, functions as F

from processing.utils.spark_session import get_spark_session
from processing.utils.audit_logger import AuditLogger

logger = logging.getLogger(__name__)


def get_jdbc_url() -> str:
    """Build PostgreSQL JDBC URL from environment variables."""
    host = os.environ["POSTGRES_HOST"]
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ["POSTGRES_DB"]
    return f"jdbc:postgresql://{host}:{port}/{db}"


def get_jdbc_props() -> dict:
    """PostgreSQL JDBC connection properties."""
    return {
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver",
        "batchsize": "1000",
    }


def upsert_dimension(spark: SparkSession, df: DataFrame, table: str, natural_key: str):
    """Write dimension table with INSERT ON CONFLICT DO NOTHING."""
    jdbc_url = get_jdbc_url()
    props = get_jdbc_props()

    # Spark JDBC doesn't support upsert natively; use a staging approach
    staging_table = f"staging.{table.split('.')[-1]}_staging"

    # Write to staging
    df.write.jdbc(url=jdbc_url, table=staging_table, mode="overwrite", properties=props)

    # Merge from staging
    # This SQL is executed via a separate JDBC connection call (see PostgresOperator in DAG)
    logger.info(f"Staged {df.count()} rows to {staging_table}")


def run_gold_modeling(date: str, s3_bucket: str) -> dict:
    """Execute Gold modeling pipeline for a given date."""
    start_time = datetime.now(timezone.utc)
    spark = get_spark_session("GoldModeling")

    silver_path = f"s3a://{s3_bucket}/silver/transactions/partition_date={date}/"
    silver_df = spark.read.parquet(silver_path)
    silver_df.createOrReplaceTempView("silver_transactions")

    jdbc_url = get_jdbc_url()
    props = get_jdbc_props()

    # ── Dim Time ─────────────────────────────────────────────────
    # Pre-populate once; skip if already exists
    # (Full calendar generation delegated to dbt seed or init script)

    # ── Dim Merchants ─────────────────────────────────────────────
    merchants_df = silver_df.select(
        "merchant_id", "mcc_code", "country_code"
    ).distinct()

    merchants_df.write.jdbc(
        url=jdbc_url,
        table="staging.dim_merchants_staging",
        mode="overwrite",
        properties=props,
    )

    # ── Dim Users (SCD2 delegated to dbt snapshot) ────────────────
    users_df = silver_df.select("user_id", "country_code").distinct()
    users_df.write.jdbc(
        url=jdbc_url,
        table="staging.dim_users_staging",
        mode="overwrite",
        properties=props,
    )

    # ── Fact Transactions ─────────────────────────────────────────
    fact_df = spark.sql("""
        SELECT
            transaction_id,
            user_id,
            merchant_id,
            CAST(REPLACE(transaction_date, '-', '') AS INT) AS time_sk,
            amount,
            currency,
            transaction_type,
            status,
            payment_method,
            FALSE AS is_flagged_fraud,
            0.0 AS fraud_score,
            latitude,
            longitude,
            country_code
        FROM silver_transactions
    """)

    fact_df.write.jdbc(
        url=jdbc_url,
        table="staging.fact_transactions_staging",
        mode="overwrite",
        properties=props,
    )

    output_rows = fact_df.count()
    duration = (datetime.now(timezone.utc) - start_time).total_seconds()

    logger.info(f"Gold modeling complete. Rows: {output_rows}. Duration: {duration:.1f}s")
    spark.stop()
    return {"output_rows": output_rows}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--bucket", default="fintech-raw-data")
    args = parser.parse_args()
    run_gold_modeling(args.date, args.bucket)
```

---

### S2-TASK-03: dbt Project

**Archivo:** `warehouse/dbt/dbt_project.yml`

```yaml
name: 'fintech_analytics'
version: '1.0.0'
config-version: 2

profile: 'fintech_analytics'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  fintech_analytics:
    staging:
      +materialized: view
      +schema: staging
    marts:
      core:
        +materialized: table
        +schema: gold
      risk:
        +materialized: table
        +schema: gold

snapshots:
  fintech_analytics:
    +target_schema: gold
    +strategy: check
    +check_cols: all
```

**Archivo:** `warehouse/dbt/models/marts/core/fct_transactions.sql`

```sql
-- fct_transactions.sql
-- Final fact table joining staging data with dimension surrogate keys

{{
  config(
    materialized='incremental',
    unique_key='transaction_id',
    on_schema_change='fail'
  )
}}

WITH source AS (
    SELECT * FROM {{ ref('stg_transactions') }}
    {% if is_incremental() %}
    WHERE _silver_processed_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
),

with_user_sk AS (
    SELECT
        s.*,
        u.user_sk
    FROM source s
    LEFT JOIN {{ ref('dim_users') }} u
        ON s.user_id = u.user_id
        AND u.is_current = TRUE
),

with_merchant_sk AS (
    SELECT
        u.*,
        m.merchant_sk
    FROM with_user_sk u
    LEFT JOIN {{ ref('dim_merchants') }} m
        ON u.merchant_id = m.merchant_id
),

with_time_sk AS (
    SELECT
        m.*,
        CAST(TO_CHAR(m.timestamp::DATE, 'YYYYMMDD') AS INT) AS time_sk
    FROM with_merchant_sk m
)

SELECT
    transaction_id,
    user_sk,
    merchant_sk,
    time_sk,
    amount,
    currency,
    transaction_type,
    status,
    payment_method,
    COALESCE(is_flagged_fraud, FALSE) AS is_flagged_fraud,
    COALESCE(fraud_score, 0.0) AS fraud_score,
    latitude,
    longitude,
    country_code,
    NOW() AS created_at
FROM with_time_sk
WHERE user_sk IS NOT NULL
  AND merchant_sk IS NOT NULL
```

**Archivo:** `warehouse/dbt/snapshots/users_snapshot.sql`

```sql
-- SCD Type 2 snapshot for dim_users
{% snapshot users_snapshot %}

{{
    config(
      target_schema='gold',
      unique_key='user_id',
      strategy='check',
      check_cols=['risk_tier', 'country_code', 'email_hash'],
      invalidate_hard_deletes=True
    )
}}

SELECT
    user_id,
    SHA256(email::bytea)::TEXT AS email_hash,
    registration_date,
    COALESCE(risk_tier, 'low') AS risk_tier,
    country_code,
    NOW() AS dbt_updated_at
FROM staging.dim_users_staging

{% endsnapshot %}
```

**Archivo:** `warehouse/dbt/tests/assert_positive_revenue.sql`

```sql
-- Falla si hay transacciones con amount negativo en Gold
SELECT *
FROM {{ ref('fct_transactions') }}
WHERE amount < 0
```

**Archivo:** `warehouse/dbt/tests/assert_user_uniqueness.sql`

```sql
-- Falla si hay user_id duplicados como current en dim_users
SELECT user_id, COUNT(*) AS cnt
FROM {{ ref('dim_users') }}
WHERE is_current = TRUE
GROUP BY user_id
HAVING COUNT(*) > 1
```

---

## SPRINT 3: Detección de Anomalías, BI y Hardening (Semanas 5-6)

**Objetivo:** Motor de fraude operativo, dashboard Streamlit, monitoring y documentación completos.

### Entregables Sprint 3

| ID | Entregable | Criterio de Aceptación |
|----|------------|----------------------|
| S3-D1 | Anomaly Detection Engine | 3 tipos de reglas; alerts_log poblado; >95% precision en test set |
| S3-D2 | ML Scoring (Stretch) | Isolation Forest entrenado; scores en alerts_log |
| S3-D3 | Streamlit Dashboard | 3 páginas funcionales; KPIs coinciden con SQL |
| S3-D4 | Monitoring Stack | Grafana muestra pipeline health; Prometheus scraping activo |
| S3-D5 | Security Hardening | Sin credenciales en código; RBAC en Airflow; PII hasheado |
| S3-D6 | Documentación Final | README completo; RUNBOOK; todos los `make` commands documentados |

---

### S3-TASK-01: Anomaly Detection Engine

**Archivo:** `processing/jobs/anomaly_detection.py`

```python
"""
Anomaly Detection Job.

Implements 3 fraud detection rules on Silver data:
1. VELOCITY_ATTACK: >=10 transactions from same user in 60 seconds
2. HIGH_AMOUNT: >500% of user's 30-day average
3. GEO_IMPOSSIBLE: Physically impossible travel (>800 km/h)

Outputs to gold.alerts_log and updates fact_transactions.is_flagged_fraud.

Usage:
    spark-submit jobs/anomaly_detection.py --date 2026-04-24
"""

from __future__ import annotations
import argparse
import logging
import math
import os
from datetime import datetime, timezone
from typing import List, Tuple

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import BooleanType, FloatType
from pyspark.sql.window import Window

from processing.utils.spark_session import get_spark_session
from processing.utils.audit_logger import AuditLogger

logger = logging.getLogger(__name__)


# ── Rule 1: Velocity Attack ────────────────────────────────────────

def detect_velocity_attack(df: DataFrame) -> DataFrame:
    """
    Detect users with >= 10 transactions in a 60-second window.
    
    Returns DataFrame with columns: transaction_id, reason_code, severity_score, description
    """
    window = Window.partitionBy("user_id").orderBy(F.col("timestamp").cast("long")).rangeBetween(-60, 0)

    velocity_df = df.withColumn("txn_count_60s", F.count("transaction_id").over(window))

    alerts = (
        velocity_df
        .filter(F.col("txn_count_60s") >= 10)
        .select(
            "transaction_id",
            F.lit("VELOCITY_ATTACK").alias("reason_code"),
            F.least(F.col("txn_count_60s") / 20.0, F.lit(1.0)).cast(FloatType()).alias("severity_score"),
            F.concat(
                F.lit("User had "),
                F.col("txn_count_60s").cast("string"),
                F.lit(" transactions in 60 seconds")
            ).alias("description")
        )
    )
    return alerts


# ── Rule 2: High Amount Deviation ─────────────────────────────────

def detect_high_amount(df: DataFrame, lookback_days: int = 30) -> DataFrame:
    """
    Detect transactions > 500% of user's rolling 30-day average amount.
    
    Returns DataFrame with columns: transaction_id, reason_code, severity_score, description
    """
    cutoff = F.current_date() - F.expr(f"INTERVAL {lookback_days} DAYS")

    user_stats = (
        df
        .filter(F.col("timestamp") >= cutoff)
        .groupBy("user_id")
        .agg(
            F.avg("amount").alias("avg_amount"),
            F.stddev("amount").alias("stddev_amount"),
        )
    )

    alerts = (
        df.join(user_stats, on="user_id", how="inner")
        .filter(
            (F.col("amount") > F.col("avg_amount") * 5.0) &
            (F.col("avg_amount") > 0)
        )
        .select(
            "transaction_id",
            F.lit("HIGH_AMOUNT").alias("reason_code"),
            F.least(
                (F.col("amount") - F.col("avg_amount")) / (F.col("avg_amount") * 5.0),
                F.lit(1.0)
            ).cast(FloatType()).alias("severity_score"),
            F.concat(
                F.lit("Amount $"),
                F.col("amount").cast("string"),
                F.lit(" vs user avg $"),
                F.round(F.col("avg_amount"), 2).cast("string"),
            ).alias("description")
        )
    )
    return alerts


# ── Rule 3: Geographic Impossibility ──────────────────────────────

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance in km between two coordinates using Haversine formula."""
    R = 6371  # Earth radius in km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def detect_geo_impossible(df: DataFrame) -> DataFrame:
    """
    Detect physically impossible travel: same user, different locations, < 1 hour, > 800km/h speed.
    
    Uses Spark window functions to compare consecutive transactions.
    """
    haversine_udf = F.udf(haversine_km, FloatType())

    window = Window.partitionBy("user_id").orderBy("timestamp")

    df_with_prev = (
        df
        .withColumn("prev_lat", F.lag("latitude").over(window))
        .withColumn("prev_lon", F.lag("longitude").over(window))
        .withColumn("prev_timestamp", F.lag("timestamp").over(window))
        .withColumn("prev_country", F.lag("country_code").over(window))
    )

    alerts = (
        df_with_prev
        .filter(
            F.col("prev_lat").isNotNull() &
            (F.col("country_code") != F.col("prev_country"))
        )
        .withColumn(
            "distance_km",
            haversine_udf(F.col("prev_lat"), F.col("prev_lon"), F.col("latitude"), F.col("longitude"))
        )
        .withColumn(
            "time_diff_hours",
            (F.unix_timestamp("timestamp") - F.unix_timestamp("prev_timestamp")) / 3600.0
        )
        .withColumn(
            "implied_speed_kmh",
            F.col("distance_km") / F.when(F.col("time_diff_hours") > 0, F.col("time_diff_hours")).otherwise(0.001)
        )
        .filter(F.col("implied_speed_kmh") > 800)
        .select(
            "transaction_id",
            F.lit("GEO_IMPOSSIBLE").alias("reason_code"),
            F.least(F.col("implied_speed_kmh") / 2000.0, F.lit(1.0)).cast(FloatType()).alias("severity_score"),
            F.concat(
                F.lit("Impossible travel: "),
                F.round(F.col("distance_km"), 0).cast("string"),
                F.lit("km in "),
                F.round(F.col("time_diff_hours"), 2).cast("string"),
                F.lit("h ("),
                F.round(F.col("implied_speed_kmh"), 0).cast("string"),
                F.lit(" km/h)")
            ).alias("description")
        )
    )
    return alerts


def run_anomaly_detection(date: str, s3_bucket: str) -> dict:
    """Execute anomaly detection for a given date."""
    spark = get_spark_session("AnomalyDetection")

    silver_path = f"s3a://{s3_bucket}/silver/transactions/partition_date={date}/"
    silver_df = spark.read.parquet(silver_path)

    # Run all rules
    velocity_alerts = detect_velocity_attack(silver_df)
    amount_alerts = detect_high_amount(silver_df)
    geo_alerts = detect_geo_impossible(silver_df)

    # Combine all alerts
    all_alerts = (
        velocity_alerts
        .union(amount_alerts)
        .union(geo_alerts)
        .withColumn("alert_timestamp", F.current_timestamp())
        .withColumn("status", F.lit("open"))
        .withColumn("ml_score", F.lit(None).cast(FloatType()))
        .withColumn("rule_score", F.col("severity_score"))
    )

    total_alerts = all_alerts.count()
    logger.info(f"Anomaly detection complete. Total alerts: {total_alerts}")

    # Write to Postgres alerts_log
    jdbc_url = f"jdbc:postgresql://{os.environ['POSTGRES_HOST']}:{os.environ.get('POSTGRES_PORT', '5432')}/{os.environ['POSTGRES_DB']}"
    props = {
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }

    all_alerts.write.jdbc(
        url=jdbc_url,
        table="gold.alerts_log",
        mode="append",
        properties=props,
    )

    spark.stop()
    return {"total_alerts": total_alerts}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--bucket", default="fintech-raw-data")
    args = parser.parse_args()
    run_anomaly_detection(args.date, args.bucket)
```

---

### S3-TASK-02: Streamlit Dashboard

**Archivo:** `analytics/app.py`

```python
"""
Fintech Analytics Dashboard — Main Entry Point.

Pages:
    1. Executive KPIs
    2. Fraud Monitoring
    3. Operational Health

Run:
    streamlit run app.py
"""

import streamlit as st
from analytics.utils.db_connection import get_db_engine

st.set_page_config(
    page_title="Fintech Analytics",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Sidebar navigation
st.sidebar.title("🏦 Fintech Analytics")
st.sidebar.markdown("---")

# Test DB connection on load
try:
    engine = get_db_engine()
    with engine.connect() as conn:
        conn.execute("SELECT 1")
    st.sidebar.success("✅ Database connected")
except Exception as e:
    st.sidebar.error(f"❌ DB Error: {e}")

st.sidebar.markdown("---")
st.sidebar.markdown("**Data Freshness**")

# Main landing page
st.title("Fintech Data Intelligence Platform")
st.markdown("""
Navigate using the sidebar to explore:
- **Executive KPIs**: TPV, transaction volume, fraud rate
- **Fraud Monitoring**: Real-time alerts, heatmaps, severity
- **Operational Health**: Pipeline status, data freshness
""")
```

**Archivo:** `analytics/pages/01_executive_kpis.py`

```python
"""Executive KPIs Page."""

import streamlit as st
import pandas as pd
from analytics.utils.db_connection import run_query

st.title("📊 Executive KPIs")

@st.cache_data(ttl=300)
def get_kpis():
    return run_query("""
        SELECT
            SUM(amount) FILTER (WHERE status = 'completed')   AS tpv,
            COUNT(*)                                           AS total_txns,
            AVG(amount) FILTER (WHERE status = 'completed')   AS avg_txn_value,
            ROUND(100.0 * COUNT(*) FILTER (WHERE is_flagged_fraud) / NULLIF(COUNT(*), 0), 4) AS fraud_rate_pct
        FROM gold.fact_transactions
        WHERE time_sk >= TO_CHAR(NOW() - INTERVAL '30 days', 'YYYYMMDD')::INT
    """)

@st.cache_data(ttl=300)
def get_tpv_trend():
    return run_query("""
        SELECT
            dt.date,
            SUM(ft.amount) AS daily_tpv
        FROM gold.fact_transactions ft
        JOIN gold.dim_time dt ON ft.time_sk = dt.time_sk
        WHERE dt.date >= NOW() - INTERVAL '30 days'
          AND ft.status = 'completed'
        GROUP BY dt.date
        ORDER BY dt.date
    """)

@st.cache_data(ttl=300)
def get_txn_by_method():
    return run_query("""
        SELECT payment_method, COUNT(*) AS count
        FROM gold.fact_transactions
        WHERE time_sk >= TO_CHAR(NOW() - INTERVAL '30 days', 'YYYYMMDD')::INT
        GROUP BY payment_method
        ORDER BY count DESC
    """)

# ── KPI Row ────────────────────────────────────────────────────────
kpis = get_kpis()
if not kpis.empty:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("💵 TPV (30d)", f"${kpis['tpv'].iloc[0]:,.2f}")
    col2.metric("🔢 Transactions", f"{kpis['total_txns'].iloc[0]:,}")
    col3.metric("📈 Avg Value", f"${kpis['avg_txn_value'].iloc[0]:,.2f}")
    col4.metric("🚨 Fraud Rate", f"{kpis['fraud_rate_pct'].iloc[0]:.2f}%")

# ── Charts ──────────────────────────────────────────────────────────
col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("TPV Trend (Last 30 Days)")
    trend_df = get_tpv_trend()
    if not trend_df.empty:
        st.line_chart(trend_df.set_index("date")["daily_tpv"])

with col_right:
    st.subheader("Volume by Payment Method")
    method_df = get_txn_by_method()
    if not method_df.empty:
        import plotly.express as px
        fig = px.pie(method_df, values="count", names="payment_method", hole=0.4)
        st.plotly_chart(fig, use_container_width=True)
```

**Archivo:** `analytics/utils/db_connection.py`

```python
"""Database connection utilities with retry logic."""

import os
import logging
import time
from functools import lru_cache

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_db_engine():
    """Create SQLAlchemy engine with connection pooling. Cached singleton."""
    conn_str = os.environ["STREAMLIT_DB_CONNECTION"]
    return create_engine(
        conn_str,
        poolclass=QueuePool,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
    )


def run_query(sql: str, retries: int = 3) -> pd.DataFrame:
    """
    Execute SQL query and return results as DataFrame.
    
    Args:
        sql: SQL query string.
        retries: Number of retry attempts on connection error.
        
    Returns:
        pandas DataFrame with query results, or empty DataFrame on error.
    """
    engine = get_db_engine()
    for attempt in range(retries):
        try:
            with engine.connect() as conn:
                return pd.read_sql(text(sql), conn)
        except Exception as e:
            logger.warning(f"Query attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
    logger.error(f"Query failed after {retries} attempts")
    return pd.DataFrame()
```

---

### S3-TASK-03: Observabilidad — Prometheus

**Archivo:** `monitoring/prometheus/prometheus.yml`

```yaml
global:
  scrape_interval: 30s
  evaluation_interval: 30s

scrape_configs:
  - job_name: 'airflow'
    static_configs:
      - targets: ['airflow-webserver:8080']
    metrics_path: '/metrics'

  - job_name: 'postgres'
    static_configs:
      - targets: ['postgres-exporter:9187']

  - job_name: 'spark'
    static_configs:
      - targets: ['spark-master:8081']
    metrics_path: '/metrics/prometheus'

  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']
```

---

## 🧪 ESTRATEGIA DE TESTING

### Tests Unitarios

| Componente | Framework | Coverage Target | Archivo |
|------------|-----------|-----------------|---------|
| Data Generator | pytest | 80% | `data_generator/tests/` |
| Bronze Job | pytest + chispa | 70% | `processing/tests/test_bronze_ingest.py` |
| Silver Job | pytest + chispa | 80% | `processing/tests/test_silver_cleanse.py` |
| Anomaly Rules | pytest | 90% | `processing/tests/test_anomaly_rules.py` |
| dbt Models | dbt test | 100% models | `warehouse/dbt/tests/` |
| Streamlit | pytest | 60% | N/A (query validation) |

### Tests de Integración

**Archivo:** `tests/integration/test_end_to_end_pipeline.py`

```python
"""
End-to-End Pipeline Integration Test.

Requires: All Docker services running.
Run: pytest tests/integration/ -v --run-integration
"""

import pytest
import boto3
from botocore.config import Config


@pytest.mark.integration
def test_bronze_files_created_after_generation(s3_client, generator):
    """After data generation, Bronze Parquet files must exist in S3."""
    generator.run("all")  # Run data generator
    
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket="fintech-raw-data", Prefix="bronze/transactions/")
    
    bronze_files = [obj for page in pages for obj in page.get("Contents", [])]
    assert len(bronze_files) > 0, "No Bronze files found after ingestion"


@pytest.mark.integration
def test_silver_quality_gates_reject_bad_data(spark, s3_bucket):
    """Silver job must reject transactions with NULL IDs and negative amounts."""
    from processing.jobs.silver_cleanse import cast_and_validate
    
    bad_records = [
        {"transaction_id": None, "user_id": "u1", "amount": 100.0, ...},
        {"transaction_id": "t1", "user_id": "u1", "amount": -50.0, ...},
    ]
    
    df = spark.createDataFrame(bad_records)
    validated = cast_and_validate(df)
    
    assert validated.count() == 0, "Bad records should be rejected"


@pytest.mark.integration
def test_alerts_log_populated_for_known_fraud(pg_connection, date):
    """alerts_log must contain alerts for fraud-injected transactions."""
    result = pg_connection.execute(
        "SELECT COUNT(*) FROM gold.alerts_log WHERE reason_code = 'VELOCITY_ATTACK'"
    ).fetchone()
    assert result[0] > 0, "No velocity attack alerts found"
```

### Tests de Anomaly Rules

**Archivo:** `processing/tests/test_anomaly_rules.py`

```python
"""Unit tests for fraud detection rules."""

import pytest
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from processing.jobs.anomaly_detection import (
    detect_velocity_attack,
    detect_high_amount,
    detect_geo_impossible,
)


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()


def test_velocity_attack_detected(spark):
    """10+ transactions in 60s from same user should trigger VELOCITY_ATTACK."""
    base_time = datetime(2026, 4, 24, 10, 0, 0)
    records = [
        {"transaction_id": f"t{i}", "user_id": "u1", "timestamp": base_time + timedelta(seconds=i * 5), "amount": 10.0}
        for i in range(12)  # 12 transactions in 60 seconds
    ]
    df = spark.createDataFrame(records)
    alerts = detect_velocity_attack(df)
    
    assert alerts.count() > 0
    assert alerts.first()["reason_code"] == "VELOCITY_ATTACK"
    assert alerts.first()["severity_score"] > 0.5


def test_velocity_attack_not_triggered_below_threshold(spark):
    """< 10 transactions in 60s should NOT trigger alert."""
    base_time = datetime(2026, 4, 24, 10, 0, 0)
    records = [
        {"transaction_id": f"t{i}", "user_id": "u1", "timestamp": base_time + timedelta(seconds=i * 10), "amount": 10.0}
        for i in range(5)  # Only 5 transactions
    ]
    df = spark.createDataFrame(records)
    alerts = detect_velocity_attack(df)
    
    assert alerts.count() == 0


def test_high_amount_detected(spark):
    """Transaction >500% of user average should trigger HIGH_AMOUNT."""
    records = [
        {"transaction_id": f"t{i}", "user_id": "u1", "amount": 10.0 + i, "timestamp": datetime(2026, 4, i % 28 + 1)}
        for i in range(30)
    ]
    records.append({
        "transaction_id": "t-fraud", "user_id": "u1", "amount": 1000.0, "timestamp": datetime(2026, 4, 24)
    })
    df = spark.createDataFrame(records)
    alerts = detect_high_amount(df)
    
    fraud_alert = alerts.filter(alerts.transaction_id == "t-fraud")
    assert fraud_alert.count() == 1
    assert fraud_alert.first()["reason_code"] == "HIGH_AMOUNT"


def test_geo_impossible_detected(spark):
    """Transactions in MX and JP within 30 min should trigger GEO_IMPOSSIBLE."""
    records = [
        {"transaction_id": "t1", "user_id": "u1", "timestamp": datetime(2026, 4, 24, 10, 0), 
         "latitude": 19.43, "longitude": -99.13, "country_code": "MX", "amount": 50.0},
        {"transaction_id": "t2", "user_id": "u1", "timestamp": datetime(2026, 4, 24, 10, 30),
         "latitude": 35.68, "longitude": 139.69, "country_code": "JP", "amount": 50.0},
    ]
    df = spark.createDataFrame(records)
    alerts = detect_geo_impossible(df)
    
    assert alerts.count() == 1
    assert alerts.first()["reason_code"] == "GEO_IMPOSSIBLE"
    assert alerts.first()["severity_score"] > 0.9
```

---

## 🔐 SEGURIDAD Y COMPLIANCE

### Checklist de Seguridad

| # | Control | Implementación |
|---|---------|----------------|
| SEC-01 | PII hasheado antes de Silver | `ip_address` → `ip_address_hash` (SHA-256) en `silver_cleanse.py` |
| SEC-02 | Email hasheado en Gold | `email_hash` en `dim_users` (SHA-256) |
| SEC-03 | Sin credenciales en código | 100% env vars vía `.env` file |
| SEC-04 | `.env` en `.gitignore` | Solo `.env.example` commiteable |
| SEC-05 | RBAC en Airflow | Roles: `viewer`, `user`, `admin` configurados en `webserver_config.py` |
| SEC-06 | Schemas separados en Postgres | `bronze`, `silver`, `gold`, `staging`, `analytics` con permisos granulares |
| SEC-07 | Least-privilege en Spark | Sin credenciales AWS en código; IAM role simulation en LocalStack |
| SEC-08 | Audit trail | Cada job loguea: run_id, input_rows, output_rows, duration, timestamp |
| SEC-09 | Encriptación S3 | AES-256 server-side encryption (simulado en LocalStack) |

### Archivo `.env.example`

```bash
# ─── Infrastructure ────────────────────────────────────────────────
LOCALSTACK_ENDPOINT=http://localstack:4566
AWS_REGION=us-east-1
S3_BUCKET=fintech-raw-data

# ─── PostgreSQL ────────────────────────────────────────────────────
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=fintech_dw
POSTGRES_USER=fintech_user
POSTGRES_PASSWORD=CHANGE_ME_BEFORE_USE

# ─── Airflow ───────────────────────────────────────────────────────
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://fintech_user:CHANGE_ME_BEFORE_USE@postgres:5432/airflow
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://fintech_user:CHANGE_ME_BEFORE_USE@postgres:5432/airflow
AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0
AIRFLOW_FERNET_KEY=GENERATE_WITH_python_-c_"from_cryptography.fernet_import_Fernet;_print(Fernet.generate_key().decode())"
AIRFLOW_UID=50000

# ─── Spark ─────────────────────────────────────────────────────────
SPARK_MASTER_URL=spark://spark-master:7077
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g
SPARK_EXECUTOR_CORES=2

# ─── Data Generator ────────────────────────────────────────────────
GENERATOR_BATCH_SIZE=1000
GENERATOR_FRAUD_RATE=0.02

# ─── Streamlit ─────────────────────────────────────────────────────
STREAMLIT_SERVER_PORT=8501
STREAMLIT_DB_CONNECTION=postgresql://fintech_user:CHANGE_ME_BEFORE_USE@postgres:5432/fintech_dw

# ─── Grafana ───────────────────────────────────────────────────────
GRAFANA_PASSWORD=CHANGE_ME_BEFORE_USE
```

---

## 📊 SLAs DE VOLUMETRÍA Y PERFORMANCE

| Métrica | Target | Cómo Medir |
|---------|--------|------------|
| Ingestión Raw | 10K txns/min | `make test-perf` con 100K batch |
| Latencia Bronze | < 30s desde S3 write | Airflow task duration |
| Latencia Silver | < 5min desde Bronze | Airflow task duration |
| Latencia Gold | < 10min desde Silver | Airflow task duration |
| Dashboard Query p95 | < 2s | Streamlit `time.perf_counter()` |
| Bronze Total (100K batch) | < 2 min | Performance test |
| Silver Total (100K batch) | < 3 min | Performance test |
| Gold Load (100K batch) | < 2 min | Performance test |
| Data Retention Raw | 30 días | S3 lifecycle policy |
| Data Retention Silver | 90 días | S3 lifecycle policy |
| Data Retention Gold | 7 años | Postgres partitioning |

---

## ✅ DEFINITION OF DONE

### Project Level (Todos deben estar en ✅ antes de declarar done)

- [ ] `make up` levanta ambiente funcional en <10 min en clone fresco
- [ ] `make test` pasa todos los unit + integration + data quality tests
- [ ] `make down` destruye todo sin recursos huérfanos ni volúmenes colgados
- [ ] DAG corre exitosamente 7 días consecutivos (simulados con backfill)
- [ ] Dashboard muestra los 6 KPIs requeridos con query time <2s
- [ ] Fraud detection identifica los 3 patrones con >95% accuracy en labeled test set
- [ ] Documentación completa (README, ARCHITECTURE, RUNBOOK, DATA_CONTRACTS)
- [ ] Cero credenciales commiteadas al repositorio
- [ ] Code review checklist completado (type hints, docstrings, error handling)
- [ ] Todos los Dockerfiles usan non-root users y multi-stage builds donde aplica
- [ ] Great Expectations suite tiene ≥10 expectations en Silver layer
- [ ] dbt `test` 100% verde antes de merge

---

## 📐 DECISIONES ARQUITECTÓNICAS

| ID | Decisión | Alternativas Consideradas | Rationale |
|----|----------|--------------------------|-----------|
| AD-01 | Spark sobre Pandas | Pandas, Dask | Escala a TBs; API unificada batch/streaming; SQL+Python |
| AD-02 | Airflow sobre Prefect/Dagster | Prefect, Dagster, Luigi | Ecosystem maduro; Celery escala; backfill nativo; amplia comunidad |
| AD-03 | PostgreSQL sobre ClickHouse | ClickHouse, Snowflake, DuckDB | Dev local simple; dbt-native; suficiente para <1TB |
| AD-04 | LocalStack sobre real AWS | Real AWS, MinIO | Costo cero; reproducible; ideal para CI/CD |
| AD-05 | Parquet sobre Delta Lake | Delta Lake, Iceberg | Setup más simple; suficiente para Bronze/Silver inmutables |
| AD-06 | Streamlit sobre Tableau/Metabase | Tableau, PowerBI, Metabase | Code-native; gratis; integra con Python data stack |
| AD-07 | CeleryExecutor sobre LocalExecutor | LocalExecutor, KubernetesExecutor | Escala horizontal; mejor para producción; Redis ya en stack |
| AD-08 | dbt Core sobre transformaciones en Spark | Pure Spark SQL, SQLMesh | SQL versionado; tests nativos; lineage automático; docs |

---

## 🔮 PUNTOS DE EXTENSIÓN (Post-MVP)

Una vez completado el MVP, estos son los siguientes pasos naturales:

1. **Real AWS:** Reemplazar LocalStack con S3 + EMR/ECS reales
2. **Kafka Streaming:** Agregar Kafka para real-time (Spark Structured Streaming)
3. **CI/CD:** GitHub Actions con lint, test, build de imágenes
4. **Delta Lake:** Migrar Bronze/Silver a Delta para ACID + time travel
5. **OAuth2 en Streamlit:** Autenticación para el dashboard
6. **MLflow:** Tracking de experimentos para el modelo de anomalías
7. **Kubernetes:** Migrar Docker Compose a Helm charts
8. **Data Catalog:** Agregar OpenMetadata o DataHub

---

## 📝 GLOSARIO

| Término | Definición |
|---------|------------|
| Bronze | Capa de datos crudos ingeridos tal cual, sin transformación |
| Silver | Capa de datos limpios, deduplicados y validados |
| Gold | Capa de datos modelados en star schema, listos para BI |
| SCD Type 2 | Slowly Changing Dimension: trackear historial de cambios con valid_from/valid_to |
| MCC | Merchant Category Code: código de 4 dígitos que clasifica el tipo de negocio |
| TPV | Total Payment Volume: suma total de transacciones completadas |
| GE | Great Expectations: framework de data quality |
| DAG | Directed Acyclic Graph: grafo de tareas en Airflow |
| JDBC | Java Database Connectivity: API de conexión a bases de datos para Spark |
| LocalStack | Emulador local de servicios AWS |
| IaC | Infrastructure as Code: infra definida como código (Terraform) |

---

*Este documento es la fuente de verdad para la implementación.*  
*Versión: 3.0 | Status: APROBADO PARA DESARROLLO | Fecha: 2026-04-24*
