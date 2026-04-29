# Fintech Data Intelligence Platform — Operations Runbook

> Version: 2.0 | Sprint 3 Final | Env: Local Docker

---

## 🚀 Starting the Stack

```bash
# 1. Copy environment template (one-time setup)
cp .env.example .env
# Edit .env with secure values before running

# 2. Start all services
make up

# 3. Verify all services are healthy
docker compose ps
```

Expected output: all 13 services with `healthy` or `running` status.

---

## ⚡ Common Make Commands

| Command | Description | Duration |
|---------|-------------|----------|
| `make up` | Start full stack | ~3-5 min |
| `make down` | Stop + destroy volumes | < 30s |
| `make generate` | Generate 10K synthetic transactions | ~2 min |
| `make test` | Run all unit tests | ~5 min |
| `make test-integration` | E2E tests (stack must be running) | ~3 min |
| `make test-perf` | Performance SLA tests | ~2 min |
| `make infra` | Re-provision Terraform infrastructure | ~1 min |
| `make dbt-run` | Execute all dbt models | ~2 min |
| `make dbt-test` | Run all dbt data quality tests | ~1 min |
| `make logs` | Tail all service logs | continuous |
| `make clean` | Remove all containers + volumes + images | ~5 min |

---

## 🔄 Triggering the Pipeline Manually

```bash
# Option A: Via Airflow CLI
docker exec fintech-airflow-scheduler-1 \
  airflow dags trigger fintech_master_pipeline

# Option B: Via Airflow UI
# 1. Go to http://localhost:8080
# 2. Login: admin / admin
# 3. Enable DAG 'fintech_master_pipeline'
# 4. Click ▶ Trigger DAG

# Option C: Via Makefile
make generate && make dbt-run
```

---

## 🔍 Verifying Data Flow

```bash
# Check Bronze Parquet exists in S3
docker exec fintech-localstack-1 \
  awslocal s3 ls s3://fintech-raw-data/bronze/transactions/ --recursive | head -20

# Check Silver Parquet exists
docker exec fintech-localstack-1 \
  awslocal s3 ls s3://fintech-raw-data/silver/transactions/ --recursive | head -20

# Check Gold row counts
docker exec fintech-postgres-1 psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -c \
  "SELECT COUNT(*) FROM gold.fact_transactions;"

# Check alerts
docker exec fintech-postgres-1 psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -c \
  "SELECT reason_code, COUNT(*), ROUND(AVG(severity_score)::NUMERIC,3)
   FROM gold.alerts_log GROUP BY reason_code;"
```

---

## 🛠️ Debugging Common Issues

### Airflow DAG Not Appearing
```bash
# Check for DAG import errors
docker exec fintech-airflow-scheduler-1 \
  airflow dags list-import-errors

# Force DAG reload
docker exec fintech-airflow-scheduler-1 \
  airflow dags reserialize
```

### LocalStack Not Ready
```bash
# Check LocalStack health
curl http://localhost:4566/_localstack/health | python -m json.tool

# Re-provision Terraform
make infra
```

### Spark Job Failing
```bash
# View Spark master UI
open http://localhost:8081

# Check Spark job logs
docker exec fintech-spark-master-1 \
  find /opt/spark/work -name "stderr" | tail -3 | xargs cat
```

### PostgreSQL Connection Error
```bash
# Test connectivity
docker exec fintech-postgres-1 \
  psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -c "SELECT version();"

# Check pg logs
docker logs fintech-postgres-1 --tail 50
```

### Redis Not Reachable (Celery)
```bash
docker exec fintech-redis-1 redis-cli ping
# Expected: PONG
```

---

## 📊 Monitoring

| Service | URL |
|---------|-----|
| Grafana (dashboards) | http://localhost:3000 |
| Prometheus (metrics) | http://localhost:9090 |
| Airflow UI | http://localhost:8080 |
| Spark UI | http://localhost:8081 |

### Key Grafana Dashboards
1. **Fintech Executive KPIs** — TPV, fraud rate, transaction volume
2. **Fintech Fraud Monitoring** — Open alerts, severity, country heatmap

---

## 🔐 Security Checklist

- [ ] `.env` is NOT committed to git (`git status` shows clean)
- [ ] All Docker containers run as non-root users
- [ ] `AIRFLOW_SECRET_KEY` is a 32+ character random string
- [ ] `AIRFLOW_FERNET_KEY` is a valid 32-byte base64 key
- [ ] `POSTGRES_PASSWORD` has been changed from `.env.example` default
- [ ] `SESSION_COOKIE_SECURE = True` when behind HTTPS in production

Generate a valid Fernet key:
```python
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
```

---

## 🔄 Data Recovery Procedures

### Reprocess a Specific Date
```bash
# 1. Re-generate raw data for date
docker exec fintech-data-generator-1 \
  python main.py generate all --date 2026-04-24

# 2. Trigger Bronze job
docker exec fintech-airflow-scheduler-1 \
  airflow dags trigger fintech_master_pipeline \
  --conf '{"date": "2026-04-24"}'
```

### Reset Gold Layer (Full Reload)
```bash
# WARNING: Destructive operation
docker exec fintech-postgres-1 psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -c \
  "TRUNCATE gold.fact_transactions, gold.alerts_log RESTART IDENTITY CASCADE;"

# Re-run dbt
make dbt-run
```

---

## 📈 SLA Targets

| SLA | Target | How to Verify |
|-----|--------|---------------|
| Pipeline end-to-end | < 45 min | Airflow DAG run duration |
| Ingest throughput | 10K txns/min | `make test-perf` |
| Dashboard p95 latency | < 2s | `make test-perf` |
| Fraud detection precision | > 95% | `make test-fraud` |
| Stack cold start | < 10 min | `time make up` |
