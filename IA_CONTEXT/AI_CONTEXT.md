# AI Context Document — Fintech Data Intelligence Ecosystem

## Purpose
This document provides the essential context an AI coding assistant needs to understand, analyze, and implement the Fintech Data Intelligence Ecosystem described in `FINTECH_TSD_v3_0_MASTER.md`.

## Project Identity
- **Name:** Fintech Analytics Engine
- **Type:** Production-grade containerized data platform
- **Complexity:** Senior Staff Data Engineer level
- **Effort:** 3 Sprints / 6 weeks
- **Status:** ✅ Approved for Development

## Core Objective
Build a containerized, production-grade data intelligence platform that:
1. Ingests synthetic high-velocity transaction streams (10K txns/min)
2. Detects financial anomalies in near-real-time (3 fraud patterns)
3. Exposes aggregated KPIs via a Streamlit BI dashboard
4. Orchestrates everything via Apache Airflow with CeleryExecutor
5. Stores data in Medallion Architecture (S3/LocalStack + PostgreSQL)

## Single Command Entrypoint
```bash
make up
```
This must bring up the entire stack from a fresh clone in <10 minutes.

## Architecture Pillars
- **Medallion Architecture:** Bronze (raw) → Silver (cleaned) → Gold (modeled)
- **Clean Architecture:** Strict layer separation
- **Infrastructure as Code:** Fully reproducible via `make up`
- **Observability First:** Logging, metrics, data quality in every layer
- **Security by Design:** PII hashed, secrets in env vars, least-privilege

## High-Level Data Flow
```
Data Generator (Python + Faker)
    → S3 LocalStack (Raw JSONL)
    → Spark Bronze Job (JSONL → Parquet)
    → Spark Silver Job (Dedup + Validate)
    → Spark Gold Job (Star Schema → PostgreSQL)
    → Airflow DAG (@hourly orchestration)
    → Anomaly Engine (Rules + ML)
    → Streamlit Dashboard (KPIs + Fraud BI)
    → Prometheus + Grafana (Observability)
```

## Success Criteria (MUST VERIFY)
| ID | Objective | Metric | Verification |
|----|-----------|--------|--------------|
| O1 | Ingest 10K+ txns/min, latency <5s to Bronze | Throughput test passes | `make test-perf` |
| O2 | Detect 3 fraud patterns with >95% precision | Benchmark on labeled test set | `make test-fraud` |
| O3 | Gold queries <2s p95 for dashboard | p95 latency check | `make test-dash` |
| O4 | 100% reproducible infrastructure | Fresh clone → running in <10 min | `make up` on clean machine |
| O5 | Data quality gates block bad data in Silver | Zero negative revenue in Gold | `dbt test` |

## Critical Constraints
- **NO hardcoded endpoints, credentials, or paths.** Everything goes to `.env`
- **Every Python function MUST have type hints and Google-style docstrings**
- **Tests before or alongside code.** No code without tests is accepted
- **One commit per deliverable.** Message format: `feat(S1-D1): description`
- **Strict sprint order.** Sprint 1 must be fully functional before touching Sprint 2
- **If a requirement is ambiguous, STOP and ask.** Do not assume

## Technology Stack Summary
| Layer | Technology | Version |
|-------|-----------|---------|
| Containers | Docker + Compose | 25.x / 2.24.x |
| Mock Cloud | LocalStack | 3.x |
| IaC | Terraform | 1.7.x |
| Orchestration | Apache Airflow | 2.9.x (CeleryExecutor) |
| Message Broker | Redis | 7.x |
| Database | PostgreSQL | 16.x |
| Processing | Apache Spark | 3.5.x (Standalone) |
| Language | Python | 3.11 |
| Data Quality | Great Expectations | 0.18.x |
| Transformations | dbt Core | 1.8.x |
| Dashboard | Streamlit | 1.35.x |
| Visualization | Plotly | 5.x |
| Metrics | Prometheus + Grafana | latest |

## Security Non-Negotiables
- PII hashed (SHA-256) before Silver layer
- `.env` in `.gitignore`; only `.env.example` committed
- RBAC in Airflow (viewer, user, admin)
- Separate Postgres schemas with granular permissions
- AES-256 S3 encryption (simulated in LocalStack)
- Audit trail in every job: run_id, input_rows, output_rows, duration, timestamp

## File References
- Master TSD: `FINTECH_TSD_v3_0_MASTER.md`
- Architecture: `ARCHITECTURE.md`
- Data Contracts: `DATA_CONTRACTS.md`
- Sprint Plan: `SPRINT_PLAN.md`
- Implementation Guide: `IMPLEMENTATION_GUIDE.md`
- Project Structure: `PROJECT_STRUCTURE.md`
