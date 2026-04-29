-- ════════════════════════════════════════════════════════════════════
-- 03_create_extensions.sql
-- Enables required PostgreSQL extensions.
-- Executed automatically on postgres container first start.
-- ════════════════════════════════════════════════════════════════════

-- UUID generation (used by surrogate keys)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Query performance statistics (used by Prometheus postgres-exporter)
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- pgcrypto for SHA-256 hashing (used in dbt snapshot)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Note: The 'airflow' database is created by the airflow-init container
-- via `airflow db migrate`, which connects using the AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
-- env var pointing to postgres:5432/airflow. No manual creation needed here.
