#!/bin/bash
# ════════════════════════════════════════════════════════════════════
# 00_create_airflow_db.sh
# Creates the Airflow metadata database on Postgres first-start.
# Docker Postgres runs all *.sh and *.sql scripts in /docker-entrypoint-initdb.d
# in alphabetical order, as the superuser.
# ════════════════════════════════════════════════════════════════════
set -e

echo ">>> Creating 'airflow' database if it does not exist..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  SELECT 'CREATE DATABASE airflow'
  WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

  GRANT ALL PRIVILEGES ON DATABASE airflow TO "$POSTGRES_USER";
EOSQL

echo ">>> Done: 'airflow' database is ready."
