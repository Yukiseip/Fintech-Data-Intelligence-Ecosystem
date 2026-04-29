-- ════════════════════════════════════════════════════════════════════
-- 01_create_roles.sql
-- Creates PostgreSQL roles with least-privilege access.
-- Executed automatically on postgres container first start.
-- ════════════════════════════════════════════════════════════════════

-- ── Application roles ─────────────────────────────────────────────
-- Read-only role for Streamlit dashboard
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'fintech_readonly') THEN
    CREATE ROLE fintech_readonly NOLOGIN;
  END IF;
END $$;

-- Write role for Spark jobs (Gold/Alerts insert)
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'fintech_writer') THEN
    CREATE ROLE fintech_writer NOLOGIN;
  END IF;
END $$;

-- Admin role for dbt and migrations
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'fintech_admin') THEN
    CREATE ROLE fintech_admin NOLOGIN;
  END IF;
END $$;

-- ── Login users ───────────────────────────────────────────────────
-- Main application user (maps to POSTGRES_USER in .env)
-- This user already exists from POSTGRES_USER env var; grant roles to it.
DO $$
BEGIN
  IF EXISTS (SELECT FROM pg_roles WHERE rolname = current_user) THEN
    EXECUTE format('GRANT fintech_writer TO %I', current_user);
    EXECUTE format('GRANT fintech_admin  TO %I', current_user);
  END IF;
END $$;

-- Airflow metadata user (same DB, separate DB in production)
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow') THEN
    CREATE ROLE airflow LOGIN PASSWORD 'airflow_local_only';
  END IF;
END $$;
