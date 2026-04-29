-- ════════════════════════════════════════════════════════════════════
-- 02_create_schemas.sql
-- Creates schemas and Gold layer tables (Kimball Star Schema).
-- Executed automatically on postgres container first start.
-- ════════════════════════════════════════════════════════════════════

-- ── Schemas ───────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- ── Grant schema privileges ───────────────────────────────────────
GRANT USAGE ON SCHEMA gold     TO fintech_readonly;
GRANT USAGE ON SCHEMA staging  TO fintech_writer;
GRANT USAGE ON SCHEMA gold     TO fintech_writer;
GRANT ALL   ON SCHEMA staging  TO fintech_admin;
GRANT ALL   ON SCHEMA gold     TO fintech_admin;

-- ════════════════════════════════════════════════════════════════════
-- GOLD LAYER — Star Schema Tables
-- ════════════════════════════════════════════════════════════════════

-- ── dim_time (pre-populated 2020-2030) ───────────────────────────
CREATE TABLE IF NOT EXISTS gold.dim_time (
    time_sk      INT         PRIMARY KEY,     -- YYYYMMDD as integer
    date         DATE        NOT NULL UNIQUE,
    year         SMALLINT    NOT NULL,
    month        SMALLINT    NOT NULL,
    day          SMALLINT    NOT NULL,
    quarter      SMALLINT    NOT NULL,
    day_of_week  SMALLINT    NOT NULL,        -- 1=Mon, 7=Sun
    is_weekend   BOOLEAN     NOT NULL,
    is_holiday   BOOLEAN     DEFAULT FALSE,
    month_name   VARCHAR(10),
    week_of_year SMALLINT
);

-- Pre-populate dim_time for 2020-01-01 to 2030-12-31
INSERT INTO gold.dim_time (
    time_sk, date, year, month, day, quarter,
    day_of_week, is_weekend, month_name, week_of_year
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT                  AS time_sk,
    d                                             AS date,
    EXTRACT(YEAR  FROM d)::SMALLINT               AS year,
    EXTRACT(MONTH FROM d)::SMALLINT               AS month,
    EXTRACT(DAY   FROM d)::SMALLINT               AS day,
    EXTRACT(QUARTER FROM d)::SMALLINT             AS quarter,
    EXTRACT(ISODOW FROM d)::SMALLINT              AS day_of_week,
    EXTRACT(ISODOW FROM d) IN (6, 7)              AS is_weekend,
    TO_CHAR(d, 'Month')                           AS month_name,
    EXTRACT(WEEK FROM d)::SMALLINT                AS week_of_year
FROM generate_series(
    '2020-01-01'::DATE,
    '2030-12-31'::DATE,
    '1 day'::INTERVAL
) AS d
ON CONFLICT (time_sk) DO NOTHING;

-- ── dim_users (SCD Type 2) ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.dim_users (
    user_sk           BIGSERIAL    PRIMARY KEY,
    user_id           UUID         NOT NULL,
    email_hash        VARCHAR(64)  NOT NULL,    -- SHA-256 of original email
    registration_date DATE,
    risk_tier         VARCHAR(10)  CHECK (risk_tier IN ('low', 'medium', 'high')),
    country_code      CHAR(2),
    valid_from        TIMESTAMP    NOT NULL DEFAULT NOW(),
    valid_to          TIMESTAMP    DEFAULT '9999-12-31'::TIMESTAMP,
    is_current        BOOLEAN      DEFAULT TRUE,
    dbt_scd_id        VARCHAR(64)                -- dbt snapshot hash key
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_users_current
    ON gold.dim_users(user_id) WHERE is_current = TRUE;

-- ── dim_merchants ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.dim_merchants (
    merchant_sk   BIGSERIAL    PRIMARY KEY,
    merchant_id   UUID         NOT NULL UNIQUE,
    merchant_name VARCHAR(255),
    mcc_code      CHAR(4),
    country_code  CHAR(2),
    risk_score    DECIMAL(5,4) DEFAULT 0.5,
    created_at    TIMESTAMP    DEFAULT NOW()
);

-- ── fact_transactions ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.fact_transactions (
    transaction_sk   BIGSERIAL     PRIMARY KEY,
    transaction_id   UUID          NOT NULL UNIQUE,
    user_sk          BIGINT        NOT NULL REFERENCES gold.dim_users(user_sk),
    merchant_sk      BIGINT        NOT NULL REFERENCES gold.dim_merchants(merchant_sk),
    time_sk          INT           NOT NULL REFERENCES gold.dim_time(time_sk),
    amount           DECIMAL(18,2) NOT NULL,
    currency         CHAR(3)       NOT NULL,
    transaction_type VARCHAR(20)   NOT NULL,
    status           VARCHAR(20)   NOT NULL,
    payment_method   VARCHAR(20)   NOT NULL,
    is_flagged_fraud BOOLEAN       DEFAULT FALSE,
    fraud_score      DECIMAL(5,4)  DEFAULT 0.0,
    latitude         DECIMAL(9,6),
    longitude        DECIMAL(9,6),
    country_code     CHAR(2),
    created_at       TIMESTAMP     DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_txn_user    ON gold.fact_transactions(user_sk);
CREATE INDEX IF NOT EXISTS idx_fact_txn_time    ON gold.fact_transactions(time_sk);
CREATE INDEX IF NOT EXISTS idx_fact_txn_fraud
    ON gold.fact_transactions(is_flagged_fraud)
    WHERE is_flagged_fraud = TRUE;

-- ── alerts_log ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.alerts_log (
    alert_id        BIGSERIAL     PRIMARY KEY,
    transaction_id  UUID          NOT NULL,
    alert_timestamp TIMESTAMP     NOT NULL DEFAULT NOW(),
    reason_code     VARCHAR(50)   NOT NULL,
    -- VELOCITY_ATTACK | HIGH_AMOUNT | GEO_IMPOSSIBLE | ML_ANOMALY
    severity_score  DECIMAL(5,4)  NOT NULL CHECK (severity_score BETWEEN 0 AND 1),
    description     TEXT,
    status          VARCHAR(40)   DEFAULT 'open',
    -- open | investigating | closed_false_positive | confirmed_fraud
    ml_score        DECIMAL(5,4),
    rule_score      DECIMAL(5,4),
    created_at      TIMESTAMP     DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_txn
    ON gold.alerts_log(transaction_id);
CREATE INDEX IF NOT EXISTS idx_alerts_status
    ON gold.alerts_log(status)
    WHERE status = 'open';
CREATE INDEX IF NOT EXISTS idx_alerts_timestamp
    ON gold.alerts_log(alert_timestamp DESC);

-- ── staging tables (Spark → Postgres staging) ─────────────────────
CREATE TABLE IF NOT EXISTS staging.dim_users_staging (
    user_id       UUID,
    country_code  CHAR(2),
    email_hash    VARCHAR(64),
    risk_tier     VARCHAR(10),
    loaded_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.dim_merchants_staging (
    merchant_id   UUID,
    merchant_name VARCHAR(255),
    mcc_code      CHAR(4),
    country_code  CHAR(2),
    loaded_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.fact_transactions_staging (
    transaction_id   UUID,
    user_id          UUID,
    merchant_id      UUID,
    time_sk          INT,
    amount           DECIMAL(18,2),
    currency         CHAR(3),
    transaction_type VARCHAR(20),
    status           VARCHAR(20),
    payment_method   VARCHAR(20),
    is_flagged_fraud BOOLEAN DEFAULT FALSE,
    fraud_score      DECIMAL(5,4) DEFAULT 0.0,
    latitude         DECIMAL(9,6),
    longitude        DECIMAL(9,6),
    country_code     CHAR(2),
    loaded_at        TIMESTAMP DEFAULT NOW()
);

-- Grant SELECT on all gold tables to readonly role
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO fintech_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO fintech_writer;
-- Grant future tables automatically
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO fintech_readonly;
