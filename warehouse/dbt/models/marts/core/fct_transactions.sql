-- fct_transactions.sql
-- Final Gold fact table joining staging with dimension surrogate keys.
-- Incremental: only processes new Silver records not yet in Gold.

{{
  config(
    materialized='incremental',
    unique_key='transaction_id',
    on_schema_change='fail',
    schema='gold',
    alias='fact_transactions'
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
        ON s.user_id::TEXT = u.user_id::TEXT
        AND u.is_current = TRUE
),

with_merchant_sk AS (
    SELECT
        u.*,
        m.merchant_sk
    FROM with_user_sk u
    LEFT JOIN {{ ref('dim_merchants') }} m
        ON u.merchant_id::TEXT = m.merchant_id::TEXT
),

with_time_sk AS (
    SELECT
        m.*,
        CAST(TO_CHAR(CAST(m._silver_processed_at AS DATE), 'YYYYMMDD') AS INT) AS derived_time_sk
    FROM with_merchant_sk m
)

SELECT
    transaction_id,
    user_sk,
    merchant_sk,
    COALESCE(time_sk, derived_time_sk)   AS time_sk,
    amount,
    currency,
    transaction_type,
    status,
    payment_method,
    COALESCE(is_flagged_fraud, FALSE)     AS is_flagged_fraud,
    COALESCE(fraud_score, 0.0)            AS fraud_score,
    latitude,
    longitude,
    country_code,
    NOW()                                 AS created_at
FROM with_time_sk
WHERE user_sk IS NOT NULL
  AND merchant_sk IS NOT NULL
