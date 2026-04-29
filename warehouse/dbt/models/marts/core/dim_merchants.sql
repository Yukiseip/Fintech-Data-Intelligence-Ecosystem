-- dim_merchants.sql
-- Gold dimension: merchants with surrogate key lookup.

{{ config(materialized='table', schema='gold') }}

WITH staging AS (
    SELECT * FROM {{ ref('stg_merchants') }}
)

SELECT
    MD5(s.merchant_id::TEXT) AS merchant_sk,
    s.merchant_id,
    s.merchant_name,
    s.mcc_code,
    s.country_code,
    0.0 AS risk_score,
    NOW() AS created_at
FROM staging s
