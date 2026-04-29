-- stg_merchants.sql
-- Staging view over dim_merchants_staging loaded by Spark.

{{ config(materialized='view', schema='staging') }}

WITH source AS (
    SELECT * FROM {{ source('staging', 'dim_merchants_staging') }}
),

deduped AS (
    SELECT DISTINCT ON (merchant_id)
        merchant_id::UUID        AS merchant_id,
        'Unknown'::VARCHAR       AS merchant_name,
        mcc_code,
        country_code,
        NOW()                    AS loaded_at
    FROM source
    ORDER BY merchant_id
)

SELECT * FROM deduped
