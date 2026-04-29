-- stg_transactions.sql
-- Staging view on top of the Spark-written staging table.
-- Applies minimal transformations: column renaming, type coercions.

{{ config(materialized='view', schema='staging') }}

WITH source AS (
    SELECT * FROM {{ source('staging', 'fact_transactions_staging') }}
),

renamed AS (
    SELECT DISTINCT ON (transaction_id)
        -- Natural keys
        transaction_id::UUID                            AS transaction_id,
        user_id::UUID                                   AS user_id,
        merchant_id::UUID                               AS merchant_id,

        -- Metrics
        amount,
        currency,
        transaction_type,
        status,
        payment_method,
        COALESCE(is_flagged_fraud, FALSE)               AS is_flagged_fraud,
        COALESCE(CAST(fraud_score AS DECIMAL(5,4)), 0)  AS fraud_score,

        -- Geography
        latitude,
        longitude,
        country_code,

        -- Time key for dim_time join
        time_sk,

        -- Metadata
        NOW()                                           AS _silver_processed_at

    FROM source
    ORDER BY transaction_id
)

SELECT * FROM renamed
