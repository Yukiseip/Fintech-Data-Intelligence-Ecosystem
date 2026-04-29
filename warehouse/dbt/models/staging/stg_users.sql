-- stg_users.sql
-- Staging view over dim_users_staging loaded by Spark.

{{ config(materialized='view', schema='staging') }}

WITH source AS (
    SELECT * FROM {{ source('staging', 'dim_users_staging') }}
),

deduped AS (
    -- Keep the most recently loaded row per user_id
    SELECT DISTINCT ON (user_id)
        user_id::UUID            AS user_id,
        COALESCE(country_code, 'XX')   AS country_code,
        COALESCE(email_hash, '')       AS email_hash,
        COALESCE(risk_tier, 'low')     AS risk_tier,
        NOW()                          AS loaded_at
    FROM source
    ORDER BY user_id
)

SELECT * FROM deduped
