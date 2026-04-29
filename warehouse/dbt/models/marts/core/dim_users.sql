-- dim_users.sql
-- Gold dimension: users with current record marker.
-- SCD2 history is managed by users_snapshot (see snapshots/).
-- This view surfaces only current records for self-service BI.

{{ config(materialized='table', schema='gold') }}

SELECT
    MD5(user_id::TEXT) AS user_sk,
    user_id,
    email_hash,
    NOW() AS registration_date,
    risk_tier,
    country_code,
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to,
    CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current,
    dbt_scd_id
FROM {{ ref('users_snapshot') }}
WHERE dbt_valid_to IS NULL
