-- SCD Type 2 snapshot for dim_users.
-- Tracks historical changes in: risk_tier, country_code, email_hash.
-- Uses dbt's 'check' strategy: if any check_col changes, a new record is inserted
-- with valid_from = NOW() and the old record gets valid_to = NOW().

{% snapshot users_snapshot %}

{{
    config(
      target_schema='gold',
      unique_key='user_id',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True
    )
}}

SELECT
    user_id,
    -- Hash email at dbt layer (staging already has email_hash from Spark)
    COALESCE(email_hash, MD5(user_id::TEXT))    AS email_hash,
    COALESCE(risk_tier, 'low')                  AS risk_tier,
    country_code,
    NOW() AS updated_at
FROM {{ source('staging', 'dim_users_staging') }}

{% endsnapshot %}
