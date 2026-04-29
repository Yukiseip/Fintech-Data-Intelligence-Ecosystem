-- assert_no_orphan_facts.sql
-- Data quality gate: FAILS if any fact_transaction references a user_sk
-- that doesn't exist in dim_users.
-- Enforces referential integrity at the dbt layer.

SELECT ft.*
FROM {{ ref('fct_transactions') }} ft
LEFT JOIN {{ source('gold_raw', 'dim_users') }} du
    ON ft.user_sk = du.user_sk
WHERE du.user_sk IS NULL
