-- assert_user_uniqueness.sql
-- Data quality gate: FAILS if any user_id has more than one current SCD2 record.
-- Guards against SCD2 merge bugs.

SELECT
    user_id,
    COUNT(*) AS current_records
FROM {{ source('gold_raw', 'dim_users') }}
WHERE is_current = TRUE
GROUP BY user_id
HAVING COUNT(*) > 1
