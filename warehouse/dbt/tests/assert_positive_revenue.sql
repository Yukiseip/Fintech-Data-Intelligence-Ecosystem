-- assert_positive_revenue.sql
-- Data quality gate: FAILS if any completed transaction has amount <= 0.
-- A non-empty result = test failure = pipeline blocked.

SELECT *
FROM {{ ref('fct_transactions') }}
WHERE amount <= 0
