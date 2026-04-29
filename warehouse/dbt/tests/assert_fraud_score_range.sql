-- assert_fraud_score_range.sql
-- Data quality gate: FAILS if any fraud_score is outside [0, 1].

SELECT *
FROM {{ ref('fct_transactions') }}
WHERE fraud_score < 0 OR fraud_score > 1
