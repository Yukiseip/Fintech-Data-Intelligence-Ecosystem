-- mart_fraud_summary.sql
-- Risk mart: daily fraud summary by rule type, severity bucket, and country.
-- Powers the Fraud Monitoring page in Streamlit.

{{ config(materialized='table', schema='gold') }}

WITH alerts AS (
    SELECT
        DATE(alert_timestamp)               AS alert_date,
        reason_code,
        CASE
            WHEN severity_score >= 0.8 THEN 'critical'
            WHEN severity_score >= 0.5 THEN 'high'
            WHEN severity_score >= 0.2 THEN 'medium'
            ELSE                            'low'
        END                                 AS severity_bucket,
        status,
        severity_score,
        transaction_id
    FROM {{ source('gold_raw', 'alerts_log') }}
),

summary AS (
    SELECT
        alert_date,
        reason_code,
        severity_bucket,
        status,
        COUNT(*)                            AS alert_count,
        AVG(severity_score)                 AS avg_severity,
        MAX(severity_score)                 AS max_severity,
        COUNT(DISTINCT transaction_id)      AS unique_transactions
    FROM alerts
    GROUP BY
        alert_date,
        reason_code,
        severity_bucket,
        status
)

SELECT
    *,
    NOW()                                   AS dbt_updated_at
FROM summary
ORDER BY alert_date DESC, alert_count DESC
