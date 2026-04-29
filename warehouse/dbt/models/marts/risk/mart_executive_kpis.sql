-- mart_executive_kpis.sql
-- Executive KPI mart: precomputed daily aggregates for the dashboard.
-- Reduces Streamlit query time below the 2s p95 SLA.

{{ config(materialized='table', schema='gold') }}

WITH daily_txns AS (
    SELECT
        dt.date                                                         AS txn_date,
        COUNT(ft.transaction_id)                                        AS total_transactions,
        COUNT(ft.transaction_id) FILTER (WHERE ft.status = 'completed') AS completed_transactions,
        SUM(ft.amount) FILTER (WHERE ft.status = 'completed')           AS daily_tpv,
        AVG(ft.amount) FILTER (WHERE ft.status = 'completed')           AS avg_txn_value,
        COUNT(*) FILTER (WHERE ft.is_flagged_fraud)                     AS fraud_alerts,
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE ft.is_flagged_fraud)
            / NULLIF(COUNT(*), 0),
            4
        )                                                               AS fraud_rate_pct
    FROM {{ ref('fct_transactions') }} ft
    JOIN {{ source('gold_raw', 'dim_time') }} dt ON ft.time_sk = dt.time_sk
    GROUP BY dt.date
),

rolling AS (
    SELECT
        *,
        SUM(daily_tpv) OVER (
            ORDER BY txn_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )                                                               AS tpv_30d,
        AVG(fraud_rate_pct) OVER (
            ORDER BY txn_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )                                                               AS fraud_rate_7d_avg
    FROM daily_txns
)

SELECT
    *,
    NOW()                                                               AS dbt_updated_at
FROM rolling
ORDER BY txn_date DESC
