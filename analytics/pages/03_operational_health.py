"""Operational Health Page — Pipeline status, data freshness, job SLAs."""

from __future__ import annotations
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

from analytics.utils.db_connection import run_query

st.title("⚙️ Operational Health")
st.caption("Pipeline SLA monitoring and data freshness indicators")


# ── Queries ───────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def get_layer_row_counts() -> "pd.DataFrame":
    """Row counts across pipeline layers to verify data flow."""
    return run_query("""
        SELECT
            'fact_transactions' AS layer,
            COUNT(*)            AS row_count,
            MAX(created_at)     AS last_updated
        FROM gold.fact_transactions
        UNION ALL
        SELECT
            'alerts_log',
            COUNT(*),
            MAX(created_at)
        FROM gold.alerts_log
        UNION ALL
        SELECT
            'dim_users',
            COUNT(*),
            MAX(valid_from)
        FROM gold.dim_users
        WHERE is_current = TRUE
        UNION ALL
        SELECT
            'dim_merchants',
            COUNT(*),
            MAX(created_at)
        FROM gold.dim_merchants
    """)


@st.cache_data(ttl=60)
def get_rejection_rate() -> "pd.DataFrame":
    """Daily rejection rate proxy: % of rows not in gold vs staging."""
    return run_query("""
        SELECT
            DATE(loaded_at)            AS load_date,
            COUNT(*)                   AS staged_rows,
            COUNT(ft.transaction_sk)   AS gold_rows,
            ROUND(
                100.0 * (COUNT(*) - COUNT(ft.transaction_sk)) / NULLIF(COUNT(*), 0),
                2
            )                          AS rejection_pct
        FROM staging.fact_transactions_staging stg
        LEFT JOIN gold.fact_transactions ft
            ON stg.transaction_id::UUID = ft.transaction_id
        WHERE stg.loaded_at >= NOW() - INTERVAL '7 days'
        GROUP BY DATE(loaded_at)
        ORDER BY load_date DESC
    """)


@st.cache_data(ttl=60)
def get_fraud_rate_today() -> "pd.DataFrame":
    return run_query("""
        SELECT
            ROUND(
                100.0 * COUNT(*) FILTER (WHERE is_flagged_fraud)
                / NULLIF(COUNT(*), 0), 4
            ) AS fraud_rate_today
        FROM gold.fact_transactions
        WHERE created_at >= NOW() - INTERVAL '1 day'
    """)


# ── Global Health Banner ──────────────────────────────────────────────
counts_df = get_layer_row_counts()
if not counts_df.empty:
    st.success("🟢 **System Health: Optimal** | Pipeline services are operational and data is flowing.")
else:
    st.error("🔴 **System Health: Critical** | No data detected in Gold layer. Pipeline may be down.")

st.markdown("---")

# ── Data Freshness & Volume Cards ─────────────────────────────────────
st.subheader("📋 Layer Data Freshness")
if not counts_df.empty:
    cols = st.columns(4)
    for i, row in counts_df.iterrows():
        layer_name = row['layer'].replace('_', ' ').title()
        row_count = int(row['row_count'])
        last_updated = row['last_updated']
        
        # Format the time safely
        time_str = "Never"
        if pd.notnull(last_updated):
            time_str = pd.to_datetime(last_updated).strftime("%Y-%m-%d %H:%M UTC")

        with cols[i % 4]:
            st.metric(
                label=layer_name,
                value=f"{row_count:,} rows",
                delta=f"Last Sync: {time_str}",
                delta_color="off"
            )
else:
    st.warning("⚠️ No data in Gold layer yet. Run the pipeline first.")

st.markdown("---")

# ── SLA Gauges ────────────────────────────────────────────────────────
st.subheader("🎯 Service Level Agreements (SLAs)")

fraud_today = get_fraud_rate_today()
fraud_rate = float(fraud_today["fraud_rate_today"].iloc[0] or 0) if not fraud_today.empty else 0

col1, col2, col3 = st.columns(3)

with col1:
    recent = run_query("""
        SELECT COUNT(*) AS count
        FROM gold.fact_transactions
        WHERE created_at >= NOW() - INTERVAL '1 day'
    """)
    recent_count = int(recent["count"].iloc[0] or 0) if not recent.empty else 0
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=min(recent_count / 10_000 * 100, 100),
        title={"text": "24H Ingest Volume<br><span style='font-size:0.8em;color:gray'>(% of 10K Target)</span>"},
        gauge={
            "axis": {"range": [0, 100]},
            "bar": {"color": "#3fb950"},
            "steps": [
                {"range": [0, 50],  "color": "#f78166"},
                {"range": [50, 80], "color": "#ffa657"},
                {"range": [80, 100],"color": "#3fb950"},
            ],
        },
    ))
    fig.update_layout(height=250, paper_bgcolor="rgba(0,0,0,0)", font_color="white", margin=dict(l=20, r=20, t=50, b=20))
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=fraud_rate,
        title={"text": "Global Fraud Rate<br><span style='font-size:0.8em;color:gray'>(Target < 5%)</span>"},
        delta={"reference": 5.0, "decreasing": {"color": "#3fb950"}},
        gauge={
            "axis": {"range": [0, 20]},
            "bar": {"color": "#d2a8ff"},
            "threshold": {
                "line": {"color": "#f78166", "width": 4},
                "thickness": 0.75,
                "value": 5,
            },
        },
    ))
    fig.update_layout(height=250, paper_bgcolor="rgba(0,0,0,0)", font_color="white", margin=dict(l=20, r=20, t=50, b=20))
    st.plotly_chart(fig, use_container_width=True)

with col3:
    alerts_count = run_query("""
        SELECT COUNT(*) AS open_count FROM gold.alerts_log WHERE status = 'open'
    """)
    open_alerts = int(alerts_count["open_count"].iloc[0] or 0) if not alerts_count.empty else 0
    fig = go.Figure(go.Indicator(
        mode="number+delta",
        value=open_alerts,
        title={"text": "Open Fraud Alerts<br><span style='font-size:0.8em;color:gray'>(Action Required)</span>"},
        delta={"reference": 0, "increasing": {"color": "#f78166"}},
        number={"font": {"color": "#f78166" if open_alerts > 10 else "#3fb950"}},
    ))
    fig.update_layout(height=250, paper_bgcolor="rgba(0,0,0,0)", font_color="white", margin=dict(l=20, r=20, t=50, b=20))
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ── Rejection Rate Chart ──────────────────────────────────────────────
st.subheader("📉 Pipeline Quality: Silver → Gold Rejection Rate (7 Days)")
st.caption("Measures the percentage of rows dropped during DWH modeling due to validation failures or missing constraints.")
rejection_df = get_rejection_rate()
if not rejection_df.empty:
    fig = px.bar(
        rejection_df,
        x="load_date",
        y="rejection_pct",
        labels={"rejection_pct": "Rejection Rate (%)", "load_date": "Load Date"},
        template="plotly_dark",
        color_discrete_sequence=["#ffa657"],
        text="rejection_pct"
    )
    fig.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
    fig.add_hline(y=5, line_dash="dash", line_color="#f78166", annotation_text="5% Threshold", annotation_position="top right")
    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", yaxis_range=[0, max(rejection_df['rejection_pct'].max() * 1.2 + 1, 6)])
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No staging-to-gold comparison data available yet.")
