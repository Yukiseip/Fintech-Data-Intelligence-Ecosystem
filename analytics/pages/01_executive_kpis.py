"""Executive KPIs Page — 30-day TPV, fraud rate, transaction volume."""

from __future__ import annotations
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from analytics.utils.db_connection import run_query

st.title("📊 Executive KPIs")
st.caption("Last 30 days | Refreshes every 5 minutes")

# ── KPI Row ───────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_kpis():
    """Fetch headline KPIs from precomputed mart."""
    return run_query("""
        SELECT
            COALESCE(SUM(daily_tpv), 0)          AS tpv,
            COALESCE(SUM(total_transactions), 0)  AS total_txns,
            COALESCE(AVG(avg_txn_value), 0)       AS avg_txn_value,
            COALESCE(AVG(fraud_rate_pct), 0)      AS fraud_rate_pct
        FROM gold.mart_executive_kpis
        WHERE txn_date >= NOW() - INTERVAL '30 days'
    """)


@st.cache_data(ttl=300)
def get_tpv_trend():
    """Fetch daily TPV trend for line chart."""
    return run_query("""
        SELECT txn_date AS date, daily_tpv
        FROM gold.mart_executive_kpis
        WHERE txn_date >= NOW() - INTERVAL '30 days'
        ORDER BY txn_date
    """)


@st.cache_data(ttl=300)
def get_txn_by_method():
    """Fetch transaction volume split by payment method."""
    return run_query("""
        SELECT payment_method, COUNT(*) AS count
        FROM gold.fact_transactions
        WHERE time_sk >= CAST(TO_CHAR(NOW() - INTERVAL '30 days', 'YYYYMMDD') AS INT)
        GROUP BY payment_method
        ORDER BY count DESC
    """)


@st.cache_data(ttl=300)
def get_txn_by_type():
    """Fetch transaction split by type."""
    return run_query("""
        SELECT transaction_type, COUNT(*) AS count
        FROM gold.fact_transactions
        WHERE time_sk >= CAST(TO_CHAR(NOW() - INTERVAL '30 days', 'YYYYMMDD') AS INT)
        GROUP BY transaction_type
        ORDER BY count DESC
    """)


# ── Render KPI metrics ────────────────────────────────────────────────
kpis = get_kpis()
if not kpis.empty:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("💵 TPV (30d)",       f"${kpis['tpv'].iloc[0]:,.2f}")
    col2.metric("🔢 Transactions",    f"{int(kpis['total_txns'].iloc[0]):,}")
    col3.metric("📈 Avg Value",       f"${kpis['avg_txn_value'].iloc[0]:,.2f}")
    col4.metric("🚨 Fraud Rate",      f"{kpis['fraud_rate_pct'].iloc[0]:.2f}%")

st.markdown("---")

# ── TPV Trend Chart ───────────────────────────────────────────────────
col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("📈 TPV Trend (Last 30 Days)")
    trend_df = get_tpv_trend()
    if not trend_df.empty:
        fig = px.area(
            trend_df,
            x="date",
            y="daily_tpv",
            labels={"daily_tpv": "Daily TPV ($)", "date": "Date"},
            color_discrete_sequence=["#58a6ff"],
            template="plotly_dark",
        )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No TPV data available yet. Run `make generate` first.")

with col_right:
    st.subheader("💳 Volume by Method")
    method_df = get_txn_by_method()
    if not method_df.empty:
        fig = px.pie(
            method_df,
            values="count",
            names="payment_method",
            hole=0.5,
            color_discrete_sequence=px.colors.sequential.Blues_r,
            template="plotly_dark",
        )
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=0, r=0, t=10, b=0),
            showlegend=True,
        )
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ── Transaction Type Breakdown ────────────────────────────────────────
st.subheader("📋 Transactions by Type")
type_df = get_txn_by_type()
if not type_df.empty:
    fig = px.bar(
        type_df,
        x="transaction_type",
        y="count",
        color="transaction_type",
        labels={"count": "Count", "transaction_type": "Type"},
        template="plotly_dark",
        color_discrete_sequence=["#58a6ff", "#3fb950", "#f78166", "#d2a8ff"],
    )
    fig.update_layout(
        showlegend=False,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=10, b=0),
    )
    st.plotly_chart(fig, use_container_width=True)
