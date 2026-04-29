"""Fintech Analytics Dashboard — Main Entry Point.

Pages:
    1. Executive KPIs     (01_executive_kpis.py)
    2. Fraud Monitoring   (02_fraud_monitoring.py)
    3. Operational Health (03_operational_health.py)

Run:
    streamlit run app.py
"""

from __future__ import annotations
import os

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import text

from analytics.utils.db_connection import get_db_engine, run_query

# ── Page configuration ────────────────────────────────────────────────
st.set_page_config(
    page_title="Fintech Analytics",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* Global Typography */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

    /* Gradient Text */
    .hero-title {
        font-size: 3rem;
        font-weight: 800;
        background: -webkit-linear-gradient(45deg, #4facfe, #00f2fe);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0px;
        padding-bottom: 0px;
    }
    
    .hero-subtitle {
        font-size: 1.2rem;
        color: #a1a1aa;
        margin-top: 5px;
        margin-bottom: 30px;
    }

    /* Metric cards enhancement */
    [data-testid="metric-container"] {
        background: linear-gradient(145deg, #1e293b, #0f172a);
        border: 1px solid #334155;
        border-radius: 16px;
        padding: 24px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    [data-testid="metric-container"]:hover {
        transform: translateY(-2px);
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
        border-color: #38bdf8;
    }

    /* Feature Cards */
    .feature-card {
        background: rgba(30, 41, 59, 0.5);
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 20px;
        height: 100%;
        backdrop-filter: blur(10px);
        transition: all 0.3s ease;
    }
    .feature-card:hover {
        background: rgba(30, 41, 59, 0.8);
        border-color: #4facfe;
    }
    .feature-icon {
        font-size: 2.5rem;
        margin-bottom: 10px;
    }
    .feature-title {
        font-size: 1.2rem;
        font-weight: 600;
        color: #f8fafc;
        margin-bottom: 8px;
    }
    .feature-desc {
        color: #94a3b8;
        font-size: 0.95rem;
        line-height: 1.5;
    }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────
st.sidebar.title("🏦 Fintech Analytics")
st.sidebar.markdown("---")

try:
    engine = get_db_engine()
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    st.sidebar.success("✅ **Database Connected**", icon="🟢")
except Exception as exc:
    st.sidebar.error(f"❌ **DB Error:** {exc}", icon="🔴")

st.sidebar.markdown("---")

freshness_df = run_query("""
    SELECT MAX(created_at) AS last_updated
    FROM gold.fact_transactions
""")
if not freshness_df.empty and freshness_df["last_updated"].iloc[0] is not None:
    st.sidebar.markdown("### ⏱️ Data Freshness")
    st.sidebar.info(f"**Last Sync:**\n{freshness_df['last_updated'].iloc[0]}")

st.sidebar.markdown("---")
st.sidebar.markdown(
    "**Tech Stack:**\n"
    "- 🟦 **Spark** (Processing)\n"
    "- 🌀 **Airflow** (Orchestration)\n"
    "- 🐘 **PostgreSQL** (DWH)\n"
    "- 🧱 **dbt** (Transformations)\n"
)

# ── Landing page ──────────────────────────────────────────────────────
st.markdown('<p class="hero-title">Data Intelligence Platform</p>', unsafe_allow_html=True)
st.markdown('<p class="hero-subtitle">Unified observability for transactions, fraud detection, and operational health.</p>', unsafe_allow_html=True)

# Quick stats at the top
st.markdown("### 📈 Live Platform Metrics")
col1, col2, col3 = st.columns(3)

stats = run_query("""
    SELECT
        COUNT(*) AS total_txns,
        SUM(amount) FILTER (WHERE status = 'completed') AS tpv,
        COUNT(*) FILTER (WHERE is_flagged_fraud) AS fraud_count
    FROM gold.fact_transactions
""")

if not stats.empty:
    with col1:
        st.metric("📦 Total Transactions", f"{int(stats['total_txns'].iloc[0]):,}", delta="All-Time", delta_color="off")
    with col2:
        tpv = stats['tpv'].iloc[0] or 0
        st.metric("💵 Total Payment Volume", f"${tpv:,.2f}", delta="Processed", delta_color="normal")
    with col3:
        fraud = int(stats['fraud_count'].iloc[0] or 0)
        st.metric("🚨 Flagged Fraud Alerts", f"{fraud:,}", delta="Requires Review", delta_color="inverse")

st.markdown("---")

# Feature Cards
st.markdown("### 🧭 Platform Modules")
st.markdown("Select a module from the **sidebar** to dive deeper into the analytics.")

fc1, fc2, fc3 = st.columns(3)

with fc1:
    st.markdown("""
    <div class="feature-card">
        <div class="feature-icon">📊</div>
        <div class="feature-title">Executive KPIs</div>
        <div class="feature-desc">High-level insights into business performance. Track Total Payment Volume (TPV), global fraud rates, and transaction volume trends across dimensions.</div>
    </div>
    """, unsafe_allow_html=True)

with fc2:
    st.markdown("""
    <div class="feature-card">
        <div class="feature-icon">🚨</div>
        <div class="feature-title">Fraud Monitoring</div>
        <div class="feature-desc">Real-time surveillance of flagged transactions. Analyze geographic hotspots, severity distributions, and respond to potential threats.</div>
    </div>
    """, unsafe_allow_html=True)

with fc3:
    st.markdown("""
    <div class="feature-card">
        <div class="feature-icon">⚙️</div>
        <div class="feature-title">Operational Health</div>
        <div class="feature-desc">Medallion architecture observability. Ensure data freshness, track pipeline SLAs, and monitor overall system integration health.</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# Mini sparklines or charts on the home page
try:
    trend_df = run_query("""
        SELECT DATE(created_at) AS date, SUM(amount) AS daily_amount
        FROM gold.fact_transactions
        WHERE status = 'completed' AND created_at >= NOW() - INTERVAL '14 days'
        GROUP BY DATE(created_at)
        ORDER BY date
    """)
    if not trend_df.empty:
        st.markdown("### 🌊 14-Day Volume Trend")
        fig = px.area(
            trend_df, x='date', y='daily_amount',
            color_discrete_sequence=['#4facfe']
        )
        fig.update_layout(
            margin=dict(l=0, r=0, t=10, b=0),
            height=200,
            xaxis_title="",
            yaxis_title="",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=True, gridcolor="#334155")
        )
        st.plotly_chart(fig, use_container_width=True)
except Exception as e:
    pass
