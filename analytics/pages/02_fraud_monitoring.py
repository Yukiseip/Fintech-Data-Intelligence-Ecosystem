"""Fraud Monitoring Page — Real-time alerts, heatmaps, severity distribution."""

from __future__ import annotations
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from analytics.utils.db_connection import run_query

st.title("🚨 Fraud Monitoring")
st.caption("Live fraud alerts and pattern analysis | Refreshes every 2 minutes")

# ── Sidebar filters ───────────────────────────────────────────────────
st.sidebar.markdown("### Filters")
lookback = st.sidebar.selectbox("Lookback Period", ["7 days", "30 days", "90 days"], index=0)
days_map = {"7 days": 7, "30 days": 30, "90 days": 90}
days = days_map[lookback]

status_filter = st.sidebar.multiselect(
    "Alert Status",
    ["open", "investigating", "closed_false_positive", "confirmed_fraud"],
    default=["open", "investigating"],
)
status_in = tuple(status_filter) if status_filter else ("open",)


# ── KPIs ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=120)
def get_alert_kpis(days: int, statuses: tuple) -> "pd.DataFrame":
    status_list = "('" + "', '".join(statuses) + "')"
    return run_query(
        f"""
        SELECT
            COUNT(*)                                        AS total_alerts,
            COUNT(*) FILTER (WHERE status = 'open')        AS open_alerts,
            ROUND(AVG(severity_score)::NUMERIC, 3)         AS avg_severity,
            COUNT(DISTINCT transaction_id)                 AS unique_txns
        FROM gold.alerts_log
        WHERE alert_timestamp >= NOW() - INTERVAL '{days} days'
          AND status IN {status_list}
        """
    )


@st.cache_data(ttl=120)
def get_alerts_by_rule(days: int) -> "pd.DataFrame":
    return run_query(
        f"""
        SELECT reason_code, COUNT(*) AS count, AVG(severity_score) AS avg_severity
        FROM gold.alerts_log
        WHERE alert_timestamp >= NOW() - INTERVAL '{days} days'
        GROUP BY reason_code
        ORDER BY count DESC
        """
    )


@st.cache_data(ttl=120)
def get_alert_timeline(days: int) -> "pd.DataFrame":
    return run_query(
        f"""
        SELECT
            DATE(alert_timestamp) AS alert_date,
            reason_code,
            COUNT(*) AS alert_count
        FROM gold.alerts_log
        WHERE alert_timestamp >= NOW() - INTERVAL '{days} days'
        GROUP BY DATE(alert_timestamp), reason_code
        ORDER BY alert_date
        """
    )


@st.cache_data(ttl=120)
def get_severity_distribution(days: int) -> "pd.DataFrame":
    return run_query(
        f"""
        SELECT
            CASE
                WHEN severity_score >= 0.8 THEN 'Critical (0.8-1.0)'
                WHEN severity_score >= 0.5 THEN 'High (0.5-0.8)'
                WHEN severity_score >= 0.2 THEN 'Medium (0.2-0.5)'
                ELSE                            'Low (0-0.2)'
            END AS severity_bucket,
            COUNT(*) AS count
        FROM gold.alerts_log
        WHERE alert_timestamp >= NOW() - INTERVAL '{days} days'
        GROUP BY 1
        ORDER BY count DESC
        """
    )


@st.cache_data(ttl=120)
def get_recent_alerts(days: int, limit: int = 50) -> "pd.DataFrame":
    return run_query(
        f"""
        SELECT
            alert_timestamp,
            transaction_id,
            reason_code,
            ROUND(severity_score::NUMERIC, 3) AS severity_score,
            description,
            status
        FROM gold.alerts_log
        WHERE alert_timestamp >= NOW() - INTERVAL '{days} days'
        ORDER BY alert_timestamp DESC
        LIMIT {limit}
        """
    )


# ── Alert KPI Row ─────────────────────────────────────────────────────
kpis = get_alert_kpis(days, status_in)
if not kpis.empty:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🚨 Total Alerts",       f"{int(kpis['total_alerts'].iloc[0]):,}")
    c2.metric("🔴 Open Alerts",        f"{int(kpis['open_alerts'].iloc[0]):,}")
    c3.metric("⚡ Avg Severity",       f"{kpis['avg_severity'].iloc[0]:.3f}")
    c4.metric("📋 Unique Transactions", f"{int(kpis['unique_txns'].iloc[0]):,}")

st.markdown("---")

# ── Charts Row ────────────────────────────────────────────────────────
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("📊 Alerts by Rule Type")
    rules_df = get_alerts_by_rule(days)
    if not rules_df.empty:
        color_map = {
            "VELOCITY_ATTACK": "#f78166",
            "HIGH_AMOUNT":     "#d2a8ff",
            "GEO_IMPOSSIBLE":  "#58a6ff",
        }
        fig = px.bar(
            rules_df,
            x="reason_code",
            y="count",
            color="reason_code",
            color_discrete_map=color_map,
            labels={"count": "Alert Count", "reason_code": "Rule"},
            template="plotly_dark",
        )
        fig.update_layout(
            showlegend=False,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No alerts yet.")

with col_b:
    st.subheader("🎯 Severity Distribution")
    severity_df = get_severity_distribution(days)
    if not severity_df.empty:
        fig = px.pie(
            severity_df,
            names="severity_bucket",
            values="count",
            hole=0.45,
            color_discrete_sequence=["#f78166", "#d2a8ff", "#ffa657", "#3fb950"],
            template="plotly_dark",
        )
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ── Timeline Chart ────────────────────────────────────────────────────
st.subheader(f"📅 Alert Timeline (Last {days} Days)")
timeline_df = get_alert_timeline(days)
if not timeline_df.empty:
    fig = px.line(
        timeline_df,
        x="alert_date",
        y="alert_count",
        color="reason_code",
        markers=True,
        labels={"alert_count": "Alerts", "alert_date": "Date", "reason_code": "Rule"},
        template="plotly_dark",
        color_discrete_sequence=["#f78166", "#d2a8ff", "#58a6ff"],
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ── Recent Alerts Table ───────────────────────────────────────────────
st.subheader("🔍 Recent Alerts")
alerts_df = get_recent_alerts(days)
if not alerts_df.empty:
    st.dataframe(
        alerts_df,
        use_container_width=True,
        column_config={
            "severity_score": st.column_config.ProgressColumn(
                "Severity", min_value=0.0, max_value=1.0, format="%.3f"
            ),
            "alert_timestamp": st.column_config.DatetimeColumn("Timestamp"),
            "transaction_id":  st.column_config.TextColumn("Transaction ID"),
        },
    )
else:
    st.info("No alerts found for the selected filters.")
