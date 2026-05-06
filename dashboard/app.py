"""
dashboard/app.py
─────────────────
SCG Carbon Footprint Tracking — Net Zero KPI Dashboard

Sections:
  1. KPI summary  — current intensity, progress %, status
  2. Net Zero progress — trend vs 2030/2050 targets
  3. Scope breakdown — Scope 1 / 2 / 3 comparison
  4. Benchmark gap — vs IEA Global / ASEAN / Thailand
  5. Pipeline log — ดู DAG run history
"""

import os
import psycopg2
import psycopg2.extras
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
from datetime import datetime
import pytz

# ─── Page config ──────────────────────────────────────────────
st.set_page_config(
    page_title="SCG Carbon Footprint",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded",
)

TZ_BKK = pytz.timezone("Asia/Bangkok")

DB_CONFIG = {
    "host":     os.getenv("CARBON_DB_HOST",   "localhost"),
    "port":     int(os.getenv("CARBON_DB_PORT", "5432")),
    "dbname":   os.getenv("CARBON_DB_NAME",   "carbondb"),
    "user":     os.getenv("CARBON_DB_USER"),
    "password": os.getenv("CARBON_DB_PASS"),
}

# SCG Net Zero targets
TARGET_2030 = 650   # kg CO2/t-cement
TARGET_2050 = 0
BASELINE    = 800   # 2020 baseline

# ─── DB helpers ───────────────────────────────────────────────
def query(sql: str, params=None) -> pd.DataFrame:
    """สร้าง connection ใหม่ทุกครั้งเพื่อหลีกเลี่ยง aborted transaction"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or ())
            df = pd.DataFrame(cur.fetchall())
        conn.close()
        return df
    except Exception as e:
        st.error(f"DB error: {e}")
        return pd.DataFrame()

# ─── SQL queries ──────────────────────────────────────────────
SQL_LATEST = """
    SELECT * FROM public_marts.mart_co2_intensity
    ORDER BY reading_date DESC LIMIT 1
"""

SQL_TREND = """
    SELECT
        reading_date,
        scope1_kg_per_t,
        scope1_7d_avg,
        scope1_30d_avg,
        total_co2_kg_per_t
    FROM public_marts.mart_co2_intensity
    WHERE reading_date >= CURRENT_DATE - INTERVAL %s
    ORDER BY reading_date ASC
"""

SQL_PROGRESS = """
    SELECT
        reading_date,
        current_intensity,
        target_2030,
        target_2050,
        progress_pct_to_2030,
        progress_pct_to_2050,
        gap_vs_2030_target,
        status_2030,
        required_annual_reduction
    FROM public_marts.mart_net_zero_progress
    WHERE reading_date >= CURRENT_DATE - INTERVAL %s
    ORDER BY reading_date ASC
"""

SQL_BENCHMARK = """
    SELECT
        reading_date,
        scg_intensity,
        global_avg,
        asean_avg,
        scg_2030_target,
        gap_vs_global,
        gap_vs_asean,
        industry_position
    FROM public_marts.mart_benchmark_gap
    WHERE reading_date >= CURRENT_DATE - INTERVAL %s
    ORDER BY reading_date ASC
"""

SQL_SCOPE_BREAKDOWN = """
    SELECT
        reading_date,
        scope1_kg_per_t,
        scope2_kg_per_t,
        scope3_kg_per_t,
        total_co2_kg_per_t
    FROM public_marts.mart_co2_intensity
    WHERE reading_date >= CURRENT_DATE - INTERVAL %s
    ORDER BY reading_date ASC
"""

SQL_PIPELINE_LOG = """
    SELECT
        run_at AT TIME ZONE 'Asia/Bangkok' AS run_at,
        dag_id, source, rows_loaded, status, message
    FROM public.pipeline_log
    ORDER BY run_at DESC
    LIMIT 20
"""

# ─── Chart helpers ────────────────────────────────────────────
def intensity_gauge(value: float, title: str = "CO2 intensity") -> go.Figure:
    color = "green"
    if value > TARGET_2030 * 1.05:
        color = "red"
    elif value > TARGET_2030:
        color = "orange"

    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=value,
        delta={"reference": TARGET_2030, "valueformat": ".1f",
               "suffix": " kg/t"},
        title={"text": f"{title}<br><sub>kg CO₂/t-cement</sub>"},
        gauge={
            "axis": {"range": [0, BASELINE * 1.2]},
            "bar":  {"color": color},
            "steps": [
                {"range": [0, TARGET_2030],     "color": "lightgreen"},
                {"range": [TARGET_2030, BASELINE], "color": "lightyellow"},
                {"range": [BASELINE, BASELINE * 1.2], "color": "lightsalmon"},
            ],
            "threshold": {
                "line":      {"color": "red", "width": 3},
                "thickness": 0.8,
                "value":     TARGET_2030,
            },
        },
        number={"suffix": " kg/t", "font": {"size": 24}},
    ))
    fig.update_layout(height=200, margin=dict(l=10, r=10, t=30, b=0))
    return fig


def trend_chart(df: pd.DataFrame, window: str) -> go.Figure:
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df["reading_date"], y=df["scope1_kg_per_t"],
        name="Daily", mode="lines",
        line=dict(color="#b4b2a9", width=1),
        opacity=0.6,
    ))
    fig.add_trace(go.Scatter(
        x=df["reading_date"], y=df["scope1_7d_avg"],
        name="7-day avg", mode="lines",
        line=dict(color="#1D9E75", width=2),
    ))
    fig.add_trace(go.Scatter(
        x=df["reading_date"], y=df["scope1_30d_avg"],
        name="30-day avg", mode="lines",
        line=dict(color="#534AB7", width=2),
    ))

    # Target lines
    if not df.empty:
        x_range = [df["reading_date"].min(), df["reading_date"].max()]
        fig.add_hline(y=TARGET_2030, line_dash="dash", line_color="red",
                      annotation_text=f"2030 target ({TARGET_2030})")
        fig.add_hline(y=BASELINE, line_dash="dot", line_color="gray",
                      annotation_text=f"2020 baseline ({BASELINE})")

    fig.update_layout(
        height=320,
        title="CO₂ intensity trend",
        yaxis_title="kg CO₂/t-cement",
        legend=dict(orientation="h", y=-0.15),
        margin=dict(l=0, r=0, t=40, b=0),
    )
    return fig


def scope_area_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df["reading_date"], y=df["scope1_kg_per_t"],
        name="Scope 1 (direct)",
        fill="tozeroy", mode="lines",
        line=dict(color="#D85A30"),
        fillcolor="rgba(216, 90, 48, 0.3)",
    ))
    fig.add_trace(go.Scatter(
        x=df["reading_date"], y=df["scope2_kg_per_t"],
        name="Scope 2 (electricity)",
        fill="tozeroy", mode="lines",
        line=dict(color="#534AB7"),
        fillcolor="rgba(83, 74, 183, 0.2)",
    ))
    fig.add_trace(go.Scatter(
        x=df["reading_date"], y=df["scope3_kg_per_t"],
        name="Scope 3 (value chain)",
        fill="tozeroy", mode="lines",
        line=dict(color="#1D9E75"),
        fillcolor="rgba(29, 158, 117, 0.2)",
    ))

    fig.update_layout(
        height=300,
        title="Scope 1 / 2 / 3 breakdown",
        yaxis_title="kg CO₂/t-cement",
        legend=dict(orientation="h", y=-0.2),
        margin=dict(l=0, r=0, t=40, b=0),
    )
    return fig


def benchmark_chart(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return go.Figure()

    latest = df.iloc[-1]
    categories = ["SCG (current)", "ASEAN avg", "Global avg", "SCG 2030 target"]
    values = [
        latest["scg_intensity"],
        latest["asean_avg"],
        latest["global_avg"],
        latest["scg_2030_target"],
    ]
    colors = ["#D85A30", "#534AB7", "#1D9E75", "#BA7517"]

    fig = go.Figure(go.Bar(
        x=categories, y=values,
        marker_color=colors,
        text=[f"{v:.0f}" for v in values],
        textposition="outside",
    ))
    fig.add_hline(y=TARGET_2030, line_dash="dash", line_color="red",
                  annotation_text=f"SCG 2030 target ({TARGET_2030})")
    fig.update_layout(
        height=320,
        title="CO₂ intensity vs industry benchmarks",
        yaxis_title="kg CO₂/t-cement",
        showlegend=False,
        margin=dict(l=0, r=0, t=40, b=0),
    )
    return fig


# ─── Sidebar ──────────────────────────────────────────────────
with st.sidebar:
    st.title("🌿 SCG Carbon")
    st.caption("Net Zero 2050 Tracker")
    st.divider()

    window_label = st.selectbox(
        "Time window",
        ["30 days", "90 days", "180 days", "1 year"],
        index=1,
    )
    window_map = {
        "30 days":  "30 days",
        "90 days":  "90 days",
        "180 days": "180 days",
        "1 year":   "365 days",
    }
    window = window_map[window_label]

    st.divider()
    st.caption("Targets")
    st.metric("2030 target", f"{TARGET_2030} kg/t")
    st.metric("2050 target", "Net Zero")
    st.metric("2020 baseline", f"{BASELINE} kg/t")
    st.divider()
    st.caption(f"Updated: {datetime.now(TZ_BKK).strftime('%H:%M:%S')} (BKK)")

# ─── Main dashboard ───────────────────────────────────────────
st.title("🌿 SCG Cement — Carbon Footprint Tracking")
st.caption(f"Plant: SCG_RAYONG_01 | Net Zero 2050 | {window_label}")

# ── KPI row ──────────────────────────────────────────────────
latest_df = query(SQL_LATEST)

if not latest_df.empty:
    r = latest_df.iloc[0]
    current = float(r.get("scope1_kg_per_t") or 0)
    progress = float(r.get("scope1_30d_avg") or current)
    total    = float(r.get("total_co2_kg_per_t") or 0)
    anomalies = int(r.get("anomaly_count") or 0)

    k1, k2, k3, k4, k5 = st.columns(5)

    k1.metric(
        "CO₂ intensity (Scope 1)",
        f"{current:.1f} kg/t",
        delta=f"{current - TARGET_2030:+.1f} vs 2030 target",
        delta_color="inverse",
    )
    progress_pct = max(0, (BASELINE - current) / (BASELINE - TARGET_2030) * 100)
    k2.metric(
        "Progress to 2030",
        f"{progress_pct:.1f}%",
        help=f"Target: {TARGET_2030} kg/t | Baseline: {BASELINE} kg/t",
    )
    k3.metric(
        "Total (Scope 1+2+3)",
        f"{total:.1f} kg/t",
    )
    k4.metric(
        "Gap vs 2030 target",
        f"{current - TARGET_2030:+.1f} kg/t",
        delta_color="inverse",
    )
    k5.metric(
        "Anomalies today",
        f"{anomalies}",
        delta_color="inverse" if anomalies > 0 else "normal",
    )
else:
    st.warning("ยังไม่มีข้อมูล — รัน Airflow DAG ก่อนครับ: `ingest_sensor_readings`")

st.divider()

# ── Gauge + Trend ─────────────────────────────────────────────
col_gauge, col_trend = st.columns([1, 2])

with col_gauge:
    if not latest_df.empty:
        st.plotly_chart(
            intensity_gauge(current, "Current CO₂ intensity"),
            use_container_width=True,
        )
        status = "✅ On track" if current <= TARGET_2030 else "❌ Off track"
        st.caption(f"2030 target status: {status}")

with col_trend:
    trend_df = query(SQL_TREND, (window,))
    if not trend_df.empty:
        st.plotly_chart(trend_chart(trend_df, window), use_container_width=True)

st.divider()

# ── Scope breakdown ───────────────────────────────────────────
st.subheader("Scope 1 / 2 / 3 emissions")
scope_df = query(SQL_SCOPE_BREAKDOWN, (window,))
if not scope_df.empty:
    s1, s2 = st.columns(2)
    with s1:
        st.plotly_chart(scope_area_chart(scope_df), use_container_width=True)
    with s2:
        # Pie chart of latest scope breakdown
        if not latest_df.empty:
            s1v = float(latest_df.iloc[0].get("scope1_kg_per_t") or 0)
            s2v = float(latest_df.iloc[0].get("scope2_kg_per_t") or 0)
            s3v = float(latest_df.iloc[0].get("scope3_kg_per_t") or 0)
            fig_pie = go.Figure(go.Pie(
                labels=["Scope 1", "Scope 2", "Scope 3"],
                values=[s1v, s2v, s3v],
                marker=dict(colors=["#D85A30", "#534AB7", "#1D9E75"]),
                hole=0.4,
                textinfo="label+percent",
            ))
            fig_pie.update_layout(
                height=300,
                title="Scope breakdown (latest)",
                margin=dict(l=0, r=0, t=40, b=0),
            )
            st.plotly_chart(fig_pie, use_container_width=True)

st.divider()

# ── Benchmark comparison ──────────────────────────────────────
st.subheader("Industry benchmark comparison")
bench_df = query(SQL_BENCHMARK, (window,))
if not bench_df.empty:
    b1, b2 = st.columns(2)
    with b1:
        st.plotly_chart(benchmark_chart(bench_df), use_container_width=True)
    with b2:
        latest_b = bench_df.iloc[-1]
        pos = latest_b.get("industry_position", "unknown")
        pos_icon = {
            "top_10pct":     "🥇 Top 10%",
            "above_average": "⬆️ Above average",
            "below_average": "⬇️ Below average",
            "bottom_10pct":  "⚠️ Bottom 10%",
        }.get(pos, pos)

        st.metric("Industry position", pos_icon)
        st.metric("Gap vs Global avg",
                  f"{float(latest_b.get('gap_vs_global') or 0):+.1f} kg/t",
                  delta_color="inverse")
        st.metric("Gap vs ASEAN avg",
                  f"{float(latest_b.get('gap_vs_asean') or 0):+.1f} kg/t",
                  delta_color="inverse")

st.divider()

# ── Pipeline log ──────────────────────────────────────────────
with st.expander("Pipeline run history"):
    log_df = query(SQL_PIPELINE_LOG)
    if not log_df.empty:
        def color_status(val):
            if val == "success": return "background-color: #E1F5EE"
            if val == "failed":  return "background-color: #FCEBEB"
            return "background-color: #FAEEDA"

        st.dataframe(
            log_df.style.applymap(color_status, subset=["status"]),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.caption("ยังไม่มี pipeline runs")