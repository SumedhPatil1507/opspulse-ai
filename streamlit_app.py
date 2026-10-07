"""
OpsPulse AI — Interactive Streamlit Dashboard
==============================================
Streamlit Cloud entry-point (streamlit_app.py).
Run locally with:
    streamlit run streamlit_app.py

Live-reloads every N seconds (configurable via sidebar).
All charts are Plotly-powered and fully interactive.
Metrics are sourced from:
  1. A live Prometheus /metrics endpoint  (when FastAPI is running)
  2. Realistic simulated data             (fallback / demo / Streamlit Cloud)

No heavy ML dependencies required — runs standalone.
"""

from __future__ import annotations

import random
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
from plotly.subplots import make_subplots

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="OpsPulse AI",
    page_icon="🔥",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "Get Help": "https://github.com/SumedhPatil1507/opspulse-ai",
        "Report a bug": "https://github.com/SumedhPatil1507/opspulse-ai/issues",
        "About": "OpsPulse AI — Autonomous Incident Remediation Platform",
    },
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* Dark gradient header */
.main-header {
    background: linear-gradient(135deg, #0f0f23 0%, #1a1a3e 50%, #0d1b2a 100%);
    padding: 2rem 2.5rem 1.5rem;
    border-radius: 12px;
    margin-bottom: 1.5rem;
    border: 1px solid #2d2d5e;
    box-shadow: 0 4px 24px rgba(0,0,0,0.4);
}
.main-header h1 { color: #e0e0ff; font-size: 2.4rem; margin: 0; letter-spacing: -0.5px; }
.main-header p  { color: #8888bb; font-size: 1rem; margin: 0.4rem 0 0; }

/* KPI metric cards */
.metric-card {
    background: linear-gradient(135deg, #1e1e3a 0%, #252545 100%);
    border: 1px solid #3a3a6a;
    border-radius: 10px;
    padding: 1.2rem 1.5rem;
    text-align: center;
    box-shadow: 0 2px 12px rgba(0,0,0,0.3);
    transition: transform 0.2s;
}
.metric-card:hover { transform: translateY(-2px); }
.metric-value  { font-size: 2.4rem; font-weight: 700; color: #7c8cf8; }
.metric-label  { font-size: 0.78rem; color: #9999cc; text-transform: uppercase; letter-spacing: 1px; margin-top: 4px; }
.metric-delta  { font-size: 0.82rem; margin-top: 6px; }
.delta-pos     { color: #4ade80; }
.delta-neg     { color: #f87171; }

/* Status badges */
.badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 99px;
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.5px;
}
.badge-green  { background: rgba(74,222,128,0.15); color: #4ade80; border: 1px solid #4ade8044; }
.badge-red    { background: rgba(248,113,113,0.15); color: #f87171; border: 1px solid #f8717144; }
.badge-yellow { background: rgba(251,191,36,0.15);  color: #fbbf24; border: 1px solid #fbbf2444; }
.badge-blue   { background: rgba(96,165,250,0.15);  color: #60a5fa; border: 1px solid #60a5fa44; }
.badge-purple { background: rgba(167,139,250,0.15); color: #a78bfa; border: 1px solid #a78bfa44; }

/* Section headers */
.section-header {
    font-size: 1.05rem; font-weight: 600; color: #c0c0e8;
    border-left: 3px solid #7c8cf8; padding-left: 10px;
    margin: 0.5rem 0 1rem;
}

/* Sidebar */
section[data-testid="stSidebar"] { background: #0d0d1f !important; }
section[data-testid="stSidebar"] .stMarkdown p { color: #9999cc; }

/* Incident table */
.incident-row { border-bottom: 1px solid #2a2a4a; padding: 8px 0; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# Sidebar — controls
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Dashboard Controls")
    st.markdown("---")

    metrics_url = st.text_input(
        "Prometheus /metrics URL",
        value="http://localhost:8000/metrics",
        help="FastAPI app must be running. Falls back to simulated data.",
    )
    auto_refresh = st.toggle("Auto-refresh", value=True)
    refresh_interval = st.slider("Refresh interval (s)", 5, 60, 10)
    time_window = st.selectbox("Time window", ["Last 1 hour", "Last 6 hours", "Last 24 hours"], index=0)

    st.markdown("---")
    st.markdown("### 🔧 Simulation Controls")
    sim_incidents = st.slider("Simulated incident count", 20, 500, 120)
    sim_services = st.multiselect(
        "Services",
        ["payment-service", "order-service", "auth-service", "inventory-service",
         "notification-service", "api-gateway", "db-proxy"],
        default=["payment-service", "order-service", "auth-service", "inventory-service"],
    )
    sim_seed = st.number_input("Random seed", value=42, step=1)

    st.markdown("---")
    st.markdown("### 📊 Display")
    show_raw_metrics = st.toggle("Show raw Prometheus text", value=False)
    chart_theme = st.selectbox("Chart theme", ["plotly_dark", "plotly", "ggplot2"], index=0)

    st.markdown("---")
    st.caption("🔥 OpsPulse AI v0.1.0")
    st.caption("Built with LangGraph · Qdrant · Groq")

# ─────────────────────────────────────────────────────────────────────────────
# Data layer — live Prometheus or simulated fallback
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=refresh_interval)
def fetch_prometheus_text(url: str) -> str | None:
    """Fetch raw Prometheus exposition text from /metrics endpoint."""
    try:
        r = requests.get(url, timeout=3)
        r.raise_for_status()
        return r.text
    except Exception:
        return None


def parse_prometheus_counter(text: str, metric_name: str) -> float:
    """Extract the sum of all label combinations for a counter metric."""
    total = 0.0
    for line in text.splitlines():
        if line.startswith(metric_name + "{") or line.startswith(metric_name + " "):
            parts = line.rsplit(" ", 1)
            if len(parts) == 2:
                try:
                    total += float(parts[1])
                except ValueError:
                    pass
    return total


@st.cache_data(ttl=refresh_interval)
def generate_simulated_data(
    n: int, services: list[str], seed: int, window_hours: int
) -> dict[str, Any]:
    """Generate realistic-looking incident + metric data for demo mode."""
    rng = random.Random(seed)
    np.random.seed(seed)

    now = datetime.now(tz=timezone.utc)
    start = now - timedelta(hours=window_hours)

    severities = ["low", "medium", "high", "critical"]
    sev_weights = [0.20, 0.35, 0.30, 0.15]
    environments = ["production", "staging", "development"]
    env_weights = [0.55, 0.30, 0.15]
    risk_tiers = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    risk_weights = [0.25, 0.35, 0.30, 0.10]
    statuses = ["completed", "awaiting_human", "failed", "rejected"]
    status_weights = [0.62, 0.22, 0.10, 0.06]

    # Time-series: incidents over time
    timestamps = [
        start + timedelta(seconds=rng.uniform(0, window_hours * 3600))
        for _ in range(n)
    ]
    timestamps.sort()

    incidents = []
    for i, ts in enumerate(timestamps):
        svc = rng.choices(services, k=1)[0]
        sev = rng.choices(severities, weights=sev_weights, k=1)[0]
        env = rng.choices(environments, weights=env_weights, k=1)[0]
        risk = rng.choices(risk_tiers, weights=risk_weights, k=1)[0]
        status = rng.choices(statuses, weights=status_weights, k=1)[0]
        duration = abs(np.random.normal(8.5, 4.2))
        incidents.append({
            "timestamp": ts,
            "alert_id": f"alert-{i:04d}",
            "service": svc,
            "severity": sev,
            "environment": env,
            "risk_tier": risk,
            "status": status,
            "duration_s": round(duration, 2),
            "auto_resolved": status == "completed" and risk in ("LOW", "MEDIUM"),
        })
    df = pd.DataFrame(incidents)

    # KPIs
    total = len(df)
    auto_resolved = df["auto_resolved"].sum()
    hitl_total = len(df[df["risk_tier"].isin(["HIGH", "CRITICAL"])])
    hitl_approved = int(hitl_total * 0.73)
    hitl_rejected = hitl_total - hitl_approved
    failed = len(df[df["status"] == "failed"])
    avg_latency = df["duration_s"].mean()
    p95_latency = float(np.percentile(df["duration_s"], 95))

    # LLM call simulation
    llm_calls = [
        {
            "provider": rng.choice(["groq", "anthropic"]),
            "status": rng.choices(["success", "error"], weights=[0.94, 0.06])[0],
            "latency_s": abs(np.random.normal(4.2, 1.8)),
        }
        for _ in range(n)
    ]
    llm_df = pd.DataFrame(llm_calls)

    # Docker sandbox ops
    sandbox_ops = [
        {
            "operation": rng.choices(["inspect", "logs", "restart"], weights=[0.5, 0.35, 0.15])[0],
            "status": rng.choices(["success", "error"], weights=[0.97, 0.03])[0],
            "latency_ms": abs(np.random.normal(180, 60)),
            "container": rng.choice(services),
        }
        for _ in range(int(n * 1.4))
    ]
    sandbox_df = pd.DataFrame(sandbox_ops)

    return {
        "df": df,
        "llm_df": llm_df,
        "sandbox_df": sandbox_df,
        "kpis": {
            "total": total,
            "auto_resolved": int(auto_resolved),
            "auto_resolve_rate": round(auto_resolved / total * 100, 1),
            "hitl_approved": hitl_approved,
            "hitl_rejected": hitl_rejected,
            "failed": failed,
            "avg_latency": round(avg_latency, 2),
            "p95_latency": round(p95_latency, 2),
        },
    }


# ── Determine data source ─────────────────────────────────────────────────────
window_map = {"Last 1 hour": 1, "Last 6 hours": 6, "Last 24 hours": 24}
window_hours = window_map[time_window]

raw_prom = fetch_prometheus_text(metrics_url)
live_mode = raw_prom is not None

if not sim_services:
    sim_services = ["payment-service", "order-service"]

data = generate_simulated_data(sim_incidents, sim_services, int(sim_seed), window_hours)
df: pd.DataFrame = data["df"]
llm_df: pd.DataFrame = data["llm_df"]
sandbox_df: pd.DataFrame = data["sandbox_df"]
kpis: dict = data["kpis"]

# Override KPIs from live Prometheus when available
if live_mode:
    auto_r = parse_prometheus_counter(raw_prom, "opspulse_incidents_auto_resolved_total")
    hitl_a = parse_prometheus_counter(raw_prom, "opspulse_hitl_approvals_total")
    if auto_r > 0:
        kpis["auto_resolved"] = int(auto_r)
    if hitl_a > 0:
        kpis["hitl_approved"] = int(hitl_a)

# ─────────────────────────────────────────────────────────────────────────────
# Header
# ─────────────────────────────────────────────────────────────────────────────
src_badge = (
    '<span class="badge badge-green">● LIVE</span>'
    if live_mode
    else '<span class="badge badge-yellow">⚡ SIMULATED</span>'
)
st.markdown(f"""
<div class="main-header">
  <h1>🔥 OpsPulse AI</h1>
  <p>Autonomous Incident Remediation Platform &nbsp;·&nbsp; {src_badge}
     &nbsp;·&nbsp; <span style="color:#666699">{datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</span>
  </p>
</div>
""", unsafe_allow_html=True)

if not live_mode:
    st.info(
        f"📡 FastAPI not reachable at `{metrics_url}` — showing simulated data. "
        "Start the app with `uvicorn src.api.app:app` to see live metrics.",
        icon="ℹ️",
    )

# ─────────────────────────────────────────────────────────────────────────────
# KPI row
# ─────────────────────────────────────────────────────────────────────────────
st.markdown('<div class="section-header">📊 Key Performance Indicators</div>', unsafe_allow_html=True)

c1, c2, c3, c4, c5, c6 = st.columns(6)

def kpi_card(col, value, label, delta=None, delta_positive=True, suffix=""):
    delta_html = ""
    if delta is not None:
        cls = "delta-pos" if delta_positive else "delta-neg"
        arrow = "▲" if delta_positive else "▼"
        delta_html = f'<div class="metric-delta {cls}">{arrow} {delta}</div>'
    col.markdown(f"""
    <div class="metric-card">
        <div class="metric-value">{value}{suffix}</div>
        <div class="metric-label">{label}</div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)

kpi_card(c1, kpis["total"],           "Total Incidents",      delta=f"+{kpis['total']//10} vs prev")
kpi_card(c2, kpis["auto_resolved"],   "Auto-Resolved",        delta=f"{kpis['auto_resolve_rate']}% rate", delta_positive=True)
kpi_card(c3, kpis["hitl_approved"],   "HITL Approvals",       delta=f"{kpis['hitl_rejected']} rejected", delta_positive=False)
kpi_card(c4, kpis["failed"],          "Failed Runs",          delta="needs review", delta_positive=False)
kpi_card(c5, kpis["avg_latency"],     "Avg Latency (s)",      suffix="s")
kpi_card(c6, kpis["p95_latency"],     "P95 Latency (s)",      suffix="s")

st.markdown("<br>", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# Row 1 — Incident timeline + Status breakdown
# ─────────────────────────────────────────────────────────────────────────────
st.markdown('<div class="section-header">📈 Incident Timeline & Status</div>', unsafe_allow_html=True)
col_left, col_right = st.columns([2, 1])

with col_left:
    # Resample incidents into time buckets
    df_ts = df.set_index("timestamp").sort_index()
    bucket = "30min" if window_hours <= 6 else "1h"
    df_resampled = (
        df_ts.groupby([pd.Grouper(freq=bucket), "status"])
        .size()
        .reset_index(name="count")
    )
    df_resampled.columns = ["timestamp", "status", "count"]

    status_colours = {
        "completed":      "#4ade80",
        "awaiting_human": "#fbbf24",
        "failed":         "#f87171",
        "rejected":       "#a78bfa",
    }
    fig_timeline = px.bar(
        df_resampled,
        x="timestamp", y="count", color="status",
        color_discrete_map=status_colours,
        title="Incident Volume Over Time",
        labels={"count": "Incidents", "timestamp": ""},
        template=chart_theme,
        barmode="stack",
    )
    fig_timeline.update_layout(
        legend_title_text="", plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)", font_color="#c0c0e0",
        height=320, margin=dict(l=0, r=0, t=40, b=0),
    )
    fig_timeline.update_traces(hovertemplate="%{y} incidents<extra>%{fullData.name}</extra>")
    st.plotly_chart(fig_timeline, use_container_width=True)

with col_right:
    status_counts = df["status"].value_counts().reset_index()
    status_counts.columns = ["status", "count"]
    fig_donut = px.pie(
        status_counts, names="status", values="count",
        title="Workflow Status Split",
        color="status", color_discrete_map=status_colours,
        hole=0.55, template=chart_theme,
    )
    fig_donut.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", font_color="#c0c0e0",
        height=320, margin=dict(l=0, r=0, t=40, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=-0.2),
        showlegend=True,
    )
    fig_donut.update_traces(
        textposition="inside", textinfo="percent+label",
        hovertemplate="%{label}: %{value} (%{percent})<extra></extra>",
    )
    st.plotly_chart(fig_donut, use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# Row 2 — Agent execution latency histogram + Risk tier heatmap
# ─────────────────────────────────────────────────────────────────────────────
st.markdown('<div class="section-header">⏱️ Agent Execution Latency</div>', unsafe_allow_html=True)
col_hist, col_heat = st.columns([3, 2])

with col_hist:
    fig_lat = go.Figure()
    for env, colour in [("production","#f87171"), ("staging","#fbbf24"), ("development","#4ade80")]:
        subset = df[df["environment"] == env]["duration_s"]
        if len(subset) == 0:
            continue
        fig_lat.add_trace(go.Histogram(
            x=subset, name=env, nbinsx=30,
            marker_color=colour, opacity=0.75,
            hovertemplate=f"<b>{env}</b><br>Duration: %{{x:.1f}}s<br>Count: %{{y}}<extra></extra>",
        ))
    # P95 line
    p95 = float(np.percentile(df["duration_s"], 95))
    fig_lat.add_vline(
        x=p95, line_dash="dash", line_color="#a78bfa",
        annotation_text=f"P95: {p95:.1f}s",
        annotation_font_color="#a78bfa",
        annotation_position="top right",
    )
    fig_lat.update_layout(
        title="agent_execution_latency_seconds",
        barmode="overlay", template=chart_theme,
        xaxis_title="Duration (s)", yaxis_title="Count",
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font_color="#c0c0e0", height=320,
        margin=dict(l=0, r=0, t=40, b=0),
        legend_title_text="Environment",
    )
    st.plotly_chart(fig_lat, use_container_width=True)

with col_heat:
    # Risk tier × severity heatmap
    heat_data = df.groupby(["risk_tier", "severity"]).size().reset_index(name="count")
    risk_order = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    sev_order  = ["low", "medium", "high", "critical"]
    heat_pivot = (
        heat_data.pivot(index="risk_tier", columns="severity", values="count")
        .reindex(index=risk_order, columns=sev_order)
        .fillna(0)
    )
    fig_heat = px.imshow(
        heat_pivot,
        color_continuous_scale="RdYlGn_r",
        title="Risk Tier × Alert Severity",
        template=chart_theme,
        labels=dict(color="Count"),
        aspect="auto",
    )
    fig_heat.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", font_color="#c0c0e0",
        height=320, margin=dict(l=0, r=0, t=40, b=0),
        coloraxis_showscale=True,
    )
    fig_heat.update_traces(
        hovertemplate="Risk: %{y}<br>Severity: %{x}<br>Count: %{z}<extra></extra>"
    )
    st.plotly_chart(fig_heat, use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# Row 3 — Service breakdown + HITL funnel
# ─────────────────────────────────────────────────────────────────────────────
st.markdown('<div class="section-header">🏗️ Service Intelligence & HITL Funnel</div>', unsafe_allow_html=True)
col_svc, col_hitl = st.columns([3, 2])

with col_svc:
    svc_stats = (
        df.groupby("service")
        .agg(
            total=("alert_id", "count"),
            auto_resolved=("auto_resolved", "sum"),
            avg_latency=("duration_s", "mean"),
            failed=("status", lambda x: (x == "failed").sum()),
        )
        .reset_index()
    )
    svc_stats["auto_rate"] = (svc_stats["auto_resolved"] / svc_stats["total"] * 100).round(1)
    svc_stats = svc_stats.sort_values("total", ascending=True)

    fig_svc = go.Figure()
    fig_svc.add_trace(go.Bar(
        y=svc_stats["service"], x=svc_stats["auto_resolved"],
        name="Auto-Resolved", orientation="h", marker_color="#4ade80",
        hovertemplate="%{y}<br>Auto-resolved: %{x}<extra></extra>",
    ))
    fig_svc.add_trace(go.Bar(
        y=svc_stats["service"],
        x=svc_stats["total"] - svc_stats["auto_resolved"],
        name="Manual / HITL", orientation="h", marker_color="#f87171",
        hovertemplate="%{y}<br>Manual: %{x}<extra></extra>",
    ))
    fig_svc.update_layout(
        title="incidents_auto_resolved_total by Service",
        barmode="stack", template=chart_theme,
        xaxis_title="Incidents", yaxis_title="",
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font_color="#c0c0e0", height=340,
        margin=dict(l=0, r=0, t=40, b=0),
    )
    st.plotly_chart(fig_svc, use_container_width=True)

with col_hitl:
    # HITL approval funnel
    total_high = len(df[df["risk_tier"].isin(["HIGH", "CRITICAL"])])
    slack_sent = int(total_high * 0.97)
    approved   = kpis["hitl_approved"]
    rejected   = kpis["hitl_rejected"]
    executed   = int(approved * 0.98)

    fig_funnel = go.Figure(go.Funnel(
        y=["HIGH/CRITICAL Incidents", "Slack Notified", "HITL Approved", "Rejected", "Executed"],
        x=[total_high, slack_sent, approved, rejected, executed],
        textposition="inside",
        textinfo="value+percent initial",
        marker=dict(color=["#7c8cf8", "#60a5fa", "#4ade80", "#f87171", "#a78bfa"]),
        connector=dict(line=dict(color="#3a3a6a", width=2)),
        hovertemplate="%{label}<br>Count: %{value}<br>%{percentInitial}<extra></extra>",
    ))
    fig_funnel.update_layout(
        title="hitl_approvals_total Funnel",
        template=chart_theme,
        paper_bgcolor="rgba(0,0,0,0)", font_color="#c0c0e0",
        height=340, margin=dict(l=0, r=0, t=40, b=0),
    )
    st.plotly_chart(fig_funnel, use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# Row 4 — LLM call metrics + Docker sandbox ops
# ─────────────────────────────────────────────────────────────────────────────
st.markdown('<div class="section-header">🤖 LLM & Docker Sandbox Metrics</div>', unsafe_allow_html=True)
col_llm, col_docker = st.columns(2)

with col_llm:
    fig_llm = make_subplots(
        rows=1, cols=2,
        subplot_titles=("LLM Calls by Provider", "LLM Latency Distribution"),
        specs=[[{"type": "pie"}, {"type": "histogram"}]],
    )
    provider_counts = llm_df.groupby(["provider", "status"]).size().reset_index(name="count")
    total_by_provider = provider_counts.groupby("provider")["count"].sum()

    fig_llm.add_trace(
        go.Pie(
            labels=total_by_provider.index.tolist(),
            values=total_by_provider.values.tolist(),
            hole=0.45,
            marker_colors=["#7c8cf8", "#f59e0b"],
            hovertemplate="%{label}: %{value} calls (%{percent})<extra></extra>",
            textinfo="label+percent",
        ),
        row=1, col=1,
    )
    for provider, colour in [("groq", "#7c8cf8"), ("anthropic", "#f59e0b")]:
        subset = llm_df[llm_df["provider"] == provider]["latency_s"]
        fig_llm.add_trace(
            go.Histogram(x=subset, name=provider, nbinsx=20,
                         marker_color=colour, opacity=0.75,
                         hovertemplate=f"<b>{provider}</b><br>%{{x:.1f}}s<br>%{{y}} calls<extra></extra>"),
            row=1, col=2,
        )
    fig_llm.update_layout(
        template=chart_theme, height=300,
        paper_bgcolor="rgba(0,0,0,0)", font_color="#c0c0e0",
        margin=dict(l=0, r=0, t=50, b=0), barmode="overlay",
        showlegend=True, legend_title_text="Provider",
    )
    fig_llm.update_xaxes(title_text="Latency (s)", row=1, col=2)
    st.plotly_chart(fig_llm, use_container_width=True)

with col_docker:
    fig_docker = make_subplots(
        rows=1, cols=2,
        subplot_titles=("Operations by Type", "Sandbox Latency (ms)"),
        specs=[[{"type": "bar"}, {"type": "box"}]],
    )
    op_counts = sandbox_df.groupby(["operation", "status"]).size().reset_index(name="count")
    op_colours = {"success": "#4ade80", "error": "#f87171"}
    for status_val in ["success", "error"]:
        subset = op_counts[op_counts["status"] == status_val]
        fig_docker.add_trace(
            go.Bar(
                x=subset["operation"], y=subset["count"],
                name=status_val, marker_color=op_colours[status_val],
                hovertemplate=f"<b>{status_val}</b><br>%{{x}}: %{{y}}<extra></extra>",
            ),
            row=1, col=1,
        )
    for op, colour in [("inspect","#60a5fa"), ("logs","#fbbf24"), ("restart","#f87171")]:
        subset = sandbox_df[sandbox_df["operation"] == op]["latency_ms"]
        fig_docker.add_trace(
            go.Box(y=subset, name=op, marker_color=colour, boxmean=True,
                   hovertemplate=f"<b>{op}</b><br>%{{y:.0f}}ms<extra></extra>"),
            row=1, col=2,
        )
    fig_docker.update_layout(
        template=chart_theme, height=300, barmode="group",
        paper_bgcolor="rgba(0,0,0,0)", font_color="#c0c0e0",
        margin=dict(l=0, r=0, t=50, b=0),
    )
    fig_docker.update_yaxes(title_text="Count", row=1, col=1)
    fig_docker.update_yaxes(title_text="ms", row=1, col=2)
    st.plotly_chart(fig_docker, use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# Row 5 — Severity trend line + Scatter (latency vs severity)
# ─────────────────────────────────────────────────────────────────────────────
st.markdown('<div class="section-header">📉 Severity Trends & Latency Scatter</div>', unsafe_allow_html=True)
col_trend, col_scatter = st.columns([3, 2])

with col_trend:
    df_sev = df.set_index("timestamp").sort_index()
    df_sev_resampled = (
        df_sev.groupby([pd.Grouper(freq=bucket), "severity"])
        .size().reset_index(name="count")
    )
    df_sev_resampled.columns = ["timestamp", "severity", "count"]
    sev_colours = {"low":"#4ade80","medium":"#fbbf24","high":"#f97316","critical":"#f87171"}
    fig_trend = px.line(
        df_sev_resampled, x="timestamp", y="count", color="severity",
        color_discrete_map=sev_colours,
        title="Incident Severity Over Time",
        template=chart_theme,
        markers=True,
    )
    fig_trend.update_layout(
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font_color="#c0c0e0", height=300,
        margin=dict(l=0, r=0, t=40, b=0),
        legend_title_text="Severity",
    )
    fig_trend.update_traces(
        hovertemplate="%{x|%H:%M}<br>%{y} incidents<extra>%{fullData.name}</extra>"
    )
    st.plotly_chart(fig_trend, use_container_width=True)

with col_scatter:
    fig_scatter = px.scatter(
        df.sample(min(300, len(df)), random_state=int(sim_seed)),
        x="duration_s", y="service", color="risk_tier",
        size_max=10, opacity=0.7,
        color_discrete_map={"LOW":"#4ade80","MEDIUM":"#fbbf24","HIGH":"#f97316","CRITICAL":"#f87171"},
        title="Latency by Service & Risk Tier",
        template=chart_theme,
        hover_data=["severity", "status", "environment"],
    )
    fig_scatter.update_layout(
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font_color="#c0c0e0", height=300,
        margin=dict(l=0, r=0, t=40, b=0),
        xaxis_title="Duration (s)", yaxis_title="",
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# Row 6 — Recent incidents table
# ─────────────────────────────────────────────────────────────────────────────
st.markdown('<div class="section-header">🗂️ Recent Incidents</div>', unsafe_allow_html=True)

status_badge_map = {
    "completed":      '<span class="badge badge-green">✓ completed</span>',
    "awaiting_human": '<span class="badge badge-yellow">⏳ awaiting human</span>',
    "failed":         '<span class="badge badge-red">✗ failed</span>',
    "rejected":       '<span class="badge badge-purple">⊘ rejected</span>',
}
risk_badge_map = {
    "LOW":      '<span class="badge badge-green">LOW</span>',
    "MEDIUM":   '<span class="badge badge-yellow">MEDIUM</span>',
    "HIGH":     '<span class="badge badge-red">HIGH</span>',
    "CRITICAL": '<span class="badge badge-red">🚨 CRITICAL</span>',
}

recent = df.sort_values("timestamp", ascending=False).head(15).copy()
recent["timestamp_str"] = recent["timestamp"].dt.strftime("%H:%M:%S")
recent["status_badge"] = recent["status"].map(status_badge_map).fillna(recent["status"])
recent["risk_badge"]   = recent["risk_tier"].map(risk_badge_map).fillna(recent["risk_tier"])

search_term = st.text_input("🔍 Filter by service or alert ID", placeholder="e.g. payment")
if search_term:
    recent = recent[
        recent["service"].str.contains(search_term, case=False) |
        recent["alert_id"].str.contains(search_term, case=False)
    ]

table_html = """
<table style="width:100%;border-collapse:collapse;font-size:0.83rem;color:#c0c0e0">
<thead>
  <tr style="border-bottom:2px solid #3a3a6a;color:#8888bb;text-transform:uppercase;font-size:0.72rem;letter-spacing:1px">
    <th style="padding:8px 12px;text-align:left">Time</th>
    <th style="padding:8px 12px;text-align:left">Alert ID</th>
    <th style="padding:8px 12px;text-align:left">Service</th>
    <th style="padding:8px 12px;text-align:left">Severity</th>
    <th style="padding:8px 12px;text-align:left">Environment</th>
    <th style="padding:8px 12px;text-align:left">Risk</th>
    <th style="padding:8px 12px;text-align:left">Status</th>
    <th style="padding:8px 12px;text-align:right">Latency</th>
  </tr>
</thead>
<tbody>
"""
for _, row in recent.iterrows():
    sev_colour = {"low":"#4ade80","medium":"#fbbf24","high":"#f97316","critical":"#f87171"}.get(row["severity"],"#888")
    table_html += f"""
  <tr class="incident-row" style="border-bottom:1px solid #2a2a4a">
    <td style="padding:7px 12px;font-family:monospace;color:#6666aa">{row['timestamp_str']}</td>
    <td style="padding:7px 12px;font-family:monospace;color:#7c8cf8">{row['alert_id']}</td>
    <td style="padding:7px 12px">{row['service']}</td>
    <td style="padding:7px 12px;color:{sev_colour}">{row['severity'].upper()}</td>
    <td style="padding:7px 12px;color:#9999cc">{row['environment']}</td>
    <td style="padding:7px 12px">{row['risk_badge']}</td>
    <td style="padding:7px 12px">{row['status_badge']}</td>
    <td style="padding:7px 12px;text-align:right;font-family:monospace">{row['duration_s']:.2f}s</td>
  </tr>"""
table_html += "</tbody></table>"
st.markdown(table_html, unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# Raw Prometheus text (optional)
# ─────────────────────────────────────────────────────────────────────────────
if show_raw_metrics:
    st.markdown('<div class="section-header">📄 Raw Prometheus Metrics</div>', unsafe_allow_html=True)
    if live_mode and raw_prom:
        st.code(raw_prom[:4000] + ("\n... (truncated)" if len(raw_prom) > 4000 else ""),
                language="text")
    else:
        st.warning("Raw metrics only available when connected to a live FastAPI instance.")

# ─────────────────────────────────────────────────────────────────────────────
# Auto-refresh
# ─────────────────────────────────────────────────────────────────────────────
if auto_refresh:
    time.sleep(0.5)           # small delay so button clicks register first
    st.markdown(
        f"<p style='color:#444466;font-size:0.75rem;text-align:right'>"
        f"Next refresh in ~{refresh_interval}s</p>",
        unsafe_allow_html=True,
    )
    time.sleep(refresh_interval - 0.5)
    st.rerun()
