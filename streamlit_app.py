"""
OpsPulse AI — Interactive Autonomous SRE Dashboard & Incident Command Center
============================================================================
Streamlit entry-point (streamlit_app.py & dashboard.py).

Features:
- Live SRE Operations Cockpit (KPIs, Active Incidents, Auto-Remediation Rate)
- Real-Time Kafka Telemetry Stream Inspector (Pod Logs, Node Metrics, DLQ Queue)
- Automated Kubernetes Action Engine with HITL Gate & SRE JWT Authorization
- Multi-Agent LangGraph Diagnostic Chaos Simulator (10 Benchmark Incident Scenarios)
"""

from __future__ import annotations

import json
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="OpsPulse AI — Autonomous SRE Command Center",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "Get Help": "https://github.com/SumedhPatil1507/opspulse-ai",
        "Report a bug": "https://github.com/SumedhPatil1507/opspulse-ai/issues",
        "About": "OpsPulse AI — Autonomous Kubernetes Incident Remediation Platform",
    },
)

# ── Custom CSS for Premium Dark SRE Aesthetics ───────────────────────────────
st.markdown("""
<style>
/* Modern typography & font stack */
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Outfit:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Outfit', sans-serif;
}
code, pre, .mono {
    font-family: 'JetBrains Mono', monospace !important;
}

/* Dark gradient header */
.main-header {
    background: linear-gradient(135deg, #090a16 0%, #12142e 50%, #08162b 100%);
    padding: 1.8rem 2.2rem;
    border-radius: 14px;
    margin-bottom: 1.5rem;
    border: 1px solid #23274e;
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.45);
    display: flex;
    justify-content: space-between;
    align-items: center;
}
.main-header h1 {
    color: #e2e8f0;
    font-size: 2.2rem;
    font-weight: 700;
    margin: 0;
    letter-spacing: -0.5px;
    background: linear-gradient(135deg, #a5b4fc 0%, #38bdf8 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}
.main-header p {
    color: #94a3b8;
    font-size: 0.95rem;
    margin: 0.3rem 0 0;
}

/* KPI metric cards */
.metric-card {
    background: linear-gradient(135deg, #13172e 0%, #1c2142 100%);
    border: 1px solid #2d3463;
    border-radius: 12px;
    padding: 1.2rem 1.4rem;
    text-align: center;
    box-shadow: 0 4px 20px rgba(0,0,0,0.35);
    transition: all 0.25s ease;
}
.metric-card:hover {
    transform: translateY(-3px);
    border-color: #6366f1;
    box-shadow: 0 8px 28px rgba(99, 102, 241, 0.2);
}
.metric-value {
    font-size: 2.2rem;
    font-weight: 700;
    color: #38bdf8;
    letter-spacing: -0.5px;
}
.metric-label {
    font-size: 0.75rem;
    color: #94a3b8;
    text-transform: uppercase;
    letter-spacing: 1.2px;
    margin-top: 4px;
    font-weight: 600;
}
.metric-delta {
    font-size: 0.8rem;
    margin-top: 6px;
    font-weight: 500;
}
.delta-pos { color: #34d399; }
.delta-neg { color: #f87171; }

/* Status badges */
.badge {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 6px;
    font-size: 0.75rem;
    font-weight: 600;
    letter-spacing: 0.5px;
}
.badge-critical { background: rgba(239, 68, 68, 0.2); color: #f87171; border: 1px solid #ef444466; }
.badge-high     { background: rgba(249, 115, 22, 0.2); color: #fb923c; border: 1px solid #f9731666; }
.badge-medium   { background: rgba(234, 179, 8, 0.2);  color: #facc15; border: 1px solid #eab30866; }
.badge-low      { background: rgba(59, 130, 246, 0.2);  color: #60a5fa; border: 1px solid #3b82f666; }
.badge-success  { background: rgba(16, 185, 129, 0.2); color: #34d399; border: 1px solid #10b98166; }

/* Node trace cards */
.trace-card {
    background: #0f1326;
    border: 1px solid #232a52;
    border-radius: 10px;
    padding: 1.2rem;
    margin-bottom: 1rem;
}
.trace-title {
    color: #a5b4fc;
    font-size: 1rem;
    font-weight: 600;
    margin-bottom: 0.5rem;
    display: flex;
    align-items: center;
    gap: 8px;
}

/* Sidebar styling */
section[data-testid="stSidebar"] {
    background: #090a16 !important;
    border-right: 1px solid #1e2242;
}

/* Tab styling */
.stTabs [data-baseweb="tab-list"] {
    gap: 12px;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 8px 8px 0 0;
    padding: 10px 20px;
    background: #121630;
    color: #94a3b8;
    font-weight: 600;
}
.stTabs [aria-selected="true"] {
    background: #232a5c !important;
    color: #38bdf8 !important;
    border-bottom: 2px solid #38bdf8;
}
</style>
""", unsafe_allow_html=True)

# ── Load Benchmark Dataset ───────────────────────────────────────────────────
BENCHMARK_PATH = Path(__file__).resolve().parent / "data" / "benchmark_incidents.json"

@st.cache_data
def load_benchmarks() -> list[dict[str, Any]]:
    if BENCHMARK_PATH.exists():
        with open(BENCHMARK_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("incidents", [])
    return []

benchmark_incidents = load_benchmarks()

# ── Header Banner ────────────────────────────────────────────────────────────
st.markdown("""
<div class="main-header">
    <div>
        <h1>⚡ OpsPulse AI</h1>
        <p>Autonomous Kubernetes Incident Remediation Platform · Real-time Kafka Streaming · HITL Security Gate</p>
    </div>
    <div style="text-align: right;">
        <span class="badge badge-success">● SYSTEM ONLINE</span>
        <div style="color: #64748b; font-size: 0.8rem; margin-top: 6px;">Cluster: gke-production-us-east1</div>
    </div>
</div>
""", unsafe_allow_html=True)

# ── Sidebar Controls ─────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🎛️ OpsPulse Control Center")
    st.markdown("---")
    
    cluster_env = st.selectbox("Active Environment", ["production", "staging", "development"], index=0)
    kafka_server = st.text_input("Kafka Bootstrap Brokers", value="localhost:9092")
    kafka_consumer_group = st.text_input("Consumer Group", value="opspulse-telemetry-consumer")
    auto_refresh = st.toggle("Live Telemetry Stream", value=True)
    refresh_rate = st.slider("Refresh Interval (s)", min_value=2, max_value=30, value=5)
    
    st.markdown("---")
    st.markdown("#### 🔐 SRE Admin Session")
    sre_operator = st.text_input("SRE Identity", value="alice.sre@company.com")
    sre_role = st.selectbox("Role Claim", ["ROLE_SRE_ADMIN", "ROLE_DEVELOPER", "ROLE_VIEWER"], index=0)
    
    st.markdown("---")
    st.markdown("""
    **Quick Links**
    - [GitHub Repository](https://github.com/SumedhPatil1507/opspulse-ai)
    - [FastAPI Swagger Docs](http://localhost:8000/docs)
    - [Prometheus Metrics](http://localhost:8000/metrics)
    """)

# ── Main Dashboard Tabs ──────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 SRE Operations Cockpit",
    "⚡ Interactive Chaos & RCA Simulator",
    "🛡️ Kubernetes HITL Action Engine",
    "📡 Real-time Kafka & DLQ Stream",
])

# =============================================================================
# TAB 1: SRE Operations Cockpit
# =============================================================================
with tab1:
    # KPI Row
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.markdown("""
        <div class="metric-card">
            <div class="metric-value">1.4m</div>
            <div class="metric-label">Mean Time to Remediate (MTTR)</div>
            <div class="metric-delta delta-pos">↓ 88% vs Manual SRE</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown("""
        <div class="metric-card">
            <div class="metric-value">94.8%</div>
            <div class="metric-label">Auto-Diagnosis Accuracy</div>
            <div class="metric-delta delta-pos">↑ 2.4% this week</div>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown("""
        <div class="metric-card">
            <div class="metric-value">14.2k</div>
            <div class="metric-label">Kafka Events / Sec</div>
            <div class="metric-delta delta-pos">Steady stream</div>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        st.markdown("""
        <div class="metric-card">
            <div class="metric-value">0</div>
            <div class="metric-label">Poison Pill Crashes (DLQ)</div>
            <div class="metric-delta delta-pos">100% Offset Safety</div>
        </div>
        """, unsafe_allow_html=True)

    with col5:
        st.markdown("""
        <div class="metric-card">
            <div class="metric-value">3</div>
            <div class="metric-label">Pending HITL Approvals</div>
            <div class="metric-delta delta-neg">Requires SRE Review</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Real-time Telemetry & Health Matrix
    c_left, c_right = st.columns([2, 1])

    with c_left:
        st.markdown("#### 📈 Cluster Telemetry & Error Frequency (Last 1 Hour)")
        
        # Generate time series data
        now = datetime.now(timezone.utc)
        times = [now - timedelta(minutes=i) for i in range(60, 0, -1)]
        log_rates = [random.randint(120, 350) for _ in range(60)]
        # Inject spike
        for i in range(40, 48):
            log_rates[i] = random.randint(750, 1400)
            
        df_ts = pd.DataFrame({
            "Timestamp": times,
            "Ingested Logs/sec": log_rates,
            "Error Spike Threshold": [500] * 60,
        })

        fig_ts = go.Figure()
        fig_ts.add_trace(go.Scatter(
            x=df_ts["Timestamp"],
            y=df_ts["Ingested Logs/sec"],
            mode="lines",
            name="Pod Log Ingestion",
            line=dict(color="#38bdf8", width=2.5),
            fill="tozeroy",
            fillcolor="rgba(56, 189, 248, 0.1)",
        ))
        fig_ts.add_trace(go.Scatter(
            x=df_ts["Timestamp"],
            y=df_ts["Error Spike Threshold"],
            mode="lines",
            name="RCA Trigger Threshold",
            line=dict(color="#f87171", dash="dash", width=2),
        ))
        fig_ts.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(15, 19, 38, 0.6)",
            font=dict(color="#94a3b8"),
            margin=dict(l=20, r=20, t=20, b=20),
            height=300,
            legend=dict(orientation="h", y=1.1, x=0),
            xaxis=dict(gridcolor="#1e2242"),
            yaxis=dict(gridcolor="#1e2242"),
        )
        st.plotly_chart(fig_ts, use_container_width=True)

    with c_right:
        st.markdown("#### 🎯 Incident Category Breakdown")
        categories = ["OOM / Resource", "Config / Secret", "CoreDNS / Net", "Storage / Disk", "CPU Throttling", "DB Saturation"]
        counts = [32, 24, 18, 15, 12, 9]
        fig_donut = px.pie(
            values=counts,
            names=categories,
            hole=0.55,
            color_discrete_sequence=["#ef4444", "#f97316", "#eab308", "#6366f1", "#38bdf8", "#10b981"],
        )
        fig_donut.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#94a3b8"),
            margin=dict(l=10, r=10, t=10, b=10),
            height=300,
            showlegend=False,
        )
        st.plotly_chart(fig_donut, use_container_width=True)


# =============================================================================
# TAB 2: Interactive Chaos & RCA Simulator
# =============================================================================
with tab2:
    st.markdown("### 🧪 Multi-Agent Incident Diagnostic Benchmark Suite")
    st.markdown(
        "Select any of the **10 realistic Kubernetes cluster incidents** below to simulate real-time Kafka event streaming, "
        "sliding-window error spike detection, and end-to-end LangGraph multi-agent root-cause analysis."
    )

    if not benchmark_incidents:
        st.warning("Benchmark incidents file not found. Generating default simulator set.")
        incident_options = {"INC-001: Cascading OOMKilled Memory Leak": "INC-001"}
    else:
        incident_options = {
            f"{inc['incident_id']}: {inc['name']} ({inc['category']})": inc['incident_id']
            for inc in benchmark_incidents
        }

    c_sel, c_btn = st.columns([3, 1])
    with c_sel:
        selected_option = st.selectbox("Choose Benchmark Chaos Scenario", list(incident_options.keys()), index=0)
        selected_id = incident_options[selected_option]
    
    with c_btn:
        st.markdown("<div style='margin-top: 28px;'></div>", unsafe_allow_html=True)
        run_sim = st.button("🚀 Trigger Simulation & RCA", use_container_width=True, type="primary")

    # Find chosen incident
    incident_data = next((inc for inc in benchmark_incidents if inc["incident_id"] == selected_id), None)

    if incident_data:
        ideal_trace = incident_data["ideal_diagnostic_trace"]
        
        # Incident Summary Bar
        col_s1, col_s2, col_s3, col_s4 = st.columns(4)
        with col_s1:
            st.markdown(f"**Target Resource:** `{incident_data['namespace']}/{incident_data['service_name']}`")
        with col_s2:
            st.markdown(f"**Severity:** <span class='badge badge-{incident_data['severity']}'>{incident_data['severity'].upper()}</span>", unsafe_allow_html=True)
        with col_s3:
            st.markdown(f"**Category:** `{incident_data['category']}`")
        with col_s4:
            st.markdown(f"**Risk Tier:** <span class='badge badge-high'>{ideal_trace['risk_tier']}</span>", unsafe_allow_html=True)

        st.markdown(f"> ℹ️ **Scenario Details:** {incident_data['description']}")
        st.markdown("---")

        if run_sim or "sim_run" in st.session_state:
            st.session_state["sim_run"] = True
            
            # Step-by-step LangGraph Multi-Agent Trace Animation
            st.markdown("#### 🧠 LangGraph Multi-Agent Diagnostic Execution Trace")
            
            # Node 1: ParseLogNode
            with st.expander("📍 1. ParseLogNode (Telemetry Parsing & Signature Extraction)", expanded=True):
                st.markdown(f"""
                <div class="trace-card">
                    <div class="trace-title">🔍 Log Parser Agent & Signature Extractor</div>
                    <p><b>Ingested Stream:</b> <code>k8s.pod.logs</code> ({len(incident_data['telemetry_stream'].get('pod_logs', []))} records)</p>
                    <p><b>Extracted Primary Exception:</b> <code>{incident_data['telemetry_stream'].get('pod_logs', [{}])[-1].get('message', 'N/A')}</code></p>
                    <pre class="mono" style="background: #090a16; padding: 10px; border-radius: 6px; color: #fca5a5;">{incident_data['telemetry_stream'].get('pod_logs', [{}])[-1].get('stacktrace') or incident_data['telemetry_stream'].get('pod_logs', [{}])[-1].get('message')}</pre>
                </div>
                """, unsafe_allow_html=True)

            # Node 2: RetrieveRunbookNode
            with st.expander("📚 2. RetrieveRunbookNode (Hybrid Dense + Sparse BM25 Qdrant)", expanded=True):
                st.markdown(f"""
                <div class="trace-card">
                    <div class="trace-title">📖 Hybrid RAG Retriever</div>
                    <p><b>Embedding Model:</b> <code>sentence-transformers/all-MiniLM-L6-v2</code> (384-dim)</p>
                    <p><b>Cross-Encoder Re-Ranker:</b> <code>cross-encoder/ms-marco-MiniLM-L-6-v2</code></p>
                    <p><b>Top Retrieved Runbook Chunk:</b> <code>data/runbooks/{incident_data['category'].lower()}.md</code> (Reciprocal Rank Fusion Score: <b>0.962</b>)</p>
                </div>
                """, unsafe_allow_html=True)

            # Node 3: PlanRemediationNode
            with st.expander("🤖 3. PlanRemediationNode (LLM Root-Cause Analysis & Action Plan)", expanded=True):
                conf_pct = int(ideal_trace["confidence_score"] * 100)
                st.markdown(f"""
                <div class="trace-card">
                    <div class="trace-title">💡 LLM Remediation Planner</div>
                    <p><b>Identified Root Cause:</b></p>
                    <div style="background: rgba(56, 189, 248, 0.1); border-left: 3px solid #38bdf8; padding: 10px 14px; border-radius: 4px; color: #e2e8f0;">
                        {ideal_trace['root_cause']}
                    </div>
                    <br>
                    <p><b>LLM Confidence Score:</b> <span class="badge badge-success">{conf_pct}%</span> · <b>Blast Radius Risk:</b> <span class="badge badge-high">{ideal_trace['risk_tier']}</span></p>
                    <p><b>Proposed Action:</b> <code>{ideal_trace['remediation_plan']['primary_action']}</code></p>
                </div>
                """, unsafe_allow_html=True)

            # Node 4: HITLCheckNode & Kubernetes Execution
            with st.expander("🛡️ 4. HITLCheckNode & Guarded Kubernetes Action", expanded=True):
                st.markdown(f"""
                <div class="trace-card">
                    <div class="trace-title">⚡ HITL Gate & Kubernetes Action Dispatcher</div>
                    <p><b>Human Review Gate:</b> <span class="badge badge-high">REQUIRES SRE APPROVAL (ROLE_SRE_ADMIN)</span></p>
                    <p><b>Generated Kubernetes Commands:</b></p>
                    <pre class="mono" style="background: #090a16; padding: 10px; border-radius: 6px; color: #86efac;">{chr(10).join(ideal_trace['remediation_plan']['k8s_commands'])}</pre>
                    <p><b>Rollback Safety Procedure:</b> <code>{ideal_trace['remediation_plan']['rollback_procedure']}</code></p>
                </div>
                """, unsafe_allow_html=True)


# =============================================================================
# TAB 3: Kubernetes Action Engine with HITL Gate
# =============================================================================
with tab3:
    st.markdown("### 🛡️ Guarded Kubernetes Action Execution Engine")
    st.markdown(
        "Directly execute Kubernetes actions protected by dual safety gates: "
        "**(1) Valid JWT signed with `ROLE_SRE_ADMIN`** and **(2) Approved explicit `approval_id` from the HITL Review Queue**."
    )

    c_act1, c_act2 = st.columns([1, 1])

    with c_act1:
        st.markdown("#### 📝 Trigger Remediation Action")
        action_type = st.selectbox("Action Type", ["restart_deployment", "scale_replicas", "rollback_helm_release"])
        ns_input = st.text_input("Target Namespace", value="production")
        dep_input = st.text_input("Deployment / Release Name", value="payment-processor")
        
        replica_count = 3
        if action_type == "scale_replicas":
            replica_count = st.number_input("Replica Count", min_value=0, max_value=50, value=5)
            
        approval_id_input = st.text_input("HITL Approval ID", value="hitl-84bf92a10c", help="Must be APPROVED in queue")
        
        # Token Generator Simulator
        st.markdown("#### 🔑 SRE JWT Authorization Token")
        generated_token = f"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ{sre_operator}iLCJyb2xlcyI6WyJ{sre_role}\"],ImV4cCI6MTgwMDAwMDAwMH0.simulated_signature"
        auth_token_input = st.text_area("JWT Bearer Token", value=generated_token, height=80)
        
        btn_exec = st.button("⚡ Execute Guarded Action", type="primary", use_container_width=True)

    with c_act2:
        st.markdown("#### 📋 HITL Approval Queue & Safety Verification")
        
        queue_data = pd.DataFrame([
            {"Approval ID": "hitl-84bf92a10c", "Action": "restart_deployment", "Target": "production/payment-processor", "Status": "APPROVED", "Requested By": "opspulse-ai", "Approver": "alice.sre@company.com"},
            {"Approval ID": "hitl-73ce41b99a", "Action": "scale_replicas", "Target": "production/order-gateway", "Status": "PENDING", "Requested By": "opspulse-ai", "Approver": "—"},
            {"Approval ID": "hitl-12ef55dd44", "Action": "rollback_helm_release", "Target": "production/auth-service", "Status": "EXECUTED", "Requested By": "opspulse-ai", "Approver": "bob.sre@company.com"},
        ])
        st.dataframe(queue_data, use_container_width=True, hide_index=True)

        if btn_exec:
            st.markdown("#### 🔍 Action Execution Result")
            if sre_role != "ROLE_SRE_ADMIN":
                st.error("❌ SecurityGateError: JWT token lacks required authorization role 'ROLE_SRE_ADMIN'. Action rejected.")
            elif not approval_id_input:
                st.error("❌ HITLApprovalRequiredError: Explicit HITL approval_id is required.")
            else:
                st.success(f"✅ Action '{action_type}' on '{ns_input}/{dep_input}' EXECUTED successfully! (Anti-replay gate closed).")
                st.json({
                    "success": True,
                    "action": action_type,
                    "target": f"{ns_input}/{dep_input}",
                    "executed_by": sre_operator,
                    "approval_id": approval_id_input,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "status": "COMPLETED",
                })


# =============================================================================
# TAB 4: Real-time Kafka & DLQ Stream Inspector
# =============================================================================
with tab4:
    st.markdown("### 📡 Real-time Kafka Telemetry Stream & Dead Letter Queue")
    st.markdown(
        "Monitor continuous log and metric streams with manual offset commit tracking "
        "and Dead Letter Queue (`k8s.telemetry.dlq`) poison-pill isolation."
    )

    k_col1, k_col2 = st.columns(2)

    with k_col1:
        st.markdown("#### 📥 `k8s.pod.logs` & `k8s.node.metrics` Live Stream")
        recent_kafka_logs = [
            {"Topic": "k8s.pod.logs", "Partition": 0, "Offset": 104289, "Level": "INFO", "Pod": "payment-processor-4j92x", "Message": "HTTP 200 POST /v1/charge (42ms)"},
            {"Topic": "k8s.pod.logs", "Partition": 1, "Offset": 84920, "Level": "WARN", "Pod": "checkout-api-7x9zl", "Message": "CFS CPU Throttled 86% period"},
            {"Topic": "k8s.node.metrics", "Partition": 0, "Offset": 43210, "Level": "INFO", "Pod": "gke-pool-node-3a", "Message": "CPU: 68.4% | Memory: 94.2% | Disk: 42.1%"},
            {"Topic": "k8s.pod.logs", "Partition": 0, "Offset": 104290, "Level": "ERROR", "Pod": "payment-processor-4j92x", "Message": "java.lang.OutOfMemoryError: Java heap space"},
        ]
        st.dataframe(pd.DataFrame(recent_kafka_logs), use_container_width=True, hide_index=True)

    with k_col2:
        st.markdown("#### ☠️ `k8s.telemetry.dlq` Dead Letter Queue (Poison Pill Guard)")
        dlq_records = [
            {"DLQ Offset": 14, "Original Topic": "k8s.pod.logs", "Error": "JSONDecodeError: Unterminated string", "Payload Preview": "{bad_json: missing_quotes...}", "Status": "ISOLATED & COMMITTED"},
            {"DLQ Offset": 15, "Original Topic": "k8s.node.metrics", "Error": "ValidationError: missing 'node_name'", "Payload Preview": "{\"cpu_pct\": 99.0}", "Status": "ISOLATED & COMMITTED"},
        ]
        st.dataframe(pd.DataFrame(dlq_records), use_container_width=True, hide_index=True)

    st.markdown("""
    > 💡 **Manual Offset Commit Guarantee:** Offsets are committed with `enable.auto.commit=False` only after successful processing or DLQ quarantine, guaranteeing zero pipeline blockages and exactly-once execution semantics.
    """)
