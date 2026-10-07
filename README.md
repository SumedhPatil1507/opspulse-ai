<div align="center">

# 🔥 OpsPulse AI

### Autonomous Incident Remediation Platform

[![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![LangGraph](https://img.shields.io/badge/LangGraph-0.2-FF6B35?style=for-the-badge&logo=langchain&logoColor=white)](https://langchain-ai.github.io/langgraph)
[![Qdrant](https://img.shields.io/badge/Qdrant-1.11-DC143C?style=for-the-badge&logo=qdrant&logoColor=white)](https://qdrant.tech)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io)
[![Docker](https://img.shields.io/badge/Docker-SDK-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://docker.com)
[![Prometheus](https://img.shields.io/badge/Prometheus-Metrics-E6522C?style=for-the-badge&logo=prometheus&logoColor=white)](https://prometheus.io)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](LICENSE)
[![GitHub](https://img.shields.io/badge/GitHub-sanjeevrpatil11--gif-181717?style=for-the-badge&logo=github)](https://github.com/sanjeevrpatil11-gif/opspulse-ai)

**OpsPulse AI** is a production-grade, agentic incident response system that ingests alert payloads, retrieves relevant runbooks via hybrid RAG, plans remediations with LLMs, and executes safe container operations — all with human-in-the-loop approval for high-risk actions.

[**Live Dashboard**](#-streamlit-dashboard) · [**Quick Start**](#-quick-start) · [**Architecture**](#-architecture) · [**API Docs**](#-api-reference)

> 📦 **Repository:** https://github.com/sanjeevrpatil11-gif/opspulse-ai

</div>

---

## ✨ Features

| Layer | Capability |
|---|---|
| 🚨 **Alert Ingestion** | FastAPI `/api/v1/alerts/ingest` with Pydantic v2 validation → 202 Accepted + Celery task |
| 🔍 **Hybrid RAG** | Dense (all-MiniLM-L6-v2) + BM25 sparse retrieval over Markdown runbooks → cross-encoder re-ranking |
| 🧠 **LangGraph Agent** | 6-node state machine: ParseLog → RetrieveRunbook → PlanRemediation → HITLCheck → Execute |
| 🤖 **LLM Planning** | Groq (llama3-70b) primary, Claude 3.5 Sonnet fallback → structured JSON tool parameters |
| 🙋 **HITL Approval** | HIGH/CRITICAL risk routes to Slack Block Kit webhook with ✅/❌ buttons |
| 🐳 **Safe Sandbox** | Docker SDK: inspect, tail 100 log lines, graceful restart — allowlisted, no destructive ops |
| 📊 **Prometheus** | 8 metrics (counters + histograms) with labels; ASGI `/metrics` endpoint |
| 📈 **Streamlit** | Interactive Plotly dashboard: timeline, heatmap, HITL funnel, latency histograms, scatter |

---

## 🏗️ Architecture

```
                          ┌──────────────────────────────────────────────────────┐
                          │                  OpsPulse AI                         │
                          │                                                      │
  Monitoring Systems      │   FastAPI (ASGI)          LangGraph State Machine    │
  ┌──────────────┐        │  ┌─────────────────┐     ┌───────────────────────┐  │
  │ Prometheus   │◄───────┼──│  GET /metrics   │     │  [START]              │  │
  │ AlertManager │        │  │  POST /ingest   │     │      │                │  │
  │ Grafana      │        │  └────────┬────────┘     │      ▼                │  │
  └──────────────┘        │           │               │  parse_log_node       │  │
                          │           ▼               │      │                │  │
  Slack Workspace         │   ┌───────────────┐      │      ▼                │  │
  ┌──────────────┐        │   │  Celery Task  │      │  retrieve_runbook     │  │
  │  #ops-alerts │◄───────┼───│  Queue        │─────►│      │                │  │
  │  HITL Buttons│        │   │  (Redis)      │      │      ▼                │  │
  └──────────────┘        │   └───────────────┘      │  plan_remediation     │  │
                          │                           │  (Groq / Claude)     │  │
  Docker Engine           │   ┌───────────────┐      │      │                │  │
  ┌──────────────┐        │   │  Qdrant       │◄─────│      ▼                │  │
  │  Containers  │◄───────┼───│  Vector DB    │      │  hitl_check_node      │  │
  │  (sandbox)   │        │   │  (runbooks)   │      │  ┌───┴─────────┐      │  │
  └──────────────┘        │   └───────────────┘      │  │             │      │  │
                          │                           │  ▼             ▼      │  │
  Streamlit               │   ┌───────────────┐      │ auto_execute await_   │  │
  ┌──────────────┐        │   │  BM25 Sparse  │      │ _node    approval     │  │
  │  Dashboard   │        │   │  Index        │      │  │                    │  │
  │  (Plotly)    │        │   └───────────────┘      │  ▼                    │  │
  └──────────────┘        │                           │ [END]                 │  │
                          │                           └───────────────────────┘  │
                          └──────────────────────────────────────────────────────┘
```

### Component Map

```
opspulse-ai/
├── dashboard.py                  ← Streamlit interactive dashboard
├── main.py                       ← uvicorn entry-point
├── requirements.txt
├── .env.example                  ← config template
│
├── data/runbooks/                ← Markdown runbooks (indexed into Qdrant)
│   ├── database_connection_exhausted.md
│   ├── oom_killed_pod.md
│   ├── redis_connection_refused.md
│   └── http_5xx_spike.md
│
└── src/
    ├── metrics.py                ← Prometheus registry (counters + histograms)
    │
    ├── api/                      ← FastAPI application
    │   ├── app.py                  create_app() factory, lifespan, CORS
    │   ├── core/
    │   │   ├── config.py           pydantic-settings (all env vars)
    │   │   └── logging_config.py   structured JSON logging
    │   ├── models/alert.py         AlertPayload (Pydantic v2) + validators
    │   └── routes/alerts.py        POST /api/v1/alerts/ingest → 202
    │
    ├── rag/                      ← Hybrid retrieval pipeline
    │   ├── chunker.py              Markdown H1-H3 splitter → RunbookChunk
    │   ├── sparse.py               BM25Okapi scorer + tokeniser
    │   ├── indexer.py              Qdrant collection manager + upsert
    │   └── retriever.py            HybridSearchRetriever (dense+sparse+rerank)
    │
    ├── agent/                    ← LangGraph state machine
    │   ├── state.py                IncidentState TypedDict + enums
    │   ├── nodes.py                All 6 node functions
    │   ├── slack_client.py         Block Kit webhook notifications
    │   └── workflow.py             build_workflow() + conditional routing
    │
    ├── tools/                    ← Safe execution
    │   └── sandbox.py              ContainerSandbox (inspect/logs/restart)
    │
    └── worker/                   ← Celery async processing
        ├── celery_app.py           Celery factory + Redis config
        └── tasks.py                process_alert task (retry, acks_late)
```

---

## 🚀 Quick Start

### Prerequisites

- Python 3.12+
- Docker Desktop running
- Groq API key (free at [console.groq.com](https://console.groq.com/keys))

### 1. Clone & install

```bash
git clone https://github.com/your-org/opspulse-ai.git
cd opspulse-ai
python -m venv .venv
# Windows
.venv\Scripts\Activate.ps1
# macOS/Linux
source .venv/bin/activate

pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
```

Open `.env` and set at minimum:

```env
GROQ_API_KEY=gsk_your_key_here          # required for LLM planning
SLACK_WEBHOOK_URL=https://hooks.slack.com/...  # optional — enables HITL
```

> ⚠️ **Windows users:** Never use `echo "..." >> .env` — PowerShell writes UTF-16.
> Edit `.env` directly in your editor instead.

### 3. Start infrastructure

```bash
# Qdrant vector database
docker run -d -p 6333:6333 --name qdrant qdrant/qdrant

# Redis (Celery broker)
docker run -d -p 6379:6379 --name redis redis:7
```

### 4. Index runbooks

```bash
python -c "from src.rag.indexer import RunbookIndexer; RunbookIndexer.build_index()"
```

### 5. Start all services

```bash
# Terminal 1 — FastAPI
uvicorn src.api.app:app --reload --port 8000

# Terminal 2 — Celery worker
celery -A src.worker.celery_app worker --loglevel=info -Q alerts

# Terminal 3 — Streamlit dashboard
streamlit run dashboard.py
```

### 6. Send a test alert

```bash
curl -X POST http://localhost:8000/api/v1/alerts/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "alert_id": "test-001",
    "service_name": "payment-service",
    "severity": "critical",
    "environment": "production",
    "raw_log_stacktrace": "sqlalchemy.exc.TimeoutError: QueuePool limit of size 5 overflow 10 reached, connection timed out",
    "timestamp": "2026-10-06T12:00:00Z"
  }'
```

Expected response:
```json
{
  "tracking_id": "celery-task-uuid",
  "status": "queued",
  "message": "Alert 'test-001' accepted and queued for processing."
}
```

---

## 📈 Streamlit Dashboard

```bash
streamlit run dashboard.py
```

Opens at **http://localhost:8501**

| Section | Charts |
|---|---|
| KPI Row | Total incidents · Auto-resolved · HITL approvals · Failed · P95 latency |
| Incident Timeline | Stacked bar over time (by workflow status) |
| Status Split | Donut — completed / awaiting_human / failed / rejected |
| Latency Histogram | Overlaid per environment with P95 marker |
| Risk × Severity | Heatmap (RdYlGn) |
| Service Breakdown | `incidents_auto_resolved_total` stacked bar per service |
| HITL Funnel | HIGH/CRITICAL → Slack sent → Approved → Executed |
| LLM Metrics | Provider pie + latency histogram |
| Docker Sandbox | Operation counts + latency box plots |
| Severity Trends | Line chart over time |
| Latency Scatter | Service × duration coloured by risk tier |
| Incidents Table | Searchable, badge-decorated last 15 incidents |

**Live mode:** connect to `http://localhost:8000/metrics` in the sidebar.
**Demo mode:** fully simulated with configurable seed and service list.

---

## 🔬 API Reference

### `POST /api/v1/alerts/ingest`

| Field | Type | Required | Notes |
|---|---|---|---|
| `alert_id` | string | ✅ | Unique alert identifier |
| `service_name` | string | ✅ | Originating service |
| `severity` | enum | ✅ | `low` / `medium` / `high` / `critical` |
| `raw_log_stacktrace` | string | ✅ | Full stack trace text |
| `timestamp` | datetime | — | Defaults to UTC now |
| `environment` | enum | ✅ | `development` / `staging` / `production` |

> **Constraint:** `production` alerts must be `high` or `critical` severity.

**Response `202 Accepted`:**
```json
{ "tracking_id": "uuid", "status": "queued", "message": "..." }
```

### `GET /healthz`
```json
{ "status": "ok", "service": "opspulse-ai" }
```

### `GET /metrics`
Prometheus exposition format. Scrape with:
```yaml
# prometheus.yml
scrape_configs:
  - job_name: opspulse
    static_configs:
      - targets: ['localhost:8000']
```

---

## 📊 Prometheus Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `opspulse_agent_execution_latency_seconds` | Histogram | `status`, `environment` | End-to-end workflow duration |
| `opspulse_incidents_auto_resolved_total` | Counter | `service_name`, `severity`, `environment` | Auto-resolved without HITL |
| `opspulse_hitl_approvals_total` | Counter | `decision`, `risk_tier`, `service_name` | Human approval/rejection |
| `opspulse_sandbox_operations_total` | Counter | `operation`, `status`, `container_name` | Docker SDK calls |
| `opspulse_sandbox_operation_latency_seconds` | Histogram | `operation` | Docker call latency |
| `opspulse_llm_calls_total` | Counter | `provider`, `status` | LLM API invocations |
| `opspulse_llm_call_latency_seconds` | Histogram | `provider` | LLM round-trip latency |
| `opspulse_runbook_retrievals_total` | Counter | `status` | RAG retrieval outcomes |

---

## 🤖 LangGraph Workflow

```
[START]
  │
  ▼
parse_log_node           ← Extracts exception signatures, keywords (regex)
  │  ↘ FAILED → [END]
  ▼
retrieve_runbook_node    ← Hybrid search: dense ANN + BM25 → cross-encoder re-rank
  │  ↘ FAILED → [END]
  ▼
plan_remediation_node    ← LLM (Groq/Claude) → structured JSON ProposedAction
  │  ↘ FAILED → [END]
  ▼
hitl_check_node          ← Risk tier evaluation
  ├── LOW/MEDIUM/APPROVED ──► auto_execute_node ──► [END: COMPLETED]
  ├── REJECTED ────────────────────────────────────► [END: REJECTED]
  └── HIGH/CRITICAL ───────► await_approval_node ──► [END: AWAITING_HUMAN]
                                 │ (Slack Block Kit webhook posted)
```

### Risk Tier → HITL Routing

| Risk Tier | Route | Slack? |
|---|---|---|
| `LOW` | auto_execute | ❌ |
| `MEDIUM` | auto_execute | ❌ |
| `HIGH` | await_approval | ✅ |
| `CRITICAL` | await_approval | ✅ |
| `UNKNOWN` | await_approval (fail-safe) | ✅ |

---

## 🐳 Docker Sandbox

```python
from src.tools.sandbox import ContainerSandbox

with ContainerSandbox() as sb:
    # Read-only — never mutates container state
    status = sb.inspect_container("payment-service")
    logs   = sb.get_container_logs("payment-service", tail=100)

    # Non-destructive restart (graceful SIGTERM → wait → SIGKILL → start)
    result = sb.restart_container("payment-service")

    print(f"Running: {status.running} | Health: {status.health}")
    print(f"Last log: {logs.lines[-1]}")
    print(f"Restart OK: {result.success} ({result.duration_ms:.0f}ms)")
```

**Safety layers:**
1. `SANDBOX_ALLOWED_PREFIXES` allowlist — empty = dev only, warn in production
2. No `kill`, `remove`, `exec`, volume ops — intentionally absent
3. `internal=True` bridge network — no outbound internet from sandboxed containers
4. Configurable `SANDBOX_RESTART_TIMEOUT` — prevents hung containers blocking the agent
5. Every call emits a Prometheus metric + structured log line

---

## 🧪 Running Tests

```bash
# All tests (no Redis or Docker required — fully mocked)
pytest

# Verbose with coverage
pytest -v --tb=short

# Single test class
pytest tests/test_api.py::TestIngestEndpoint -v
```

Tests use `httpx.AsyncClient` + `ASGITransport` — no real HTTP server needed.
Celery is injected via FastAPI's `Depends` override — no broker required.

---

## ⚙️ Configuration Reference

All settings are read from environment variables or `.env`:

| Variable | Default | Description |
|---|---|---|
| `GROQ_API_KEY` | — | **Required** for LLM planning (primary) |
| `ANTHROPIC_API_KEY` | — | Claude fallback when Groq key absent |
| `SLACK_WEBHOOK_URL` | — | HITL notifications; HIGH risk skipped if unset |
| `REDIS_URL` | `redis://localhost:6379/0` | Celery broker + backend |
| `QDRANT_HOST` | `localhost` | Qdrant vector DB host |
| `QDRANT_PORT` | `6333` | Qdrant port |
| `RUNBOOKS_DIR` | `data/runbooks` | Path to Markdown runbook files |
| `HITL_RISK_TIERS` | `["HIGH","CRITICAL"]` | Tiers requiring human approval |
| `SANDBOX_ALLOWED_PREFIXES` | `[]` | Container name prefixes sandbox may touch |
| `METRICS_ENABLED` | `true` | Toggle Prometheus metric collection |
| `DOCKER_BASE_URL` | `npipe:////./pipe/docker_engine` | Docker daemon socket |

---

## 📁 Tech Stack

| Layer | Technology |
|---|---|
| API | FastAPI + Pydantic v2 + uvicorn |
| Task Queue | Celery 5 + Redis |
| Vector DB | Qdrant |
| Embeddings | `sentence-transformers/all-MiniLM-L6-v2` |
| Re-ranking | `cross-encoder/ms-marco-MiniLM-L-6-v2` |
| Sparse Retrieval | BM25Okapi (`rank-bm25`) |
| Agent Framework | LangGraph 0.2 |
| LLM | Groq (llama3-70b-8192) / Claude 3.5 Sonnet |
| Container Ops | Docker SDK 7 |
| Metrics | Prometheus Client |
| Dashboard | Streamlit + Plotly |
| Notifications | Slack Incoming Webhooks (Block Kit) |

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feat/my-feature`
3. Run tests: `pytest`
4. Commit: `git commit -m "feat: add my feature"`
5. Push and open a Pull Request

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

<div align="center">
Built with ❤️ using LangGraph · Qdrant · Groq · FastAPI · Streamlit
</div>
