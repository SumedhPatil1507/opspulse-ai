<div align="center">

# ⚡ OpsPulse AI

### Autonomous Kubernetes Incident Remediation Platform · Real-Time Kafka Telemetry · HITL Security Gate

[![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![LangGraph](https://img.shields.io/badge/LangGraph-0.2-FF6B35?style=for-the-badge&logo=langchain&logoColor=white)](https://langchain-ai.github.io/langgraph)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-Confluent-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-Client_v30-326CE5?style=for-the-badge&logo=kubernetes&logoColor=white)](https://kubernetes.io)
[![Qdrant](https://img.shields.io/badge/Qdrant-1.11-DC143C?style=for-the-badge&logo=qdrant&logoColor=white)](https://qdrant.tech)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io)
[![Tests](https://img.shields.io/badge/Tests-99_Passed-34D399?style=for-the-badge&logo=pytest&logoColor=white)](tests/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](LICENSE)
[![GitHub](https://img.shields.io/badge/GitHub-SumedhPatil1507-181717?style=for-the-badge&logo=github)](https://github.com/SumedhPatil1507/opspulse-ai)

**OpsPulse AI** is a production-grade, agentic SRE incident response system that continuously streams cluster telemetry from Kafka, detects anomalous error spikes, retrieves relevant runbooks via hybrid RAG, plans remediations with LLMs, and safely executes Kubernetes cluster actions (`AppsV1Api`, `CoreV1Api`, `Helm`) behind Human-In-The-Loop (HITL) and cryptographic JWT authorization gates.

[**Live Dashboard**](#-interactive-streamlit-command-center) · [**Quick Start**](#-quick-start) · [**Architecture**](#-architecture) · [**Kubernetes Action Engine**](#-kubernetes-hitl-action-engine) · [**Kafka Streaming & DLQ**](#-real-time-kafka-event-streaming) · [**Benchmark Suite**](#-10-benchmark-kubernetes-chaos-scenarios)

> 📦 **Repository:** https://github.com/SumedhPatil1507/opspulse-ai

</div>

---

## ✨ Key Capabilities

|| Subsystem | Enterprise Capability |
||---|---|
|| 📡 **Real-time Kafka Streaming** | Continuous consumer for `k8s.pod.logs` & `k8s.node.metrics` with manual offset commit (`enable.auto.commit=False`) and Dead Letter Queue (`k8s.telemetry.dlq`) poison-pill quarantine. |
|| 📈 **Sliding-Window Spike Detector** | Stateful sliding-window error frequency tracker triggering diagnostic agents upon exceeding error burst thresholds. |
|| 🔍 **Hybrid RAG Retrieval** | Dense (`all-MiniLM-L6-v2`) + Sparse BM25 retrieval over Markdown runbooks with Cross-Encoder re-ranking (`ms-marco-MiniLM-L-6-v2`). |
|| 🧠 **LangGraph Multi-Agent RCA** | 6-node state machine: `ParseLogNode` → `RetrieveRunbookNode` → `PlanRemediationNode` → `HITLCheckNode` → `AutoExecute / AwaitApproval`. |
|| 🛡️ **Kubernetes Action Engine** | Native `AppsV1Api` & `CoreV1Api` remediation executor: `restart_deployment()`, `scale_replicas()`, and `rollback_helm_release()`. |
|| 🔐 **Dual Safety Gates** | Enforces cryptographically verified JWT tokens signed with `ROLE_SRE_ADMIN` and explicit, non-replayable `approval_id` from the HITL Review Queue. |
|| 🧪 **Chaos Benchmark Suite** | 10 realistic Kubernetes cluster incident scenarios (`INC-001` to `INC-010`) with automated diagnostic evaluation scorecard. |
|| 📊 **Interactive Streamlit Cockpit** | Live SRE operations cockpit with real-time telemetry charts, chaos scenario simulator, HITL review queue manager, and DLQ inspector. |

---

## 🏗️ Architecture

```
                          ┌─────────────────────────────────────────────────────────────┐
                          │                        OpsPulse AI                          │
                          │                                                             │
  Telemetry Stream        │   Kafka Ingestion Pipeline      LangGraph State Machine     │
  ┌─────────────────┐     │  ┌────────────────────────┐    ┌─────────────────────────┐  │
  │  k8s.pod.logs   │────►┼──│ KafkaTelemetryConsumer │    │  [START]                │  │
  │  k8s.node.metrics│    │  │ (Manual Offset Commit) │    │      │                  │  │
  └─────────────────┘     │  └───────────┬────────────┘    │      ▼                  │  │
                          │              │                 │  parse_log_node         │  │
  Poison Pill Records     │              ▼                 │      │                  │  │
  ┌─────────────────┐     │  ┌────────────────────────┐    │      ▼                  │  │
  │k8s.telemetry.dlq│◄────┼──│ Dead Letter Queue (DLQ)│    │  retrieve_runbook       │  │
  └─────────────────┘     │  └────────────────────────┘    │  (Dense + Sparse RAG)   │  │
                          │              │                 │      │                  │  │
  HTTP Alerts             │              ▼                 │      ▼                  │  │
  ┌─────────────────┐     │  ┌────────────────────────┐    │  plan_remediation       │  │
  │ POST /ingest    │────►┼──│ ErrorSpikeTracker      │───►│  (Groq Llama-3 / Claude)│  │
  └─────────────────┘     │  └────────────────────────┘    │      │                  │  │
                          │                                │      ▼                  │  │
  Slack HITL Webhook      │   Kubernetes Action Engine     │  hitl_check_node        │  │
  ┌─────────────────┐     │  ┌────────────────────────┐    │  ┌───┴───────────┐      │  │
  │ SRE Review      │◄────┼──│ JWT: ROLE_SRE_ADMIN    │◄───┤  ▼               ▼      │  │
  │ Interactive Btn │     │  │ HITL Review Queue      │    │ auto_execute  await_    │  │
  └─────────────────┘     │  │ AppsV1Api / CoreV1Api  │    │ _node         approval  │  │
                          │  │ Helm Rollback          │    │  │               │      │  │
  Kubernetes Cluster      │  └───────────┬────────────┘    │  ▼               ▼      │  │
  ┌─────────────────┐     │              │                 │ [END: SUCCESS] [AWAIT]  │  │
  │ Pods / Deploys  │◄────┼──────────────┘                 └─────────────────────────┘  │
  └─────────────────┘     └─────────────────────────────────────────────────────────────┘
```

---

## 🛡️ Kubernetes HITL Action Engine

All cluster mutations are executed via [`src/k8s_executor.py`](file:///c:/Users/Sumedh/projects/opspulse-ai/src/k8s_executor.py), requiring two non-bypassable security gates:

1. **Cryptographic JWT Gate**: Verifies JWT signature and checks for `ROLE_SRE_ADMIN` claim.
2. **HITL Review Queue Gate**: Verifies that the action has an explicit `approval_id` in `APPROVED` status with matching action type and target resource.
3. **Anti-Replay Protection**: Immediately transitions approval state to `EXECUTED` upon execution.

```python
from src.k8s_executor import restart_deployment, scale_replicas, rollback_helm_release

# Execute rolling restart with HITL authorization
result = restart_deployment(
    namespace="production",
    deployment_name="payment-processor",
    auth_token="Bearer eyJhbGciOiJIUzI1NiIs...",
    approval_id="hitl-84bf92a10c",
)
print(result.success, result.duration_ms, result.details)
```

---

## 📡 Real-time Kafka Event Streaming

The streaming consumer in [`src/kafka_consumer.py`](file:///c:/Users/Sumedh/projects/opspulse-ai/src/kafka_consumer.py) operates with **at-least-once delivery guarantees**:

* **Manual Offset Commits**: `enable.auto.commit=False` ensures offsets are only committed after successful handling or DLQ routing.
* **Dead Letter Queue (`k8s.telemetry.dlq`)**: Corrupted JSON or schema validation failures are immediately quarantined into the DLQ topic with diagnostic metadata, keeping the consumer partition moving.
* **Error Spike Detector**: Tracks error frequencies per service/pod over a sliding window (default: 5 errors in 60s) to automatically trigger the LangGraph RCA workflow.

---

## 🧪 10 Benchmark Kubernetes Chaos Scenarios

Located in [`incident-simulation/data/`](file:///c:/Users/Sumedh/projects/opspulse-ai/incident-simulation/data/) and executed via [`src/incident_simulator.py`](file:///c:/Users/Sumedh/projects/opspulse-ai/src/incident_simulator.py):

|| ID | Scenario | Category | Target | Recommended Action |
||---|---|---|---|---|
|| **INC-001** | Cascading OOMKilled Memory Leak | `RESOURCE_EXHAUSTION` | `deployment/payment-processor` | Memory limit increase to 4Gi & rolling restart |
|| **INC-002** | CrashLoopBackOff (Missing Secret) | `CONFIGURATION_ERROR` | `deployment/auth-service` | Inject `JWT_SIGNING_KEY` & rollout restart |
|| **INC-003** | CoreDNS Resolution Timeout Storm | `NETWORK_INFRASTRUCTURE` | `deployment/coredns` | Scale CoreDNS replicas to 6 |
|| **INC-004** | PersistentVolumeDiskFull (Write Stall) | `STORAGE_EXHAUSTION` | `statefulset/event-store-db` | Expand PVC storage to 100Gi |
|| **INC-005** | CFS CPU Throttling (p99 Latency Spike) | `CPU_SATURATION` | `deployment/checkout-api` | Increase CPU limit to 1000m / request 500m |
|| **INC-006** | Ingress TLS Certificate Expiry | `SECURITY_CERTIFICATE` | `ingress/api-gateway-ingress` | ACME cert renewal via `cmctl` |
|| **INC-007** | Node NotReady (DiskPressure Eviction) | `NODE_DEGRADATION` | `node/gke-prod-pool-node-3b` | Cordon, drain, and prune rootfs logs |
|| **INC-008** | HikariCP Connection Pool Starvation | `DATABASE_CONTENTION` | `deployment/order-service` | Scale connection pool size to 100 |
|| **INC-009** | Istio Envoy Sidecar mTLS Handshake Error | `SERVICE_MESH` | `deployment/billing-service` | Flush SDS cert cache & rollout restart |
|| **INC-010** | Flawed Readiness Probe (0/3 Available) | `DEPLOYMENT_MISCONFIG` | `deployment/notification-dispatcher` | Fix probe port to 8000 |

---

## 📊 Interactive Streamlit Command Center

The dashboard runs standalone or connected to live Prometheus / Kafka endpoints:

```powershell
streamlit run streamlit_app.py
```

### Dashboard Tabs
1. **SRE Operations Cockpit**: Real-time MTTR, incident rate, error spike frequency, and health breakdown.
2. **Interactive Chaos & RCA Simulator**: Live replay of all 10 Kubernetes benchmark scenarios with step-by-step LangGraph multi-agent diagnostic trace inspection, interactive metrics visualization, and confidence scoring.
3. **Kubernetes HITL Action Engine**: Guarded action dispatcher with SRE JWT token validator and HITL approval queue.
4. **Real-time Kafka & DLQ Stream**: Live event streaming inspector with Dead Letter Queue quarantine monitor.

### Incident Simulation Framework

The project includes a comprehensive **Principal SRE-grade incident simulation framework** in the `incident-simulation/` directory for testing and validating OpsPulse AI's incident detection and diagnostic capabilities.

#### Framework Overview

This benchmark framework provides:
- **10 Realistic Kubernetes Cluster Incidents** with detailed log streams and metrics formatted as raw JSON
- **LangGraph Multi-Agent Diagnostic Traces** for each incident, showing step-by-step root-cause analysis
- **Benchmark Test Inputs** formatted as raw JSON for easy integration
- **Confidence Scoring** and remediation recommendations for each incident
- **Interactive Streamlit Visualization** with:
  - Real-time metrics time-series charts (Plotly - fully interactive)
  - Log level distribution pie charts
  - Interactive log stream explorer with filtering
  - Step-by-step LangGraph agent trace visualization
  - Confidence scoring with visual progress bars
  - Remediation recommendations with priority levels

#### Incident Catalog

| ID | Type | Namespace | Complexity | Overall Confidence | Category |
|----|------|-----------|------------|-------------------|----------|
| INC-2026-001 | Cascading OOMKilled | payment-service | High | 0.94 | Resource Exhaustion |
| INC-2026-002 | CrashLoopBackOff Missing Secret | auth-service | Medium | 0.96 | Configuration Error |
| INC-2026-003 | CoreDNS Resolution Timeout | kube-system | High | 0.93 | Network Infrastructure |
| INC-2026-004 | PersistentVolume Disk Full | database | High | 0.95 | Storage Exhaustion |
| INC-2026-005 | CPU Throttling | api-service | Medium | 0.95 | CPU Saturation |
| INC-2026-006 | ImagePullBackOff Registry Unavailable | order-service | Medium | 0.93 | Registry Connectivity |
| INC-2026-007 | Network Policy Connection Blocked | microservices | Medium | 0.96 | Network Security |
| INC-2026-008 | Resource Quota Exceeded | development | Low | 0.96 | Resource Management |
| INC-2026-009 | Node NotReady Disk Pressure | all-namespaces | Medium | 0.95 | Node Health |
| INC-2026-010 | HPA Scale Failure | web-frontend | Medium | 0.94 | Autoscaling |

#### Usage

**Running the Interactive Streamlit Dashboard:**

```bash
streamlit run streamlit_app.py
```

Navigate to the **"⚡ Interactive Chaos & RCA Simulator"** tab to:
1. Select any of the 10 benchmark incidents from the dropdown
2. Click "🚀 Trigger Simulation & RCA" to replay the incident
3. View interactive metrics visualizations with Plotly charts
4. Explore the log stream with filtering capabilities
5. Inspect the step-by-step LangGraph multi-agent diagnostic trace
6. Review confidence scores and remediation recommendations

**Programmatic Usage:**

```python
from src.incident_simulator import IncidentSimulationFramework

# Initialize the framework
simulator = IncidentSimulationFramework()

# Run a single incident simulation
result = simulator.run_simulation("INC-2026-001")
print(f"Incident: {result.name}")
print(f"Passed: {result.passed}")
print(f"Confidence: {result.confidence_score}")
print(f"Root Cause: {result.diagnosed_root_cause}")

# Run all 10 benchmark incidents
all_results = simulator.run_all()
for res in all_results:
    print(f"{res.incident_id}: {res.passed}")
```

For detailed documentation, see [`incident-simulation/docs/README.md`](incident-simulation/docs/README.md) and [`incident-simulation/docs/incident_catalog.md`](incident-simulation/docs/incident_catalog.md).

---

## 🚀 Quick Start

### 1. Clone & Setup Environment

```bash
git clone https://github.com/SumedhPatil1507/opspulse-ai.git
cd opspulse-ai

# Create virtual environment
python -m venv .venv

# Windows
.venv\Scripts\Activate.ps1
# Linux / macOS
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment Variables

```bash
cp .env.example .env
```

Set keys in `.env`:
```env
GROQ_API_KEY=gsk_your_groq_api_key
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
JWT_SECRET_KEY=opspulse-secure-jwt-secret-key-32bytes
```

### 3. Run Test Suite

```powershell
# Run full 99-test suite
pytest -v

# Run individual subsystems
pytest tests/test_k8s_executor.py -v
pytest tests/test_kafka_consumer.py -v
pytest tests/test_incident_simulator.py -v
pytest tests/test_api.py -v
```

---

## 📂 Project Structure

```
opspulse-ai/
├── data/
│   ├── benchmark_incidents.json    ← 10 realistic Kubernetes cluster incidents
│   └── runbooks/                   ← Markdown SRE runbooks (Qdrant hybrid RAG)
│
├── src/
│   ├── agent/                      ← LangGraph multi-agent state machine
│   │   ├── nodes.py                ← 6 core agent nodes (Parse, Retrieve, Plan, HITL, Execute)
│   │   ├── state.py                ← IncidentState TypedDict + reducers
│   │   ├── workflow.py             ← StateGraph compilation & conditional routing
│   │   └── slack_client.py         ← Slack Block Kit HITL interactive webhook
│   │
│   ├── api/                        ← FastAPI service & routing
│   │   ├── app.py                  ← FastAPI factory & OpenAPI docs
│   │   ├── core/config.py          ← Pydantic v2 Settings (Kafka, K8s, JWT, Redis)
│   │   └── routes/alerts.py        ← POST /api/v1/alerts/ingest endpoint
│   │
│   ├── k8s_executor.py             ← Kubernetes AppsV1Api/CoreV1Api HITL execution engine
│   ├── kafka_consumer.py           ← Real-time Kafka consumer with manual offset & DLQ
│   ├── incident_simulator.py       ← Benchmark chaos runner & RCA evaluator
│   ├── metrics.py                  ← Prometheus metrics registry & ASGI /metrics
│   └── worker/tasks.py             ← Celery async alert processing tasks
│
├── tests/
│   ├── test_k8s_executor.py        ← Kubernetes & HITL JWT security gate tests
│   ├── test_kafka_consumer.py      ← Kafka consumer, manual offset & DLQ tests
│   ├── test_incident_simulator.py  ← 10 benchmark incident simulation tests
│   └── test_api.py                 ← FastAPI ingestion & validation tests
│
├── incident-simulation/            ← Principal SRE-grade incident simulation framework
│   ├── data/                       ← 10 realistic Kubernetes incident log streams
│   ├── traces/                     ← LangGraph multi-agent diagnostic traces
│   └── docs/                       ← Documentation and usage examples
│
├── dashboard.py                    ← Streamlit SRE command center
├── streamlit_app.py                ← Streamlit entry point
├── requirements.txt                ← Lightweight dependencies
└── requirements-full.txt           ← Full production dependencies
```

---

## 📜 License

Distributed under the **MIT License**. See [LICENSE](LICENSE) for more information.

---

<div align="center">
Built with ❤️ using LangGraph · Kafka · Kubernetes · FastAPI · Streamlit
</div>
