# OpsPulse AI Incident Simulation Framework

A comprehensive benchmark framework for testing and validating OpsPulse AI's incident detection and diagnostic capabilities using realistic Kubernetes incident scenarios.

## Overview

This framework provides:
- **10 realistic Kubernetes incident scenarios** with detailed log streams and metrics
- **LangGraph multi-agent diagnostic traces** for each incident, showing step-by-step root-cause analysis
- **Benchmark test inputs** formatted as raw JSON for easy integration
- **Confidence scoring** and remediation recommendations for each incident

## Directory Structure

```
incident-simulation/
├── data/                    # Incident log streams and metrics
│   ├── incident_01_cascading_oom.json
│   ├── incident_02_crashloop_secret.json
│   ├── incident_03_coredns_timeout.json
│   ├── incident_04_pv_disk_full.json
│   ├── incident_05_cpu_throttling.json
│   ├── incident_06_image_pull_error.json
│   ├── incident_07_network_policy.json
│   ├── incident_08_resource_quota.json
│   ├── incident_09_node_not_ready.json
│   └── incident_10_hpa_scale_error.json
├── traces/                  # LangGraph diagnostic traces
│   ├── trace_01_cascading_oom.json
│   ├── trace_02_crashloop_secret.json
│   ├── trace_03_coredns_timeout.json
│   ├── trace_04_pv_disk_full.json
│   ├── trace_05_cpu_throttling.json
│   ├── trace_06_image_pull_error.json
│   ├── trace_07_network_policy.json
│   ├── trace_08_resource_quota.json
│   ├── trace_09_node_not_ready.json
│   └── trace_10_hpa_scale_error.json
└── docs/                    # Documentation
    └── README.md
```

## Incident Scenarios

### 1. Cascading OOMKilled Errors
- **Type**: Memory exhaustion leading to cascading pod failures
- **Impact**: Payment service, Redis cache, and API gateway
- **Root Cause**: Insufficient memory limits during traffic spike
- **Key Signals**: OOMKilled events, memory pressure, crash loop

### 2. CrashLoopBackOff Due to Missing Secret
- **Type**: Application startup failure due to missing Kubernetes secrets
- **Impact**: Authentication service (all pods)
- **Root Cause**: Secrets not created during deployment
- **Key Signals**: Exit code 1, secret not found errors, crash loop

### 3. CoreDNS Resolution Timeouts
- **Type**: DNS resolution failures across cluster
- **Impact**: All services experiencing connection failures
- **Root Cause**: External DNS server (8.8.8.8) high latency
- **Key Signals**: DNS query timeouts, cache miss rate, network policy drops

### 4. PersistentVolume Disk Full
- **Type**: Storage exhaustion on PostgreSQL PVC
- **Impact**: Database write operations blocked, replication lag
- **Root Cause**: Database growth exceeded PVC capacity
- **Key Signals**: Disk usage at 98%, write failures, WAL errors

### 5. CPU Throttling Performance Degradation
- **Type**: CPU limits causing performance degradation
- **Impact**: API frontend and backend services
- **Root Cause**: CPU limits too low without horizontal scaling
- **Key Signals**: High throttling rate, increased latency, CPU at limit

### 6. ImagePullBackOff Registry Unavailable
- **Type**: Container registry connectivity failure
- **Impact**: Order service deployment unable to pull images
- **Root Cause**: DNS resolution failure for ACR domain
- **Key Signals**: Image pull errors, DNS resolution failures

### 7. Network Policy Connection Blocked
- **Type**: Overly restrictive network policy blocking traffic
- **Impact**: Microservices unable to communicate
- **Root Cause**: Misconfigured network policy denying all egress
- **Key Signals**: Connection blocked errors, high packet drop rate

### 8. Resource Quota Exceeded
- **Type**: Namespace resource limits exceeded
- **Impact**: Development namespace unable to create new resources
- **Root Cause**: Quota limits too low for workload
- **Key Signals**: Quota exceeded errors, pending pods

### 9. Node NotReady Disk Pressure
- **Type**: Node disk exhaustion causing node unavailability
- **Impact**: Pods evicted from affected node
- **Root Cause**: Accumulated Docker images and logs filling disk
- **Key Signals**: Disk pressure condition, node NotReady, pod evictions

### 10. HPA Scale Failure Insufficient Metrics
- **Type**: Horizontal Pod Autoscaler unable to scale
- **Impact**: Web frontend unable to handle load
- **Root Cause**: Metrics server not responding
- **Key Signals**: Missing metrics, HPA stuck at min replicas

## Data Format

### Incident Log Streams

Each incident file contains:
- `incident_id`: Unique identifier
- `incident_type`: Description of incident type
- `timestamp_start`/`timestamp_end`: Incident duration
- `cluster`/`namespace`: Kubernetes context
- `affected_pods`: List of impacted pods
- `log_stream`: Array of log entries with timestamp, level, component, and message
- `metrics_stream`: Array of Prometheus metrics with labels and values

Example:
```json
{
  "incident_id": "INC-2026-001",
  "incident_type": "Cascading_OOMKilled",
  "timestamp_start": "2026-10-08T10:15:00Z",
  "log_stream": [
    {
      "timestamp": "2026-10-08T10:15:25.123Z",
      "level": "ERROR",
      "component": "kubelet",
      "message": "Kill container payment-app with OOMKilled"
    }
  ],
  "metrics_stream": [
    {
      "timestamp": "2026-10-08T10:15:00Z",
      "metric": "container_memory_usage_bytes",
      "value": 8388608000
    }
  ]
}
```

### Diagnostic Traces

Each trace file contains:
- `incident_id`: Reference to incident
- `langgraph_diagnostic_trace`: Multi-agent analysis workflow
- `agents_involved`: List of LangGraph agents
- `diagnostic_steps`: Step-by-step analysis with findings and confidence
- `overall_confidence`: Aggregate confidence score
- `remediation_validation`: Post-remediation verification steps

Example:
```json
{
  "incident_id": "INC-2026-001",
  "langgraph_diagnostic_trace": {
    "agents_involved": ["LogAnalyzer", "MetricsCorrelator", "TopologyMapper"],
    "diagnostic_steps": [
      {
        "step": 1,
        "agent": "LogAnalyzer",
        "action": "Parse log stream for OOMKilled events",
        "findings": ["Detected 5+ OOMKilled events"],
        "confidence": 0.95
      }
    ],
    "overall_confidence": 0.94
  }
}
```

## Usage

### Loading Incident Data

```python
import json

# Load incident log stream
with open('incident-simulation/data/incident_01_cascading_oom.json', 'r') as f:
    incident = json.load(f)

# Access log stream
for log_entry in incident['log_stream']:
    print(f"{log_entry['timestamp']}: {log_entry['message']}")

# Access metrics
for metric in incident['metrics_stream']:
    print(f"{metric['metric']}: {metric['value']}")
```

### Loading Diagnostic Traces

```python
import json

# Load diagnostic trace
with open('incident-simulation/traces/trace_01_cascading_oom.json', 'r') as f:
    trace = json.load(f)

# Access diagnostic steps
for step in trace['langgraph_diagnostic_trace']['diagnostic_steps']:
    print(f"Step {step['step']}: {step['agent']} - {step['action']}")
    print(f"Confidence: {step['confidence']}")
    print(f"Findings: {step['findings']}")
```

### Benchmark Testing

```python
def test_incident_detection(incident_data, expected_trace):
    """
    Test OpsPulse AI's incident detection against expected diagnostic trace
    """
    # Feed incident data to OpsPulse AI
    detected_incident = opspulse_ai.analyze(incident_data)

    # Compare with expected trace
    confidence_match = abs(detected_incident.confidence - expected_trace['overall_confidence']) < 0.1
    agent_match = set(detected_incident.agents) == set(expected_trace['langgraph_diagnostic_trace']['agents_involved'])

    return confidence_match and agent_match
```

## LangGraph Agent Workflow

The diagnostic traces follow a multi-agent workflow:

1. **LogAnalyzer**: Parses log streams for error patterns and events
2. **MetricsCorrelator**: Correlates metrics with log events
3. **TopologyMapper**: Maps service dependencies and infrastructure
4. **NetworkDiagnostics**: Analyzes network connectivity and policies
5. **SecretValidator**: Validates secret existence and permissions
6. **StorageDiagnostics**: Analyzes storage and disk usage
7. **QuotaAuditor**: Audits resource quotas and limits
8. **RootCauseAnalyzer**: Identifies root cause using combined evidence
9. **RemediationAdvisor**: Generates remediation commands with priority

Each agent produces findings with confidence scores (0.0-1.0), contributing to the overall incident confidence.

## Extending the Framework

### Adding New Incidents

1. Create new incident file in `data/` directory
2. Follow the existing JSON structure
3. Include realistic log entries and metrics
4. Create corresponding trace file in `traces/` directory
5. Define agent workflow and remediation steps

### Custom Scenarios

To create custom scenarios:

1. Define incident type and context
2. Generate realistic log stream with timestamps
3. Add relevant Prometheus metrics
4. Define expected agent workflow
5. Specify remediation commands with priorities
6. Set confidence scores for each step

## Validation Metrics

When testing OpsPulse AI against this framework, measure:

- **Detection Accuracy**: Percentage of incidents correctly identified
- **Confidence Calibration**: Difference between expected and actual confidence
- **Agent Selection**: Correct agents invoked for incident type
- **Root Cause Accuracy**: Correct root cause identification
- **Remediation Relevance**: Appropriateness of remediation commands
- **Time to Diagnosis**: Time from incident start to root cause identification

## Contributing

When adding new incidents:
- Ensure log entries are realistic and timestamp-ordered
- Include both logs and metrics for comprehensive testing
- Define clear agent workflow with logical progression
- Provide executable remediation commands
- Set appropriate confidence scores based on evidence strength

## License

This framework is part of OpsPulse AI and follows the project license.
