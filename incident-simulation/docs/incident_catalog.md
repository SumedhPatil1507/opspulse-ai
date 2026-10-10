# Incident Catalog

Complete catalog of all 10 incident scenarios in the OpsPulse AI simulation framework.

## Incident Summary Table

| ID | Type | Namespace | Complexity | Agents Required | Overall Confidence |
|----|------|-----------|------------|-----------------|-------------------|
| INC-2026-001 | Cascading OOMKilled | payment-service | High | 5 | 0.94 |
| INC-2026-002 | CrashLoopBackOff Missing Secret | auth-service | Medium | 5 | 0.96 |
| INC-2026-003 | CoreDNS Resolution Timeout | kube-system | High | 5 | 0.93 |
| INC-2026-004 | PersistentVolume Disk Full | database | High | 5 | 0.95 |
| INC-2026-005 | CPU Throttling | api-service | Medium | 5 | 0.95 |
| INC-2026-006 | ImagePullBackOff Registry Unavailable | order-service | Medium | 5 | 0.93 |
| INC-2026-007 | Network Policy Connection Blocked | microservices | Medium | 5 | 0.96 |
| INC-2026-008 | Resource Quota Exceeded | development | Low | 5 | 0.96 |
| INC-2026-009 | Node NotReady Disk Pressure | all-namespaces | Medium | 5 | 0.95 |
| INC-2026-010 | HPA Scale Failure | web-frontend | Medium | 5 | 0.94 |

## Detailed Incident Descriptions

### INC-2026-001: Cascading OOMKilled Errors

**Scenario**: Memory exhaustion in payment service leads to cascading failures across Redis cache and API gateway.

**Key Characteristics**:
- 5 pods affected across 3 services
- Memory limits too low for peak load
- Cascading failure pattern
- High restart rate (5+ per pod)

**Detection Signals**:
- OOMKilled events in logs
- Memory usage spikes
- Exit code 137
- CrashLoopBackOff state

**LangGraph Agents**: LogAnalyzer, MetricsCorrelator, TopologyMapper, RootCauseAnalyzer, RemediationAdvisor

**Remediation Focus**: Increase memory limits, enable HPA, add PDB

---

### INC-2026-002: CrashLoopBackOff Missing Secret

**Scenario**: Authentication service fails to start due to missing Kubernetes secrets for JWT signing and database credentials.

**Key Characteristics**:
- All 3 pods in CrashLoopBackOff
- Exit code 1 (application error)
- Secret not found errors
- No graceful degradation

**Detection Signals**:
- CrashLoopBackOff state
- Secret not found error messages
- Exit code 1
- Zero ready replicas

**LangGraph Agents**: LogAnalyzer, SecretValidator, ConfigurationAuditor, RootCauseAnalyzer, RemediationAdvisor

**Remediation Focus**: Create missing secrets, configure external-secrets, add startup validation

---

### INC-2026-003: CoreDNS Resolution Timeout

**Scenario**: External DNS server (8.8.8.8) experiencing high latency causes DNS resolution failures across cluster.

**Key Characteristics**:
- 98% DNS query timeout rate
- 95% cache miss rate
- 5s+ query latency
- All services affected

**Detection Signals**:
- DNS query timeout errors
- High cache miss rate
- Upstream DNS latency
- Connection resolution failures

**LangGraph Agents**: LogAnalyzer, NetworkDiagnostics, DNSValidator, RootCauseAnalyzer, RemediationAdvisor

**Remediation Focus**: Add multiple upstream DNS servers, enable caching, increase timeout

---

### INC-2026-004: PersistentVolume Disk Full

**Scenario**: PostgreSQL PVC at 98% capacity blocks write operations and causes replication lag.

**Key Characteristics**:
- Disk usage at 98% (490GB/500GB)
- Write operations blocked
- WAL rotation failed
- 10-minute replication lag

**Detection Signals**:
- "No space left on device" errors
- High disk usage metrics
- WAL write failures
- Database in recovery mode

**LangGraph Agents**: LogAnalyzer, StorageDiagnostics, DatabaseAuditor, RootCauseAnalyzer, RemediationAdvisor

**Remediation Focus**: Expand PVC, force vacuum, rotate WAL logs, configure retention policy

---

### INC-2026-005: CPU Throttling Performance Degradation

**Scenario**: CPU limits too restrictive cause 45% CPU throttling and increased latency.

**Key Characteristics**:
- 45% CPU throttling rate
- p95 latency at 2500ms (baseline: 200ms)
- CPU at 100% limit
- No horizontal scaling

**Detection Signals**:
- CPU throttling logs
- High throttling rate metrics
- Increased latency
- CPU at limit

**LangGraph Agents**: LogAnalyzer, MetricsCorrelator, ResourceAnalyzer, RootCauseAnalyzer, RemediationAdvisor

**Remediation Focus**: Increase CPU limits, enable HPA, add priority class, configure PDB

---

### INC-2026-006: ImagePullBackOff Registry Unavailable

**Scenario**: DNS resolution failure for Azure Container Registry prevents image pulls.

**Key Characteristics**:
- All 3 pods in ImagePullBackOff
- DNS resolution failed for ACR
- Connection refused errors
- TLS handshake timeout

**Detection Signals**:
- Image pull errors
- DNS resolution failures
- ImagePullBackOff state
- Network unreachable

**LangGraph Agents**: LogAnalyzer, NetworkDiagnostics, RegistryValidator, RootCauseAnalyzer, RemediationAdvisor

**Remediation Focus**: Configure Azure DNS forwarder, update CoreDNS, re-attach ACR to AKS

---

### INC-2026-007: Network Policy Connection Blocked

**Scenario**: Overly restrictive network policy "deny-all-egress" blocks all traffic in microservices namespace.

**Key Characteristics**:
- 98% connection block rate
- Inter-service communication failures
- External API calls blocked
- 15,000 packet drops/sec

**Detection Signals**:
- Network policy deny logs
- High packet drop rate
- Connection refused errors
- Service-to-service failures

**LangGraph Agents**: LogAnalyzer, NetworkPolicyAuditor, ConnectivityValidator, RootCauseAnalyzer, RemediationAdvisor

**Remediation Focus**: Remove restrictive policy, apply permissive policy, allow necessary traffic

---

### INC-2026-008: Resource Quota Exceeded

**Scenario**: Development namespace resource quotas too low for workload requirements.

**Key Characteristics**:
- CPU quota at 95% (9500m/10000m)
- Memory quota at 95% (19Gi/20Gi)
- Services quota at 90% (9/10)
- ConfigMaps quota at 98% (49/50)

**Detection Signals**:
- Quota exceeded errors
- Pending pods
- Resource creation failures
- Insufficient quota metrics

**LangGraph Agents**: LogAnalyzer, QuotaAuditor, ResourceAnalyzer, RootCauseAnalyzer, RemediationAdvisor

**Remediation Focus**: Increase quota limits, clean up old resources, consider namespace splitting

---

### INC-2026-009: Node NotReady Disk Pressure

**Scenario**: Node disk at 98% capacity causes NodeNotReady state and pod evictions.

**Key Characteristics**:
- Disk usage at 98%
- Node marked NotReady
- Pods evicted from node
- Disk pressure taint applied

**Detection Signals**:
- Disk pressure condition
- Node NotReady status
- Pod eviction logs
- High disk usage metrics

**LangGraph Agents**: LogAnalyzer, NodeHealthChecker, StorageDiagnostics, RootCauseAnalyzer, RemediationAdvisor

**Remediation Focus**: Cordon and drain node, clean up Docker images, configure periodic cleanup

---

### INC-2026-010: HPA Scale Failure Insufficient Metrics

**Scenario**: Metrics server not responding prevents Horizontal Pod Autoscaler from scaling under load.

**Key Characteristics**:
- HPA stuck at 2 replicas (desired: 10)
- Metrics server health check failed
- Request rate at 5000 req/s
- p95 latency at 3000ms

**Detection Signals**:
- Missing CPU metrics
- Metrics server not responding
- HPA unable to scale
- High load with no scaling

**LangGraph Agents**: LogAnalyzer, MetricsValidator, HPAAuditor, RootCauseAnalyzer, RemediationAdvisor

**Remediation Focus**: Restart metrics server, increase resource limits, manually scale, add memory metrics

---

## Agent Usage Frequency

| Agent | Usage Count | Percentage |
|-------|-------------|------------|
| LogAnalyzer | 10 | 100% |
| RootCauseAnalyzer | 10 | 100% |
| RemediationAdvisor | 10 | 100% |
| MetricsCorrelator | 2 | 20% |
| NetworkDiagnostics | 2 | 20% |
| TopologyMapper | 1 | 10% |
| SecretValidator | 1 | 10% |
| ConfigurationAuditor | 1 | 10% |
| DNSValidator | 1 | 10% |
| StorageDiagnostics | 2 | 20% |
| DatabaseAuditor | 1 | 10% |
| ResourceAnalyzer | 2 | 20% |
| RegistryValidator | 1 | 10% |
| NetworkPolicyAuditor | 1 | 10% |
| ConnectivityValidator | 1 | 10% |
| QuotaAuditor | 1 | 10% |
| NodeHealthChecker | 1 | 10% |
| MetricsValidator | 1 | 10% |
| HPAAuditor | 1 | 10% |

## Complexity Distribution

- **High Complexity**: 3 incidents (30%) - Cascading failures, DNS issues, storage issues
- **Medium Complexity**: 6 incidents (60%) - Most common scenarios
- **Low Complexity**: 1 incident (10%) - Resource quota issues

## Testing Recommendations

1. **Start with low complexity**: INC-2026-008 (Resource Quota)
2. **Progress to medium**: INC-2026-002, INC-2026-005, INC-2026-006, INC-2026-007, INC-2026-009, INC-2026-010
3. **Test high complexity last**: INC-2026-001, INC-2026-003, INC-2026-004

## Coverage Areas

The framework covers the following Kubernetes incident categories:

- **Resource Management**: Memory, CPU, quotas (4 incidents)
- **Storage**: PVC, disk space (2 incidents)
- **Networking**: DNS, network policies (2 incidents)
- **Configuration**: Secrets, image registry (2 incidents)
- **Scalability**: HPA, autoscaling (1 incident)
- **Node Health**: Node readiness, disk pressure (1 incident)
