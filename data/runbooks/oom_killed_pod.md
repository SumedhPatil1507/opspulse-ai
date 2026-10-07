# Runbook: Kubernetes Pod OOMKilled

## Error Signatures

```
OOMKilled: Container payment-service exceeded memory limit
```

```
Last State: Terminated
  Reason:    OOMKilled
  Exit Code: 137
```

```
java.lang.OutOfMemoryError: Java heap space
    at java.util.Arrays.copyOf(Arrays.java:3210)
```

## Severity
CRITICAL — container is repeatedly crash-looping; live traffic is impacted.

## Root Causes
1. Memory limit set too low for the actual workload.
2. Memory leak in application code (unbounded caches, retained references).
3. Sudden traffic spike exceeding baseline memory footprint.
4. JVM heap not tuned for container memory limits.

## Immediate Remediation

### Step 1 — Confirm OOMKill and inspect recent events
```bash
kubectl describe pod <pod-name> -n <namespace> | grep -A 10 "Last State"
kubectl get events -n <namespace> --sort-by='.lastTimestamp' | tail -20
```

### Step 2 — Check current resource limits
```bash
kubectl get pod <pod-name> -n <namespace> -o jsonpath=\
  '{.spec.containers[*].resources}'
```

### Step 3 — Temporarily raise memory limit (patch without redeployment)
```bash
kubectl patch deployment <deployment-name> -n <namespace> \
  --type=json \
  -p='[{"op":"replace","path":"/spec/template/spec/containers/0/resources/limits/memory","value":"1Gi"}]'
```

### Step 4 — Tune JVM heap (Java services)
Set env var in deployment manifest:
```yaml
env:
  - name: JAVA_OPTS
    value: "-Xms256m -Xmx768m -XX:+UseContainerSupport"
```

### Step 5 — Scale out to distribute memory pressure
```bash
kubectl scale deployment <deployment-name> -n <namespace> --replicas=4
```

## Permanent Fix
- Set Vertical Pod Autoscaler (VPA) in recommendation mode to right-size limits.
- Add JVM `-XX:MaxRAMPercentage=75.0` so heap scales with container memory.
- Profile heap with async-profiler or Eclipse Memory Analyzer.
- Add `requests.memory` ≈ 80% of `limits.memory` to enable VPA recommendations.

## Escalation
If crash-loop continues after limit increase, page the Platform Engineering
on-call: PagerDuty service `kubernetes-infra`. Attach `kubectl logs` from the
previous container instance: `kubectl logs <pod> --previous`.
