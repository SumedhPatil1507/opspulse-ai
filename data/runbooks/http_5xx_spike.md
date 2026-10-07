# Runbook: HTTP 5xx Error Rate Spike

## Error Signatures

```
HTTPError: 503 Service Unavailable
```

```
upstream connect error or disconnect/reset before headers.
reset reason: connection failure, transport failure reason: delayed connect error
```

```
Traceback (most recent call last):
  File "app/middleware.py", line 88, in dispatch
    response = await call_next(request)
  File "app/routes/payments.py", line 54, in process_payment
    raise HTTPException(status_code=500, detail="Internal server error")
```

## Severity
CRITICAL — customer-facing errors; SLA breach likely if sustained > 5 minutes.

## Root Causes
1. Upstream dependency (database, cache, third-party API) is unavailable.
2. Unhandled exception in application code after a bad deploy.
3. Resource exhaustion (CPU throttling, memory pressure).
4. Configuration drift — missing env var or secret after rotation.

## Immediate Remediation

### Step 1 — Check error rate and identify failing endpoints
```bash
# Prometheus / Grafana query
sum(rate(http_requests_total{status=~"5.."}[5m])) by (path, status)
```

### Step 2 — Tail application logs for the root exception
```bash
kubectl logs -l app=<service-name> -n <namespace> --tail=100 | grep -i "error\|exception\|traceback"
```

### Step 3 — Check recent deployments
```bash
kubectl rollout history deployment/<deployment-name> -n <namespace>
```

If a bad deploy is suspected, rollback immediately:
```bash
kubectl rollout undo deployment/<deployment-name> -n <namespace>
kubectl rollout status deployment/<deployment-name> -n <namespace>
```

### Step 4 — Verify upstream dependencies
```bash
# Database
psql $DATABASE_URL -c "SELECT 1;"

# Redis
redis-cli -u $REDIS_URL ping

# Third-party API
curl -o /dev/null -sw "%{http_code}" https://api.third-party.com/health
```

### Step 5 — Check CPU / memory throttling
```bash
kubectl top pods -n <namespace> --sort-by=cpu
kubectl describe pod <throttled-pod> | grep -A5 "Limits\|Requests"
```

## Permanent Fix
- Implement circuit breakers (e.g., `tenacity` retry with exponential back-off).
- Add health checks for all upstream dependencies to the `/healthz` endpoint.
- Use structured error logging with `alert_id` correlation for faster triage.
- Set up automated rollback on error-rate alert via ArgoCD or Flux.

## Escalation
If 5xx rate exceeds 10% for > 5 minutes and rollback does not resolve it,
declare an incident in PagerDuty (`#sre-oncall`) and open a War Room bridge.
