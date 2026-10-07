# Runbook: Redis Connection Refused / Timeout

## Error Signatures

```
redis.exceptions.ConnectionError: Error 111 connecting to localhost:6379. Connection refused.
```

```
redis.exceptions.TimeoutError: Timeout reading from socket
```

```
celery.exceptions.OperationalError: [Errno 111] Connection refused
```

```
kombu.exceptions.OperationalError: [Errno 110] Connection timed out
```

## Severity
HIGH — task queue is unavailable; all async jobs will fail to enqueue or consume.

## Root Causes
1. Redis process crashed or was OOMKilled.
2. Network policy or firewall rule blocking port 6379.
3. Redis `maxmemory` reached with `maxmemory-policy noeviction`.
4. Too many connected clients hitting `maxclients` limit.
5. Redis Sentinel failover in progress (transient).

## Immediate Remediation

### Step 1 — Check Redis process health
```bash
# On the Redis host
systemctl status redis-server
redis-cli ping   # expect: PONG
```

If the process is dead:
```bash
sudo systemctl start redis-server
sudo journalctl -u redis-server -n 50 --no-pager
```

### Step 2 — Inspect memory usage
```bash
redis-cli info memory | grep -E "used_memory_human|maxmemory_human|mem_fragmentation_ratio"
```

If `used_memory >= maxmemory`:
```bash
# Emergency: flush expired keys
redis-cli --scan --pattern '*' | xargs redis-cli unlink
# Or change eviction policy temporarily
redis-cli config set maxmemory-policy allkeys-lru
```

### Step 3 — Check client count
```bash
redis-cli info clients | grep connected_clients
redis-cli config get maxclients
```

Raise the limit if needed:
```bash
redis-cli config set maxclients 1000
```

### Step 4 — Verify network connectivity from app host
```bash
nc -zv <redis-host> 6379
telnet <redis-host> 6379
```

### Step 5 — Restart Celery workers after Redis recovers
```bash
celery -A src.worker.celery_app control shutdown
# Workers will be restarted by the process supervisor (systemd / Kubernetes)
```

## Permanent Fix
- Enable Redis persistence (`appendonly yes`) to survive restarts.
- Deploy Redis Sentinel or Redis Cluster for HA.
- Set `maxmemory-policy allkeys-lru` to prevent hard OOM blocks.
- Add liveness probe to Kubernetes Redis deployment:
  ```yaml
  livenessProbe:
    exec:
      command: ["redis-cli", "ping"]
    initialDelaySeconds: 10
    periodSeconds: 5
  ```

## Escalation
If Redis cannot be recovered within 5 minutes, failover to the standby
Redis instance by updating `REDIS_URL` in the app's Secret and rolling
the deployment. Page `#platform-oncall` in PagerDuty.
