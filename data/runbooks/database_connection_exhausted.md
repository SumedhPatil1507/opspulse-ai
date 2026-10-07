# Runbook: Database Connection Pool Exhausted

## Error Signatures

```
sqlalchemy.exc.TimeoutError: QueuePool limit of size 5 overflow 10 reached,
connection timed out, timeout 30
```

```
psycopg2.OperationalError: FATAL: remaining connection slots are reserved
for non-replication superuser connections
```

## Severity
HIGH — service degradation; new requests will fail until connections are released.

## Root Causes
1. Long-running transactions holding connections open.
2. Connection pool misconfiguration (`pool_size` too small for traffic).
3. Application leak — connections not returned to pool after exceptions.
4. Runaway batch job consuming all available slots.

## Immediate Remediation

### Step 1 — Identify connection hogs
```sql
SELECT pid, usename, application_name, state, query_start,
       now() - query_start AS duration, query
FROM   pg_stat_activity
WHERE  state != 'idle'
ORDER  BY duration DESC
LIMIT  20;
```

### Step 2 — Kill long-running idle-in-transaction connections
```sql
SELECT pg_terminate_backend(pid)
FROM   pg_stat_activity
WHERE  state = 'idle in transaction'
  AND  now() - query_start > interval '5 minutes';
```

### Step 3 — Temporarily raise max_connections (requires restart)
Edit `/etc/postgresql/14/main/postgresql.conf`:
```
max_connections = 200   # was 100
```
Then: `sudo systemctl restart postgresql`

### Step 4 — Increase application pool size
In `config.py` or env vars:
```
DB_POOL_SIZE=20
DB_MAX_OVERFLOW=10
DB_POOL_TIMEOUT=60
```

## Permanent Fix
- Add connection pool monitoring (PgBouncer or RDS Proxy).
- Set statement timeouts: `SET statement_timeout = '30s';`
- Enable `pool_pre_ping=True` in SQLAlchemy to detect stale connections.

## Escalation
If connections are not released within 10 minutes after killing idle sessions,
escalate to the DBA on-call via PagerDuty runbook tag `#db-oncall`.
