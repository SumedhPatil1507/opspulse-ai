"""
OpsPulse AI — Kubernetes Outage Simulation Benchmark
=====================================================

Streams 10 complex, multi-pod failure scenarios into Kafka to stress-test
the agent's root-cause analysis pipeline end-to-end:

    INC-2026-001  Cascading OOMKilled cascade      (payment-service)
    INC-2026-002  CrashLoopBackOff missing Secret   (auth-service)
    INC-2026-003  CoreDNS resolution timeouts       (kube-system)
    INC-2026-004  PersistentVolume disk full        (database)
    INC-2026-005  CPU throttling                    (api-service)
    INC-2026-006  ImagePullBackOff registry outage  (order-service)
    INC-2026-007  NetworkPolicy connection blocked  (microservices)
    INC-2026-008  ResourceQuota exceeded            (development)
    INC-2026-009  Node NotReady / disk pressure     (all-namespaces)
    INC-2026-010  HPA scale failure                 (web-frontend)

Topic mapping
-------------
* application/container lines  -> ``k8s.pod.logs``      (PodLogEvent)
* kubelet/control-plane lines  -> ``k8s.system.alerts`` (SystemAlertEvent)

Usage
-----
    # Stream everything at 20 msg/s (requires a Kafka broker on :9092)
    python scripts/simulate_k8s_outages.py --rate 20

    # Only the OOM + CoreDNS + CrashLoop scenarios
    python scripts/simulate_k8s_outages.py --scenario 1 --scenario 2 --scenario 3

    # No broker? Dry-run prints what would be produced (used in CI)
    python scripts/simulate_k8s_outages.py --dry-run

    # Also produce intentionally-corrupt records to exercise the DLQ
    python scripts/simulate_k8s_outages.py --with-poison

Exit code is 0 on success, 1 if production failed or no scenario matched.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Path bootstrap so the script works from a bare checkout
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.api.core.config import get_settings  # noqa: E402

INCIDENT_DIR = PROJECT_ROOT / "incident-simulation" / "data"

# Components whose lines are control-plane alerts rather than pod logs.
_ALERT_COMPONENTS = {
    "kubelet",
    "kube-controller-manager",
    "kube-scheduler",
    "coredns",
    "kube-system",
}


# ---------------------------------------------------------------------------
# Scenario loading
# ---------------------------------------------------------------------------

def load_scenarios(scenario_ids: list[int] | None = None) -> list[dict[str, Any]]:
    """
    Load incident JSON fixtures from ``incident-simulation/data``.

    Parameters
    ----------
    scenario_ids : 1-based incident numbers to include (e.g. ``[1, 3]``).
                   ``None`` loads all available incidents.

    Returns
    -------
    List of incident dicts sorted by filename.
    """
    files = sorted(INCIDENT_DIR.glob("incident_*.json"))
    if not files:
        raise FileNotFoundError(
            f"No incident fixtures found in {INCIDENT_DIR}. "
            "Clone the full repository (incident-simulation/data/)."
        )

    selected: list[dict[str, Any]] = []
    for idx, path in enumerate(files, start=1):
        if scenario_ids and idx not in scenario_ids:
            continue
        with path.open(encoding="utf-8") as fh:
            selected.append(json.load(fh))
    return selected


def to_kafka_records(incident: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """
    Convert one incident fixture into ``(topic, payload)`` Kafka records.

    Control-plane components (kubelet, CoreDNS, ...) map to
    ``k8s.system.alerts``; everything else maps to ``k8s.pod.logs``.
    """
    settings = get_settings()
    records: list[tuple[str, dict[str, Any]]] = []

    for entry in incident.get("log_stream", []):
        component = str(entry.get("component", "")).lower()
        level = str(entry.get("level", "INFO")).upper()
        pod = entry.get("pod") or f"{str(incident.get('incident_type', 'incident')).lower()}-synthetic"

        is_control_plane = component in _ALERT_COMPONENTS
        if is_control_plane:
            records.append((settings.KAFKA_ALERTS_TOPIC, {
                "timestamp": entry.get("timestamp"),
                "severity": "critical" if level in ("ERROR", "CRITICAL") else "warning",
                "source": component or "kubelet",
                "namespace": incident.get("namespace", "default"),
                "reason": incident.get("incident_type", "Unknown"),
                "message": entry.get("message", ""),
                "affected_pods": incident.get("affected_pods", []),
                "environment": "production",
                "metadata": {
                    "incident_id": incident.get("incident_id"),
                    "node": entry.get("node"),
                },
            }))
        else:
            records.append((settings.KAFKA_LOGS_TOPIC, {
                "timestamp": entry.get("timestamp"),
                "namespace": incident.get("namespace", "default"),
                "pod_name": pod,
                "container_name": entry.get("container") or entry.get("component") or "app",
                "service_name": incident.get("namespace"),
                "log_level": level,
                "message": entry.get("message", ""),
                "raw_log": entry.get("message", ""),
                "stacktrace": entry.get("message") if level == "ERROR" else None,
                "environment": "production",
                "metadata": {
                    "incident_id": incident.get("incident_id"),
                    "node": entry.get("node"),
                    "component": entry.get("component"),
                },
            }))

    return records


def poison_records() -> list[tuple[str, dict[str, Any]]]:
    """
    Deliberately malformed records that MUST land in ``k8s.telemetry.dlq``.

    Exercises the schema-validation DLQ path (missing/empty required fields).
    """
    settings = get_settings()
    return [
        # Schema violation: missing required pod_name/message fields.
        (settings.KAFKA_LOGS_TOPIC, {"namespace": "default", "log_level": "ERROR"}),
        # Schema violation: empty required reason/message fields.
        (settings.KAFKA_ALERTS_TOPIC, {"reason": "", "message": ""}),
    ]


# ---------------------------------------------------------------------------
# Producer helpers
# ---------------------------------------------------------------------------

class DryRunProducer:
    """Print records instead of producing — used when no broker is present."""

    def __init__(self) -> None:
        self.count = 0

    def produce(self, topic: str, value: bytes, key: bytes | None = None) -> None:  # noqa: ARG002
        self.count += 1
        preview = value[:120].decode("utf-8", errors="replace")
        print(f"  [dry-run #{self.count}] {topic}: {preview}...")

    def flush(self, timeout: float | None = None) -> int:  # noqa: ARG002
        return 0


def make_real_producer(bootstrap: str):
    """Create a confluent-kafka Producer with durable acks."""
    from confluent_kafka import Producer

    return Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "linger.ms": 5,
        "client.id": f"opspulse-sim-{uuid.uuid4().hex[:8]}",
    })


def make_records(
    scenario_ids: list[int] | None,
    with_poison: bool,
) -> list[tuple[str, dict[str, Any]]]:
    """Flatten all selected scenarios into a single record list."""
    records: list[tuple[str, dict[str, Any]]] = []
    for incident in load_scenarios(scenario_ids):
        recs = to_kafka_records(incident)
        print(
            f"  + {incident['incident_id']}: {incident['incident_type']}"
            f" -> {len(recs)} records"
        )
        records.extend(recs)
    if with_poison:
        poison = poison_records()
        print(f"  ! {len(poison)} poison records (DLQ exercise)")
        records.extend(poison)
    return records


def stream(
    records: list[tuple[str, dict[str, Any]]],
    rate: float,
    dry_run: bool,
    bootstrap: str,
) -> int:
    """Produce records at ``rate`` messages/second. Returns produced count."""
    producer: Any = DryRunProducer() if dry_run else make_real_producer(bootstrap)
    delay = 1.0 / rate if rate > 0 else 0.0
    total = len(records)

    print(
        f"\nStreaming {total} records at {rate:g} msg/s "
        f"({'DRY RUN' if dry_run else bootstrap}) ...\n"
    )

    for i, (topic, payload) in enumerate(records, start=1):
        key = str(payload.get("pod_name") or payload.get("reason") or "event").encode()
        payload = dict(payload)
        # Inject a synthetic ingest timestamp so consumers see fresh events.
        payload.setdefault("timestamp", datetime.now(tz=timezone.utc).isoformat())
        payload["metadata"] = {
            **(payload.get("metadata") or {}),
            "simulation_seq": i,
            "simulation_total": total,
            "simulated_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        producer.produce(topic, value=json.dumps(payload).encode("utf-8"), key=key)
        if not dry_run and i % 100 == 0:
            producer.flush(timeout=10.0)
        if delay:
            time.sleep(delay)

    if not dry_run:
        producer.flush(timeout=10.0)
    return total


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream 10 Kubernetes outage scenarios into Kafka.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--scenario", type=int, action="append", dest="scenarios",
        metavar="N", help="1-based incident number to include (repeatable).",
    )
    parser.add_argument(
        "--rate", type=float, default=20.0,
        help="Messages per second (default: 20; 0 = as fast as possible).",
    )
    parser.add_argument(
        "--bootstrap", default=None,
        help="Kafka bootstrap servers (default: settings.KAFKA_BOOTSTRAP_SERVERS).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print records instead of producing (no broker required).",
    )
    parser.add_argument(
        "--with-poison", action="store_true",
        help="Append malformed records to exercise the DLQ path.",
    )
    parser.add_argument(
        "--list", action="store_true",
        help="List available scenarios and exit.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if args.list:
        for idx, incident in enumerate(load_scenarios(None), start=1):
            print(
                f"  {idx:2d}. {incident['incident_id']}  "
                f"{incident['incident_type']}  ({incident['namespace']})"
            )
        return 0

    try:
        selected = make_records(args.scenarios, args.with_poison)
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if not selected:
        print("ERROR: no scenarios selected (check --scenario values).", file=sys.stderr)
        return 1

    settings = get_settings()
    bootstrap = args.bootstrap or settings.KAFKA_BOOTSTRAP_SERVERS

    try:
        produced = stream(
            selected,
            rate=args.rate,
            dry_run=args.dry_run,
            bootstrap=bootstrap,
        )
    except Exception as exc:
        print(
            f"ERROR: production failed: {exc}\n"
            "Hint: is Kafka reachable? Start it with:\n"
            "  docker run -d --name opspulse-kafka -p 9092:9092 apache/kafka:3.7.0\n"
            "Or re-run with --dry-run to inspect records without a broker.",
            file=sys.stderr,
        )
        return 1

    print(f"\nDone. {produced} records streamed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

