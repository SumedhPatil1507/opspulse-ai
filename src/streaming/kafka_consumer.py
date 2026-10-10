"""
OpsPulse AI — Kafka Telemetry Event Streaming Consumer & RCA Trigger.
======================================================================
Continuously consumes Kubernetes telemetry events from Kafka topics:
- `k8s.pod.logs`       — container / application log streams
- `k8s.system.alerts`  — cluster-level system alerts (node health, OOM,
                         CrashLoopBackOff, CoreDNS failures, ...)

Features:
---------
1. Manual Offset Commit Management (`enable.auto.commit=False`):
   - Ensures at-least-once delivery semantics.
   - Offsets are committed ONLY after the LangGraph triage workflow has
     successfully processed the record (or after the record has been
     quarantined in the Dead Letter Queue). A crash mid-triage therefore
     redelivers the record instead of silently losing it.
2. Dead Letter Queue (DLQ) Routing:
   - Malformed, corrupt, or unparseable telemetry records are caught and
     produced to `k8s.telemetry.dlq` along with error diagnostics, before
     committing the offset so the main partition is never blocked.
3. Error Spike Detection & LangGraph RCA Integration:
   - Maintains a sliding-window tracker of error frequencies by service/pod.
   - Automatically triggers the LangGraph root-cause analysis workflow
     (`build_workflow`) when an error spike threshold is reached; the offset
     for the triggering record is committed only after that triage returns.
"""

from __future__ import annotations

import collections
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

try:
    import confluent_kafka
    from confluent_kafka import Consumer, KafkaError, Message, Producer
    _CONFLUENT_KAFKA_AVAILABLE = True
except ImportError:  # pragma: no cover
    confluent_kafka = None  # type: ignore[assignment]
    Consumer = Any  # type: ignore[assignment,misc]
    Producer = Any  # type: ignore[assignment,misc]
    Message = Any  # type: ignore[assignment,misc]
    KafkaError = Any  # type: ignore[assignment,misc]
    _CONFLUENT_KAFKA_AVAILABLE = False

from pydantic import BaseModel, Field, ValidationError

from src.agent.state import IncidentState, initial_state
from src.agent.workflow import build_workflow
from src.api.core.config import Settings, get_settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Telemetry Event Models
# ---------------------------------------------------------------------------

class PodLogEvent(BaseModel):
    """Structured Kubernetes Pod log event schema (`k8s.pod.logs`)."""
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO-8601 timestamp of log emission.",
    )
    namespace: str = Field(default="default", min_length=1)
    pod_name: str = Field(..., min_length=1)
    container_name: str = Field(default="app", min_length=1)
    service_name: str | None = None
    log_level: str = Field(default="INFO")
    message: str = Field(..., min_length=1)
    raw_log: str | None = None
    stacktrace: str | None = None
    environment: str = Field(default="production")
    metadata: dict[str, Any] = Field(default_factory=dict)

    def is_error(self) -> bool:
        """Determine if log event signifies an error or critical failure."""
        lvl = (self.log_level or "").upper()
        if lvl in ("ERROR", "CRITICAL", "FATAL", "PANIC", "EMERGENCY"):
            return True
        if self.stacktrace:
            return True
        msg = (self.message or "").lower()
        if (
            "traceback (most recent call last)" in msg
            or "exception:" in msg
            or "fatal error" in msg
        ):
            return True
        return False


class SystemAlertEvent(BaseModel):
    """
    Structured Kubernetes system alert schema (`k8s.system.alerts`).

    Raised by control-plane components (kubelet, CoreDNS, scheduler, ...)
    rather than by application containers.
    """
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO-8601 timestamp of alert emission.",
    )
    alert_id: str = Field(
        default_factory=lambda: f"sys-{uuid.uuid4().hex[:12]}",
        description="Unique identifier for this system alert.",
    )
    severity: str = Field(
        default="warning",
        description="Alert severity: info | warning | critical.",
    )
    source: str = Field(
        default="kubelet",
        description="Emitting component, e.g. kubelet, coredns, scheduler.",
    )
    namespace: str = Field(default="default")
    reason: str = Field(..., min_length=1, description="Machine-readable reason, e.g. OOMKilled.")
    message: str = Field(..., min_length=1, description="Human-readable alert description.")
    affected_pods: list[str] = Field(default_factory=list)
    environment: str = Field(default="production")
    metadata: dict[str, Any] = Field(default_factory=dict)

    def is_critical(self) -> bool:
        """Critical alerts always route into the RCA pipeline."""
        return (self.severity or "").lower() in ("critical", "fatal", "page")


class DLQEnvelope(BaseModel):
    """Standard Dead Letter Queue payload envelope."""
    original_topic: str
    partition: int
    offset: int
    key: str | None = None
    error: str
    error_type: str
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    raw_payload: str


# ---------------------------------------------------------------------------
# Error Spike Detection Engine
# ---------------------------------------------------------------------------

@dataclass
class ErrorSpikeTracker:
    """
    Sliding-window counter for pod/service error occurrences.
    Deterministic tracking used to trigger RCA workflows.
    """
    threshold: int = 5
    window_seconds: int = 60
    # Map of entity_key -> deque of timestamps
    _error_windows: dict[str, collections.deque[float]] = field(
        default_factory=lambda: collections.defaultdict(collections.deque)
    )
    _recent_logs: dict[str, list[PodLogEvent]] = field(
        default_factory=lambda: collections.defaultdict(list)
    )
    _last_spike_triggered: dict[str, float] = field(default_factory=dict)
    # Cooldown to prevent triggering LangGraph repeatedly for the same spike
    cooldown_seconds: int = 120

    def record_error(
        self,
        log_event: PodLogEvent,
        now_ts: float | None = None,
    ) -> tuple[bool, int, list[PodLogEvent]]:
        """
        Record an error event and evaluate the sliding-window threshold.

        Returns
        -------
        (is_spike, current_error_count, recent_error_logs)
        """
        now = now_ts if now_ts is not None else time.time()
        entity_key = f"{log_event.namespace}/{log_event.service_name or log_event.pod_name}"

        dq = self._error_windows[entity_key]
        dq.append(now)

        # Evict timestamps older than the sliding window
        window_cutoff = now - self.window_seconds
        while dq and dq[0] < window_cutoff:
            dq.popleft()

        # Keep recent error logs for RCA diagnosis context
        logs_list = self._recent_logs[entity_key]
        logs_list.append(log_event)
        if len(logs_list) > 20:
            self._recent_logs[entity_key] = logs_list[-20:]

        current_count = len(dq)

        if current_count >= self.threshold:
            last_triggered = self._last_spike_triggered.get(entity_key, 0.0)
            if (now - last_triggered) >= self.cooldown_seconds:
                self._last_spike_triggered[entity_key] = now
                return True, current_count, list(self._recent_logs[entity_key])

        return False, current_count, []

    def reset(self) -> None:
        """Clear all tracking state."""
        self._error_windows.clear()
        self._recent_logs.clear()
        self._last_spike_triggered.clear()


# ---------------------------------------------------------------------------
# Kafka Telemetry Consumer Pipeline
# ---------------------------------------------------------------------------

class KafkaTelemetryConsumer:
    """
    Continuous Kafka event streaming pipeline for Kubernetes telemetry.

    Guarantees
    ----------
    * Manual offset commit management — ``enable.auto.commit=False`` is
      asserted at construction; offsets are committed exactly once per
      record, only **after** LangGraph triage succeeds or the record is
      quarantined in ``k8s.telemetry.dlq``.
    * Poison-pill isolation: malformed events never block a partition.
    * Error-spike detection that triggers LangGraph root-cause analysis.
    """

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        group_id: str | None = None,
        logs_topic: str | None = None,
        alerts_topic: str | None = None,
        dlq_topic: str | None = None,
        consumer: Consumer | None = None,
        producer: Producer | None = None,
        spike_tracker: ErrorSpikeTracker | None = None,
        workflow_invoker: Callable[[IncidentState], dict[str, Any]] | None = None,
        settings: Settings | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.bootstrap_servers = bootstrap_servers or self.settings.KAFKA_BOOTSTRAP_SERVERS
        self.group_id = group_id or self.settings.KAFKA_GROUP_ID
        self.logs_topic = logs_topic or self.settings.KAFKA_LOGS_TOPIC
        self.alerts_topic = alerts_topic or self.settings.KAFKA_ALERTS_TOPIC
        self.dlq_topic = dlq_topic or self.settings.KAFKA_DLQ_TOPIC

        # Safety: auto-commit must NEVER be enabled — offsets are managed
        # manually after triage/DLQ quarantine. Guard against config drift.
        if self.settings.KAFKA_ENABLE_AUTO_COMMIT:
            raise RuntimeError(
                "KAFKA_ENABLE_AUTO_COMMIT must be False — offsets are only "
                "committed manually after LangGraph triage completes."
            )

        self._consumer = consumer
        self._producer = producer
        self.spike_tracker = spike_tracker or ErrorSpikeTracker(
            threshold=self.settings.KAFKA_ERROR_SPIKE_THRESHOLD,
            window_seconds=self.settings.KAFKA_ERROR_SPIKE_WINDOW_SECONDS,
        )
        self._workflow_invoker = workflow_invoker
        self._is_running = False
        self._compiled_graph = None
        # Observability counters consumed by the Streamlit dashboard.
        self.stats: dict[str, int] = {
            "consumed": 0,
            "triaged": 0,
            "dlq": 0,
            "committed": 0,
        }

    def _get_consumer(self) -> Consumer:
        """Lazy-instantiate Kafka Consumer with manual offset commit config."""
        if self._consumer is not None:
            return self._consumer

        if not _CONFLUENT_KAFKA_AVAILABLE or confluent_kafka is None:
            raise RuntimeError("confluent-kafka package is not installed or available.")

        conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": self.settings.KAFKA_AUTO_OFFSET_RESET,
            "enable.auto.commit": False,  # Manual offset commit management
            "session.timeout.ms": 15000,
            "max.poll.interval.ms": 300000,
        }
        self._consumer = Consumer(conf)
        self._consumer.subscribe([self.logs_topic, self.alerts_topic])
        logger.info(
            "Subscribed Kafka consumer (group=%s) to topics: %s, %s",
            self.group_id,
            self.logs_topic,
            self.alerts_topic,
        )
        return self._consumer

    def _get_producer(self) -> Producer:
        """Lazy-instantiate Kafka Producer for DLQ publishing."""
        if self._producer is not None:
            return self._producer

        if not _CONFLUENT_KAFKA_AVAILABLE or confluent_kafka is None:
            raise RuntimeError("confluent-kafka package is not installed or available.")

        conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": f"{self.group_id}-dlq-producer",
            "acks": "all",
        }
        self._producer = Producer(conf)
        return self._producer

    # -----------------------------------------------------------------------
    # Dead Letter Queue routing
    # -----------------------------------------------------------------------

    def route_to_dlq(
        self,
        raw_payload: str,
        error_msg: str,
        error_type: str,
        topic: str,
        partition: int,
        offset: int,
        key: str | None = None,
    ) -> None:
        """
        Produce a failed record to ``k8s.telemetry.dlq`` with diagnostics.

        The record keeps its original payload plus an error envelope so it
        can be replayed after the root cause is fixed. Publishing is
        flushed synchronously so the subsequent offset commit can never
        precede quarantine.
        """
        envelope = DLQEnvelope(
            original_topic=topic,
            partition=partition,
            offset=offset,
            key=key,
            error=error_msg,
            error_type=error_type,
            raw_payload=raw_payload[:16384],
        )
        producer = self._get_producer()
        producer.produce(
            topic=self.dlq_topic,
            key=(key or f"{partition}-{offset}").encode("utf-8"),
            value=envelope.model_dump_json().encode("utf-8"),
        )
        producer.flush(timeout=10.0)
        self.stats["dlq"] += 1
        logger.warning(
            "Routed failed record to DLQ topic=%s partition=%s offset=%s error_type=%s: %s",
            topic, partition, offset, error_type, error_msg,
        )

    # -----------------------------------------------------------------------
    # LangGraph RCA integration
    # -----------------------------------------------------------------------

    def _get_workflow_app(self):
        """Compile (once) and return the LangGraph triage workflow."""
        if self._compiled_graph is None:
            self._compiled_graph = build_workflow(self.settings)
        return self._compiled_graph

    def trigger_rca_workflow(
        self,
        alert_id: str,
        entity_name: str,
        recent_events: list[Any],
        error_count: int,
    ) -> dict[str, Any]:
        """
        Build an alert payload from the spike context and run LangGraph triage.

        This call is **synchronous on purpose**: the caller commits the
        Kafka offset only after this returns, satisfying the
        "commit after triage" contract.
        """
        log_lines = []
        for ev in recent_events:
            if isinstance(ev, SystemAlertEvent):
                log_lines.append(
                    f"[{ev.timestamp}] {ev.severity.upper()} {ev.source}: {ev.message}"
                )
            else:
                line = (
                    f"[{ev.timestamp}] {ev.log_level} {ev.pod_name}: "
                    f"{ev.raw_log or ev.message}"
                )
                if ev.stacktrace:
                    line += f"\n{ev.stacktrace}"
                log_lines.append(line)

        alert_payload_dict = {
            "alert_id": alert_id or f"kafka-{uuid.uuid4().hex[:12]}",
            # entity_name is "namespace/service" — AlertPayload.service_name
            # carries just the service segment.
            "service_name": entity_name.rsplit("/", 1)[-1],
            "severity": "critical" if error_count >= self.spike_tracker.threshold * 2 else "high",
            "raw_log_stacktrace": "\n".join(log_lines),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "environment": "production",
        }

        logger.info(
            "Triggering LangGraph RCA workflow for error spike: alert_id=%s entity=%s count=%d",
            alert_id, entity_name, error_count,
        )

        state = initial_state(alert_payload_dict)

        if self._workflow_invoker is not None:
            return self._workflow_invoker(state)

        app = self._get_workflow_app()
        return app.invoke(state)

    # -----------------------------------------------------------------------
    # Single Message Processing Pipeline
    # -----------------------------------------------------------------------

    def process_message(self, msg: Any) -> dict[str, Any]:
        """
        Process a single Kafka message through the ingestion pipeline:

        1. Parse JSON payload.
        2. Route to PodLogEvent or SystemAlertEvent handler.
        3. Handlers run LangGraph triage when a spike/critical alert fires.
        4. On success → commit offset (post-triage).
        5. On failure → quarantine in ``k8s.telemetry.dlq`` → commit offset.

        Either way exactly one commit happens per record, and it never
        precedes triage or DLQ quarantine.
        """
        consumer = self._get_consumer()
        topic = getattr(msg, "topic", lambda: "unknown")() if callable(getattr(msg, "topic", None)) else getattr(msg, "topic", "unknown")
        partition = getattr(msg, "partition", lambda: 0)() if callable(getattr(msg, "partition", None)) else getattr(msg, "partition", 0)
        offset = getattr(msg, "offset", lambda: 0)() if callable(getattr(msg, "offset", None)) else getattr(msg, "offset", 0)
        key_raw = getattr(msg, "key", lambda: None)() if callable(getattr(msg, "key", None)) else getattr(msg, "key", None)
        key = key_raw.decode("utf-8") if isinstance(key_raw, bytes) else str(key_raw) if key_raw else None

        raw_val = getattr(msg, "value", lambda: b"")() if callable(getattr(msg, "value", None)) else getattr(msg, "value", b"")
        raw_str = raw_val.decode("utf-8") if isinstance(raw_val, bytes) else str(raw_val)

        self.stats["consumed"] += 1

        try:
            parsed_data = json.loads(raw_str)
            if not isinstance(parsed_data, dict):
                raise ValueError(f"Payload must be a JSON object, got {type(parsed_data).__name__}")
        except Exception as e:
            # Malformed JSON -> Route to DLQ, then commit (post-quarantine).
            self.route_to_dlq(
                raw_payload=raw_str,
                error_msg=f"JSON parsing error: {e}",
                error_type="JSONDecodeError",
                topic=topic,
                partition=partition,
                offset=offset,
                key=key,
            )
            self._commit_message(consumer, msg)
            return {"status": "dlq", "reason": "invalid_json", "error": str(e)}

        # Process by topic type
        try:
            if topic == self.alerts_topic:
                result = self._handle_system_alert(parsed_data)
            elif topic == self.logs_topic:
                result = self._handle_pod_log(parsed_data)
            else:
                # Default generic routing: check which schema fits
                if "reason" in parsed_data and "message" in parsed_data:
                    result = self._handle_system_alert(parsed_data)
                elif "pod_name" in parsed_data or "message" in parsed_data:
                    result = self._handle_pod_log(parsed_data)
                else:
                    raise ValueError(f"Unknown message format for topic '{topic}'")

            # Triage (if triggered) completed successfully → commit offset.
            self._commit_message(consumer, msg)
            return result

        except ValidationError as val_err:
            # Schema validation failure -> Route to DLQ, then commit.
            self.route_to_dlq(
                raw_payload=raw_str,
                error_msg=f"Schema validation error: {val_err}",
                error_type="ValidationError",
                topic=topic,
                partition=partition,
                offset=offset,
                key=key,
            )
            self._commit_message(consumer, msg)
            return {"status": "dlq", "reason": "validation_error", "error": str(val_err)}

        except Exception as proc_err:
            # Triage or handler crashed -> Route to DLQ, then commit so the
            # partition keeps flowing; the record can be replayed from the DLQ.
            logger.error("Processing error for message at offset %d: %s", offset, proc_err, exc_info=True)
            self.route_to_dlq(
                raw_payload=raw_str,
                error_msg=f"Runtime processing error: {proc_err}",
                error_type=proc_err.__class__.__name__,
                topic=topic,
                partition=partition,
                offset=offset,
                key=key,
            )
            self._commit_message(consumer, msg)
            return {"status": "dlq", "reason": "processing_error", "error": str(proc_err)}

    def _commit_message(self, consumer: Consumer, msg: Any) -> None:
        """Manually commit the offset for a fully processed (or quarantined) message."""
        try:
            if hasattr(consumer, "commit"):
                consumer.commit(message=msg, asynchronous=False)
                self.stats["committed"] += 1
        except Exception as e:
            logger.error("Failed to manually commit Kafka offset: %s", e)

    # -----------------------------------------------------------------------
    # Topic handlers
    # -----------------------------------------------------------------------

    def _handle_pod_log(self, data: dict[str, Any]) -> dict[str, Any]:
        """Validate pod log, track error spikes, and triage RCA on breach."""
        log_event = PodLogEvent.model_validate(data)

        if log_event.is_error():
            is_spike, count, recent_logs = self.spike_tracker.record_error(log_event)
            if is_spike:
                entity_name = f"{log_event.namespace}/{log_event.service_name or log_event.pod_name}"
                # LangGraph triage runs to completion BEFORE the caller
                # commits this record's offset.
                workflow_result = self.trigger_rca_workflow(
                    alert_id=f"spike-{uuid.uuid4().hex[:12]}",
                    entity_name=entity_name,
                    recent_events=recent_logs,
                    error_count=count,
                )
                self.stats["triaged"] += 1
                return {
                    "status": "spike_triggered_rca",
                    "entity": entity_name,
                    "error_count": count,
                    "workflow_result": workflow_result,
                }
            return {"status": "error_logged", "current_error_count": count}

        return {"status": "processed", "log_level": log_event.log_level}

    def _handle_system_alert(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        Validate a system alert; critical alerts trigger LangGraph triage
        immediately (single-event pages don't need a spike window).
        """
        alert_event = SystemAlertEvent.model_validate(data)

        if alert_event.is_critical():
            entity = f"{alert_event.namespace}/{alert_event.source}"
            workflow_result = self.trigger_rca_workflow(
                alert_id=alert_event.alert_id,
                entity_name=entity,
                recent_events=[alert_event],
                error_count=self.spike_tracker.threshold,
            )
            self.stats["triaged"] += 1
            return {
                "status": "critical_alert_triaged",
                "entity": entity,
                "reason": alert_event.reason,
                "workflow_result": workflow_result,
            }

        logger.info(
            "System alert recorded: %s (%s) %s",
            alert_event.reason, alert_event.severity, alert_event.message,
        )
        return {"status": "alert_recorded", "reason": alert_event.reason}

    # -----------------------------------------------------------------------
    # Continuous Polling Loop
    # -----------------------------------------------------------------------

    def poll_once(self, timeout: float = 1.0) -> dict[str, Any] | None:
        """Poll a single message and execute the processing pipeline."""
        consumer = self._get_consumer()
        msg = consumer.poll(timeout=timeout)

        if msg is None:
            return None

        # Check for Kafka error in message
        if hasattr(msg, "error") and callable(msg.error):
            err = msg.error()
            if err is not None:
                if hasattr(err, "code") and err.code() == getattr(KafkaError, "_PARTITION_EOF", -191):
                    logger.debug("Reached end of partition: %s", msg)
                    return None
                logger.error("Kafka consumer error: %s", err)
                return {"status": "kafka_error", "error": str(err)}

        return self.process_message(msg)

    def start_consuming(self, max_messages: int | None = None) -> None:
        """
        Start the continuous consumer loop.
        Allows setting `max_messages` for controlled testing and batch runs.
        """
        self._is_running = True
        logger.info("Starting Kafka Telemetry Consumer loop...")
        messages_processed = 0

        try:
            while self._is_running:
                result = self.poll_once(timeout=1.0)
                if result is not None:
                    messages_processed += 1
                    if max_messages is not None and messages_processed >= max_messages:
                        break
        except KeyboardInterrupt:
            logger.info("Kafka consumer interrupted by user.")
        finally:
            self.close()

    def close(self) -> None:
        """Gracefully close Kafka consumer and producer."""
        self._is_running = False
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception as e:
                logger.debug("Error closing Kafka consumer: %s", e)
        if self._producer is not None:
            try:
                self._producer.flush(timeout=5.0)
            except Exception as e:
                logger.debug("Error flushing Kafka producer: %s", e)
        logger.info("Kafka Telemetry Consumer shut down. stats=%s", self.stats)


# ---------------------------------------------------------------------------
# CLI entrypoint — `python -m src.streaming.kafka_consumer`
# ---------------------------------------------------------------------------

def main() -> None:
    """Run the telemetry consumer until interrupted (Ctrl-C)."""
    logging.basicConfig(level=logging.INFO)
    consumer = KafkaTelemetryConsumer()
    logger.info(
        "Consuming topics [%s, %s] -> DLQ [%s] (group=%s, auto-commit=OFF)",
        consumer.logs_topic,
        consumer.alerts_topic,
        consumer.dlq_topic,
        consumer.group_id,
    )
    consumer.start_consuming()


if __name__ == "__main__":
    main()





