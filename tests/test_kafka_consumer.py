"""
Unit tests for OpsPulse AI Kafka Telemetry Event Streaming Consumer.
====================================================================
Verifies:
1. Pod log and system alert stream consumption.
2. Manual offset commit management (enable.auto.commit=False).
3. Dead Letter Queue (DLQ) routing for malformed JSON and schema validation errors.
4. Error spike detection & LangGraph RCA workflow invocation.
5. System alert severity handling.
6. Polling loop and graceful shutdown.
"""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, call, patch

import pytest

from src.agent.state import WorkflowStatus
from src.streaming.kafka_consumer import (
    DLQEnvelope,
    ErrorSpikeTracker,
    KafkaTelemetryConsumer,
    SystemAlertEvent,
    PodLogEvent,
)


# ---------------------------------------------------------------------------
# Helpers & Fixtures
# ---------------------------------------------------------------------------

class FakeKafkaMessage:
    """Mock confluent_kafka.Message object."""

    def __init__(
        self,
        value: str | bytes,
        topic: str = "k8s.pod.logs",
        partition: int = 0,
        offset: int = 42,
        key: str | bytes | None = None,
        error_val: Any = None,
    ) -> None:
        self._value = value.encode("utf-8") if isinstance(value, str) else value
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._key = key.encode("utf-8") if isinstance(key, str) else key
        self._error = error_val

    def value(self) -> bytes:
        return self._value

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def key(self) -> bytes | None:
        return self._key

    def error(self) -> Any:
        return self._error


@pytest.fixture
def mock_consumer() -> MagicMock:
    consumer = MagicMock()
    consumer.commit = MagicMock()
    consumer.poll = MagicMock()
    consumer.close = MagicMock()
    return consumer


@pytest.fixture
def mock_producer() -> MagicMock:
    producer = MagicMock()
    producer.produce = MagicMock()
    producer.flush = MagicMock()
    return producer


@pytest.fixture
def mock_workflow_invoker() -> MagicMock:
    mock = MagicMock()
    mock.return_value = {
        "workflow_status": WorkflowStatus.COMPLETED,
        "analysis_summary": "Root cause identified: ConnectionPoolTimeout",
    }
    return mock


@pytest.fixture
def telemetry_consumer(
    mock_consumer: MagicMock,
    mock_producer: MagicMock,
    mock_workflow_invoker: MagicMock,
) -> KafkaTelemetryConsumer:
    tracker = ErrorSpikeTracker(threshold=3, window_seconds=60, cooldown_seconds=10)
    return KafkaTelemetryConsumer(
        bootstrap_servers="localhost:9092",
        group_id="test-consumer-group",
        logs_topic="k8s.pod.logs",
        alerts_topic="k8s.system.alerts",
        dlq_topic="k8s.telemetry.dlq",
        consumer=mock_consumer,
        producer=mock_producer,
        spike_tracker=tracker,
        workflow_invoker=mock_workflow_invoker,
    )


# ---------------------------------------------------------------------------
# Test Group 1: Pod Log and Node Metric Ingestion
# ---------------------------------------------------------------------------

class TestTelemetryIngestion:
    """Tests for parsing and handling standard telemetry streams."""

    def test_valid_info_pod_log_processed_and_offset_committed(
        self,
        telemetry_consumer: KafkaTelemetryConsumer,
        mock_consumer: MagicMock,
    ) -> None:
        payload = {
            "namespace": "production",
            "pod_name": "checkout-svc-789df-12",
            "container_name": "checkout",
            "service_name": "checkout-svc",
            "log_level": "INFO",
            "message": "Order 9821 processed successfully",
        }
        msg = FakeKafkaMessage(value=json.dumps(payload), topic="k8s.pod.logs", offset=101)

        result = telemetry_consumer.process_message(msg)

        assert result["status"] == "processed"
        assert result["log_level"] == "INFO"
        # Manual offset commit must be called for this message
        mock_consumer.commit.assert_called_once_with(message=msg, asynchronous=False)

    def test_valid_node_metric_processed(
        self,
        telemetry_consumer: KafkaTelemetryConsumer,
        mock_consumer: MagicMock,
    ) -> None:
        payload = {
            "severity": "warning",
            "source": "kubelet",
            "namespace": "production",
            "reason": "ContainerHasRestarted",
            "message": "Container checkout restarted 1 time(s)",
            "affected_pods": ["checkout-svc-789df-12"],
        }
        msg = FakeKafkaMessage(value=json.dumps(payload), topic="k8s.system.alerts", offset=102)

        result = telemetry_consumer.process_message(msg)

        assert result["status"] == "alert_recorded"
        assert result["reason"] == "ContainerHasRestarted"
        # Non-critical alerts never enter the RCA pipeline.
        mock_consumer.commit.assert_called_once_with(message=msg, asynchronous=False)

    def test_anomalous_node_metric_detected(
        self,
        telemetry_consumer: KafkaTelemetryConsumer,
        mock_consumer: MagicMock,
        mock_workflow_invoker: MagicMock,
    ) -> None:
        """A critical system alert triggers LangGraph triage immediately."""
        payload = {
            "severity": "critical",
            "source": "kubelet",
            "namespace": "payment-service",
            "reason": "OOMKilled",
            "message": "Container payment-app OOMKilled, exit code 137",
            "affected_pods": ["payment-processor-7f8b9-k2x4m"],
        }
        msg = FakeKafkaMessage(value=json.dumps(payload), topic="k8s.system.alerts", offset=103)

        result = telemetry_consumer.process_message(msg)

        assert result["status"] == "critical_alert_triaged"
        assert result["reason"] == "OOMKilled"
        mock_workflow_invoker.assert_called_once()
        # Offset committed only AFTER triage returned.
        mock_consumer.commit.assert_called_once_with(message=msg, asynchronous=False)


# ---------------------------------------------------------------------------
# Test Group 2: Dead Letter Queue (DLQ) & Malformed Records
# ---------------------------------------------------------------------------

class TestDeadLetterQueueHandling:
    """Tests for DLQ routing and unblocking poison pill records."""

    def test_malformed_json_routed_to_dlq_and_offset_committed(
        self,
        telemetry_consumer: KafkaTelemetryConsumer,
        mock_consumer: MagicMock,
        mock_producer: MagicMock,
    ) -> None:
        corrupted_payload = "{bad_json: missing_quotes"
        msg = FakeKafkaMessage(value=corrupted_payload, topic="k8s.pod.logs", offset=201, key="pod-key-1")

        result = telemetry_consumer.process_message(msg)

        assert result["status"] == "dlq"
        assert result["reason"] == "invalid_json"

        # Producer must send to DLQ topic
        mock_producer.produce.assert_called_once()
        call_kwargs = mock_producer.produce.call_args.kwargs
        assert call_kwargs["topic"] == "k8s.telemetry.dlq"
        assert call_kwargs["key"] == b"pod-key-1"

        dlq_data = json.loads(call_kwargs["value"].decode("utf-8"))
        assert dlq_data["original_topic"] == "k8s.pod.logs"
        assert dlq_data["offset"] == 201
        assert dlq_data["error_type"] == "JSONDecodeError"
        assert dlq_data["raw_payload"] == corrupted_payload

        # Manual offset commit must still occur so pipeline does not stall
        mock_consumer.commit.assert_called_once_with(message=msg, asynchronous=False)

    def test_schema_validation_failure_routed_to_dlq(
        self,
        telemetry_consumer: KafkaTelemetryConsumer,
        mock_consumer: MagicMock,
        mock_producer: MagicMock,
    ) -> None:
        # Missing required field 'pod_name'
        invalid_schema = {
            "namespace": "default",
            "log_level": "ERROR",
            # missing pod_name and message
        }
        msg = FakeKafkaMessage(value=json.dumps(invalid_schema), topic="k8s.pod.logs", offset=202)

        result = telemetry_consumer.process_message(msg)

        assert result["status"] == "dlq"
        assert result["reason"] == "validation_error"

        mock_producer.produce.assert_called_once()
        mock_consumer.commit.assert_called_once_with(message=msg, asynchronous=False)


# ---------------------------------------------------------------------------
# Test Group 3: Error Spike Tracker & LangGraph RCA Integration
# ---------------------------------------------------------------------------

class TestErrorSpikeAndRCAWorkflow:
    """Tests for sliding window error spike detection and LangGraph triggering."""

    def test_single_error_logged_without_triggering_rca(
        self,
        telemetry_consumer: KafkaTelemetryConsumer,
        mock_workflow_invoker: MagicMock,
    ) -> None:
        payload = {
            "namespace": "production",
            "pod_name": "payment-api-abc",
            "service_name": "payment-service",
            "log_level": "ERROR",
            "message": "Failed to connect to database timeout 5000ms",
        }
        msg = FakeKafkaMessage(value=json.dumps(payload), topic="k8s.pod.logs", offset=301)

        result = telemetry_consumer.process_message(msg)

        assert result["status"] == "error_logged"
        assert result["current_error_count"] == 1
        mock_workflow_invoker.assert_not_called()

    def test_error_spike_triggers_langgraph_rca_workflow(
        self,
        telemetry_consumer: KafkaTelemetryConsumer,
        mock_workflow_invoker: MagicMock,
    ) -> None:
        # Threshold is set to 3 in fixture
        for i in range(2):
            payload = {
                "namespace": "production",
                "pod_name": "payment-api-abc",
                "service_name": "payment-service",
                "log_level": "ERROR",
                "message": f"Connection error attempt {i+1}",
                "stacktrace": "Traceback:\n  File 'db.py', line 45, in connect\nTimeoutError: Connection timed out",
            }
            msg = FakeKafkaMessage(value=json.dumps(payload), topic="k8s.pod.logs", offset=400 + i)
            res = telemetry_consumer.process_message(msg)
            assert res["status"] == "error_logged"

        # 3rd error breaches threshold -> triggers RCA workflow
        third_payload = {
            "namespace": "production",
            "pod_name": "payment-api-abc",
            "service_name": "payment-service",
            "log_level": "ERROR",
            "message": "Fatal: Connection pool exhausted",
            "stacktrace": "Traceback:\n  File 'pool.py', line 120, in get_connection\nPoolExhaustedError",
        }
        msg3 = FakeKafkaMessage(value=json.dumps(third_payload), topic="k8s.pod.logs", offset=402)
        res3 = telemetry_consumer.process_message(msg3)

        assert res3["status"] == "spike_triggered_rca"
        assert res3["entity"] == "production/payment-service"
        assert res3["error_count"] == 3

        # Verify LangGraph invocation
        mock_workflow_invoker.assert_called_once()
        invoked_state = mock_workflow_invoker.call_args[0][0]
        assert "alert_data" in invoked_state
        assert invoked_state["alert_data"]["service_name"] == "payment-service"
        assert "PoolExhaustedError" in invoked_state["alert_data"]["raw_log_stacktrace"]

    def test_sliding_window_eviction(self) -> None:
        tracker = ErrorSpikeTracker(threshold=3, window_seconds=10)
        log1 = PodLogEvent(pod_name="auth-pod", message="error 1", log_level="ERROR")

        # Record at t=0
        is_spike, count, _ = tracker.record_error(log1, now_ts=100.0)
        assert count == 1
        assert not is_spike

        # Record at t=5
        is_spike, count, _ = tracker.record_error(log1, now_ts=105.0)
        assert count == 2
        assert not is_spike

        # Record at t=114 (window cutoff is 104; t=100 is evicted, t=105 remains)
        is_spike, count, _ = tracker.record_error(log1, now_ts=114.0)
        assert count == 2  # t=105 and t=114 are within the 10s window
        assert not is_spike

        # Record at t=130 (window cutoff is 120; both previous are evicted)
        is_spike, count, _ = tracker.record_error(log1, now_ts=130.0)
        assert count == 1
        assert not is_spike



# ---------------------------------------------------------------------------
# Test Group 4: Polling Loop and Lifecycle
# ---------------------------------------------------------------------------

class TestConsumerLifecycle:
    """Tests for poll loop and consumer closure."""

    def test_poll_once_none_message(
        self,
        telemetry_consumer: KafkaTelemetryConsumer,
        mock_consumer: MagicMock,
    ) -> None:
        mock_consumer.poll.return_value = None
        result = telemetry_consumer.poll_once(timeout=0.1)
        assert result is None

    def test_start_consuming_batch_limit(
        self,
        telemetry_consumer: KafkaTelemetryConsumer,
        mock_consumer: MagicMock,
    ) -> None:
        payload = {
            "namespace": "default",
            "pod_name": "nginx-pod",
            "message": "GET /health 200",
            "log_level": "INFO",
        }
        mock_consumer.poll.return_value = FakeKafkaMessage(
            value=json.dumps(payload), topic="k8s.pod.logs"
        )

        telemetry_consumer.start_consuming(max_messages=3)
        assert mock_consumer.poll.call_count == 3
        mock_consumer.close.assert_called_once()
