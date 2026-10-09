"""
OpsPulse AI — real-time telemetry streaming package.

Public exports
--------------
``KafkaTelemetryConsumer``  — manual-offset Kafka consumer with DLQ routing
                              and LangGraph triage triggering.
``PodLogEvent``             — `k8s.pod.logs` event schema.
``SystemAlertEvent``        — `k8s.system.alerts` event schema.
``ErrorSpikeTracker``       — sliding-window error spike detector.
"""

from src.streaming.kafka_consumer import (
    DLQEnvelope,
    ErrorSpikeTracker,
    KafkaTelemetryConsumer,
    PodLogEvent,
    SystemAlertEvent,
)

__all__ = [
    "DLQEnvelope",
    "ErrorSpikeTracker",
    "KafkaTelemetryConsumer",
    "PodLogEvent",
    "SystemAlertEvent",
]
