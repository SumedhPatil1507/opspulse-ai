"""Unit tests for Saga compensation ordering and failure reporting."""

from __future__ import annotations

import pytest

from src.agent.saga_orchestrator import (
    SagaExecutionError,
    SagaOrchestrator,
    SagaStep,
)


def test_successful_saga_does_not_compensate() -> None:
    calls: list[str] = []
    result = SagaOrchestrator().execute([
        SagaStep("scale", lambda: calls.append("scale") or {"success": True},
                 lambda _value: calls.append("undo_scale")),
        SagaStep("verify", lambda: calls.append("verify") or {"success": True},
                 lambda _value: calls.append("undo_verify")),
    ])

    assert result.status == "COMPLETED"
    assert result.completed_steps == ["scale", "verify"]
    assert calls == ["scale", "verify"]


def test_downstream_failure_compensates_completed_steps_in_reverse_order() -> None:
    calls: list[str] = []

    def fail_downstream() -> None:
        calls.append("verify")
        raise RuntimeError("health check failed")

    with pytest.raises(SagaExecutionError) as raised:
        SagaOrchestrator().execute([
            SagaStep("scale", lambda: calls.append("scale") or {"success": True},
                     lambda _value: calls.append("undo_scale")),
            SagaStep("shift_route", lambda: calls.append("shift") or {"success": True},
                     lambda _value: calls.append("restore_route")),
            SagaStep("verify", fail_downstream, lambda _value: None),
        ])

    assert calls == ["scale", "shift", "verify", "restore_route", "undo_scale"]
    assert raised.value.result.status == "COMPENSATED"
    assert [item["step"] for item in raised.value.result.compensations] == [
        "shift_route", "scale",
    ]


def test_false_success_result_is_compensated() -> None:
    calls: list[str] = []

    with pytest.raises(SagaExecutionError) as raised:
        SagaOrchestrator().execute([
            SagaStep("restart", lambda: {"success": False, "error": "API timeout"},
                     lambda value: calls.append(value["error"])),
        ])

    assert calls == ["API timeout"]
    assert raised.value.result.status == "COMPENSATED"


def test_compensation_failure_is_retained_in_saga_result() -> None:
    def fail_compensation(_value: object) -> None:
        raise RuntimeError("rollback API unavailable")

    with pytest.raises(SagaExecutionError) as raised:
        SagaOrchestrator().execute([
            SagaStep("scale", lambda: {"success": True}, fail_compensation),
            SagaStep("verify", lambda: (_ for _ in ()).throw(ValueError("verification failed")),
                     lambda _value: None),
        ])

    assert raised.value.result.status == "COMPENSATION_FAILED"
    rollback = raised.value.result.compensations[0]
    assert rollback["step"] == "scale"
    assert rollback["success"] is False
    assert "rollback API unavailable" in rollback["error"]
