"""In-process Saga orchestration for multi-step remediation transactions.

Each completed forward action is placed on a compensation stack. If a later
step fails, compensations run in reverse order (best effort), and the result
retains both the original failure and any rollback failures for audit.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SagaStep:
    name: str
    action: Callable[[], Any]
    compensate: Callable[[Any], Any]


@dataclass
class SagaResult:
    transaction_id: str
    status: str
    completed_steps: list[str] = field(default_factory=list)
    results: dict[str, Any] = field(default_factory=dict)
    compensations: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None


class SagaExecutionError(RuntimeError):
    def __init__(self, message: str, result: SagaResult) -> None:
        super().__init__(message)
        self.result = result


class SagaOrchestrator:
    """Run actions in order and compensate completed work after any failure."""

    def execute(self, steps: Iterable[SagaStep]) -> SagaResult:
        transaction_id = uuid.uuid4().hex
        result = SagaResult(transaction_id=transaction_id, status="RUNNING")
        completed: list[tuple[SagaStep, Any]] = []
        try:
            for step in steps:
                value = step.action()
                completed.append((step, value))
                result.completed_steps.append(step.name)
                result.results[step.name] = value
                # A false success flag is a failed step even when no exception
                # was raised by the underlying client.
                if isinstance(value, dict) and value.get("success") is False:
                    raise RuntimeError(value.get("error") or f"{step.name} returned success=False")
                if getattr(value, "success", True) is False:
                    raise RuntimeError(getattr(value, "error", None) or f"{step.name} returned success=False")
            result.status = "COMPLETED"
            return result
        except Exception as exc:
            result.status = "COMPENSATING"
            result.error = f"{type(exc).__name__}: {exc}"
            rollback_failed = False
            for step, value in reversed(completed):
                try:
                    compensation_result = step.compensate(value)
                    result.compensations.append({
                        "step": step.name,
                        "success": True,
                        "result": compensation_result,
                    })
                except Exception as rollback_exc:
                    rollback_failed = True
                    logger.exception("Saga compensation failed for transaction=%s step=%s", transaction_id, step.name)
                    result.compensations.append({
                        "step": step.name,
                        "success": False,
                        "error": f"{type(rollback_exc).__name__}: {rollback_exc}",
                    })
            result.status = "COMPENSATION_FAILED" if rollback_failed else "COMPENSATED"
            raise SagaExecutionError(result.error, result) from exc
