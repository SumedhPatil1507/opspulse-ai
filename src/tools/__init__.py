"""
src.tools — Safe execution tooling for the OpsPulse AI agent.

Public surface
--------------
ContainerSandbox    — Docker SDK wrapper for safe container operations
ContainerStatus     — Dataclass holding container inspection results
ContainerLogResult  — Dataclass holding retrieved log lines
RestartResult       — Dataclass holding restart operation outcome
SandboxError        — Base exception for all sandbox failures
ContainerNotFound   — Raised when a container ID/name does not exist
AllowlistViolation  — Raised when a container is not in the allowed prefix list
"""

from src.tools.sandbox import (
    AllowlistViolation,
    ContainerLogResult,
    ContainerNotFound,
    ContainerSandbox,
    ContainerStatus,
    RestartResult,
    SandboxError,
)

__all__ = [
    "AllowlistViolation",
    "ContainerLogResult",
    "ContainerNotFound",
    "ContainerSandbox",
    "ContainerStatus",
    "RestartResult",
    "SandboxError",
]
