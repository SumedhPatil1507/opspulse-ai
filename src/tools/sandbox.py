"""
Safe container execution sandbox using the Python Docker SDK.

Safety model
------------
This module enforces several layers of protection so the AI agent can never
accidentally destroy data or disrupt unrelated workloads:

1. **Allowlist enforcement** — ``SANDBOX_ALLOWED_PREFIXES`` (config) gates
   every operation.  If the list is non-empty, only containers whose names
   start with an allowed prefix may be touched.  Empty list = dev-only
   permissive mode (log a warning).

2. **Non-destructive operations only** — The public API exposes:
   - ``inspect_container``  → read-only status snapshot
   - ``get_container_logs`` → read-only log tail
   - ``restart_container``  → graceful stop + start (NOT kill, NOT remove)
   Destructive Docker calls (``kill``, ``remove``, ``pause``, ``exec``,
   volume ops) are intentionally absent from this module.

3. **Isolated sandbox network** — ``ensure_sandbox_network()`` creates a
   dedicated bridge network (``opspulse_sandbox`` by default) with
   ``internal=True`` so containers attached to it have no outbound internet
   access.  Restarted containers are *not* forcibly moved to this network
   (that would break their existing connectivity); the network is available
   for future workload isolation use-cases.

4. **Timeout on restart** — ``restart_container`` passes
   ``SANDBOX_RESTART_TIMEOUT`` as the graceful stop deadline before SIGKILL,
   preventing hung containers from blocking the agent indefinitely.

5. **Prometheus instrumentation** — every operation records duration and
   outcome via ``src.metrics.record_sandbox_operation``.

6. **Structured audit logging** — every operation logs at INFO with
   ``container_name``, ``operation``, ``status``, and ``duration_ms`` so
   the audit trail is searchable in any log aggregator.

Connecting to Docker
--------------------
``ContainerSandbox`` accepts an optional pre-built ``DockerClient``.  When
none is provided, ``from_env()`` is used (reads ``DOCKER_HOST`` env var or
falls back to the default socket).  Pass ``base_url`` explicitly for
remote/TLS daemons:

    sb = ContainerSandbox(base_url="tcp://docker-host:2376")

Thread safety
-------------
``DockerClient`` is thread-safe for concurrent reads (inspect, logs).
``restart_container`` holds no Python-level lock — Docker daemon serialises
concurrent restarts on the same container internally.

Usage example
-------------
    from src.tools.sandbox import ContainerSandbox

    sb = ContainerSandbox()
    status  = sb.inspect_container("payment-service")
    logs    = sb.get_container_logs("payment-service")
    result  = sb.restart_container("payment-service")

    print(status.running, status.health, status.image)
    print(logs.lines[-10:])
    print(result.success, result.duration_ms)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import docker
import docker.errors
from docker import DockerClient
from docker.models.containers import Container
from docker.models.networks import Network

from src.api.core.config import Settings, get_settings
from src.metrics import record_sandbox_operation

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------

class SandboxError(Exception):
    """Base class for all sandbox failures."""


class ContainerNotFound(SandboxError):
    """Raised when the requested container does not exist on the Docker host."""

    def __init__(self, container_name: str) -> None:
        self.container_name = container_name
        super().__init__(f"Container not found: '{container_name}'")


class AllowlistViolation(SandboxError):
    """
    Raised when an operation targets a container not on the allowed prefix list.

    This is a hard security boundary — callers must not catch and swallow it.
    """

    def __init__(self, container_name: str, allowed_prefixes: list[str]) -> None:
        self.container_name = container_name
        self.allowed_prefixes = allowed_prefixes
        super().__init__(
            f"Container '{container_name}' is not permitted by the sandbox allowlist. "
            f"Allowed prefixes: {allowed_prefixes}"
        )


class DockerConnectionError(SandboxError):
    """Raised when the Docker daemon cannot be reached."""


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ContainerStatus:
    """
    Point-in-time snapshot of a container's runtime state.

    All fields are safe to serialise to JSON and include in LangGraph state.
    """

    container_id: str
    """Short 12-char container ID."""

    container_name: str
    """Primary container name without leading '/'."""

    image: str
    """Image name:tag."""

    status: str
    """Docker status string: running | exited | paused | restarting | dead | created."""

    running: bool
    """True iff ``status == 'running'``."""

    started_at: str
    """ISO-8601 UTC timestamp when the container last started."""

    finished_at: str
    """ISO-8601 UTC timestamp when the container last stopped (empty if running)."""

    exit_code: int | None
    """Last exit code (None if currently running)."""

    restart_count: int
    """Number of times Docker has automatically restarted this container."""

    health: str
    """
    Docker health-check status: healthy | unhealthy | starting | none.
    ``none`` when no HEALTHCHECK is defined in the image.
    """

    ports: dict[str, Any]
    """Exposed port bindings dict from Docker inspect."""

    labels: dict[str, str]
    """Container labels."""

    network_names: list[str]
    """Names of all networks this container is attached to."""

    cpu_shares: int
    """Relative CPU weight (0 = default)."""

    memory_limit_bytes: int
    """Hard memory limit in bytes (0 = unlimited)."""

    inspected_at: str = field(default_factory=lambda: datetime.now(tz=timezone.utc).isoformat())
    """UTC timestamp when this snapshot was taken."""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable dict for LangGraph state or API responses."""
        return {
            "container_id": self.container_id,
            "container_name": self.container_name,
            "image": self.image,
            "status": self.status,
            "running": self.running,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "exit_code": self.exit_code,
            "restart_count": self.restart_count,
            "health": self.health,
            "ports": self.ports,
            "labels": self.labels,
            "network_names": self.network_names,
            "cpu_shares": self.cpu_shares,
            "memory_limit_bytes": self.memory_limit_bytes,
            "inspected_at": self.inspected_at,
        }


@dataclass
class ContainerLogResult:
    """
    Result of a ``get_container_logs`` call.
    """

    container_name: str
    """Target container name."""

    lines: list[str]
    """Log lines (up to ``tail`` lines), newest last."""

    line_count: int
    """Actual number of lines returned (≤ requested tail)."""

    tail_requested: int
    """Number of lines that was requested."""

    retrieved_at: str = field(default_factory=lambda: datetime.now(tz=timezone.utc).isoformat())

    stderr_included: bool = True
    """True if stderr was merged into the output (default)."""

    def to_dict(self) -> dict[str, Any]:
        return {
            "container_name": self.container_name,
            "lines": self.lines,
            "line_count": self.line_count,
            "tail_requested": self.tail_requested,
            "retrieved_at": self.retrieved_at,
            "stderr_included": self.stderr_included,
        }

    @property
    def text(self) -> str:
        """Full log text as a single newline-joined string."""
        return "\n".join(self.lines)


@dataclass
class RestartResult:
    """
    Outcome of a ``restart_container`` call.
    """

    container_name: str
    """Target container name."""

    success: bool
    """True if the container restarted without error."""

    duration_ms: float
    """Wall-clock milliseconds the restart operation took."""

    pre_restart_status: str
    """Container status before the restart was issued."""

    post_restart_status: str
    """Container status after the restart completed (or 'unknown' on error)."""

    restart_count_before: int
    """Docker restart counter before this restart."""

    restart_count_after: int
    """Docker restart counter after this restart."""

    error_message: str | None = None
    """Set when ``success=False``."""

    restarted_at: str = field(default_factory=lambda: datetime.now(tz=timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return {
            "container_name": self.container_name,
            "success": self.success,
            "duration_ms": round(self.duration_ms, 2),
            "pre_restart_status": self.pre_restart_status,
            "post_restart_status": self.post_restart_status,
            "restart_count_before": self.restart_count_before,
            "restart_count_after": self.restart_count_after,
            "error_message": self.error_message,
            "restarted_at": self.restarted_at,
        }


# ---------------------------------------------------------------------------
# Sandbox
# ---------------------------------------------------------------------------

class ContainerSandbox:
    """
    Safe, audited Docker container operations for the OpsPulse agent.

    Parameters
    ----------
    settings : Application settings.  Loaded from ``get_settings()`` if None.
    base_url : Docker daemon URL override.  Supersedes ``settings.DOCKER_BASE_URL``
               when provided.
    client   : Pre-built ``DockerClient``.  Useful for testing with mocks.

    Raises
    ------
    DockerConnectionError
        On construction if the Docker daemon is unreachable and ``client`` is
        not provided.
    """

    def __init__(
        self,
        settings: Settings | None = None,
        base_url: str | None = None,
        client: DockerClient | None = None,
    ) -> None:
        self._cfg = settings or get_settings()
        self._client: DockerClient = client or self._connect(
            base_url or self._cfg.DOCKER_BASE_URL
        )

    # ── Construction ───────────────────────────────────────────────────────

    @staticmethod
    def _connect(base_url: str) -> DockerClient:
        """Connect to Docker daemon; raise ``DockerConnectionError`` on failure."""
        try:
            if base_url in ("", "env"):
                client = docker.from_env()
            else:
                client = docker.DockerClient(base_url=base_url)
            # Ping validates the connection immediately
            client.ping()
            logger.info("Docker sandbox connected (base_url=%s)", base_url)
            return client
        except docker.errors.DockerException as exc:
            raise DockerConnectionError(
                f"Cannot connect to Docker daemon at '{base_url}': {exc}"
            ) from exc

    # ── Allowlist enforcement ──────────────────────────────────────────────

    def _assert_allowed(self, container_name: str) -> None:
        """
        Raise ``AllowlistViolation`` if the container is not on the allowlist.

        Skips the check (with a warning) when ``SANDBOX_ALLOWED_PREFIXES`` is
        empty — intended for local development only.
        """
        prefixes = self._cfg.SANDBOX_ALLOWED_PREFIXES
        if not prefixes:
            logger.warning(
                "Sandbox allowlist is EMPTY — all containers are reachable. "
                "Set SANDBOX_ALLOWED_PREFIXES in production."
            )
            return

        # Strip leading '/' that Docker sometimes includes in names
        clean_name = container_name.lstrip("/")
        if not any(clean_name.startswith(p) for p in prefixes):
            raise AllowlistViolation(clean_name, prefixes)

    # ── Container resolution ───────────────────────────────────────────────

    def _get_container(self, name_or_id: str) -> Container:
        """
        Fetch a ``Container`` object by name or ID.

        Raises
        ------
        ContainerNotFound : If Docker returns a 404.
        SandboxError      : On unexpected Docker API errors.
        """
        try:
            return self._client.containers.get(name_or_id)
        except docker.errors.NotFound:
            raise ContainerNotFound(name_or_id)
        except docker.errors.APIError as exc:
            raise SandboxError(f"Docker API error fetching '{name_or_id}': {exc}") from exc

    # ── Public operations ──────────────────────────────────────────────────

    def inspect_container(self, name_or_id: str) -> ContainerStatus:
        """
        Return a structured snapshot of a container's current state.

        This is a **read-only** operation — it never modifies container state.

        Parameters
        ----------
        name_or_id : Container name (e.g. ``"payment-service"``) or short/full ID.

        Returns
        -------
        ContainerStatus
            Populated with all fields from Docker's low-level inspect data.

        Raises
        ------
        ContainerNotFound   : Container does not exist.
        AllowlistViolation  : Container not on allowlist.
        SandboxError        : Unexpected Docker API error.
        """
        self._assert_allowed(name_or_id)
        t_start = time.monotonic()
        success = True

        try:
            container: Container = self._get_container(name_or_id)
            container.reload()  # ensure attrs are fresh from daemon

            attrs: dict[str, Any] = container.attrs
            state: dict[str, Any] = attrs.get("State", {})
            host_cfg: dict[str, Any] = attrs.get("HostConfig", {})
            net_settings: dict[str, Any] = attrs.get("NetworkSettings", {})

            # Canonical container name (strip leading '/')
            raw_name: str = attrs.get("Name", name_or_id)
            clean_name = raw_name.lstrip("/")

            # Health status
            health_obj = state.get("Health") or {}
            health_status = health_obj.get("Status", "none")

            # Network names
            networks: dict = net_settings.get("Networks") or {}
            network_names = list(networks.keys())

            # Image reference
            image_ref: str = attrs.get("Config", {}).get("Image", "unknown")

            status_snap = ContainerStatus(
                container_id=container.short_id,
                container_name=clean_name,
                image=image_ref,
                status=state.get("Status", "unknown"),
                running=bool(state.get("Running", False)),
                started_at=state.get("StartedAt", ""),
                finished_at=state.get("FinishedAt", ""),
                exit_code=state.get("ExitCode") if not state.get("Running") else None,
                restart_count=state.get("RestartCount", 0),
                health=health_status,
                ports=net_settings.get("Ports") or {},
                labels=attrs.get("Config", {}).get("Labels") or {},
                network_names=network_names,
                cpu_shares=host_cfg.get("CpuShares", 0),
                memory_limit_bytes=host_cfg.get("Memory", 0),
            )

            duration = time.monotonic() - t_start
            record_sandbox_operation("inspect", clean_name, True, duration)
            logger.info(
                "sandbox.inspect container=%s status=%s running=%s "
                "health=%s restart_count=%d duration_ms=%.1f",
                clean_name,
                status_snap.status,
                status_snap.running,
                status_snap.health,
                status_snap.restart_count,
                duration * 1000,
            )
            return status_snap

        except (ContainerNotFound, AllowlistViolation):
            success = False
            duration = time.monotonic() - t_start
            record_sandbox_operation("inspect", name_or_id, False, duration)
            raise
        except Exception as exc:
            success = False
            duration = time.monotonic() - t_start
            record_sandbox_operation("inspect", name_or_id, False, duration)
            logger.exception("sandbox.inspect failed container=%s: %s", name_or_id, exc)
            raise SandboxError(f"inspect_container failed for '{name_or_id}': {exc}") from exc

    def get_container_logs(
        self,
        name_or_id: str,
        tail: int | None = None,
        include_stderr: bool = True,
    ) -> ContainerLogResult:
        """
        Retrieve the last ``tail`` log lines from a container.

        This is a **read-only** operation.

        Parameters
        ----------
        name_or_id     : Container name or ID.
        tail           : Number of lines to retrieve.  Defaults to
                         ``settings.SANDBOX_LOG_LINES`` (100).
        include_stderr : Merge stderr into the output stream (default True).

        Returns
        -------
        ContainerLogResult
            Structured result with individual lines and metadata.

        Raises
        ------
        ContainerNotFound  : Container does not exist.
        AllowlistViolation : Container not on allowlist.
        SandboxError       : Docker API or decoding error.
        """
        self._assert_allowed(name_or_id)
        n_lines = tail if tail is not None else self._cfg.SANDBOX_LOG_LINES
        t_start = time.monotonic()

        try:
            container: Container = self._get_container(name_or_id)
            clean_name = container.name.lstrip("/")

            raw_logs: bytes = container.logs(
                stdout=True,
                stderr=include_stderr,
                tail=n_lines,
                timestamps=False,
                stream=False,
            )

            # Decode; replace bad bytes to handle binary-mixed log streams
            log_text = raw_logs.decode("utf-8", errors="replace")
            lines = [ln for ln in log_text.splitlines() if ln]  # drop blank lines

            duration = time.monotonic() - t_start
            record_sandbox_operation("logs", clean_name, True, duration)
            logger.info(
                "sandbox.logs container=%s lines_returned=%d tail=%d "
                "stderr=%s duration_ms=%.1f",
                clean_name,
                len(lines),
                n_lines,
                include_stderr,
                duration * 1000,
            )

            return ContainerLogResult(
                container_name=clean_name,
                lines=lines,
                line_count=len(lines),
                tail_requested=n_lines,
                stderr_included=include_stderr,
            )

        except (ContainerNotFound, AllowlistViolation):
            duration = time.monotonic() - t_start
            record_sandbox_operation("logs", name_or_id, False, duration)
            raise
        except Exception as exc:
            duration = time.monotonic() - t_start
            record_sandbox_operation("logs", name_or_id, False, duration)
            logger.exception("sandbox.logs failed container=%s: %s", name_or_id, exc)
            raise SandboxError(f"get_container_logs failed for '{name_or_id}': {exc}") from exc

    def restart_container(
        self,
        name_or_id: str,
        timeout: int | None = None,
    ) -> RestartResult:
        """
        Perform a graceful container restart (``docker restart``).

        Safety guarantees
        -----------------
        * **Not destructive** — equivalent to ``docker restart``, which sends
          SIGTERM, waits ``timeout`` seconds, then SIGKILL if still running,
          and finally starts the container again.
        * **Not ``docker kill``** — SIGKILL is only used as a last resort after
          the graceful stop window.
        * **Not ``docker rm``** — the container is never removed.
        * **Allowlist enforced** — raises ``AllowlistViolation`` for containers
          not on the configured prefix list.

        Parameters
        ----------
        name_or_id : Container name or ID.
        timeout    : Seconds to wait for graceful stop before SIGKILL.
                     Defaults to ``settings.SANDBOX_RESTART_TIMEOUT`` (10).

        Returns
        -------
        RestartResult
            Full audit record of the operation including pre/post status and
            timing.

        Raises
        ------
        ContainerNotFound  : Container does not exist.
        AllowlistViolation : Container not on allowlist.
        SandboxError       : Docker API error during restart.
        """
        self._assert_allowed(name_or_id)
        stop_timeout = timeout if timeout is not None else self._cfg.SANDBOX_RESTART_TIMEOUT
        t_start = time.monotonic()

        try:
            container: Container = self._get_container(name_or_id)
            clean_name = container.name.lstrip("/")

            # Snapshot pre-restart state
            container.reload()
            pre_state = container.attrs.get("State", {})
            pre_status = pre_state.get("Status", "unknown")
            pre_restart_count = pre_state.get("RestartCount", 0)

            logger.info(
                "sandbox.restart STARTING container=%s pre_status=%s "
                "stop_timeout=%ds",
                clean_name, pre_status, stop_timeout,
            )

            # ── The only mutation this module performs ──────────────────
            container.restart(timeout=stop_timeout)
            # ────────────────────────────────────────────────────────────

            # Snapshot post-restart state
            container.reload()
            post_state = container.attrs.get("State", {})
            post_status = post_state.get("Status", "unknown")
            post_restart_count = post_state.get("RestartCount", 0)

            duration = time.monotonic() - t_start
            record_sandbox_operation("restart", clean_name, True, duration)

            logger.info(
                "sandbox.restart COMPLETED container=%s "
                "pre=%s post=%s restart_count=%d→%d duration_ms=%.1f",
                clean_name,
                pre_status,
                post_status,
                pre_restart_count,
                post_restart_count,
                duration * 1000,
            )

            return RestartResult(
                container_name=clean_name,
                success=True,
                duration_ms=duration * 1000,
                pre_restart_status=pre_status,
                post_restart_status=post_status,
                restart_count_before=pre_restart_count,
                restart_count_after=post_restart_count,
            )

        except (ContainerNotFound, AllowlistViolation):
            duration = time.monotonic() - t_start
            record_sandbox_operation("restart", name_or_id, False, duration)
            raise
        except Exception as exc:
            duration = time.monotonic() - t_start
            record_sandbox_operation("restart", name_or_id, False, duration)
            err_msg = f"restart_container failed for '{name_or_id}': {exc}"
            logger.exception("sandbox.restart FAILED container=%s: %s", name_or_id, exc)
            return RestartResult(
                container_name=name_or_id,
                success=False,
                duration_ms=duration * 1000,
                pre_restart_status="unknown",
                post_restart_status="unknown",
                restart_count_before=0,
                restart_count_after=0,
                error_message=err_msg,
            )

    # ── Network management ─────────────────────────────────────────────────

    def ensure_sandbox_network(self) -> Network:
        """
        Create the isolated sandbox bridge network if it does not exist.

        The network is created with ``internal=True``, which means containers
        attached to it have no outbound internet access — reducing blast radius
        for any sandbox workload.

        Returns
        -------
        docker.models.networks.Network
            The existing or newly-created network object.

        Raises
        ------
        SandboxError : On Docker API errors during network creation.
        """
        network_name = self._cfg.SANDBOX_NETWORK_NAME
        try:
            existing = self._client.networks.list(names=[network_name])
            if existing:
                net = existing[0]
                logger.info(
                    "sandbox.network already exists name=%s id=%s",
                    network_name, net.short_id,
                )
                return net

            logger.info("sandbox.network creating name=%s (internal=True)", network_name)
            net = self._client.networks.create(
                name=network_name,
                driver="bridge",
                internal=True,         # no outbound internet access
                check_duplicate=True,
                labels={
                    "managed-by": "opspulse-ai",
                    "purpose": "sandbox-isolation",
                },
            )
            logger.info(
                "sandbox.network created name=%s id=%s",
                network_name, net.short_id,
            )
            return net

        except docker.errors.APIError as exc:
            raise SandboxError(
                f"Failed to ensure sandbox network '{network_name}': {exc}"
            ) from exc

    def remove_sandbox_network(self) -> bool:
        """
        Remove the sandbox network if it exists and has no active endpoints.

        Returns ``True`` if removed, ``False`` if it did not exist.
        Raises ``SandboxError`` if containers are still attached.
        """
        network_name = self._cfg.SANDBOX_NETWORK_NAME
        try:
            existing = self._client.networks.list(names=[network_name])
            if not existing:
                return False
            existing[0].remove()
            logger.info("sandbox.network removed name=%s", network_name)
            return True
        except docker.errors.APIError as exc:
            raise SandboxError(
                f"Failed to remove sandbox network '{network_name}': {exc}"
            ) from exc

    # ── Utility helpers ────────────────────────────────────────────────────

    def ping(self) -> bool:
        """
        Return ``True`` if the Docker daemon is reachable.

        Safe to call at startup to gate-check Docker availability without
        raising.
        """
        try:
            return bool(self._client.ping())
        except Exception:
            return False

    def list_allowed_containers(self) -> list[dict[str, Any]]:
        """
        List all running containers that pass the allowlist check.

        Returns a list of minimal dicts (id, name, status, image) suitable
        for display in a Slack message or FastAPI response.
        """
        prefixes = self._cfg.SANDBOX_ALLOWED_PREFIXES
        try:
            containers = self._client.containers.list(all=False)  # running only
        except docker.errors.APIError as exc:
            raise SandboxError(f"Failed to list containers: {exc}") from exc

        result = []
        for c in containers:
            clean = c.name.lstrip("/")
            if prefixes and not any(clean.startswith(p) for p in prefixes):
                continue
            result.append({
                "id": c.short_id,
                "name": clean,
                "status": c.status,
                "image": c.image.tags[0] if c.image.tags else "unknown",
            })
        return result

    def close(self) -> None:
        """Close the underlying Docker SDK HTTP session."""
        try:
            self._client.close()
        except Exception:
            pass

    def __enter__(self) -> "ContainerSandbox":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
