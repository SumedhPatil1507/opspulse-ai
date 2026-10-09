"""
OpsPulse AI — Kubernetes Action Execution Engine with HITL Authorization.
=========================================================================
Provides automated, guarded remediation actions on Kubernetes clusters:
1. restart_deployment(namespace, deployment_name, auth_token, approval_id)
2. scale_deployment(namespace, deployment_name, count, auth_token, approval_id)
3. rollback_helm_release(release_name, auth_token, approval_id, namespace, revision)
4. fetch_pod_logs(namespace, pod_name, auth_token, tail_lines, container, previous)

Security & Safety Architecture:
-------------------------------
* Human-In-The-Loop (HITL) Gate: No mutating action is executed without an
  explicit, pre-approved `approval_id` present in the HITL review queue.
* JWT Authorization Gate: Every action requires a cryptographically valid JWT
  bearing the `ROLE_SRE_ADMIN` role claim — including read-only operations.
* Anti-Replay: Once consumed by an execution, the approval request is marked as
  EXECUTED and cannot be reused.
* Granular Validation: Target deployment/release and action type are verified
  against the approval request to prevent cross-action authorization spoofing.
* Non-Destructive Only: The executor deliberately exposes only rolling
  restarts, scaling, rollbacks, and log reads — never deletions or drains.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import subprocess
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable

try:
    from kubernetes import client, config
    from kubernetes.client import AppsV1Api, CoreV1Api
    from kubernetes.client.exceptions import ApiException
    _K8S_AVAILABLE = True
except ImportError:  # pragma: no cover
    client = None  # type: ignore[assignment]
    config = None  # type: ignore[assignment]
    AppsV1Api = Any  # type: ignore[assignment,misc]
    CoreV1Api = Any  # type: ignore[assignment,misc]
    ApiException = Exception  # type: ignore[assignment,misc]
    _K8S_AVAILABLE = False

try:
    import jwt as pyjwt
    _PYJWT_AVAILABLE = True
except ImportError:  # pragma: no cover
    pyjwt = None  # type: ignore[assignment]
    _PYJWT_AVAILABLE = False

from src.api.core.config import Settings, get_settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class K8sExecutorError(Exception):
    """Base exception for all Kubernetes executor errors."""


class SecurityGateError(K8sExecutorError):
    """Base exception for authorization and security gate violations."""


class UnauthorizedActionError(SecurityGateError):
    """Raised when JWT is invalid, expired, missing, or lacks ROLE_SRE_ADMIN."""


class HITLApprovalRequiredError(SecurityGateError):
    """Raised when an action is attempted without a valid approved HITL approval_id."""


class InvalidParameterError(K8sExecutorError):
    """Raised when input parameters (e.g. replica count, names) are invalid."""


class K8sExecutionError(K8sExecutorError):
    """Raised when Kubernetes API or Helm command execution fails."""


# ---------------------------------------------------------------------------
# Enums and Data Models
# ---------------------------------------------------------------------------

class ApprovalStatus(str, Enum):
    """Status lifecycle of a Human-In-The-Loop review request."""
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    EXECUTED = "EXECUTED"


@dataclass
class HITLApprovalRequest:
    """Represents a human-in-the-loop review item in the approval queue."""
    approval_id: str
    action_type: str
    target: str
    params: dict[str, Any]
    requested_by: str
    approved_by: str | None = None
    status: ApprovalStatus = ApprovalStatus.PENDING
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    expires_at: str | None = None
    executed_at: str | None = None
    rejection_reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class K8sActionResult:
    """Structured response returned by all Kubernetes executor actions."""
    success: bool
    action: str
    target: str
    namespace: str | None
    approval_id: str
    executed_by: str
    details: dict[str, Any]
    error: str | None = None
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    duration_ms: float = 0.0


# ---------------------------------------------------------------------------
# HITL Review Queue Store
# ---------------------------------------------------------------------------

class HITLReviewQueue:
    """
    In-memory / managed HITL approval queue.
    Enforces atomic lifecycle transitions: PENDING -> APPROVED -> EXECUTED.
    """

    def __init__(self) -> None:
        self._queue: dict[str, HITLApprovalRequest] = {}

    def submit_for_review(
        self,
        action_type: str,
        target: str,
        params: dict[str, Any],
        requested_by: str = "opspulse-ai-agent",
        ttl_seconds: int | None = 3600,
        metadata: dict[str, Any] | None = None,
    ) -> HITLApprovalRequest:
        """Create and register a new pending approval request."""
        approval_id = f"hitl-{uuid.uuid4().hex[:12]}"
        now = datetime.now(timezone.utc)
        expires_at = (
            datetime.fromtimestamp(now.timestamp() + ttl_seconds, tz=timezone.utc).isoformat()
            if ttl_seconds
            else None
        )

        request = HITLApprovalRequest(
            approval_id=approval_id,
            action_type=action_type,
            target=target,
            params=params,
            requested_by=requested_by,
            status=ApprovalStatus.PENDING,
            created_at=now.isoformat(),
            expires_at=expires_at,
            metadata=metadata or {},
        )
        self._queue[approval_id] = request
        logger.info(
            "HITL approval request created: id=%s action=%s target=%s requested_by=%s",
            approval_id,
            action_type,
            target,
            requested_by,
        )
        return request

    def approve(self, approval_id: str, approved_by: str) -> HITLApprovalRequest:
        """Mark an existing request as APPROVED by a human reviewer."""
        if approval_id not in self._queue:
            raise HITLApprovalRequiredError(f"Approval ID '{approval_id}' not found in review queue.")

        req = self._queue[approval_id]
        if req.status == ApprovalStatus.EXECUTED:
            raise HITLApprovalRequiredError(f"Approval request '{approval_id}' has already been executed.")
        if req.status == ApprovalStatus.REJECTED:
            raise HITLApprovalRequiredError(f"Approval request '{approval_id}' was previously rejected.")

        self._check_expiry(req)

        req.status = ApprovalStatus.APPROVED
        req.approved_by = approved_by
        logger.info("HITL approval granted: id=%s approved_by=%s", approval_id, approved_by)
        return req

    def reject(self, approval_id: str, rejected_by: str, reason: str = "Rejected by reviewer") -> HITLApprovalRequest:
        """Mark an existing request as REJECTED."""
        if approval_id not in self._queue:
            raise HITLApprovalRequiredError(f"Approval ID '{approval_id}' not found in review queue.")

        req = self._queue[approval_id]
        req.status = ApprovalStatus.REJECTED
        req.approved_by = rejected_by
        req.rejection_reason = reason
        logger.info("HITL approval rejected: id=%s rejected_by=%s reason=%s", approval_id, rejected_by, reason)
        return req

    def get(self, approval_id: str) -> HITLApprovalRequest | None:
        """Retrieve approval request by ID without mutating."""
        return self._queue.get(approval_id)

    def consume(self, approval_id: str, expected_action: str, expected_target: str) -> HITLApprovalRequest:
        """
        Validate that the approval request is APPROVED, matches action & target,
        is not expired, and atomically transition it to EXECUTED.
        """
        if not approval_id or not isinstance(approval_id, str):
            raise HITLApprovalRequiredError("Missing or invalid 'approval_id'. Explicit HITL authorization is required.")

        if approval_id not in self._queue:
            raise HITLApprovalRequiredError(f"Approval ID '{approval_id}' not found in HITL review queue.")

        req = self._queue[approval_id]
        self._check_expiry(req)

        if req.status == ApprovalStatus.PENDING:
            raise HITLApprovalRequiredError(
                f"Approval request '{approval_id}' is still PENDING human review."
            )
        if req.status == ApprovalStatus.REJECTED:
            raise HITLApprovalRequiredError(
                f"Approval request '{approval_id}' was REJECTED: {req.rejection_reason}"
            )
        if req.status == ApprovalStatus.EXECUTED:
            raise HITLApprovalRequiredError(
                f"Approval request '{approval_id}' was already EXECUTED (anti-replay protection)."
            )
        if req.status != ApprovalStatus.APPROVED:
            raise HITLApprovalRequiredError(
                f"Approval request '{approval_id}' has invalid status '{req.status}'."
            )

        # Validate action and target alignment
        if req.action_type != expected_action:
            raise HITLApprovalRequiredError(
                f"Approval ID '{approval_id}' was approved for '{req.action_type}', "
                f"cannot be used for '{expected_action}'."
            )
        if req.target != expected_target:
            raise HITLApprovalRequiredError(
                f"Approval ID '{approval_id}' was approved for target '{req.target}', "
                f"cannot be used for target '{expected_target}'."
            )

        # Mark as EXECUTED to prevent replay
        req.status = ApprovalStatus.EXECUTED
        req.executed_at = datetime.now(timezone.utc).isoformat()
        return req

    def _check_expiry(self, req: HITLApprovalRequest) -> None:
        if req.expires_at:
            try:
                exp_dt = datetime.fromisoformat(req.expires_at)
                if datetime.now(timezone.utc) > exp_dt:
                    req.status = ApprovalStatus.EXPIRED
                    raise HITLApprovalRequiredError(f"Approval request '{req.approval_id}' has EXPIRED.")
            except (ValueError, TypeError):
                pass


# Global singleton queue
_global_hitl_queue = HITLReviewQueue()


def get_hitl_queue() -> HITLReviewQueue:
    """Access the singleton HITL review queue."""
    return _global_hitl_queue


# ---------------------------------------------------------------------------
# JWT Token Verification & Minting
# ---------------------------------------------------------------------------

REQUIRED_SRE_ROLE = "ROLE_SRE_ADMIN"


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("utf-8")


def _b64url_decode(s: str) -> bytes:
    padded = s + "=" * ((4 - len(s) % 4) % 4)
    return base64.urlsafe_b64decode(padded)


def generate_jwt_token(
    subject: str = "sre-admin",
    roles: list[str] | None = None,
    expires_in_seconds: int = 3600,
    secret: str | None = None,
    extra_claims: dict[str, Any] | None = None,
) -> str:
    """
    Generate a signed JWT token containing standard claims and SRE roles.
    Useful for testing, CLI operations, and API client auth.
    """
    if roles is None:
        roles = [REQUIRED_SRE_ROLE]
    if secret is None:
        secret = get_settings().JWT_SECRET_KEY

    now = int(time.time())
    payload: dict[str, Any] = {
        "sub": subject,
        "roles": roles,
        "role": roles[0] if roles else "",
        "iat": now,
        "exp": now + expires_in_seconds,
        "iss": "opspulse-ai",
    }
    if extra_claims:
        payload.update(extra_claims)

    if _PYJWT_AVAILABLE and pyjwt is not None:
        return pyjwt.encode(payload, secret, algorithm="HS256")  # type: ignore[no-any-return]

    # Native standard HMAC-SHA256 JWT generation
    header = {"alg": "HS256", "typ": "JWT"}
    hdr_b64 = _b64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_b64 = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{hdr_b64}.{payload_b64}".encode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
    sig_b64 = _b64url_encode(signature)
    return f"{hdr_b64}.{payload_b64}.{sig_b64}"


def verify_jwt_token(
    token: str,
    secret: str | None = None,
    required_role: str = REQUIRED_SRE_ROLE,
) -> dict[str, Any]:
    """
    Verify signature, expiration, and role claims of the supplied JWT token.
    Raises `UnauthorizedActionError` on any verification failure.
    """
    if not token or not isinstance(token, str):
        raise UnauthorizedActionError("Missing or empty JWT authorization token.")

    # Strip 'Bearer ' prefix if present
    clean_token = token.strip()
    if clean_token.lower().startswith("bearer "):
        clean_token = clean_token[7:].strip()

    if secret is None:
        secret = get_settings().JWT_SECRET_KEY

    payload: dict[str, Any]

    if _PYJWT_AVAILABLE and pyjwt is not None:
        try:
            payload = pyjwt.decode(
                clean_token,
                secret,
                algorithms=["HS256"],
                options={"require": ["exp", "sub"]},
            )
        except pyjwt.ExpiredSignatureError as err:
            raise UnauthorizedActionError("JWT token has expired.") from err
        except pyjwt.InvalidTokenError as err:
            raise UnauthorizedActionError(f"Invalid JWT token: {err}") from err
    else:
        # Native verification fallback
        parts = clean_token.split(".")
        if len(parts) != 3:
            raise UnauthorizedActionError("Malformed JWT token format (expected 3 parts).")

        hdr_b64, payload_b64, sig_b64 = parts
        try:
            signing_input = f"{hdr_b64}.{payload_b64}".encode("utf-8")
            expected_sig = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
            actual_sig = _b64url_decode(sig_b64)
            if not hmac.compare_digest(expected_sig, actual_sig):
                raise UnauthorizedActionError("Invalid JWT signature.")

            payload = json.loads(_b64url_decode(payload_b64).decode("utf-8"))
        except Exception as err:
            if isinstance(err, UnauthorizedActionError):
                raise
            raise UnauthorizedActionError(f"Could not parse JWT payload: {err}") from err

        # Verify expiration
        exp = payload.get("exp")
        if exp is None or not isinstance(exp, (int, float)):
            raise UnauthorizedActionError("JWT token is missing a valid 'exp' claim.")
        if time.time() > exp:
            raise UnauthorizedActionError("JWT token has expired.")

    # Validate required role claim
    roles = payload.get("roles")
    role = payload.get("role")
    permissions = payload.get("permissions", [])
    scope = payload.get("scope", "")

    user_roles: set[str] = set()
    if isinstance(roles, list):
        user_roles.update(roles)
    elif isinstance(roles, str):
        user_roles.add(roles)
    if isinstance(role, str):
        user_roles.add(role)
    if isinstance(permissions, list):
        user_roles.update(permissions)
    if isinstance(scope, str):
        user_roles.update(scope.split())

    if required_role not in user_roles:
        raise UnauthorizedActionError(
            f"JWT token lacks required authorization role '{required_role}'. User roles: {list(user_roles)}"
        )

    return payload


# ---------------------------------------------------------------------------
# Kubernetes Action Execution Engine
# ---------------------------------------------------------------------------

class KubernetesExecutor:
    """
    Automated Kubernetes action execution engine protected by HITL review and JWT gates.
    """

    def __init__(
        self,
        apps_v1_api: AppsV1Api | None = None,
        core_v1_api: CoreV1Api | None = None,
        hitl_queue: HITLReviewQueue | None = None,
        jwt_secret: str | None = None,
        helm_binary_path: str | None = None,
        load_config: bool = True,
    ) -> None:
        # Fall back to application settings so a bare KubernetesExecutor()
        # always honours the JWT/Helm configuration from .env.
        settings = get_settings()
        self._apps_v1_api = apps_v1_api
        self._core_v1_api = core_v1_api
        self._hitl_queue = hitl_queue or get_hitl_queue()
        self._jwt_secret = jwt_secret or settings.JWT_SECRET_KEY
        self._helm_binary_path = helm_binary_path or settings.HELM_BINARY_PATH
        self._load_config = load_config
        self._initialized = False

    def _ensure_k8s_clients(self) -> tuple[AppsV1Api, CoreV1Api]:
        """Lazy-initializes Kubernetes API clients with cluster or kubeconfig credentials."""
        if self._apps_v1_api is not None and self._core_v1_api is not None:
            return self._apps_v1_api, self._core_v1_api

        if not _K8S_AVAILABLE or client is None:
            raise K8sExecutionError("The 'kubernetes' Python package is not installed or available.")

        if not self._initialized and self._load_config and config is not None:
            settings = get_settings()
            try:
                if settings.K8S_KUBECONFIG_PATH:
                    config.load_kube_config(config_file=settings.K8S_KUBECONFIG_PATH)
                else:
                    try:
                        config.load_incluster_config()
                    except Exception:
                        config.load_kube_config()
            except Exception as e:
                logger.warning("Could not auto-load kubeconfig: %s (using default ApiClient)", e)
            self._initialized = True

        if self._apps_v1_api is None:
            self._apps_v1_api = client.AppsV1Api()
        if self._core_v1_api is None:
            self._core_v1_api = client.CoreV1Api()

        return self._apps_v1_api, self._core_v1_api

    def verify_authorization(
        self,
        auth_token: str,
        approval_id: str,
        action: str,
        target: str,
    ) -> tuple[dict[str, Any], HITLApprovalRequest]:
        """
        Enforce the dual safety gates:
        1. Valid JWT with ROLE_SRE_ADMIN.
        2. Valid approved explicit approval_id in the HITL review queue.
        """
        claims = verify_jwt_token(auth_token, secret=self._jwt_secret, required_role=REQUIRED_SRE_ROLE)
        hitl_req = self._hitl_queue.consume(
            approval_id=approval_id,
            expected_action=action,
            expected_target=target,
        )
        return claims, hitl_req

    def restart_deployment(
        self,
        namespace: str,
        deployment_name: str,
        auth_token: str,
        approval_id: str,
    ) -> K8sActionResult:
        """
        Trigger a rolling restart of a Kubernetes deployment.
        Applies a restart annotation patch (`kubectl.kubernetes.io/restartedAt`)
        to the Pod template spec via AppsV1Api.
        """
        start_time = time.perf_counter()
        action = "restart_deployment"
        target = f"{namespace}/{deployment_name}"

        if not namespace or not deployment_name:
            raise InvalidParameterError("Both 'namespace' and 'deployment_name' must be non-empty strings.")

        claims, hitl_req = self.verify_authorization(auth_token, approval_id, action, target)
        executed_by = claims.get("sub", "unknown-sre")

        apps_v1, _ = self._ensure_k8s_clients()
        now_iso = datetime.now(timezone.utc).isoformat()

        patch_body = {
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "kubectl.kubernetes.io/restartedAt": now_iso,
                            "opspulse.ai/restartedBy": executed_by,
                            "opspulse.ai/approvalId": approval_id,
                        }
                    }
                }
            }
        }

        try:
            resp = apps_v1.patch_namespaced_deployment(
                name=deployment_name,
                namespace=namespace,
                body=patch_body,
            )
            duration_ms = (time.perf_counter() - start_time) * 1000.0

            details = {
                "namespace": namespace,
                "deployment": deployment_name,
                "restarted_at": now_iso,
                "approved_by": hitl_req.approved_by,
                "approval_id": approval_id,
                "generation": getattr(resp.metadata, "generation", None) if hasattr(resp, "metadata") else None,
            }

            logger.info(
                "Deployment restarted successfully: %s by %s (approval: %s, duration: %.2fms)",
                target,
                executed_by,
                approval_id,
                duration_ms,
            )

            return K8sActionResult(
                success=True,
                action=action,
                target=target,
                namespace=namespace,
                approval_id=approval_id,
                executed_by=executed_by,
                details=details,
                duration_ms=duration_ms,
            )

        except Exception as e:
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            error_msg = f"K8s API failed to restart deployment '{target}': {e}"
            logger.error(error_msg, exc_info=True)
            raise K8sExecutionError(error_msg) from e

    def scale_deployment(
        self,
        namespace: str,
        deployment_name: str,
        count: int,
        auth_token: str,
        approval_id: str,
    ) -> K8sActionResult:
        """
        Scale the replica count of a Kubernetes deployment via AppsV1Api.

        Non-destructive horizontal scaling — guarded by the dual safety gates
        (JWT with ROLE_SRE_ADMIN + approved HITL approval_id).
        """
        start_time = time.perf_counter()
        action = "scale_deployment"
        target = f"{namespace}/{deployment_name}"

        if not namespace or not deployment_name:
            raise InvalidParameterError("Both 'namespace' and 'deployment_name' must be non-empty strings.")
        if not isinstance(count, int) or count < 0:
            raise InvalidParameterError(f"Replica count must be a non-negative integer, got: {count}")

        claims, hitl_req = self.verify_authorization(auth_token, approval_id, action, target)
        executed_by = claims.get("sub", "unknown-sre")

        apps_v1, _ = self._ensure_k8s_clients()

        scale_patch = {
            "spec": {
                "replicas": count,
            }
        }

        try:
            # Try scale subresource first, fallback to standard deployment patch if needed
            if hasattr(apps_v1, "patch_namespaced_deployment_scale"):
                resp = apps_v1.patch_namespaced_deployment_scale(
                    name=deployment_name,
                    namespace=namespace,
                    body=scale_patch,
                )
            else:
                resp = apps_v1.patch_namespaced_deployment(
                    name=deployment_name,
                    namespace=namespace,
                    body=scale_patch,
                )

            duration_ms = (time.perf_counter() - start_time) * 1000.0
            details = {
                "namespace": namespace,
                "deployment": deployment_name,
                "target_replicas": count,
                "approved_by": hitl_req.approved_by,
                "approval_id": approval_id,
            }

            logger.info(
                "Deployment scaled: %s to %d replicas by %s (approval: %s, duration: %.2fms)",
                target,
                count,
                executed_by,
                approval_id,
                duration_ms,
            )

            return K8sActionResult(
                success=True,
                action=action,
                target=target,
                namespace=namespace,
                approval_id=approval_id,
                executed_by=executed_by,
                details=details,
                duration_ms=duration_ms,
            )

        except Exception as e:
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            error_msg = f"K8s API failed to scale deployment '{target}' to {count} replicas: {e}"
            logger.error(error_msg, exc_info=True)
            raise K8sExecutionError(error_msg) from e

    def rollback_helm_release(
        self,
        release_name: str,
        auth_token: str,
        approval_id: str,
        namespace: str | None = None,
        revision: int | None = None,
    ) -> K8sActionResult:
        """
        Execute a safe Helm release rollback.
        Invokes `helm rollback <release_name> [<revision>] [--namespace <namespace>]`.
        """
        start_time = time.perf_counter()
        action = "rollback_helm_release"
        target = f"{namespace}/{release_name}" if namespace else release_name

        if not release_name or not isinstance(release_name, str):
            raise InvalidParameterError("'release_name' must be a non-empty string.")
        if revision is not None and (not isinstance(revision, int) or revision < 0):
            raise InvalidParameterError(f"Helm revision must be a positive integer, got: {revision}")

        claims, hitl_req = self.verify_authorization(auth_token, approval_id, action, target)
        executed_by = claims.get("sub", "unknown-sre")

        # Construct safe helm rollback command
        cmd: list[str] = [self._helm_binary_path, "rollback", release_name]
        if revision is not None and revision > 0:
            cmd.append(str(revision))
        if namespace:
            cmd.extend(["--namespace", namespace])

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=60,
            )
            duration_ms = (time.perf_counter() - start_time) * 1000.0

            details = {
                "release_name": release_name,
                "namespace": namespace,
                "revision": revision,
                "stdout": proc.stdout.strip(),
                "approved_by": hitl_req.approved_by,
                "approval_id": approval_id,
            }

            logger.info(
                "Helm rollback completed: %s by %s (approval: %s, duration: %.2fms)",
                target,
                executed_by,
                approval_id,
                duration_ms,
            )

            return K8sActionResult(
                success=True,
                action=action,
                target=target,
                namespace=namespace,
                approval_id=approval_id,
                executed_by=executed_by,
                details=details,
                duration_ms=duration_ms,
            )

        except subprocess.CalledProcessError as e:
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            error_msg = (
                f"Helm rollback command failed for release '{target}' (code {e.returncode}): "
                f"{e.stderr.strip() or e.stdout.strip()}"
            )
            logger.error(error_msg)
            raise K8sExecutionError(error_msg) from e
        except Exception as e:
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            error_msg = f"Helm rollback execution error for '{target}': {e}"
            logger.error(error_msg, exc_info=True)
            raise K8sExecutionError(error_msg) from e

    def fetch_pod_logs(
        self,
        namespace: str,
        pod_name: str,
        auth_token: str,
        tail_lines: int = 200,
        container: str | None = None,
        previous: bool = False,
    ) -> dict[str, Any]:
        """
        Fetch recent logs for a Kubernetes pod via CoreV1Api (read-only).

        Non-destructive diagnostics: gated by a valid JWT bearing
        ``ROLE_SRE_ADMIN`` but does **not** consume an HITL approval slot
        because it mutates nothing in the cluster.

        Returns
        -------
        dict
            ``{"namespace", "pod_name", "logs", "tail_lines", "fetched_by"}``
        """
        start_time = time.perf_counter()

        if not namespace or not pod_name:
            raise InvalidParameterError("Both 'namespace' and 'pod_name' must be non-empty strings.")
        if not isinstance(tail_lines, int) or tail_lines < 1 or tail_lines > 5000:
            raise InvalidParameterError(
                f"'tail_lines' must be an integer between 1 and 5000, got: {tail_lines}"
            )

        # JWT gate — read-only ops still require ROLE_SRE_ADMIN.
        claims = verify_jwt_token(auth_token, secret=self._jwt_secret, required_role=REQUIRED_SRE_ROLE)
        fetched_by = claims.get("sub", "unknown-sre")

        _, core_v1 = self._ensure_k8s_clients()

        try:
            log_text: str = core_v1.read_namespaced_pod_log(
                name=pod_name,
                namespace=namespace,
                container=container,
                tail_lines=tail_lines,
                previous=previous,
            )
            duration_ms = (time.perf_counter() - start_time) * 1000.0

            logger.info(
                "Fetched pod logs: %s/%s (tail=%d, by=%s, duration=%.2fms)",
                namespace,
                pod_name,
                tail_lines,
                fetched_by,
                duration_ms,
            )

            return {
                "namespace": namespace,
                "pod_name": pod_name,
                "container": container,
                "tail_lines": tail_lines,
                "previous": previous,
                "logs": log_text,
                "fetched_by": fetched_by,
                "duration_ms": duration_ms,
            }

        except Exception as e:
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            error_msg = f"K8s API failed to fetch logs for pod '{namespace}/{pod_name}': {e}"
            logger.error(error_msg, exc_info=True)
            raise K8sExecutionError(error_msg) from e


# ---------------------------------------------------------------------------
# Module-level convenience functions
# ---------------------------------------------------------------------------

_default_executor: KubernetesExecutor | None = None


def get_k8s_executor() -> KubernetesExecutor:
    """Return the global default KubernetesExecutor singleton."""
    global _default_executor
    if _default_executor is None:
        _default_executor = KubernetesExecutor()
    return _default_executor


def restart_deployment(
    namespace: str,
    deployment_name: str,
    auth_token: str,
    approval_id: str,
    executor: KubernetesExecutor | None = None,
) -> K8sActionResult:
    """
    Guarded action to trigger a rolling restart of a Kubernetes deployment.
    Requires valid JWT token with ROLE_SRE_ADMIN and approved HITL approval_id.
    """
    exec_instance = executor or get_k8s_executor()
    return exec_instance.restart_deployment(
        namespace=namespace,
        deployment_name=deployment_name,
        auth_token=auth_token,
        approval_id=approval_id,
    )


def scale_deployment(
    namespace: str,
    deployment_name: str,
    count: int,
    auth_token: str,
    approval_id: str,
    executor: KubernetesExecutor | None = None,
) -> K8sActionResult:
    """
    Guarded action to scale a Kubernetes deployment's replica count.
    Requires valid JWT token with ROLE_SRE_ADMIN and approved HITL approval_id.
    """
    exec_instance = executor or get_k8s_executor()
    return exec_instance.scale_deployment(
        namespace=namespace,
        deployment_name=deployment_name,
        count=count,
        auth_token=auth_token,
        approval_id=approval_id,
    )


# Backwards-compatible alias (pre-upgrade name).
scale_replicas = scale_deployment


def fetch_pod_logs(
    namespace: str,
    pod_name: str,
    auth_token: str,
    tail_lines: int = 200,
    container: str | None = None,
    previous: bool = False,
    executor: KubernetesExecutor | None = None,
) -> dict[str, Any]:
    """
    Guarded read-only action to fetch recent logs for a Kubernetes pod.
    Requires valid JWT token with ROLE_SRE_ADMIN (no HITL approval needed).
    """
    exec_instance = executor or get_k8s_executor()
    return exec_instance.fetch_pod_logs(
        namespace=namespace,
        pod_name=pod_name,
        auth_token=auth_token,
        tail_lines=tail_lines,
        container=container,
        previous=previous,
    )


def rollback_helm_release(
    release_name: str,
    auth_token: str,
    approval_id: str,
    namespace: str | None = None,
    revision: int | None = None,
    executor: KubernetesExecutor | None = None,
) -> K8sActionResult:
    """
    Guarded action to perform a rollback on a Helm release.
    Requires valid JWT token with ROLE_SRE_ADMIN and approved HITL approval_id.
    """
    exec_instance = executor or get_k8s_executor()
    return exec_instance.rollback_helm_release(
        release_name=release_name,
        auth_token=auth_token,
        approval_id=approval_id,
        namespace=namespace,
        revision=revision,
    )
