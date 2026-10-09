"""
Unit tests for Kubernetes Action Execution Engine with HITL Authorization.
==========================================================================
Verifies:
1. JWT verification gate (ROLE_SRE_ADMIN enforcement, expiration, tampering).
2. HITL Review Queue safety gate (pending, approved, rejected, expired, anti-replay).
3. K8s API interactions with unittest.mock (restart_deployment, scale_deployment).
4. Helm CLI rollback interactions with subprocess mocking.
"""

from __future__ import annotations

import subprocess
import time
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.k8s_executor import (
    ApprovalStatus,
    HITLApprovalRequest,
    HITLApprovalRequiredError,
    HITLReviewQueue,
    InvalidParameterError,
    K8sActionResult,
    K8sExecutionError,
    KubernetesExecutor,
    REQUIRED_SRE_ROLE,
    SecurityGateError,
    UnauthorizedActionError,
    generate_jwt_token,
    restart_deployment,
    rollback_helm_release,
    scale_deployment,
    verify_jwt_token,
)

TEST_JWT_SECRET = "test-sre-secret-key-1234567890-secure-32bytes"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def hitl_queue() -> HITLReviewQueue:
    """Provide a fresh, isolated HITLReviewQueue instance for each test."""
    return HITLReviewQueue()


@pytest.fixture
def mock_apps_v1() -> MagicMock:
    """Mock kubernetes.client.AppsV1Api."""
    mock = MagicMock()
    mock.patch_namespaced_deployment.return_value = MagicMock(
        metadata=MagicMock(generation=4)
    )
    mock.patch_namespaced_deployment_scale.return_value = MagicMock(
        metadata=MagicMock(generation=5)
    )
    return mock


@pytest.fixture
def mock_core_v1() -> MagicMock:
    """Mock kubernetes.client.CoreV1Api."""
    return MagicMock()


@pytest.fixture
def k8s_executor(
    mock_apps_v1: MagicMock,
    mock_core_v1: MagicMock,
    hitl_queue: HITLReviewQueue,
) -> KubernetesExecutor:
    """Instantiate a test KubernetesExecutor with injected mocks."""
    return KubernetesExecutor(
        apps_v1_api=mock_apps_v1,
        core_v1_api=mock_core_v1,
        hitl_queue=hitl_queue,
        jwt_secret=TEST_JWT_SECRET,
        load_config=False,
    )


@pytest.fixture
def sre_jwt_token() -> str:
    """Return a valid JWT signed with ROLE_SRE_ADMIN."""
    return generate_jwt_token(
        subject="alice-sre@company.com",
        roles=[REQUIRED_SRE_ROLE],
        expires_in_seconds=3600,
        secret=TEST_JWT_SECRET,
    )


@pytest.fixture
def viewer_jwt_token() -> str:
    """Return a JWT lacking the ROLE_SRE_ADMIN role."""
    return generate_jwt_token(
        subject="bob-viewer@company.com",
        roles=["ROLE_VIEWER", "ROLE_DEVELOPER"],
        expires_in_seconds=3600,
        secret=TEST_JWT_SECRET,
    )


# ---------------------------------------------------------------------------
# Test Group 1: JWT Verification and Security Gates
# ---------------------------------------------------------------------------

class TestJWTVerification:
    """Tests for cryptographic validation and role checking."""

    def test_valid_sre_token_decodes_successfully(self, sre_jwt_token: str) -> None:
        claims = verify_jwt_token(sre_jwt_token, secret=TEST_JWT_SECRET)
        assert claims["sub"] == "alice-sre@company.com"
        assert REQUIRED_SRE_ROLE in claims["roles"]

    def test_bearer_prefix_supported(self, sre_jwt_token: str) -> None:
        bearer_token = f"Bearer {sre_jwt_token}"
        claims = verify_jwt_token(bearer_token, secret=TEST_JWT_SECRET)
        assert claims["sub"] == "alice-sre@company.com"

    def test_token_lacking_sre_admin_role_rejected(self, viewer_jwt_token: str) -> None:
        with pytest.raises(UnauthorizedActionError) as exc_info:
            verify_jwt_token(viewer_jwt_token, secret=TEST_JWT_SECRET)
        assert "lacks required authorization role" in str(exc_info.value)

    def test_expired_token_rejected(self) -> None:
        expired_token = generate_jwt_token(
            subject="expired-sre",
            roles=[REQUIRED_SRE_ROLE],
            expires_in_seconds=-10,  # Expired in past
            secret=TEST_JWT_SECRET,
        )
        with pytest.raises(UnauthorizedActionError) as exc_info:
            verify_jwt_token(expired_token, secret=TEST_JWT_SECRET)
        assert "expired" in str(exc_info.value).lower()

    def test_tampered_signature_rejected(self, sre_jwt_token: str) -> None:
        # Change secret to verify mismatch
        with pytest.raises(UnauthorizedActionError):
            verify_jwt_token(sre_jwt_token, secret="wrong-secret-key-987654-secure-32bytes")


    def test_malformed_token_rejected(self) -> None:
        with pytest.raises(UnauthorizedActionError):
            verify_jwt_token("not-a-valid-jwt-token", secret=TEST_JWT_SECRET)

    def test_empty_token_rejected(self) -> None:
        with pytest.raises(UnauthorizedActionError):
            verify_jwt_token("", secret=TEST_JWT_SECRET)


# ---------------------------------------------------------------------------
# Test Group 2: HITL Review Queue & Safety Gate Lifecycle
# ---------------------------------------------------------------------------

class TestHITLReviewQueue:
    """Tests for HITL approval state machine and anti-replay protections."""

    def test_submit_and_approve_lifecycle(self, hitl_queue: HITLReviewQueue) -> None:
        req = hitl_queue.submit_for_review(
            action_type="restart_deployment",
            target="prod/payment-service",
            params={"namespace": "prod", "deployment_name": "payment-service"},
            requested_by="ai-agent",
        )
        assert req.status == ApprovalStatus.PENDING
        assert req.approval_id.startswith("hitl-")

        approved_req = hitl_queue.approve(req.approval_id, approved_by="sre-lead")
        assert approved_req.status == ApprovalStatus.APPROVED
        assert approved_req.approved_by == "sre-lead"

    def test_pending_request_cannot_be_consumed(self, hitl_queue: HITLReviewQueue) -> None:
        req = hitl_queue.submit_for_review(
            action_type="restart_deployment",
            target="prod/payment-service",
            params={},
        )
        with pytest.raises(HITLApprovalRequiredError) as exc_info:
            hitl_queue.consume(req.approval_id, expected_action="restart_deployment", expected_target="prod/payment-service")
        assert "PENDING" in str(exc_info.value)

    def test_rejected_request_cannot_be_consumed(self, hitl_queue: HITLReviewQueue) -> None:
        req = hitl_queue.submit_for_review(
            action_type="restart_deployment",
            target="prod/payment-service",
            params={},
        )
        hitl_queue.reject(req.approval_id, rejected_by="security-team", reason="High traffic window")

        with pytest.raises(HITLApprovalRequiredError) as exc_info:
            hitl_queue.consume(req.approval_id, expected_action="restart_deployment", expected_target="prod/payment-service")
        assert "REJECTED" in str(exc_info.value)
        assert "High traffic window" in str(exc_info.value)

    def test_expired_request_cannot_be_consumed(self, hitl_queue: HITLReviewQueue) -> None:
        req = hitl_queue.submit_for_review(
            action_type="restart_deployment",
            target="prod/payment-service",
            params={},
            ttl_seconds=-10,  # already expired
        )
        with pytest.raises(HITLApprovalRequiredError) as exc_info:
            hitl_queue.approve(req.approval_id, approved_by="sre-lead")
        assert "EXPIRED" in str(exc_info.value)

    def test_anti_replay_executed_request_cannot_be_reused(self, hitl_queue: HITLReviewQueue) -> None:
        req = hitl_queue.submit_for_review(
            action_type="restart_deployment",
            target="prod/payment-service",
            params={},
        )
        hitl_queue.approve(req.approval_id, approved_by="sre-lead")

        # First consumption succeeds
        consumed = hitl_queue.consume(req.approval_id, "restart_deployment", "prod/payment-service")
        assert consumed.status == ApprovalStatus.EXECUTED

        # Second consumption fails (anti-replay)
        with pytest.raises(HITLApprovalRequiredError) as exc_info:
            hitl_queue.consume(req.approval_id, "restart_deployment", "prod/payment-service")
        assert "EXECUTED" in str(exc_info.value) or "replay" in str(exc_info.value).lower()

    def test_action_or_target_mismatch_rejected(self, hitl_queue: HITLReviewQueue) -> None:
        req = hitl_queue.submit_for_review(
            action_type="scale_deployment",
            target="prod/payment-service",
            params={"count": 5},
        )
        hitl_queue.approve(req.approval_id, approved_by="sre-lead")

        # Action mismatch
        with pytest.raises(HITLApprovalRequiredError) as exc_info:
            hitl_queue.consume(req.approval_id, expected_action="restart_deployment", expected_target="prod/payment-service")
        assert "approved for 'scale_deployment'" in str(exc_info.value)

        # Target mismatch
        with pytest.raises(HITLApprovalRequiredError) as exc_info:
            hitl_queue.consume(req.approval_id, expected_action="scale_deployment", expected_target="prod/other-service")
        assert "approved for target 'prod/payment-service'" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Test Group 3: Kubernetes API Actions
# ---------------------------------------------------------------------------

class TestRestartDeployment:
    """Tests for restart_deployment execution with AppsV1Api."""

    def test_successful_restart(
        self,
        k8s_executor: KubernetesExecutor,
        hitl_queue: HITLReviewQueue,
        sre_jwt_token: str,
        mock_apps_v1: MagicMock,
    ) -> None:
        # Prepare approval
        req = hitl_queue.submit_for_review(
            action_type="restart_deployment",
            target="production/order-service",
            params={"namespace": "production", "deployment_name": "order-service"},
        )
        hitl_queue.approve(req.approval_id, approved_by="senior-sre")

        result = k8s_executor.restart_deployment(
            namespace="production",
            deployment_name="order-service",
            auth_token=sre_jwt_token,
            approval_id=req.approval_id,
        )

        assert result.success is True
        assert result.action == "restart_deployment"
        assert result.target == "production/order-service"
        assert result.executed_by == "alice-sre@company.com"
        assert result.approval_id == req.approval_id
        assert result.duration_ms >= 0

        # Verify AppsV1Api was called with patched annotations
        mock_apps_v1.patch_namespaced_deployment.assert_called_once()
        call_kwargs = mock_apps_v1.patch_namespaced_deployment.call_args.kwargs
        assert call_kwargs["name"] == "order-service"
        assert call_kwargs["namespace"] == "production"
        annotations = call_kwargs["body"]["spec"]["template"]["metadata"]["annotations"]
        assert "kubectl.kubernetes.io/restartedAt" in annotations
        assert annotations["opspulse.ai/approvalId"] == req.approval_id

    def test_restart_fails_without_sre_role(
        self,
        k8s_executor: KubernetesExecutor,
        hitl_queue: HITLReviewQueue,
        viewer_jwt_token: str,
    ) -> None:
        req = hitl_queue.submit_for_review(
            action_type="restart_deployment",
            target="production/order-service",
            params={},
        )
        hitl_queue.approve(req.approval_id, approved_by="lead")

        with pytest.raises(UnauthorizedActionError):
            k8s_executor.restart_deployment(
                namespace="production",
                deployment_name="order-service",
                auth_token=viewer_jwt_token,
                approval_id=req.approval_id,
            )

    def test_restart_fails_without_approval_id(
        self,
        k8s_executor: KubernetesExecutor,
        sre_jwt_token: str,
    ) -> None:
        with pytest.raises(HITLApprovalRequiredError):
            k8s_executor.restart_deployment(
                namespace="production",
                deployment_name="order-service",
                auth_token=sre_jwt_token,
                approval_id="",
            )

    def test_restart_k8s_api_failure_handled(
        self,
        k8s_executor: KubernetesExecutor,
        hitl_queue: HITLReviewQueue,
        sre_jwt_token: str,
        mock_apps_v1: MagicMock,
    ) -> None:
        req = hitl_queue.submit_for_review(
            action_type="restart_deployment",
            target="prod/failing-service",
            params={},
        )
        hitl_queue.approve(req.approval_id, approved_by="sre")
        mock_apps_v1.patch_namespaced_deployment.side_effect = Exception("K8s API Connection Refused")

        with pytest.raises(K8sExecutionError) as exc_info:
            k8s_executor.restart_deployment(
                namespace="prod",
                deployment_name="failing-service",
                auth_token=sre_jwt_token,
                approval_id=req.approval_id,
            )
        assert "Connection Refused" in str(exc_info.value)


class TestScaleReplicas:
    """Tests for scale_deployment execution with AppsV1Api."""

    def test_successful_scale(
        self,
        k8s_executor: KubernetesExecutor,
        hitl_queue: HITLReviewQueue,
        sre_jwt_token: str,
        mock_apps_v1: MagicMock,
    ) -> None:
        req = hitl_queue.submit_for_review(
            action_type="scale_deployment",
            target="staging/checkout-api",
            params={"namespace": "staging", "deployment_name": "checkout-api", "count": 6},
        )
        hitl_queue.approve(req.approval_id, approved_by="devops-lead")

        result = k8s_executor.scale_deployment(
            namespace="staging",
            deployment_name="checkout-api",
            count=6,
            auth_token=sre_jwt_token,
            approval_id=req.approval_id,
        )

        assert result.success is True
        assert result.action == "scale_deployment"
        assert result.details["target_replicas"] == 6

        mock_apps_v1.patch_namespaced_deployment_scale.assert_called_once_with(
            name="checkout-api",
            namespace="staging",
            body={"spec": {"replicas": 6}},
        )

    def test_scale_negative_count_rejected(
        self,
        k8s_executor: KubernetesExecutor,
        hitl_queue: HITLReviewQueue,
        sre_jwt_token: str,
    ) -> None:
        req = hitl_queue.submit_for_review(
            action_type="scale_deployment",
            target="staging/checkout-api",
            params={},
        )
        hitl_queue.approve(req.approval_id, approved_by="lead")

        with pytest.raises(InvalidParameterError) as exc_info:
            k8s_executor.scale_deployment(
                namespace="staging",
                deployment_name="checkout-api",
                count=-3,
                auth_token=sre_jwt_token,
                approval_id=req.approval_id,
            )
        assert "non-negative integer" in str(exc_info.value)


class TestRollbackHelmRelease:
    """Tests for rollback_helm_release execution."""

    @patch("subprocess.run")
    def test_successful_helm_rollback(
        self,
        mock_subproc_run: MagicMock,
        k8s_executor: KubernetesExecutor,
        hitl_queue: HITLReviewQueue,
        sre_jwt_token: str,
    ) -> None:
        mock_subproc_run.return_value = MagicMock(
            returncode=0,
            stdout="Rollback was a success! Happy Helming!\n",
            stderr="",
        )

        req = hitl_queue.submit_for_review(
            action_type="rollback_helm_release",
            target="production/ingress-nginx",
            params={"release_name": "ingress-nginx", "namespace": "production", "revision": 2},
        )
        hitl_queue.approve(req.approval_id, approved_by="cluster-admin")

        result = k8s_executor.rollback_helm_release(
            release_name="ingress-nginx",
            auth_token=sre_jwt_token,
            approval_id=req.approval_id,
            namespace="production",
            revision=2,
        )

        assert result.success is True
        assert result.action == "rollback_helm_release"
        assert result.details["release_name"] == "ingress-nginx"
        assert result.details["revision"] == 2

        mock_subproc_run.assert_called_once_with(
            ["helm", "rollback", "ingress-nginx", "2", "--namespace", "production"],
            capture_output=True,
            text=True,
            check=True,
            timeout=60,
        )

    @patch("subprocess.run")
    def test_helm_rollback_command_error(
        self,
        mock_subproc_run: MagicMock,
        k8s_executor: KubernetesExecutor,
        hitl_queue: HITLReviewQueue,
        sre_jwt_token: str,
    ) -> None:
        mock_subproc_run.side_effect = subprocess.CalledProcessError(
            returncode=1,
            cmd=["helm", "rollback", "bad-release"],
            stderr="Error: release: not found\n",
        )

        req = hitl_queue.submit_for_review(
            action_type="rollback_helm_release",
            target="bad-release",
            params={},
        )
        hitl_queue.approve(req.approval_id, approved_by="lead")

        with pytest.raises(K8sExecutionError) as exc_info:
            k8s_executor.rollback_helm_release(
                release_name="bad-release",
                auth_token=sre_jwt_token,
                approval_id=req.approval_id,
            )
        assert "Error: release: not found" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Test Group 4: Module-level Convenience Functions
# ---------------------------------------------------------------------------

class TestConvenienceFunctions:
    """Verify module-level restart_deployment, scale_deployment, rollback_helm_release."""

    def test_convenience_restart(
        self,
        k8s_executor: KubernetesExecutor,
        hitl_queue: HITLReviewQueue,
        sre_jwt_token: str,
    ) -> None:
        req = hitl_queue.submit_for_review(
            action_type="restart_deployment",
            target="kube-system/coredns",
            params={},
        )
        hitl_queue.approve(req.approval_id, approved_by="lead")

        res = restart_deployment(
            namespace="kube-system",
            deployment_name="coredns",
            auth_token=sre_jwt_token,
            approval_id=req.approval_id,
            executor=k8s_executor,
        )
        assert res.success is True

    def test_convenience_scale(
        self,
        k8s_executor: KubernetesExecutor,
        hitl_queue: HITLReviewQueue,
        sre_jwt_token: str,
    ) -> None:
        req = hitl_queue.submit_for_review(
            action_type="scale_deployment",
            target="default/redis-cache",
            params={},
        )
        hitl_queue.approve(req.approval_id, approved_by="lead")

        res = scale_deployment(
            namespace="default",
            deployment_name="redis-cache",
            count=2,
            auth_token=sre_jwt_token,
            approval_id=req.approval_id,
            executor=k8s_executor,
        )
        assert res.success is True
        assert res.details["target_replicas"] == 2

    @patch("subprocess.run")
    def test_convenience_rollback(
        self,
        mock_subproc: MagicMock,
        k8s_executor: KubernetesExecutor,
        hitl_queue: HITLReviewQueue,
        sre_jwt_token: str,
    ) -> None:
        mock_subproc.return_value = MagicMock(returncode=0, stdout="success", stderr="")
        req = hitl_queue.submit_for_review(
            action_type="rollback_helm_release",
            target="prometheus-stack",
            params={},
        )
        hitl_queue.approve(req.approval_id, approved_by="lead")

        res = rollback_helm_release(
            release_name="prometheus-stack",
            auth_token=sre_jwt_token,
            approval_id=req.approval_id,
            executor=k8s_executor,
        )
        assert res.success is True


# ---------------------------------------------------------------------------
# Test Group 5: fetch_pod_logs (read-only diagnostics, JWT-gated)
# ---------------------------------------------------------------------------

class TestFetchPodLogs:
    """Tests for CoreV1Api read-only pod log retrieval."""

    def test_fetch_logs_returns_content(
        self,
        k8s_executor: KubernetesExecutor,
        mock_core_v1: MagicMock,
        sre_jwt_token: str,
    ) -> None:
        mock_core_v1.read_namespaced_pod_log.return_value = (
            "2026-10-08T10:15:25Z ERROR OOMKilled exit code 137"
        )

        result = k8s_executor.fetch_pod_logs(
            namespace="payment-service",
            pod_name="payment-processor-7f8b9-k2x4m",
            auth_token=sre_jwt_token,
            tail_lines=100,
        )

        assert "OOMKilled" in result["logs"]
        assert result["namespace"] == "payment-service"
        assert result["fetched_by"] == "alice-sre@company.com"
        mock_core_v1.read_namespaced_pod_log.assert_called_once()

    def test_fetch_logs_requires_sre_admin_jwt(
        self,
        k8s_executor: KubernetesExecutor,
        mock_core_v1: MagicMock,
        viewer_jwt_token: str,
    ) -> None:
        """Read-only ops must still reject tokens without ROLE_SRE_ADMIN."""
        with pytest.raises(UnauthorizedActionError):
            k8s_executor.fetch_pod_logs(
                namespace="default",
                pod_name="nginx-abc",
                auth_token=viewer_jwt_token,
            )
        mock_core_v1.read_namespaced_pod_log.assert_not_called()

    def test_fetch_logs_rejects_missing_token(
        self,
        k8s_executor: KubernetesExecutor,
        mock_core_v1: MagicMock,
    ) -> None:
        with pytest.raises(UnauthorizedActionError):
            k8s_executor.fetch_pod_logs(
                namespace="default",
                pod_name="nginx-abc",
                auth_token="",
            )
        mock_core_v1.read_namespaced_pod_log.assert_not_called()

    def test_fetch_logs_invalid_tail_lines_rejected(
        self,
        k8s_executor: KubernetesExecutor,
        sre_jwt_token: str,
    ) -> None:
        with pytest.raises(InvalidParameterError):
            k8s_executor.fetch_pod_logs(
                namespace="default",
                pod_name="nginx-abc",
                auth_token=sre_jwt_token,
                tail_lines=0,
            )

    def test_fetch_logs_k8s_api_failure_raises_execution_error(
        self,
        k8s_executor: KubernetesExecutor,
        mock_core_v1: MagicMock,
        sre_jwt_token: str,
    ) -> None:
        mock_core_v1.read_namespaced_pod_log.side_effect = RuntimeError("404 pod not found")
        with pytest.raises(K8sExecutionError):
            k8s_executor.fetch_pod_logs(
                namespace="default",
                pod_name="missing-pod",
                auth_token=sre_jwt_token,
            )


# ---------------------------------------------------------------------------
# Test Group 6: Authorization gate — no execution without a valid JWT
# ---------------------------------------------------------------------------

class TestAuthorizationGate:
    """
    Verify the core security invariant: NO Kubernetes execution occurs
    unless accompanied by a valid JWT signed with ROLE_SRE_ADMIN.
    """

    def test_restart_blocked_without_sre_role(
        self,
        k8s_executor: KubernetesExecutor,
        hitl_queue: HITLReviewQueue,
        viewer_jwt_token: str,
    ) -> None:
        req = hitl_queue.submit_for_review(
            action_type="restart_deployment",
            target="default/web",
            params={},
        )
        hitl_queue.approve(req.approval_id, approved_by="lead")

        with pytest.raises(UnauthorizedActionError):
            k8s_executor.restart_deployment(
                namespace="default",
                deployment_name="web",
                auth_token=viewer_jwt_token,
                approval_id=req.approval_id,
            )

    def test_scale_blocked_with_tampered_jwt(
        self,
        k8s_executor: KubernetesExecutor,
        hitl_queue: HITLReviewQueue,
        sre_jwt_token: str,
    ) -> None:
        req = hitl_queue.submit_for_review(
            action_type="scale_deployment",
            target="default/web",
            params={},
        )
        hitl_queue.approve(req.approval_id, approved_by="lead")

        tampered = sre_jwt_token[:-3] + "aaa"
        with pytest.raises(UnauthorizedActionError):
            k8s_executor.scale_deployment(
                namespace="default",
                deployment_name="web",
                count=3,
                auth_token=tampered,
                approval_id=req.approval_id,
            )

    def test_rollback_blocked_without_sre_role(
        self,
        k8s_executor: KubernetesExecutor,
        hitl_queue: HITLReviewQueue,
        viewer_jwt_token: str,
    ) -> None:
        req = hitl_queue.submit_for_review(
            action_type="rollback_helm_release",
            target="prometheus-stack",
            params={},
        )
        hitl_queue.approve(req.approval_id, approved_by="lead")

        with patch("subprocess.run") as mock_run:
            with pytest.raises(UnauthorizedActionError):
                k8s_executor.rollback_helm_release(
                    release_name="prometheus-stack",
                    auth_token=viewer_jwt_token,
                    approval_id=req.approval_id,
                )
            mock_run.assert_not_called()

    def test_expired_jwt_rejected(
        self,
        k8s_executor: KubernetesExecutor,
        hitl_queue: HITLReviewQueue,
    ) -> None:
        expired = generate_jwt_token(
            subject="alice-sre@company.com",
            roles=[REQUIRED_SRE_ROLE],
            expires_in_seconds=-10,
            secret=TEST_JWT_SECRET,
        )
        req = hitl_queue.submit_for_review(
            action_type="restart_deployment",
            target="default/web",
            params={},
        )
        hitl_queue.approve(req.approval_id, approved_by="lead")

        with pytest.raises(UnauthorizedActionError) as exc:
            k8s_executor.restart_deployment(
                namespace="default",
                deployment_name="web",
                auth_token=expired,
                approval_id=req.approval_id,
            )
        assert "expired" in str(exc.value).lower()

    def test_missing_approval_id_blocks_mutation(
        self,
        k8s_executor: KubernetesExecutor,
        sre_jwt_token: str,
    ) -> None:
        """A valid JWT alone is insufficient — HITL approval must exist."""
        with pytest.raises(HITLApprovalRequiredError):
            k8s_executor.restart_deployment(
                namespace="default",
                deployment_name="web",
                auth_token=sre_jwt_token,
                approval_id="hitl-nonexistent",
            )
