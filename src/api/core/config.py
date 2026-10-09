"""
Application settings loaded from environment variables / .env file.

Uses pydantic-settings v2 so every value is type-validated at startup.
Override any setting by exporting the corresponding env var, e.g.:
    export REDIS_URL=redis://my-redis:6379/0
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field, RedisDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Central configuration object.  All Celery / Redis knobs live here so
    the rest of the codebase stays free of hard-coded connection strings.
    """

    model_config = SettingsConfigDict(
        # Load from .env when present; silently skip if absent.
        env_file=".env",
        env_file_encoding="utf-8",
        # Allow case-insensitive env var names.
        case_sensitive=False,
        # Ignore unrecognised env vars — avoids breakage in shared envs.
        extra="ignore",
    )

    # ── App meta ──────────────────────────────────────────────────────────
    APP_NAME: str = Field(default="opspulse-ai", description="Human-readable service name.")
    APP_VERSION: str = Field(default="0.1.0")
    DEBUG: bool = Field(default=False)

    # ── Redis / Celery ────────────────────────────────────────────────────
    REDIS_URL: RedisDsn = Field(
        default="redis://localhost:6379/0",  # type: ignore[assignment]
        description="Full Redis DSN used as both broker and result backend.",
    )
    CELERY_TASK_SERIALIZER: str = Field(default="json")
    CELERY_RESULT_SERIALIZER: str = Field(default="json")
    CELERY_ACCEPT_CONTENT: list[str] = Field(default=["json"])
    # Hard limit per task — prevents runaway alert processors.
    CELERY_TASK_SOFT_TIME_LIMIT: int = Field(
        default=300, description="Soft time limit (seconds) before a task gets a SoftTimeLimitExceeded."
    )
    CELERY_TASK_TIME_LIMIT: int = Field(
        default=360, description="Hard time limit (seconds) before a task is killed."
    )
    # Queue name dedicated to alert ingestion.
    ALERT_QUEUE_NAME: str = Field(default="alerts")

    # ── Qdrant ────────────────────────────────────────────────────────────
    QDRANT_HOST: str = Field(default="localhost")
    QDRANT_PORT: int = Field(default=6333)
    QDRANT_COLLECTION: str = Field(
        default="runbooks",
        description="Qdrant collection that stores runbook chunk embeddings.",
    )
    QDRANT_API_KEY: str | None = Field(
        default=None,
        description="API key for Qdrant Cloud; leave None for local instances.",
    )

    # ── RAG / Retriever ───────────────────────────────────────────────────
    RUNBOOKS_DIR: str = Field(
        default="data/runbooks",
        description="Path (relative to project root) containing Markdown runbooks.",
    )
    DENSE_MODEL_NAME: str = Field(
        default="sentence-transformers/all-MiniLM-L6-v2",
        description="HuggingFace model used for dense passage embeddings.",
    )
    CROSS_ENCODER_MODEL_NAME: str = Field(
        default="cross-encoder/ms-marco-MiniLM-L-6-v2",
        description="Cross-encoder model used to re-rank candidate chunks.",
    )
    # Embedding vector dimension for all-MiniLM-L6-v2
    DENSE_VECTOR_SIZE: int = Field(default=384)
    # How many candidates each leg (dense + sparse) fetches before fusion
    RETRIEVAL_CANDIDATES: int = Field(default=10)
    # Final results returned after re-ranking
    TOP_K_RESULTS: int = Field(default=2)
    # Reciprocal Rank Fusion constant (higher = flatter score distribution)
    RRF_K: int = Field(default=60)
    # Markdown chunk size (characters)
    CHUNK_SIZE: int = Field(default=512)
    CHUNK_OVERLAP: int = Field(default=64)

    # ── LLM providers ─────────────────────────────────────────────────────
    # Primary: Groq (fast inference, llama / mixtral family)
    GROQ_API_KEY: str | None = Field(
        default=None,
        description="Groq Cloud API key. Set to use Groq as the LLM backend.",
    )
    GROQ_MODEL: str = Field(
        default="llama3-70b-8192",
        description="Groq model identifier for remediation planning.",
    )
    # Fallback: Anthropic Claude
    ANTHROPIC_API_KEY: str | None = Field(
        default=None,
        description="Anthropic API key. Used when GROQ_API_KEY is not set.",
    )
    ANTHROPIC_MODEL: str = Field(
        default="claude-3-5-sonnet-20241022",
        description="Anthropic Claude model identifier.",
    )
    # Shared LLM generation settings
    LLM_TEMPERATURE: float = Field(
        default=0.1,
        description="Sampling temperature (lower = more deterministic).",
    )
    LLM_MAX_TOKENS: int = Field(
        default=2048,
        description="Maximum tokens in a single LLM completion.",
    )
    LLM_TIMEOUT: int = Field(
        default=60,
        description="LLM API request timeout in seconds.",
    )

    # ── Slack HITL notifications ───────────────────────────────────────────
    SLACK_WEBHOOK_URL: str | None = Field(
        default=None,
        description=(
            "Slack Incoming Webhook URL. HIGH-risk actions are posted here "
            "for human approval before execution."
        ),
    )
    SLACK_TIMEOUT: int = Field(
        default=10,
        description="HTTP timeout (seconds) for Slack Webhook calls.",
    )

    # ── LangGraph / agent settings ────────────────────────────────────────
    AGENT_MAX_RETRIES: int = Field(
        default=2,
        description="Maximum node-level retries before marking a run as failed.",
    )
    # Risk tiers that require human-in-the-loop approval
    HITL_RISK_TIERS: list[str] = Field(
        default=["HIGH", "CRITICAL"],
        description="Proposed action risk tiers that must be routed to Slack HITL.",
    )
    LANGGRAPH_RECURSION_LIMIT: int = Field(
        default=25,
        description="Maximum graph recursion depth (LangGraph config key).",
    )

    # ── Docker sandbox ────────────────────────────────────────────────────
    DOCKER_BASE_URL: str = Field(
        default="unix://var/run/docker.sock",
        description=(
            "Docker daemon socket. "
            "Windows named pipe: 'npipe:////./pipe/docker_engine'. "
            "Remote TCP: 'tcp://host:2376'."
        ),
    )
    DOCKER_TIMEOUT: int = Field(
        default=30,
        description="Docker SDK API call timeout in seconds.",
    )
    SANDBOX_NETWORK_NAME: str = Field(
        default="opspulse_sandbox",
        description="Isolated Docker bridge network used for sandboxed restarts.",
    )
    SANDBOX_RESTART_TIMEOUT: int = Field(
        default=10,
        description="Seconds to wait for a graceful container stop before SIGKILL.",
    )
    SANDBOX_LOG_LINES: int = Field(
        default=100,
        description="Number of tail log lines returned by inspect_container_logs().",
    )
    # Allowlist of container name prefixes that sandbox operations may touch.
    # Empty list = allow any container (use with caution in production).
    SANDBOX_ALLOWED_PREFIXES: list[str] = Field(
        default=[],
        description=(
            "Container name prefixes the sandbox is permitted to operate on. "
            "Empty list disables the allowlist check (dev only)."
        ),
    )

    # ── Prometheus metrics ────────────────────────────────────────────────
    METRICS_ENABLED: bool = Field(
        default=True,
        description="Enable Prometheus metrics collection.",
    )
    METRICS_NAMESPACE: str = Field(
        default="opspulse",
        description="Prometheus metric name namespace prefix.",
    )

    # ── Kubernetes & HITL Executor ─────────────────────────────────────────────
    JWT_SECRET_KEY: str = Field(
        default="opspulse-insecure-default-jwt-secret-key-change-in-production",
        description=(
            "Secret key for signing and verifying HITL action JWT tokens. "
            "Every Kubernetes execution requires a JWT signed with ROLE_SRE_ADMIN."
        ),
    )
    JWT_ALGORITHM: str = Field(
        default="HS256",
        description="Algorithm used for JWT signature verification.",
    )
    K8S_KUBECONFIG_PATH: str | None = Field(
        default=None,
        description=(
            "Path to kubeconfig file; None uses in-cluster config or the "
            "default ~/.kube/config."
        ),
    )
    HELM_BINARY_PATH: str = Field(
        default="helm",
        description="Path or command name for the Helm CLI binary.",
    )

    # ── Kafka telemetry event streaming ────────────────────────────────────────
    KAFKA_BOOTSTRAP_SERVERS: str = Field(
        default="localhost:9092",
        description="Comma-separated Kafka broker addresses.",
    )
    KAFKA_GROUP_ID: str = Field(
        default="opspulse-telemetry-consumer",
        description="Kafka consumer group ID.",
    )
    KAFKA_LOGS_TOPIC: str = Field(
        default="k8s.pod.logs",
        description="Kafka topic for Kubernetes pod log events.",
    )
    KAFKA_ALERTS_TOPIC: str = Field(
        default="k8s.system.alerts",
        description="Kafka topic for Kubernetes system alert events.",
    )
    KAFKA_DLQ_TOPIC: str = Field(
        default="k8s.telemetry.dlq",
        description="Kafka Dead Letter Queue (DLQ) topic for failed records.",
    )
    KAFKA_AUTO_OFFSET_RESET: str = Field(
        default="earliest",
        description="Kafka auto.offset.reset setting (earliest | latest).",
    )
    KAFKA_ENABLE_AUTO_COMMIT: bool = Field(
        default=False,
        description=(
            "Manual Kafka offset commit control. Must remain False — offsets "
            "are only committed after successful LangGraph triage (or DLQ "
            "quarantine) to guarantee at-least-once processing."
        ),
    )
    KAFKA_ERROR_SPIKE_THRESHOLD: int = Field(
        default=5,
        description="Errors within the sliding window required to trigger RCA.",
    )
    KAFKA_ERROR_SPIKE_WINDOW_SECONDS: int = Field(
        default=60,
        description="Sliding window duration in seconds for error spike detection.",
    )

    @field_validator("REDIS_URL", mode="before")
    @classmethod
    def _coerce_redis_url(cls, v: object) -> object:
        """Accept plain strings; pydantic-settings will validate them as RedisDsn."""
        return v


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Return a cached singleton Settings instance.

    Using lru_cache means the .env file is read exactly once per process,
    and tests can override settings by clearing the cache:
        get_settings.cache_clear()
    """
    return Settings()
