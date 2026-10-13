"""Linux eBPF telemetry sensor that forwards kernel events to Kafka.

Requires Linux, BCC (including its kernel headers/toolchain), sufficient
CAP_BPF/CAP_PERFMON or root privileges, and a Kafka broker. This module is
importable on non-Linux hosts; platform-specific dependencies are loaded only
when the sensor is started.
"""

from __future__ import annotations

import argparse
import json
import logging
import platform
import signal
import socket
import threading
from datetime import datetime, timezone
from typing import Any

KAFKA_TOPIC = "k8s.ebpf.kernel.telemetry"
logger = logging.getLogger(__name__)

# Trace syscall entry points and skb frees carrying a kernel drop reason.
# Payloads intentionally retain syscall arguments as raw values; they do not
# dereference user pointers (except execve's filename) in kernel context.
BPF_PROGRAM = r"""
#include <uapi/linux/ptrace.h>
#include <linux/skbuff.h>

enum event_type { EVENT_CONNECT = 1, EVENT_EXECVE = 2, EVENT_SOCKET_DROP = 3 };
struct event_t {
    u64 ts_ns;
    u32 pid;
    u32 uid;
    s32 type;
    s64 arg0;
    u64 arg1;
    u64 arg2;
    u32 drop_reason;
    char comm[TASK_COMM_LEN];
    char filename[128];
};
BPF_PERF_OUTPUT(events);

TRACEPOINT_PROBE(syscalls, sys_enter_connect) {
    struct event_t event = {};
    u64 id = bpf_get_current_pid_tgid();
    event.ts_ns = bpf_ktime_get_ns();
    event.pid = id >> 32;
    event.uid = bpf_get_current_uid_gid();
    event.type = EVENT_CONNECT;
    event.arg0 = args->fd;
    event.arg1 = args->uservaddr;
    event.arg2 = args->addrlen;
    bpf_get_current_comm(&event.comm, sizeof(event.comm));
    events.perf_submit(args, &event, sizeof(event));
    return 0;
}

TRACEPOINT_PROBE(syscalls, sys_enter_execve) {
    struct event_t event = {};
    u64 id = bpf_get_current_pid_tgid();
    event.ts_ns = bpf_ktime_get_ns();
    event.pid = id >> 32;
    event.uid = bpf_get_current_uid_gid();
    event.type = EVENT_EXECVE;
    event.arg0 = args->filename;
    event.arg1 = args->argv;
    event.arg2 = args->envp;
    bpf_get_current_comm(&event.comm, sizeof(event.comm));
    bpf_probe_read_user_str(&event.filename, sizeof(event.filename),
                            (void *)args->filename);
    events.perf_submit(args, &event, sizeof(event));
    return 0;
}

TRACEPOINT_PROBE(skb, kfree_skb) {
    struct event_t event = {};
    u64 id = bpf_get_current_pid_tgid();
    event.ts_ns = bpf_ktime_get_ns();
    event.pid = id >> 32;
    event.uid = bpf_get_current_uid_gid();
    event.type = EVENT_SOCKET_DROP;
    event.arg0 = (s64)args->skbaddr;
    event.arg1 = args->location;
    event.drop_reason = args->reason;
    bpf_get_current_comm(&event.comm, sizeof(event.comm));
    events.perf_submit(args, &event, sizeof(event));
    return 0;
}
"""

_EVENT_NAMES = {1: "sys_enter_connect", 2: "sys_enter_execve", 3: "socket_drop"}


class EBPFKafkaSensor:
    """Attach BCC probes and publish each event to the configured Kafka topic."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = KAFKA_TOPIC,
        *,
        bpf: Any = None,
        producer: Any = None,
        hostname: str | None = None,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self._bpf = bpf
        self._producer = producer
        self.hostname = hostname or socket.gethostname()
        self._stop = threading.Event()
        self._sequence = 0

    @staticmethod
    def _dependencies() -> tuple[Any, Any]:
        if platform.system() != "Linux":
            raise RuntimeError("The eBPF sensor can only run on Linux.")
        try:
            from bcc import BPF
            from confluent_kafka import Producer
        except ImportError as exc:
            raise RuntimeError(
                "Install the Linux BCC Python bindings and confluent-kafka before "
                "running the eBPF sensor."
            ) from exc
        return BPF, Producer

    def _ensure_runtime(self) -> None:
        if self._bpf is None or self._producer is None:
            BPF, Producer = self._dependencies()
            if self._bpf is None:
                self._bpf = BPF(text=BPF_PROGRAM)
            if self._producer is None:
                self._producer = Producer({"bootstrap.servers": self.bootstrap_servers})

    def _on_event(self, _cpu: int, data: Any, _size: int) -> None:
        event = self._bpf["events"].event(data)
        event_type = int(event.type)
        payload = {
            "schema_version": 1,
            "event_id": f"{self.hostname}:{int(event.ts_ns)}:{self._sequence}",
            # bpf_ktime_get_ns() is monotonic time since boot, not Unix time.
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "kernel_timestamp_ns": int(event.ts_ns),
            "source": "ebpf",
            "event_type": _EVENT_NAMES.get(event_type, f"unknown_{event_type}"),
            "host": self.hostname,
            "pid": int(event.pid),
            "uid": int(event.uid),
            "comm": self._decode(event.comm),
            "syscall_args": {
                "arg0": int(event.arg0),
                "arg1": int(event.arg1),
                "arg2": int(event.arg2),
            },
        }
        if event_type == 2:
            payload["filename"] = self._decode(event.filename)
        if event_type == 3:
            payload["drop_reason"] = int(event.drop_reason)
        self._sequence += 1
        encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        try:
            self._producer.produce(
                self.topic,
                key=str(payload["pid"]).encode("ascii"),
                value=encoded,
                on_delivery=self._on_delivery,
            )
            # Serve delivery callbacks and apply producer backpressure.
            self._producer.poll(0)
        except BufferError:
            self._producer.poll(1.0)
            self._producer.produce(
                self.topic, key=str(payload["pid"]).encode("ascii"), value=encoded,
                on_delivery=self._on_delivery,
            )

    @staticmethod
    def _decode(value: Any) -> str:
        if isinstance(value, bytes):
            return value.split(b"\0", 1)[0].decode("utf-8", errors="replace")
        return str(value).split("\0", 1)[0]

    @staticmethod
    def _on_delivery(error: Any, message: Any) -> None:
        if error is not None:
            logger.error("Kafka delivery failed for %s: %s", message.topic(), error)

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        """Attach probes and publish until interrupted; flush Kafka on exit."""
        self._ensure_runtime()
        self._bpf["events"].open_perf_buffer(self._on_event, page_cnt=64)
        logger.info("Publishing eBPF events to Kafka topic %s", self.topic)
        try:
            while not self._stop.is_set():
                self._bpf.perf_buffer_poll(timeout=100)
                self._producer.poll(0)
        finally:
            remaining = self._producer.flush(10.0)
            if remaining:
                logger.warning("%d eBPF events remain undelivered after flush", remaining)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default=KAFKA_TOPIC)
    args = parser.parse_args()
    sensor = EBPFKafkaSensor(args.bootstrap_servers, args.topic)
    signal.signal(signal.SIGINT, lambda *_: sensor.stop())
    signal.signal(signal.SIGTERM, lambda *_: sensor.stop())
    logging.basicConfig(level=logging.INFO)
    sensor.run()


if __name__ == "__main__":  # pragma: no cover - exercised as a Linux service
    main()
