from __future__ import annotations

import json
import subprocess
import uuid
from pathlib import Path
import sys

import pytest


@pytest.mark.e2e
def test_kafka_compose_can_produce_and_consume_bounded_messages() -> None:
    """E2E-ish smoke: docker compose brings Kafka up, then we produce+consume a bounded set."""

    topic = f"eth-logs-smoke-{uuid.uuid4().hex[:10]}"
    messages = [
        {"n": 1, "tx": "0x" + "11" * 32, "logIndex": 1},
        {"n": 2, "tx": "0x" + "22" * 32, "logIndex": 2},
        {"n": 3, "tx": "0x" + "33" * 32, "logIndex": 3},
    ]

    def run(*args: str, input_text: str | None = None) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            list(args),
            text=True,
            input=input_text,
            capture_output=True,
            check=False,
        )

    workspace_root = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(workspace_root / "__kafka_prototype" / "python"))

    from kafka_eth_log_topic import (  # type: ignore
        ensure_compose_up,
        get_end_offset,
        produce_raw_logs,
        consume_raw_logs,
    )

    project = "kafka-proto"
    topic = f"eth-logs-smoke-{uuid.uuid4().hex[:10]}"

    compose = ("docker", "compose", "--project-name", project, "-f", "docker/docker-compose.yml")

    # Start Kafka (must be deterministic; rely on compose --wait + healthchecks).
    run(*compose, "down", "-v", "--remove-orphans")
    ensure_compose_up(project=project)

    try:
        before_end = get_end_offset(project=project, topic=topic, partition=0)
        produce_raw_logs(project=project, topic=topic, raw_logs=messages)
        after_end = get_end_offset(project=project, topic=topic, partition=0)

        start = after_end - len(messages)
        consumed = consume_raw_logs(
            project=project,
            topic=topic,
            partition=0,
            offset=start,
            max_messages=len(messages),
        )

        assert before_end <= start and len(consumed) == len(messages)
    finally:
        run(*compose, "down", "-v", "--remove-orphans")
