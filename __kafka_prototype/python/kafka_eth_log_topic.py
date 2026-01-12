from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Iterable


def _workspace_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _compose_file() -> Path:
    return _workspace_root() / "__kafka_prototype" / "docker" / "docker-compose.yml"


def ensure_compose_up(*, project: str) -> None:
    subprocess.run(
        [
            "docker",
            "compose",
            "-p",
            project,
            "-f",
            str(_compose_file()),
            "up",
            "-d",
            "--wait",
        ],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def get_end_offset(*, project: str, topic: str, partition: int) -> int:
    _create_topic_if_needed(project=project, topic=topic)

    out = subprocess.run(
        [
            "docker",
            "compose",
            "-p",
            project,
            "-f",
            str(_compose_file()),
            "exec",
            "-T",
            "kafka",
            "kafka-get-offsets",
            "--bootstrap-server",
            "kafka:9092",
            "--topic",
            topic,
            "--partitions",
            str(partition),
            "--time",
            "latest",
        ],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ).stdout

    # Expected line format: <topic>:<partition>:<offset>
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(":")
        if len(parts) == 3 and parts[0] == topic and int(parts[1]) == int(partition):
            return int(parts[2])

    raise ValueError(f"Unexpected kafka-get-offsets output for {topic}:{partition}: {out!r}")


def _create_topic_if_needed(*, project: str, topic: str) -> None:
    subprocess.run(
        [
            "docker",
            "compose",
            "-p",
            project,
            "-f",
            str(_compose_file()),
            "exec",
            "-T",
            "kafka",
            "kafka-topics",
            "--bootstrap-server",
            "kafka:9092",
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        ],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def produce_raw_logs(*, project: str, topic: str, raw_logs: Iterable[dict]) -> None:
    _create_topic_if_needed(project=project, topic=topic)

    payload = "\n".join(json.dumps(r, separators=(",", ":"), sort_keys=True) for r in raw_logs) + "\n"

    subprocess.run(
        [
            "docker",
            "compose",
            "-p",
            project,
            "-f",
            str(_compose_file()),
            "exec",
            "-T",
            "kafka",
            "kafka-console-producer",
            "--bootstrap-server",
            "kafka:9092",
            "--topic",
            topic,
        ],
        check=True,
        input=payload,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def consume_raw_logs(
    *,
    project: str,
    topic: str,
    partition: int,
    offset: int,
    max_messages: int,
) -> list[dict]:
    _create_topic_if_needed(project=project, topic=topic)

    out = subprocess.run(
        [
            "docker",
            "compose",
            "-p",
            project,
            "-f",
            str(_compose_file()),
            "exec",
            "-T",
            "kafka",
            "kafka-console-consumer",
            "--bootstrap-server",
            "kafka:9092",
            "--topic",
            topic,
            "--property",
            "print.summary=false",
            "--partition",
            str(partition),
            "--offset",
            str(offset),
            "--max-messages",
            str(max_messages),
        ],
        check=True,
        stdout=subprocess.PIPE,
        # kafka-console-consumer prints a summary line to stderr; keep stdout parseable.
        stderr=subprocess.PIPE,
        text=True,
    ).stdout

    lines = [line for line in out.splitlines() if line.strip()]
    return [json.loads(line) for line in lines]
