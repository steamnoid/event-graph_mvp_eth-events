from __future__ import annotations

import subprocess
from pathlib import Path

import pytest


@pytest.mark.e2e
def test_kafka_kraft_compose_up_and_topic_admin_works() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose_file = repo_root / "docker" / "docker-compose.yml"

    project = "eventgraph-kafka-prototype"

    subprocess.run(
        [
            "docker",
            "compose",
            "-p",
            project,
            "-f",
            str(compose_file),
            "up",
            "-d",
            "--wait",
        ],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    topic = "eth.raw_logs"

    subprocess.run(
        [
            "docker",
            "compose",
            "-p",
            project,
            "-f",
            str(compose_file),
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

    out = subprocess.run(
        [
            "docker",
            "compose",
            "-p",
            project,
            "-f",
            str(compose_file),
            "exec",
            "-T",
            "kafka",
            "kafka-topics",
            "--bootstrap-server",
            "kafka:9092",
            "--list",
        ],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    ).stdout

    assert topic in out.splitlines()
