import subprocess
from pathlib import Path

import pytest


@pytest.mark.e2e
def test_kafka_dag_runs_without_conf():
    repo_root = Path(__file__).resolve().parents[2]
    docker_dir = repo_root / "docker"

    subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "airflow-webserver",
            "airflow",
            "dags",
            "test",
            "kafka_eth_to_neo4j_graph",
            "2026-01-04",
        ],
        cwd=str(docker_dir),
        check=True,
    )
