"""Airflow integration test: execute the deterministic ingest DAG.

This uses `airflow dags test` (scheduler-independent) with `latest_blocks`.
It requires ALCHEMY_URL to be present in the Airflow container environment.
"""

import os
import subprocess

import pytest


def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    """Run a subprocess command and capture stdout/stderr for assertions."""
    return subprocess.run(cmd, check=False, capture_output=True, text=True)


def test_airflow_can_execute_uniswap_dag_with_latest_blocks_conf():
    """The ingest DAG can run end-to-end with deterministic `dag_run.conf`."""
    compose = ["docker", "compose", "-f", "docker/docker-compose.yml"]

    up = _run(compose + ["up", "-d", "postgres", "neo4j", "airflow-webserver", "airflow-scheduler"])
    assert up.returncode == 0, up.stderr

    conf = '{"latest_blocks": 5}'

    has_alchemy_url = _run(
        compose
        + [
            "exec",
            "-T",
            "airflow-webserver",
            "python",
            "-c",
            "import os; raise SystemExit(0 if os.getenv('ALCHEMY_URL') else 2)",
        ]
    )
    assert (
        has_alchemy_url.returncode == 0
    ), "Missing ALCHEMY_URL in containers. Put it in docker/.env (preferred) or export it in your shell."

    res = _run(
        compose
        + [
            "exec",
            "-T",
            "airflow-webserver",
            "env",
            "PYTHONWARNINGS=ignore",
            "airflow",
            "dags",
            "test",
            "uniswap_weth_usdc_ingest_100_events_to_neo4j",
            "2026-01-01",
            "--conf",
            conf,
        ]
    )

    assert res.returncode == 0, res.stdout + "\n" + res.stderr
