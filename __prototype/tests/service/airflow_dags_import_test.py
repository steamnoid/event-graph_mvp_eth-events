"""Airflow integration test: DAGs import cleanly inside the container."""

import json
import subprocess


def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    """Run a subprocess command and capture stdout/stderr for assertions."""
    return subprocess.run(cmd, check=False, capture_output=True, text=True)


def _extract_json_array(stdout: str) -> list:
    """Extract the first JSON array found in Airflow CLI output."""
    start = stdout.find("[")
    end = stdout.rfind("]")
    if start == -1 or end == -1 or end < start:
        raise AssertionError(f"Could not find JSON array in output:\n{stdout}")
    return json.loads(stdout[start : end + 1])


def test_airflow_dags_import_without_errors():
    """Airflow container reports no DAG import errors."""
    compose = ["docker", "compose", "-f", "docker/docker-compose.yml"]

    up = _run(compose + ["up", "-d", "postgres", "airflow-webserver", "airflow-scheduler"])
    assert up.returncode == 0, up.stderr

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
            "list-import-errors",
            "-o",
            "json",
        ]
    )
    assert res.returncode == 0, res.stderr

    errors = _extract_json_array(res.stdout)
    assert errors == [], errors
