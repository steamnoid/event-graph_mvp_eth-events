import json
import subprocess
import time
from pathlib import Path

import pytest


@pytest.mark.e2e
def test_airflow_dag_enrollment_to_neo4j_graph_produces_c5_artifact(tmp_path):
    """Trigger the Enrollment DAG and assert the final validation artifact exists.

    This test intentionally avoids querying Airflow's Postgres metastore directly.
    Instead it checks for the DAG-produced artifact C5.txt, written only by the
    final validation task (Neo4j readback + C4==C5 assertion).
    """

    repo_root = Path(__file__).resolve().parents[2]
    docker_dir = repo_root / "docker"

    fixture = repo_root / "test" / "fixtures" / "events" / "enrollment_events.json"
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    assert isinstance(payload, list) and len(payload) > 0

    dag_id = "enrollment_to_neo4j_graph"
    run_id = f"e2e__{int(time.time())}"

    host_shared_dir = docker_dir / "logs" / "e2e_inputs" / run_id
    host_shared_dir.mkdir(parents=True, exist_ok=True)
    host_events_file = host_shared_dir / "events.json"
    host_events_file.write_text(json.dumps(payload), encoding="utf-8")
    container_events_file = f"/opt/airflow/logs/e2e_inputs/{run_id}/events.json"

    conf = json.dumps({"source_events_file": container_events_file, "expect_canonical": True})

    subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "airflow-webserver",
            "airflow",
            "dags",
            "trigger",
            "-r",
            run_id,
            "-c",
            conf,
            dag_id,
        ],
        cwd=str(docker_dir),
        check=True,
    )

    # Poll for the final artifact (written under the shared logs volume).
    host_run_dir = docker_dir / "logs" / "eventgraph" / run_id
    c5_path = host_run_dir / "C5.txt"

    deadline = time.time() + 180
    while time.time() < deadline:
        if c5_path.exists():
            text = c5_path.read_text(encoding="utf-8")
            assert f"run_id={run_id}" in text
            return
        time.sleep(2)

    # Failure context: list whatever artifacts exist.
    existing = []
    if host_run_dir.exists():
        existing = sorted(p.name for p in host_run_dir.glob("*") if p.is_file())
    pytest.fail(f"Timed out waiting for {c5_path}. Existing artifacts: {existing}")
