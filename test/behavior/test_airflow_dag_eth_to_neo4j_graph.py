import json
import subprocess
import time
from pathlib import Path

import pytest


@pytest.mark.e2e
def test_airflow_dag_eth_to_neo4j_graph_run_all_tasks_success(tmp_path):
    """Trigger the DAG and assert all four tasks end in SUCCESS."""

    repo_root = Path(__file__).resolve().parents[2]
    docker_dir = repo_root / "docker"

    # Use the existing deterministic fixture (no RPC required).
    fixture = repo_root / "test" / "fixtures" / "eth_logs" / "fetch_logs_uniswap_v2_weth_usdc.json"
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    assert isinstance(payload, list) and len(payload) > 0

    dag_id = "eth_to_neo4j_graph"
    run_id = f"e2e__{int(time.time())}"

    # Put logs into the Airflow shared logs volume so both webserver and scheduler can read it.
    host_shared_dir = docker_dir / "logs" / "e2e_inputs" / run_id
    host_shared_dir.mkdir(parents=True, exist_ok=True)
    host_logs_file = host_shared_dir / "raw_logs.json"
    host_logs_file.write_text(json.dumps(payload), encoding="utf-8")
    container_logs_file = f"/opt/airflow/logs/e2e_inputs/{run_id}/raw_logs.json"

    conf = json.dumps({"source_logs_file": container_logs_file})

    # Trigger the DAG.
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

    # Poll Airflow metadata DB for task completion.
    expected_tasks = {
        "fetch_logs_to_file",
        "transform_logs_to_events_file",
        "transform_events_to_graph_file",
        "write_graph_to_neo4j",
    }

    def _psql_scalar(sql: str) -> str:
        p = subprocess.run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "postgres",
                "psql",
                "-U",
                "airflow",
                "-d",
                "airflow",
                "-tAc",
                sql,
            ],
            cwd=str(docker_dir),
            capture_output=True,
            text=True,
            check=True,
        )
        return (p.stdout or "").strip()

    deadline = time.time() + 120
    while time.time() < deadline:
        dag_state = _psql_scalar(
            f"SELECT COALESCE(state,'') FROM dag_run WHERE dag_id='{dag_id}' AND run_id='{run_id}' LIMIT 1;"
        )
        success_tasks = int(
            _psql_scalar(
                "SELECT COUNT(1) FROM task_instance "
                f"WHERE dag_id='{dag_id}' AND run_id='{run_id}' AND state='success';"
            )
            or "0"
        )
        failed_tasks = int(
            _psql_scalar(
                "SELECT COUNT(1) FROM task_instance "
                f"WHERE dag_id='{dag_id}' AND run_id='{run_id}' AND state='failed';"
            )
            or "0"
        )

        if failed_tasks > 0:
            pytest.fail("At least one task ended in FAILED")

        if dag_state == "success" and success_tasks == len(expected_tasks):
            assert True
            return

        time.sleep(2)

    # Helpful failure context.
    final_state = _psql_scalar(
        f"SELECT COALESCE(state,'') FROM dag_run WHERE dag_id='{dag_id}' AND run_id='{run_id}' LIMIT 1;"
    )
    pytest.fail(f"Timed out waiting for DAG success. dag_run.state='{final_state}'")
