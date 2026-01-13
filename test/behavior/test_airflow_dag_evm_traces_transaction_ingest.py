import subprocess
import time
from pathlib import Path
import json

import pytest


@pytest.mark.e2e
def test_airflow_dag_evm_traces_transaction_ingest_run_all_tasks_success():
    """Trigger the DAG and assert its single task ends in SUCCESS.

    This DAG is intentionally NOT fixture-driven: it fetches traces from mainnet via ALCHEMY_URL.
    """

    repo_root = Path(__file__).resolve().parents[2]
    docker_dir = repo_root / "docker"

    def _airflow_env(name: str) -> str:
        p = subprocess.run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "airflow-webserver",
                "printenv",
                name,
            ],
            cwd=str(docker_dir),
            capture_output=True,
            text=True,
            check=True,
        )
        return (p.stdout or "").strip()

    # This DAG fetches from mainnet via ALCHEMY_URL; verify it's configured in the
    # running Airflow container (source of truth when using docker compose --env-file).
    if not _airflow_env("ALCHEMY_URL"):
        pytest.skip("ALCHEMY_URL not set in airflow-webserver container")

    dag_id = "evm_traces_transaction_ingest_to_neo4j"
    run_id = f"e2e__{int(time.time())}"

    conf = json.dumps({"latest_blocks": 1, "max_tx": 1, "min_erc20_tx": 1, "max_blocks_to_scan": 1})

    # Trigger the DAG. Override latest blocks to 1 for a bounded e2e run.
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

    expected_tasks = {
        "plan_block_range_to_file",
        "fetch_tx_hashes_to_file",
        "fetch_traces_to_graph_file",
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

    deadline = time.time() + 600
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

    final_state = _psql_scalar(
        f"SELECT COALESCE(state,'') FROM dag_run WHERE dag_id='{dag_id}' AND run_id='{run_id}' LIMIT 1;"
    )
    pytest.fail(f"Timed out waiting for DAG success. dag_run.state='{final_state}'")
