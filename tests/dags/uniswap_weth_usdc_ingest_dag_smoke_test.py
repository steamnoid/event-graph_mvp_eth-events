"""Smoke test: DAG file exists and uses production-direction helpers."""

from pathlib import Path


def test_uniswap_ingest_dag_file_exists_and_uses_helpers():
    """DAG definition should reference deterministic fetch + graph write helpers."""
    dag_file = Path("docker/dags/uniswap_weth_usdc_ingest_dag.py")
    assert dag_file.exists()

    content = dag_file.read_text(encoding="utf-8")

    assert "fetch_events_in_block_range" in content
    assert "build_causes_edges" in content
    assert "write_events_with_edges" in content

    # XCom payloads are size-limited; the DAG must use Postgres as a handoff
    # between tasks rather than passing full event/edge lists via XCom.
    assert "store_events_in_postgres" in content

    # MVP direction: backfill by `latest_blocks` from the Airflow run.
    assert "latest_blocks" in content
    assert "dag_run" in content or "get_current_context" in content
