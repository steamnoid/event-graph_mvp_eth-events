"""Airflow integration test: execute the deterministic ingest DAG.

This uses `airflow dags test` (scheduler-independent) with `latest_blocks`.
It requires ALCHEMY_URL to be present in the Airflow container environment.
"""

import os
import re
import subprocess

import pytest
from neo4j import GraphDatabase


def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    """Run a subprocess command and capture stdout/stderr for assertions."""
    return subprocess.run(cmd, check=False, capture_output=True, text=True)


def _extract_latest_block_range(output: str) -> tuple[int, int, int] | None:
    """Extract (from_block, to_block, latest_blocks) from the DAG logs."""
    m = re.search(
        r"LATEST_BLOCK_RANGE\s+from_block=(\d+)\s+to_block=(\d+)\s+latest_blocks=(\d+)",
        output,
    )
    if not m:
        return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


def _pg_exec(compose: list[str], sql: str) -> subprocess.CompletedProcess:
    """Execute SQL against the Airflow Postgres DB inside the postgres container."""
    return _run(
        compose
        + [
            "exec",
            "-T",
            "postgres",
            "psql",
            "-U",
            "airflow",
            "-d",
            "airflow",
            "-c",
            sql,
        ]
    )


def _extract_postgres_store_marker(output: str) -> tuple[str, int] | None:
    """Extract (run_id, events_stored) from the DAG logs."""
    m = re.search(r"POSTGRES_EVENTS_STORED\s+run_id=([^\s]+)\s+events_stored=(\d+)", output)
    if not m:
        return None
    return m.group(1), int(m.group(2))


def _extract_neo4j_store_marker(output: str) -> tuple[str, int] | None:
    """Extract (run_id, events_written) from the DAG logs."""
    m = re.search(r"NEO4J_EVENTS_WRITTEN\s+run_id=([^\s]+)\s+events_written=(\d+)", output)
    if not m:
        return None
    return m.group(1), int(m.group(2))


def _extract_neo4j_causes_coverage_marker(output: str) -> tuple[str, int, int, int] | None:
    """Extract (run_id, total_events, events_with_causes, causes_edges) from DAG logs."""
    m = re.search(
        r"NEO4J_CAUSES_COVERAGE\s+run_id=([^\s]+)\s+total_events=(\d+)\s+events_with_causes=(\d+)\s+causes_edges=(\d+)",
        output,
    )
    if not m:
        return None
    return m.group(1), int(m.group(2)), int(m.group(3)), int(m.group(4))


def _extract_postgres_edges_store_marker(output: str) -> tuple[str, int] | None:
    """Extract (run_id, edges_stored) from the DAG logs."""
    m = re.search(r"POSTGRES_EDGES_STORED\s+run_id=([^\s]+)\s+edges_stored=(\d+)", output)
    if not m:
        return None
    return m.group(1), int(m.group(2))


def _neo4j_config() -> tuple[str, str, str]:
    uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "test")
    return uri, user, password


@pytest.mark.parametrize("latest_blocks", [10, 100, 500])
def test_airflow_dag_honors_latest_blocks_parameter(latest_blocks: int):
    """The ingest DAG honors `latest_blocks` and computes a matching inclusive range."""
    compose = ["docker", "compose", "-f", "docker/docker-compose.yml"]

    up = _run(compose + ["up", "-d", "postgres", "neo4j", "airflow-webserver", "airflow-scheduler"])
    assert up.returncode == 0, up.stderr

    conf = '{"latest_blocks": %d}' % latest_blocks

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
            # Keep this integration test fast and stable: the block-range logic is what
            # we're testing here, not event volume. A mostly-inactive address prevents
            # large eth_getLogs responses for 500 blocks.
            "UNISWAP_PAIR_ADDRESS=0x0000000000000000000000000000000000000000",
            "airflow",
            "dags",
            "test",
            "uniswap_weth_usdc_ingest_events_to_neo4j",
            "2026-01-01",
            "--conf",
            conf,
        ]
    )

    output = (res.stdout or "") + "\n" + (res.stderr or "")
    assert res.returncode == 0, output

    extracted = _extract_latest_block_range(output)
    assert extracted is not None, "Expected LATEST_BLOCK_RANGE marker in Airflow logs"
    from_block, to_block, logged_latest_blocks = extracted
    assert logged_latest_blocks == latest_blocks
    assert (to_block - from_block + 1) == latest_blocks


@pytest.mark.parametrize("logs_number", [10, 100, 500])
def test_airflow_dag_honors_logs_number_parameter(logs_number: int):
    """The ingest DAG honors `logs_number` by storing exactly N events in Postgres."""
    compose = ["docker", "compose", "-f", "docker/docker-compose.yml"]

    up = _run(compose + ["up", "-d", "postgres", "neo4j", "airflow-webserver", "airflow-scheduler"])
    assert up.returncode == 0, up.stderr

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

    # Isolation: prove the DAG creates and populates these tables.
    drop = _pg_exec(compose, "DROP TABLE IF EXISTS event_graph_edge; DROP TABLE IF EXISTS event_graph_event;")
    assert drop.returncode == 0, drop.stdout + "\n" + drop.stderr

    conf = '{"logs_number": %d}' % logs_number
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
            "uniswap_weth_usdc_ingest_events_to_neo4j",
            "2026-01-01",
            "--conf",
            conf,
        ]
    )

    output = (res.stdout or "") + "\n" + (res.stderr or "")
    assert res.returncode == 0, output

    marker = _extract_postgres_store_marker(output)
    assert marker is not None, "Expected POSTGRES_EVENTS_STORED marker in Airflow logs"
    run_id, events_stored = marker
    assert events_stored == logs_number

    # Assert persistence for this run_id (and table existence).
    count = _pg_exec(compose, "SELECT count(*) FROM event_graph_event WHERE run_id = '%s';" % run_id)
    assert count.returncode == 0, count.stdout + "\n" + count.stderr
    stored_in_db = int(count.stdout.strip().splitlines()[-2].strip())
    assert stored_in_db == logs_number


def test_airflow_e2e_event_count_matches_postgres_and_neo4j():
    """E2E: the same events count is reflected in Airflow logs, Postgres, and Neo4j."""
    compose = ["docker", "compose", "-f", "docker/docker-compose.yml"]

    up = _run(compose + ["up", "-d", "postgres", "neo4j", "airflow-webserver", "airflow-scheduler"])
    assert up.returncode == 0, up.stderr

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

    # Isolation: ensure a clean staging area in Postgres.
    drop = _pg_exec(compose, "DROP TABLE IF EXISTS event_graph_edge; DROP TABLE IF EXISTS event_graph_event;")
    assert drop.returncode == 0, drop.stdout + "\n" + drop.stderr

    logs_number = 50
    conf = '{"logs_number": %d}' % logs_number

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
            "uniswap_weth_usdc_ingest_events_to_neo4j",
            "2026-01-01",
            "--conf",
            conf,
        ]
    )

    output = (res.stdout or "") + "\n" + (res.stderr or "")
    assert res.returncode == 0, output

    pg_marker = _extract_postgres_store_marker(output)
    assert pg_marker is not None, "Expected POSTGRES_EVENTS_STORED marker in Airflow logs"
    run_id, events_stored = pg_marker
    assert events_stored == logs_number

    neo_marker = _extract_neo4j_store_marker(output)
    assert neo_marker is not None, "Expected NEO4J_EVENTS_WRITTEN marker in Airflow logs"
    neo_run_id, events_written = neo_marker
    assert neo_run_id == run_id
    assert events_written == events_stored

    # Postgres: count rows for this run_id.
    count = _pg_exec(compose, "SELECT count(*) FROM event_graph_event WHERE run_id = '%s';" % run_id)
    assert count.returncode == 0, count.stdout + "\n" + count.stderr
    stored_in_db = int(count.stdout.strip().splitlines()[-2].strip())
    assert stored_in_db == events_stored

    # Neo4j: count included events for this run_id.
    uri, user, password = _neo4j_config()
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session() as session:
            record = session.run(
                """
                MATCH (r:Run {run_id: $run_id})-[:INCLUDES]->(:Event)
                RETURN count(*) AS c
                """,
                run_id=run_id,
            ).single()
            assert record is not None
            assert record["c"] == events_stored

            # Keep the DB tidy across repeated pytest runs.
            session.run(
                """
                MATCH (r:Run {run_id: $run_id})
                DETACH DELETE r
                """,
                run_id=run_id,
            )
    finally:
        driver.close()


def test_airflow_e2e_edges_count_matches_postgres_and_neo4j_causes():
    """E2E: edges are not lost between Postgres staging and Neo4j CAUSES writes."""
    compose = ["docker", "compose", "-f", "docker/docker-compose.yml"]

    up = _run(compose + ["up", "-d", "postgres", "neo4j", "airflow-webserver", "airflow-scheduler"])
    assert up.returncode == 0, up.stderr

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

    # Isolation: ensure clean staging tables for this run.
    drop = _pg_exec(compose, "DROP TABLE IF EXISTS event_graph_edge; DROP TABLE IF EXISTS event_graph_event;")
    assert drop.returncode == 0, drop.stdout + "\n" + drop.stderr

    logs_number = 50
    conf = '{"logs_number": %d}' % logs_number

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
            "uniswap_weth_usdc_ingest_events_to_neo4j",
            "2026-01-01",
            "--conf",
            conf,
        ]
    )

    output = (res.stdout or "") + "\n" + (res.stderr or "")
    assert res.returncode == 0, output

    pg_events = _extract_postgres_store_marker(output)
    assert pg_events is not None, "Expected POSTGRES_EVENTS_STORED marker in Airflow logs"
    run_id, events_stored = pg_events
    assert events_stored == logs_number

    pg_edges = _extract_postgres_edges_store_marker(output)
    assert pg_edges is not None, "Expected POSTGRES_EDGES_STORED marker in Airflow logs"
    edges_run_id, edges_stored = pg_edges
    assert edges_run_id == run_id

    # Postgres: count edges for this run_id.
    edge_count = _pg_exec(compose, "SELECT count(*) FROM event_graph_edge WHERE run_id = '%s';" % run_id)
    assert edge_count.returncode == 0, edge_count.stdout + "\n" + edge_count.stderr
    stored_edges_in_db = int(edge_count.stdout.strip().splitlines()[-2].strip())
    assert stored_edges_in_db == edges_stored

    # Neo4j: count CAUSES edges between events included in this run.
    uri, user, password = _neo4j_config()
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session() as session:
            record = session.run(
                """
                MATCH (r:Run {run_id: $run_id})-[:INCLUDES]->(a:Event)
                MATCH (r)-[:INCLUDES]->(b:Event)
                MATCH (a)-[rel:CAUSES]->(b)
                RETURN count(rel) AS c
                """,
                run_id=run_id,
            ).single()
            assert record is not None
            assert record["c"] == edges_stored

            coverage = _extract_neo4j_causes_coverage_marker(output)
            assert coverage is not None, "Expected NEO4J_CAUSES_COVERAGE marker in Airflow logs"
            coverage_run_id, total_events, events_with_causes, causes_edges = coverage
            assert coverage_run_id == run_id
            assert total_events == events_stored
            assert causes_edges == edges_stored
            assert 0 <= events_with_causes <= total_events

            session.run(
                """
                MATCH (r:Run {run_id: $run_id})
                DETACH DELETE r
                """,
                run_id=run_id,
            )
    finally:
        driver.close()
