"""Integration test: Postgres staging retention cleanup.

Runs fully in Docker containers:
- inserts old rows into event_graph_event/event_graph_edge
- calls cleanup helper inside airflow-webserver container
- asserts rows are deleted
"""

import subprocess


def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=False, capture_output=True, text=True)


def _pg_exec(compose: list[str], sql: str) -> subprocess.CompletedProcess:
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


def test_postgres_store_cleanup_removes_old_rows():
    compose = ["docker", "compose", "-f", "docker/docker-compose.yml"]

    up = _run(compose + ["up", "-d", "postgres", "airflow-webserver", "airflow-scheduler"])
    assert up.returncode == 0, up.stderr

    # Fresh tables.
    drop = _pg_exec(compose, "DROP TABLE IF EXISTS event_graph_edge; DROP TABLE IF EXISTS event_graph_event;")
    assert drop.returncode == 0, drop.stdout + "\n" + drop.stderr

    run_id = "retention:test"

    create = _pg_exec(
        compose,
        """
        CREATE TABLE IF NOT EXISTS event_graph_event (
            run_id TEXT NOT NULL,
            event_id TEXT NOT NULL,
            event JSONB NOT NULL,
            PRIMARY KEY (run_id, event_id)
        );
        CREATE TABLE IF NOT EXISTS event_graph_edge (
            run_id TEXT NOT NULL,
            parent_event_id TEXT NOT NULL,
            child_event_id TEXT NOT NULL,
            PRIMARY KEY (run_id, parent_event_id, child_event_id)
        );
        """,
    )
    assert create.returncode == 0, create.stdout + "\n" + create.stderr

    # Ensure schema hardening is applied (adds ingested_at columns).
    ensure = _run(
        compose
        + [
            "exec",
            "-T",
            "airflow-webserver",
            "env",
            "PYTHONPATH=/opt/airflow/dags",
            "python",
            "-c",
            "from dag_helper_functions.postgres_store import ensure_event_graph_tables; ensure_event_graph_tables()",
        ]
    )
    assert ensure.returncode == 0, ensure.stdout + "\n" + ensure.stderr

    # These inserts require `ingested_at` columns to exist (schema hardening).
    insert = _pg_exec(
        compose,
        """
        INSERT INTO event_graph_event(run_id, event_id, event, ingested_at)
        VALUES ('retention:test', 'e1', '{}'::jsonb, now() - interval '2 hours');

        INSERT INTO event_graph_edge(run_id, parent_event_id, child_event_id, ingested_at)
        VALUES ('retention:test', 'e1', 'e2', now() - interval '2 hours');
        """,
    )
    assert insert.returncode == 0, insert.stdout + "\n" + insert.stderr

    # Run cleanup inside Airflow container (uses AIRFLOW__DATABASE__SQL_ALCHEMY_CONN).
    cleanup = _run(
        compose
        + [
            "exec",
            "-T",
            "airflow-webserver",
            "env",
            "PYTHONPATH=/opt/airflow/dags",
            "python",
            "-c",
            "from dag_helper_functions.postgres_store import cleanup_event_graph_tables; cleanup_event_graph_tables(retention_hours=1)",
        ]
    )
    assert cleanup.returncode == 0, cleanup.stdout + "\n" + cleanup.stderr

    remaining_events = _pg_exec(compose, "SELECT count(*) FROM event_graph_event WHERE run_id = '%s';" % run_id)
    assert remaining_events.returncode == 0
    remaining_event_count = int(remaining_events.stdout.strip().splitlines()[-2].strip())
    assert remaining_event_count == 0

    remaining_edges = _pg_exec(compose, "SELECT count(*) FROM event_graph_edge WHERE run_id = '%s';" % run_id)
    assert remaining_edges.returncode == 0
    remaining_edge_count = int(remaining_edges.stdout.strip().splitlines()[-2].strip())
    assert remaining_edge_count == 0


def test_airflow_dag_runs_postgres_cleanup_retention():
    """E2E: DAG can run a retention cleanup step after writing the graph."""
    compose = ["docker", "compose", "-f", "docker/docker-compose.yml"]

    up = _run(compose + ["up", "-d", "postgres", "neo4j", "airflow-webserver", "airflow-scheduler"])
    assert up.returncode == 0, up.stderr

    # Fresh staging tables.
    drop = _pg_exec(compose, "DROP TABLE IF EXISTS event_graph_edge; DROP TABLE IF EXISTS event_graph_event;")
    assert drop.returncode == 0, drop.stdout + "\n" + drop.stderr

    # Create/upgrade tables (adds ingested_at).
    ensure = _run(
        compose
        + [
            "exec",
            "-T",
            "airflow-webserver",
            "env",
            "PYTHONPATH=/opt/airflow/dags",
            "python",
            "-c",
            "from dag_helper_functions.postgres_store import ensure_event_graph_tables; ensure_event_graph_tables()",
        ]
    )
    assert ensure.returncode == 0, ensure.stdout + "\n" + ensure.stderr

    old_run_id = "retention:old"

    insert = _pg_exec(
        compose,
        """
        INSERT INTO event_graph_event(run_id, event_id, event, ingested_at)
        VALUES ('retention:old', 'e1', '{}'::jsonb, now() - interval '2 hours');

        INSERT INTO event_graph_edge(run_id, parent_event_id, child_event_id, ingested_at)
        VALUES ('retention:old', 'e1', 'e2', now() - interval '2 hours');
        """,
    )
    assert insert.returncode == 0, insert.stdout + "\n" + insert.stderr

    before_events = _pg_exec(compose, "SELECT count(*) FROM event_graph_event WHERE run_id = '%s';" % old_run_id)
    assert before_events.returncode == 0
    assert int(before_events.stdout.strip().splitlines()[-2].strip()) == 1

    before_edges = _pg_exec(compose, "SELECT count(*) FROM event_graph_edge WHERE run_id = '%s';" % old_run_id)
    assert before_edges.returncode == 0
    assert int(before_edges.stdout.strip().splitlines()[-2].strip()) == 1

    # Run the DAG with a config that triggers cleanup.
    # Keep it fast: use an inactive address so eth_getLogs returns quickly.
    conf = '{"latest_blocks": 10, "retention_hours": 1}'
    res = _run(
        compose
        + [
            "exec",
            "-T",
            "airflow-webserver",
            "env",
            "PYTHONWARNINGS=ignore",
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

    after_events = _pg_exec(compose, "SELECT count(*) FROM event_graph_event WHERE run_id = '%s';" % old_run_id)
    assert after_events.returncode == 0
    assert int(after_events.stdout.strip().splitlines()[-2].strip()) == 0

    after_edges = _pg_exec(compose, "SELECT count(*) FROM event_graph_edge WHERE run_id = '%s';" % old_run_id)
    assert after_edges.returncode == 0
    assert int(after_edges.stdout.strip().splitlines()[-2].strip()) == 0
