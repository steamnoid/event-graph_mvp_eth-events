"""Postgres-backed handoff between Airflow tasks.

Airflow XCom payloads are intentionally small and have practical size limits.
For larger intermediate datasets (lists of events/edges), we store them in the
Airflow Postgres database and pass only a small `run_id` via XCom.

Tables created (idempotently):
- event_graph_event: one normalized event per (run_id, event_id)
- event_graph_edge: one causal edge per (run_id, parent_event_id, child_event_id)

All functions are import-safe: they perform no network or DB I/O at import time.
"""

import os
from typing import Dict, Iterable, List, Tuple
from urllib.parse import urlparse


def _airflow_sqlalchemy_conn() -> str:
    """Return the SQLAlchemy connection string for Airflow's metadata DB."""
    return (
        os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
        or os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN")
        or ""
    )


def _psycopg2_connect_kwargs(sqlalchemy_conn: str) -> Dict:
    """Convert an Airflow SQLAlchemy Postgres conn string into psycopg2 kwargs."""
    if not sqlalchemy_conn:
        raise ValueError("Missing Airflow SQLAlchemy connection string")

    # Airflow often uses postgresql+psycopg2://...; urlparse does not care about the '+'.
    parsed = urlparse(sqlalchemy_conn)
    dbname = (parsed.path or "").lstrip("/")
    if not dbname:
        raise ValueError("Invalid SQLAlchemy conn string (missing db name)")

    return {
        "dbname": dbname,
        "user": parsed.username,
        "password": parsed.password,
        "host": parsed.hostname,
        "port": parsed.port or 5432,
    }


def _connect():
    """Create a psycopg2 connection to the Airflow Postgres DB (lazy import)."""
    sqlalchemy_conn = _airflow_sqlalchemy_conn()
    kwargs = _psycopg2_connect_kwargs(sqlalchemy_conn)

    import psycopg2  # imported lazily to keep host imports lightweight

    return psycopg2.connect(**kwargs)


def ensure_event_graph_tables() -> None:
    """Create event-graph tables if they don't exist."""
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS event_graph_event (
                    run_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    event JSONB NOT NULL,
                    PRIMARY KEY (run_id, event_id)
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS event_graph_edge (
                    run_id TEXT NOT NULL,
                    parent_event_id TEXT NOT NULL,
                    child_event_id TEXT NOT NULL,
                    PRIMARY KEY (run_id, parent_event_id, child_event_id)
                );
                """
            )

            # Production hardening: add ingestion timestamps for retention/cleanup.
            cur.execute(
                """
                ALTER TABLE event_graph_event
                ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ NOT NULL DEFAULT now();
                """
            )
            cur.execute(
                """
                ALTER TABLE event_graph_edge
                ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ NOT NULL DEFAULT now();
                """
            )


def store_events_in_postgres(run_id: str, events: List[Dict]) -> int:
    """Upsert normalized events for a given run_id. Returns number of rows written."""
    if not run_id:
        raise ValueError("run_id is required")

    ensure_event_graph_tables()

    # psycopg2.extras.Json is the simplest safe JSONB adapter.
    from psycopg2.extras import Json  # type: ignore

    rows = 0
    with _connect() as conn:
        with conn.cursor() as cur:
            for e in events:
                event_id = e.get("event_id")
                if not event_id:
                    continue
                cur.execute(
                    """
                    INSERT INTO event_graph_event (run_id, event_id, event)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (run_id, event_id)
                    DO UPDATE SET event = EXCLUDED.event,
                                 ingested_at = now()
                    """,
                    (run_id, event_id, Json(e)),
                )
                rows += 1
    return rows


def load_events_from_postgres(run_id: str) -> List[Dict]:
    """Load normalized events for a given run_id."""
    if not run_id:
        raise ValueError("run_id is required")

    ensure_event_graph_tables()

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT event
                FROM event_graph_event
                WHERE run_id = %s
                """,
                (run_id,),
            )
            return [row[0] for row in cur.fetchall()]


def store_edges_in_postgres(run_id: str, edges: Iterable[Tuple[str, str]]) -> int:
    """Upsert edges for a given run_id. Returns number of edges written."""
    if not run_id:
        raise ValueError("run_id is required")

    ensure_event_graph_tables()

    rows = 0
    with _connect() as conn:
        with conn.cursor() as cur:
            for parent_id, child_id in edges:
                if not parent_id or not child_id:
                    continue
                cur.execute(
                    """
                    INSERT INTO event_graph_edge (run_id, parent_event_id, child_event_id)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (run_id, parent_event_id, child_event_id)
                    DO UPDATE SET ingested_at = now()
                    """,
                    (run_id, parent_id, child_id),
                )
                rows += 1
    return rows


def cleanup_event_graph_tables(retention_hours: int) -> None:
    """Delete old staging rows based on `ingested_at`.

    This is intended to keep the Airflow metadata DB from growing unbounded.
    """
    if not isinstance(retention_hours, int):
        raise TypeError("retention_hours must be an integer")
    if retention_hours <= 0:
        raise ValueError("retention_hours must be a positive integer")

    ensure_event_graph_tables()

    with _connect() as conn:
        with conn.cursor() as cur:
            # Delete edges first (potentially referencing events).
            cur.execute(
                """
                DELETE FROM event_graph_edge
                WHERE ingested_at < (now() - (%s * interval '1 hour'));
                """,
                (retention_hours,),
            )
            cur.execute(
                """
                DELETE FROM event_graph_event
                WHERE ingested_at < (now() - (%s * interval '1 hour'));
                """,
                (retention_hours,),
            )


def load_edges_from_postgres(run_id: str) -> List[Tuple[str, str]]:
    """Load edges for a given run_id."""
    if not run_id:
        raise ValueError("run_id is required")

    ensure_event_graph_tables()

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT parent_event_id, child_event_id
                FROM event_graph_edge
                WHERE run_id = %s
                """,
                (run_id,),
            )
            return [(row[0], row[1]) for row in cur.fetchall()]
