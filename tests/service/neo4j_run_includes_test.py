"""Neo4j integration test: Run/INCLUDES model.

These tests validate that multiple ingestion runs can reference the same events
without overwriting or losing run scoping.
"""

import uuid

from neo4j import GraphDatabase

from dags.dag_helper_functions.neo4j_graph import cleanup_run, write_events_with_edges


def test_write_events_keeps_multiple_runs_without_overwrite():
    """Writing the same event under different runs should preserve both INCLUDES edges."""
    run_id_1 = f"run:{uuid.uuid4().hex}:1"
    run_id_2 = f"run:{uuid.uuid4().hex}:2"

    event_id = f"0xtest:{uuid.uuid4().hex}:0"
    events = [
        {
            "event_id": event_id,
            "tx_hash": "0xtest",
            "log_index": 0,
            "block_number": 1,
            "transaction_index": 0,
            "address": "0x0000000000000000000000000000000000000000",
            "event_name": "Test",
            "event": "0xdeadbeef",
        }
    ]

    uri = "neo4j://localhost:7687"
    user = "neo4j"
    password = "test"

    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        write_events_with_edges(events, edges=[], run_id=run_id_1)
        write_events_with_edges(events, edges=[], run_id=run_id_2)

        with driver.session() as session:
            c1 = session.run(
                """
                MATCH (:Run {run_id: $run_id})-[:INCLUDES]->(:Event {event_id: $event_id})
                RETURN count(*) AS c
                """,
                run_id=run_id_1,
                event_id=event_id,
            ).single()["c"]
            assert c1 == 1

            c2 = session.run(
                """
                MATCH (:Run {run_id: $run_id})-[:INCLUDES]->(:Event {event_id: $event_id})
                RETURN count(*) AS c
                """,
                run_id=run_id_2,
                event_id=event_id,
            ).single()["c"]
            assert c2 == 1
    finally:
        driver.close()
        cleanup_run(run_id_1)
        cleanup_run(run_id_2)
