"""Neo4j smoke test.

Verifies that the Neo4j service is reachable and can create a minimal
(:Event)-[:CAUSES]->(:Event) pattern.
"""

import os
import uuid

from neo4j import GraphDatabase


def _neo4j_config():
    """Read Neo4j connection settings from environment variables."""
    uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "test")
    return uri, user, password


def test_neo4j_can_create_event_nodes_and_causes_edge():
    """Neo4j can create nodes and a CAUSES relationship."""
    uri, user, password = _neo4j_config()

    run_id = uuid.uuid4().hex
    a_id = f"test:{run_id}:a"
    b_id = f"test:{run_id}:b"

    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session() as session:
            session.run(
                """
                CREATE (a:Event {event_id: $a_id})
                CREATE (b:Event {event_id: $b_id})
                CREATE (a)-[:CAUSES]->(b)
                """,
                a_id=a_id,
                b_id=b_id,
            )

            record = session.run(
                """
                MATCH (a:Event {event_id: $a_id})-[:CAUSES]->(b:Event {event_id: $b_id})
                RETURN count(*) AS rel_count
                """,
                a_id=a_id,
                b_id=b_id,
            ).single()

            assert record is not None
            assert record["rel_count"] == 1

            session.run(
                """
                MATCH (e:Event)
                WHERE e.event_id IN [$a_id, $b_id]
                DETACH DELETE e
                """,
                a_id=a_id,
                b_id=b_id,
            )
    finally:
        driver.close()
