"""End-to-end-ish integration test: fetch Ethereum logs and persist an event graph.

This validates the full path:
- Web3 fetch (requires ALCHEMY_URL)
- Causal edge inference
- Neo4j write

The test is skipped if ALCHEMY_URL is not configured.
"""

import os

from neo4j import GraphDatabase

import pytest

from web3 import Web3

from dags.dag_helper_functions.events import fetch_events_in_block_range
from dags.dag_helper_functions.neo4j_graph import build_causes_edges, cleanup_run, new_run_id, write_events_with_edges

CONTRACT_ADDRESS = "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc"


def test_ingest_100_events_creates_some_relationships():
    """Ingesting real logs should create at least some CAUSES relationships."""
    alchemy_url = os.getenv("ALCHEMY_URL")
    assert alchemy_url, "Missing ALCHEMY_URL. Set it in docker/.env or your shell environment."

    run_id = new_run_id("weth_usdc")
    try:
        w3 = Web3(Web3.HTTPProvider(alchemy_url))
        assert w3.is_connected(), "Unable to reach the Alchemy endpoint"
        to_block = int(w3.eth.block_number)
        from_block = max(0, to_block - 200)

        events = fetch_events_in_block_range(
            CONTRACT_ADDRESS,
            provider_url=alchemy_url,
            from_block=from_block,
            to_block=to_block,
        )
        assert len(events) > 0

        edges = build_causes_edges(events)
        # This is the key check: not 100 disconnected nodes.
        assert len(edges) > 0

        write_events_with_edges(events, edges, run_id=run_id)

        uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
        user = os.getenv("NEO4J_USER", "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "test")

        driver = GraphDatabase.driver(uri, auth=(user, password))
        try:
            with driver.session() as session:
                rel_count = session.run(
                    """
                    MATCH (:Run {run_id: $run_id})-[:INCLUDES]->(a:Event)
                    MATCH (:Run {run_id: $run_id})-[:INCLUDES]->(b:Event)
                    MATCH (a)-[r:CAUSES]->(b)
                    RETURN count(r) AS rel_count
                    """,
                    run_id=run_id,
                ).single()["rel_count"]
                assert rel_count > 0

                connected_nodes = session.run(
                    """
                    MATCH (:Run {run_id: $run_id})-[:INCLUDES]->(e:Event)
                    WHERE (e)--()
                    RETURN count(e) AS connected_nodes
                    """,
                    run_id=run_id,
                ).single()["connected_nodes"]
                assert connected_nodes > 0
        finally:
            driver.close()
    finally:
        cleanup_run(run_id)
