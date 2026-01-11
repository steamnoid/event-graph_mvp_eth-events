"""Neo4j persistence and edge inference for the event graph MVP.

This module contains:
- a minimal causal edge builder (`build_causes_edges`) operating on normalized events
- Neo4j write helpers that are safe to re-run for the same `run_id`

Graph model:
- (:Run {run_id})-[:INCLUDES]->(:Event {event_id})
- (:Event)-[:CAUSES]->(:Event)
"""

import os
import uuid
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from neo4j import GraphDatabase


def _neo4j_config() -> Tuple[str, str, str]:
    """Read Neo4j connection settings from environment variables."""
    uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "test")
    return uri, user, password


def build_causes_edges(events: Sequence[Dict]) -> List[Tuple[str, str]]:
    """Build a minimal CAUSES edge set respecting local truth:

    - Parents come from same tx
    - Parent log_index < child log_index
    - DAG (monotonic by log_index per tx)

    This is intentionally minimal: for Swap/Mint we connect from all earlier Transfer
    events in the same tx ("Transfer IN" simplified). For Burn we connect from the
    last Mint/Burn in the same tx. Transfer events get no parents.
    """

    by_tx: Dict[str, List[Dict]] = {}
    for e in events:
        tx = e.get("tx_hash")
        if not tx:
            continue
        by_tx.setdefault(tx, []).append(e)

    edges: List[Tuple[str, str]] = []

    for tx_hash, tx_events in by_tx.items():
        tx_events_sorted = sorted(tx_events, key=lambda x: x.get("log_index", -1))
        transfers_so_far: List[str] = []
        last_mint_or_burn = None  # type: Optional[str]
        last_state_change = None  # type: Optional[str]
        last_event_id = None  # type: Optional[str]

        for e in tx_events_sorted:
            child_id = e.get("event_id")
            if not child_id:
                continue

            name = e.get("event_name")

            if name == "Transfer":
                transfers_so_far.append(child_id)
                last_event_id = child_id
                continue

            if name in {"Swap", "Mint"}:
                for parent_id in transfers_so_far:
                    edges.append((parent_id, child_id))

            if name == "Sync" and last_state_change is not None:
                edges.append((last_state_change, child_id))

            if name == "Burn" and last_mint_or_burn is not None:
                edges.append((last_mint_or_burn, child_id))

            if name in {"Swap", "Mint", "Burn"}:
                last_state_change = child_id

            if name in {"Mint", "Burn"}:
                last_mint_or_burn = child_id

            if name not in {"Transfer", "Swap", "Mint", "Burn", "Sync"} and last_event_id is not None:
                edges.append((last_event_id, child_id))

            last_event_id = child_id

    # De-dupe while preserving order
    seen = set()  # type: Set[Tuple[str, str]]
    unique: List[Tuple[str, str]] = []
    for a, b in edges:
        if (a, b) in seen:
            continue
        seen.add((a, b))
        unique.append((a, b))

    return unique


def write_events_with_edges(events: Sequence[Dict], edges: Iterable[Tuple[str, str]], run_id: str) -> None:
    """Upsert events and edges into Neo4j under a specific `run_id`.

    Idempotency direction:
    - Events are MERGEd by `event_id`
    - Run is MERGEd by `run_id`
    - INCLUDES and CAUSES relationships are MERGEd
    """
    uri, user, password = _neo4j_config()

    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session() as session:
            session.run(
                """
                MERGE (r:Run {run_id: $run_id})
                """,
                run_id=run_id,
            )

            for e in events:
                session.run(
                    """
                    MERGE (n:Event {event_id: $event_id})
                    SET n.tx_hash = $tx_hash,
                        n.log_index = $log_index,
                        n.block_number = $block_number,
                        n.transaction_index = $transaction_index,
                        n.address = $address,
                        n.event_name = $event_name,
                        n.event_sig = $event_sig
                    """,
                    event_id=e.get("event_id"),
                    tx_hash=e.get("tx_hash"),
                    log_index=e.get("log_index"),
                    block_number=e.get("block_number"),
                    transaction_index=e.get("transaction_index"),
                    address=e.get("address"),
                    event_name=e.get("event_name"),
                    event_sig=e.get("event"),
                )

                session.run(
                    """
                    MATCH (r:Run {run_id: $run_id})
                    MATCH (n:Event {event_id: $event_id})
                    MERGE (r)-[:INCLUDES]->(n)
                    """,
                    run_id=run_id,
                    event_id=e.get("event_id"),
                )

            for parent_id, child_id in edges:
                session.run(
                    """
                    MATCH (a:Event {event_id: $parent_id})
                    MATCH (b:Event {event_id: $child_id})
                    MERGE (a)-[:CAUSES]->(b)
                    """,
                    parent_id=parent_id,
                    child_id=child_id,
                )
    finally:
        driver.close()


def get_causes_coverage_for_run(run_id: str) -> Tuple[int, int, int]:
    """Return (total_events, events_with_causes, causes_edges) for a run.

    Counts are restricted to events included in the given run_id.
    """
    if not run_id:
        raise ValueError("run_id is required")

    uri, user, password = _neo4j_config()
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session() as session:
            totals = session.run(
                """
                MATCH (r:Run {run_id: $run_id})-[:INCLUDES]->(e:Event)
                WITH collect(e) AS es
                UNWIND es AS e
                OPTIONAL MATCH (e)-[out:CAUSES]->(o:Event)
                WHERE o IN es
                WITH es, e, count(o) AS out_c
                OPTIONAL MATCH (i:Event)-[:CAUSES]->(e)
                WHERE i IN es
                WITH es, e, out_c, count(i) AS in_c
                WITH es, sum(CASE WHEN (out_c + in_c) > 0 THEN 1 ELSE 0 END) AS events_with_causes
                RETURN size(es) AS total_events,
                       events_with_causes AS events_with_causes
                """,
                run_id=run_id,
            ).single()
            if totals is None:
                return 0, 0, 0

            rels = session.run(
                """
                MATCH (r:Run {run_id: $run_id})-[:INCLUDES]->(a:Event)
                MATCH (r)-[:INCLUDES]->(b:Event)
                MATCH (a)-[rel:CAUSES]->(b)
                RETURN count(rel) AS causes_edges
                """,
                run_id=run_id,
            ).single()
            causes_edges = int(rels["causes_edges"]) if rels is not None else 0

            return int(totals["total_events"]), int(totals["events_with_causes"]), causes_edges
    finally:
        driver.close()


def new_run_id(prefix: str = "alchemy") -> str:
    """Return a globally unique run id with a stable prefix."""
    return f"{prefix}:{uuid.uuid4().hex}"
