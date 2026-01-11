"""Airflow DAG: deterministic backfill from Ethereum logs into Neo4j.

This DAG is import-safe: it performs no network calls or DB writes at import time.

Runtime configuration:
- Requires `ALCHEMY_URL` in the task environment.
- Accepts `latest_blocks` via `dag_run.conf` (default: 1000).
"""

import os
from datetime import datetime
from typing import Dict, List

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from web3 import HTTPProvider, Web3

from dag_helper_functions.events import fetch_events_in_block_range
from dag_helper_functions.neo4j_graph import build_causes_edges, new_run_id, write_events_with_edges
from dag_helper_functions.postgres_store import (
    load_edges_from_postgres,
    load_events_from_postgres,
    store_edges_in_postgres,
    store_events_in_postgres,
)


DEFAULT_CONTRACT_ADDRESS = "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc"  # Uniswap V2 WETH/USDC pair


@dag(
    dag_id="uniswap_weth_usdc_ingest_100_events_to_neo4j",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["event-graph", "uniswap"],
)
def uniswap_weth_usdc_ingest_100_events_to_neo4j():
    """Define the DAG topology using TaskFlow tasks."""

    @task
    def init_run() -> str:
        """Create a unique `run_id` for this ingestion run."""
        return new_run_id("weth_usdc")

    @task
    def get_block_range() -> List[int]:
        """Compute a block range from `latest_blocks` in `dag_run.conf`.

        Defaults to 1000 latest blocks.
        """
        ctx = get_current_context()
        dag_run = ctx.get("dag_run")
        conf = (dag_run.conf or {}) if dag_run is not None else {}

        latest_blocks = conf.get("latest_blocks", 1000)
        latest_blocks_int = int(latest_blocks)
        if latest_blocks_int <= 0:
            raise ValueError("latest_blocks must be a positive integer")

        provider_url = os.environ["ALCHEMY_URL"]
        w3 = Web3(HTTPProvider(provider_url, request_kwargs={"timeout": 30}))
        if not w3.is_connected():
            raise ConnectionError("Unable to reach {}".format(provider_url))

        to_block = int(w3.eth.block_number)
        from_block = max(0, to_block - latest_blocks_int + 1)
        return [from_block, to_block]

    @task
    def fetch_events(graph_run_id: str, block_range: List[int]) -> Dict:
        """Fetch logs and store them in Postgres. Returns small metadata."""
        provider_url = os.environ["ALCHEMY_URL"]
        contract_address = os.getenv("UNISWAP_PAIR_ADDRESS", DEFAULT_CONTRACT_ADDRESS)
        from_block, to_block = block_range

        events = fetch_events_in_block_range(
            contract_address,
            provider_url=provider_url,
            from_block=from_block,
            to_block=to_block,
        )
        stored = store_events_in_postgres(graph_run_id, events)
        return {"run_id": graph_run_id, "events": stored}

    @task
    def build_edges(run_meta: Dict) -> Dict:
        """Infer causal edges and store them in Postgres. Returns small metadata."""
        run_id = run_meta["run_id"]
        events = load_events_from_postgres(run_id)
        edges = build_causes_edges(events)
        stored = store_edges_in_postgres(run_id, edges)
        return {"run_id": run_id, "edges": stored}

    @task
    def write_graph(run_meta: Dict, edge_meta: Dict) -> str:
        """Persist the run's events and edges into Neo4j and return `run_id`."""
        run_id = run_meta["run_id"]
        if edge_meta.get("run_id") != run_id:
            raise ValueError("run_id mismatch between tasks")

        events = load_events_from_postgres(run_id)
        edges = load_edges_from_postgres(run_id)
        write_events_with_edges(events, edges, run_id=run_id)
        return run_id

    graph_run_id = init_run()
    block_range = get_block_range()
    run_meta = fetch_events(graph_run_id, block_range)
    edge_meta = build_edges(run_meta)
    write_graph(run_meta, edge_meta)


uniswap_weth_usdc_ingest_100_events_to_neo4j()
