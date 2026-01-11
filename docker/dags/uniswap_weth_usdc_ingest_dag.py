"""Airflow DAG: deterministic backfill from Ethereum logs into Neo4j.

This DAG is import-safe: it performs no network calls or DB writes at import time.

Runtime configuration:
- Requires `ALCHEMY_URL` in the task environment.
- Accepts `latest_blocks` via `dag_run.conf` (default: 1000).
- Accepts `logs_number` via `dag_run.conf` (when provided, it takes precedence).
"""

import os
from datetime import datetime
from typing import Dict, List

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from web3 import HTTPProvider, Web3

from dag_helper_functions.events import (
    compute_latest_block_range,
    fetch_events_in_block_range,
    fetch_latest_events_by_log_count,
)
from dag_helper_functions.ingest_params import parse_logs_number_ingest_params
from dag_helper_functions.neo4j_graph import (
    build_causes_edges,
    get_causes_coverage_for_run,
    new_run_id,
    write_events_with_edges,
)
from dag_helper_functions.postgres_store import (
    cleanup_event_graph_tables,
    load_edges_from_postgres,
    load_events_from_postgres,
    store_edges_in_postgres,
    store_events_in_postgres,
)


DEFAULT_CONTRACT_ADDRESS = "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc"  # Uniswap V2 WETH/USDC pair


@dag(
    dag_id="uniswap_weth_usdc_ingest_events_to_neo4j",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["event-graph", "uniswap"],
)
def uniswap_weth_usdc_ingest_events_to_neo4j():
    """Define the DAG topology using TaskFlow tasks."""

    @task
    def init_run() -> str:
        """Create a unique `run_id` for this ingestion run."""
        return new_run_id("weth_usdc")

    @task
    def get_ingest_params() -> Dict:
        """Build ingestion parameters from `dag_run.conf`.

        Modes:
        - latest_blocks (default)
        - logs_number (takes precedence when provided)
        """
        ctx = get_current_context()
        dag_run = ctx.get("dag_run")
        conf = (dag_run.conf or {}) if dag_run is not None else {}

        provider_url = os.environ["ALCHEMY_URL"]
        w3 = Web3(HTTPProvider(provider_url, request_kwargs={"timeout": 30}))
        if not w3.is_connected():
            raise ConnectionError("Unable to reach {}".format(provider_url))

        to_block = int(w3.eth.block_number)

        if conf.get("logs_number") is not None:
            return parse_logs_number_ingest_params(conf=conf, to_block=to_block)

        latest_blocks_int = int(conf.get("latest_blocks", 1000))
        if latest_blocks_int <= 0:
            raise ValueError("latest_blocks must be a positive integer")

        from_block, to_block = compute_latest_block_range(to_block=to_block, latest_blocks=latest_blocks_int)

        # Stable marker for integration tests (parsed from Airflow task logs).
        print(
            "LATEST_BLOCK_RANGE from_block={} to_block={} latest_blocks={}".format(
                from_block,
                to_block,
                latest_blocks_int,
            )
        )
        return {
            "mode": "latest_blocks",
            "from_block": from_block,
            "to_block": to_block,
            "latest_blocks": latest_blocks_int,
        }

    @task
    def fetch_events(graph_run_id: str, ingest_params: Dict) -> Dict:
        """Fetch logs and store them in Postgres. Returns small metadata."""
        provider_url = os.environ["ALCHEMY_URL"]
        contract_address = os.getenv("UNISWAP_PAIR_ADDRESS", DEFAULT_CONTRACT_ADDRESS)

        mode = ingest_params.get("mode")
        if mode == "logs_number":
            events = fetch_latest_events_by_log_count(
                contract_address=contract_address,
                provider_url=provider_url,
                to_block=int(ingest_params["to_block"]),
                logs_number=int(ingest_params["logs_number"]),
                chunk_size=int(ingest_params.get("chunk_size_blocks", 10_000)),
                max_scan_blocks=ingest_params.get("max_scan_blocks"),
            )
        else:
            events = fetch_events_in_block_range(
                contract_address,
                provider_url=provider_url,
                from_block=int(ingest_params["from_block"]),
                to_block=int(ingest_params["to_block"]),
            )

        stored = store_events_in_postgres(graph_run_id, events)

        # Stable marker for integration tests.
        print("POSTGRES_EVENTS_STORED run_id={} events_stored={}".format(graph_run_id, stored))
        return {"run_id": graph_run_id, "events": stored}

    @task
    def build_edges(run_meta: Dict) -> Dict:
        """Infer causal edges and store them in Postgres. Returns small metadata."""
        run_id = run_meta["run_id"]
        events = load_events_from_postgres(run_id)
        edges = build_causes_edges(events)
        stored = store_edges_in_postgres(run_id, edges)

        # Stable marker for integration tests.
        print("POSTGRES_EDGES_STORED run_id={} edges_stored={}".format(run_id, stored))
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

        # Stable marker for integration tests.
        print("NEO4J_EVENTS_WRITTEN run_id={} events_written={}".format(run_id, len(events)))

        total_events, events_with_causes, causes_edges = get_causes_coverage_for_run(run_id)
        print(
            "NEO4J_CAUSES_COVERAGE run_id={} total_events={} events_with_causes={} causes_edges={}".format(
                run_id,
                total_events,
                events_with_causes,
                causes_edges,
            )
        )
        return run_id

    @task
    def cleanup_staging() -> None:
        """Optionally cleanup old staging rows in Postgres.

        Enable by passing `retention_hours` in dag_run.conf.
        """
        ctx = get_current_context()
        dag_run = ctx.get("dag_run")
        conf = (dag_run.conf or {}) if dag_run is not None else {}
        if conf.get("retention_hours") is None:
            return

        retention_hours = int(conf.get("retention_hours"))
        cleanup_event_graph_tables(retention_hours=retention_hours)
        print("POSTGRES_CLEANUP retention_hours={}".format(retention_hours))

    graph_run_id = init_run()
    ingest_params = get_ingest_params()
    run_meta = fetch_events(graph_run_id, ingest_params)
    edge_meta = build_edges(run_meta)
    write_graph(run_meta, edge_meta)
    cleanup_staging()


uniswap_weth_usdc_ingest_events_to_neo4j()
