from __future__ import annotations

from datetime import datetime
from pathlib import Path
import json

import os

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


DAG_ID = "evm_traces_transaction_ingest_to_neo4j"


def _run_dir(run_id: str) -> Path:
    base = Path("/opt/airflow/logs") / "eventgraph" / run_id
    base.mkdir(parents=True, exist_ok=True)
    return base


def plan_block_range_to_file(**context) -> str:
    provider_url = os.getenv("ALCHEMY_URL")
    if not provider_url:
        raise ValueError("ALCHEMY_URL is required")

    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run is not None else {}
    latest_blocks = int(conf.get("latest_blocks") or os.getenv("EVM_TRACES_LATEST_BLOCKS", "1"))

    run_id = context.get("run_id") or "manual"
    out_file = _run_dir(str(run_id)) / "block_range.json"

    # Import helpers inside task to keep DAG import-safe.
    from helpers.evm.traces.block_adapter import _get_web3
    from helpers.evm.traces.block_range import compute_latest_block_range

    w3 = _get_web3(provider_url)
    to_block = int(w3.eth.block_number)
    from_block, to_block = compute_latest_block_range(to_block=to_block, latest_blocks=latest_blocks)

    out_file.write_text(json.dumps({"from_block": from_block, "to_block": to_block}), encoding="utf-8")
    return str(out_file)


def fetch_tx_hashes_to_file(block_range_file: str, **context) -> str:
    provider_url = os.getenv("ALCHEMY_URL")
    if not provider_url:
        raise ValueError("ALCHEMY_URL is required")

    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run is not None else {}
    max_tx_per_block = int(conf.get("max_tx_per_block") or os.getenv("EVM_TRACES_MAX_TX_PER_BLOCK", "3"))
    min_erc20_tx = int(conf.get("min_erc20_tx") or os.getenv("EVM_TRACES_MIN_ERC20_TX", "5"))
    max_blocks_to_scan = int(conf.get("max_blocks_to_scan") or os.getenv("EVM_TRACES_MAX_BLOCKS_TO_SCAN", "50"))

    run_id = context.get("run_id") or "manual"
    out_file = _run_dir(str(run_id)) / "tx_hashes.json"

    with open(block_range_file, "r", encoding="utf-8") as f:
        block_range = json.load(f)

    from_block = int(block_range.get("from_block"))
    to_block = int(block_range.get("to_block"))

    # Import helpers inside task to keep DAG import-safe.
    from helpers.evm.traces.tx_select import select_tx_hashes_for_recent_blocks_with_erc20_transfer_receipts_until_min

    tx_hashes = select_tx_hashes_for_recent_blocks_with_erc20_transfer_receipts_until_min(
        provider_url=provider_url,
        to_block=to_block,
        max_blocks_to_scan=max_blocks_to_scan,
        min_total_tx=min_erc20_tx,
        max_tx_per_block=max_tx_per_block,
    )

    out_file.write_text(json.dumps(tx_hashes), encoding="utf-8")
    return str(out_file)


def fetch_traces_to_graph_file(tx_hashes_file: str, **context) -> str:
    provider_url = os.getenv("ALCHEMY_URL")
    if not provider_url:
        raise ValueError("ALCHEMY_URL is required")

    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run is not None else {}
    max_tx = int(conf.get("max_tx") or os.getenv("EVM_TRACES_MAX_TX", "0"))

    run_id = context.get("run_id") or "manual"
    out_file = _run_dir(str(run_id)) / "graph.json"

    with open(tx_hashes_file, "r", encoding="utf-8") as f:
        tx_hashes = json.load(f)

    # Import helpers inside task to keep DAG import-safe.
    from helpers.evm.traces.client import fetch_trace_transaction_graph
    from helpers.evm.traces.neo4j import merge_neo4j_graphs, trace_graph_to_neo4j_graph
    from helpers.evm.traces.tx_select import select_tx_hashes_for_run
    from helpers.neo4j.transformer import write_graph_to_file

    per_tx_graphs: list[dict] = []
    for tx_hash in select_tx_hashes_for_run(tx_hashes or [], max_tx=max_tx):
        trace_graph = fetch_trace_transaction_graph(url=provider_url, tx_hash=str(tx_hash))
        per_tx_graphs.append(trace_graph_to_neo4j_graph(trace_graph))

    merged = merge_neo4j_graphs(per_tx_graphs)
    write_graph_to_file(events=merged.get("events") or [], edges=merged.get("edges") or [], run_id=str(merged.get("run_id")), filename=str(out_file))
    return str(out_file)


def write_graph_to_neo4j(graph_file: str, **_context) -> int:
    from helpers.neo4j.adapter import load_graph_from_file, write_graph_to_db

    graph = load_graph_from_file(graph_file)
    write_graph_to_db(graph)
    return len(graph.get("events") or [])


with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["event-graph", "evm-traces"],
):
    t1 = PythonOperator(task_id="plan_block_range_to_file", python_callable=plan_block_range_to_file)

    t2 = PythonOperator(
        task_id="fetch_tx_hashes_to_file",
        python_callable=fetch_tx_hashes_to_file,
        op_args=["{{ ti.xcom_pull(task_ids='plan_block_range_to_file') }}"],
    )

    t3 = PythonOperator(
        task_id="fetch_traces_to_graph_file",
        python_callable=fetch_traces_to_graph_file,
        op_args=["{{ ti.xcom_pull(task_ids='fetch_tx_hashes_to_file') }}"],
    )

    t4 = PythonOperator(
        task_id="write_graph_to_neo4j",
        python_callable=write_graph_to_neo4j,
        op_args=["{{ ti.xcom_pull(task_ids='fetch_traces_to_graph_file') }}"],
    )

    t1 >> t2 >> t3 >> t4
