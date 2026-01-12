from __future__ import annotations

from datetime import datetime
from pathlib import Path
import json

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


DAG_ID = "eth_to_neo4j_graph"


def _run_dir(run_id: str) -> Path:
    base = Path("/opt/airflow/logs") / "eventgraph" / run_id
    base.mkdir(parents=True, exist_ok=True)
    return base


def fetch_logs_to_file(**context) -> str:
    run_id = context.get("run_id") or "manual"
    out_file = _run_dir(str(run_id)) / "raw_logs.json"

    dag_run = context.get("dag_run")
    source_logs_file = None
    if dag_run is not None and getattr(dag_run, "conf", None):
        source_logs_file = dag_run.conf.get("source_logs_file")

    if source_logs_file:
        with open(source_logs_file, "r", encoding="utf-8") as f:
            raw_logs = json.load(f)

        out_file.write_text(json.dumps(raw_logs), encoding="utf-8")
        return str(out_file)

    from helpers.eth.adapter import fetch_logs, write_logs_to_file

    raw_logs = fetch_logs()
    write_logs_to_file(raw_logs, str(out_file))
    return str(out_file)


def transform_logs_to_events_file(logs_file: str, **context) -> str:
    from helpers.eth.logs.transformer import load_logs_from_file, transform_logs, write_events_to_file

    run_id = context.get("run_id") or "manual"
    out_file = _run_dir(str(run_id)) / "events.json"

    raw_logs = load_logs_from_file(logs_file)
    events = transform_logs(raw_logs)
    write_events_to_file(events, str(out_file))
    return str(out_file)


def transform_events_to_graph_file(events_file: str, **context) -> str:
    from helpers.neo4j.transformer import load_events_from_file, transform_events, write_graph_to_file

    run_id = context.get("run_id") or "manual"
    out_file = _run_dir(str(run_id)) / "graph.json"

    events = load_events_from_file(events_file)
    edges = transform_events(events)
    write_graph_to_file(events=events, edges=edges, run_id=str(run_id), filename=str(out_file))
    return str(out_file)


def write_graph_to_neo4j(graph_file: str, **_context) -> int:
    from helpers.neo4j.adapter import load_graph_from_file, write_graph_to_db

    graph = load_graph_from_file(graph_file)
    return write_graph_to_db(graph)


with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["event-graph"],
) as dag:
    t1 = PythonOperator(task_id="fetch_logs_to_file", python_callable=fetch_logs_to_file)

    t2 = PythonOperator(
        task_id="transform_logs_to_events_file",
        python_callable=transform_logs_to_events_file,
        op_args=["{{ ti.xcom_pull(task_ids='fetch_logs_to_file') }}"],
    )

    t3 = PythonOperator(
        task_id="transform_events_to_graph_file",
        python_callable=transform_events_to_graph_file,
        op_args=["{{ ti.xcom_pull(task_ids='transform_logs_to_events_file') }}"],
    )

    t4 = PythonOperator(
        task_id="write_graph_to_neo4j",
        python_callable=write_graph_to_neo4j,
        op_args=["{{ ti.xcom_pull(task_ids='transform_events_to_graph_file') }}"],
    )

    t1 >> t2 >> t3 >> t4
