from __future__ import annotations

import json
from pathlib import Path

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


DAG_ID = "kafka_eth_to_neo4j_graph"


def _run_dir(run_id: str) -> Path:
    base = Path("/opt/airflow/logs") / "eventgraph" / run_id
    base.mkdir(parents=True, exist_ok=True)
    return base


def consume_kafka_logs_to_file(**context) -> str:
    run_id = context.get("run_id") or "manual"
    out_file = _run_dir(str(run_id)) / "raw_logs.json"

    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) if dag_run is not None else None

    # Deterministic escape hatch (same as the non-kafka DAG): allow passing a fixture file.
    source_logs_file = conf.get("source_logs_file") if conf else None
    if source_logs_file:
        with open(source_logs_file, "r", encoding="utf-8") as f:
            raw_logs = json.load(f)
        out_file.write_text(json.dumps(raw_logs), encoding="utf-8")
        return str(out_file)

    topic = (conf.get("topic") if conf else None) or "eth.raw_logs"
    partition = int((conf.get("partition") if conf else 0) or 0)
    offset = int((conf.get("offset") if conf else 0) or 0)
    max_messages = int((conf.get("max_messages") if conf else 1) or 1)

    from helpers.kafka.adapter import consume_raw_logs_to_file

    consume_raw_logs_to_file(
        topic=topic,
        out_file=str(out_file),
        partition=partition,
        offset=offset,
        max_messages=max_messages,
    )

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
    tags=["event-graph", "kafka"],
) as dag:
    t1 = PythonOperator(task_id="consume_kafka_logs_to_file", python_callable=consume_kafka_logs_to_file)

    t2 = PythonOperator(
        task_id="transform_logs_to_events_file",
        python_callable=transform_logs_to_events_file,
        op_args=["{{ ti.xcom_pull(task_ids='consume_kafka_logs_to_file') }}"],
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
