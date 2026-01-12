from __future__ import annotations

import json
from pathlib import Path

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


DAG_ID = "kafka_eth_to_neo4j_graph"
DEFAULT_BOOTSTRAP_TOPIC = "eth.raw_logs.bootstrap_default"
DEFAULT_BOOTSTRAP_FIXTURE_FILE = "/opt/airflow/dags/kafka_default_raw_logs.json"


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

    bootstrap_conf = None
    if not conf:
        ti = context.get("ti")
        if ti is not None:
            bootstrap_conf = ti.xcom_pull(task_ids="bootstrap_kafka_input")

    topic = (conf.get("topic") if conf else None) or (bootstrap_conf or {}).get("topic") or "eth.raw_logs"
    partition = int((conf.get("partition") if conf else None) or (bootstrap_conf or {}).get("partition") or 0)
    offset = int((conf.get("offset") if conf else None) or (bootstrap_conf or {}).get("offset") or 0)
    max_messages = int((conf.get("max_messages") if conf else None) or (bootstrap_conf or {}).get("max_messages") or 1)

    from helpers.kafka.adapter import consume_raw_logs_to_file

    consume_raw_logs_to_file(
        topic=topic,
        out_file=str(out_file),
        partition=partition,
        offset=offset,
        max_messages=max_messages,
    )

    return str(out_file)


def bootstrap_kafka_input(**context) -> dict:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) if dag_run is not None else None

    # If the user provided any deterministic input config, do nothing.
    if conf and (
        conf.get("source_logs_file")
        or conf.get("topic")
        or conf.get("offset") is not None
        or conf.get("max_messages") is not None
    ):
        return {}

    payload = json.loads(Path(DEFAULT_BOOTSTRAP_FIXTURE_FILE).read_text(encoding="utf-8"))
    if not isinstance(payload, list) or len(payload) == 0:
        raise RuntimeError(f"Default bootstrap fixture is empty: {DEFAULT_BOOTSTRAP_FIXTURE_FILE}")

    try:
        from kafka import KafkaConsumer, KafkaProducer, TopicPartition  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"kafka-python is required at runtime: {e}")

    from helpers.kafka.adapter import _bootstrap_servers

    tp = TopicPartition(DEFAULT_BOOTSTRAP_TOPIC, 0)
    consumer = KafkaConsumer(
        bootstrap_servers=_bootstrap_servers(),
        enable_auto_commit=False,
        auto_offset_reset="none",
        consumer_timeout_ms=2000,
    )
    end_offsets = consumer.end_offsets([tp])
    start_offset = int(end_offsets.get(tp, 0))
    consumer.close()

    producer = KafkaProducer(
        bootstrap_servers=_bootstrap_servers(),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    for log in payload:
        producer.send(DEFAULT_BOOTSTRAP_TOPIC, value=log, partition=0)
    producer.flush()
    producer.close()

    return {
        "topic": DEFAULT_BOOTSTRAP_TOPIC,
        "partition": 0,
        "offset": start_offset,
        "max_messages": len(payload),
    }


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
    t0 = PythonOperator(task_id="bootstrap_kafka_input", python_callable=bootstrap_kafka_input)
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

    t0 >> t1 >> t2 >> t3 >> t4
