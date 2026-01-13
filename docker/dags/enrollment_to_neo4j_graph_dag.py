from __future__ import annotations

import json
from pathlib import Path

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


DAG_ID = "enrollment_to_neo4j_graph"


def _run_dir(run_id: str) -> Path:
    base = Path("/opt/airflow/logs") / "eventgraph" / run_id
    base.mkdir(parents=True, exist_ok=True)
    return base


def _write_text(path: Path, text: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return str(path)


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _write_json(path: Path, payload) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return str(path)


def _load_conf_file_path(*, context, key: str) -> str | None:
    dag_run = context.get("dag_run")
    if dag_run is None or getattr(dag_run, "conf", None) is None:
        return None
    value = dag_run.conf.get(key)
    return str(value) if value else None


def fetch_events_to_file(**context) -> str:
    """Stage C1: get raw Enrollment events into the run directory.

    Deterministic mode: inject a fixture path via dag_run.conf["source_events_file"].

    Optional: also inject a generator rules text file via dag_run.conf["source_rules_file"].
    If present, it will be copied to C0.txt for later cross-stage equality checks.
    """

    run_id = str(context.get("run_id") or "manual")
    run_dir = _run_dir(run_id)

    source_events_file = _load_conf_file_path(context=context, key="source_events_file")
    if not source_events_file:
        raise ValueError(
            "Enrollment DAG requires dag_run.conf['source_events_file'] (JSON array or NDJSON)."
        )

    # Optional: C0 rules text (produced by generator outside Airflow).
    source_rules_file = _load_conf_file_path(context=context, key="source_rules_file")
    if source_rules_file:
        rules_text = Path(source_rules_file).read_text(encoding="utf-8")
        _write_text(run_dir / "C0.txt", rules_text)

    out_file = run_dir / "events.json"

    # Copy fixture as-is. Helpers can read JSON array or NDJSON.
    out_file.write_text(Path(source_events_file).read_text(encoding="utf-8"), encoding="utf-8")
    return str(out_file)


def validate_raw_events(events_file: str, **context) -> str:
    """Validate raw events schema and produce canonical causality text C1.

    If C0.txt exists (source rules), assert C0 == C1.
    """

    run_id = str(context.get("run_id") or "manual")
    run_dir = _run_dir(run_id)

    from helpers.enrollment.adapter import load_events_from_file
    from helpers.enrollment.causality_rules import causality_rules_from_events, format_causality_rules_text
    from helpers.enrollment.transformer import normalize_events

    raw_events = load_events_from_file(events_file)

    # Schema validation (does not change the events).
    _ = normalize_events(raw_events)

    c1 = format_causality_rules_text(causality_rules_from_events(run_id=run_id, events=raw_events))
    c1_path = run_dir / "C1.txt"
    _write_text(c1_path, c1)

    c0_path = run_dir / "C0.txt"
    if c0_path.exists():
        c0 = _read_text(c0_path)
        if c0 != c1:
            raise ValueError("C0 != C1 (source rules do not match raw fixture events)")

    return str(c1_path)


def transform_events_to_normalized_file(events_file: str, **context) -> str:
    """Stage C2: normalize events to a stable, schema-compatible representation."""

    run_id = str(context.get("run_id") or "manual")
    run_dir = _run_dir(run_id)

    from helpers.enrollment.adapter import load_events_from_file
    from helpers.enrollment.transformer import normalize_events

    raw_events = load_events_from_file(events_file)
    normalized = normalize_events(raw_events)

    out_file = run_dir / "normalized_events.json"
    _write_json(out_file, normalized)
    return str(out_file)


def validate_normalized_events(normalized_file: str, **context) -> str:
    """Validate normalized events and assert C1 == C2."""

    run_id = str(context.get("run_id") or "manual")
    run_dir = _run_dir(run_id)

    from helpers.enrollment.adapter import load_events_from_file
    from helpers.enrollment.causality_rules import causality_rules_from_events, format_causality_rules_text

    normalized = load_events_from_file(normalized_file)

    c2 = format_causality_rules_text(causality_rules_from_events(run_id=run_id, events=normalized))
    c2_path = run_dir / "C2.txt"
    _write_text(c2_path, c2)

    c1 = _read_text(run_dir / "C1.txt")
    if c1 != c2:
        raise ValueError("C1 != C2")

    return str(c2_path)


def transform_normalized_to_edges_file(normalized_file: str, **context) -> str:
    """Stage C3: build edges from declared parent_event_ids."""

    run_id = str(context.get("run_id") or "manual")
    run_dir = _run_dir(run_id)

    from helpers.enrollment.adapter import load_events_from_file
    from helpers.enrollment.graph import build_edges

    normalized = load_events_from_file(normalized_file)
    edges = build_edges(normalized)

    out_file = run_dir / "edges.json"
    _write_json(out_file, edges)
    return str(out_file)


def validate_edges(normalized_file: str, edges_file: str, **context) -> str:
    """Validate edges and assert C2 == C3."""

    run_id = str(context.get("run_id") or "manual")
    run_dir = _run_dir(run_id)

    from helpers.enrollment.adapter import load_events_from_file
    from helpers.enrollment.causality_rules import causality_rules_from_edges, format_causality_rules_text

    normalized = load_events_from_file(normalized_file)
    edges = load_events_from_file(edges_file)

    c3 = format_causality_rules_text(
        causality_rules_from_edges(run_id=run_id, events=normalized, edges=edges)
    )
    c3_path = run_dir / "C3.txt"
    _write_text(c3_path, c3)

    c2 = _read_text(run_dir / "C2.txt")
    if c2 != c3:
        raise ValueError("C2 != C3")

    return str(c3_path)


def transform_edges_to_graph_file(normalized_file: str, edges_file: str, **context) -> str:
    """Stage C4: write graph.json {run_id, events, edges}."""

    run_id = str(context.get("run_id") or "manual")
    run_dir = _run_dir(run_id)

    from helpers.enrollment.adapter import load_events_from_file
    from helpers.enrollment.graph import write_graph_to_file

    normalized = load_events_from_file(normalized_file)
    edges = load_events_from_file(edges_file)

    out_file = run_dir / "graph.json"
    write_graph_to_file(events=normalized, edges=edges, run_id=run_id, filename=str(out_file))
    return str(out_file)


def validate_graph(graph_file: str, **context) -> str:
    """Validate graph file and assert C3 == C4."""

    run_id = str(context.get("run_id") or "manual")
    run_dir = _run_dir(run_id)

    from helpers.enrollment.causality_rules import causality_rules_from_graph_dict, format_causality_rules_text
    from helpers.neo4j.adapter import load_graph_from_file

    graph = load_graph_from_file(graph_file)

    c4 = format_causality_rules_text(causality_rules_from_graph_dict(graph=graph))
    c4_path = run_dir / "C4.txt"
    _write_text(c4_path, c4)

    c3 = _read_text(run_dir / "C3.txt")
    if c3 != c4:
        raise ValueError("C3 != C4")

    return str(c4_path)


def write_graph_to_neo4j(graph_file: str, **_context) -> int:
    from helpers.neo4j.adapter import load_graph_from_file, write_graph_to_db

    graph = load_graph_from_file(graph_file)
    write_graph_to_db(graph)
    return 0


def validate_neo4j_readback(**context) -> str:
    """Stage C5: export causality rules from Neo4j and assert C4 == C5.

    Optional canonical check: dag_run.conf["expect_canonical"] == true
    """

    run_id = str(context.get("run_id") or "manual")
    run_dir = _run_dir(run_id)

    expect_canonical = False
    dag_run = context.get("dag_run")
    if dag_run is not None and getattr(dag_run, "conf", None):
        expect_canonical = bool(dag_run.conf.get("expect_canonical") or False)

    from helpers.enrollment.neo4j_causality_rules import export_causality_rules_text

    c5_path = run_dir / "C5.txt"
    export_causality_rules_text(run_id=run_id, out_file=str(c5_path), expect_canonical=expect_canonical)

    c4 = _read_text(run_dir / "C4.txt")
    c5 = _read_text(c5_path)
    if c4 != c5:
        raise ValueError("C4 != C5")

    return str(c5_path)


with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["event-graph", "enrollment"],
) as dag:
    t1 = PythonOperator(task_id="fetch_events_to_file", python_callable=fetch_events_to_file)

    v1 = PythonOperator(
        task_id="validate_raw_events",
        python_callable=validate_raw_events,
        op_args=["{{ ti.xcom_pull(task_ids='fetch_events_to_file') }}"],
    )

    t2 = PythonOperator(
        task_id="transform_events_to_normalized_file",
        python_callable=transform_events_to_normalized_file,
        op_args=["{{ ti.xcom_pull(task_ids='fetch_events_to_file') }}"],
    )

    v2 = PythonOperator(
        task_id="validate_normalized_events",
        python_callable=validate_normalized_events,
        op_args=["{{ ti.xcom_pull(task_ids='transform_events_to_normalized_file') }}"],
    )

    t3 = PythonOperator(
        task_id="transform_normalized_to_edges_file",
        python_callable=transform_normalized_to_edges_file,
        op_args=["{{ ti.xcom_pull(task_ids='transform_events_to_normalized_file') }}"],
    )

    v3 = PythonOperator(
        task_id="validate_edges",
        python_callable=validate_edges,
        op_args=[
            "{{ ti.xcom_pull(task_ids='transform_events_to_normalized_file') }}",
            "{{ ti.xcom_pull(task_ids='transform_normalized_to_edges_file') }}",
        ],
    )

    t4 = PythonOperator(
        task_id="transform_edges_to_graph_file",
        python_callable=transform_edges_to_graph_file,
        op_args=[
            "{{ ti.xcom_pull(task_ids='transform_events_to_normalized_file') }}",
            "{{ ti.xcom_pull(task_ids='transform_normalized_to_edges_file') }}",
        ],
    )

    v4 = PythonOperator(
        task_id="validate_graph",
        python_callable=validate_graph,
        op_args=["{{ ti.xcom_pull(task_ids='transform_edges_to_graph_file') }}"],
    )

    t5 = PythonOperator(
        task_id="write_graph_to_neo4j",
        python_callable=write_graph_to_neo4j,
        op_args=["{{ ti.xcom_pull(task_ids='transform_edges_to_graph_file') }}"],
    )

    v5 = PythonOperator(task_id="validate_neo4j_readback", python_callable=validate_neo4j_readback)

    t1 >> v1 >> t2 >> v2 >> t3 >> v3 >> t4 >> v4 >> t5 >> v5
