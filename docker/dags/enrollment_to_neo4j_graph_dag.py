from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_helpers.enrollment.fetch_events_to_file.task import task_fetch_events_to_file
from dag_helpers.enrollment.transform_edges_to_graph_file.task import task_transform_edges_to_graph_file
from dag_helpers.enrollment.transform_events_to_normalized_file.task import (
    task_transform_events_to_normalized_file,
)
from dag_helpers.enrollment.transform_normalized_to_edges_file.task import (
    task_transform_normalized_to_edges_file,
)
from dag_helpers.enrollment.validate_edges.task import task_validate_edges
from dag_helpers.enrollment.validate_graph.task import task_validate_graph
from dag_helpers.enrollment.validate_neo4j_readback.task import task_validate_neo4j_readback
from dag_helpers.enrollment.validate_normalized_events.task import task_validate_normalized_events
from dag_helpers.enrollment.validate_raw_events.task import task_validate_raw_events
from dag_helpers.enrollment.write_graph_to_neo4j.task import task_write_graph_to_neo4j


DAG_ID = "enrollment_to_neo4j_graph"


with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["event-graph", "enrollment"],
) as dag:
    t1 = PythonOperator(task_id="fetch_events_to_file", python_callable=task_fetch_events_to_file)

    v1 = PythonOperator(
        task_id="validate_raw_events",
        python_callable=task_validate_raw_events,
        op_args=["{{ ti.xcom_pull(task_ids='fetch_events_to_file') }}"],
    )

    t2 = PythonOperator(
        task_id="transform_events_to_normalized_file",
        python_callable=task_transform_events_to_normalized_file,
        op_args=["{{ ti.xcom_pull(task_ids='fetch_events_to_file') }}"],
    )

    v2 = PythonOperator(
        task_id="validate_normalized_events",
        python_callable=task_validate_normalized_events,
        op_args=["{{ ti.xcom_pull(task_ids='transform_events_to_normalized_file') }}"],
    )

    t3 = PythonOperator(
        task_id="transform_normalized_to_edges_file",
        python_callable=task_transform_normalized_to_edges_file,
        op_args=["{{ ti.xcom_pull(task_ids='transform_events_to_normalized_file') }}"],
    )

    v3 = PythonOperator(
        task_id="validate_edges",
        python_callable=task_validate_edges,
        op_args=[
            "{{ ti.xcom_pull(task_ids='transform_events_to_normalized_file') }}",
            "{{ ti.xcom_pull(task_ids='transform_normalized_to_edges_file') }}",
        ],
    )

    t4 = PythonOperator(
        task_id="transform_edges_to_graph_file",
        python_callable=task_transform_edges_to_graph_file,
        op_args=[
            "{{ ti.xcom_pull(task_ids='transform_events_to_normalized_file') }}",
            "{{ ti.xcom_pull(task_ids='transform_normalized_to_edges_file') }}",
        ],
    )

    v4 = PythonOperator(
        task_id="validate_graph",
        python_callable=task_validate_graph,
        op_args=["{{ ti.xcom_pull(task_ids='transform_edges_to_graph_file') }}"],
    )

    t5 = PythonOperator(
        task_id="write_graph_to_neo4j",
        python_callable=task_write_graph_to_neo4j,
        op_args=["{{ ti.xcom_pull(task_ids='transform_edges_to_graph_file') }}"],
    )

    v5 = PythonOperator(task_id="validate_neo4j_readback", python_callable=task_validate_neo4j_readback)

    t1 >> v1 >> t2 >> v2 >> t3 >> v3 >> t4 >> v4 >> t5 >> v5
