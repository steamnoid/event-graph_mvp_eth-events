# enrollment_dag_validation.instructions.md — ENROLLMENT DAG WITH VALIDATION TASKS

Purpose: define the required Airflow orchestration shape for the Enrollment pipeline.

## Non‑negotiable

- The Enrollment Airflow DAG MUST be a thin orchestrator.
- Real logic stays in `src/helpers/enrollment/*` and `src/helpers/neo4j/*`.
- Between every transformation task, insert a validation task.

Note:
- PostgreSQL exists only as Airflow's metadata DB (metastore).
- Enrollment pipeline logic and validation MUST NOT use Postgres as an analytics store.

## DAG

- File: `docker/dags/enrollment_to_neo4j_graph_dag.py`
- DAG id: `enrollment_to_neo4j_graph`

## Inputs (deterministic)

- The DAG MUST support deterministic runs by accepting a fixture path:
  - `dag_run.conf["source_events_file"]` → file path inside the Airflow container
  - file may be JSON array or NDJSON

Optional:
- `dag_run.conf["source_rules_file"]` → generator causality rules text (C0). If provided, the DAG must assert `C0 == C1`.

## Validation tasks (required)

For run_id `R`, let `Ck` be the canonical causality rules text per stage.

- After raw events load: write `C1.txt` and validate raw schema.
- After normalize: write `C2.txt` and assert `C1 == C2`.
- After edges: write `C3.txt` and assert `C2 == C3`.
- After graph file: write `C4.txt` and assert `C3 == C4`.
- After Neo4j write: write `C5.txt` from Neo4j readback and assert `C4 == C5`.

Optional strict mode:
- `dag_run.conf["expect_canonical"] == true` → validation must also assert canonical shape and identical graphs across entities.

## Artifacts

All intermediate artifacts MUST be written under:

- `/opt/airflow/logs/eventgraph/<run_id>/`

Including:

- `events.json`
- `normalized_events.json`
- `edges.json`
- `graph.json`
- `C0.txt` (optional)
- `C1.txt` … `C5.txt`

