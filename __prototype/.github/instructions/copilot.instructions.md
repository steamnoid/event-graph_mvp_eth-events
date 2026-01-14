# copilot.instructions.md — Event Graph MVP

## Purpose

Implement a read-only, post-factum event graph system for **Enrollment Events**.
The system is deterministic, fixture-driven, and validates that declared causality is preserved across stages.

## Canonical rule

Upstream declares causality.
Downstream validates declarations.

## Authoritative specs

This repo is governed by these instruction files:

- `.github/instructions/events_generator.instructions.md` (event generator + schema)
- `.github/instructions/causality_rules_pipeline.instructions.md` (canonical causality rules text + C0..C5 equality)
- `.github/instructions/enrollment_dag_validation.instructions.md` (required Airflow DAG shape)
- `.github/instructions/enrollment_flow_blueprint.instructions.md` (fixture-driven Enrollment pipeline blueprint)

## Tech stack

- Apache Airflow
- Python
- Neo4j
- Cypher

## Airflow DAG conventions

- DAG files live under `docker/dags/` and must be import-safe.
- DAG tasks are wiring only; all real logic lives in `src/helpers/` and must be test-covered.
- The Enrollment DAG must support deterministic runs via `dag_run.conf["source_events_file"]`.

## Testing scheme

- `@pytest.mark.unit`: small deterministic helper tests
- `@pytest.mark.behavior`: fixture-driven helper pipeline tests
- `@pytest.mark.functional`: black-box tests for the generator
- `@pytest.mark.e2e`: Docker/Airflow wiring tests that validate deterministic artifacts
