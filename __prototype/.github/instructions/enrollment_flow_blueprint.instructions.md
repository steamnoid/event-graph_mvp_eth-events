# enrollment_flow_blueprint.instructions.md — FIXTURE-DRIVEN FLOW BLUEPRINT (AUTHORITATIVE FOR ENROLLMENT PIPELINE)

## 0. Purpose

This document describes the **Enrollment → events → graph → Neo4j** pipeline in this repo:

- how the Enrollment Airflow DAG is structured
- which helper modules implement the real logic
- how fixtures are used to make tests deterministic
- how unit/behavior/functional/e2e tests fit together

This blueprint is intended to keep the Enrollment flow coherent: thin DAG + tested helpers + deterministic artifacts.

---

## 1. Enrollment flow (what exists today)

### 1.1 End-to-end DAG structure

The Airflow DAG is: `docker/dags/enrollment_to_neo4j_graph_dag.py`

It is intentionally a **thin orchestrator** with file-based handoffs between steps.
Artifacts are written under `/opt/airflow/logs/eventgraph/<run_id>/`.

The DAG supports deterministic runs via:

- `dag_run.conf["source_events_file"]` → JSON array or NDJSON
- optional `dag_run.conf["source_rules_file"]` → C0 rules text (if provided, assert `C0 == C1`)

Validation tasks write and compare canonical causality artifacts:

- `C1.txt` after raw events load
- `C2.txt` after normalization
- `C3.txt` after edges
- `C4.txt` after graph file
- `C5.txt` after Neo4j write (readback)

### 1.2 Helper module responsibilities (real logic lives here)

Helpers live under `src/helpers/`.

In Docker Airflow, helpers are mounted read-only to `/opt/airflow/dags/helpers`.

- `src/helpers/enrollment/adapter.py`: load raw events fixture (JSON / NDJSON)
- `src/helpers/enrollment/transformer.py`: normalize events into a stable schema
- `src/helpers/enrollment/graph.py`: build edges from declared parents (no inference)
- `src/helpers/enrollment/validator.py`: schema checks + cross-stage C1..C5 equality checks
- `src/helpers/enrollment/artifacts.py`: run directory + artifact file read/write
- `src/helpers/neo4j/adapter.py`: persist and read back the run subgraph

---

## 2. Test strategy (layered, fixture-driven)

### 2.1 Unit tests (`@pytest.mark.unit`)

Goal: fast feedback for pure helper functions.

### 2.2 Behavior tests (`@pytest.mark.behavior`)

Goal: deterministic black-box validation using committed fixtures.

Key pattern:

- load fixtures from `test/fixtures/events/`
- run multiple helper stages end-to-end using file handoffs
- assertions focus on invariants and schema correctness, not timing

### 2.3 Functional tests (`@pytest.mark.functional`)

Goal: black-box validation of the synthetic generator.

### 2.4 E2E tests (`@pytest.mark.e2e`)

Goal: validate Docker/Airflow wiring and service readiness.

Key pattern:

- copy a committed fixture into the Airflow shared logs volume
- trigger the DAG with `dag_run.conf` pointing at that fixture
- poll for deterministic artifacts under `/opt/airflow/logs/eventgraph/<run_id>/` (for example `C5.txt`)

Important:

- avoid UI-driven E2E
- no timing-based assertions beyond eventual completion

---

## 3. Non-negotiables

The Enrollment pipeline MUST follow the same overall structure:

- thin DAG under `docker/dags/`
- real logic in `src/helpers/...`
- deterministic behavior tests driven by committed fixtures

Additional rule:

- DAG files are wiring only. All transformation, validation, saving, and loading must live in helper modules and be test-covered.

### 4.1 Module layout

- `src/helpers/enrollment/adapter.py`
  - file I/O and any external I/O (if any)

- `src/helpers/enrollment/artifacts.py`
  - artifact directory handling and writing intermediate artifacts (events/normalized/edges/graph/Ck)

- `src/helpers/enrollment/validator.py`
  - schema validation and cross-stage equality checks (C1==C2==...==C5)

- `src/helpers/enrollment/transformer.py`
  - normalization of enrollment events into a stable schema

- `src/helpers/enrollment/graph.py`
  - edge construction / declared-causality validation (no inference)

- `src/helpers/neo4j/adapter.py` reused for persistence

### 4.2 Enrollment fixtures

- fixtures live under `test/fixtures/events/` (JSON array or NDJSON)
- fixtures are generated deterministically using the synthetic generator
  - e.g. `scripts/generate_event_generator_fixture.py`

### 4.3 Enrollment behavior tests (fixture-driven)

Write `@pytest.mark.behavior` tests that:

- load the enrollment fixture
- validate schema invariants
- validate graph-shape declarations when using a consistent fixture
- validate that graph edges (if created) reference existing IDs and respect declared causality

### 4.4 Enrollment helper e2e (deterministic, NO DAG)

Before adding any Enrollment DAG, add an `@pytest.mark.e2e` test that validates the Enrollment flow end-to-end by calling helpers directly.

Pattern:

- take a committed fixture from `test/fixtures/events/`
- run the Enrollment helpers in-process (file handoffs are OK and encouraged)
- (optionally) write to Neo4j via `src/helpers/neo4j/adapter.py` if the repo’s Docker services are available
- assertions focus on:
  - schema invariants
  - declared-causality correctness for consistent fixtures
  - graph write succeeds (if Neo4j is included)

This test MUST NOT import or execute an Airflow DAG.

### 4.5 Enrollment DAG e2e (deterministic)

Only after Stage A is complete, add an `@pytest.mark.e2e` test that triggers the enrollment DAG using the same injection pattern:

- put the fixture file into Airflow shared logs volume
- pass path via `dag_run.conf` to DAG task

---

## 5. Non-negotiables (to keep the repo coherent)

- DAGs must be import-safe.
- All “real logic” must be in helpers with tests.
- Behavior tests must be deterministic and fixture-driven.
- No timestamps/sleeps for correctness (only eventual-state polling for infrastructure).
- For Enrollment decisions: upstream declares causality; downstream validates declarations.
