# enrollment_flow_blueprint.instructions.md — FIXTURE-DRIVEN FLOW BLUEPRINT (AUTHORITATIVE FOR ENROLLMENT PIPELINE)

## 0. Purpose

This document describes the **existing ETH→events→graph→Neo4j pipeline** in this repo:

- how the Airflow DAG is structured
- which helper modules implement the real logic
- how fixtures are used to make tests deterministic
- how unit/behavior/e2e tests fit together

It also defines the **blueprint** for implementing an analogous pipeline for **Enrollment Events**:

- same “thin DAG + tested helpers” architecture
- same fixture-driven behavior tests
- deterministic, post-factum processing

This blueprint is intended to be followed when building the Enrollment flow.

---

## 1. The current ETH flow (what exists today)

### 1.1 End-to-end DAG structure

The Airflow DAG is: `docker/dags/eth_to_neo4j_graph_dag.py`

It is intentionally a **thin orchestrator** with file-based handoffs between steps:

1) `fetch_logs_to_file`
- writes `raw_logs.json`
- supports deterministic runs by allowing a fixture path injection via `dag_run.conf["source_logs_file"]`

2) `transform_logs_to_events_file`
- reads `raw_logs.json`
- writes `events.json` (normalized events)

3) `transform_events_to_graph_file`
- reads `events.json`
- writes `graph.json` containing `{run_id, events, edges}`

4) `write_graph_to_neo4j`
- reads `graph.json`
- writes nodes/edges to Neo4j

Why file handoffs?
- makes each stage replayable
- simplifies debugging
- allows deterministic tests by “pinning input” via fixtures
- avoids reconstructing state from time/order

### 1.2 Helper module responsibilities (real logic lives here)

Helpers live under `src/helpers/`.

In Docker Airflow, helpers are mounted read-only to `/opt/airflow/dags/helpers` (see `docker/docker-compose.yml`).

#### ETH input (I/O)

- `src/helpers/eth/adapter.py`
  - fetches logs from RPC (`fetch_logs`) when environment variables are present
  - **serializes** logs to a JSON-serializable representation (`write_logs_to_file`) so fixtures can be committed and replayed

#### ETH decoding (pure / deterministic)

- `src/helpers/eth/logs/decoder.py`
  - maps `topic0` to known event names
  - decodes ABI payloads for supported events
  - returns `{"event_name": ..., "decoded": ...}`

#### ETH normalization

- `src/helpers/eth/logs/transformer.py`
  - `load_logs_from_file()` loads raw logs fixture
  - `transform_logs()` produces a normalized event list with stable identity:
    - `event_id = tx_hash + ":" + log_index`
    - and convenience fields like `tx_hash`, `log_index`, `event_name`, `decoded`
  - `write_events_to_file()` persists normalized events

#### Causal graph construction (no timestamps, no workflow inference)

- `src/helpers/neo4j/transformer.py`
  - groups events by `tx_hash`
  - sorts by `log_index`
  - applies local causal rules (same transaction, `parent.log_index < child.log_index`)
  - emits edges as `{"from": parent_event_id, "to": child_event_id}`

#### Graph persistence

- `src/helpers/neo4j/adapter.py`
  - writes `(:Run {run_id})` snapshot
  - merges `(:Event {event_id})`
  - creates `(:Event)-[:CAUSES]->(:Event)` relationships

---

## 2. Test strategy in this repo (TDD-shaped)

The repo follows a TDD-style layering:

### 2.1 Unit tests (`@pytest.mark.unit`)

Goal: fast feedback for pure functions.

Examples:
- `test/unit/test_helpers_eth_decoder.py`
  - decodes individual event fixtures (`erc20_transfer.json`, `uniswap_v2_swap.json`, …)
  - asserts exact decoded output

- `test/unit/test_helpers_neo4j_transformer_unit.py`
  - builds tiny event lists and asserts exact edge outputs

### 2.2 Behavior tests (`@pytest.mark.behavior`)

Goal: deterministic black-box validation using committed fixtures.

Key pattern:
- behavior tests load `test/fixtures/eth_logs/fetch_logs_uniswap_v2_weth_usdc.json`
- then run multiple helper stages end-to-end (filesystem-based)
- assertions focus on **invariants** and **schema correctness**, not timing

Examples:
- `test/behavior/test_helpers_eth_transformer.py`
  - ensures all fixture logs decode to known events
  - ensures write→read roundtrips remain decoder-compatible

- `test/behavior/test_helpers_neo4j_transformer.py`
  - full stack: fixture → normalized events → edges → graph file
  - validates every edge respects same-tx + increasing log index
  - includes a Neo4j write/read assertion (still fixture-driven, but requires Neo4j)

### 2.3 E2E tests (`@pytest.mark.e2e`)

Goal: validate Docker/Airflow wiring and service readiness.

Key pattern for deterministic DAG execution:
- copy a committed fixture into the Airflow shared logs volume
- trigger a DAG run with `dag_run.conf["source_logs_file"]` pointing at that file

Preferred polling/verification (avoid direct Postgres queries):
- poll for deterministic filesystem artifacts under the shared logs volume
  (e.g. wait for `C5.txt` in `/opt/airflow/logs/eventgraph/<run_id>/`)

Example:
- `test/behavior/test_airflow_dag_eth_to_neo4j_graph.py` (marked `e2e`)

Important:
- avoid UI-driven E2E
- no timing-based assertions beyond eventual completion (timeouts are infrastructure, not behavior)

---

## 3. Fixture management

### 3.1 Canonical ETH fixture

- `test/fixtures/eth_logs/fetch_logs_uniswap_v2_weth_usdc.json` is the deterministic input for behavior tests.

### 3.2 Refresh script

- `scripts/refresh_behavior_fixture.py` fetches logs from RPC and refreshes the shared fixture.

Constraints:
- refreshing a fixture is an explicit action (not done in tests)
- committed fixtures must remain JSON-serializable and decoder-compatible

---

## 4. Blueprint for Enrollment Events (what we will build next)

### 4.1 High-level rule

Enrollment pipeline MUST follow the same structure, but MUST be delivered in two stages:

- Stage A (helpers first): implement + test the helper pipeline end-to-end **without Airflow DAG**
- Stage B (orchestration): add a thin Airflow DAG + deterministic DAG e2e test

The intent is to keep core logic testable and deterministic before any orchestration wiring.

The Enrollment pipeline MUST follow the same overall structure:

- thin DAG under `docker/dags/`
- real logic in `src/helpers/...`
- deterministic behavior tests driven by committed fixtures

Additional rule for Enrollment DAGs:
- DAG files are wiring only. All transformation, validation, saving, and loading must live in helper modules and be test-covered.

### 4.2 Suggested module layout

Use a parallel structure to ETH helpers:

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

### 4.3 Enrollment fixtures

- fixtures live under `test/fixtures/events/` (JSON array or NDJSON)
- fixtures are generated deterministically using the synthetic generator
  - e.g. `scripts/generate_event_generator_fixture.py`

### 4.4 Enrollment behavior tests (fixture-driven)

Write `@pytest.mark.behavior` tests that:

- load the enrollment fixture
- validate schema invariants
- validate graph-shape declarations when using a consistent fixture
- validate that graph edges (if created) reference existing IDs and respect declared causality

### 4.5 Enrollment helper e2e (deterministic, NO DAG)

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

### 4.6 Enrollment DAG e2e (deterministic, later)

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
