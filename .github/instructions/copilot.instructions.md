# instructions.md — Event Graph MVP

## Purpose

Implement a read-only, post-factum event graph system for Uniswap V3 (WETH/USDC),
focused on causality, invariants, and behavioral reasoning.

## Conceptual foundations

Mandatory reading:
1. https://medium.com/@picospitfire/when-flaky-tests-stop-being-a-qa-problem-c66cf2e346cd
2. https://medium.com/@picospitfire/eventdrivenarchitecture-part-2-4-event-graphs-in-event-driven-systems-47347a2574d6
3. https://medium.com/@picospitfire/eventdrivenarchitecture-part-3-4-event-graphs-as-a-decoupling-layer-1210fab3ef90
4. https://medium.com/@picospitfire/eventdrivenarchitecture-part-4-4-how-event-graphs-change-the-role-of-qa-and-test-automation-479a9947f29b

## Event identity

event_id = tx_hash + ":" + log_index

## Causality rules

- Parents must come from the same transaction
- Parent logIndex must be lower
- Resulting graph must be a DAG

### Transfer IN
- No parents

### Swap / Mint
- Parents = all Transfer IN earlier in the transaction

### Transfer OUT
- Parent = last pool state-changing event

### Burn
- Parent = last Mint or Burn (simplified)

## Tech stack

- Apache Airflow
- Python
- web3.py
- Neo4j
- Cypher

No AI/ML. No E2E tests. No timing-based assertions.

Note: Prefer avoiding UI E2E tests. If import/CLI/API validation is insufficient, UI smoke tests (e.g. Playwright) are allowed as a last resort.

## Airflow DAG conventions (production direction)

- DAG files live under `docker/dags/` and must be import-safe (no network calls, no DB writes at import time).
- DAG tasks should be thin orchestration only; all logic belongs in tested helper functions under `docker/dags/dag_helper_functions/`.
- Configuration must come from environment variables or Airflow Connections/Variables (no secrets or API keys in code).
	- Example: `ALCHEMY_URL`, `UNISWAP_PAIR_ADDRESS`, `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`.
- Tasks must be idempotent:
	- Writing events should use stable identity (`event_id = tx_hash:log_index`).
	- Re-running a task for the same input must not duplicate nodes/edges.
- Determinism guideline:
	- Prefer “post-factum backfill” by explicit block ranges (derived from the Airflow run) instead of “latest N events”.
	- If using “latest N” for the MVP, keep it explicit and contained to a single DAG/task, and test the helper functions that implement it.
- Clean code direction:
	- Keep helper functions small and pure where possible.
	- Split I/O from transformation: fetch/normalize/build-edges/write are separate functions.

## DAG validation (import + execution)

Goal: continuously verify that DAGs (a) import without errors and (b) can execute with deterministic inputs.

Constraints:
- Prefer non-UI validation (CLI/API). Use UI-driven E2E (Playwright/Selenium) only if there is no reliable alternative.
- Avoid timing-based assertions; prefer deterministic commands that execute in-process. If polling is required, assert eventual states, not durations.

Recommended approaches:

### 1) Import validation (preferred)

Run inside the Airflow container:
- `airflow dags list-import-errors`
- `airflow dags list`

In Docker Compose (example):
- `docker compose -f docker/docker-compose.yml exec airflow-webserver airflow dags list-import-errors`
- `docker compose -f docker/docker-compose.yml exec airflow-webserver airflow dags list | cat`

### 2) Deterministic execution validation (preferred)

Use `airflow dags test` to run a DAG for a logical date without relying on the scheduler:
- `docker compose -f docker/docker-compose.yml exec airflow-webserver airflow dags test \
	uniswap_weth_usdc_ingest_100_events_to_neo4j \
	2026-01-01 \
	--conf '{"from_block": 19000000, "to_block": 19001000}'`

This avoids UI, avoids scheduler timing, and is the closest-to-production execution check while remaining deterministic.

### 3) REST API validation (optional)

If you want programmatic checks, prefer Airflow’s REST API:
- List import errors: `GET /api/v1/importErrors`
- Trigger a run with block range: `POST /api/v1/dags/{dag_id}/dagRuns` with `conf`
- Query run state: `GET /api/v1/dags/{dag_id}/dagRuns/{dag_run_id}`

If polling is used, treat retries as infrastructure behavior; don’t assert exact durations.

### 4) UI smoke validation (last resort)

If (1) import validation and (2)/(3) deterministic execution checks are not possible for a particular requirement, a minimal UI smoke test may be used:

- Tooling: Playwright preferred.
- Scope: “can reach Airflow UI”, “DAG appears in list”, “trigger run with conf”, “run reaches terminal state (success/failed)”.
- Assertions: only on observable states (elements present, run state value). Do not assert exact timing.
- Waiting: use Playwright’s built-in auto-waits and explicit waits for state transitions; timeouts are acceptable as test infrastructure, but never assert that something happens within N seconds.
