# evm_traces.instructions.md — Airflow + EVM Traces (Alchemy) (MANDATORY)

## 0. Purpose

This project introduces **EVM execution traces** as an additional event source,
ingested and processed **via Apache Airflow**, using **Alchemy Trace API**.

Implementation order for this repo (current state):
- helpers first (pure + deterministic)
- host-run tests (pytest) to validate helpers
- Airflow container integration later

GitHub Copilot must assist in building this pipeline **strictly according to
event-graph and invariant principles** defined in the project.

This document is **normative**.

---

## 0.1 MVP intent (clarification)

The current MVP direction is:

- An **Airflow DAG** builds trace-derived graphs for **transactions** in the **latest ~100 blocks**.
- For each block in that range, the DAG enumerates transaction hashes (via standard Ethereum RPC), then fetches **per-transaction traces** using `trace_transaction(tx_hash)`.
- Graph construction uses shared helpers only (deterministic `event_id`, parent via `traceAddress`, stable ordering).

Out of scope for the current MVP:

- cursor / incremental “only new blocks” processing (we re-process the latest ~100 blocks each run for now)
- durability strategy (it is acceptable to manually clear Neo4j during MVP)
- using `trace_block` as the primary ingestion method (it can exist for fixtures / shape testing)

---

## 0.2 Airflow task architecture (helpers-first)

We follow the same structure as the existing **eth logs** pipeline:

- Airflow DAG code and operators are **orchestration only**.
- Each Airflow task must call **shared helpers** to do all interpretation/transform work.
- All trace semantics must live in `src/helpers/...` (pure, deterministic, TDD-covered).

Concretely, tasks should be thin wrappers around helpers:

- block range planning (pure helper)
- fetching tx hashes (RPC adapter/helper)
- fetching traces per tx (`trace_transaction`) (client/helper)
- building trace graphs and Neo4j payloads (transformer/neo4j helper)
- writing graph snapshots to Neo4j (Neo4j adapter/helper)

Neo4j write semantics (MVP):

- The task that writes to Neo4j must accept a helper-produced payload shaped like `{run_id, events, edges}`.
- `run_id` should be deterministic (e.g. derived from `graph_hash`) so re-runs of the same input are idempotent.
- The DAG must not implement Cypher or merge semantics directly; it calls the existing Neo4j adapter.

Airflow must NOT contain embedded parsing rules, parent inference, event_id logic, or ad-hoc graph shaping.

---

## 1. Source of truth

EVM execution traces are fetched from **Ethereum Mainnet** using:

- Alchemy Trace API
- RPC methods:
  - `trace_block`
  - `trace_transaction`
  - (optionally) `debug_traceTransaction`

The human already has an **Alchemy Pay-As-You-Go plan**.

Copilot must assume:
- traces are available on mainnet
- traces are fetched via JSON-RPC
- no testnet is required

---

## 2. Conceptual model (critical)

Execution traces represent **attempted behavior**, not only successful outcomes.

Unlike contract logs:
- traces exist even when transactions revert
- traces expose CALL / RETURN / REVERT structure
- traces form a **tree of execution**

Each trace step is treated as an **event candidate**.

---

## 3. Event identity for traces

Each trace-derived event must have a stable identity:

```text
event_id = tx_hash + ":" + trace_address
```

Where:
- `trace_address` is the call path (e.g. [0,1,0])
- identity is deterministic and replayable

Copilot must never invent alternative IDs.

---

## 4. Parent rules (DO NOT IMPROVISE)

Parent-child relationships are **explicit in the trace structure**:

- parent = previous frame in the call stack
- root call has no parent
- REVERT and RETURN close a call frame

No heuristics.
No timestamps.
No inference.

The execution trace tree **is the causality graph**.

---

## 5. Graph construction discipline

Graph construction rules:

- traces are grouped by **transaction**
- transactions are processed inside **block batches**
- graph building happens only on **complete blocks**
- no partial graph construction

Streaming is allowed **only as transport**.
Graph construction is **batch-only**.

---

## 6. Airflow responsibilities

Apache Airflow is responsible for:

- scheduling block ranges
- invoking trace RPC calls
- persisting raw trace payloads
- triggering downstream processing

Airflow must NOT:
- infer causality
- build graphs directly
- interpret REVERT semantics

---

## 7. Spark interaction (if used)

If Apache Spark is used:

- Spark may fetch traces in batch or micro-batch mode
- Spark executes **shared helpers**
- Spark materializes nodes and edges
- Spark writes to Neo4j

Spark must NOT:
- redefine parent rules
- alter event identity logic
- introduce new semantics

---

## 8. Shared helpers requirement

All trace interpretation logic must live in **shared helpers**, including:

- trace event classification (CALL / REVERT / RETURN)
- event_id generation
- parent resolution
- invariant definitions

Helpers must be:
- pure Python
- deterministic
- fully covered by TDD

Airflow and Spark must both import these helpers.

---

## 9. Invariants specific to traces

Copilot should help define invariants such as:

- every CALL must end in RETURN or REVERT
- REVERT must propagate unless explicitly handled
- no successful state-changing calls after REVERT
- no orphaned trace frames

Invariants are expressed as **graph queries**, not tests.

---

## 10. TDD constraints

All new trace-related logic must follow:

- STRICT TDD
- Red → Green → Refactor
- one behavior per cycle

Copilot must:
- first suggest the next test
- then minimal implementation
- then refactor

---

## 10.1 Autonomy contract (for this project)

Copilot must operate **autonomously** by default:

- Do **not** ask for confirmation after every micro-step.
- Proceed through multiple small Red → Green → Refactor cycles when the next step is the obvious continuation of the current behavior.

Copilot must ask the human only at **larger milestones / decision points**, such as:

- choosing between Trace API vs Debug API for a new pipeline stage
- introducing a new container/service or changing Docker Compose topology
- changing Neo4j schema or write semantics
- introducing a new data model that affects multiple layers (Airflow + helpers + tests)
- any change that would widen scope beyond the current MVP behavior

---

## 11. Agent execution (VS Code)

Copilot operates in VS Code agent mode and may:

- create/edit tests and production code using workspace tools
- run the smallest possible test command(s)

This project still enforces STRICT TDD (Red → Green → Refactor) as defined in
`copilot_tdd.instructions.md`.

---

## 11.1 Local testing (current workflow)

Until Airflow integration is introduced, we run tests locally using the repo venv:

- `.venv/bin/python -m pytest`

Important constraint:
- Host-run tests must not make live network calls to Ethereum RPC.

This keeps tests deterministic and respects the project containerization rules.

---

## 11.2 E2E traces tests (parse-only, no DB)

We do want an end-to-end style test that proves:
- a real trace payload shape can be parsed, normalized, and transformed into a graph
- without writing to Neo4j/Postgres yet

However, for now E2E tests must be **fixture-based**:
- Store one or more raw trace payloads (captured from mainnet) under a fixtures directory (e.g. `test/fixtures/evm_traces/`).
- The E2E test loads the fixture JSON and runs it through the trace response adapter + transformer.

Live trace capture is allowed as a manual step (outside pytest) and will be containerized later.

Fixture refresh script (manual):

- Refresh the committed trace fixture from Alchemy:
  - `./.venv/bin/python scripts/refresh_evm_traces_fixture.py`
  - `./.venv/bin/python scripts/refresh_evm_traces_fixture.py --mode block`

Default output:

- `test/fixtures/evm_traces/trace_transaction_sample.json`
- `test/fixtures/evm_traces/trace_block_sample.json`

---

## 12. Non-goals

Copilot must NOT:

- treat traces as logs
- ignore REVERT paths
- collapse trace trees into flat tables
- introduce ML or heuristics
- require testnets for trace access

---

## 13. Guiding principle

> Execution traces show **what the system tried to do**.
> Event graphs explain **why it failed or succeeded**.

---

## 14. Status

This document is **normative**.
All EVM trace-related code must comply with it.
