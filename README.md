# Event Graph MVP — Ethereum Events → Neo4j

This repository is an MVP that demonstrates how to turn on-chain Ethereum logs into a **causal event graph**:

- raw logs (fixture or RPC) → normalized `Event` records
- deterministic, local causality (`:CAUSES`) edges
- persistence into Neo4j for querying **graph invariants** and behavior analysis

The goal is to make correctness and system behavior analyzable as **structure** (graphs) rather than fragile, timing-based end-to-end assertions.

## Reading (4 articles)

This project is directly inspired by the following series:

1. **When Flaky Tests Stop Being a QA Problem**  
   https://medium.com/@picospitfire/when-flaky-tests-stop-being-a-qa-problem-c66cf2e346cd
2. **Event Graphs in Event-Driven Systems (Part 2/4)**  
   https://medium.com/@picospitfire/eventdrivenarchitecture-part-2-4-event-graphs-in-event-driven-systems-47347a2574d6
3. **Event Graphs as a Decoupling Layer (Part 3/4)**  
   https://medium.com/@picospitfire/eventdrivenarchitecture-part-3-4-event-graphs-as-a-decoupling-layer-1210fab3ef90
4. **How Event Graphs Change the Role of QA and Test Automation (Part 4/4)**  
   https://medium.com/@picospitfire/eventdrivenarchitecture-part-4-4-how-event-graphs-change-the-role-of-qa-and-test-automation-479a9947f29b

## Project intent (short)

- Treat each on-chain event as an **atomic fact** with stable identity: `event_id = tx_hash:log_index`.
- Build a minimal, deterministic event graph where edges always represent **local causal truth** (same transaction and ordered by log index).
- Use Neo4j + Cypher to validate invariants and explore behavior without relying on “sleep/retry” or timestamp heuristics.

## Repo layout (high level)

- `docker/` — docker-compose stack (Airflow + Postgres + Neo4j) and DAG/helper code
- `docker/dags/helpers/` — Python helpers (ETH decode/transform, Neo4j transform/adapter)
- `test/` — pytest suite (unit/behavior/end2end)
- `test/fixtures/eth_logs/` — deterministic fixture logs used by tests

## Running services (Docker)

From the `docker/` directory:

- Start services: `docker compose up -d`
- View logs: `docker compose logs -f airflow-webserver`
- Stop: `docker compose down`

Environment:

- Optional: set `ALCHEMY_URL` (used by live-RPC ingestion paths). If not set, fixture-based tests still run.
- Airflow container deps are installed via `_PIP_ADDITIONAL_REQUIREMENTS` from `docker/requirements-airflow.txt`.

## Running tests (host)

This repo runs pytest on the host for helper logic (fast iteration), while dependent services (Neo4j/Postgres/Airflow) run in Docker.

- Run the full suite using the repo venv:
  - `.venv/bin/python -m pytest`

If you don’t have a venv yet, create one (example):

- `python3 -m venv .venv`
- `.venv/bin/pip install -r requirements.txt`

## Notes

- This repository follows **strict TDD** (Red → Green → Refactor).
- Causality is intentionally conservative: no timestamp-based inference, no heuristics, no aggregation.
