# Event Graph MVP — Enrollment Events → Neo4j

This repository is an MVP that demonstrates how to turn **synthetic Enrollment events** into a **causal event graph**:

- declared-causality rules text (C0) → events fixture (JSON / NDJSON)
- normalization → edges → graph file
- persistence into Neo4j
- deterministic validation via canonical causality artifacts C0..C5

The goal is to make correctness and system behavior analyzable as **structure** (graphs), using upstream-declared causality instead of timestamps or workflow inference.

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

- Upstream declares causality; downstream validates declarations.
- Every stage renders the same canonical causality rules text and asserts equality across stages.
- Use Neo4j + Cypher to validate invariants and explore behavior without timing-based assertions.

## Repo layout (high level)

- `docker/` — docker-compose stack (Airflow + Postgres + Neo4j) and DAG wiring
- `src/helpers/` — Enrollment + Neo4j helpers (real logic)
- `test/` — pytest suite (unit/behavior/end2end/functional)
- `test/fixtures/events/` — deterministic Enrollment fixtures (JSON / NDJSON)

## Running services (Docker)

From the `docker/` directory:

- Start services: `docker compose up -d`
- View logs: `docker compose logs -f airflow-webserver`
- Stop: `docker compose down`

Environment:

- Airflow container deps are installed via `_PIP_ADDITIONAL_REQUIREMENTS` from `docker/requirements-airflow.txt`.

## Running tests (host)

This repo runs pytest on the host for helper logic (fast iteration), while dependent services (Neo4j/Postgres/Airflow) run in Docker.

- Run the full suite using the repo venv:
  - `.venv/bin/python -m pytest`

If you don’t have a venv yet, create one (example):

- `python3 -m venv .venv`
- `.venv/bin/pip install -r requirements.txt`

## Notes

- The generator is specified in `.github/instructions/events_generator.instructions.md`.
- Causality is based on explicit parent declarations; downstream must not infer workflow.
