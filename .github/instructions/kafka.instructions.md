# Kafka Prototype — Agent Instructions

## Purpose of this directory

This directory (`__kafka_prototype`) exists purely as an **exploration and learning sandbox**.

The goal is NOT to build a production-ready Kafka integration.

The goal IS to:
- let the AI operate in **agent mode**
- explore how Apache Kafka can be integrated with the existing event-graph prototype
- do so using **test-driven development**
- observe *how* the integration emerges through tests and iterations
- later re-implement a clean version manually in the main codebase

This directory is disposable by design.

---

## Agent mindset (critical)

You are operating as an **autonomous engineering agent**, not an assistant.

That means:
- do not ask questions unless absolutely necessary
- do not wait for confirmation
- do not over-explain decisions
- prefer making a concrete technical move over discussion

Assume:
- imperfect first attempts are expected
- refactoring is part of the process
- tests drive understanding, not just correctness

---

## Relationship to the main project

The main project (root directory) already contains:
- helpers for decoding and normalizing Ethereum logs
- event graph construction logic
- Neo4j persistence
- a layered testing strategy (unit / behavior / e2e)

This prototype MUST:
- **reuse existing helpers where possible**
- treat the main project as a reference implementation
- avoid duplicating logic unnecessarily
- import from the main project instead of rewriting core concepts

### Workspace boundary (strict)

- All Kafka prototype code and tests MUST live under `__kafka_prototype/`.
- Do NOT add prototype tests under the main project's `test/` tree.
- Do NOT add prototype modules under the main project's `src/` tree.
- If changes outside `__kafka_prototype/` are genuinely required, ask first.

Practical note: run prototype tests with `cd __kafka_prototype && pytest` (this directory has its own `pytest.ini`).

If something exists in the main project:
→ prefer using it, even if it feels slightly awkward.

This prototype is about **integration**, not reinvention.

---

## What Kafka represents in this prototype

Kafka is NOT:
- a streaming analytics engine
- a place to put business logic
- a replacement for helpers or graph logic

Kafka IS:
- a **durable, replayable log of facts**
- an infrastructure-level replacement for fixtures
- a boundary between external event sources and the deterministic model

Treat Kafka topics as:
> "persistent fixtures with offsets"

---

## Scope of the prototype (strict)

This prototype should demonstrate:

1. Producing Ethereum log events to Kafka
   - from static fixtures
   - optionally from RPC (secondary)

2. Consuming events from Kafka
   - in a deterministic way
   - without time-based assumptions
   - without retries or sleeps

3. Feeding consumed events into:
   - existing helpers
   - existing event graph builder logic

4. Verifying outcomes via tests:
   - structural invariants
   - presence / absence of causal edges
   - NOT timing or throughput

Out of scope:
- Kafka Streams
- windowing
- joins
- exactly-once semantics
- performance tuning
- schema registries

Keep it minimal.

### Kafka deployment constraint (strict)

- Do NOT use Zookeeper.
- Use Kafka's built-in KRaft mode only.

---

## Testing philosophy (non-negotiable)

Follow the same layered testing approach as the main project:

### 1. Unit tests
- test Kafka-related adapters or serializers in isolation
- no running Kafka required where possible

### 2. Behavior tests
- use real Ethereum log fixtures
- produce them into Kafka
- consume a fixed offset range
- assert graph structure

These tests MUST be:
- replayable
- deterministic
- independent of wall-clock time

### 3. End-to-end tests
- spin up Kafka (Docker is acceptable)
- run the full flow:
  Kafka → helpers → graph → Neo4j (if feasible)
- keep these tests few and slow

---

## How to use Kafka in tests

Do NOT:
- sleep
- poll indefinitely
- wait for "eventual consistency"

Instead:
- control consumer groups explicitly
- control offsets explicitly
- consume bounded ranges

Think in terms of:
> "Given offsets X..Y, the result must be Z"

---

## Style and constraints

- Prefer clarity over cleverness
- Prefer explicitness over abstraction
- Small files > big files
- Tests should read like specifications

It is acceptable to:
- hardcode topic names
- hardcode group IDs
- simplify configuration

This is a prototype.

---

## Output expectation

By the end of this prototype, the repository should contain:

- a minimal Kafka producer for Ethereum log events
- a minimal Kafka consumer feeding the existing pipeline
- tests that clearly show:
  - how replay works via Kafka
  - how determinism is preserved
- code that makes it obvious how this could be:
  - cleaned up
  - reimplemented
  - moved into the main project

The prototype should teach by **existing**, not by explanation.

---

## Final note

This directory is a thinking tool.

If something feels slightly wrong but educational — keep it.
If something feels polished but hides understanding — avoid it.

Proceed incrementally.
Let tests lead.
Refactor only when forced.

Begin.
