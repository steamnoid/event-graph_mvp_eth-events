# Summary of @picospitfire Medium articles (9 documents)

Date compiled: 2026-01-14

## Executive summary

Across these 9 articles, the author argues for a structural approach to understanding and validating complex systems—especially event-driven architectures and data pipelines.

Core through-line:
- **Stop validating behavior through timing** (sleep/poll/eventually). Instead, validate behavior through **structure**.
- Represent runtime behavior as an **event graph** (events as nodes; declared causality as edges).
- Define correctness as **graph invariants** (properties that must hold for all valid executions), not step-by-step scripts.
- Treat “flaky tests” as **signals of hidden coupling / nondeterministic behavior**, not merely test defects.
- Apply the same idea to data engineering: pipeline steps must preserve the **structure that carries meaning**, and that preservation should be validated at every boundary.

## Shared model and vocabulary

### Minimal event model
- `event_id`: identity only (unique reference to a concrete event instance)
- `parents_event_id(s)`: explicit causality (why this event exists)
- `event_type`: semantic label (what happened), not control-flow encoding

This yields a directed graph:
- Nodes: events
- Edges: parent links (`child -> parent` or `parent -> child` depending on convention)

### Why graphs
Graphs make behaviors measurable without encoding behavior into names:
- retries become chains or loops in topology
- convergence (multi-parent) represents “decision” points
- missing parents produce “graph gaps” (ambiguity that forces downstream guessing)

### Invariants (examples used across the series)
- **Required predecessor**: an event must not exist unless another event exists in its ancestry.
- **Convergence requirement**: an event must have multiple required ancestors.
- **Bounded retry depth**: a path cannot contain more than N consecutive repeats of some event type.

## Article-by-article summary

### 1) When Flaky Tests Stop Being a QA Problem
Source: https://medium.com/@picospitfire/when-flaky-tests-stop-being-a-qa-problem-c66cf2e346cd

Main idea:
- In distributed/event-driven systems, flakiness is often a **symptom of real system nondeterminism** (race conditions, hidden coupling, timing assumptions), not merely bad automation.

Key points:
- Classic automation is step-oriented; it breaks down when systems are asynchronous and eventually consistent.
- Shift the question from “pass/fail” to “what actually happened?”
- Consume existing events/logs read-only and reconstruct behavioral graphs.
- Test via invariants—properties that should hold regardless of timing noise.

Practical takeaway:
- Use test runs as “behavior capture”: produce graphs, then check invariants instead of writing brittle step assertions.

---

### 2) [EventDrivenArchitecture -Part 2/6] Event Graphs in Event-Driven Systems
Source: https://medium.com/@picospitfire/eventdrivenarchitecture-part-2-4-event-graphs-in-event-driven-systems-47347a2574d6

Main idea:
- A minimal, orthogonal model (identity, causality, semantics) is enough to build an event graph that supports durable testing and analysis.

Key points:
- Separate concerns:
  - `event_id` is identity only.
  - `parents_event_id(s)` is explicit causality; do not infer downstream.
  - `event_type` is semantic label; avoid encoding retries/failure modes in names.
- Meaning emerges from topology; strings shouldn’t carry control-flow.
- Invariants become the main validation mechanism.

Practical takeaway:
- Move complexity from naming/taxonomies into structure: declare parents; derive patterns from graph shape.

---

### 3) [EventDrivenArchitecture -Part 3/6] Event Graphs as a Decoupling Layer
Source: https://medium.com/@picospitfire/eventdrivenarchitecture-part-3-4-event-graphs-as-a-decoupling-layer-1210fab3ef90

Main idea:
- Event graphs create a **read-only decoupling boundary** that can serve testing and business intelligence without increasing production risk.

Key points:
- The application emits only minimal facts; everything downstream is observational.
- Causality becomes a stable substrate across refactors and system evolution.
- Testing becomes “does structure satisfy properties?” rather than “did we follow this path?”
- BI and QA can query the same graph artifact: “Testing and BI operate on the same artifact.”

Practical takeaway:
- Keep graphs observational (no control). Use them as the shared truth for invariants, dashboards, and investigations.

---

### 4) [EventDrivenArchitecture -Part 4/6] How Event Graphs Change the Role of QA and Test Automation Without Replacing Them
Source: https://medium.com/@picospitfire/eventdrivenarchitecture-part-4-4-how-event-graphs-change-the-role-of-qa-and-test-automation-479a9947f29b

Main idea:
- Event graphs don’t replace QA or test automation; they change interpretation—from binary verdicts to behavioral insight.

Key points:
- Automation becomes a “sensor network”: it stimulates the system and produces structured evidence.
- QA shifts from fighting flakiness to interpreting patterns and defining invariants.
- Prioritization becomes evidence-based (patterns, frequencies, structural violations).

Practical takeaway:
- Keep tests, but add a parallel layer: capture behavior graphs and evaluate invariant violations instead of treating every failure as equally urgent.

---

### 5) [EventDrivenArchitecture -Part 5/6] Declaring Causality Upstream: Not Overhead, but an Observability Boundary
Source: https://medium.com/@picospitfire/eventdrivenarchitecture-part-5-6-declaring-causality-upstream-not-overhead-but-an-92375df82055

Main idea:
- Declaring causality (and emitting “decision events”) upstream is not “derived state”; it is how you make decisions observable and prevent downstream reconstruction.

Key points:
- Distinguish **derived state** (internal, replaceable) from **business decision** (a fact the system acted).
- Without events/causality metadata, downstream must guess; that produces duplicated logic and drift.
- Cost asymmetry: upstream declaration is small/local; downstream reconstruction becomes global/exponential.
- Guardrail: treat these as observability signals, not operational control.

Practical takeaway:
- Attach parent references where decisions are made. Make observability a first-class boundary.

---

### 6) [EventDrivenArchitecture -Part 6/6] The Minimal Event Contract
Source: https://medium.com/@picospitfire/eventdrivenarchitecture-part-6-6-the-minimal-event-contract-58e7cf9fc4fe

Main idea:
- The minimal contract for reconstructable causality is simple: **if an event exists, it must declare its parents**. No inference.

Key points:
- “Graph gaps” happen when events appear without determinable causal links.
- Scope reduction: no need to emit “nothing happened” events; silence is fine as long as no node exists.
- Only unacceptable case: event exists but parents are missing/ambiguous.
- Introduces “fact events” vs “decision events” only as structural categories (multi-parent nodes).

Practical takeaway:
- Make parent declaration mandatory for emitted events. Keep the contract minimal to reduce overhead and prevent ambiguity.

---

### 7) Testing Data Engineering Pipelines
Source: https://medium.com/@picospitfire/testing-data-engineering-pipelines-c2e6afdf6beb

Main idea:
- Pipeline tests become reliable when you stop testing “through time” and instead use deterministic replay, layered test scopes, and structural assertions.

Key points:
- Split testing strategy by layer:
  - unit tests for pure helpers
  - behavior tests using static fixtures (replay)
  - small number of true end-to-end tests (real DAG + real DB)
- Avoid mocks and network calls by capturing fixtures once and replaying them.
- Avoid `sleep()` / “eventually”; if tests require waiting, it’s a design smell.

Practical takeaway:
- Make the pipeline importable and deterministic; use fixtures to turn pipeline runs into pure transformations.

---

### 8) Preserving Data Integrity across Data Transformation Steps
Source: https://medium.com/@picospitfire/preserving-data-integrity-across-data-transformation-steps-782af4c016b1

Main idea:
- Data integrity is not only schema correctness; it’s the preservation of **meaning**, which often lives in structural properties.

Key points:
- Pipelines traverse many representations; meaning can drift even when jobs succeed.
- Meaning depends on the domain’s structural axis:
  - tables: keys/dependencies
  - time-series: ordering/windows
  - events: partitioning/causation
  - graphs: edges/relationships
- In the author’s case, causality edges are an invariant: never inferred, preserved verbatim.
- Use a canonical representation as a contract; validate after every stage.
- Treat storage (e.g., Neo4j) as “just another representation”: write, read back, validate.

Practical takeaway:
- Identify your “meaning axis”, make it explicit, and enforce it via canonicalization + stage-by-stage validation.

---

### 9) A Project Structure That Makes Pipelines Readable, Testable, and Boring (in the Best Way)
Source: https://medium.com/@picospitfire/a-project-structure-that-makes-pipelines-readable-testable-and-boring-in-the-best-way-f581147c328f

Main idea:
- A clean project structure + explicit stage contracts + canonical baselines make pipelines readable and deterministically testable.

Key points:
- Separate lanes:
  - runtime orchestration (Airflow/Docker)
  - importable pipeline logic
  - tests
- Use “canonical baselines” as explicit contracts, not incidental outputs.
- Validate candidate outputs against a single canonical reference to prevent “coupled drift” between adjacent stages.

Practical takeaway:
- Make stage I/O explicit and diff-friendly; treat orchestration as wiring and keep logic in testable modules.

## Cross-cutting takeaways (what to do differently)

- Prefer **structural correctness** over time-based correctness.
- Make causality **declarative upstream**: emit parent references where knowledge exists.
- Build a **read-only** graph layer (no feedback control to production).
- Test with **invariants**, not scripts; interpret flakiness as behavior.
- For pipelines, treat each step as a representation change and validate that your “meaning axis” survives every boundary.

## Practical checklist (actionable)

Event-driven systems:
- Ensure every emitted event has a stable `event_id`.
- Require `parents_event_id(s)` whenever an event has causal dependencies.
- Keep `event_type` stable and avoid control-flow encoding in event names.
- Define invariants (predecessors, convergence rules, retry bounds) and run them on the graph.

Data pipelines:
- Define a canonical, text-based representation of the invariant structure.
- After each transformation and after persistence, re-derive and compare against the canonical reference.
- Use deterministic fixtures and replay to avoid time-based flakiness.

QA/Testing organization:
- Treat automation as stimulus + data generation (“sensors”), not only pass/fail.
- Use graph patterns to triage: distinguish incidental instability from invariant violations.

## Sources (index)

- https://medium.com/@picospitfire/when-flaky-tests-stop-being-a-qa-problem-c66cf2e346cd
- https://medium.com/@picospitfire/eventdrivenarchitecture-part-2-4-event-graphs-in-event-driven-systems-47347a2574d6
- https://medium.com/@picospitfire/eventdrivenarchitecture-part-3-4-event-graphs-as-a-decoupling-layer-1210fab3ef90
- https://medium.com/@picospitfire/eventdrivenarchitecture-part-4-4-how-event-graphs-change-the-role-of-qa-and-test-automation-479a9947f29b
- https://medium.com/@picospitfire/eventdrivenarchitecture-part-5-6-declaring-causality-upstream-not-overhead-but-an-92375df82055
- https://medium.com/@picospitfire/eventdrivenarchitecture-part-6-6-the-minimal-event-contract-58e7cf9fc4fe
- https://medium.com/@picospitfire/testing-data-engineering-pipelines-c2e6afdf6beb
- https://medium.com/@picospitfire/preserving-data-integrity-across-data-transformation-steps-782af4c016b1
- https://medium.com/@picospitfire/a-project-structure-that-makes-pipelines-readable-testable-and-boring-in-the-best-way-f581147c328f
