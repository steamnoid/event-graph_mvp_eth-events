# instructions_unified.md — AUTHORITATIVE
# AUTONOMOUS EVENT GENERATOR WITH DECLARED CAUSALITY (MANDATORY)

## 0. Status

This document is the **single authoritative instruction file** for GitHub Copilot
for the synthetic event generator.

All previous generator-related instruction files are superseded by this one.

Copilot MUST:
- follow this document strictly
- implement the generator autonomously (end-to-end)
- NOT ask the human for clarifications or options
- choose reasonable defaults when multiple valid choices exist
- NOT use strict TDD for the generator implementation
- add functional tests at the end (see Section 15)
- NOT simplify the model
- NOT introduce additional logic

If a build/run issue blocks progress (e.g., missing toolchain), Copilot MUST
attempt to resolve it autonomously; only if genuinely blocked may it ask the
human for an environment action.

---

## 1. Purpose

Build a **synthetic event generator** that simulates a real,
event-driven production system where:

- systems emit **facts**
- systems emit **decisions**
- **causality is explicitly declared by upstream applications**
- multi-parent relationships are first-class
- downstream systems do NOT reconstruct business logic

The generator is a **source of truth simulation**, not an analytical system.

---

## 2. Core architectural principle (NON-NEGOTIABLE)

> **Applications declare causality where decisions are made.  
> Analytics validates declarations. It does not reproduce logic.**

This principle MUST be preserved in all code.

---

## 3. Domain overview (human-readable)

Domain: **Online Learning Platform – Course Enrollment**

A user enrolls in a course.
Independent subsystems emit events:

- payment
- eligibility
- content preparation
- access provisioning

Some events are **facts**.
Some events are **decisions triggered by business logic**.

Decision events MAY depend on multiple upstream facts.
Those dependencies MUST be explicitly declared by the application.

---

## 4. Event taxonomy (MANDATORY)

Each event MUST declare:

```
event_kind ∈ { fact, decision }
```

### 4.1 Fact events

- represent observed state or completed work
- have **0 or 1 parent**
- do NOT encode business decisions

### 4.2 Decision events

- represent execution of business logic
- MAY have **multiple parents**
- parents represent **declared semantic reasons**
- generator MUST NOT infer or compute these parents

---

## 5. Canonical causal graph (REFERENCE MODEL)

This graph defines the **intended semantic structure**.
Copilot MUST align generation to this shape,
but MUST NOT enforce correctness beyond declaration.

```
[L0] CourseEnrollmentRequested (fact)
        │
[L1] PaymentProcessingRequested (fact)
        │
[L2] PaymentConfirmed (fact)
        │
        ├──────────────┐
        ▼              ▼
[L3] UserProfileLoaded (fact)   CourseAccessRequested (fact)
        │                          │
[L4] EligibilityChecked (fact)   ContentAvailabilityChecked (fact)
        │                          │
[L5] EligibilityPassed (fact)    ContentAvailable (fact)
        │                          │
        └──────────────┬───────────┘
                       ▼
[L6] ContentPrepared (fact)
                       │
        ┌──────────────┴─────────────────────────┐
        ▼                                        ▼
[L6] AccessGranted (decision)
     parents:
       - PaymentConfirmed
       - EligibilityPassed
       - ContentPrepared

[L7] EnrollmentCompleted (decision)
     parents:
       - AccessGranted

[L8] EnrollmentArchived (fact)
        parent:
          - EnrollmentCompleted
```

Notes:
- Decision events explicitly declare multi-parent causality
- Fact events remain local and single-parent
- Graph is NOT a workflow

---

## 6. Authoritative event catalog

### 6.1 Fact events

| Event type | Layer | Max parents |
|-----------|-------|-------------|
| CourseEnrollmentRequested | L0 | 0 |
| PaymentProcessingRequested | L1 | 1 |
| PaymentConfirmed | L2 | 1 |
| UserProfileLoaded | L3 | 1 |
| CourseAccessRequested | L3 | 1 |
| EligibilityChecked | L4 | 1 |
| ContentAvailabilityChecked | L4 | 1 |
| EligibilityPassed | L5 | 1 |
| ContentAvailable | L5 | 1 |
| ContentPrepared | L6 | 1 |
| EnrollmentArchived | L8 | 1 |

### 6.2 Decision events

| Event type | Layer | Parent cardinality |
|-----------|-------|--------------------|
| AccessGranted | L6 | multiple |
| EnrollmentCompleted | L7 | 1 (AccessGranted) |

---

## 7. Generator responsibilities (CRITICAL)

Copilot MUST implement a generator that:

- emits BOTH fact and decision events
- NEVER infers business logic
- ONLY declares causality explicitly on events
- supports deterministic output via random seed
- emits events in arbitrary order
- supports multiple entities (enrollment_id)

The generator MUST treat decision parents as:
> **declared metadata, not computed truth**

### 7.1 Autonomy rule (NON-NEGOTIABLE)

Copilot MUST implement the full generator without asking questions.

- If the spec allows multiple representations (e.g., JSON vs NDJSON), Copilot MUST
        implement both where required, picking safe defaults.
- If an interface detail is unspecified, Copilot MUST choose a minimal, stdlib-only
        shape that satisfies the required outputs and stays deterministic under `seed`.

---

## 8. Inconsistency model

The generator MUST support:

```
inconsistency_rate ∈ [0.0, 1.0]
```

This parameter controls **declaration correctness**, not logic.

### With inconsistency_rate = 0.0
- all decision events declare correct, semantically valid parents
- stream represents an ideal, healthy system

### With inconsistency_rate > 0.0
Generator MAY:
- emit decision events with missing parents
- emit decision events with incorrect parents
- omit decision events entirely
- emit fact events that never participate in decisions

The generator MUST NOT repair or correct inconsistencies.

---

## 9. Output modes (MANDATORY)

The generator MUST support BOTH:

### 9.1 File output (batch)
- write events to file (JSON or NDJSON)
- closed, replayable batch

### 9.2 Stream output
- emit events incrementally
- no ordering guarantees
- abstraction-level streaming (iterator, callback, stdout)

---

## 10. Event schema (MANDATORY)

Each event MUST contain:

```
event_id              # unique
event_type
event_kind            # fact | decision
parent_event_ids      # empty, 1, or many
layer
entity_id
payload
emitted_at            # informational only
```

---

## 11. Forbidden behaviors

Copilot MUST NOT:

- reconstruct or infer business rules
- enforce correctness
- validate conditions
- collapse layers
- convert graph into workflow
- rely on timestamps or ordering
- invent new event types

---

## 12. Implementation constraints

- Language: Python
- Minimal dependencies (stdlib preferred)
- In-memory generation
- Single public API exposing:
  - batch generation
  - streaming generation

---

## 15. Testing mode (MANDATORY)

This generator MUST be implemented without strict TDD.

After the implementation is complete, Copilot MUST add **functional tests** that validate
the generator end-to-end as a black box.

### 15.1 Functional test requirements

- Tests MUST be deterministic (use explicit `seed`).
- Tests MUST validate the **event schema** and **event taxonomy** (`event_kind`).
- Tests MUST validate the **canonical causal graph shape** is representable by declarations
        (i.e., decision events declare the correct parent relationships when `inconsistency_rate = 0.0`).
- Tests MUST validate the generator supports **multiple entities** (`entity_id`).
- Tests MUST validate both output modes:
        - batch mode (returns a full collection)
        - stream mode (yields incrementally)
- Tests MUST NOT infer business logic; they only check that declarations match the requested mode
        (consistent vs inconsistent).

### 15.2 Test placement and markers

- Functional tests MUST live under `test/functional/`.
- Functional tests MUST be marked with `@pytest.mark.functional`.
- Existing unit tests under `test/unit/` may remain; functional tests are additive.

### 15.3 Suggested functional test file

- `test/functional/test_event_generator_functional.py`

### 15.4 Suggested commands

- Run only functional tests: `python -m pytest -q -m functional`
- Run generator tests: `python -m pytest -q test/unit/test_event_generator_unit.py test/functional/test_event_generator_functional.py`

---

## 13. Guiding sentence

> **Upstream declares causality.  
> Downstream validates declarations.**

---

## 14. End of document

This document is **binding and final**.
