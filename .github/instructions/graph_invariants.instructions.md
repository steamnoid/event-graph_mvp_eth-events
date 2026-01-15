# Graph invariants — extracted from prototype event generator instructions

Source: `__prototype/.github/instructions/events_generator.instructions.md`

This document extracts only the content needed to define **graph invariants** for validation/analytics.

## 1. Graph model

Treat the event stream as a directed graph:

- **Node** = one event
- **Edge** = for each `parent_event_id` in `parent_event_ids`, create a directed edge:
  `parent_event_id -> event_id`

Ordering in files/streams is irrelevant; invariants are graph-structural.

## 2. Event schema invariants (MUST hold for all modes)

Every event MUST contain:

- `event_id` (unique)
- `event_type`
- `event_kind` ∈ `{fact, decision}`
- `parent_event_ids` (list; can be empty)
- `layer`
- `entity_id`
- `payload`
- `emitted_at` (informational only)

Additional schema requirements:

- `event_id` MUST be globally unique within a dataset/file.
- `parent_event_ids` MUST be a list (possibly empty) of IDs (string-coercible).
- The graph MUST NOT rely on timestamps (`emitted_at`) or file order.

## 3. Event taxonomy invariants (MUST hold for all modes)

### 3.1 Fact events

- `event_kind == "fact"`
- Must have **0 or 1 parent** (i.e., `len(parent_event_ids) <= 1`).
- Facts must NOT encode business decisions.

### 3.2 Decision events

- `event_kind == "decision"`
- MAY have **multiple parents**.
- Parents represent **declared semantic reasons**.
- Downstream validation MUST NOT infer/compute missing parents.

## 4. Authoritative event catalog invariants (MUST hold for all modes)

Event types are restricted to the catalog below, including their layers and parent cardinality constraints.

### 4.1 Fact event types

| event_type | layer | max parents |
|---|---:|---:|
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

### 4.2 Decision event types

| event_type | layer | parent cardinality |
|---|---:|---|
| AccessGranted | L6 | multiple |
| EnrollmentCompleted | L7 | 1 (AccessGranted) |

## 5. Causality declaration invariants

### 5.1 Core rule

Applications declare causality where decisions are made.
Analytics/validation validates declarations; it does not reproduce business logic.

### 5.2 Canonical causal graph (reference model)

When validation is performed in **consistent mode** (see §6), the following declared structure is the reference model:

- `CourseEnrollmentRequested` (fact) has no parents
- Chain: `PaymentProcessingRequested` -> `PaymentConfirmed`
- Parallel facts: `UserProfileLoaded` and `CourseAccessRequested`
- `EligibilityChecked` -> `EligibilityPassed`
- `ContentAvailabilityChecked` -> `ContentAvailable` -> `ContentPrepared`
- `AccessGranted` (decision) parents MUST include:
  - `PaymentConfirmed`
  - `EligibilityPassed`
  - `ContentPrepared`
- `EnrollmentCompleted` (decision) parent MUST be:
  - `AccessGranted`
- `EnrollmentArchived` (fact) parent MUST be:
  - `EnrollmentCompleted`

Notes:
- Multi-parent is first-class only for decision events.
- This is not a workflow; do not infer missing edges.

## 6. Inconsistency model (how invariants change by mode)

A generator/stream may be parameterized by:

- `inconsistency_rate ∈ [0.0, 1.0]`

Interpretation: it controls **declaration correctness**, not business logic.

### 6.1 Consistent mode (`inconsistency_rate = 0.0`)

Validation invariants MUST enforce:

- All schema + taxonomy + catalog invariants (§2–§4)
- Decision events declare the correct parent relationships as per the canonical reference model (§5.2)

### 6.2 Inconsistent mode (`inconsistency_rate > 0.0`)

Validation invariants MUST still enforce:

- Schema + taxonomy + catalog invariants (§2–§4)

But MUST allow declaration-level anomalies, including:

- decision events with missing parents
- decision events with incorrect parents
- omitted decision events
- fact events that never participate in decisions

And MUST NOT:

- repair/auto-correct inconsistencies
- infer/compute “true” parents

## 7. Forbidden validation behaviors

Validation MUST NOT:

- reconstruct or infer business rules
- enforce correctness beyond the declared mode (consistent vs inconsistent)
- rely on timestamps or ordering
- convert the graph into a workflow
- invent new event types
