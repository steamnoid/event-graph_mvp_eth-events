# copilot_tdd_mode.md — STRICT AGENT TDD (MANDATORY)

## 0. Mode of work

This project is developed in **STRICT Test-Driven Development (TDD)** mode.

GitHub Copilot operates in **STRICT AGENT MODE** while strictly following the
**Red → Green → Refactor** cycle.

Copilot is allowed to:
- create/edit tests and production code
- run the smallest possible test command(s)
- apply minimal refactors after GREEN

Copilot must still be disciplined: one behavior per cycle, and no “future-proofing”.

---

## 1. Absolute TDD rules (non-negotiable)

Copilot must always follow this exact sequence:

1. **RED:** add exactly one failing test for the next behavior
2. Run the smallest test command that demonstrates the failure
3. **GREEN:** implement the minimal production change to pass
4. Re-run the same test command and confirm it passes
5. **REFACTOR:** apply a small refactor (optional) without changing behavior
6. Re-run tests relevant to the refactor and stop

Copilot must never skip steps.
Copilot must never jump ahead.

---

## 2. Red phase — Test first

In the RED phase, Copilot must:

- describe **what behavior is missing**
- explain **why this behavior matters**
- propose **one and only one test**
- keep the test minimal and focused
- avoid implementation hints inside the test

Copilot must NOT:
- write production code
- suggest multiple tests at once
- refactor anything

Example response format:
- Goal of the test
- Given / When / Then description
- Test name suggestion

---

## 3. Green phase — Minimal implementation

In the GREEN phase, Copilot must:

- suggest the **smallest possible code change**
- pass only the current test (not future ones)
- avoid abstractions
- avoid optimizations
- allow duplication if necessary

Copilot must NOT:
- introduce new features
- refactor proactively
- generalize logic prematurely

The implementation should be intentionally naive.

---

## 4. Refactor phase — Improve without changing behavior

In the REFACTOR phase, Copilot may:

- remove duplication
- improve naming
- clarify intent
- simplify logic

Copilot must ensure:
- all tests still pass
- no new behavior is introduced
- no test logic is changed

Refactor suggestions must be small and reversible.

---

## 5. Scope discipline

Each TDD cycle must focus on **exactly one behavior**, such as:

- generating a valid `event_id`
- classifying an event type
- assigning parents for a Swap
- detecting missing parents
- identifying a graph invariant violation

Copilot must stop after completing one behavior.

---

## 6. Event graph–specific constraints

During TDD cycles, Copilot must never:

- infer causality from timestamps
- use heuristics to guess parents
- collapse multiple events into aggregates
- bypass the event graph abstraction

If a test would violate local causal truth,
Copilot must explicitly warn the human.

---

## 7. Test style guidelines

Tests should be:

- deterministic
- explicit
- readable
- independent
- fast

Preferred testing style:
- Given / When / Then
- One assertion per test (when reasonable)

---

## 8. Copilot interaction contract

Copilot responses must:

- be short and precise
- focus on the current TDD step only
- explain reasoning before suggesting code
- show what command(s) were run and what passed/failed (succinctly)

Copilot must prefer minimal, reversible changes.

---

## 9. Forbidden behaviors

Copilot must NOT:

- write code without a failing test
- refactor during RED or GREEN
- introduce frameworks or libraries without approval
- generate large code blocks
- do multi-behavior batches (only one behavior per cycle)

---

## 10. Guiding principles

> Tests describe behavior, not implementation.  
> Implementation exists only to satisfy tests.  
> Refactoring improves structure, not behavior.

---

## 11. Definition of success

TDD is considered correctly applied if:

- every line of production code is covered by a test
- every behavior was introduced via a failing test
- refactors never introduce new tests
- Copilot never jumps ahead of the human

---

## 12. Status

This document is **normative**.
All Copilot interactions must comply with it.
