# USE-CASE–CENTRIC PROJECT STRUCTURE (COPILOT INSTRUCTIONS)

## Objective

Organize the project structure so that **a human can understand the system
by reading directories and filenames alone**.

The structure must support:
- ETL pipelines (Airflow DAGs)
- AI-generated code
- non-native English readers
- narrative understanding of data flow

This is a **structural refactor**, not a logic change.

---

## Core Principle (Non-Negotiable)

> **Organize code by what the system DOES, not by what it is MADE OF.**

This means:
- use-case centric, not component-centric
- meaning before mechanics
- intention before order

---

## Top-Level Rule

Each **use case** gets its own directory.

A use case is:
- one complete data flow
- one business or system story
- one DAG or closely related set of DAGs

Bad:
```
validators/
transformers/
adapters/
```

Good:
```
enrollment/
settlement/
analytics/
```

---

## DAG Helpers Structure (Mandatory)

For Airflow DAGs, use a dedicated helpers directory:

```
dag_helpers/
  <use_case>/
    <task_name>/
      task.py
      implementation_guide.md (optional, for complex tasks)
      tests/
```

### Rules

- One DAG task = one directory
- Directory name describes **what the task does**
- NEVER use numeric names (`task_1`, `stage_2`)
- Order and dependencies live ONLY in the DAG

---

## Task Naming Rules

Task directories must:
- use verbs + nouns (`fetch_events`, `build_graph`)
- describe intent, not order
- remain valid even if DAG order changes

Bad:
```
task_1/
stage_2/
```

Good:
```
fetch_events/
normalize_events/
build_edges/
validate_rules/
```

---

## What Goes Inside a Task Directory

```
build_edges/
  task.py          # Airflow operator / entrypoint
  logic.py         # Core logic (pure if possible)
  adapters.py      # I/O or integration helpers
  tests/
```

### Rules

- `task.py` wires inputs/outputs
- Logic lives outside `task.py`
- Tests live next to the task
- Avoid cross-task imports

---

## Pipeline vs Domain Separation

- Pipeline code describes **flow and orchestration**
- Domain code describes **meaning and invariants**
- Do NOT mix them

Example:
- `pipeline.py` → orchestration
- `validator.py` → domain truth

---

## Ordering Inside Files (Mandatory)

Inside each file, order code top-down:

1. Public entrypoints (highest abstraction)
2. Public stage-level functions
3. Domain logic / invariants
4. Private helpers (prefixed with `_`) at the bottom

The file must read like a story.

---

## Shared Code Rules

### Use-Case Local Sharing

Code shared only within one use case goes into:

```
<use_case>/shared/
```

### Global Sharing

Code shared across use cases goes into:

```
common/
```

Rules:
- `common` must not depend on use cases
- Use cases may depend on `common`

---

## Documentation Placement

Each use case SHOULD include:

```
implementation_guide.md
```

This file:
- explains WHY the use case exists
- describes the high-level flow
- lists key invariants
- tells where to start reading code

This guide restores human understanding when AI generates code.

---

## Forbidden Anti-Patterns

- Numeric directories (`task_1`, `stage_3`)
- Component-only grouping (`utils/`, `helpers/` without meaning)
- Cross-task logic imports
- Business logic inside DAG files

---

## Final Check (Mandatory)

Before accepting structural changes, verify:

- [ ] Each directory name explains its purpose
- [ ] A new developer can find a task without reading code
- [ ] DAG order is not duplicated in directory names
- [ ] AI-generated code stays inside the correct task folder

---

## Meta-Rule

> **If you need to open many directories to understand one thing,
> the structure is wrong.**

# TEST PLACEMENT RULES (COPILOT ADDENDUM)

## Objective

Ensure that tests support **human understanding, locality, and control**
when working with AI-generated code.

Tests are treated as **executable documentation** of behavior and invariants.

---

## Core Rule (Mandatory)

> **Tests must live next to the code they test.**

This maximizes:
- readability
- discoverability
- correctness during refactors
- AI awareness of behavioral contracts

---

## Local Tests (Required)

Unit-level and task-level tests MUST be colocated with the code.

Example:

```
build_edges/
  task.py
  logic.py
  tests/
    test_logic.py
    test_task.py
```

Rules:
- Each task directory MUST contain a `tests/` folder.
- Tests should cover behavior and invariants of that task only.
- Tests must not rely on other tasks unless explicitly integration-level.

---

## Central Tests (Allowed)

Integration, cross-task, and end-to-end tests MAY live in a central location.

Example:

```
tests/
  integration/
    test_enrollment_pipeline.py
```

Use central tests for:
- full DAG execution
- multi-task interactions
- system-wide invariants

---

## What NOT to Do

- Do NOT put all tests in a single global `tests/` folder.
- Do NOT separate tests from code purely for convention.
- Do NOT hide tests far away from the logic they validate.

---

## Naming Rules

- Test filenames must describe the behavior being tested.
- Avoid generic names like `test_utils.py`.
- Prefer names that read like sentences.

Good:
```
test_fails_when_rules_diverge.py
```

Bad:
```
test_helpers.py
```

---

## AI-Specific Rule

When refactoring code with AI assistance:

- Tests MUST be visible in the same directory context.
- If a test fails, behavior MUST be preserved.
- Renames MAY update imports, but not expectations.

---

## Review Checklist (Mandatory)

Before accepting changes:

- [ ] Tests are colocated with the code they test
- [ ] Each task has its own `tests/` directory
- [ ] Integration tests are clearly separated
- [ ] Tests read as behavior descriptions

---

## Meta-Rule

> **If a test is not easy to find, it will not protect you.**
