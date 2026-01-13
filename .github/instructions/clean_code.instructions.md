# CANONICAL CLEAN CODE ETL RULES (COPILOT)

## 1. Objective

Refactor code into a **Clean Code, ETL-oriented structure** that is:

- behavior-preserving
- easy to read top-down
- understandable for **non-native English speakers**

Do **NOT** rewrite logic. Do **NOT** change behavior.

---

## 2. Absolute Rules (Non-Negotiable)

- Do NOT change business logic
- Do NOT change observable behavior
- Do NOT change tests or assertions
- Do NOT add features or “improvements”
- If behavior is unclear → preserve it exactly

**Tests define the contract.**

Exception (narrow): a rename-only refactor MAY update tests to keep them compiling.

- Allowed: mechanical updates to imports and symbol names.
- Forbidden: changing expectations, assertions, fixture content, or test coverage scope.

---

## 3. Structural Rules (Clean Code)

### Stepdown Rule

- High-level functions call lower-level functions
- Each level is **one level of abstraction**
- Code must read **top-down like a story**

Stepdown is about **abstraction**, not call depth.

### One Level of Abstraction

- Do NOT mix domain decisions with technical details
- Each function answers **one question**

---

## 4. ETL Semantics (Mandatory)

### Canonical ETL Shape

```python
def run_pipeline():
    raw = extract()
    data = transform(raw)
    load(data)
```

### Transform Rules

- Transform steps may be **pure functions**
- Narrative composition `f(g(h(x)))` is allowed ONLY if:
  - no side effects
  - no branching
  - no error handling
  - reads like a sentence

---

## 5. Forbidden Patterns

- Mechanical call chains (`step1 → step2 → step3`)
- Hidden control flow inside helpers
- Pipelines hidden in the call stack

---

## 6. Language Rules (Critical)

### English-Only

- All names, comments, messages: **English only**
- No mixed languages
- No abbreviations that hide meaning

---

### Simple English (Non-Native Friendly)

Prefer **plain, common verbs**.

### Child-Readable Rule (Mandatory)

Write code so that a child could understand the intent from names and top-down flow.

If a name or a function needs explanation, it is wrong.

Apply the same rule to error messages:
- they should be simple and concrete
- they should explain the violated invariant in plain English
- if a test matches a specific substring/regex, keep that part unchanged

**Prefer:**  
get, load, save, read, write, build, create, check, copy, fail_if

**Avoid unless necessary:**  
ensure, resolve, obtain, materialize, assert, derive, validate

### Naming Consistency (Mandatory)

- Choose one term per concept and use it everywhere.
- Prefer short, idiomatic English.
- For domain-relative time language, prefer **prior** over **previous/current**.
  - Use **previous/current** only when domain meaning would become unclear.

---

### Name by Actual Behavior

Names MUST describe **what the function really does**.

Bad:
```python
ensure_run_artifact_directory()
```

Good:
```python
get_run_directory()
```

---

## 7. Domain vs Pipeline Language

- **Pipeline / orchestration code** may use technical verbs  
  (`load`, `write`, `copy`)
- **Domain / invariant code** must describe **meaning**, not mechanics

Avoid leaking:
file, path, text, format, comparison mechanics

### Boundary Exception (I/O Modules)

The “avoid leaking file/path/text” rule applies to **domain / invariant code**.

At I/O boundaries (for example adapters and artifact utilities), it is acceptable and often correct
to use technical vocabulary such as:

- file, path
- read, write
- json, text

Rule of thumb:
- Domain modules describe meaning and invariants.
- Boundary modules describe I/O mechanics.

### Public API vs Private Helpers

- Public pipeline entrypoints may keep explicit artifact language (for example `events_file`, `graph_file`) when it is part of the integration contract.
- Private helpers should prefer domain language and avoid I/O mechanics in names.

---

## 8. Error Semantics

Errors must explain:
- **what invariant was violated**
- **after which transformation**

Bad:
```
C3 != C4
```

Good:
```
Rules meaning diverged after graph materialization.
```

### Error Messages as Contract

Tests define observable behavior.

If a test matches an error message (for example `pytest.raises(..., match=...)`),
the message text is part of the contract and MUST remain compatible.

---

## 9. Final Checklist (Must Pass)

- [ ] Behavior unchanged
- [ ] Tests behavior unchanged and passing
- [ ] Flow readable top-down
- [ ] Names understandable for non-native English speakers
- [ ] No abstract or “legal-sounding” verbs
- [ ] Code reads like a process description

---

## Final Meta-Rule

If a name needs explanation, it is wrong.  
Prefer clarity over elegance.
