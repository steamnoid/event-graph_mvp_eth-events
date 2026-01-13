# PYTHON DOCUMENTATION RULES (DOCSTRINGS & GENERATED DOCS)

## Objective

Provide **clear, minimal, human-readable documentation** for Python code
using docstrings that can be **automatically converted into documentation**.

Documentation must support:
- non-native English speakers
- junior developers
- quick orientation in ETL pipelines

This is documentation, not a tutorial.

---

## Docstring Style (Mandatory)

- Use **Google-style docstrings**
- One short summary sentence is mandatory
- Use **simple, plain English**
- Avoid technical jargon unless it is a domain term

---

## What Docstrings SHOULD Describe

Docstrings should explain:

- **What the function does in the domain**
- **Why the function exists**
- **Which invariant or responsibility it owns** (if any)

Good:
```python
def validate_graph(...):
    """Ensure that causality rules remain invariant after graph creation."""
```

---

## What Docstrings MUST NOT Describe

Do NOT describe:

- control flow
- implementation steps
- loops, conditions, or algorithms
- file formats or serialization details
- obvious information already present in the name

Bad:
```python
"""Read a file, compare strings, and raise an error."""
```

---

## Args and Returns

- Include `Args` and `Returns` ONLY when the meaning is not obvious
- Do NOT restate type hints
- Keep descriptions short and concrete

Example:
```python
def load_events(source: str) -> list[Event]:
    """Load enrollment events from an external source.

    Args:
        source: Location of the raw enrollment events.

    Returns:
        A list of enrollment events.
    """
```

---

## Error Documentation

- Document errors only if they represent **domain violations**
- Describe **what invariant is violated**, not how the error is raised

Good:
```python
Raises:
    ValueError: If rules meaning diverges after normalization.
```

---

## Language Rules

- English only
- Child-readable when possible
- Short sentences
- No idioms, metaphors, or jokes

If a sentence needs explanation, simplify it.

---

## Placement Rules

- Every public function MUST have a docstring
- Private helpers (`_helper`) MAY omit docstrings unless non-obvious
- Module-level docstring is RECOMMENDED for pipelines

---

## Documentation Generation

Docstrings MUST be compatible with automatic documentation tools.

Recommended tools:
- **pdoc** (simple, zero-config)
- **Sphinx** with `napoleon` extension (formal)

Docstrings must stand on their own without extra prose.

---

## Review Checklist (Mandatory)

Before accepting documentation:

- [ ] Summary sentence explains intent
- [ ] Language is simple and clear
- [ ] No implementation details are described
- [ ] Docstring adds information not obvious from the name
- [ ] Documentation can be read without reading the code

---

## Meta-Rule

> **If the code is the truth, the docstring is the signpost.**
> It points the reader in the right direction and then gets out of the way.
