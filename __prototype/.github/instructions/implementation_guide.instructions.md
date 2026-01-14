# IMPLEMENTATION GUIDE GENERATION (COPILOT INSTRUCTIONS)

## Objective

Generate or update a **Narrative Implementation Guide** that restores human
understanding and control over AI-generated code.

The guide must be readable as a short essay and explain **why the system exists,
what it guarantees, and how to read the code**, without describing line-by-line logic.

This is a **documentation artifact**, not code.

---

## When to Generate or Update

Generate or update the Implementation Guide when:

- significant refactoring occurs
- pipeline stages change
- invariants are added, removed, or redefined
- module boundaries change
- AI-generated code volume increases

Do NOT update the guide for trivial renames or formatting changes.

---

## Output Location

Write or update:

```
docs/implementation_guide.md
```

(or a module-local `implementation_guide.md` next to the code).

The guide must NOT be embedded in code comments or docstrings.

---

## Writing Style (Mandatory)

- Simple, narrative English
- Short paragraphs
- No jargon unless it is a domain term
- No code blocks unless absolutely necessary
- No diagrams required

The guide should be understandable by:
- a non-native English speaker
- a junior developer
- a reasonably smart child

---

## What the Guide MUST Contain

### 1. Why This Module Exists

Explain the problem the module solves and the core idea behind the design.

Example:
- What must stay true while data moves?
- What failure is considered unacceptable?

---

### 2. High-Level Flow (Conceptual)

Describe the pipeline in **numbered steps** using plain language.

- Do NOT list function names
- Do NOT describe control flow
- Describe what *happens to the data*

---

### 3. Key Invariants

List the invariants the system enforces.

Each invariant should be:
- one sentence
- written as a truth that must always hold

---

### 4. Where to Start Reading the Code

Give explicit reading guidance.

Example:
- which file to open first
- which function is the entrypoint
- what to ignore on the first read

---

### 5. What This Module Deliberately Does NOT Do

Clarify non-goals and rejected responsibilities.

This prevents incorrect assumptions.

---

### 6. Design Decisions Worth Knowing

Explain 3–5 intentional choices that affect how the code is structured.

Focus on *why*, not *how*.

---

## What the Guide MUST NOT Contain

- line-by-line explanations
- implementation details
- function-by-function listings
- variable names
- TODOs or speculative ideas
- references to specific libraries unless essential

---

## Update Rules

- The guide must reflect the **current code**
- If the guide and code disagree, the guide must be updated
- Preserve a calm, factual tone
- Do NOT oversell cleverness or complexity

---

## Final Check (Mandatory)

Before finalizing the guide, verify:

- [ ] It can be read in under 10 minutes
- [ ] It explains the system without opening the code
- [ ] It restores a clear mental model of the pipeline
- [ ] It increases confidence rather than confusion

---

## Meta-Rule

> **If the guide feels like documentation, it is too technical.
> If it feels like a story about the system, it is correct.**
