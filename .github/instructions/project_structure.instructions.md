## Project Structure Rule (Mandatory)

Organize code by **use case**, not by technical component.

- One use case = one directory
- A use case directory contains the full story:
  - pipeline / orchestration
  - domain rules / invariants
  - adapters
  - tests
  - implementation guide

Shared utilities live in a separate, dependency-free `common` module.
