"""Neo4j store stage package.

Keep this module intentionally lightweight.

Why: importing submodules like `dag_helpers.store_data.neo4j.adapter` causes Python
to execute this package `__init__` first. Eager re-exports can create fragile
import-time coupling and surprising circular import errors.

Import what you need directly from:
- `dag_helpers.store_data.neo4j.task`
- `dag_helpers.store_data.neo4j.adapter`
- `dag_helpers.store_data.neo4j.canonical_baseline_helper`
"""

__all__: list[str] = []
