"""Print per-entity graph stats for an Enrollment run stored in Neo4j.

This is a convenience script to confirm that a run contains N entities and that
all per-entity graphs are identical (same type set + same edge set), optionally
matching the canonical spec.

Usage:
  ./.venv/bin/python scripts/neo4j_enrollment_run_stats.py --run-id "..."
  ./.venv/bin/python scripts/neo4j_enrollment_run_stats.py --run-id "..." --expect-canonical
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC_DIR = _REPO_ROOT / "src"
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from enrollment_canonical import canonical_edges, canonical_types
from helpers.enrollment.neo4j_causality_rules import fetch_causality_model_from_neo4j


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--expect-canonical", action="store_true")
    args = parser.parse_args()

    model = fetch_causality_model_from_neo4j(run_id=args.run_id)
    entities = sorted(model.types_by_entity.keys())

    print(f"run_id={model.run_id}")
    print(f"entity_count={len(entities)}")

    if not entities:
        return 1

    type_lists = [tuple(model.types_by_entity[e]) for e in entities]
    edge_lists = [tuple(model.edges_by_entity.get(e, [])) for e in entities]

    type_lens = [len(t) for t in type_lists]
    edge_lens = [len(e) for e in edge_lists]

    print(f"types_per_entity min={min(type_lens)} max={max(type_lens)}")
    print(f"edges_per_entity min={min(edge_lens)} max={max(edge_lens)}")

    distinct_type_sets = len(set(type_lists))
    distinct_edge_sets = len(set(edge_lists))
    print(f"distinct_type_sets={distinct_type_sets}")
    print(f"distinct_edge_sets={distinct_edge_sets}")

    if args.expect_canonical:
        ct = canonical_types()
        ce = canonical_edges()
        all_canonical_types = all(set(model.types_by_entity[e]) == ct for e in entities)
        all_canonical_edges = all(set(model.edges_by_entity.get(e, [])) == ce for e in entities)
        print(f"all_canonical_types={all_canonical_types}")
        print(f"all_canonical_edges={all_canonical_edges}")
        if not all_canonical_types or not all_canonical_edges:
            return 2

    # We expect identical graphs across entities in p=0.0 mode.
    if distinct_type_sets != 1 or distinct_edge_sets != 1:
        return 3

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
