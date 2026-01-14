"""Generate an Enrollment fixture with 2 entities: one canonical, one missing+cascaded.

Goal: a simple "chaos" example where an upstream event is missing and therefore
all downstream dependent events are missing too.

- enrollment-1: canonical (all 13 event types, 14 edges)
- enrollment-2: missing PaymentConfirmed, cascades to keep only the upstream chain

Outputs:
- JSON events file
- causality rules text file used for materialization

Usage:
  ./.venv/bin/python scripts/generate_enrollment_fixture_one_missing.py \
    --seed 20260113 \
    --out-events test/fixtures/events/enrollment_events_one_missing.json \
    --out-rules test/fixtures/events/enrollment_events_one_missing.rules.txt
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC_DIR = _REPO_ROOT / "src"
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=20260113)
    parser.add_argument(
        "--out-events",
        default="test/fixtures/events/enrollment_events_one_missing.json",
    )
    parser.add_argument(
        "--out-rules",
        default="test/fixtures/events/enrollment_events_one_missing.rules.txt",
    )
    args = parser.parse_args()

    from event_generator import generate_causality_rules_text, materialize_events_from_causality_rules_text
    from helpers.enrollment.causality_rules import parse_causality_rules_text, format_causality_rules_text

    seed: int = args.seed

    run_id = f"fixture:one-missing:seed={seed}"
    rules_text = generate_causality_rules_text(seed=seed, entity_count=2, inconsistency_rate=0.0, run_id=run_id)
    rules = parse_causality_rules_text(rules_text)

    good_entity = "enrollment-1"
    bad_entity = "enrollment-2"

    # Remove one upstream type from the bad entity and cascade by removing all
    # edges and types that reference missing types.
    missing_type = "PaymentConfirmed"

    bad_types = set(rules.types_by_entity.get(bad_entity, []))
    bad_types.discard(missing_type)

    # Cascade: iteratively remove any child that has a parent not present.
    # (This mirrors the canonical dependency direction.)
    bad_edges = set(rules.edges_by_entity.get(bad_entity, []))

    changed = True
    while changed:
        changed = False

        # Drop edges referencing missing nodes.
        bad_edges = {(p, c) for (p, c) in bad_edges if p in bad_types and c in bad_types}

        # If a node has a canonical parent edge in the rules but that parent is missing,
        # it would have been removed already via edge filtering. Now remove nodes that
        # have become unreachable by losing their incoming edges (except root).
        # We keep this simple: require that every non-root node has at least one incoming edge.
        incoming = {c for (_p, c) in bad_edges}
        for t in list(bad_types):
            if t == "CourseEnrollmentRequested":
                continue
            if t not in incoming:
                bad_types.remove(t)
                changed = True

    new_types_by_entity = dict(rules.types_by_entity)
    new_edges_by_entity = dict(rules.edges_by_entity)
    new_types_by_entity[bad_entity] = sorted(bad_types)
    new_edges_by_entity[bad_entity] = sorted(bad_edges)

    new_rules = rules.__class__(
        run_id=rules.run_id,
        types_by_entity=new_types_by_entity,
        edges_by_entity=new_edges_by_entity,
    )

    out_rules = Path(args.out_rules)
    out_rules.parent.mkdir(parents=True, exist_ok=True)
    out_rules.write_text(format_causality_rules_text(new_rules), encoding="utf-8")

    events = materialize_events_from_causality_rules_text(
        rules_text=format_causality_rules_text(new_rules),
        seed=seed,
    )

    out_events = Path(args.out_events)
    out_events.parent.mkdir(parents=True, exist_ok=True)
    out_events.write_text(json.dumps(events, indent=2), encoding="utf-8")

    print(f"run_id={run_id}")
    print(f"out_events={out_events} event_count={len(events)}")
    print(f"good_entity={good_entity} bad_entity={bad_entity} missing_type={missing_type}")
    print(f"out_rules={out_rules}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
