"""Generate an Enrollment fixture with 2 entities: one canonical, one faulty.

- Entity 1: canonical declared causality (p=0.0)
- Entity 2: deliberately corrupted declared causality (still self-consistent)

This is useful for Neo4j visualization: you can compare a correct subgraph vs a bad one.

Outputs:
- JSON events file
- (optional) causality rules text file used for materialization

Usage:
  ./.venv/bin/python scripts/generate_enrollment_fixture_one_bad.py \
    --seed 20260113 \
    --out-events test/fixtures/events/enrollment_events_one_bad.json \
    --out-rules test/fixtures/events/enrollment_events_one_bad.rules.txt
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
    parser.add_argument("--out-events", default="test/fixtures/events/enrollment_events_one_bad.json")
    parser.add_argument("--out-rules", default="test/fixtures/events/enrollment_events_one_bad.rules.txt")
    args = parser.parse_args()

    from event_generator import generate_causality_rules_text, materialize_events_from_causality_rules_text
    from helpers.enrollment.causality_rules import parse_causality_rules_text, format_causality_rules_text

    seed: int = args.seed

    # Start from a fully-canonical rules text.
    run_id = f"fixture:one-bad:seed={seed}"
    rules_text = generate_causality_rules_text(seed=seed, entity_count=2, inconsistency_rate=0.0, run_id=run_id)
    rules = parse_causality_rules_text(rules_text)

    good_entity = "enrollment-1"
    bad_entity = "enrollment-2"

    # Corrupt ONLY the bad entity's declared edges (do not touch types).
    # 1) Remove two parents of AccessGranted (missing parents)
    # 2) Make EnrollmentCompleted depend on PaymentConfirmed instead of AccessGranted (wrong parent)
    edges = list(rules.edges_by_entity.get(bad_entity, []))

    def drop(pair: tuple[str, str]) -> None:
        nonlocal edges
        edges = [e for e in edges if e != pair]

    def replace(old: tuple[str, str], new: tuple[str, str]) -> None:
        nonlocal edges
        edges = [e for e in edges if e != old]
        edges.append(new)

    drop(("EligibilityPassed", "AccessGranted"))
    drop(("ContentPrepared", "AccessGranted"))
    replace(("AccessGranted", "EnrollmentCompleted"), ("PaymentConfirmed", "EnrollmentCompleted"))

    # Write back.
    new_edges_by_entity = dict(rules.edges_by_entity)
    new_edges_by_entity[bad_entity] = sorted(set(edges))

    new_rules = rules.__class__(
        run_id=rules.run_id,
        types_by_entity=rules.types_by_entity,
        edges_by_entity=new_edges_by_entity,
    )

    out_rules = Path(args.out_rules)
    out_rules.parent.mkdir(parents=True, exist_ok=True)
    out_rules.write_text(format_causality_rules_text(new_rules), encoding="utf-8")

    events = materialize_events_from_causality_rules_text(rules_text=format_causality_rules_text(new_rules), seed=seed)

    out_events = Path(args.out_events)
    out_events.parent.mkdir(parents=True, exist_ok=True)
    out_events.write_text(json.dumps(events, indent=2), encoding="utf-8")

    # Minimal summary.
    print(f"run_id={run_id}")
    print(f"out_events={out_events} event_count={len(events)}")
    print(f"good_entity={good_entity} bad_entity={bad_entity}")
    print(f"out_rules={out_rules}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
