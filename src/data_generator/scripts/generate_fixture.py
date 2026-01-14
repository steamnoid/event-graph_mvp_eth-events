"""General-purpose fixture generator for the `data_generator` package.

This script exposes the same high-level knobs as the generator module:
- `seed`
- `entity_count`
- `inconsistency_rate`
- `missing_event_rate`
- `run_id`

It generates causality-rules text first (optionally writes it), then materializes
JSON events and writes them as JSON array or NDJSON.

Usage:
    # Recommended (no PYTHONPATH needed):
    ./.venv/bin/python src/data_generator/scripts/generate_fixture.py \
    --seed 20260113 \
    --entity-count 2 \
    --inconsistency-rate 0.2 \
    --missing-event-rate 0.1 \
    --run-id "fixture:custom" \
    --out-events src/data_generator/fixtures/events/enrollment_events.json \
    --format json \
    --out-rules src/data_generator/fixtures/events/enrollment_events.rules.txt

    # Alternative:
    PYTHONPATH=src ./.venv/bin/python -m data_generator.scripts.generate_fixture --help
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[3]
_SRC_DIR = _REPO_ROOT / "src"
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate synthetic fixtures via data_generator")

    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--entity-count", type=int, default=1)
    parser.add_argument("--inconsistency-rate", type=float, default=0.0)
    parser.add_argument("--missing-event-rate", type=float, default=0.0)
    parser.add_argument("--run-id", type=str, default=None)

    parser.add_argument(
        "--out-events",
        type=str,
        default="src/data_generator/fixtures/events/enrollment_events.ndjson",
        help="Output path for events. Extension does not control format; use --format.",
    )
    parser.add_argument(
        "--format",
        choices=["json", "ndjson"],
        default="ndjson",
        help="Output format for events file.",
    )
    parser.add_argument(
        "--out-rules",
        type=str,
        default=None,
        help="Optional output path for causality rules text.",
    )

    args = parser.parse_args()

    from data_generator.generator import (
        generate_causality_rules_text,
        materialize_events_from_causality_rules_text,
        write_events_file,
    )

    rules_text = generate_causality_rules_text(
        seed=args.seed,
        entity_count=args.entity_count,
        inconsistency_rate=args.inconsistency_rate,
        missing_event_rate=args.missing_event_rate,
        run_id=args.run_id,
    )

    if args.out_rules:
        out_rules = Path(args.out_rules)
        out_rules.parent.mkdir(parents=True, exist_ok=True)
        out_rules.write_text(rules_text, encoding="utf-8")

    events = materialize_events_from_causality_rules_text(rules_text=rules_text, seed=args.seed)

    out_events = write_events_file(
        events=events,
        path=args.out_events,
        format=args.format,
    )

    print(f"out_events={out_events} event_count={len(events)}")
    if args.out_rules:
        print(f"out_rules={args.out_rules}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
