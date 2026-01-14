"""General-purpose fixture validator for the `data_generator` package.

Validates an events fixture file written as:
- JSON array, or
- NDJSON (one JSON object per line)

It uses `data_generator.fixture_validator`.

Usage:
  ./.venv/bin/python src/data_generator/scripts/validate_fixture.py \
    --events src/data_generator/fixtures/events/enrollment_events.ndjson \
    --mode consistent

Exit codes:
- 0: valid
- 1: invalid
- 2: usage / unexpected error
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
    parser = argparse.ArgumentParser(description="Validate a generated fixture events file")
    parser.add_argument(
        "--events",
        required=True,
        help="Path to events file (JSON array or NDJSON).",
    )
    parser.add_argument(
        "--mode",
        choices=["consistent", "inconsistent"],
        default="consistent",
        help="Validation strictness. 'consistent' enforces canonical types/parents; 'inconsistent' checks only schema + references.",
    )
    args = parser.parse_args()

    try:
        from data_generator.fixture_validator import validate_events_file

        result = validate_events_file(args.events, mode=args.mode)

        if result.is_valid:
            print(f"OK: {args.events}")
            return 0

        print(f"INVALID: {args.events}")
        for err in result.errors:
            print(f"- {err}")
        return 1
    except Exception as e:
        print(f"ERROR: {e}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
