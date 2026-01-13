from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

try:
	from event_generator import generate_events_file
except ModuleNotFoundError as e:
	raise SystemExit(
		f"Missing dependency while importing the event generator: {e}.\n\n"
		"Run this script with the project venv instead:\n"
		"  ./.venv/bin/python scripts/generate_event_generator_fixture.py --out test/fixtures/synth/events.ndjson\n"
	)


def main(argv: list[str] | None = None) -> int:
	parser = argparse.ArgumentParser(
		description="Generate a synthetic events fixture using src/event_generator.py"
	)
	parser.add_argument(
		"--out",
		required=True,
		help="Output file path (e.g. test/fixtures/synth/events.ndjson)",
	)
	parser.add_argument(
		"--format",
		choices=("ndjson", "json"),
		default="ndjson",
		help="File format to write",
	)
	parser.add_argument(
		"--seed",
		type=int,
		default=123,
		help="Random seed for deterministic output",
	)
	parser.add_argument(
		"--entity-count",
		type=int,
		default=1,
		help="Number of enrollments (entities) to generate",
	)
	parser.add_argument(
		"--inconsistency-rate",
		type=float,
		default=0.0,
		help="Rate in [0.0, 1.0] controlling incorrect/missing decision declarations",
	)
	args = parser.parse_args(argv)

	output_path = Path(args.out)
	generate_events_file(
		path=output_path,
		format=args.format,
		seed=args.seed,
		entity_count=args.entity_count,
		inconsistency_rate=args.inconsistency_rate,
	)

	print(f"wrote fixture: {output_path} (format={args.format}, seed={args.seed})")
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
