from __future__ import annotations

from pathlib import Path

from .transformer import (
	FixtureFormat,
	enhance_events_add_event_name,
	read_events_from_file,
	write_events_to_file,
)

from .canonical_baseline_helper import save_canonical_baseline_artifact


def enhance_data(
	*,
	artifact_dir: str | Path,
	source_events: str | Path,
	out_events: str | Path,
	format: FixtureFormat = "ndjson",

) -> tuple[Path, Path]:
	"""Transform stage: enhance events and emit canonical baselines.

	Returns:
		(candidate_baseline_path, out_events_path)
	"""
	artifact_dir = Path(artifact_dir)
	artifact_dir.mkdir(parents=True, exist_ok=True)

	fixture_data = read_events_from_file(source_events)
	enhanced = enhance_events_add_event_name(fixture_data)

	baseline_path = save_canonical_baseline_artifact(
		events=enhanced,
		path=artifact_dir / "baseline_events.json",
	)

	out_events_path = write_events_to_file(events=enhanced, path=out_events, format=format)

	return baseline_path, out_events_path
