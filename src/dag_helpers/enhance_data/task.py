from __future__ import annotations

from pathlib import Path
from typing import Optional

from dag_helpers.fetch_data.adapter import FixtureFormat, read_fixture_file, write_events_to_file

from .transformer import (
	enhance_events_add_event_name,
	save_post_transformation_canonical_baseline_artifact,
	save_pre_transformation_canonical_baseline_artifact,
)


def enhance_data(
	*,
	artifact_dir: str | Path,
	source_events: str | Path,
	out_events: str | Path,
	format: FixtureFormat = "ndjson",
) -> tuple[Path, Path, Path]:
	"""Stage: enhance events (currently: add `event_name`) and emit canonical baselines.

	Returns:
		(pre_baseline_path, post_baseline_path, out_events_path)
	"""
	artifact_dir = Path(artifact_dir)
	artifact_dir.mkdir(parents=True, exist_ok=True)

	fixture_data = read_fixture_file(source_events)

	pre_path = save_pre_transformation_canonical_baseline_artifact(
		fixture_data=fixture_data,
		path=artifact_dir / "pre_baseline.json",
	)

	enhanced = enhance_events_add_event_name(fixture_data)

	post_path = save_post_transformation_canonical_baseline_artifact(
		events=enhanced,
		path=artifact_dir / "post_baseline.json",
	)

	out_events_path = write_events_to_file(events=enhanced, path=out_events, format=format)

	return pre_path, post_path, out_events_path
