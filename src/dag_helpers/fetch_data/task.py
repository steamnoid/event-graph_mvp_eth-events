from __future__ import annotations

from pathlib import Path
from typing import Optional

from .adapter import (
	FixtureFormat,
	generate_fixture_files,
	read_fixture_file,
	save_post_transformation_canonical_baseline_artifact,
	save_pre_transformation_canonical_baseline_artifact,
	transform_fixture_data_to_events,
)


def fetch_data(
	*,
	artifact_dir: str | Path,
	out_events: str | Path,
	format: FixtureFormat = "ndjson",
	seed: int | None = None,
	entity_count: int = 1,
	inconsistency_rate: float = 0.0,
	missing_event_rate: float = 0.0,
	run_id: str | None = None,
	out_rules: Optional[str | Path] = None,
) -> tuple[Path, Path]:
	"""Stage: fetch/generate data and emit canonical baselines.

	This is intended to support a generalized pipeline where each stage can be
	validated via a (pre, post) canonical baseline artifact pair.

	Returns:
		(pre_baseline_path, post_baseline_path)
	"""
	artifact_dir = Path(artifact_dir)
	artifact_dir.mkdir(parents=True, exist_ok=True)

	# Extract/Fetch (here: generate fixtures deterministically)
	events_path = generate_fixture_files(
		out_events=out_events,
		format=format,
		seed=seed,
		entity_count=entity_count,
		inconsistency_rate=inconsistency_rate,
		missing_event_rate=missing_event_rate,
		run_id=run_id,
		out_rules=out_rules,
	)

	fixture_data = read_fixture_file(events_path)

	# Canonical baseline before any transformation
	pre_path = save_pre_transformation_canonical_baseline_artifact(
		fixture_data=fixture_data,
		path=artifact_dir / "pre_baseline.json",
	)

	# Transform (currently identity, but kept as a seam)
	events = transform_fixture_data_to_events(fixture_data)

	# Canonical baseline after transformation
	post_path = save_post_transformation_canonical_baseline_artifact(
		events=events,
		path=artifact_dir / "post_baseline.json",
	)

	return pre_path, post_path
