from __future__ import annotations

from pathlib import Path
from typing import Optional

from .adapter import (
	FixtureFormat,
	generate_fixture_files,
	read_fixture_file,
	transform_fixture_data_to_events,
)

from .canonical_baseline_helper import save_canonical_baseline_artifact as save_events_baseline

from dag_helpers.transform_data.events_to_edges.transformer import build_edges
from dag_helpers.transform_data.events_to_edges.canonical_baseline_helper import (
	save_canonical_baseline_artifact as save_edges_baseline,
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
		(reference_events_baseline_path, reference_edges_baseline_path)
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

	# Transform fixture payload into event dicts.
	events = transform_fixture_data_to_events(fixture_data)

	# Reference canonical baselines for downstream validation.
	events_baseline_path = save_events_baseline(
		events=events,
		path=artifact_dir / "baseline_events.json",
	)

	edges = build_edges(events)
	edges_baseline_path = save_edges_baseline(
		edges=edges,
		path=artifact_dir / "baseline_edges.json",
	)

	return events_baseline_path, edges_baseline_path
