from __future__ import annotations

from pathlib import Path

from .transformer import (
	build_edges,
	read_edges_from_file,
	read_events_from_file,
	save_post_transformation_canonical_baseline_artifact,
	save_pre_transformation_canonical_baseline_artifact,
	write_edges_to_file,
)


def events_to_edges(
	*,
	artifact_dir: str | Path,
	source_events: str | Path,
	out_edges: str | Path,
) -> tuple[Path, Path, Path]:
	"""Transform stage: build edges from events and emit canonical baselines.

	`source_events` is expected to be a *normalized* events file (schema-wise we only
	require `event_id` and `parent_event_ids`).

	Returns:
		(pre_baseline_path, post_baseline_path, out_edges_path)

	Notes:
		This stage's canonical baselines are for the *edges* payload:
		- pre: canonical edges baseline derived from in-memory edges
		- post: canonical edges baseline derived from the edges file re-read from disk
	"""
	artifact_dir = Path(artifact_dir)
	artifact_dir.mkdir(parents=True, exist_ok=True)

	events = read_events_from_file(source_events)
	edges = build_edges(events)

	# Canonical baseline before transformation (edges baseline).
	pre_path = save_pre_transformation_canonical_baseline_artifact(
		fixture_data=edges,
		path=artifact_dir / "pre_baseline.json",
	)

	out_edges_path = write_edges_to_file(edges=edges, path=out_edges)

	# Canonical baseline after transformation (edges baseline), re-read from disk.
	edges_on_disk = read_edges_from_file(out_edges_path)
	post_path = save_post_transformation_canonical_baseline_artifact(
		edges=edges_on_disk,
		path=artifact_dir / "post_baseline.json",
	)

	return pre_path, post_path, out_edges_path
