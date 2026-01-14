from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from helpers.enrollment import artifacts
from helpers.enrollment import validator


DEFAULT_ARTIFACT_ROOT = artifacts.DEFAULT_ARTIFACT_ROOT


Event = dict[str, Any]


def run_dir(*, run_id: str, artifact_root: str = DEFAULT_ARTIFACT_ROOT) -> Path:
	return artifacts.run_dir(run_id=run_id, artifact_root=artifact_root)


def fetch_events_to_file(
	*,
	run_id: str,
	source_events_file: str,
	artifact_root: str = DEFAULT_ARTIFACT_ROOT,
	source_rules_file: Optional[str] = None,
) -> str:
	"""Stage C1: copy raw Enrollment events into the run artifact directory.

	The source file may be a JSON array or NDJSON; it is copied as-is.
	If source_rules_file is provided, it is copied to C0.txt.
	"""

	# Extract / Load (artifact handoff)
	run_path = _get_run_directory(run_id=run_id, artifact_root=artifact_root)
	if source_rules_file:
		artifacts.copy_text_file(source_file=source_rules_file, dest_file=run_path / "C0.txt")
	return artifacts.copy_text_file(source_file=source_events_file, dest_file=run_path / "events.json")


def validate_raw_events(
	*,
	run_id: str,
	events_file: str,
	artifact_root: str = DEFAULT_ARTIFACT_ROOT,
) -> str:
	"""Stage C1 validation: validate raw schema and write C1.

	If C0.txt exists in the run directory, assert C0 == C1.
	"""

	return validator.validate_raw_events(run_id=run_id, events_file=events_file, artifact_root=artifact_root)


def transform_events_to_normalized_file(
	*,
	run_id: str,
	events_file: str,
	artifact_root: str = DEFAULT_ARTIFACT_ROOT,
) -> str:
	"""Stage C2: normalize events and write normalized_events.json."""

	from helpers.enrollment.transformer import normalize_events

	# Extract
	run_path = _get_run_directory(run_id=run_id, artifact_root=artifact_root)
	raw_events = _load_event_batch(events_file)

	# Transform
	normalized = normalize_events(raw_events)

	# Load
	return _store_run_artifact(run_path=run_path, name="normalized_events.json", payload=normalized)


def validate_normalized_events(
	*,
	run_id: str,
	normalized_file: str,
	artifact_root: str = DEFAULT_ARTIFACT_ROOT,
) -> str:
	"""Stage C2 validation: write C2 and assert C1 == C2."""

	return validator.validate_normalized_events(
		run_id=run_id,
		normalized_file=normalized_file,
		artifact_root=artifact_root,
	)


def transform_normalized_to_edges_file(
	*,
	run_id: str,
	normalized_file: str,
	artifact_root: str = DEFAULT_ARTIFACT_ROOT,
) -> str:
	"""Stage C3: build edges from declared parent_event_ids and write edges.json."""

	from helpers.enrollment.graph import build_edges

	# Extract
	run_path = _get_run_directory(run_id=run_id, artifact_root=artifact_root)
	normalized = _load_event_batch(normalized_file)

	# Transform
	edges = build_edges(normalized)

	# Load
	return _store_run_artifact(run_path=run_path, name="edges.json", payload=edges)


def validate_edges(
	*,
	run_id: str,
	normalized_file: str,
	edges_file: str,
	artifact_root: str = DEFAULT_ARTIFACT_ROOT,
) -> str:
	"""Stage C3 validation: write C3 and assert C2 == C3."""

	return validator.validate_edges(
		run_id=run_id,
		normalized_file=normalized_file,
		edges_file=edges_file,
		artifact_root=artifact_root,
	)


def transform_edges_to_graph_file(
	*,
	run_id: str,
	normalized_file: str,
	edges_file: str,
	artifact_root: str = DEFAULT_ARTIFACT_ROOT,
) -> str:
	"""Stage C4: write graph.json {run_id, events, edges}."""

	from helpers.enrollment.graph import write_graph_to_file

	# Extract
	run_path = _get_run_directory(run_id=run_id, artifact_root=artifact_root)
	normalized = _load_event_batch(normalized_file)
	edges = _load_event_batch(edges_file)

	# Transform / Load (graph is a materialized artifact)
	out_file = run_path / "graph.json"
	write_graph_to_file(events=normalized, edges=edges, run_id=run_id, filename=str(out_file))
	return str(out_file)


def validate_graph(
	*,
	run_id: str,
	graph_file: str,
	artifact_root: str = DEFAULT_ARTIFACT_ROOT,
) -> str:
	"""Stage C4 validation: write C4 and assert C3 == C4."""

	return validator.validate_graph(run_id=run_id, graph_file=graph_file, artifact_root=artifact_root)


def write_graph_to_neo4j(*, graph_file: str) -> int:
	"""Persist graph.json to Neo4j."""

	from helpers.neo4j.adapter import load_graph_from_file, write_graph_to_db

	graph = load_graph_from_file(graph_file)
	write_graph_to_db(graph)
	return 0


def validate_neo4j_readback(
	*,
	run_id: str,
	expect_canonical: bool = False,
	artifact_root: str = DEFAULT_ARTIFACT_ROOT,
) -> str:
	"""Stage C5: export C5 from Neo4j and assert C4 == C5."""

	return validator.validate_neo4j_readback(
		run_id=run_id,
		expect_canonical=expect_canonical,
		artifact_root=artifact_root,
	)


def _get_run_directory(*, run_id: str, artifact_root: str) -> Path:
	return artifacts.run_dir(run_id=run_id, artifact_root=artifact_root)


def _load_event_batch(source: str) -> list[Event]:
	from helpers.enrollment.adapter import load_events_from_file

	return load_events_from_file(source)


def _store_run_artifact(*, run_path: Path, name: str, payload: Any) -> str:
	"""Persist a run-scoped artifact.

	Storage mechanics (format, filesystem) are an implementation detail.
	"""
	return artifacts.write_json(path=run_path / name, payload=payload)
