from __future__ import annotations

from pathlib import Path

from .canonical_baseline_helper import save_canonical_baseline_artifact
from .transformer import edges_from_graph, read_events_from_file, build_graph_batch, write_graph_to_file


def events_to_graphs(
	*,
	artifact_dir: str | Path,
	source_events: str | Path,
	out_graph: str | Path,
	run_id: str | None = None,
	node_label: str = "Event",
	rel_type: str = "CAUSES",
) -> tuple[Path, Path]:
	"""Transform stage: build a Neo4j-friendly graph batch from events.

	Returns:
		(candidate_edges_baseline_path, out_graph_path)

	Notes:
	We keep the canonical baseline contract *edges-based* so downstream validation
	still compares against the fetch_data reference edges baseline.
	"""
	artifact_dir = Path(artifact_dir)
	artifact_dir.mkdir(parents=True, exist_ok=True)

	events = read_events_from_file(source_events)
	graph = build_graph_batch(events=events, run_id=run_id, node_label=node_label, rel_type=rel_type)
	out_graph_path = write_graph_to_file(graph=graph, path=out_graph)

	edges = edges_from_graph(graph)
	baseline_path = save_canonical_baseline_artifact(
		edges=edges,
		path=artifact_dir / "baseline_edges.json",
	)

	return baseline_path, out_graph_path
