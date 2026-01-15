from __future__ import annotations

from pathlib import Path

from .adapter import (
	Neo4jConfig,
	canonical_edges_baseline_from_neo4j,
	read_edges_from_file,
	write_graph_to_neo4j,
	write_edges_to_file,
	write_edges_to_neo4j,
)

from .canonical_baseline_helper import save_canonical_baseline_artifact


def store_edges_in_neo4j(
	*,
	artifact_dir: str | Path,
	source_edges: str | Path | None = None,
	source_graph: str | Path | None = None,
	run_id: str,
	config: Neo4jConfig | None = None,
	clear_run_first: bool = True,
	rel_type: str = "CAUSES",

) -> tuple[Path, Path]:
	"""Store stage: persist edges to Neo4j and emit baselines after readback.

	Pipeline semantics:
	- pre_baseline: canonical baseline computed from the input edges file
	- write: persist edges to Neo4j (side effect)
	- readback: export edges from Neo4j for this run_id
	- post_baseline: canonical baseline computed from Neo4j readback

	Returns:
		(candidate_baseline_path, readback_edges_path)
	"""
	artifact_dir = Path(artifact_dir)
	artifact_dir.mkdir(parents=True, exist_ok=True)

	if (source_edges is None) == (source_graph is None):
		raise ValueError("provide exactly one of source_edges or source_graph")

	if source_graph is not None:
		# Preferred path: ingest a Neo4j-friendly graph batch produced by events_to_graphs.
		import json

		graph_path = Path(source_graph)
		graph_text = graph_path.read_text(encoding="utf-8")
		graph = json.loads(graph_text)
		if not isinstance(graph, dict):
			raise ValueError("graph file must contain a top-level JSON object")
		write_graph_to_neo4j(
			graph=graph,
			run_id=run_id,
			config=config,
			clear_run_first=clear_run_first,
			node_label="Event",
			rel_type=rel_type,
		)
	else:
		edges = read_edges_from_file(Path(source_edges))
		write_edges_to_neo4j(
			edges=edges,
			run_id=run_id,
			config=config,
			clear_run_first=clear_run_first,
			rel_type=rel_type,
		)

	readback_edges = canonical_edges_baseline_from_neo4j(run_id=run_id, config=config, rel_type=rel_type)
	readback_path = write_edges_to_file(edges=readback_edges, path=artifact_dir / "readback_edges.json")

	baseline_path = save_canonical_baseline_artifact(
		edges=readback_edges,
		path=artifact_dir / "baseline_edges_readback.json",
	)

	return baseline_path, readback_path
