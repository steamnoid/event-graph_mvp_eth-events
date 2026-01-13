"""Export Enrollment subgraphs from Neo4j for debugging and visualization.

This script is intentionally lightweight:
- exports nodes/edges for a specific `run_id` (and optional `entity_id`)
- writes a JSON snapshot and a Graphviz DOT file
- optionally renders PNG/SVG if Graphviz (`dot`) is installed
- can optionally validate strict canonical shape (for consistent runs)

Usage examples:
  python scripts/export_neo4j_enrollment_run.py --run-id "e2e:enrollment-generated:seed=1337:entities=4" --expect-canonical
  python scripts/export_neo4j_enrollment_run.py --run-id "..." --entity-id "enrollment-1" --out /tmp/enrollment-1

Connection is taken from env vars (defaults match docker-compose):
  NEO4J_URI=neo4j://localhost:7687
  NEO4J_USER=neo4j
  NEO4J_PASSWORD=test
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import sys
from typing import Any, Dict, List, Sequence, Tuple

from neo4j import GraphDatabase


_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC_DIR = _REPO_ROOT / "src"
if str(_SRC_DIR) not in sys.path:
	sys.path.insert(0, str(_SRC_DIR))


def _neo4j_config() -> tuple[str, str, str]:
	uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
	user = os.getenv("NEO4J_USER", "neo4j")
	password = os.getenv("NEO4J_PASSWORD", "test")
	return uri, user, password


def _canonical_model() -> tuple[Sequence[str], set[tuple[str, str]]]:
	from enrollment_canonical import CANONICAL_ENROLLMENT_SPECS

	canonical_types = [spec.event_type for spec in CANONICAL_ENROLLMENT_SPECS]
	edges: set[tuple[str, str]] = set()
	for child_spec in CANONICAL_ENROLLMENT_SPECS:
		for parent_type in child_spec.parent_event_types:
			edges.add((parent_type, child_spec.event_type))
	return canonical_types, edges


def _fetch_enrollment_graph(*, run_id: str, entity_id: str | None) -> dict:
	uri, user, password = _neo4j_config()
	driver = GraphDatabase.driver(uri, auth=(user, password))
	try:
		with driver.session() as session:
			node_rows = list(
				session.run(
					"""
					MATCH (r:Run {run_id: $run_id})-[:INCLUDES]->(e:EnrollmentEvent)
					WHERE $entity_id IS NULL OR e.entity_id = $entity_id
					RETURN
						e.event_id AS event_id,
						e.entity_id AS entity_id,
						e.event_type AS event_type,
						e.event_kind AS event_kind,
						e.layer AS layer
					ORDER BY e.entity_id, e.layer, e.event_type
					""",
					run_id=run_id,
					entity_id=entity_id,
				)
			)
			edge_rows = list(
				session.run(
					"""
					MATCH (a:EnrollmentEvent)-[:CAUSES {run_id: $run_id}]->(b:EnrollmentEvent)
					WHERE $entity_id IS NULL OR (a.entity_id = $entity_id AND b.entity_id = $entity_id)
					RETURN a.event_id AS src, b.event_id AS dst
					ORDER BY src, dst
					""",
					run_id=run_id,
					entity_id=entity_id,
				)
			)

		nodes = [dict(r) for r in node_rows]
		edges = [dict(r) for r in edge_rows]
		return {"run_id": run_id, "nodes": nodes, "edges": edges}
	finally:
		driver.close()


def _write_json(path: Path, payload: dict) -> None:
	path.parent.mkdir(parents=True, exist_ok=True)
	path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_dot(path: Path, payload: dict) -> None:
	path.parent.mkdir(parents=True, exist_ok=True)

	nodes_by_id: Dict[str, Dict[str, Any]] = {
		n["event_id"]: n for n in payload.get("nodes", []) if n.get("event_id")
	}

	lines: List[str] = []
	lines.append("digraph EnrollmentRun {")
	lines.append('  graph [rankdir="LR"];')
	lines.append('  node [shape="box", fontname="Helvetica", fontsize="10"];')
	lines.append('  edge [color="#888888"];')

	for event_id, n in nodes_by_id.items():
		label_parts = [str(n.get("event_type") or ""), str(n.get("entity_id") or "")]
		label = "\\n".join([p for p in label_parts if p])
		lines.append(
			"  \"{}\" [label=\"{}\"];".format(
				event_id.replace('"', "\\\""),
				label.replace('"', "\\\""),
			)
		)

	for e in payload.get("edges", []):
		src = e.get("src")
		dst = e.get("dst")
		if not src or not dst:
			continue
		if src not in nodes_by_id or dst not in nodes_by_id:
			continue
		lines.append("  \"{}\" -> \"{}\";".format(src.replace('"', "\\\""), dst.replace('"', "\\\"")))

	lines.append("}")
	path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _maybe_render_graphviz(dot_file: Path) -> List[Path]:
	dot = shutil.which("dot")
	if not dot:
		return []

	outputs: List[Path] = []
	for fmt in ("png", "svg"):
		out = dot_file.with_suffix(f".{fmt}")
		cmd = [dot, f"-T{fmt}", str(dot_file), "-o", str(out)]
		res = os.spawnv(os.P_WAIT, dot, cmd)
		if res == 0 and out.exists():
			outputs.append(out)
	return outputs


def _validate_canonical(payload: dict) -> None:
	canonical_types, canonical_edges = _canonical_model()
	canonical_type_set = set(canonical_types)

	# Build lookup by entity_id and by event_id->type
	by_entity: Dict[str, List[Dict[str, Any]]] = {}
	for n in payload.get("nodes", []):
		entity_id = n.get("entity_id")
		if not entity_id:
			continue
		by_entity.setdefault(entity_id, []).append(n)

	node_by_id: Dict[str, Dict[str, Any]] = {
		n["event_id"]: n for n in payload.get("nodes", []) if n.get("event_id")
	}

	for entity_id, nodes in sorted(by_entity.items()):
		types = {n.get("event_type") for n in nodes if n.get("event_type")}
		if types != canonical_type_set:
			raise SystemExit(
				f"Entity {entity_id}: event_type set mismatch. "
				f"expected={sorted(canonical_type_set)} actual={sorted(types)}"
			)

		# Compute actual edges as (parent_type, child_type) pairs
		actual_pairs: set[tuple[str, str]] = set()
		for e in payload.get("edges", []):
			src = e.get("src")
			dst = e.get("dst")
			if not src or not dst:
				continue
			src_node = node_by_id.get(src)
			dst_node = node_by_id.get(dst)
			if not src_node or not dst_node:
				continue
			if src_node.get("entity_id") != entity_id or dst_node.get("entity_id") != entity_id:
				continue
			actual_pairs.add((src_node.get("event_type"), dst_node.get("event_type")))

		if actual_pairs != canonical_edges:
			missing = sorted(canonical_edges - actual_pairs)
			extra = sorted(actual_pairs - canonical_edges)
			raise SystemExit(
				f"Entity {entity_id}: canonical edges mismatch. missing={missing} extra={extra}"
			)


def main() -> None:
	parser = argparse.ArgumentParser()
	parser.add_argument("--run-id", required=True)
	parser.add_argument("--entity-id", default=None)
	parser.add_argument(
		"--out",
		default=None,
		help="Output base path (no extension). Default: ./neo4j_exports/<run_id>[/<entity_id>]",
	)
	parser.add_argument(
		"--expect-canonical",
		action="store_true",
		help="Fail if subgraph does not match the canonical enrollment graph per entity.",
	)
	args = parser.parse_args()

	run_id: str = args.run_id
	entity_id: str | None = args.entity_id

	if args.out:
		base = Path(args.out)
	else:
		safe_run = run_id.replace("/", "_").replace(":", "_")
		base = Path("neo4j_exports") / safe_run
		if entity_id:
			base = base / entity_id

	payload = _fetch_enrollment_graph(run_id=run_id, entity_id=entity_id)
	json_file = base.with_suffix(".json")
	dot_file = base.with_suffix(".dot")
	_write_json(json_file, payload)
	_write_dot(dot_file, payload)

	if args.expect_canonical:
		_validate_canonical(payload)

	rendered = _maybe_render_graphviz(dot_file)

	nodes = len(payload.get("nodes", []))
	edges = len(payload.get("edges", []))
	print(f"Exported run_id={run_id} entity_id={entity_id or '*'} nodes={nodes} edges={edges}")
	print(f"JSON: {json_file}")
	print(f"DOT:  {dot_file}")
	if rendered:
		for p in rendered:
			print(f"Rendered: {p}")
	else:
		print("Graphviz not found; install it to render PNG/SVG (macOS: `brew install graphviz`).")


if __name__ == "__main__":
	main()
