from __future__ import annotations

# PATCH_MARKER_VISIBILITY_CHECK

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from neo4j import GraphDatabase


Edge = dict[str, str]
EventNameById = Mapping[str, str]
GraphBatch = Mapping[str, Any]

from .canonical_baseline_helper import (
	save_canonical_baseline_artifact,
	transform_edges_to_canonical_baseline_format,
)


@dataclass(frozen=True)
class Neo4jConfig:
	uri: str
	user: str
	password: str


def neo4j_config_from_env() -> Neo4jConfig:
	uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
	user = os.getenv("NEO4J_USER", "neo4j")
	password = os.getenv("NEO4J_PASSWORD", "test")
	return Neo4jConfig(uri=uri, user=user, password=password)


def read_edges_from_file(path: str | Path) -> list[Edge]:
	"""Read edges from disk (JSON array)."""
	path = Path(path)
	text = path.read_text(encoding="utf-8").strip()
	if not text:
		return []

	data = json.loads(text)
	if not isinstance(data, list):
		raise ValueError("edges file must contain a top-level JSON array")

	edges: list[Edge] = []
	for idx, edge in enumerate(data):
		if not isinstance(edge, dict):
			raise ValueError(f"edge[{idx}] must be a dict")
		if "from" not in edge or "to" not in edge:
			raise ValueError(f"edge[{idx}] must contain 'from' and 'to'")
		edges.append({"from": str(edge["from"]), "to": str(edge["to"])})
	return edges


def write_edges_to_file(*, edges: Iterable[Edge], path: str | Path) -> Path:
	"""Write edges as a JSON array."""
	path = Path(path)
	path.parent.mkdir(parents=True, exist_ok=True)
	payload = list(edges)
	path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
	return path


def canonical_edges_baseline_from_file(*, source_edges: str | Path) -> list[Edge]:
	"""Load edges from file and return canonical baseline list."""
	edges = read_edges_from_file(source_edges)
	return transform_edges_to_canonical_baseline_format(edges)


def _ensure_schema(*, session: Any) -> None:
	# Composite constraint requires Neo4j 5+ (we're on 5.x in docker-compose).
	session.run(
		"CREATE CONSTRAINT event_unique IF NOT EXISTS FOR (e:Event) REQUIRE (e.run_id, e.event_id) IS UNIQUE"
	)


def _clear_run(*, session: Any, run_id: str) -> None:
	session.run("MATCH (n:Event {run_id: $run_id}) DETACH DELETE n", run_id=run_id)


def write_edges_to_neo4j(
	*,
	edges: Sequence[Edge],
	run_id: str,
	event_names_by_id: EventNameById | None = None,
	config: Neo4jConfig | None = None,
	clear_run_first: bool = True,
	rel_type: str = "CAUSES",
) -> int:
	"""Persist edges to Neo4j.

	Stores nodes as (:Event {run_id, event_id}) and relationships as
	(:Event)-[:<rel_type> {run_id}]->(:Event).
	"""
	if config is None:
		config = neo4j_config_from_env()

	canonical = transform_edges_to_canonical_baseline_format(edges)

	event_rows: list[dict[str, str]] = []
	if event_names_by_id:
		node_ids = {e["from"] for e in canonical} | {e["to"] for e in canonical}
		for event_id in sorted(node_ids):
			event_name = event_names_by_id.get(event_id)
			if not event_name:
				continue
			event_rows.append({"event_id": str(event_id), "event_name": str(event_name)})

	driver = GraphDatabase.driver(config.uri, auth=(config.user, config.password))
	try:
		with driver.session() as session:
			_ensure_schema(session=session)
			if clear_run_first:
				_clear_run(session=session, run_id=run_id)

			if event_rows:
				# Populate node display properties (useful for Neo4j Browser captions).
				session.run(
					"""
					UNWIND $events AS ev
					MERGE (n:Event {run_id: $run_id, event_id: ev.event_id})
					SET n.event_name = ev.event_name,
						n.name = ev.event_name
					""",
					events=event_rows,
					run_id=run_id,
				)

			result = session.run(
				"""
				UNWIND $edges AS e
				MERGE (a:Event {run_id: $run_id, event_id: e.from})
				MERGE (b:Event {run_id: $run_id, event_id: e.to})
				MERGE (a)-[r:%s {run_id: $run_id}]->(b)
				RETURN count(r) AS rel_count
				"""
				% rel_type,
				edges=canonical,
				run_id=run_id,
			)
			record = result.single()
			return int(record["rel_count"]) if record and "rel_count" in record else 0
	finally:
		driver.close()


def write_graph_to_neo4j(
	*,
	graph: GraphBatch,
	run_id: str,
	config: Neo4jConfig | None = None,
	clear_run_first: bool = True,
	node_label: str = "Event",
	rel_type: str = "CAUSES",
) -> int:
	"""Persist a Neo4j-friendly graph batch to Neo4j.

	Expected graph format (minimal):
	- graph["nodes"]: list[{"event_id": str, "properties": dict}]
	- graph["relationships"]: list[{"from": str, "to": str, "type": str, "properties": dict}]

	This is optimized for ingestion (UNWIND batches) and keeps the write path
	deterministic and explicit.
	"""
	if config is None:
		config = neo4j_config_from_env()

	graph_run_id = graph.get("run_id")
	if graph_run_id is not None and str(graph_run_id) != str(run_id):
		raise ValueError("graph.run_id does not match run_id argument")

	if node_label != "Event":
		# We can add label-per-node later if we really need it.
		raise ValueError("only node_label='Event' is supported")

	nodes_raw = graph.get("nodes")
	if not isinstance(nodes_raw, list):
		raise ValueError("graph.nodes must be a list")

	rels_raw = graph.get("relationships")
	if not isinstance(rels_raw, list):
		raise ValueError("graph.relationships must be a list")

	node_rows: list[dict[str, Any]] = []
	event_names_by_id: dict[str, str] = {}
	for idx, node in enumerate(nodes_raw):
		if not isinstance(node, dict):
			raise ValueError(f"node[{idx}] must be a dict")
		event_id = node.get("event_id")
		if not event_id:
			raise ValueError(f"node[{idx}] must contain 'event_id'")
		props = node.get("properties") or {}
		if not isinstance(props, dict):
			raise ValueError(f"node[{idx}].properties must be a dict")

		# Preserve any provided caption properties.
		name = props.get("event_name") or props.get("name")
		if name:
			event_names_by_id[str(event_id)] = str(name)

		node_rows.append({"event_id": str(event_id), "properties": props})

	# Validate and canonicalize edges (for deterministic behavior).
	rel_rows: list[dict[str, Any]] = []
	for idx, rel in enumerate(rels_raw):
		if not isinstance(rel, dict):
			raise ValueError(f"relationship[{idx}] must be a dict")
		if rel.get("type") not in (None, rel_type):
			raise ValueError(f"relationship[{idx}].type must be '{rel_type}'")
		if "from" not in rel or "to" not in rel:
			raise ValueError(f"relationship[{idx}] must contain 'from' and 'to'")
		props = rel.get("properties") or {}
		if not isinstance(props, dict):
			raise ValueError(f"relationship[{idx}].properties must be a dict")
		# Ensure run_id is always present and consistent.
		props = dict(props)
		props["run_id"] = str(run_id)
		rel_rows.append({"from": str(rel["from"]), "to": str(rel["to"]), "properties": props})

	# Deterministic ordering.
	rel_rows = sorted(rel_rows, key=lambda r: (r.get("from", ""), r.get("to", "")))
	canonical_edges = transform_edges_to_canonical_baseline_format(
		[{"from": r["from"], "to": r["to"]} for r in rel_rows]
	)

	driver = GraphDatabase.driver(config.uri, auth=(config.user, config.password))
	try:
		with driver.session() as session:
			_ensure_schema(session=session)
			if clear_run_first:
				_clear_run(session=session, run_id=run_id)

			# Ensure nodes exist and carry properties (including Neo4j Browser caption field).
			if node_rows:
				session.run(
					"""
					UNWIND $nodes AS n
					MERGE (e:Event {run_id: $run_id, event_id: n.event_id})
					SET e += n.properties
					""",
					nodes=node_rows,
					run_id=run_id,
				)

			# Backfill display fields even if they weren't included in node properties.
			if event_names_by_id:
				event_rows = [
					{"event_id": event_id, "event_name": event_name}
					for event_id, event_name in sorted(event_names_by_id.items(), key=lambda kv: kv[0])
				]
				session.run(
					"""
					UNWIND $events AS ev
					MERGE (n:Event {run_id: $run_id, event_id: ev.event_id})
					SET n.event_name = coalesce(n.event_name, ev.event_name),
						n.name = coalesce(n.name, ev.event_name)
					""",
					events=event_rows,
					run_id=run_id,
				)

			result = session.run(
				(
					"""
					UNWIND $rels AS e
					MERGE (a:Event {run_id: $run_id, event_id: e.from})
					MERGE (b:Event {run_id: $run_id, event_id: e.to})
					MERGE (a)-[r:%s {run_id: $run_id}]->(b)
					SET r += e.properties
					RETURN count(r) AS rel_count
					"""
					% rel_type
				),
				rels=rel_rows,
				run_id=run_id,
			)
			record = result.single()
			return int(record["rel_count"]) if record and "rel_count" in record else 0
	finally:
		driver.close()


def canonical_edges_baseline_from_neo4j(
	*,
	run_id: str,
	config: Neo4jConfig | None = None,
	rel_type: str = "CAUSES",
) -> list[Edge]:
	"""Read edges back from Neo4j and return canonical baseline list."""
	if config is None:
		config = neo4j_config_from_env()

	driver = GraphDatabase.driver(config.uri, auth=(config.user, config.password))
	try:
		with driver.session() as session:
			rows = session.run(
				"""
				MATCH (a:Event {run_id: $run_id})-[r:%s {run_id: $run_id}]->(b:Event {run_id: $run_id})
				RETURN a.event_id AS `from`, b.event_id AS `to`
				ORDER BY `from`, `to`
				"""
				% rel_type,
				run_id=run_id,
			)
			edges = [{"from": str(r["from"]), "to": str(r["to"])} for r in rows]
			return transform_edges_to_canonical_baseline_format(edges)
	finally:
		driver.close()
