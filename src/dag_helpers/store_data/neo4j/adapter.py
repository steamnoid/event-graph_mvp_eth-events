from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

from neo4j import GraphDatabase


Edge = dict[str, str]

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

	driver = GraphDatabase.driver(config.uri, auth=(config.user, config.password))
	try:
		with driver.session() as session:
			_ensure_schema(session=session)
			if clear_run_first:
				_clear_run(session=session, run_id=run_id)

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
