from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from neo4j import GraphDatabase

from helpers.enrollment.canonical import canonical_edges, canonical_types
from helpers.enrollment.causality_rules import CausalityRules, format_causality_rules_text


@dataclass(frozen=True)
class CausalityModel:
	run_id: str
	edges_by_entity: Dict[str, List[Tuple[str, str]]]
	types_by_entity: Dict[str, List[str]]


def _neo4j_config() -> tuple[str, str, str]:
	uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
	user = os.getenv("NEO4J_USER", "neo4j")
	password = os.getenv("NEO4J_PASSWORD", "test")
	return uri, user, password


def _canonical_types_and_edges() -> tuple[set[str], set[tuple[str, str]]]:
	return canonical_types(), canonical_edges()


def fetch_causality_model_from_neo4j(*, run_id: str) -> CausalityModel:
	uri, user, password = _neo4j_config()
	driver = GraphDatabase.driver(uri, auth=(user, password))
	try:
		with driver.session() as session:
			types_rows = list(
				session.run(
					"""
					MATCH (r:Run {run_id: $run_id})-[:INCLUDES]->(e:EnrollmentEvent)
					RETURN e.entity_id AS entity_id, collect(DISTINCT e.event_type) AS types
					""",
					run_id=run_id,
				)
			)

			edges_rows = list(
				session.run(
					"""
					MATCH (a:EnrollmentEvent)-[:CAUSES {run_id: $run_id}]->(b:EnrollmentEvent)
					RETURN a.entity_id AS entity_id, collect(DISTINCT [a.event_type, b.event_type]) AS edges
					""",
					run_id=run_id,
				)
			)

		types_by_entity: Dict[str, List[str]] = {}
		for row in types_rows:
			entity_id = row.get("entity_id")
			if not entity_id:
				continue
			types = row.get("types") or []
			types_by_entity[entity_id] = sorted({t for t in types if t})

		edges_by_entity: Dict[str, List[Tuple[str, str]]] = {}
		for row in edges_rows:
			entity_id = row.get("entity_id")
			if not entity_id:
				continue
			pairs = row.get("edges") or []
			edges_set: set[Tuple[str, str]] = set()
			for pair in pairs:
				if not isinstance(pair, list) or len(pair) != 2:
					continue
				parent_type, child_type = pair[0], pair[1]
				if parent_type and child_type:
					edges_set.add((parent_type, child_type))
			edges_by_entity[entity_id] = sorted(edges_set)

		return CausalityModel(
			run_id=run_id,
			edges_by_entity=edges_by_entity,
			types_by_entity=types_by_entity,
		)
	finally:
		driver.close()


def _as_rules(model: CausalityModel) -> CausalityRules:
	return CausalityRules(
		run_id=model.run_id,
		types_by_entity=model.types_by_entity,
		edges_by_entity=model.edges_by_entity,
	)


def _assert_canonical_and_identical(model: CausalityModel) -> None:
	canonical_types, canonical_edges = _canonical_types_and_edges()

	# Canonical: each entity must match canonical types + edges.
	for entity_id, types in model.types_by_entity.items():
		if set(types) != canonical_types:
			raise AssertionError(
				f"entity {entity_id}: event types mismatch: expected={sorted(canonical_types)} actual={types}"
			)

	for entity_id, edges in model.edges_by_entity.items():
		if set(edges) != canonical_edges:
			raise AssertionError(
				f"entity {entity_id}: edges mismatch: expected={sorted(canonical_edges)} actual={edges}"
			)

	# Identical across entities: edges/types lists must be exactly identical.
	entities = sorted(model.types_by_entity.keys())
	if not entities:
		return

	reference = entities[0]
	ref_types = model.types_by_entity.get(reference) or []
	ref_edges = model.edges_by_entity.get(reference) or []
	for entity_id in entities[1:]:
		if (model.types_by_entity.get(entity_id) or []) != ref_types:
			raise AssertionError(f"entity {entity_id}: types not identical to {reference}")
		if (model.edges_by_entity.get(entity_id) or []) != ref_edges:
			raise AssertionError(f"entity {entity_id}: edges not identical to {reference}")


def export_causality_rules_text(*, run_id: str, out_file: str, expect_canonical: bool = False) -> None:
	model = fetch_causality_model_from_neo4j(run_id=run_id)
	if expect_canonical:
		_assert_canonical_and_identical(model)

	text = format_causality_rules_text(_as_rules(model))
	with open(out_file, "w", encoding="utf-8") as f:
		f.write(text)
