from pathlib import Path

import pytest


_FIXTURE_EVENTS_FILE = (
	Path(__file__).resolve().parents[1]
	/ "fixtures"
	/ "events"
	/ "enrollment_events_one_missing.json"
)


@pytest.mark.e2e
def test_enrollment_fixture_one_missing_entity_writes_to_neo4j(tmp_path):
	"""Fixture-driven e2e: one entity canonical, one entity missing upstream with cascade.

	This models 'chaos' as missing facts causing missing downstream events.
	"""
	from neo4j import GraphDatabase

	from enrollment_canonical import canonical_edges, canonical_types
	from helpers.enrollment.adapter import load_events_from_file
	from helpers.enrollment.graph import build_edges, write_graph_to_file
	from helpers.enrollment.transformer import normalize_events
	from helpers.neo4j.adapter import load_graph_from_file, write_graph_to_db

	raw_events = load_events_from_file(str(_FIXTURE_EVENTS_FILE))
	normalized = normalize_events(raw_events)
	edges = build_edges(normalized)

	run_id = "e2e:enrollment-one-missing"
	graph_file = tmp_path / "graph.json"
	write_graph_to_file(events=normalized, edges=edges, run_id=run_id, filename=str(graph_file))

	graph = load_graph_from_file(str(graph_file))
	write_graph_to_db(graph)

	driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "test"))
	try:
		with driver.session() as session:
			entity_ids = [
				rec["entity_id"]
				for rec in session.run(
					"""
					MATCH (r:Run {run_id: $run_id})-[:INCLUDES]->(e:EnrollmentEvent)
					RETURN DISTINCT e.entity_id AS entity_id
					ORDER BY entity_id
					""",
					run_id=run_id,
				)
				if rec.get("entity_id")
			]
			assert entity_ids == ["enrollment-1", "enrollment-2"]

			canonical_types_set = canonical_types()
			canonical_edges_set = canonical_edges()

			# enrollment-1 is canonical (all types + edges).
			types_1 = session.run(
				"""
				MATCH (r:Run {run_id: $run_id})-[:INCLUDES]->(e:EnrollmentEvent {entity_id: 'enrollment-1'})
				RETURN collect(DISTINCT e.event_type) AS types
				""",
				run_id=run_id,
			).single()["types"]
			assert set(types_1) == canonical_types_set

			edges_1 = session.run(
				"""
				MATCH (a:EnrollmentEvent {entity_id: 'enrollment-1'})-[rel:CAUSES {run_id: $run_id}]->(b:EnrollmentEvent {entity_id: 'enrollment-1'})
				RETURN collect(DISTINCT [a.event_type, b.event_type]) AS edges
				""",
				run_id=run_id,
			).single()["edges"]
			assert {tuple(p) for p in edges_1} == canonical_edges_set

			# enrollment-2 is missing upstream: only the first two facts remain.
			types_2 = session.run(
				"""
				MATCH (r:Run {run_id: $run_id})-[:INCLUDES]->(e:EnrollmentEvent {entity_id: 'enrollment-2'})
				RETURN collect(DISTINCT e.event_type) AS types
				""",
				run_id=run_id,
			).single()["types"]
			assert set(types_2) == {"CourseEnrollmentRequested", "PaymentProcessingRequested"}

			edges_2 = session.run(
				"""
				MATCH (a:EnrollmentEvent {entity_id: 'enrollment-2'})-[rel:CAUSES {run_id: $run_id}]->(b:EnrollmentEvent {entity_id: 'enrollment-2'})
				RETURN collect(DISTINCT [a.event_type, b.event_type]) AS edges
				""",
				run_id=run_id,
			).single()["edges"]
			assert {tuple(p) for p in edges_2} == {("CourseEnrollmentRequested", "PaymentProcessingRequested")}
	finally:
		driver.close()
