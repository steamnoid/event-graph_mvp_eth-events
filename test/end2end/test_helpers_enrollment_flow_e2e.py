from pathlib import Path
import pytest


_FIXTURE_EVENTS_FILE = (
	Path(__file__).resolve().parents[1]
	/ "fixtures"
	/ "events"
	/ "enrollment_events.json"
)


@pytest.mark.e2e
def test_enrollment_helpers_end2end_to_neo4j(tmp_path):
	"""Stage A e2e: helpers only, no Airflow DAG."""
	from neo4j import GraphDatabase

	from helpers.enrollment.adapter import load_events_from_file
	from helpers.enrollment.graph import build_edges, write_graph_to_file
	from helpers.enrollment.transformer import normalize_events
	from helpers.neo4j.adapter import load_graph_from_file, write_graph_to_db

	raw_events = load_events_from_file(str(_FIXTURE_EVENTS_FILE))
	normalized = normalize_events(raw_events)
	edges = build_edges(normalized)

	run_id = "e2e:enrollment-helpers"
	graph_file = tmp_path / "graph.json"
	write_graph_to_file(events=normalized, edges=edges, run_id=run_id, filename=str(graph_file))

	graph = load_graph_from_file(str(graph_file))
	write_graph_to_db(graph)

	# Validate counts are as expected for this run snapshot.
	driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "test"))
	try:
		with driver.session() as session:
			row = session.run(
				"""
				MATCH (r:Run {run_id: $run_id})-[:INCLUDES]->(e:Event)
				WITH r, count(DISTINCT e) AS events
				MATCH (r)-[:INCLUDES]->(a:Event)
				MATCH (r)-[:INCLUDES]->(b:Event)
				OPTIONAL MATCH (a)-[rel:CAUSES]->(b)
				RETURN events AS events, count(DISTINCT rel) AS rels
				""",
				run_id=run_id,
			).single()

			assert row is not None
			assert int(row["events"]) == len(normalized)
			assert int(row["rels"]) == len(edges)

			# Stronger assertion: every entity graph must match the full canonical spec.
			canonical_parents = {
				"CourseEnrollmentRequested": [],
				"PaymentProcessingRequested": ["CourseEnrollmentRequested"],
				"PaymentConfirmed": ["PaymentProcessingRequested"],
				"UserProfileLoaded": ["PaymentConfirmed"],
				"CourseAccessRequested": ["PaymentConfirmed"],
				"EligibilityChecked": ["UserProfileLoaded"],
				"ContentAvailabilityChecked": ["CourseAccessRequested"],
				"EligibilityPassed": ["EligibilityChecked"],
				"ContentAvailable": ["ContentAvailabilityChecked"],
				"ContentPrepared": ["ContentAvailable"],
				"AccessGranted": [
					"PaymentConfirmed",
					"EligibilityPassed",
					"ContentPrepared",
				],
				"EnrollmentCompleted": ["AccessGranted"],
				"EnrollmentArchived": ["EnrollmentCompleted"],
			}
			canonical_types = set(canonical_parents.keys())
			expected_edges = {
				(parent, child)
				for child, parents in canonical_parents.items()
				for parent in parents
			}

			entity_ids = [
				rec["entity_id"]
				for rec in session.run(
					"""
					MATCH (r:Run {run_id: $run_id})-[:INCLUDES]->(e:EnrollmentEvent)
					RETURN DISTINCT e.entity_id AS entity_id
					""",
					run_id=run_id,
				)
				if rec.get("entity_id")
			]
			assert len(entity_ids) > 0

			for entity_id in entity_ids:
				types_row = session.run(
					"""
					MATCH (r:Run {run_id: $run_id})-[:INCLUDES]->(e:EnrollmentEvent {entity_id: $entity_id})
					RETURN collect(DISTINCT e.event_type) AS types
					""",
					run_id=run_id,
					entity_id=entity_id,
				).single()
				assert types_row is not None
				assert set(types_row["types"]) == canonical_types

				edges_row = session.run(
					"""
					MATCH (a:EnrollmentEvent {entity_id: $entity_id})-[rel:CAUSES {run_id: $run_id}]->(b:EnrollmentEvent {entity_id: $entity_id})
					RETURN collect(DISTINCT [a.event_type, b.event_type]) AS edges
					""",
					run_id=run_id,
					entity_id=entity_id,
				).single()
				assert edges_row is not None
				actual_edges = {tuple(pair) for pair in edges_row["edges"]}
				assert actual_edges == expected_edges
	finally:
		driver.close()
