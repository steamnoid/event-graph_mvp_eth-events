import pytest


@pytest.mark.e2e
def test_generated_enrollment_fixture_entity_count_4_writes_four_canonical_graphs(tmp_path):
	from neo4j import GraphDatabase

	from event_generator import generate_events_batch
	from helpers.enrollment.graph import build_edges, write_graph_to_file
	from helpers.enrollment.transformer import normalize_events
	from helpers.neo4j.adapter import load_graph_from_file, write_graph_to_db

	seed = 1337
	entity_count = 4
	inconsistency_rate = 0.0

	raw_events = generate_events_batch(
		seed=seed,
		entity_count=entity_count,
		inconsistency_rate=inconsistency_rate,
	)
	normalized = normalize_events(raw_events)
	edges = build_edges(normalized)

	run_id = f"e2e:enrollment-generated:seed={seed}:entities={entity_count}"
	graph_file = tmp_path / "graph.json"
	write_graph_to_file(events=normalized, edges=edges, run_id=run_id, filename=str(graph_file))

	graph = load_graph_from_file(str(graph_file))
	write_graph_to_db(graph)

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

	driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "test"))
	try:
		with driver.session() as session:
			counts = session.run(
				"""
				MATCH (r:Run {run_id: $run_id})-[:INCLUDES]->(e:EnrollmentEvent)
				RETURN count(DISTINCT e.entity_id) AS entities, count(DISTINCT e) AS events
				""",
				run_id=run_id,
			).single()
			assert counts is not None
			assert int(counts["entities"]) == entity_count
			assert int(counts["events"]) == (entity_count * len(canonical_types))

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
			assert len(entity_ids) == entity_count

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
