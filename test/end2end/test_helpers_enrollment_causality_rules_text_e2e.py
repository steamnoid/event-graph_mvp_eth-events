import pytest


@pytest.mark.e2e
def test_enrollment_causality_rules_export_text_is_canonical_and_identical(tmp_path):
	"""E2E: after writing Enrollment graphs to Neo4j, export causality rules as text.

	The export must:
	- match the canonical enrollment graph for each entity
	- be identical across entities when inconsistency_rate=0.0
	"""
	from event_generator import generate_events_batch
	from helpers.enrollment.graph import build_edges, write_graph_to_file
	from helpers.enrollment.transformer import normalize_events
	from helpers.neo4j.adapter import load_graph_from_file, write_graph_to_db

	# New behavior under test (should be implemented in helpers, not only as an ad-hoc script).
	from helpers.enrollment.neo4j_causality_rules import persist_causality_rules

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

	run_id = f"e2e:enrollment-causality-text:seed={seed}:entities={entity_count}"
	graph_file = tmp_path / "graph.json"
	write_graph_to_file(events=normalized, edges=edges, run_id=run_id, filename=str(graph_file))

	graph = load_graph_from_file(str(graph_file))
	write_graph_to_db(graph)

	out_file = tmp_path / "causality.txt"
	persist_causality_rules(run_id=run_id, out_file=str(out_file), expect_canonical=True)

	text = out_file.read_text(encoding="utf-8")
	assert "run_id=" in text
	assert "entity_id=enrollment-1" in text
	assert "entity_id=enrollment-4" in text
	assert "AccessGranted" in text
	assert "PaymentConfirmed -> AccessGranted" in text
