import pytest


@pytest.mark.e2e
@pytest.mark.parametrize("entity_count", [1, 4])
@pytest.mark.parametrize("inconsistency_rate", [0.0, 0.6])
def test_enrollment_causality_rules_are_identical_across_all_stages(
	tmp_path, entity_count: int, inconsistency_rate: float
):
	"""E2E: derive canonical causality rules text from every pipeline stage.

	Must satisfy cross-stage equality:
	C(0)=C(1)=C(2)=C(3)=C(4)=C(5)

	- C(0): generator rules text
	- C(1): raw JSON events (fixture)
	- C(2): normalized events
	- C(3): edges list
	- C(4): graph file
	- C(5): Neo4j readback

	When inconsistency_rate==0.0, also assert canonical shape + identical across entities.
	"""
	from enrollment_canonical import canonical_edges, canonical_types
	from event_generator import generate_causality_rules_text, generate_events_batch
	from helpers.enrollment.adapter import load_events_from_file, write_events_to_file
	from helpers.enrollment.causality_rules import (
		CausalityRules,
		causality_rules_from_edges,
		causality_rules_from_events,
		format_causality_rules_text,
	)
	from helpers.enrollment.graph import build_edges, write_graph_to_file
	from helpers.enrollment.neo4j_causality_rules import fetch_causality_model_from_neo4j
	from helpers.enrollment.transformer import normalize_events
	from helpers.neo4j.adapter import load_graph_from_file, write_graph_to_db

	seed = 1337
	run_id = f"e2e:enrollment-causality-xstage:seed={seed}:entities={entity_count}:p={inconsistency_rate}"

	# Stage 0: generator's textual causality rules
	c0 = generate_causality_rules_text(
		seed=seed,
		entity_count=entity_count,
		inconsistency_rate=inconsistency_rate,
		run_id=run_id,
	)

	# Stage 1: raw JSON events fixture
	raw_events = generate_events_batch(
		seed=seed,
		entity_count=entity_count,
		inconsistency_rate=inconsistency_rate,
	)
	fixture_path = tmp_path / "events.json"
	write_events_to_file(events=raw_events, filename=str(fixture_path))
	fixture_events = load_events_from_file(str(fixture_path))
	c1 = format_causality_rules_text(causality_rules_from_events(run_id=run_id, events=fixture_events))

	# Stage 2: normalized
	normalized = normalize_events(fixture_events)
	c2 = format_causality_rules_text(causality_rules_from_events(run_id=run_id, events=normalized))

	# Stage 3: edges
	edges = build_edges(normalized)
	c3 = format_causality_rules_text(
		causality_rules_from_edges(run_id=run_id, events=normalized, edges=edges)
	)

	# Stage 4: graph file
	graph_file = tmp_path / "graph.json"
	write_graph_to_file(events=normalized, edges=edges, run_id=run_id, filename=str(graph_file))
	graph = load_graph_from_file(str(graph_file))
	c4 = format_causality_rules_text(causality_rules_from_edges(run_id=run_id, events=graph["events"], edges=graph["edges"]))

	# Stage 5: Neo4j
	write_graph_to_db(graph)
	neo_model = fetch_causality_model_from_neo4j(run_id=run_id)
	c5 = format_causality_rules_text(
		CausalityRules(
			run_id=neo_model.run_id,
			types_by_entity=neo_model.types_by_entity,
			edges_by_entity=neo_model.edges_by_entity,
		)
	)

	assert c0 == c1 == c2 == c3 == c4 == c5

	if inconsistency_rate == 0.0:
		# canonical shape per entity
		ct = canonical_types()
		ce = canonical_edges()
		for entity_id, types in neo_model.types_by_entity.items():
			assert set(types) == ct
		for entity_id, edges_pairs in neo_model.edges_by_entity.items():
			assert set(edges_pairs) == ce

		# identical across entities
		entities = sorted(neo_model.types_by_entity.keys())
		ref = entities[0]
		for entity_id in entities[1:]:
			assert neo_model.types_by_entity[entity_id] == neo_model.types_by_entity[ref]
			assert neo_model.edges_by_entity[entity_id] == neo_model.edges_by_entity[ref]
