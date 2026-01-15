from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import pytest


# Ensure `pytest -m invariants` selects this suite.
pytestmark = pytest.mark.invariants


_NEO4J_LAST_ERROR: str | None = None


def _neo4j_config() -> tuple[str, str, str]:
	# Prefer bolt:// for single-instance Docker setups (avoids routing discovery issues).
	uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
	user = os.getenv("NEO4J_USER", "neo4j")
	password = os.getenv("NEO4J_PASSWORD", "test")
	return uri, user, password


def _neo4j_is_reachable() -> bool:
	"""Best-effort reachability check so invariant tests skip cleanly."""
	global _NEO4J_LAST_ERROR
	_NEO4J_LAST_ERROR = None
	try:
		from neo4j import GraphDatabase  # type: ignore
		from neo4j.exceptions import Neo4jError  # type: ignore
	except Exception:
		_NEO4J_LAST_ERROR = "neo4j Python driver not importable"
		return False

	uri, user, password = _neo4j_config()
	try:
		driver = GraphDatabase.driver(uri, auth=(user, password))
		try:
			with driver.session() as session:
				session.run("RETURN 1 AS ok").single()
		finally:
			driver.close()
		return True
	except (OSError, Neo4jError, Exception) as e:
		_NEO4J_LAST_ERROR = repr(e)
		return False


@pytest.fixture(scope="module")
def neo4j_driver():
	uri, user, password = _neo4j_config()
	if not _neo4j_is_reachable():
		reason = _NEO4J_LAST_ERROR or "unknown"
		pytest.skip(f"Neo4j not reachable on {uri} ({reason})")

	from neo4j import GraphDatabase  # type: ignore

	driver = GraphDatabase.driver(uri, auth=(user, password))
	try:
		yield driver
	finally:
		driver.close()


@pytest.fixture(scope="module")
def invariant_run_id(tmp_path_factory: pytest.TempPathFactory, neo4j_driver) -> str:
	"""Create a fresh, known-consistent dataset in Neo4j for invariant checks.

	We intentionally build the dataset through the pipeline stages (generate -> enhance -> graph -> Neo4j)
	so these tests validate the real production data shape stored in Neo4j.
	"""
	from dag_helpers.fetch_data.task import fetch_data
	from dag_helpers.store_data.neo4j.task import store_edges_in_neo4j
	from dag_helpers.transform_data.enhance_data.task import enhance_data
	from dag_helpers.transform_data.events_to_graphs.task import events_to_graphs

	tmp_path = tmp_path_factory.mktemp("neo4j_invariants")
	run_id = f"inv:{uuid4().hex}"

	# Stage 1: generate (consistent mode)
	_ref_events_baseline, _ref_edges_baseline = fetch_data(
		artifact_dir=tmp_path / "artifacts_fetch",
		out_events=tmp_path / "events.ndjson",
		out_rules=tmp_path / "rules.txt",
		format="ndjson",
		seed=20260115,
		entity_count=6,
		inconsistency_rate=0.0,
		missing_event_rate=0.0,
		run_id=run_id,
	)

	# Stage 2: enhance
	_enhance_baseline, enhanced_events_path = enhance_data(
		artifact_dir=tmp_path / "artifacts_enhance",
		source_events=tmp_path / "events.ndjson",
		out_events=tmp_path / "enhanced_events.ndjson",
		format="ndjson",
	)

	# Stage 3: graph batch
	_edges_baseline, graph_path = events_to_graphs(
		artifact_dir=tmp_path / "artifacts_graphs",
		source_events=enhanced_events_path,
		out_graph=tmp_path / "graph.json",
		run_id=run_id,
	)

	# Stage 4: store in Neo4j
	store_edges_in_neo4j(
		artifact_dir=tmp_path / "artifacts_neo4j",
		source_graph=graph_path,
		run_id=run_id,
		clear_run_first=True,
	)

	yield run_id

	# Cleanup: remove only the nodes for this run.
	with neo4j_driver.session() as session:
		session.run("MATCH (n:Event {run_id: $run_id}) DETACH DELETE n", run_id=run_id)


def _scalar(session, query: str, **params) -> int:
	record = session.run(query, **params).single()
	if not record:
		raise AssertionError("Query returned no rows")
	# Return the first column value.
	key = list(record.keys())[0]
	return int(record[key])


# Authoritative catalog used by invariants.
CATALOG_LAYER: dict[str, str] = {
	"CourseEnrollmentRequested": "L0",
	"PaymentProcessingRequested": "L1",
	"PaymentConfirmed": "L2",
	"UserProfileLoaded": "L3",
	"CourseAccessRequested": "L3",
	"EligibilityChecked": "L4",
	"ContentAvailabilityChecked": "L4",
	"EligibilityPassed": "L5",
	"ContentAvailable": "L5",
	"ContentPrepared": "L6",
	"AccessGranted": "L6",
	"EnrollmentCompleted": "L7",
	"EnrollmentArchived": "L8",
}

CATALOG_KIND: dict[str, str] = {
	"CourseEnrollmentRequested": "fact",
	"PaymentProcessingRequested": "fact",
	"PaymentConfirmed": "fact",
	"UserProfileLoaded": "fact",
	"CourseAccessRequested": "fact",
	"EligibilityChecked": "fact",
	"ContentAvailabilityChecked": "fact",
	"EligibilityPassed": "fact",
	"ContentAvailable": "fact",
	"ContentPrepared": "fact",
	"EnrollmentArchived": "fact",
	"AccessGranted": "decision",
	"EnrollmentCompleted": "decision",
}


@pytest.mark.invariants
def test_invariant_nodes_exist_for_run(invariant_run_id: str, neo4j_driver) -> None:
	# Invariant: The stored run must materialize at least one Event node; an empty run graph is invalid.
	with neo4j_driver.session() as session:
		count = _scalar(session, "MATCH (e:Event {run_id: $run_id}) RETURN count(e)", run_id=invariant_run_id)
		assert count > 0


@pytest.mark.invariants
def test_invariant_event_id_unique_within_run(invariant_run_id: str, neo4j_driver) -> None:
	# Invariant: event_id must be unique within a run_id partition (no duplicate nodes per (run_id,event_id)).
	with neo4j_driver.session() as session:
		dupes = _scalar(
			session,
			"""
			MATCH (e:Event {run_id: $run_id})
			WITH e.event_id AS event_id, count(*) AS n
			WHERE n > 1
			RETURN count(*) AS dupes
			""",
			run_id=invariant_run_id,
		)
		assert dupes == 0


@pytest.mark.invariants
def test_invariant_required_schema_fields_present_on_nodes(invariant_run_id: str, neo4j_driver) -> None:
	# Invariant: Every stored node must include the core schema fields required for downstream graph validation.
	with neo4j_driver.session() as session:
		missing = _scalar(
			session,
			"""
			MATCH (e:Event {run_id: $run_id})
			WHERE e.event_id IS NULL
			   OR e.event_type IS NULL
			   OR e.event_kind IS NULL
			   OR e.parent_event_ids IS NULL
			   OR e.layer IS NULL
			   OR e.entity_id IS NULL
			   OR e.payload IS NULL
			   OR e.emitted_at IS NULL
			RETURN count(e) AS missing
			""",
			run_id=invariant_run_id,
		)
		assert missing == 0


@pytest.mark.invariants
def test_invariant_event_kind_is_fact_or_decision(invariant_run_id: str, neo4j_driver) -> None:
	# Invariant: event_kind is a closed enum and must be either 'fact' or 'decision' for every node.
	with neo4j_driver.session() as session:
		bad = _scalar(
			session,
			"""
			MATCH (e:Event {run_id: $run_id})
			WHERE NOT e.event_kind IN ['fact', 'decision']
			RETURN count(e) AS bad
			""",
			run_id=invariant_run_id,
		)
		assert bad == 0


@pytest.mark.invariants
def test_invariant_event_type_is_in_authoritative_catalog(invariant_run_id: str, neo4j_driver) -> None:
	# Invariant: event_type must be restricted to the authoritative catalog (no invented event types).
	allowed = list(CATALOG_LAYER.keys())
	with neo4j_driver.session() as session:
		bad = _scalar(
			session,
			"""
			MATCH (e:Event {run_id: $run_id})
			WHERE NOT e.event_type IN $allowed
			RETURN count(e) AS bad
			""",
			run_id=invariant_run_id,
			allowed=allowed,
		)
		assert bad == 0


@pytest.mark.invariants
def test_invariant_layer_matches_catalog_for_each_event_type(invariant_run_id: str, neo4j_driver) -> None:
	# Invariant: layer is dictated by the catalog and must match the expected layer for each event_type.
	with neo4j_driver.session() as session:
		rows = session.run(
			"""MATCH (e:Event {run_id: $run_id}) RETURN e.event_type AS t, e.layer AS layer""",
			run_id=invariant_run_id,
		)
		bad = 0
		for r in rows:
			t = str(r["t"])
			layer = str(r["layer"])
			if CATALOG_LAYER.get(t) != layer:
				bad += 1
		assert bad == 0


@pytest.mark.invariants
def test_invariant_event_kind_matches_catalog_for_each_event_type(invariant_run_id: str, neo4j_driver) -> None:
	# Invariant: event_kind is dictated by the catalog and must match the expected kind for each event_type.
	with neo4j_driver.session() as session:
		rows = session.run(
			"""MATCH (e:Event {run_id: $run_id}) RETURN e.event_type AS t, e.event_kind AS k""",
			run_id=invariant_run_id,
		)
		bad = 0
		for r in rows:
			t = str(r["t"])
			k = str(r["k"])
			if CATALOG_KIND.get(t) != k:
				bad += 1
		assert bad == 0


@pytest.mark.invariants
def test_invariant_parent_event_ids_is_list_of_strings(invariant_run_id: str, neo4j_driver) -> None:
	# Invariant: parent_event_ids must be a list (possibly empty) and every element must be string-coercible.
	with neo4j_driver.session() as session:
		rows = session.run(
			"""MATCH (e:Event {run_id: $run_id}) RETURN e.parent_event_ids AS p""",
			run_id=invariant_run_id,
		)
		for r in rows:
			p = r["p"]
			assert isinstance(p, list)
			for item in p:
				assert isinstance(item, str)


@pytest.mark.invariants
def test_invariant_fact_events_have_at_most_one_parent(invariant_run_id: str, neo4j_driver) -> None:
	# Invariant: fact events must declare 0 or 1 parents (multi-parent is forbidden for facts).
	with neo4j_driver.session() as session:
		violations = _scalar(
			session,
			"""
			MATCH (e:Event {run_id: $run_id, event_kind: 'fact'})
			WHERE size(e.parent_event_ids) > 1
			RETURN count(e) AS violations
			""",
			run_id=invariant_run_id,
		)
		assert violations == 0


@pytest.mark.invariants
def test_invariant_course_enrollment_requested_has_zero_parents(invariant_run_id: str, neo4j_driver) -> None:
	# Invariant: CourseEnrollmentRequested is the root fact and must declare zero parents.
	with neo4j_driver.session() as session:
		violations = _scalar(
			session,
			"""
			MATCH (e:Event {run_id: $run_id, event_type: 'CourseEnrollmentRequested'})
			WHERE size(e.parent_event_ids) <> 0
			RETURN count(e) AS violations
			""",
			run_id=invariant_run_id,
		)
		assert violations == 0


@pytest.mark.invariants
def test_invariant_no_self_loops_in_causality_edges(invariant_run_id: str, neo4j_driver) -> None:
	# Invariant: The causality graph must not contain self-loop edges (an event cannot be its own parent).
	with neo4j_driver.session() as session:
		loops = _scalar(
			session,
			"""
			MATCH (a:Event {run_id: $run_id})-[r:CAUSES {run_id: $run_id}]->(b:Event {run_id: $run_id})
			WHERE a.event_id = b.event_id
			RETURN count(r) AS loops
			""",
			run_id=invariant_run_id,
		)
		assert loops == 0


@pytest.mark.invariants
def test_invariant_parent_event_ids_count_matches_incoming_edge_count(invariant_run_id: str, neo4j_driver) -> None:
	# Invariant: For every node, the number of declared parent_event_ids must equal the number of incoming CAUSES edges.
	with neo4j_driver.session() as session:
		mismatches = _scalar(
			session,
			"""
			MATCH (e:Event {run_id: $run_id})
			OPTIONAL MATCH (:Event {run_id: $run_id})-[r:CAUSES {run_id: $run_id}]->(e)
			WITH e, count(r) AS indeg
			WHERE indeg <> size(e.parent_event_ids)
			RETURN count(e) AS mismatches
			""",
			run_id=invariant_run_id,
		)
		assert mismatches == 0


@pytest.mark.invariants
def test_invariant_parent_event_ids_match_incoming_parent_event_ids(invariant_run_id: str, neo4j_driver) -> None:
	# Invariant: For every node, the declared parent_event_ids set must exactly match the set of incoming parent node event_id values.
	with neo4j_driver.session() as session:
		rows = session.run(
			"""
			MATCH (e:Event {run_id: $run_id})
			OPTIONAL MATCH (p:Event {run_id: $run_id})-[:CAUSES {run_id: $run_id}]->(e)
			RETURN e.event_id AS child_id, e.parent_event_ids AS declared, collect(p.event_id) AS incoming
			""",
			run_id=invariant_run_id,
		)
		for r in rows:
			declared = sorted(r["declared"]) if r["declared"] else []
			incoming = sorted([x for x in r["incoming"] if x is not None])
			assert declared == incoming


@pytest.mark.invariants
def test_invariant_enrollment_completed_has_exactly_one_parent_and_it_is_access_granted(
	invariant_run_id: str, neo4j_driver
) -> None:
	# Invariant: EnrollmentCompleted must have exactly one parent, and that parent must be an AccessGranted event.
	with neo4j_driver.session() as session:
		offenders = _scalar(
			session,
			"""
			MATCH (ec:Event {run_id: $run_id, event_type: 'EnrollmentCompleted'})
			OPTIONAL MATCH (p:Event {run_id: $run_id})-[:CAUSES {run_id: $run_id}]->(ec)
			WITH ec, count(p) AS indeg, collect(DISTINCT p.event_type) AS pt
			WHERE indeg <> 1 OR size(pt) <> 1 OR pt[0] <> 'AccessGranted'
			RETURN count(ec) AS offenders
			""",
			run_id=invariant_run_id,
		)
		assert offenders == 0


@pytest.mark.invariants
def test_invariant_access_granted_has_exactly_three_required_parents(invariant_run_id: str, neo4j_driver) -> None:
	# Invariant: AccessGranted must declare exactly the three canonical semantic parents: PaymentConfirmed, EligibilityPassed, ContentPrepared.
	with neo4j_driver.session() as session:
		offenders = _scalar(
			session,
			"""
			MATCH (ag:Event {run_id: $run_id, event_type: 'AccessGranted'})
			MATCH (p:Event {run_id: $run_id})-[:CAUSES {run_id: $run_id}]->(ag)
			WITH ag, count(p) AS indeg, collect(DISTINCT p.event_type) AS pt
			WHERE indeg <> 3
			   OR size(pt) <> 3
			   OR NOT all(t IN ['PaymentConfirmed','EligibilityPassed','ContentPrepared'] WHERE t IN pt)
			RETURN count(ag) AS offenders
			""",
			run_id=invariant_run_id,
		)
		assert offenders == 0


@pytest.mark.invariants
def test_invariant_enrollment_archived_parent_is_enrollment_completed(invariant_run_id: str, neo4j_driver) -> None:
	# Invariant: EnrollmentArchived must have EnrollmentCompleted as its parent (canonical causal declaration).
	with neo4j_driver.session() as session:
		offenders = _scalar(
			session,
			"""
			MATCH (arch:Event {run_id: $run_id, event_type: 'EnrollmentArchived'})
			WHERE NOT EXISTS {
				MATCH (:Event {run_id: $run_id, event_type: 'EnrollmentCompleted'})-[:CAUSES {run_id: $run_id}]->(arch)
			}
			RETURN count(arch) AS offenders
			""",
			run_id=invariant_run_id,
		)
		assert offenders == 0


@pytest.mark.invariants
def test_invariant_component_id_present_on_all_nodes(invariant_run_id: str, neo4j_driver) -> None:
	# Invariant: Every node must be assigned a component_id identifying its connected sub-graph within the run.
	with neo4j_driver.session() as session:
		missing = _scalar(
			session,
			"""MATCH (e:Event {run_id: $run_id}) WHERE e.component_id IS NULL RETURN count(e) AS missing""",
			run_id=invariant_run_id,
		)
		assert missing == 0


@pytest.mark.invariants
def test_invariant_component_id_present_on_all_relationships(invariant_run_id: str, neo4j_driver) -> None:
	# Invariant: Every CAUSES relationship must carry component_id so sub-graphs can be filtered without recomputation.
	with neo4j_driver.session() as session:
		missing = _scalar(
			session,
			"""
			MATCH ()-[r:CAUSES {run_id: $run_id}]->()
			WHERE r.component_id IS NULL
			RETURN count(r) AS missing
			""",
			run_id=invariant_run_id,
		)
		assert missing == 0


@pytest.mark.invariants
def test_invariant_component_id_consistent_between_nodes_and_relationships(invariant_run_id: str, neo4j_driver) -> None:
	# Invariant: For every CAUSES edge, start node, end node, and relationship must all share the same component_id.
	with neo4j_driver.session() as session:
		offenders = _scalar(
			session,
			"""
			MATCH (a:Event {run_id: $run_id})-[r:CAUSES {run_id: $run_id}]->(b:Event {run_id: $run_id})
			WHERE a.component_id IS NULL
			   OR b.component_id IS NULL
			   OR r.component_id IS NULL
			   OR a.component_id <> b.component_id
			   OR r.component_id <> a.component_id
			RETURN count(r) AS offenders
			""",
			run_id=invariant_run_id,
		)
		assert offenders == 0


@pytest.mark.invariants
def test_invariant_component_id_does_not_mix_entity_ids(invariant_run_id: str, neo4j_driver) -> None:
	# Invariant: A connected component must not contain nodes from multiple entity_id values.
	with neo4j_driver.session() as session:
		mixed = _scalar(
			session,
			"""
			MATCH (e:Event {run_id: $run_id})
			WITH e.component_id AS cid, collect(DISTINCT e.entity_id) AS entities
			WHERE size(entities) <> 1
			RETURN count(*) AS mixed
			""",
			run_id=invariant_run_id,
		)
		assert mixed == 0


@pytest.mark.invariants
def test_invariant_relationship_run_id_matches_node_run_id(invariant_run_id: str, neo4j_driver) -> None:
	# Invariant: Relationship run_id must match both endpoint nodes' run_id to avoid cross-run edges.
	with neo4j_driver.session() as session:
		offenders = _scalar(
			session,
			"""
			MATCH (a:Event)-[r:CAUSES]->(b:Event)
			WHERE r.run_id = $run_id AND (a.run_id <> $run_id OR b.run_id <> $run_id)
			RETURN count(r) AS offenders
			""",
			run_id=invariant_run_id,
		)
		assert offenders == 0


@pytest.mark.invariants
def test_invariant_payload_is_valid_json_string(invariant_run_id: str, neo4j_driver) -> None:
	# Invariant: payload must be stored as a JSON string so it remains machine-parseable from Neo4j properties.
	with neo4j_driver.session() as session:
		rows = session.run(
			"""MATCH (e:Event {run_id: $run_id}) RETURN e.payload AS payload LIMIT 50""",
			run_id=invariant_run_id,
		)
		for r in rows:
			payload = r["payload"]
			assert isinstance(payload, str)
			json.loads(payload)


@pytest.mark.invariants
def test_invariant_emitted_at_is_parseable_timestamp_string(invariant_run_id: str, neo4j_driver) -> None:
	# Invariant: emitted_at must be a non-empty string timestamp that can be parsed into a datetime.
	def _parse(ts: str) -> datetime:
		# Accept both ISO-8601 with 'Z' and Python's fromisoformat.
		if ts.endswith("Z"):
			ts = ts[:-1] + "+00:00"
		return datetime.fromisoformat(ts)

	with neo4j_driver.session() as session:
		rows = session.run(
			"""MATCH (e:Event {run_id: $run_id}) RETURN e.emitted_at AS ts LIMIT 50""",
			run_id=invariant_run_id,
		)
		for r in rows:
			ts = r["ts"]
			assert isinstance(ts, str)
			assert ts.strip() != ""
			_parse(ts)
