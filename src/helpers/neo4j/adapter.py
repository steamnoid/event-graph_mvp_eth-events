import json
import os
import re
from typing import Optional

from neo4j import GraphDatabase


_CAMEL_1 = re.compile(r"([A-Z]+)([A-Z][a-z])")
_CAMEL_2 = re.compile(r"([a-z0-9])([A-Z])")


def _camelcase_to_words(value: Optional[str]) -> Optional[str]:
	if not value:
		return None
	# Split: "HTTPServerError" -> "HTTP Server Error", "CourseEnrollmentRequested" -> "Course Enrollment Requested"
	value = _CAMEL_1.sub(r"\1 \2", value)
	value = _CAMEL_2.sub(r"\1 \2", value)
	return value


def load_graph_from_file(filename: str) -> dict:
	with open(filename, "r", encoding="utf-8") as f:
		return json.load(f)


def task_write_graph_to_neo4j(graph_file: str, **_context) -> int:
	"""Airflow task callable: persist graph.json to Neo4j."""
	graph = load_graph_from_file(graph_file)
	write_graph_to_db(graph)
	return 0


def _neo4j_config() -> tuple[str, str, str]:
	uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
	user = os.getenv("NEO4J_USER", "neo4j")
	password = os.getenv("NEO4J_PASSWORD", "test")
	return uri, user, password


def write_graph_to_db(graph: dict) -> None:
	run_id = graph.get("run_id")
	events = graph.get("events") or []
	edges = graph.get("edges") or []

	# Keep snapshot semantics deterministic: do not accumulate stale edges across runs
	# for the same set of events.
	event_ids_in_run = [
		e.get("event_id")
		for e in events
		if isinstance(e, dict) and e.get("event_id")
	]

	uri, user, password = _neo4j_config()
	driver = GraphDatabase.driver(uri, auth=(user, password))
	try:
		with driver.session() as session:
			# Treat run_id as a snapshot: overwrite any previous contents.
			session.run(
				"MATCH (r:Run {run_id: $run_id}) DETACH DELETE r",
				run_id=run_id,
			)
			session.run("MERGE (r:Run {run_id: $run_id})", run_id=run_id)

			# Remove any existing CAUSES edges between events in this snapshot.
			if event_ids_in_run:
				session.run(
					"""
					MATCH (n:Event)
					WHERE n.event_id IN $event_ids
					WITH collect(n) AS nodes
					UNWIND nodes AS n1
					OPTIONAL MATCH (n1)-[rel:CAUSES]->()
					DELETE rel
					WITH nodes
					UNWIND nodes AS n2
					OPTIONAL MATCH ()-[rel2:CAUSES]->(n2)
					DELETE rel2
					""",
					event_ids=event_ids_in_run,
				)

			for event in events:
				event_id = event.get("event_id")
				if not event_id:
					continue

				# Enrollment events carry `entity_id` and `event_type`; ETH events carry `tx_hash` etc.
				entity_id = event.get("entity_id")
				event_type = event.get("event_type")
				event_kind = event.get("event_kind")
				layer = event.get("layer")
				emitted_at = event.get("emitted_at")
				event_type_words = _camelcase_to_words(event_type)
				event_name = event.get("event_name") or event_type
				# Make Enrollment nodes readable in Neo4j Browser without custom styling.
				# Keep `event_type` canonical; only adjust the display-oriented `event_name`.
				if entity_id is not None and event_type_words:
					event_name = event_type_words
				tx_hash = event.get("tx_hash")
				log_index = event.get("log_index")

				session.run(
					"""
					MERGE (e:Event {event_id: $event_id})
					SET e.tx_hash = $tx_hash,
						e.log_index = $log_index,
						e.event_name = $event_name,
						e.entity_id = $entity_id,
						e.event_type = $event_type,
						e.event_type_words = $event_type_words,
						e.event_kind = $event_kind,
						e.layer = $layer,
						e.emitted_at = $emitted_at
					FOREACH (_ IN CASE WHEN $entity_id IS NULL THEN [] ELSE [1] END | SET e:EnrollmentEvent)
					FOREACH (_ IN CASE WHEN $tx_hash IS NULL THEN [] ELSE [1] END | SET e:EthEvent)
					""",
					event_id=event_id,
					tx_hash=tx_hash,
					log_index=log_index,
					event_name=event_name,
					entity_id=entity_id,
					event_type=event_type,
					event_type_words=event_type_words,
					event_kind=event_kind,
					layer=layer,
					emitted_at=emitted_at,
				)

				session.run(
					"""
					MATCH (r:Run {run_id: $run_id})
					MATCH (e:Event {event_id: $event_id})
					MERGE (r)-[:INCLUDES]->(e)
					""",
					run_id=run_id,
					event_id=event_id,
				)

			for edge in edges:
				parent_id = edge.get("from")
				child_id = edge.get("to")
				if not parent_id or not child_id:
					continue

				session.run(
					"""
					MATCH (a:Event {event_id: $parent_id})
					MATCH (b:Event {event_id: $child_id})
					MERGE (a)-[:CAUSES {run_id: $run_id}]->(b)
					""",
					parent_id=parent_id,
					child_id=child_id,
					run_id=run_id,
				)
	finally:
		driver.close()
