import json
import os

from neo4j import GraphDatabase


def load_graph_from_file(filename: str) -> dict:
	with open(filename, "r", encoding="utf-8") as f:
		return json.load(f)


def _neo4j_config() -> tuple[str, str, str]:
	uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
	user = os.getenv("NEO4J_USER", "neo4j")
	password = os.getenv("NEO4J_PASSWORD", "test")
	return uri, user, password


def write_graph_to_db(graph: dict) -> None:
	run_id = graph.get("run_id")
	events = graph.get("events") or []
	edges = graph.get("edges") or []

	uri, user, password = _neo4j_config()
	driver = GraphDatabase.driver(uri, auth=(user, password))
	try:
		with driver.session() as session:
			session.run("MERGE (r:Run {run_id: $run_id})", run_id=run_id)

			for event in events:
				event_id = event.get("event_id")
				if not event_id:
					continue

				session.run(
					"""
					MERGE (e:Event {event_id: $event_id})
					SET e.tx_hash = $tx_hash,
						e.log_index = $log_index,
						e.event_name = $event_name
					""",
					event_id=event_id,
					tx_hash=event.get("tx_hash"),
					log_index=event.get("log_index"),
					event_name=event.get("event_name"),
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
					MERGE (a)-[:CAUSES]->(b)
					""",
					parent_id=parent_id,
					child_id=child_id,
				)
	finally:
		driver.close()
