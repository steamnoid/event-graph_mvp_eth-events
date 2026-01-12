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
	graph_hash = graph.get("graph_hash")
	events = graph.get("events") or []
	edges = graph.get("edges") or []

	uri, user, password = _neo4j_config()
	driver = GraphDatabase.driver(uri, auth=(user, password))
	try:
		with driver.session() as session:
			# Treat run_id as a snapshot: overwrite any previous contents.
			session.run(
				"MATCH (r:Run {run_id: $run_id}) DETACH DELETE r",
				run_id=run_id,
			)
			session.run(
				"MERGE (r:Run {run_id: $run_id}) SET r.graph_hash = $graph_hash",
				run_id=run_id,
				graph_hash=graph_hash,
			)

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


def compute_graph_hash_from_db(run_id: str) -> str:
	"""Compute a deterministic hash of what is actually stored in Neo4j for a given run.

	This hashes the same subset of fields as helpers.neo4j.transformer.compute_graph_hash,
	but sources them from Neo4j nodes/relationships instead of a file payload.
	"""
	from helpers.neo4j.transformer import compute_graph_hash

	uri, user, password = _neo4j_config()
	driver = GraphDatabase.driver(uri, auth=(user, password))
	try:
		with driver.session() as session:
			events_rows = session.run(
				"""
				MATCH (r:Run {run_id: $run_id})-[:INCLUDES]->(e:Event)
				RETURN e.event_id AS event_id, e.tx_hash AS tx_hash, e.log_index AS log_index, e.event_name AS event_name
				""",
				run_id=run_id,
			)
			events = [
				{
					"event_id": row["event_id"],
					"tx_hash": row["tx_hash"],
					"log_index": row["log_index"],
					"event_name": row["event_name"],
				}
				for row in events_rows
			]

			edges_rows = session.run(
				"""
				MATCH (r:Run {run_id: $run_id})-[:INCLUDES]->(a:Event)
				MATCH (r:Run {run_id: $run_id})-[:INCLUDES]->(b:Event)
				MATCH (a)-[:CAUSES]->(b)
				RETURN a.event_id AS from, b.event_id AS to
				""",
				run_id=run_id,
			)
			edges = [{"from": row["from"], "to": row["to"]} for row in edges_rows]

		return compute_graph_hash(events=events, edges=edges)
	finally:
		driver.close()
