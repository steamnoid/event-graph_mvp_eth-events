import json
import os

from neo4j import GraphDatabase


def event_display_name(event: dict) -> str | None:
	name = event.get("event_name")
	order = event.get("event_order")
	if order and name:
		return f"{order} {name}"
	return name


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
			# Treat run_id as a snapshot: overwrite any previous contents.
			session.run(
				"MATCH (r:Run {run_id: $run_id}) DETACH DELETE r",
				run_id=run_id,
			)
			session.run("MERGE (r:Run {run_id: $run_id})", run_id=run_id)

			for event in events:
				event_id = event.get("event_id")
				if not event_id:
					continue

				name = event_display_name(event)

				session.run(
					"""
					MERGE (e:Event {event_id: $event_id})
					SET e.tx_hash = $tx_hash,
						e.log_index = $log_index,
						e.event_name = $event_name,
						e.name = $name,
						e.display_name = $display_name,
						e.trace_address = $trace_address,
						e.trace_type = $trace_type,
						e.event_order = $event_order,
						e.from_address = $from_address,
						e.to_address = $to_address,
						e.call_type = $call_type,
						e.value = $value,
						e.input = $input,
						e.input_sig = $input_sig
					""",
					event_id=event_id,
					tx_hash=event.get("tx_hash"),
					log_index=event.get("log_index"),
					event_name=event.get("event_name"),
					name=name,
					display_name=name,
					trace_address=event.get("trace_address"),
					trace_type=event.get("type"),
					event_order=event.get("event_order"),
					from_address=event.get("from_address"),
					to_address=event.get("to_address"),
					call_type=event.get("call_type"),
					value=event.get("value"),
					input=event.get("input"),
					input_sig=event.get("input_sig"),
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
