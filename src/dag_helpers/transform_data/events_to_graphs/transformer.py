from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


Event = dict[str, Any]
GraphNode = dict[str, Any]
GraphRelationship = dict[str, Any]
GraphBatch = dict[str, Any]


def read_events_from_file(path: str | Path) -> list[Event]:
	"""Read events from disk (JSON array or NDJSON)."""
	path = Path(path)
	text = path.read_text(encoding="utf-8").strip()
	if not text:
		return []

	if text.startswith("["):
		data = json.loads(text)
		if not isinstance(data, list):
			raise ValueError("events file must contain a top-level JSON array")
		return [dict(e) for e in data]

	events: list[Event] = []
	for line in text.splitlines():
		line = line.strip()
		if not line:
			continue
		events.append(json.loads(line))
	return [dict(e) for e in events]


def build_edges(events: Sequence[Event]) -> list[dict[str, str]]:
	"""Build a causal edge list using declared `parent_event_ids`.

	Edge shape: {"from": <parent_event_id>, "to": <child_event_id>}.
	"""
	by_id = _index_events_by_id(events)

	edges: list[dict[str, str]] = []
	for idx, event in enumerate(events):
		event_id = event.get("event_id")
		if not event_id:
			continue

		parent_ids = _coerce_parent_ids(parent_ids=event.get("parent_event_ids"), idx=idx)
		child_id = str(event_id)
		for parent_id in parent_ids:
			if parent_id == child_id:
				raise ValueError(f"event[{idx}] self-parent edge not allowed: {child_id}")
			if parent_id not in by_id:
				raise ValueError(f"event[{idx}] parent_event_id not found in events: {parent_id}")
			edges.append({"from": parent_id, "to": child_id})

	return sorted(edges, key=lambda e: (e.get("from", ""), e.get("to", "")))


def build_graph_batch(
	*,
	events: Sequence[Event],
	run_id: str | None = None,
	node_label: str = "Event",
	rel_type: str = "CAUSES",
	include_node_properties: Iterable[str] = ("event_type", "event_name"),
) -> GraphBatch:
	"""Build a Neo4j-friendly graph batch.

	Graph batch is intentionally flat and ingestion-friendly:
	- nodes[]: {event_id, labels[], properties{...}}
	- relationships[]: {from, to, type, properties{...}}

	`include_node_properties` selects which event fields to carry onto nodes.
	"""
	by_id = _index_events_by_id(events)
	include = set(include_node_properties)

	nodes: list[GraphNode] = []
	for event_id, event in sorted(by_id.items(), key=lambda kv: kv[0]):
		props: dict[str, Any] = {}
		for key in include:
			if key in event and event[key] is not None:
				props[key] = event[key]

		# Neo4j Browser caption convention: prefer `name`.
		if "event_name" in props and "name" not in props:
			props["name"] = props["event_name"]

		nodes.append(
			{
				"event_id": str(event_id),
				"labels": [node_label],
				"properties": props,
			}
		)

	edges = build_edges(events)
	relationships: list[GraphRelationship] = []
	for edge in edges:
		rel_props: dict[str, Any] = {}
		if run_id is not None:
			rel_props["run_id"] = run_id
		relationships.append(
			{
				"from": edge["from"],
				"to": edge["to"],
				"type": rel_type,
				"properties": rel_props,
			}
		)

	return {
		"graph_type": "neo4j_property_graph_batch",
		"run_id": run_id,
		"nodes": nodes,
		"relationships": relationships,
	}


def write_graph_to_file(*, graph: GraphBatch, path: str | Path) -> Path:
	path = Path(path)
	path.parent.mkdir(parents=True, exist_ok=True)
	path.write_text(json.dumps(graph, indent=2, sort_keys=True) + "\n", encoding="utf-8")
	return path


def read_graph_from_file(path: str | Path) -> GraphBatch:
	path = Path(path)
	text = path.read_text(encoding="utf-8").strip()
	if not text:
		raise ValueError("graph file is empty")
	data = json.loads(text)
	if not isinstance(data, dict):
		raise ValueError("graph file must contain a top-level JSON object")
	return dict(data)


def _index_events_by_id(events: Sequence[Event]) -> dict[str, Event]:
	by_id: dict[str, Event] = {}
	for event in events:
		if not isinstance(event, dict):
			continue
		event_id = event.get("event_id")
		if not event_id:
			continue
		by_id[str(event_id)] = event
	return by_id


def _coerce_parent_ids(*, parent_ids: Any, idx: int) -> list[str]:
	if parent_ids is None:
		return []
	if not isinstance(parent_ids, list):
		raise ValueError(f"event[{idx}] parent_event_ids must be a list")
	return [str(pid) for pid in parent_ids]


def event_names_by_id_from_graph(graph: Mapping[str, Any]) -> dict[str, str]:
	"""Extract {event_id: event_name} mapping from a GraphBatch."""
	names: dict[str, str] = {}
	nodes = graph.get("nodes")
	if not isinstance(nodes, list):
		return names
	for node in nodes:
		if not isinstance(node, dict):
			continue
		event_id = node.get("event_id")
		props = node.get("properties")
		if not event_id or not isinstance(props, dict):
			continue
		name = props.get("event_name") or props.get("name")
		if name:
			names[str(event_id)] = str(name)
	return names


def edges_from_graph(graph: Mapping[str, Any]) -> list[dict[str, str]]:
	rels = graph.get("relationships")
	if not isinstance(rels, list):
		raise ValueError("graph.relationships must be a list")

	edges: list[dict[str, str]] = []
	for idx, rel in enumerate(rels):
		if not isinstance(rel, dict):
			raise ValueError(f"relationship[{idx}] must be a dict")
		if "from" not in rel or "to" not in rel:
			raise ValueError(f"relationship[{idx}] must contain 'from' and 'to'")
		edges.append({"from": str(rel["from"]), "to": str(rel["to"])})

	return sorted(edges, key=lambda e: (e.get("from", ""), e.get("to", "")))
