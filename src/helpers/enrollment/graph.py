import json
from typing import Any, Dict, List


Event = dict[str, Any]
Edge = Dict[str, str]


def build_edges(events: List[Event]) -> List[Edge]:
	"""Build a causal edge list using declared `parent_event_ids`.

	No workflow inference: upstream declares causality; downstream validates and materializes.
	"""
	by_id = {e.get("event_id"): e for e in events if isinstance(e, dict) and e.get("event_id")}

	edges: List[Edge] = []
	for idx, event in enumerate(events):
		event_id = event.get("event_id")
		if not event_id:
			continue

		parent_ids = event.get("parent_event_ids") or []
		if not isinstance(parent_ids, list):
			raise ValueError(f"event[{idx}] parent_event_ids must be a list")

		for parent_id in parent_ids:
			if parent_id == event_id:
				raise ValueError(f"event[{idx}] self-parent edge not allowed: {event_id}")
			if parent_id not in by_id:
				raise ValueError(f"event[{idx}] parent_event_id not found in events: {parent_id}")
			edges.append({"from": parent_id, "to": event_id})

	return edges


def write_graph_to_file(*, events: List[Event], edges: List[Edge], run_id: str, filename: str) -> None:
	graph = {"run_id": run_id, "events": events, "edges": edges}
	with open(filename, "w", encoding="utf-8") as f:
		json.dump(graph, f)
