import json
from typing import Any, Dict, List


Event = dict[str, Any]
Edge = Dict[str, str]


def task_transform_normalized_to_edges_file(normalized_file: str, **context) -> str:
	"""Airflow task callable: Stage C3 transform."""
	from helpers.enrollment import pipeline

	run_id = str(context.get("run_id") or "manual")
	return pipeline.transform_normalized_to_edges_file(run_id=run_id, normalized_file=normalized_file)


def task_transform_edges_to_graph_file(normalized_file: str, edges_file: str, **context) -> str:
	"""Airflow task callable: Stage C4 transform."""
	from helpers.enrollment import pipeline

	run_id = str(context.get("run_id") or "manual")
	return pipeline.transform_edges_to_graph_file(
		run_id=run_id,
		normalized_file=normalized_file,
		edges_file=edges_file,
	)


def build_edges(events: List[Event]) -> List[Edge]:
	"""Build a causal edge list using declared `parent_event_ids`.

	No workflow inference: upstream declares causality; downstream validates and materializes.
	"""
	by_id = _index_events_by_id(events)

	edges: List[Edge] = []
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
			edges.append(_edge(parent_id=parent_id, child_id=child_id))

	return edges


def write_graph_to_file(*, events: List[Event], edges: List[Edge], run_id: str, filename: str) -> None:
	graph = {"run_id": run_id, "events": events, "edges": edges}
	with open(filename, "w", encoding="utf-8") as f:
		json.dump(graph, f)


def _index_events_by_id(events: List[Event]) -> dict[str, Event]:
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


def _edge(parent_id: str, child_id: str) -> Edge:
	return {"from": parent_id, "to": child_id}
