from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Sequence


Event = dict[str, Any]
Edge = dict[str, str]


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


def read_edges_from_file(path: str | Path) -> list[Edge]:
	"""Read edges from disk (JSON array)."""
	path = Path(path)
	text = path.read_text(encoding="utf-8").strip()
	if not text:
		return []

	data = json.loads(text)
	if not isinstance(data, list):
		raise ValueError("edges file must contain a top-level JSON array")

	edges: list[Edge] = []
	for idx, edge in enumerate(data):
		if not isinstance(edge, dict):
			raise ValueError(f"edge[{idx}] must be a dict")
		if "from" not in edge or "to" not in edge:
			raise ValueError(f"edge[{idx}] must contain 'from' and 'to'")
		edges.append({"from": str(edge["from"]), "to": str(edge["to"])})

	return edges


def build_edges(events: Sequence[Event]) -> list[Edge]:
	"""Build a causal edge list using declared `parent_event_ids`.

	No workflow inference: upstream declares causality; downstream validates and materializes.

	Edge shape matches the prototype: {"from": <parent_event_id>, "to": <child_event_id>}.
	"""
	by_id = _index_events_by_id(events)

	edges: list[Edge] = []
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

	# Make output deterministic.
	return sorted(edges, key=lambda e: (e.get("from", ""), e.get("to", "")))


def write_edges_to_file(*, edges: Iterable[Edge], path: str | Path) -> Path:
	"""Write edges as a JSON array."""
	path = Path(path)
	path.parent.mkdir(parents=True, exist_ok=True)
	payload = list(edges)
	path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
	return path


def transform_edges_to_canonical_baseline_format(edges: Iterable[Edge]) -> list[Edge]:
	"""Make edges diff-friendly and stable (canonical baseline).

	Baseline rules:
	- retain only {from, to}
	- coerce values to strings
	- stable ordering by (from, to)
	"""
	baseline: list[Edge] = []
	for idx, edge in enumerate(edges):
		if not isinstance(edge, dict):
			raise ValueError(f"edge[{idx}] must be a dict")
		if "from" not in edge or "to" not in edge:
			raise ValueError(f"edge[{idx}] must contain 'from' and 'to'")
		baseline.append({"from": str(edge["from"]), "to": str(edge["to"])})

	return sorted(baseline, key=lambda e: (e.get("from", ""), e.get("to", "")))


def save_pre_transformation_canonical_baseline_artifact(
	*,
	fixture_data: Iterable[Edge],
	path: str | Path,
) -> Path:
	"""Persist a canonical edge baseline before this transformation step."""
	path = Path(path)
	path.parent.mkdir(parents=True, exist_ok=True)
	baseline = transform_edges_to_canonical_baseline_format(fixture_data)
	path.write_text(json.dumps(baseline, indent=2, sort_keys=True) + "\n", encoding="utf-8")
	return path


def save_post_transformation_canonical_baseline_artifact(
	*,
	edges: Iterable[Edge],
	path: str | Path,
) -> Path:
	"""Persist a canonical edge baseline after this transformation step."""
	path = Path(path)
	path.parent.mkdir(parents=True, exist_ok=True)
	baseline = transform_edges_to_canonical_baseline_format(edges)
	path.write_text(json.dumps(baseline, indent=2, sort_keys=True) + "\n", encoding="utf-8")
	return path


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


def _edge(*, parent_id: str, child_id: str) -> Edge:
	return {"from": parent_id, "to": child_id}
