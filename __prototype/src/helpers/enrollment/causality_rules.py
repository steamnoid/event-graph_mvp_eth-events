from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple


Event = dict[str, Any]
Edge = dict[str, str]


@dataclass(frozen=True)
class CausalityRules:
	run_id: str
	types_by_entity: dict[str, list[str]]
	edges_by_entity: dict[str, list[tuple[str, str]]]


def render_causality_rules(rules: CausalityRules) -> str:
	lines: list[str] = []
	lines.append(f"run_id={rules.run_id}")

	all_entities = sorted(set(rules.types_by_entity.keys()) | set(rules.edges_by_entity.keys()))
	for entity_id in all_entities:
		lines.append("")
		lines.append(f"entity_id={entity_id}")
		lines.append("types:")
		for t in sorted(set(rules.types_by_entity.get(entity_id) or [])):
			lines.append(f"  - {t}")
		lines.append("edges:")
		for parent_type, child_type in sorted(set(rules.edges_by_entity.get(entity_id) or [])):
			lines.append(f"  - {parent_type} -> {child_type}")

	return "\n".join(lines) + "\n"


def parse_causality_rules(text: str) -> CausalityRules:
	run_id = ""
	types_by_entity: dict[str, list[str]] = {}
	edges_by_entity: dict[str, list[tuple[str, str]]] = {}

	current_entity: Optional[str] = None
	section: Optional[str] = None

	for raw_line in text.splitlines():
		line = raw_line.strip()
		if not line:
			continue
		if line.startswith("run_id="):
			run_id = line.split("=", 1)[1]
			continue
		if line.startswith("entity_id="):
			current_entity = line.split("=", 1)[1]
			types_by_entity.setdefault(current_entity, [])
			edges_by_entity.setdefault(current_entity, [])
			section = None
			continue
		if line == "types:":
			section = "types"
			continue
		if line == "edges:":
			section = "edges"
			continue
		if not current_entity or not section:
			continue
		if not line.startswith("-"):
			continue

		item = line.lstrip("-").strip()
		if section == "types":
			types_by_entity[current_entity].append(item)
		else:
			if "->" not in item:
				continue
			left, right = [p.strip() for p in item.split("->", 1)]
			edges_by_entity[current_entity].append((left, right))

	for entity_id in list(types_by_entity.keys()):
		types_by_entity[entity_id] = sorted(set(types_by_entity[entity_id]))
	for entity_id in list(edges_by_entity.keys()):
		edges_by_entity[entity_id] = sorted(set(edges_by_entity[entity_id]))

	return CausalityRules(run_id=run_id, types_by_entity=types_by_entity, edges_by_entity=edges_by_entity)


def causality_rules_from_events(*, run_id: str, events: list[Event]) -> CausalityRules:
	by_event_id: dict[str, tuple[str, str]] = {}
	for e in events:
		if not isinstance(e, dict):
			continue
		event_id = e.get("event_id")
		entity_id = e.get("entity_id")
		event_type = e.get("event_type")
		if event_id and entity_id and event_type:
			by_event_id[str(event_id)] = (str(entity_id), str(event_type))

	types_by_entity: dict[str, set[str]] = {}
	edges_by_entity: dict[str, set[tuple[str, str]]] = {}

	for e in events:
		if not isinstance(e, dict):
			continue
		entity_id = e.get("entity_id")
		child_type = e.get("event_type")
		if not entity_id or not child_type:
			continue
		entity_id = str(entity_id)
		child_type = str(child_type)
		types_by_entity.setdefault(entity_id, set()).add(child_type)

		parent_ids = e.get("parent_event_ids") or []
		if not isinstance(parent_ids, list):
			continue
		for pid in parent_ids:
			pid = str(pid)
			parent_meta = by_event_id.get(pid)
			if not parent_meta:
				continue
			parent_entity, parent_type = parent_meta
			if parent_entity != entity_id:
				# Ignore cross-entity linkage for the canonical enrollment model.
				continue
			edges_by_entity.setdefault(entity_id, set()).add((parent_type, child_type))

	return CausalityRules(
		run_id=run_id,
		types_by_entity={k: sorted(v) for k, v in types_by_entity.items()},
		edges_by_entity={k: sorted(v) for k, v in edges_by_entity.items()},
	)


def causality_rules_from_edges(*, run_id: str, events: list[Event], edges: list[Edge]) -> CausalityRules:
	id_to_entity_type: dict[str, tuple[str, str]] = {}
	for e in events:
		if not isinstance(e, dict):
			continue
		event_id = e.get("event_id")
		entity_id = e.get("entity_id")
		event_type = e.get("event_type")
		if event_id and entity_id and event_type:
			id_to_entity_type[str(event_id)] = (str(entity_id), str(event_type))

	types_by_entity: dict[str, set[str]] = {}
	for e in events:
		if not isinstance(e, dict):
			continue
		entity_id = e.get("entity_id")
		event_type = e.get("event_type")
		if entity_id and event_type:
			types_by_entity.setdefault(str(entity_id), set()).add(str(event_type))

	edges_by_entity: dict[str, set[tuple[str, str]]] = {}
	for edge in edges:
		if not isinstance(edge, dict):
			continue
		a_id = edge.get("from")
		b_id = edge.get("to")
		if not a_id or not b_id:
			continue
		parent_meta = id_to_entity_type.get(str(a_id))
		child_meta = id_to_entity_type.get(str(b_id))
		if not parent_meta or not child_meta:
			continue
		parent_entity, parent_type = parent_meta
		child_entity, child_type = child_meta
		if parent_entity != child_entity:
			continue
		edges_by_entity.setdefault(parent_entity, set()).add((parent_type, child_type))

	return CausalityRules(
		run_id=run_id,
		types_by_entity={k: sorted(v) for k, v in types_by_entity.items()},
		edges_by_entity={k: sorted(v) for k, v in edges_by_entity.items()},
	)


def causality_rules_from_graph_dict(*, graph: dict) -> CausalityRules:
	run_id = str(graph.get("run_id") or "")
	events = graph.get("events") or []
	edges = graph.get("edges") or []
	return causality_rules_from_edges(run_id=run_id, events=events, edges=edges)
