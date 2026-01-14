from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


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
