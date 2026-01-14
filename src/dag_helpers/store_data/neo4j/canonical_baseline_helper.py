from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable


Edge = dict[str, str]


def transform_edges_to_canonical_baseline_format(edges: Iterable[Edge]) -> list[Edge]:
	"""Canonical baseline for Neo4j store stage (edges)."""
	baseline: list[Edge] = []
	for idx, edge in enumerate(edges):
		if not isinstance(edge, dict):
			raise ValueError(f"edge[{idx}] must be a dict")
		if "from" not in edge or "to" not in edge:
			raise ValueError(f"edge[{idx}] must contain 'from' and 'to'")
		baseline.append({"from": str(edge["from"]), "to": str(edge["to"])})
	return sorted(baseline, key=lambda e: (e.get("from", ""), e.get("to", "")))


def save_canonical_baseline_artifact(*, edges: Iterable[Edge], path: str | Path) -> Path:
	path = Path(path)
	path.parent.mkdir(parents=True, exist_ok=True)
	baseline = transform_edges_to_canonical_baseline_format(edges)
	path.write_text(json.dumps(baseline, indent=2, sort_keys=True) + "\n", encoding="utf-8")
	return path
