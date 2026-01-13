import json
from pathlib import Path
from typing import Any, List, Optional


def _load_conf_value(*, context: dict[str, Any], key: str) -> Optional[str]:
	dag_run = context.get("dag_run")
	if dag_run is None or getattr(dag_run, "conf", None) is None:
		return None
	value = dag_run.conf.get(key)
	return str(value) if value else None


def task_fetch_events_to_file(**context) -> str:
	"""Airflow task callable: Stage C1 fetch.

	Delegates all IO + artifact writes to helpers.
	"""
	from helpers.enrollment import pipeline

	run_id = str(context.get("run_id") or "manual")
	source_events_file = _load_conf_value(context=context, key="source_events_file")
	if not source_events_file:
		raise ValueError("Enrollment DAG requires dag_run.conf['source_events_file'] (JSON array or NDJSON).")

	source_rules_file = _load_conf_value(context=context, key="source_rules_file")
	return pipeline.fetch_events_to_file(
		run_id=run_id,
		source_events_file=source_events_file,
		source_rules_file=source_rules_file,
	)


Event = dict[str, Any]


def load_events_from_file(filename: str) -> List[Event]:
	"""Load Enrollment events from JSON array or NDJSON.

	This is intentionally file-format tolerant so tests and Airflow handoffs
	can pin deterministic fixtures.
	"""
	text = Path(filename).read_text(encoding="utf-8").strip()
	if not text:
		return []

	if text.startswith("["):
		data = json.loads(text)
		if not isinstance(data, list):
			raise ValueError("JSON file must contain a top-level array")
		return data

	# NDJSON
	events: List[Event] = []
	for line in text.splitlines():
		line = line.strip()
		if not line:
			continue
		events.append(json.loads(line))
	return events


def write_events_to_file(events: List[Event], filename: str) -> int:
	with open(filename, "w", encoding="utf-8") as f:
		json.dump(events, f)
	return len(events)
