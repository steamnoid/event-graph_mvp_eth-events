import json
from pathlib import Path
from typing import Any, List, Optional


Event = dict[str, Any]


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


def load_events_from_file(filename: str) -> List[Event]:
	"""Load Enrollment events from JSON array or NDJSON.

	This is intentionally file-format tolerant so tests and Airflow handoffs
	can pin deterministic fixtures.
	"""
	text = _read_text_file(Path(filename))
	return _parse_events_text(text)


def write_events_to_file(events: List[Event], filename: str) -> int:
	"""Write events as a JSON array.

	Enrollment artifacts are written as JSON arrays (not NDJSON) so they are
	easy to diff and re-load.
	"""
	with open(filename, "w", encoding="utf-8") as f:
		json.dump(events, f)
	return len(events)


def _load_conf_value(*, context: dict[str, Any], key: str) -> Optional[str]:
	dag_run = context.get("dag_run")
	if dag_run is None or getattr(dag_run, "conf", None) is None:
		return None
	value = dag_run.conf.get(key)
	return str(value) if value else None


def _read_text_file(path: Path) -> str:
	return path.read_text(encoding="utf-8")


def _parse_events_text(text: str) -> List[Event]:
	"""Parse Enrollment events from either JSON-array or NDJSON text."""
	stripped = text.strip()
	if not stripped:
		return []
	if stripped.startswith("["):
		return _parse_json_array(stripped)
	return _parse_ndjson(stripped)


def _parse_json_array(text: str) -> List[Event]:
	data = json.loads(text)
	if not isinstance(data, list):
		raise ValueError("JSON file must contain a top-level array")
	return data


def _parse_ndjson(text: str) -> List[Event]:
	events: List[Event] = []
	for raw_line in text.splitlines():
		line = raw_line.strip()
		if not line:
			continue
		events.append(json.loads(line))
	return events
