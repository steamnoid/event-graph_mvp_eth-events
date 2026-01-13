import json
from pathlib import Path
from typing import Any, List


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
