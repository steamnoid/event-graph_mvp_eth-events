import json


def load_events_from_file(filename: str) -> list[dict]:
	with open(filename, "r", encoding="utf-8") as f:
		return json.load(f)
