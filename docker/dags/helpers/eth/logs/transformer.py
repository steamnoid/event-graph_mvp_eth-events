import json


def load_logs_from_file(filename: str) -> list[dict]:
	with open(filename, "r", encoding="utf-8") as f:
		return json.load(f)


def transform_logs(raw_logs: list[dict]) -> list[dict]:
	from dags.helpers.eth.logs.decoder import decode_log

	return [decode_log(raw_log) for raw_log in raw_logs]


def write_events_to_file(events: list[dict], filename: str) -> int:
	with open(filename, "w", encoding="utf-8") as f:
		json.dump(events, f)
	return len(events)

