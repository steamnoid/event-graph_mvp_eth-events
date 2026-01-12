import json


def load_logs_from_file(filename: str) -> list[dict]:
	with open(filename, "r", encoding="utf-8") as f:
		return json.load(f)


def transform_logs(raw_logs: list[dict]) -> list[dict]:
	from helpers.eth.logs.decoder import decode_log

	def _as_0x_prefixed(value):
		if value is None:
			return None
		if isinstance(value, str):
			return value if value.startswith("0x") else "0x" + value
		if hasattr(value, "hex"):
			hex_str = value.hex()
			return hex_str if hex_str.startswith("0x") else "0x" + hex_str
		return str(value)

	events: list[dict] = []
	for raw_log in raw_logs:
		decoded = decode_log(raw_log)
		tx_hash = _as_0x_prefixed(raw_log.get("transactionHash"))
		log_index = raw_log.get("logIndex")
		event_id = f"{tx_hash}:{log_index}" if tx_hash is not None and log_index is not None else None

		events.append(
			{
				"event_id": event_id,
				"tx_hash": tx_hash,
				"log_index": int(log_index) if log_index is not None else None,
				"block_number": int(raw_log.get("blockNumber")) if raw_log.get("blockNumber") is not None else None,
				"transaction_index": int(raw_log.get("transactionIndex")) if raw_log.get("transactionIndex") is not None else None,
				"address": raw_log.get("address"),
				"event_name": decoded.get("event_name"),
				"decoded": decoded.get("decoded"),
			}
		)

	return events


def write_events_to_file(events: list[dict], filename: str) -> int:
	with open(filename, "w", encoding="utf-8") as f:
		json.dump(events, f)
	return len(events)

