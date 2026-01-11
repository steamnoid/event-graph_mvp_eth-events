def transform_logs(raw_logs: list[dict]) -> list[dict]:
	from dags.helpers.eth.logs.decoder import decode_log

	return [decode_log(raw_log) for raw_log in raw_logs]

