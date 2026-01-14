from __future__ import annotations

from typing import Any

from dag_helpers.enrollment.shared import conf_value, run_id_from_context


def task_fetch_events_to_file(**context: Any) -> str:
	from helpers.enrollment import pipeline

	run_id = run_id_from_context(context)
	source_events_file = conf_value(context, "source_events_file")
	if not source_events_file:
		raise ValueError("Enrollment DAG requires dag_run.conf['source_events_file'] (JSON array or NDJSON).")

	source_rules_file = conf_value(context, "source_rules_file")
	return pipeline.fetch_events_to_file(
		run_id=run_id,
		source_events_file=source_events_file,
		source_rules_file=source_rules_file,
	)
