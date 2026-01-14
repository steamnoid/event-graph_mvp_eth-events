from __future__ import annotations

from typing import Any

from dag_helpers.enrollment.shared import run_id_from_context


def task_transform_events_to_normalized_file(events_file: str, **context: Any) -> str:
	from helpers.enrollment import pipeline

	run_id = run_id_from_context(context)
	return pipeline.transform_events_to_normalized_file(run_id=run_id, events_file=events_file)
