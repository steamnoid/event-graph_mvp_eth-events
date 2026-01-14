from __future__ import annotations

from typing import Any

from dag_helpers.enrollment.shared import run_id_from_context


def task_validate_normalized_events(normalized_file: str, **context: Any) -> str:
	from helpers.enrollment import validator

	run_id = run_id_from_context(context)
	return validator.validate_normalized_events(run_id=run_id, normalized_file=normalized_file)
