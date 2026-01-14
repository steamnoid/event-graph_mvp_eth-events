from __future__ import annotations

from typing import Any

from dag_helpers.enrollment.shared import run_id_from_context


def task_transform_edges_to_graph_file(normalized_file: str, edges_file: str, **context: Any) -> str:
	from helpers.enrollment import pipeline

	run_id = run_id_from_context(context)
	return pipeline.transform_edges_to_graph_file(
		run_id=run_id,
		normalized_file=normalized_file,
		edges_file=edges_file,
	)
