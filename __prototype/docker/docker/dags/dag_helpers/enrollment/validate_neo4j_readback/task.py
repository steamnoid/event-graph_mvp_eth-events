from __future__ import annotations

from typing import Any

from dag_helpers.enrollment.shared import bool_conf, run_id_from_context


def task_validate_neo4j_readback(**context: Any) -> str:
	from helpers.enrollment import validator

	run_id = run_id_from_context(context)
	expect_canonical = bool_conf(context=context, key="expect_canonical", default=False)
	return validator.validate_neo4j_readback(run_id=run_id, expect_canonical=expect_canonical)
