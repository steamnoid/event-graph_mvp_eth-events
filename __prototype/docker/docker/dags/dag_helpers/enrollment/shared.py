from __future__ import annotations

from typing import Any, Optional


def run_id_from_context(context: dict[str, Any]) -> str:
	return str(context.get("run_id") or "manual")


def conf_value(context: dict[str, Any], key: str) -> Optional[str]:
	dag_run = context.get("dag_run")
	if dag_run is None or getattr(dag_run, "conf", None) is None:
		return None
	value = dag_run.conf.get(key)
	return str(value) if value else None


def bool_conf(*, context: dict[str, Any], key: str, default: bool = False) -> bool:
	dag_run = context.get("dag_run")
	if dag_run is None or getattr(dag_run, "conf", None) is None:
		return default
	return bool(dag_run.conf.get(key) if key in dag_run.conf else default)
