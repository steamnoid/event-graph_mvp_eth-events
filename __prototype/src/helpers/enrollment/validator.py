from __future__ import annotations

from pathlib import Path

from helpers.enrollment import artifacts


def task_validate_raw_events(events_file: str, **context) -> str:
	run_id = _run_id_from_context(context)
	return validate_raw_events(run_id=run_id, events_file=events_file)


def task_validate_normalized_events(normalized_file: str, **context) -> str:
	run_id = _run_id_from_context(context)
	return validate_normalized_events(run_id=run_id, normalized_file=normalized_file)


def task_validate_edges(normalized_file: str, edges_file: str, **context) -> str:
	run_id = _run_id_from_context(context)
	return validate_edges(run_id=run_id, normalized_file=normalized_file, edges_file=edges_file)


def task_validate_graph(graph_file: str, **context) -> str:
	run_id = _run_id_from_context(context)
	return validate_graph(run_id=run_id, graph_file=graph_file)


def task_validate_neo4j_readback(**context) -> str:
	run_id = _run_id_from_context(context)
	expect_canonical = _bool_conf(context=context, key="expect_canonical", default=False)
	return validate_neo4j_readback(run_id=run_id, expect_canonical=expect_canonical)


def validate_raw_events(*, run_id: str, events_file: str, artifact_root: str = artifacts.DEFAULT_ARTIFACT_ROOT) -> str:
	"""Stage C1 validation: validate raw schema and write C1.

	If C0.txt exists in the run directory, assert C0 == C1.
	"""

	run_path = _get_run_directory(run_id=run_id, artifact_root=artifact_root)

	raw_events = _load_events_from_file(events_file)
	_fail_if_raw_events_cannot_be_normalized(raw_events)

	c1_text = _derive_causality_rules_from_events(run_id=run_id, events=raw_events)
	c1_path = store_rules_as_stage_artifact(run_path=run_path, stage="C1", rules_text=c1_text)
	_fail_if_source_rules_do_not_match_raw_fixture(run_path=run_path, c1_text=c1_text)

	return c1_path


def validate_normalized_events(
	*,
	run_id: str,
	normalized_file: str,
	artifact_root: str = artifacts.DEFAULT_ARTIFACT_ROOT,
) -> str:
	"""Stage C2 validation: write C2 and assert C1 == C2."""

	run_path = _get_run_directory(run_id=run_id, artifact_root=artifact_root)

	normalized_events = _load_events_from_file(normalized_file)
	c2_text = _derive_causality_rules_from_events(run_id=run_id, events=normalized_events)
	c2_path = store_rules_as_stage_artifact(run_path=run_path, stage="C2", rules_text=c2_text)

	_fail_if_rules_meaning_diverged_since_prior_stage(
		run_path=run_path,
		prior_stage="C1",
		current_rules_text=c2_text,
		invariant_violation_message="rules meaning diverged after normalization (C1 != C2)",
	)
	return c2_path


def validate_edges(
	*,
	run_id: str,
	normalized_file: str,
	edges_file: str,
	artifact_root: str = artifacts.DEFAULT_ARTIFACT_ROOT,
) -> str:
	"""Stage C3 validation: write C3 and assert C2 == C3."""

	run_path = _get_run_directory(run_id=run_id, artifact_root=artifact_root)

	normalized_events = _load_events_from_file(normalized_file)
	edges = _load_edges_from_file(edges_file)

	c3_text = _derive_causality_rules_from_events_and_edges(run_id=run_id, events=normalized_events, edges=edges)
	c3_path = store_rules_as_stage_artifact(run_path=run_path, stage="C3", rules_text=c3_text)

	_fail_if_rules_meaning_diverged_since_prior_stage(
		run_path=run_path,
		prior_stage="C2",
		current_rules_text=c3_text,
		invariant_violation_message="rules meaning diverged after edge materialization (C2 != C3)",
	)
	return c3_path


def validate_graph(*, run_id: str, graph_file: str, artifact_root: str = artifacts.DEFAULT_ARTIFACT_ROOT) -> str:
	"""Stage C4 validation: write C4 and assert C3 == C4."""

	run_path = _get_run_directory(run_id=run_id, artifact_root=artifact_root)

	graph = _load_graph_from_file(graph_file)
	c4_text = _derive_causality_rules_from_graph(graph)
	c4_path = store_rules_as_stage_artifact(run_path=run_path, stage="C4", rules_text=c4_text)

	_fail_if_rules_meaning_diverged_since_prior_stage(
		run_path=run_path,
		prior_stage="C3",
		current_rules_text=c4_text,
		invariant_violation_message="rules meaning diverged after graph materialization (C3 != C4)",
	)
	return c4_path


def validate_neo4j_readback(
	*,
	run_id: str,
	expect_canonical: bool = False,
	artifact_root: str = artifacts.DEFAULT_ARTIFACT_ROOT,
) -> str:
	"""Stage C5: export C5 from Neo4j and assert C4 == C5."""

	run_path = _get_run_directory(run_id=run_id, artifact_root=artifact_root)

	c5_path = _export_c5_rules_text_from_neo4j(run_path=run_path, run_id=run_id, expect_canonical=expect_canonical)
	c5_text = artifacts.read_text(path=c5_path)
	_fail_if_rules_meaning_diverged_since_prior_stage(
		run_path=run_path,
		prior_stage="C4",
		current_rules_text=c5_text,
		invariant_violation_message="rules meaning diverged after Neo4j persistence (C4 != C5)",
	)

	return str(c5_path)


def _run_id_from_context(context: dict) -> str:
	return str(context.get("run_id") or "manual")


def _bool_conf(*, context: dict, key: str, default: bool = False) -> bool:
	dag_run = context.get("dag_run")
	if dag_run is None or getattr(dag_run, "conf", None) is None:
		return default
	return bool(dag_run.conf.get(key) if key in dag_run.conf else default)


def _get_run_directory(*, run_id: str, artifact_root: str) -> Path:
	return artifacts.run_dir(run_id=run_id, artifact_root=artifact_root)


def _load_events_from_file(filename: str) -> list[dict]:
	from helpers.enrollment.adapter import load_events_from_file

	return load_events_from_file(filename)


def _load_edges_from_file(filename: str) -> list[dict]:
	# Edges share the same “json list of objects” artifact format.
	return _load_events_from_file(filename)


def _fail_if_raw_events_cannot_be_normalized(events: list[dict]) -> None:
	"""Validate raw input schema by reusing the canonical normalizer."""
	from helpers.enrollment.transformer import normalize_events

	_ = normalize_events(events)


def _derive_causality_rules_from_events(*, run_id: str, events: list[dict]) -> str:
	from helpers.enrollment.causality_rules import causality_rules_from_events, render_causality_rules

	rules = causality_rules_from_events(run_id=run_id, events=events)
	return render_causality_rules(rules)


def _derive_causality_rules_from_events_and_edges(*, run_id: str, events: list[dict], edges: list[dict]) -> str:
	from helpers.enrollment.causality_rules import causality_rules_from_edges, render_causality_rules

	rules = causality_rules_from_edges(run_id=run_id, events=events, edges=edges)
	return render_causality_rules(rules)


def _load_graph_from_file(graph_file: str) -> dict:
	from helpers.neo4j.adapter import load_graph_from_file

	return load_graph_from_file(graph_file)


def _derive_causality_rules_from_graph(graph: dict) -> str:
	from helpers.enrollment.causality_rules import causality_rules_from_graph_dict, render_causality_rules

	rules = causality_rules_from_graph_dict(graph=graph)
	return render_causality_rules(rules)


def _stage_path(*, run_path: Path, stage: str) -> Path:
	return run_path / f"{stage}.txt"


def store_rules_as_stage_artifact(*, run_path: Path, stage: str, rules_text: str) -> str:
	path = _stage_path(run_path=run_path, stage=stage)
	artifacts.write_text(path=path, text=rules_text)
	return str(path)


def load_rules_from_stage_artifact(*, run_path: Path, stage: str) -> str:
	return artifacts.read_text(path=_stage_path(run_path=run_path, stage=stage))


def _fail_if_rules_meaning_diverged_since_prior_stage(
	*,
	run_path: Path,
	prior_stage: str,
	current_rules_text: str,
	invariant_violation_message: str,
) -> None:
	previous_text = load_rules_from_stage_artifact(run_path=run_path, stage=prior_stage)
	if previous_text != current_rules_text:
		raise ValueError(invariant_violation_message)


def _fail_if_source_rules_do_not_match_raw_fixture(*, run_path: Path, c1_text: str) -> None:
	"""If C0.txt is present, it must be identical to the raw fixture C1."""
	c0_path = run_path / "C0.txt"
	if not c0_path.exists():
		return
	c0_text = artifacts.read_text(path=c0_path)
	if c0_text != c1_text:
		raise ValueError("C0 != C1 (source rules do not match raw fixture events)")


def _export_c5_rules_text_from_neo4j(*, run_path: Path, run_id: str, expect_canonical: bool) -> Path:
	from helpers.enrollment.neo4j_causality_rules import persist_causality_rules

	c5_path = _stage_path(run_path=run_path, stage="C5")
	persist_causality_rules(run_id=run_id, out_file=str(c5_path), expect_canonical=expect_canonical)
	return c5_path
