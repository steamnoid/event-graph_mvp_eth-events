from __future__ import annotations

from pathlib import Path

from helpers.enrollment import artifacts


def task_validate_raw_events(events_file: str, **context) -> str:
	run_id = str(context.get("run_id") or "manual")
	return validate_raw_events(run_id=run_id, events_file=events_file)


def task_validate_normalized_events(normalized_file: str, **context) -> str:
	run_id = str(context.get("run_id") or "manual")
	return validate_normalized_events(run_id=run_id, normalized_file=normalized_file)


def task_validate_edges(normalized_file: str, edges_file: str, **context) -> str:
	run_id = str(context.get("run_id") or "manual")
	return validate_edges(run_id=run_id, normalized_file=normalized_file, edges_file=edges_file)


def task_validate_graph(graph_file: str, **context) -> str:
	run_id = str(context.get("run_id") or "manual")
	return validate_graph(run_id=run_id, graph_file=graph_file)


def task_validate_neo4j_readback(**context) -> str:
	run_id = str(context.get("run_id") or "manual")

	expect_canonical = False
	dag_run = context.get("dag_run")
	if dag_run is not None and getattr(dag_run, "conf", None):
		expect_canonical = bool(dag_run.conf.get("expect_canonical") or False)

	return validate_neo4j_readback(run_id=run_id, expect_canonical=expect_canonical)


def validate_raw_events(*, run_id: str, events_file: str, artifact_root: str = artifacts.DEFAULT_ARTIFACT_ROOT) -> str:
	"""Stage C1 validation: validate raw schema and write C1.

	If C0.txt exists in the run directory, assert C0 == C1.
	"""

	run_path = artifacts.run_dir(run_id=run_id, artifact_root=artifact_root)

	from helpers.enrollment.adapter import load_events_from_file
	from helpers.enrollment.causality_rules import causality_rules_from_events, format_causality_rules_text
	from helpers.enrollment.transformer import normalize_events

	raw_events = load_events_from_file(events_file)
	_ = normalize_events(raw_events)

	c1 = format_causality_rules_text(causality_rules_from_events(run_id=run_id, events=raw_events))
	c1_path = run_path / "C1.txt"
	artifacts.write_text(path=c1_path, text=c1)

	c0_path = run_path / "C0.txt"
	if c0_path.exists():
		c0 = artifacts.read_text(path=c0_path)
		if c0 != c1:
			raise ValueError("C0 != C1 (source rules do not match raw fixture events)")

	return str(c1_path)


def validate_normalized_events(
	*,
	run_id: str,
	normalized_file: str,
	artifact_root: str = artifacts.DEFAULT_ARTIFACT_ROOT,
) -> str:
	"""Stage C2 validation: write C2 and assert C1 == C2."""

	run_path = artifacts.run_dir(run_id=run_id, artifact_root=artifact_root)

	from helpers.enrollment.adapter import load_events_from_file
	from helpers.enrollment.causality_rules import causality_rules_from_events, format_causality_rules_text

	normalized = load_events_from_file(normalized_file)
	c2 = format_causality_rules_text(causality_rules_from_events(run_id=run_id, events=normalized))
	c2_path = run_path / "C2.txt"
	artifacts.write_text(path=c2_path, text=c2)

	c1 = artifacts.read_text(path=run_path / "C1.txt")
	if c1 != c2:
		raise ValueError("C1 != C2")

	return str(c2_path)


def validate_edges(
	*,
	run_id: str,
	normalized_file: str,
	edges_file: str,
	artifact_root: str = artifacts.DEFAULT_ARTIFACT_ROOT,
) -> str:
	"""Stage C3 validation: write C3 and assert C2 == C3."""

	run_path = artifacts.run_dir(run_id=run_id, artifact_root=artifact_root)

	from helpers.enrollment.adapter import load_events_from_file
	from helpers.enrollment.causality_rules import causality_rules_from_edges, format_causality_rules_text

	normalized = load_events_from_file(normalized_file)
	edges = load_events_from_file(edges_file)

	c3 = format_causality_rules_text(
		causality_rules_from_edges(run_id=run_id, events=normalized, edges=edges)
	)
	c3_path = run_path / "C3.txt"
	artifacts.write_text(path=c3_path, text=c3)

	c2 = artifacts.read_text(path=run_path / "C2.txt")
	if c2 != c3:
		raise ValueError("C2 != C3")

	return str(c3_path)


def validate_graph(*, run_id: str, graph_file: str, artifact_root: str = artifacts.DEFAULT_ARTIFACT_ROOT) -> str:
	"""Stage C4 validation: write C4 and assert C3 == C4."""

	run_path = artifacts.run_dir(run_id=run_id, artifact_root=artifact_root)

	from helpers.enrollment.causality_rules import causality_rules_from_graph_dict, format_causality_rules_text
	from helpers.neo4j.adapter import load_graph_from_file

	graph = load_graph_from_file(graph_file)
	c4 = format_causality_rules_text(causality_rules_from_graph_dict(graph=graph))
	c4_path = run_path / "C4.txt"
	artifacts.write_text(path=c4_path, text=c4)

	c3 = artifacts.read_text(path=run_path / "C3.txt")
	if c3 != c4:
		raise ValueError("C3 != C4")

	return str(c4_path)


def validate_neo4j_readback(
	*,
	run_id: str,
	expect_canonical: bool = False,
	artifact_root: str = artifacts.DEFAULT_ARTIFACT_ROOT,
) -> str:
	"""Stage C5: export C5 from Neo4j and assert C4 == C5."""

	run_path = artifacts.run_dir(run_id=run_id, artifact_root=artifact_root)

	from helpers.enrollment.neo4j_causality_rules import export_causality_rules_text

	c5_path = run_path / "C5.txt"
	export_causality_rules_text(run_id=run_id, out_file=str(c5_path), expect_canonical=expect_canonical)

	c4 = artifacts.read_text(path=run_path / "C4.txt")
	c5 = artifacts.read_text(path=c5_path)
	if c4 != c5:
		raise ValueError("C4 != C5")

	return str(c5_path)
