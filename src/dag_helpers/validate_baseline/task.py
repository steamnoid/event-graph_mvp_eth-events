from __future__ import annotations

from pathlib import Path

from .validator import assert_canonical_baseline_files_identical


def validate_canonical_baseline(
	*,
	reference_baseline_path: str | Path,
	candidate_baseline_path: str | Path,
	artifact_dir: str | Path,
	out_name: str = "validated_baseline.json",
) -> Path:
	"""Validation stage: assert candidate baseline matches the reference baseline.

	Reference baseline is expected to come from `fetch_data`.

	Validates that `reference_baseline_path` and `candidate_baseline_path` are identical.
	If they are, writes a "handoff" baseline artifact in `artifact_dir` and
	returns its path.
	"""
	artifact_dir = Path(artifact_dir)
	artifact_dir.mkdir(parents=True, exist_ok=True)

	assert_canonical_baseline_files_identical(pre_path=reference_baseline_path, post_path=candidate_baseline_path)

	out_path = artifact_dir / out_name
	out_path.write_text(Path(candidate_baseline_path).read_text(encoding="utf-8"), encoding="utf-8")
	return out_path


def validate_canonical_baseline_between_tasks(
	*,
	pre_baseline_path: str | Path,
	post_baseline_path: str | Path,
	artifact_dir: str | Path,
	out_name: str = "validated_baseline.json",
) -> Path:
	"""Backwards-compatible wrapper for the old adjacent-task validation API."""
	return validate_canonical_baseline(
		reference_baseline_path=pre_baseline_path,
		candidate_baseline_path=post_baseline_path,
		artifact_dir=artifact_dir,
		out_name=out_name,
	)
