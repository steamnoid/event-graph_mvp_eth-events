from __future__ import annotations

from pathlib import Path

from .validator import assert_canonical_baseline_files_identical


def validate_canonical_baseline(
	*,
	pre_baseline_path: str | Path,
	post_baseline_path: str | Path,	
	artifact_dir: str | Path,
	out_name: str = "validated_baseline.json",
) -> Path:
	"""Validation stage between pipeline tasks.

	Validates that `pre_baseline_path` and `post_baseline_path` are identical.
	If they are, writes a "handoff" baseline artifact in `artifact_dir` and
	returns its path.
	"""
	artifact_dir = Path(artifact_dir)
	artifact_dir.mkdir(parents=True, exist_ok=True)

	assert_canonical_baseline_files_identical(pre_path=pre_baseline_path, post_path=post_baseline_path)

	out_path = artifact_dir / out_name
	out_path.write_text(Path(pre_baseline_path).read_text(encoding="utf-8"), encoding="utf-8")
	return out_path
