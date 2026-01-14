from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.unit
def test_validate_canonical_baseline_writes_handoff_file(tmp_path: Path) -> None:
	from dag_helpers.validate_baseline.task import validate_canonical_baseline

	pre = tmp_path / "pre.json"
	post = tmp_path / "post.json"
	pre.write_text("[\n  {\"a\": 1}\n]\n", encoding="utf-8")
	post.write_text("[\n  {\"a\": 1}\n]\n", encoding="utf-8")

	out = validate_canonical_baseline(
		reference_baseline_path=pre,
		candidate_baseline_path=post,
		artifact_dir=tmp_path / "artifacts",
		out_name="C1.json",
	)

	assert out.exists()
	assert out.name == "C1.json"
	assert out.read_text(encoding="utf-8") == post.read_text(encoding="utf-8")


@pytest.mark.unit
def test_validate_canonical_baseline_raises_on_mismatch(tmp_path: Path) -> None:
	from dag_helpers.validate_baseline.task import validate_canonical_baseline

	pre = tmp_path / "pre.json"
	post = tmp_path / "post.json"
	pre.write_text("[\n  {\"a\": 1}\n]\n", encoding="utf-8")
	post.write_text("[\n  {\"a\": 2}\n]\n", encoding="utf-8")

	with pytest.raises(ValueError, match="not identical"):
		validate_canonical_baseline(
			reference_baseline_path=pre,
			candidate_baseline_path=post,
			artifact_dir=tmp_path / "artifacts",
		)
