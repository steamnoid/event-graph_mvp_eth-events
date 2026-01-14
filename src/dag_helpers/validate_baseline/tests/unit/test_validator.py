from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.unit
def test_canonical_baseline_files_identical_true(tmp_path: Path) -> None:
	from dag_helpers.validate_baseline.validator import canonical_baseline_files_identical

	pre = tmp_path / "pre.json"
	post = tmp_path / "post.json"
	content = "[\n  {\"a\": 1}\n]\n"
	pre.write_text(content, encoding="utf-8")
	post.write_text(content, encoding="utf-8")

	assert canonical_baseline_files_identical(pre_path=pre, post_path=post) is True


@pytest.mark.unit
def test_canonical_baseline_files_identical_false(tmp_path: Path) -> None:
	from dag_helpers.validate_baseline.validator import canonical_baseline_files_identical

	pre = tmp_path / "pre.json"
	post = tmp_path / "post.json"
	pre.write_text("[\n  {\"a\": 1}\n]\n", encoding="utf-8")
	post.write_text("[\n  {\"a\": 2}\n]\n", encoding="utf-8")

	assert canonical_baseline_files_identical(pre_path=pre, post_path=post) is False


@pytest.mark.unit
def test_assert_canonical_baseline_files_identical_ok(tmp_path: Path) -> None:
	from dag_helpers.validate_baseline.validator import assert_canonical_baseline_files_identical

	pre = tmp_path / "pre.json"
	post = tmp_path / "post.json"
	content = "[\n  {\"a\": 1}\n]\n"
	pre.write_text(content, encoding="utf-8")
	post.write_text(content, encoding="utf-8")

	assert_canonical_baseline_files_identical(pre_path=pre, post_path=post)


@pytest.mark.unit
def test_assert_canonical_baseline_files_identical_raises(tmp_path: Path) -> None:
	from dag_helpers.validate_baseline.validator import assert_canonical_baseline_files_identical

	pre = tmp_path / "pre.json"
	post = tmp_path / "post.json"
	pre.write_text("[\n  {\"a\": 1}\n]\n", encoding="utf-8")
	post.write_text("[\n  {\"a\": 2}\n]\n", encoding="utf-8")

	with pytest.raises(ValueError, match="not identical"):
		assert_canonical_baseline_files_identical(pre_path=pre, post_path=post)
