from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest


def _repo_root() -> Path:
    # .../src/data_generator/tests/test_*.py -> parents[3] is repo root
    return Path(__file__).resolve().parents[3]


def _run_script(script_rel_path: str, args: list[str]) -> subprocess.CompletedProcess[str]:
    repo = _repo_root()
    script_path = repo / script_rel_path
    assert script_path.exists(), f"Missing script: {script_path}"

    return subprocess.run(
        [sys.executable, str(script_path), *args],
        cwd=str(repo),
        text=True,
        capture_output=True,
    )


@pytest.mark.functional
def test_generate_fixture_json_and_validate_consistent(tmp_path: Path) -> None:
    out_events = tmp_path / "events.json"
    out_rules = tmp_path / "rules.txt"

    cp = _run_script(
        "src/data_generator/scripts/generate_fixture.py",
        [
            "--seed",
            "20260113",
            "--entity-count",
            "2",
            "--inconsistency-rate",
            "0.0",
            "--missing-event-rate",
            "0.0",
            "--run-id",
            "fixture:test-json",
            "--format",
            "json",
            "--out-events",
            str(out_events),
            "--out-rules",
            str(out_rules),
        ],
    )
    assert cp.returncode == 0, cp.stderr
    assert out_events.exists() and out_events.stat().st_size > 0
    assert out_rules.exists() and "run_id=fixture:test-json" in out_rules.read_text(encoding="utf-8")

    cp = _run_script(
        "src/data_generator/scripts/validate_fixture.py",
        ["--events", str(out_events), "--mode", "consistent"],
    )
    assert cp.returncode == 0, cp.stdout + cp.stderr


@pytest.mark.functional
def test_generate_fixture_ndjson_and_validate_inconsistent(tmp_path: Path) -> None:
    out_events = tmp_path / "events.ndjson"

    cp = _run_script(
        "src/data_generator/scripts/generate_fixture.py",
        [
            "--seed",
            "20260113",
            "--entity-count",
            "3",
            "--inconsistency-rate",
            "0.3",
            "--missing-event-rate",
            "0.2",
            "--format",
            "ndjson",
            "--out-events",
            str(out_events),
        ],
    )
    assert cp.returncode == 0, cp.stderr
    assert out_events.exists() and out_events.stat().st_size > 0

    # With missing/inconsistent generation, canonical consistency is not guaranteed,
    # but basic schema/ID checks should still pass.
    cp = _run_script(
        "src/data_generator/scripts/validate_fixture.py",
        ["--events", str(out_events), "--mode", "inconsistent"],
    )
    assert cp.returncode == 0, cp.stdout + cp.stderr
