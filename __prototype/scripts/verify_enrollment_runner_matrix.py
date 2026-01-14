"""Matrix-check the Enrollment causality diff runner.

This script runs the CLI runner across a set of parameter combinations and prints
only a compact summary. It is meant for fast manual confidence checks outside pytest.

Exit codes:
  0: all cases passed
  1: at least one case failed
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from dataclasses import dataclass


@dataclass(frozen=True)
class Case:
    seed: int
    entity_count: int
    inconsistency_rate: float
    expect_canonical: bool


def main() -> int:
    runner = "scripts/enrollment_causality_diff_runner.py"

    repo_root = Path(__file__).resolve().parents[1]
    venv_python = repo_root / ".venv" / "bin" / "python"
    python_exe = str(venv_python) if venv_python.exists() else sys.executable

    cases: list[Case] = []

    # Canonical-consistent (C0..C5 equality + canonical shape)
    for seed in (1, 2, 1337, 999):
        for entity_count in (1, 2, 4):
            cases.append(Case(seed=seed, entity_count=entity_count, inconsistency_rate=0.0, expect_canonical=True))

    # Inconsistent (C0..C5 equality only)
    for seed in (3, 7, 42, 1337):
        for entity_count in (1, 4):
            for p in (0.1, 0.3, 0.6, 1.0):
                cases.append(Case(seed=seed, entity_count=entity_count, inconsistency_rate=p, expect_canonical=False))

    failures: list[tuple[Case, int, str]] = []

    for case in cases:
        cmd = [
            python_exe,
            runner,
            "--seed",
            str(case.seed),
            "--entity-count",
            str(case.entity_count),
            "--inconsistency-rate",
            str(case.inconsistency_rate),
        ]
        if case.expect_canonical:
            cmd.append("--expect-canonical")

        result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            failures.append((case, result.returncode, (result.stderr or "").strip()))

    print(f"checked={len(cases)} failures={len(failures)}")
    for case, rc, err in failures[:20]:
        mode = "canonical" if case.expect_canonical else "equality"
        print(
            f"FAIL seed={case.seed} entities={case.entity_count} p={case.inconsistency_rate} mode={mode} rc={rc}"
        )
        if err:
            print("  stderr_last_line=", err.splitlines()[-1])

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
