from __future__ import annotations

import filecmp
from pathlib import Path
import shutil
import subprocess


def canonical_baseline_files_identical(*, pre_path: str | Path, post_path: str | Path) -> bool:
	"""Return True if canonical baseline artifacts match byte-for-byte.

	This uses a streaming compare (does not load entire files into memory).
	"""
	return filecmp.cmp(str(pre_path), str(post_path), shallow=False)


def assert_canonical_baseline_files_identical(*, pre_path: str | Path, post_path: str | Path) -> None:
	"""Assert canonical baseline artifacts are identical.

	Raises ValueError with a small unified diff preview when they differ.
	"""
	pre_path = Path(pre_path)
	post_path = Path(post_path)

	if canonical_baseline_files_identical(pre_path=pre_path, post_path=post_path):
		return

	preview = _diff_preview(pre_path=pre_path, post_path=post_path, max_lines=200)
	raise ValueError(
		"Canonical baseline artifacts are not identical. "
		"This means the stage changed canonical semantics unexpectedly.\n\n"
		+ preview
	)


def _diff_preview(*, pre_path: Path, post_path: Path, max_lines: int) -> str:
	"""Generate a bounded diff preview without loading whole files.

	Prefers the system `diff -u` if available; otherwise returns a short message.
	"""
	diff_exe = shutil.which("diff")
	if not diff_exe:
		return f"(diff preview unavailable: `diff` not found)\npre={pre_path}\npost={post_path}\n"

	proc = subprocess.Popen(
		[diff_exe, "-u", str(pre_path), str(post_path)],
		stdout=subprocess.PIPE,
		stderr=subprocess.STDOUT,
		text=True,
	)
	assert proc.stdout is not None
	lines: list[str] = []
	try:
		for _ in range(max_lines):
			line = proc.stdout.readline()
			if not line:
				break
			lines.append(line)
	finally:
		# Ensure process doesn't hang around if we stopped early.
		try:
			proc.kill()
		except Exception:
			pass
		proc.wait(timeout=1)

	if not lines:
		return f"(diff produced no output)\npre={pre_path}\npost={post_path}\n"
	return "".join(lines)
