from pathlib import Path

import pytest


_FIXTURE_EVENTS_FILE = (
	Path(__file__).resolve().parents[1]
	/ "fixtures"
	/ "events"
	/ "enrollment_events.json"
)


@pytest.mark.unit
def test_pipeline_validate_raw_events_writes_c1_and_accepts_matching_c0(tmp_path):
	from helpers.enrollment import pipeline

	run_id = "unit:run"
	artifact_root = str(tmp_path)

	# Stage C1: copy raw fixture to run dir.
	events_file = pipeline.fetch_events_to_file(
		run_id=run_id,
		source_events_file=str(_FIXTURE_EVENTS_FILE),
		artifact_root=artifact_root,
	)
	assert Path(events_file).exists()

	# First run writes C1.
	c1_path = Path(
		pipeline.validate_raw_events(
			run_id=run_id,
			events_file=events_file,
			artifact_root=artifact_root,
		)
	)
	assert c1_path.exists()

	# Create a matching C0 and assert it is accepted.
	c0_path = c1_path.parent / "C0.txt"
	c0_path.write_text(c1_path.read_text(encoding="utf-8"), encoding="utf-8")

	_ = pipeline.validate_raw_events(
		run_id=run_id,
		events_file=events_file,
		artifact_root=artifact_root,
	)


@pytest.mark.unit
def test_pipeline_validate_raw_events_raises_on_c0_mismatch(tmp_path):
	from helpers.enrollment import pipeline

	run_id = "unit:run"
	artifact_root = str(tmp_path)

	events_file = pipeline.fetch_events_to_file(
		run_id=run_id,
		source_events_file=str(_FIXTURE_EVENTS_FILE),
		artifact_root=artifact_root,
	)

	# Ensure C1 exists.
	c1_path = Path(
		pipeline.validate_raw_events(
			run_id=run_id,
			events_file=events_file,
			artifact_root=artifact_root,
		)
	)

	# Force mismatch.
	c0_path = c1_path.parent / "C0.txt"
	c0_path.write_text("run_id=wrong\n", encoding="utf-8")

	with pytest.raises(ValueError, match=r"C0 != C1"):
		pipeline.validate_raw_events(
			run_id=run_id,
			events_file=events_file,
			artifact_root=artifact_root,
		)
