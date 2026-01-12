import pytest


@pytest.mark.unit
def test_plan_bootstrap_offsets_returns_end_minus_n():
    from helpers.kafka.adapter import plan_bootstrap_offsets

    assert plan_bootstrap_offsets(end_offset=26, num_messages=13) == 13


@pytest.mark.unit
def test_plan_bootstrap_offsets_rejects_negative_start():
    from helpers.kafka.adapter import plan_bootstrap_offsets

    with pytest.raises(ValueError):
        plan_bootstrap_offsets(end_offset=5, num_messages=13)
