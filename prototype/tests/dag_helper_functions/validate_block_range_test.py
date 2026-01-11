"""Unit test: strict typing for block range inputs."""

import pytest

from dags.dag_helper_functions.events import validate_block_range


def test_validate_block_range_rejects_non_int_inputs():
    """Non-int inputs are rejected to keep behavior deterministic."""
    with pytest.raises(TypeError):
        validate_block_range(1.0, 2)
