"""Unit test: validation should happen before network calls."""

import pytest

from dags.dag_helper_functions.events import fetch_events_in_block_range


def test_fetch_events_in_block_range_validates_before_network_call():
    """Invalid ranges raise before attempting to connect to the provider."""
    # If validation is done first, we should get ValueError even if provider_url is unreachable.
    with pytest.raises(ValueError):
        fetch_events_in_block_range(
            contract_address="0x0000000000000000000000000000000000000000",
            provider_url="http://127.0.0.1:0",
            from_block=10,
            to_block=9,
        )
