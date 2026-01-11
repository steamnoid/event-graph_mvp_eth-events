"""Web3 integration tests for fetching and normalizing real Ethereum logs.

These tests require ALCHEMY_URL and are skipped if it is not configured.
"""

import logging
import os

import pytest
from web3 import Web3

from dags.dag_helper_functions.events import fetch_events_in_block_range

CONTRACT_ADDRESS = "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc"

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def contract_events():
    alchemy_url = os.getenv("ALCHEMY_URL")
    assert alchemy_url, "Missing ALCHEMY_URL. Set it in docker/.env or your shell environment."

    w3 = Web3(Web3.HTTPProvider(alchemy_url))
    assert w3.is_connected(), "Unable to reach the Alchemy endpoint"
    to_block = int(w3.eth.block_number)
    from_block = max(0, to_block - 200)
    return fetch_events_in_block_range(
        CONTRACT_ADDRESS,
        provider_url=alchemy_url,
        from_block=from_block,
        to_block=to_block,
    )


def test_connects_to_mainnet_chain():
    """Configured provider connects to Ethereum mainnet (chain_id=1)."""
    alchemy_url = os.getenv("ALCHEMY_URL")
    assert alchemy_url, "Missing ALCHEMY_URL. Set it in docker/.env or your shell environment."
    w3 = Web3(Web3.HTTPProvider(alchemy_url))
    assert w3.is_connected(), "Unable to reach the Alchemy endpoint"
    assert w3.eth.chain_id == 1


def test_fetch_events_in_recent_range_returns_some_entries(contract_events):
    """Fetcher returns at least one normalized entry in a recent range."""
    assert isinstance(contract_events, list)
    assert len(contract_events) > 0


def test_first_event_contains_event_property(contract_events):
    """Normalized events include the signature hash under `event`."""
    assert contract_events, "Expected at least one event"
    assert "event" in contract_events[0]


def test_first_event_has_graph_identity_and_ordering_fields(contract_events):
    """Normalized events include identity and ordering fields used for causality."""
    assert contract_events, "Expected at least one event"
    e = contract_events[0]
    assert e.get("tx_hash")
    assert e.get("log_index") is not None
    assert e.get("event_id") == f"{e['tx_hash']}:{e['log_index']}"
    assert e.get("block_number") is not None
    assert e.get("address")


def test_logs_most_recent_event(contract_events):
    """Log one sample event for debugging during local runs."""
    assert contract_events, "Expected at least one event"
    most_recent = contract_events[0]
    print(
        "Most recent event:",
        most_recent.get("event_name"),
        most_recent.get("event_id"),
        most_recent,
    )
    logger.info("Most recent event: %s", most_recent)