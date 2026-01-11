import pytest
from dags.helpers.eth.adapter import fetch_chain_id

def test_chain_id_is_1():
    assert fetch_chain_id() == 1, "Chain ID is not 1 for Ethereum Mainnet"