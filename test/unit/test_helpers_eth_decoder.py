import json
import pytest

@pytest.mark.unit
@pytest.mark.parametrize(
	"fixture_path",
	[
		"test/fixtures/eth_logs/erc20_transfer.json",
		"test/fixtures/eth_logs/uniswap_v2_swap.json",
		"test/fixtures/eth_logs/uniswap_v2_mint.json",
		"test/fixtures/eth_logs/uniswap_v2_burn.json",
		"test/fixtures/eth_logs/uniswap_v2_sync.json",
	],
)
def test_decode_log_unified(fixture_path):
	"""Given a known log fixture, unified decoder returns name + decoded args."""
	from web3 import Web3

	from dags.helpers.eth.logs.decoder import decode_log

	with open(fixture_path, "r", encoding="utf-8") as f:
		payload = json.load(f)

	raw_log = payload["raw_log"]
	expected = payload["expected"]

	decoded = decode_log(raw_log)

	# Normalize expected address strings to checksum to match decoder output.
	if expected.get("decoded"):
		for key in ("from", "to", "sender"):
			if key in expected["decoded"]:
				expected["decoded"][key] = Web3.to_checksum_address(expected["decoded"][key])

	assert decoded == expected
