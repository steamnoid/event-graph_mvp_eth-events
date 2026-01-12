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

	from helpers.eth.logs.decoder import decode_log

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


@pytest.mark.unit
def test_decoder_handles_web3_keccak_hex_with_0x_prefix(monkeypatch):
	"""Given web3 keccak().hex() returns a 0x-prefixed string, decoder still matches canonical topic0."""
	import importlib
	
	import web3
	
	transfer_topic0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

	original_keccak = web3.Web3.keccak

	class _HexWith0x:
		def __init__(self, inner):
			self._inner = inner

		def hex(self):
			h = self._inner.hex()
			return h if h.startswith("0x") else "0x" + h

	def keccak_with_0x(*, text=None, primitive=None):
		return _HexWith0x(original_keccak(text=text, primitive=primitive))

	monkeypatch.setattr(web3.Web3, "keccak", keccak_with_0x)
	
	from helpers.eth.logs import decoder as decoder_module
	decoder_module = importlib.reload(decoder_module)

	assert decoder_module._ERC20_TRANSFER_TOPIC0 == transfer_topic0
	assert (
		decoder_module.decode_event_name({"topics": [transfer_topic0]})
		== "Transfer"
	)

	monkeypatch.setattr(web3.Web3, "keccak", original_keccak)
	importlib.reload(decoder_module)
