from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

try:
    from helpers.evm.traces.http import post_json_rpc
    from helpers.evm.traces.rpc import build_trace_block_request
    from helpers.evm.traces.rpc import build_trace_transaction_request
except ModuleNotFoundError as e:
    raise SystemExit(
        f"Missing dependency while importing helpers: {e}.\n\n"
        "Run this script with the project venv instead:\n"
        "  ./.venv/bin/python scripts/refresh_evm_traces_fixture.py\n"
    )


DEFAULT_ETH_LOGS_FIXTURE = Path("test/fixtures/eth_logs/fetch_logs_uniswap_v2_weth_usdc.json")
DEFAULT_TRACE_TX_FIXTURE_PATH = Path("test/fixtures/evm_traces/trace_transaction_sample.json")
DEFAULT_TRACE_BLOCK_FIXTURE_PATH = Path("test/fixtures/evm_traces/trace_block_sample.json")


def _as_int(value: object) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        if value.startswith("0x"):
            return int(value, 16)
        return int(value)
    raise TypeError("expected int or str")


def _default_tx_hash_from_eth_logs_fixture(path: Path) -> str:
    raw = json.loads(path.read_text())

    if isinstance(raw, dict) and isinstance(raw.get("result"), list):
        logs = raw["result"]
    elif isinstance(raw, list):
        logs = raw
    else:
        raise ValueError("Unexpected eth logs fixture shape")

    for item in logs:
        if not isinstance(item, dict):
            continue
        tx_hash = item.get("transactionHash") or item.get("tx_hash")
        if isinstance(tx_hash, str) and tx_hash:
            return tx_hash

    raise ValueError("No transactionHash found in eth logs fixture")


def _default_block_number_from_eth_logs_fixture(path: Path) -> int:
    raw = json.loads(path.read_text())

    if isinstance(raw, dict) and isinstance(raw.get("result"), list):
        logs = raw["result"]
    elif isinstance(raw, list):
        logs = raw
    else:
        raise ValueError("Unexpected eth logs fixture shape")

    for item in logs:
        if not isinstance(item, dict):
            continue
        block_number = item.get("blockNumber") if "blockNumber" in item else item.get("block_number")
        if block_number is None:
            continue
        return _as_int(block_number)

    raise ValueError("No blockNumber found in eth logs fixture")


def refresh_evm_traces_fixture(*, output_file: Path, alchemy_url: str, tx_hash: str, request_id: int = 1) -> int:
    output_file.parent.mkdir(parents=True, exist_ok=True)

    payload = build_trace_transaction_request(tx_hash=tx_hash, request_id=request_id)
    try:
        response = post_json_rpc(url=alchemy_url, payload=payload)
    except Exception as e:  # noqa: BLE001
        response_text = getattr(getattr(e, "response", None), "text", None)
        inned = (
            f"trace_transaction fixture refresh failed (tx_hash={tx_hash}).\n"
            "Most common causes:\n"
            "- running with system python instead of .venv (missing requests/web3 deps)\n"
            "- invalid ALCHEMY_URL / network issue\n\n"
            "Run with:\n"
            "  ALCHEMY_URL=... ./.venv/bin/python scripts/refresh_evm_traces_fixture.py\n\n"
            f"Original error: {e}"
        )
        if isinstance(response_text, str) and response_text.strip():
            inned = inned + f"\n\nHTTP response body: {response_text.strip()}"
        raise SystemExit(inned) from e

    output_file.write_text(json.dumps(response, indent=2, sort_keys=True) + "\n")
    return 1


def refresh_evm_trace_block_fixture(*, output_file: Path, alchemy_url: str, block_number: int, request_id: int = 1) -> int:
    output_file.parent.mkdir(parents=True, exist_ok=True)

    payload = build_trace_block_request(block_number=block_number, request_id=request_id)
    try:
        response = post_json_rpc(url=alchemy_url, payload=payload)
    except Exception as e:  # noqa: BLE001
        response_text = getattr(getattr(e, "response", None), "text", None)
        inned = (
            f"trace_block fixture refresh failed (block_number={block_number}).\n"
            "Most common causes:\n"
            "- block_number is above chain tip (e.g. 99999999)\n"
            "- running with system python instead of .venv (missing requests/web3 deps)\n"
            "- invalid ALCHEMY_URL / network issue\n\n"
            "Try a known-good default (derives block from eth logs fixture):\n"
            "  ALCHEMY_URL=... ./.venv/bin/python scripts/refresh_evm_traces_fixture.py --mode block\n\n"
            f"Original error: {e}"
        )
        if isinstance(response_text, str) and response_text.strip():
            inned = inned + f"\n\nHTTP response body: {response_text.strip()}"
        raise SystemExit(inned) from e

    output_file.write_text(json.dumps(response, indent=2, sort_keys=True) + "\n")
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Refresh the shared behavior-test EVM traces fixture")

    parser.add_argument(
        "--mode",
        choices=["transaction", "block"],
        default="transaction",
        help="Which Alchemy trace method to fetch: trace_transaction or trace_block",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Path to write the raw trace fixture JSON",
    )
    parser.add_argument(
        "--tx-hash",
        default=None,
        help="Transaction hash to fetch traces for. If omitted, derives it from the eth logs fixture.",
    )
    parser.add_argument(
        "--eth-logs-fixture",
        default=str(DEFAULT_ETH_LOGS_FIXTURE),
        help="Path to an eth logs fixture JSON used to derive defaults (tx hash / block number).",
    )
    parser.add_argument(
        "--block-number",
        default=None,
        help="Block number to fetch traces for (used when --mode=block). If omitted, derives it from the eth logs fixture.",
    )
    parser.add_argument(
        "--alchemy-url",
        default=None,
        help="Alchemy HTTPS URL. If omitted, uses ALCHEMY_URL from the environment.",
    )
    parser.add_argument(
        "--request-id",
        type=int,
        default=1,
        help="JSON-RPC request id to use.",
    )

    args = parser.parse_args(argv)

    alchemy_url = args.alchemy_url or os.getenv("ALCHEMY_URL")
    if not alchemy_url:
        raise SystemExit(
            "Missing ALCHEMY_URL. Provide --alchemy-url or set ALCHEMY_URL in your environment.\n"
            "Example:\n"
            "  ALCHEMY_URL=... ./.venv/bin/python scripts/refresh_evm_traces_fixture.py\n"
        )

    eth_logs_fixture = Path(args.eth_logs_fixture)
    if args.mode == "transaction":
        tx_hash = args.tx_hash
        if not tx_hash:
            tx_hash = _default_tx_hash_from_eth_logs_fixture(eth_logs_fixture)

        out_path = Path(args.out) if args.out else DEFAULT_TRACE_TX_FIXTURE_PATH
        written = refresh_evm_traces_fixture(
            output_file=out_path,
            alchemy_url=alchemy_url,
            tx_hash=tx_hash,
            request_id=args.request_id,
        )
        print(f"wrote {written} trace response to {out_path} (tx_hash={tx_hash})")
    else:
        block_number = args.block_number
        if block_number is None:
            block_number = _default_block_number_from_eth_logs_fixture(eth_logs_fixture)
        block_number_int = _as_int(block_number)

        out_path = Path(args.out) if args.out else DEFAULT_TRACE_BLOCK_FIXTURE_PATH
        written = refresh_evm_trace_block_fixture(
            output_file=out_path,
            alchemy_url=alchemy_url,
            block_number=block_number_int,
            request_id=args.request_id,
        )
        print(f"wrote {written} trace response to {out_path} (block_number={block_number_int})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
