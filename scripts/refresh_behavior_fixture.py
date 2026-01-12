from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

try:
    from helpers.eth.adapter import fetch_logs, write_logs_to_file
except ModuleNotFoundError as e:
    # Common case: user runs with system python which doesn't have web3 installed.
    raise SystemExit(
        f"Missing dependency while importing DAG helpers: {e}.\n\n"
        "Run this script with the project venv instead:\n"
        "  PYTHONPATH=docker ./.venv/bin/python scripts/refresh_behavior_fixture.py\n\n"
        "(Or install dependencies into the python you are using.)\n"
    )


DEFAULT_FIXTURE_PATH = Path("test/fixtures/eth_logs/fetch_logs_uniswap_v2_weth_usdc.json")


def refresh_behavior_fixture(output_file: Path = DEFAULT_FIXTURE_PATH) -> int:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    raw_logs = fetch_logs()
    return write_logs_to_file(raw_logs, str(output_file))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Refresh the shared behavior-test eth logs fixture")
    parser.add_argument(
        "--out",
        default=str(DEFAULT_FIXTURE_PATH),
        help="Path to write the raw logs fixture JSON",
    )
    args = parser.parse_args(argv)

    written = refresh_behavior_fixture(Path(args.out))
    print(f"wrote {written} logs to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
