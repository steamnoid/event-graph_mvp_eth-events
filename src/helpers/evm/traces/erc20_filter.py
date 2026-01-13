ERC20_TRANSFER_TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def receipt_has_erc20_transfer(receipt: dict) -> bool:
    logs = receipt.get("logs") if isinstance(receipt, dict) else None
    if not isinstance(logs, list):
        return False

    for log in logs:
        if not isinstance(log, dict):
            continue
        topics = log.get("topics")
        if not isinstance(topics, list) or not topics:
            continue
        topic0 = topics[0]
        if isinstance(topic0, str) and topic0.lower() == ERC20_TRANSFER_TOPIC0:
            return True

    return False
