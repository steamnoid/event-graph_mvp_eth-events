from web3 import Web3, HttpProvider

def fetch_chain_id():
    """Fetch the chain ID from the Ethereum mainnet via Alchemy."""
    alchemy_url = "https://eth-mainnet.g.alchemy.com/v2/wTYnccIW9DN1QL1j8xGpP"
    w3 = Web3(Web3.HTTPProvider(alchemy_url))
    if not w3.is_connected():
        raise ConnectionError("Unable to connect to the Ethereum mainnet via Alchemy.")
    return w3.eth.chain_id    