from web3 import Web3, HTTPProvider
from os import getenv
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Fetch Alchemy URL from environment variables
ALCHEMY_URL = getenv("ALCHEMY_URL")
UNISWAP_V2_CONTRACT_ADDRESS = getenv("UNISWAP_V2_CONTRACT_ADDRESS")

def fetch_chain_id():
    """Fetch the chain ID from the Ethereum mainnet via Alchemy."""
    w3 = _get_web3_instance()
    if not w3.is_connected():
        raise ConnectionError("Unable to connect to the Ethereum mainnet via Alchemy.")
    return w3.eth.chain_id

def fetch_latest_block_number():
    """Fetch the latest block number from the Ethereum mainnet via Alchemy."""
    w3 = _get_web3_instance()
    return w3.eth.block_number

def fetch_logs():
    """Fetch recent logs from a specific contract on Ethereum mainnet via Alchemy."""
    w3 = _get_web3_instance()
    to_block = fetch_latest_block_number()
    from_block = max(0, to_block - 2000)
    
    logs = w3.eth.get_logs({
        'fromBlock': from_block,
        'toBlock': to_block,
        'address': _get_contract_address(),
    })
    
    return logs

def _get_alchemy_url():
    """Retrieve the Alchemy URL from environment variables."""
    if not ALCHEMY_URL:
        raise EnvironmentError("ALCHEMY_URL is not set in the environment variables.")
    return ALCHEMY_URL

def _get_web3_instance():
    """Create and return a Web3 instance connected to the Ethereum mainnet via Alchemy."""
    w3 = Web3(HTTPProvider(_get_alchemy_url()))
    if not w3.is_connected():
        raise ConnectionError("Unable to connect to the Ethereum mainnet via Alchemy.")
    return w3

def _get_contract_address():
    """Retrieve the Uniswap V2 contract address from environment variables."""
    if not UNISWAP_V2_CONTRACT_ADDRESS:
        raise EnvironmentError("UNISWAP_V2_CONTRACT_ADDRESS is not set in the environment variables.")
    return UNISWAP_V2_CONTRACT_ADDRESS