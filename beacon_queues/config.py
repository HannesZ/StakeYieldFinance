import os
from dotenv import load_dotenv

load_dotenv()

def get_env_for_chain(base_key: str, chain_id: str):
    """
    Prefer CHAIN_ID-suffixed env (e.g. EXECUTION_RPC_URL_1) over generic (EXECUTION_RPC_URL).
    Return None if neither is set.
    """
    return os.getenv(f"{base_key}_{chain_id}") or os.getenv(base_key)

# Select network (string, e.g. "1", "11155111", "17000", "560048")
CHAIN_ID = os.getenv("CHAIN_ID", "1").strip()

# Use external EL node for backlog?
USE_EXTERNAL_NODE = os.getenv("USE_EXTERNAL_NODE_FOR_EL_BACKLOG", "true").lower() == "true"

# EL RPC
if USE_EXTERNAL_NODE:
    EXECUTION_RPC_URL = get_env_for_chain("EXTERNAL_EXECUTION_RPC_URL", CHAIN_ID)
else:
    EXECUTION_RPC_URL = get_env_for_chain("EXECUTION_RPC_URL", CHAIN_ID)
if not EXECUTION_RPC_URL:
    raise RuntimeError(
        f"Missing EL RPC URL. Set "
        f"{'EXTERNAL_EXECUTION_RPC_URL' if USE_EXTERNAL_NODE else 'EXECUTION_RPC_URL'} "
        f"or a chain-specific variant with _{CHAIN_ID}."
    )

# CL (beacon) RPC
BEACON_NODE_URL = get_env_for_chain("BEACON_NODE_URL", CHAIN_ID)
if not BEACON_NODE_URL:
    raise RuntimeError(f"BEACON_NODE_URL (or BEACON_NODE_URL_{CHAIN_ID}) is required.")

# Deposit contract info
DEPOSIT_CONTRACT_ADDRESS = get_env_for_chain("DEPOSIT_CONTRACT_ADDRESS", CHAIN_ID)
if not DEPOSIT_CONTRACT_ADDRESS:
    raise RuntimeError(f"DEPOSIT_CONTRACT_ADDRESS (or ..._{CHAIN_ID}) is required.")

_deposit_block_env = get_env_for_chain("DEPOSIT_CONTRACT_CREATION_BLOCK", CHAIN_ID)
if not _deposit_block_env:
    raise RuntimeError(f"DEPOSIT_CONTRACT_CREATION_BLOCK (or ..._{CHAIN_ID}) is required.")
DEPOSIT_CONTRACT_CREATION_BLOCK = int(_deposit_block_env)
