import os
from dotenv import load_dotenv

load_dotenv()

USE_EXTERNAL_NODE = os.getenv("USE_EXTERNAL_NODE_FOR_EL_BACKLOG", "true").lower() == "true"
EXECUTION_RPC_URL = os.getenv("EXTERNAL_EXECUTION_RPC_URL") if USE_EXTERNAL_NODE else os.getenv("EXECUTION_RPC_URL")

BEACON_NODE_URL = os.getenv("BEACON_NODE_URL")
if not BEACON_NODE_URL:
    raise RuntimeError("BEACON_NODE_URL is not set. Put it in your .env or environment.")

DEPOSIT_CONTRACT_ADDRESS = os.getenv("DEPOSIT_CONTRACT_ADDRESS")
DEPOSIT_CONTRACT_CREATION_BLOCK = int(os.getenv("DEPOSIT_CONTRACT_CREATION_BLOCK") or 0)
