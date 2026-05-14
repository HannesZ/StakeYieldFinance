# scripts/debug_backlog.py
import sys
from pathlib import Path
import logging

# Make repo root importable so "import beacon_queues" works
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from beacon_queues.config import (
    EXECUTION_RPC_URL, DEPOSIT_CONTRACT_CREATION_BLOCK
)
from beacon_queues.beacon import (
    get_deposit_contract_address_from_beacon,
    get_beacon_processed_deposit_count,
)
from beacon_queues.el_backlog import get_el_backlog_precise

def main():
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    addr = get_deposit_contract_address_from_beacon()
    print("Deposit contract:", addr)

    processed = get_beacon_processed_deposit_count("head")
    print("Beacon processed count:", processed)

    pending_events, pending_gwei = get_el_backlog_precise(
        EXECUTION_RPC_URL,
        addr,
        processed_count=processed,
        cache_path="deposit_index.cache.json",
        from_block_hint=DEPOSIT_CONTRACT_CREATION_BLOCK or None,
    )

    print("Backlog -> events:", pending_events, "gwei:", pending_gwei)

if __name__ == "__main__":
    main()
