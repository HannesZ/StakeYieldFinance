from config import BEACON_NODE_URL, DEPOSIT_CONTRACT_ADDRESS
from http import get_with_retries

def get_deposit_contract_address_from_beacon():
    """
    Retrieve the deposit contract address from the beacon node’s REST API.
    This function first attempts to get the address from the `/eth/v1/config/deposit_contract`
    endpoint. If that fails, it falls back to `/eth/v1/config/spec` to
    extract the `DEPOSIT_CONTRACT_ADDRESS` field.
    Returns:

        str | None: The deposit contract address if found, otherwise None.
    """

    url1 = f"{BEACON_NODE_URL}/eth/v1/config/deposit_contract"
    r = get_with_retries(url1)
    if r and r.status_code == 200:
        try:
            return r.json()["data"]["address"]
        except Exception:
            pass
    url2 = f"{BEACON_NODE_URL}/eth/v1/config/spec"
    r = get_with_retries(url2)
    if r and r.status_code == 200:
        try:
            data = r.json().get("data", {})
            addr = data.get("DEPOSIT_CONTRACT_ADDRESS") or data.get("DepositContractAddress")
            if addr:
                return addr
        except Exception:
            pass
    return DEPOSIT_CONTRACT_ADDRESS

def get_validators_by_status(state_id="head", status="pending_queued"):
    """
    Retrieve a list of validators from the beacon chain filtered by their status.
    This function queries a beacon node’s REST API to get validators in a specific
    state (identified by `state_id`) and with a given status.

    Args:



        state_id (str): Identifier of the beacon state or block root.
                        Common values include:
                            - "head": the node’s current head block 
                            - "finalized": the latest finalized block
                            - "justified": the latest justified block
                            - or a block root / slot number
                        Defaults to "head".
        status (str): Comma-separated list of validator statuses to filter by.

                        Examples include "pending_queued", "active_ongoing",

                        "active_exiting", "active_slashed", "withdrawal_possible", etc.
                        A full list of statuses can be found in the Ethereum consensus
                        specifications.
                        Defaults to "pending_queued".   
    Returns:
        list: A list of validator objects matching the specified status.
                Returns an empty list if the request fails or no validators match.
    """
    
    url = f"{BEACON_NODE_URL}/eth/v1/beacon/states/{state_id}/validators"
    resp = get_with_retries(url, params={"status": status})
    if resp is None:
        return []
    try:
        return resp.json().get("data", [])
    except ValueError:
        print(f"Invalid JSON for state {state_id} / status {status}")
        return []

def sum_effective_eth(validators):
    total_gwei = 0
    for v in validators:
        try:
            total_gwei += int(v["validator"]["effective_balance"])
        except (KeyError, ValueError, TypeError):
            continue
    return total_gwei / 1e9

def get_beacon_processed_deposit_count(state_id="head"):
    """
    Retrieve the cumulative deposit count from the beacon chain for a given state.

    This function queries a beacon node’s REST API to determine the number of
    validator deposits acknowledged by the beacon chain up to the block
    corresponding to the given `state_id`.

    Specifically, it extracts `eth1_data.deposit_count` from the block body,
    which represents the total number of deposits observed in the Ethereum
    deposit contract (on the execution layer) that have been included in
    the beacon chain’s view of Eth1.

    Args:
        state_id (str): Identifier of the beacon state or block root.
                        Common values include:
                          - "head": the node’s current head block
                          - "finalized": the latest finalized block
                          - "justified": the latest justified block
                          - or a block root / slot number
                        Defaults to "head".

    Returns:
        int | None: The cumulative deposit count if available, otherwise None.
    """

    # Fetch the block header for the given state_id
    hdr_url = f"{BEACON_NODE_URL}/eth/v1/beacon/headers/{state_id}"
    hdr = get_with_retries(hdr_url)
    if not hdr or hdr.status_code != 200:
        print(f"Could not resolve header for state_id={state_id} (status {getattr(hdr,'status_code',None)})")
        return None

    try:
        # Parse the JSON response to extract the block root (unique ID for the block)
        hdr_data = hdr.json()["data"]
        block_id = hdr_data.get("root")
        if not block_id:
            print("Header response missing 'root'")
            return None
    except Exception:
        print("Invalid JSON from headers endpoint")
        return None

    # Fetch the full beacon block data using the block root
    blk = (
        get_with_retries(f"{BEACON_NODE_URL}/eth/v2/beacon/blocks/{block_id}")
        or get_with_retries(f"{BEACON_NODE_URL}/eth/v1/beacon/blocks/{block_id}")
    )
    if not blk or blk.status_code != 200:
        print(f"Could not fetch block for root {block_id} (status {getattr(blk,'status_code',None)})")
        return None

    try:
        # Extract the block body from the JSON response
        j = blk.json()
        data = j.get("data") or j
        message = data.get("message") or data.get("signed_block", {}).get("message") or {}
        body = message.get("body", {})

        # Get eth1_data, which links the beacon chain to the execution-layer deposit contract
        eth1_data = body.get("eth1_data", {})

        # The cumulative deposit count recorded at this block
        deposit_count = eth1_data.get("deposit_count")
        if deposit_count is None:
            print("Block JSON missing eth1_data.deposit_count")
            return None

        return int(deposit_count)
    except Exception as e:
        print(f"Failed to parse deposit_count: {e}")
        return None
