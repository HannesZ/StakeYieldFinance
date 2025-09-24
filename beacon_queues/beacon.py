from .config import BEACON_NODE_URL, DEPOSIT_CONTRACT_ADDRESS
from .http import get_with_retries

def get_deposit_contract_address_from_beacon():
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
    hdr_url = f"{BEACON_NODE_URL}/eth/v1/beacon/headers/{state_id}"
    hdr = get_with_retries(hdr_url)
    if not hdr or hdr.status_code != 200:
        print(f"Could not resolve header for state_id={state_id} (status {getattr(hdr,'status_code',None)})")
        return None

    try:
        hdr_data = hdr.json()["data"]
        block_id = hdr_data.get("root")
        if not block_id:
            print("Header response missing 'root'")
            return None
    except Exception:
        print("Invalid JSON from headers endpoint")
        return None

    blk = get_with_retries(f"{BEACON_NODE_URL}/eth/v2/beacon/blocks/{block_id}")           or get_with_retries(f"{BEACON_NODE_URL}/eth/v1/beacon/blocks/{block_id}")
    if not blk or blk.status_code != 200:
        print(f"Could not fetch block for root {block_id} (status {getattr(blk,'status_code',None)})")
        return None

    try:
        j = blk.json()
        data = j.get("data") or j
        message = data.get("message") or data.get("signed_block", {}).get("message") or {}
        body = message.get("body", {})
        eth1_data = body.get("eth1_data", {})
        deposit_count = eth1_data.get("deposit_count")
        if deposit_count is None:
            print("Block JSON missing eth1_data.deposit_count")
            return None
        return int(deposit_count)
    except Exception as e:
        print(f"Failed to parse deposit_count: {e}")
        return None
