import os
import csv
import time
import requests
from dotenv import load_dotenv
# ------------------ EL deposit backlog (precise) ------------------
from web3 import Web3
import json
import pathlib

# Choose which execution node to use for EL backlog queries.
use_external_node=True
if use_external_node:
    print("Using external node for EL backlog queries as per USE_EXTERNAL_NODE_FOR_EL_BACKLOG")
    EXECUTION_RPC_URL = os.getenv("EXTERNAL_EXECUTION_RPC_URL")
else:
    EXECUTION_RPC_URL = os.getenv("EXECUTION_RPC_URL")
    

DEPOSIT_CONTRACT_ADDRESS = os.getenv("DEPOSIT_CONTRACT_ADDRESS")
DEPOSIT_CONTRACT_CREATION_BLOCK = os.getenv("DEPOSIT_CONTRACT_CREATION_BLOCK")

# Minimal ABI to decode DepositEvent
DEPOSIT_EVENT_ABI = json.loads("""
[{
  "anonymous": false,
  "inputs": [
    {"indexed": false, "internalType": "bytes", "name": "pubkey", "type": "bytes"},
    {"indexed": false, "internalType": "bytes", "name": "withdrawal_credentials", "type": "bytes"},
    {"indexed": false, "internalType": "bytes", "name": "amount", "type": "bytes"},
    {"indexed": false, "internalType": "bytes", "name": "signature", "type": "bytes"},
    {"indexed": false, "internalType": "bytes", "name": "index", "type": "bytes"}
  ],
  "name": "DepositEvent",
  "type": "event"
}]
""")
def get_deposit_contract_address_from_beacon():
    """
    Some beacon nodes expose the deposit contract address via config.
    We'll try a couple of endpoints; fall back to env if unavailable.
    """
    # Teku/others: /eth/v1/config/deposit_contract (preferred, if supported)
    url1 = f"{BEACON_NODE_URL}/eth/v1/config/deposit_contract"
    r = _get_with_retries(url1)
    if r and r.status_code == 200:
        try:
            return r.json()["data"]["address"]
        except Exception:
            pass

    # Fallback: try /eth/v1/config/spec and look for DEPOSIT_CONTRACT_ADDRESS
    url2 = f"{BEACON_NODE_URL}/eth/v1/config/spec"
    r = _get_with_retries(url2)
    if r and r.status_code == 200:
        try:
            data = r.json().get("data", {})
            addr = data.get("DEPOSIT_CONTRACT_ADDRESS") or data.get("DepositContractAddress")
            if addr:
                return addr
        except Exception:
            pass

    # Final fallback: env
    return DEPOSIT_CONTRACT_ADDRESS

def get_beacon_processed_deposit_count(state_id="head"):
    """
    Return eth1_data.deposit_count at the given block/state,
    using block routes that Teku supports.

    Strategy:
      1) Resolve a canonical block root for {state_id} via /eth/v1/beacon/headers/{state_id}
      2) Fetch that block via /eth/v2/beacon/blocks/{root}
      3) Read message.body.eth1_data.deposit_count
    """
    # 1) Resolve a canonical block root (works for "head", a slot, block root, etc.)
    hdr_url = f"{BEACON_NODE_URL}/eth/v1/beacon/headers/{state_id}"
    hdr = _get_with_retries(hdr_url)
    if not hdr or hdr.status_code != 200:
        print(f"Could not resolve header for state_id={state_id} (status {getattr(hdr,'status_code',None)})")
        return None

    try:
        hdr_data = hdr.json()["data"]
        # Teku returns: {"root": "...", "canonical": true, "header": {...}}
        block_id = hdr_data.get("root")
        if not block_id:
            print("Header response missing 'root'")
            return None
    except Exception:
        print("Invalid JSON from headers endpoint")
        return None

    # 2) Fetch the block by root (v2 preferred)
    blk_url = f"{BEACON_NODE_URL}/eth/v2/beacon/blocks/{block_id}"
    blk = _get_with_retries(blk_url)
    if not blk or blk.status_code != 200:
        # Fallback to v1 if v2 isn’t available
        blk_url = f"{BEACON_NODE_URL}/eth/v1/beacon/blocks/{block_id}"
        blk = _get_with_retries(blk_url)
        if not blk or blk.status_code != 200:
            print(f"Could not fetch block for root {block_id} (status {getattr(blk,'status_code',None)})")
            return None

    # 3) Parse deposit_count from the block body
    try:
        j = blk.json()
        # v2 shape: {"data":{"message":{"body":{"eth1_data":{"deposit_count":"123"}}}}}
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
    
def _decode_amount_bytes_to_gwei(b: bytes) -> int:
    # 8-byte little-endian gwei amount as emitted by the deposit contract
    if not b or len(b) < 8:
        return 0
    return int.from_bytes(b[:8], byteorder="little", signed=False)

def _w3(exec_url: str) -> Web3:
    w3 = Web3(Web3.HTTPProvider(exec_url, request_kwargs={"timeout": 60}))
    if not w3.is_connected():
        raise RuntimeError("Could not connect to EXECUTION_RPC_URL")
    return w3

def _load_cache(path: str) -> dict:
    p = pathlib.Path(path)
    if not p.exists():
        return {"checkpoints": []}  # list of {"event_index": int, "block": int, "log_index": int}
    try:
        return json.loads(p.read_text())
    except Exception:
        return {"checkpoints": []}

def _save_cache(path: str, data: dict):
    pathlib.Path(path).write_text(json.dumps(data))

def _best_checkpoint(cache: dict, target_index: int) -> dict | None:
    # Return the checkpoint with the largest event_index <= target_index
    cps = sorted(cache.get("checkpoints", []), key=lambda c: c["event_index"])
    best = None
    for cp in cps:
        if cp["event_index"] <= target_index:
            best = cp
        else:
            break
    return best

def _push_checkpoint(cache: dict, event_index: int, block: int, log_index: int):
    cache.setdefault("checkpoints", []).append({
        "event_index": int(event_index),
        "block": int(block),
        "log_index": int(log_index)
    })

def _scan_logs(w3: Web3, addr: str, start_block: int, end_block: int, step: int):
    """Yield logs in increasing block ranges; resiliently shrink step on provider limits."""
    addr = Web3.to_checksum_address(addr)
    curr = int(start_block)
    end = int(end_block)
    step_curr = int(step)
    while curr <= end:
        to_blk = min(curr + step_curr - 1, end)
        try:
            logs = w3.eth.get_logs({"fromBlock": curr, "toBlock": to_blk, "address": addr})
            yield logs
            curr = to_blk + 1
            # opportunistically grow a bit (helps with fast providers)
            if step_curr < 50_000:
                step_curr = min(50_000, step_curr + 2_000)
        except Exception:
            # back off the window if provider chokes
            if step_curr > 2_000:
                step_curr = max(2_000, step_curr // 2)
            else:
                # skip problematic block and move on
                curr = to_blk + 1

def _find_boundary_and_pending(exec_url: str,
                               contract_address: str,
                               processed_count: int,
                               cache_path: str,
                               from_block_hint: int | None = None,
                               initial_step: int = 20_000):
    """
    Find the exact boundary (block/logIndex) of the processed_count-th DepositEvent,
    then sum precise pending gwei for events with (block,logIndex) strictly AFTER that.
    Returns: (pending_events, pending_gwei, total_events, total_gwei)
    Also updates cache with useful checkpoints.
    """
    if processed_count is None:
        return 0, 0, 0, 0

    w3 = _w3(exec_url)
    latest_block = w3.eth.block_number
    cache = _load_cache(cache_path)
    # Choose a starting point
    cp = _best_checkpoint(cache, processed_count)  # <= processed_count
    if cp:
        start_block = max(0, int(cp["block"]))
        event_index_so_far = int(cp["event_index"])
        # When resuming within the same block we need to respect log ordering
        boundary_found = (event_index_so_far == processed_count)
        boundary_block = int(cp["block"]) if boundary_found else None
        boundary_log_index = int(cp["log_index"]) if boundary_found else None
    else:
        # Optional fast path: if you know the contract creation block, set from_block_hint
        start_block = from_block_hint if from_block_hint is not None else 0
        event_index_so_far = 0
        boundary_found = (processed_count == 0)
        boundary_block = -1 if boundary_found else None  # before genesis
        boundary_log_index = -1 if boundary_found else None

    pending_events = 0
    pending_gwei = 0
    total_events = 0
    total_gwei = 0

    # We will make two passes in one linear scan to latest:
    # 1) scan until we reach the processed boundary (counting)
    # 2) then sum amounts strictly after boundary to latest
    reached_boundary = boundary_found

    for logs in _scan_logs(w3, contract_address, start_block, latest_block, initial_step):
        # logs are naturally ordered by (blockNumber, logIndex)
        # First, if we haven't reached boundary, walk until processed_count
        if not reached_boundary:
            for i, log in enumerate(logs):
                event_index_so_far += 1
                # keep total stats too
                total_events += 1
                try:
                    # decode and add to total_gwei
                    deposit_event = w3.eth.contract(abi=DEPOSIT_EVENT_ABI).events.DepositEvent
                    decoded = deposit_event().process_log(log)
                    total_gwei += _decode_amount_bytes_to_gwei(decoded["args"]["amount"])
                except Exception:
                    pass

                if event_index_so_far == processed_count:
                    reached_boundary = True
                    boundary_block = log["blockNumber"]
                    boundary_log_index = log["logIndex"]
                    # Add a precise checkpoint at the boundary
                    _push_checkpoint(cache, event_index_so_far, boundary_block, boundary_log_index)
                    # Remaining logs in this same batch occurring AFTER the boundary count toward pending
                    tail = logs[i+1:]
                    for tlog in tail:
                        pending_events += 1
                        total_events += 1
                        try:
                            decoded2 = deposit_event().process_log(tlog)
                            amt = _decode_amount_bytes_to_gwei(decoded2["args"]["amount"])
                            pending_gwei += amt
                            total_gwei += amt
                        except Exception:
                            pass
                    break  # stop iterating this batch; outer loop continues to future ranges
            else:
                # We didn't hit boundary in this batch, continue to next batch
                continue
        else:
            # Already past boundary: all logs are pending
            total_events += len(logs)
            for log in logs:
                pending_events += 1
                try:
                    deposit_event = w3.eth.contract(abi=DEPOSIT_EVENT_ABI).events.DepositEvent
                    decoded = deposit_event().process_log(log)
                    amt = _decode_amount_bytes_to_gwei(decoded["args"]["amount"])
                    pending_gwei += amt
                    total_gwei += amt
                except Exception:
                    pass

    # If we never reached boundary but processed_count > total_events, clamp to zero backlog
    if not reached_boundary and processed_count > (event_index_so_far):
        # We saw fewer events than the beacon claims processed (EL not archival / behind)
        # In that case we cannot compute pending; return zeros (and log a warning).
        print("Warning: EL node returned fewer deposit events than beacon's processed_count. "
              "Consider using an archive node or a higher from_block_hint.")
        pending_events = 0
        pending_gwei = 0

    # Save cache for next runs
    _save_cache(cache_path, cache)
    return pending_events, pending_gwei, total_events, total_gwei

def get_el_backlog_precise(exec_url: str,
                           contract_address: str,
                           processed_count: int,
                           cache_path: str = "deposit_index.cache.json",
                           from_block_hint: int | None = None):
    """
    Public wrapper: returns (pending_events, pending_gwei)
    """
    pe, pg, _te, _tg = _find_boundary_and_pending(
        exec_url,
        contract_address,
        int(processed_count) if processed_count is not None else 0,
        cache_path=cache_path,
        from_block_hint=from_block_hint,
        initial_step=20_000
    )
    return pe, pg

# ------------------ Setup ------------------
load_dotenv()

BEACON_NODE_URL = os.getenv("BEACON_NODE_URL")
if not BEACON_NODE_URL:
    raise RuntimeError("BEACON_NODE_URL is not set. Put it in your .env or environment.")

# ------------------ HTTP helper with retries ------------------
def _get_with_retries(url, params=None, retries=3, backoff=1.5, timeout=20):
    attempt = 0
    while True:
        try:
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code == 200:
                return r
            attempt += 1
            if attempt > retries:
                print(f"HTTP {r.status_code} for {url} with {params}. Giving up.")
                return None
            time.sleep(backoff ** attempt)
        except requests.RequestException as e:
            attempt += 1
            if attempt > retries:
                print(f"Request error for {url} with {params}: {e}. Giving up.")
                return None
            time.sleep(backoff ** attempt)

# ------------------ Beacon helpers ------------------
def get_validators_by_status(state_id="head", status="pending_queued"):
    """
    Returns a list of validator objects for the given state_id and status filter.
    """
    url = f"{BEACON_NODE_URL}/eth/v1/beacon/states/{state_id}/validators"
    params = {"status": status}
    resp = _get_with_retries(url, params=params)
    if resp is None:
        return []
    try:
        return resp.json().get("data", [])
    except ValueError:
        print(f"Invalid JSON for state {state_id} / status {status}")
        return []

def sum_effective_eth(validators):
    """
    Sum validators' effective_balance and convert from gwei to ETH.
    """
    total_gwei = 0
    for v in validators:
        # Many clients return decimal strings; be robust:
        try:
            total_gwei += int(v["validator"]["effective_balance"])
        except (KeyError, ValueError, TypeError):
            continue
    return total_gwei / 1e9

# ------------------ CSV helpers ------------------
CSV_HEADER = [
    "epoch",
    "slot",
    "active_count",
    "active_eth",
    "entry_queue_count",
    "entry_queue_eth",
    "exit_queue_count",
    "exit_queue_eth",
    "el_backlog_events",
    "el_backlog_eth",
    "entry_total_estimated"
]


def _csv_has_header(path):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return False
    with open(path, "r", newline="") as f:
        try:
            first = next(csv.reader(f))
            return first == CSV_HEADER
        except StopIteration:
            return False

def _last_written_slot(path):
    """
    Return the last slot written (int), or None if no data rows yet.
    """
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return None
    last_row = None
    with open(path, "r", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            last_row = row
    # If the last row is the header, there are no data rows
    if last_row and last_row[0] != "epoch":
        try:
            return int(last_row[1])  # slot column
        except (ValueError, IndexError):
            return None
    return None


def get_deposit_contract_address_from_beacon():
    url1 = f"{BEACON_NODE_URL}/eth/v1/config/deposit_contract"
    r = _get_with_retries(url1)
    if r and r.status_code == 200:
        try:
            return r.json()["data"]["address"]
        except Exception:
            pass
    url2 = f"{BEACON_NODE_URL}/eth/v1/config/spec"
    r = _get_with_retries(url2)
    if r and r.status_code == 200:
        try:
            data = r.json().get("data", {})
            addr = data.get("DEPOSIT_CONTRACT_ADDRESS") or data.get("DepositContractAddress")
            if addr:
                return addr
        except Exception:
            pass
    return DEPOSIT_CONTRACT_ADDRESS

deposit_contract = get_deposit_contract_address_from_beacon()

# ------------------ Main (resume-safe) ------------------
def query_epochs(start_slot, end_slot, interval=1, filename=None, sleep_between=0.05):
    """
    Resume-safe query: if filename exists and contains rows,
    resume from last_slot + interval; otherwise start at start_slot.

    Writes both counts and ETH-weighted metrics (effective balances).
    """
    if filename is None:
        filename = f"beacon_chain_queues_{start_slot}_{end_slot}.csv"

    needs_header = not _csv_has_header(filename)
    last_slot = _last_written_slot(filename)

    if last_slot is None:
        resume_slot = start_slot
    else:
        resume_slot = last_slot + interval
        if resume_slot < start_slot:
            resume_slot = start_slot

    if resume_slot > end_slot:
        print(f"Nothing to do. CSV already covers up to slot {last_slot}.")
        print(f"Results are in {filename}")
        return filename

    # Open file (append mode) and write header if needed
    with open(filename, "a", newline="") as file:
        writer = csv.writer(file)
        if needs_header:
            writer.writerow(CSV_HEADER)

        # Carry-over counters for between-epoch slots
        initialized_epoch_block = False
        active_count = active_eth = 0
        entry_count = entry_eth = 0.0
        exit_count = exit_eth = 0.0

        for slot in range(resume_slot, end_slot + 1, interval):
            # Refresh fully at epoch boundaries (or first iteration)
            if slot % 32 == 0 or not initialized_epoch_block:
                print(f"\n**************************************************************************** Epoch {slot//32} ****************************************************************************")

                entry_queue = get_validators_by_status(state_id=slot, status="pending_queued")
                exit_queue  = get_validators_by_status(state_id=slot, status="active_exiting,active_slashed")
                active_ongoing = get_validators_by_status(state_id=slot, status="active_ongoing")

                entry_count = len(entry_queue)
                exit_count  = len(exit_queue)
                active_count = len(active_ongoing)

                entry_eth = sum_effective_eth(entry_queue)
                exit_eth  = sum_effective_eth(exit_queue)
                active_eth = sum_effective_eth(active_ongoing)

                # ---- NEW: precise EL backlog after beacon's processed deposits ----
                el_backlog_events = 0
                el_backlog_eth = 0.0
                if EXECUTION_RPC_URL and deposit_contract:
                    beacon_processed = get_beacon_processed_deposit_count(state_id=slot)
                    if beacon_processed is not None:
                        try:
                            pending_events, pending_gwei = get_el_backlog_precise(
                                EXECUTION_RPC_URL,
                                deposit_contract,
                                processed_count=beacon_processed,
                                cache_path="deposit_index.cache.json",
                                # Optional: if you know the deposit contract creation block on your network, pass it here
                                from_block_hint=DEPOSIT_CONTRACT_CREATION_BLOCK
                            )
                            el_backlog_events = pending_events
                            el_backlog_eth = pending_gwei / 1e9
                        except Exception as e:
                            print(f"Warning: precise EL backlog failed at slot {slot}: {e}")
                estimated_additional_entries = int((el_backlog_eth * 1e9) // 32_000_000_000)  # floor(pending_gwei / 32e9)
                entry_total_estimated = entry_count + estimated_additional_entries

                initialized_epoch_block = True
            else:
                # Between epoch boundaries, we still recompute the exit queue to capture drift,
                # and keep entry queue as of last epoch-boundary snapshot (cheaper + good enough).
                exit_queue  = get_validators_by_status(state_id=slot, status="active_exiting,active_slashed")
                exit_count  = len(exit_queue)
                exit_eth    = sum_effective_eth(exit_queue)

                # Adjust active ETH/count by the delta in exit queue since epoch boundary if needed.
                # (If exit queue grows, active shrinks; if it shrinks, active grows.)
                # We can't know the exact previous values without storing them; simplest is to re-fetch active_ongoing
                # OR keep the epoch snapshot for active and just re-fetch active on each non-epoch slot.
                # We'll re-fetch active here to stay accurate.
                active_ongoing = get_validators_by_status(state_id=slot, status="active_ongoing")
                active_count = len(active_ongoing)
                active_eth   = sum_effective_eth(active_ongoing)
                # Entry queue left as-is until next epoch boundary snapshot.
                # If you want per-slot entry drift too, uncomment these two lines:
                # entry_queue = get_validators_by_status(state_id=slot, status="pending_queued")
                # entry_count, entry_eth = len(entry_queue), sum_effective_eth(entry_queue)



            writer.writerow([
                slot // 32,
                slot,
                active_count,
                f"{active_eth:.9f}",
                entry_count,
                f"{entry_eth:.9f}",
                exit_count,
                f"{exit_eth:.9f}",
                el_backlog_events,
                f"{el_backlog_eth:.9f}",
                entry_total_estimated
            ])

            print(
                f"Slot {slot} - Active: {active_count} / {active_eth:.3f} ETH, "
                f"Entry(beacon): {entry_count} / {entry_eth:.3f} ETH, "
                f"Exit: {exit_count} / {exit_eth:.3f} ETH, "
                f"EL backlog: {el_backlog_events} ev / {el_backlog_eth:.3f} ETH, "
                f"Entry total est: {entry_total_estimated}"
            )

            if sleep_between:
                time.sleep(sleep_between)

    print(f"\n✅ Results saved/resumed in {filename}")
    print(f"Covered slots: {resume_slot} -> {end_slot}")
    return filename

# ------------------ Run directly ------------------
if __name__ == "__main__":
    # Example range
    end_slot = 12566081
    start_slot = end_slot -20 #12464182
    query_epochs(start_slot, end_slot, interval=1)
