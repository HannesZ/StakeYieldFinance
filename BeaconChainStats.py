import os
import csv
import time
import requests
from dotenv import load_dotenv

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
                print(f"\n********************************************** Epoch {slot//32} **********************************************")

                entry_queue = get_validators_by_status(state_id=slot, status="pending_queued")
                exit_queue  = get_validators_by_status(state_id=slot, status="active_exiting,active_slashed")
                active_ongoing = get_validators_by_status(state_id=slot, status="active_ongoing")

                entry_count = len(entry_queue)
                exit_count  = len(exit_queue)
                active_count = len(active_ongoing)

                entry_eth = sum_effective_eth(entry_queue)
                exit_eth  = sum_effective_eth(exit_queue)
                active_eth = sum_effective_eth(active_ongoing)

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
            ])

            print(
                f"Slot {slot} - Active: {active_count} / {active_eth:.3f} ETH, "
                f"Entry: {entry_count} / {entry_eth:.3f} ETH, "
                f"Exit: {exit_count} / {exit_eth:.3f} ETH"
            )

            if sleep_between:
                time.sleep(sleep_between)

    print(f"\n✅ Results saved/resumed in {filename}")
    print(f"Covered slots: {resume_slot} -> {end_slot}")
    return filename

# ------------------ Run directly ------------------
if __name__ == "__main__":
    # Example range
    end_slot = 12464282
    start_slot = 12364282
    query_epochs(start_slot, end_slot, interval=1)
