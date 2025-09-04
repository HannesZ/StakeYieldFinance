import os
import csv
import time
import requests
from dotenv import load_dotenv

load_dotenv()

BEACON_NODE_URL = os.getenv("BEACON_NODE_URL")
if not BEACON_NODE_URL:
    raise RuntimeError("BEACON_NODE_URL is not set. Put it in your .env or environment.")

# ---------- HTTP helper with retries ----------
def _get_with_retries(url, params=None, retries=3, backoff=1.5, timeout=20):
    attempt = 0
    while True:
        try:
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code == 200:
                return r
            # Non-200: brief backoff and retry
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

# ---------- Beacon helpers ----------
def get_validators_by_status(state_id="head", status="pending_queued"):
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

# ---------- CSV helpers ----------
def _csv_has_header(path):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return False
    with open(path, "r", newline="") as f:
        try:
            first = next(csv.reader(f))
            return first == ["epoch", "slot", "active", "entry_queue", "exit_queue"]
        except StopIteration:
            return False

def _last_written_slot(path):
    """Return the last slot written (int), or None if no data rows yet."""
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return None
    last_row = None
    with open(path, "r", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            last_row = row
    if last_row and last_row and last_row[0] != "epoch":
        try:
            return int(last_row[1])  # slot column
        except (ValueError, IndexError):
            return None
    return None

# ---------- Main ----------
def query_epochs(start_slot, end_slot, interval=1, filename=None, sleep_between=0.05):
    """
    Resume-safe query: if filename exists and contains rows,
    resume from last_slot + interval; otherwise start at start_slot.
    """
    if filename is None:
        filename = f"beacon_chain_queues_{start_slot}_{end_slot}.csv"

    # Ensure header exists; detect resume point
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
            writer.writerow(["epoch", "slot", "active", "entry_queue", "exit_queue"])

        # Carry-over counters between non-epoch slots
        nr_exit_queue = 0
        nr_entry_queue = 0
        nr_active_ongoing = 0
        initialized_exit_queue = False

        for slot in range(resume_slot, end_slot + 1, interval):
            # Refresh fully at epoch boundaries (or first iteration)
            if slot % 32 == 0 or not initialized_exit_queue:
                print(f"\n**************************** Epoch {slot//32} ****************************")
                entry_queue = get_validators_by_status(state_id=slot, status="pending_queued")
                nr_entry_queue = len(entry_queue)

                active_ongoing = get_validators_by_status(state_id=slot, status="active_ongoing")
                nr_active_ongoing = len(active_ongoing)

                exit_queue = get_validators_by_status(state_id=slot, status="active_exiting,active_slashed")
                nr_exit_queue = len(exit_queue)

                initialized_exit_queue = True
            else:
                # Between epoch boundaries: track exit changes; entry queue held constant until next epoch refresh
                exit_queue = get_validators_by_status(state_id=slot, status="active_exiting,active_slashed")
                new_exit = len(exit_queue)
                if new_exit != nr_exit_queue:
                    # If exit queue shrinks, active set grows; if it grows, active shrinks
                    nr_active_ongoing += (nr_exit_queue - new_exit)
                    nr_exit_queue = new_exit

            writer.writerow([slot // 32, slot, nr_active_ongoing, nr_entry_queue, nr_exit_queue])
            print(f"Slot {slot} - Active: {nr_active_ongoing}, Entry: {nr_entry_queue}, Exit: {nr_exit_queue}")

            # Gentle rate limit
            if sleep_between:
                time.sleep(sleep_between)

    print(f"\n✅ Results saved/resumed in {filename}")
    print(f"Covered slots: {resume_slot} -> {end_slot}")
    return filename

if __name__ == "__main__":
    # Example range
    end_slot = 12464282
    start_slot = 12364282
    query_epochs(start_slot, end_slot, interval=1)
