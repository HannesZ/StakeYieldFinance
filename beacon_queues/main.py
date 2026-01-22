#!/usr/bin/env python3
import csv
import time

from config import (
    EXECUTION_RPC_URL,
    BEACON_NODE_URL,
)

from beacon import (
    get_validators_by_status,
    sum_effective_eth,
    get_beacon_eth1_snapshot,
    get_deposit_contract_address_from_beacon,
)

from beacon_helper import resolve_el_block_hash
from http_helper import get_with_retries
from csv_io import CSV_HEADER, csv_has_header, last_written_slot


# -----------------------------
# Pending deposits via Beacon API
# -----------------------------

def get_pending_deposits(state_id):
    """
    Retrieve pending deposits from the beacon chain for a given state.
    Uses /eth/v1/beacon/states/{state_id}/pending_deposits endpoint.

    Returns:
        tuple: (pending_count, pending_eth) or (None, None) on failure
    """
    url = f"{BEACON_NODE_URL}/eth/v1/beacon/states/{state_id}/pending_deposits"
    resp = get_with_retries(url)

    if resp is None or resp.status_code != 200:
        print(f"Warning: Could not fetch pending deposits for state {state_id}")
        return None, None

    try:
        data = resp.json().get("data", [])
        pending_count = len(data)

        # Sum up the amounts (amounts are in Gwei)
        total_gwei = 0
        for deposit in data:
            amount = deposit.get("amount")
            if amount:
                total_gwei += int(amount)

        pending_eth = total_gwei / 1e9
        return pending_count, pending_eth
    except Exception as e:
        print(f"Warning: Failed to parse pending deposits for state {state_id}: {e}")
        return None, None


def enrich_csv_with_pending_deposits(in_csv: str, out_csv: str):
    """
    Reads the base CSV and writes an enriched CSV with pending deposit backlog.
    Uses the slot number from each row to query the beacon API for pending deposits.
    """
    rows = []

    with open(in_csv, newline="") as f:
        r = csv.DictReader(f)
        header_in = r.fieldnames or []
        for row in r:
            rows.append(row)

    if not rows:
        raise RuntimeError("No rows found in input CSV.")

    header_out = header_in + [
        "pending_deposits_count",
        "pending_deposits_eth",
    ]

    with open(out_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header_out)
        w.writeheader()

        for row in rows:
            slot = row.get("slot")

            if not slot:
                row["pending_deposits_count"] = ""
                row["pending_deposits_eth"] = ""
                w.writerow(row)
                continue

            pending_count, pending_eth = get_pending_deposits(state_id=slot)

            if pending_count is not None:
                row["pending_deposits_count"] = str(pending_count)
                row["pending_deposits_eth"] = f"{pending_eth:.9f}"
            else:
                row["pending_deposits_count"] = ""
                row["pending_deposits_eth"] = ""

            print(f"Slot {slot}: {pending_count} pending deposits ({pending_eth:.3f} ETH)" if pending_count else f"Slot {slot}: no data")
            w.writerow(row)


# -----------------------------
# Exit queue binary search optimization
# -----------------------------

def get_exit_queue_data(slot):
    """Query exit queue for a single slot, returns (count, eth)."""
    exit_queue = get_validators_by_status(state_id=slot, status="active_exiting,active_slashed")
    return len(exit_queue), sum_effective_eth(exit_queue)


def find_exit_queue_changes_recursive(slots, cache):
    """
    Recursively find change points using binary search.
    Fills cache with queried slots only (change points).
    """
    if len(slots) <= 1:
        if slots and slots[0] not in cache:
            cache[slots[0]] = get_exit_queue_data(slots[0])
        return

    first_slot = slots[0]
    last_slot = slots[-1]

    # Get data for first and last slots (use cache if available)
    if first_slot not in cache:
        cache[first_slot] = get_exit_queue_data(first_slot)
    if last_slot not in cache:
        cache[last_slot] = get_exit_queue_data(last_slot)

    # If same count, no changes in this range (don't recurse)
    if cache[first_slot][0] == cache[last_slot][0]:
        return

    # If adjacent slots with different values, we've found the boundary
    if len(slots) == 2:
        return

    # Different and not adjacent - query middle and recurse both halves
    mid_idx = len(slots) // 2
    mid_slot = slots[mid_idx]

    if mid_slot not in cache:
        cache[mid_slot] = get_exit_queue_data(mid_slot)

    # Recurse on left half [first, mid] and right half [mid, last]
    find_exit_queue_changes_recursive(slots[:mid_idx + 1], cache)
    find_exit_queue_changes_recursive(slots[mid_idx:], cache)


def find_exit_queue_changes(slots):
    """
    Use binary search to find exit queue changes within a list of slots.
    Returns a dict mapping slot -> (exit_count, exit_eth).
    """
    if not slots:
        return {}

    cache = {}

    # Find all change points via binary search
    find_exit_queue_changes_recursive(slots, cache)

    # Fill in all slots by propagating values forward from queried slots
    result = {}
    queried_slots = sorted(cache.keys())
    current_data = cache[queried_slots[0]]

    q_idx = 0
    for slot in slots:
        # Move to next queried slot if we've passed it
        while q_idx < len(queried_slots) - 1 and queried_slots[q_idx + 1] <= slot:
            q_idx += 1
            current_data = cache[queried_slots[q_idx]]
        result[slot] = current_data

    return result


# -----------------------------
# Main query
# -----------------------------

deposit_contract = get_deposit_contract_address_from_beacon()

def query_epochs(start_slot, end_slot, interval=1, filename=None, sleep_between=0.05):
    if filename is None:
        filename = f"beacon_chain_queues_{start_slot}_{end_slot}.csv"

    needs_header = not csv_has_header(filename)
    last_slot = last_written_slot(filename)
    resume_slot = start_slot if last_slot is None else max(start_slot, last_slot + interval)
    if resume_slot > end_slot:
        print(f"Nothing to do. CSV already covers up to slot {last_slot}.")
        print(f"Results are in {filename}")
        return filename

    with open(filename, "a", newline="") as file:
        writer = csv.writer(file)
        if needs_header:
            writer.writerow(CSV_HEADER)

        # Process slots epoch by epoch for efficiency
        slot = resume_slot

        while slot <= end_slot:
            epoch = slot // 32
            epoch_start = epoch * 32
            epoch_end = epoch_start + 31

            # Determine which slots in this epoch we need to process
            first_slot_in_epoch = max(slot, epoch_start)
            last_slot_in_epoch = min(end_slot, epoch_end)
            epoch_slots = list(range(first_slot_in_epoch, last_slot_in_epoch + 1, interval))

            if not epoch_slots:
                slot = epoch_end + 1
                continue

            print(f"\n**************************************************************************** Epoch {epoch} ****************************************************************************")

            # Query entry queue and active validators at epoch boundary (first slot we process)
            boundary_slot = epoch_slots[0]
            entry_queue = get_validators_by_status(state_id=boundary_slot, status="pending_queued")
            active_ongoing = get_validators_by_status(state_id=boundary_slot, status="active_ongoing")

            entry_count = len(entry_queue)
            active_count = len(active_ongoing)
            entry_eth = sum_effective_eth(entry_queue)
            active_eth = sum_effective_eth(active_ongoing)

            # Get eth1 snapshot at epoch boundary
            cur_beacon_deposit_count = ""
            cur_eth1_block_hash = ""
            cur_eth1_block_number = ""
            cur_eth1_block_timestamp = ""

            snap = get_beacon_eth1_snapshot(state_id=boundary_slot)
            if snap:
                cur_beacon_deposit_count, cur_eth1_block_hash = snap
                if (EXECUTION_RPC_URL and cur_eth1_block_hash and
                    cur_eth1_block_hash != "0x0000000000000000000000000000000000000000000000000000000000000000"):
                    try:
                        cur_eth1_block_number, cur_eth1_block_timestamp = resolve_el_block_hash(
                            EXECUTION_RPC_URL, cur_eth1_block_hash
                        )
                    except Exception as e:
                        print(f"Warning: EL resolve failed at slot {boundary_slot}: {e}")

            # Use binary search to efficiently find exit queue changes within epoch
            print(f"  Finding exit queue changes for {len(epoch_slots)} slots...")
            exit_queue_cache = find_exit_queue_changes(epoch_slots)
            queries_made = len(set(exit_queue_cache.values()))  # Unique queries
            print(f"  Exit queue: {queries_made} unique states found")

            # Write rows for all slots in this epoch
            for s in epoch_slots:
                exit_count, exit_eth = exit_queue_cache[s]

                writer.writerow([
                    s // 32, s,
                    active_count, f"{active_eth:.9f}",
                    entry_count,  f"{entry_eth:.9f}",
                    exit_count,   f"{exit_eth:.9f}",
                    cur_beacon_deposit_count,
                    cur_eth1_block_hash,
                    cur_eth1_block_number,
                    cur_eth1_block_timestamp,
                ])

                print(
                    f"Slot {s} - Active: {active_count} / {active_eth:.3f} ETH, "
                    f"Entry(beacon): {entry_count} / {entry_eth:.3f} ETH, "
                    f"Exit: {exit_count} / {exit_eth:.3f} ETH"
                )

            if sleep_between:
                time.sleep(sleep_between)

            # Move to next epoch
            slot = epoch_end + 1

    print(f"\n✅ Results saved/resumed in {filename}")
    print(f"Covered slots: {resume_slot} -> {end_slot}")
    return filename


if __name__ == "__main__":
    end_slot = 13523871
    start_slot = end_slot - 10000

    base_csv = query_epochs(start_slot, end_slot, interval=1)

    # Step 2: enrich with pending deposits from beacon API
    enriched_csv = base_csv.replace(".csv", "_with_pending_deposits.csv")
    enrich_csv_with_pending_deposits(
        in_csv=base_csv,
        out_csv=enriched_csv,
    )
    print(f"✅ Enriched CSV written to {enriched_csv}")
