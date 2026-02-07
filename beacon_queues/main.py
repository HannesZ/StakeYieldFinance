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

# Set to True for detailed algorithm logging (for debugging/verification)
PENDING_DEPOSITS_DEBUG_LOGGING = True

# Window size as power of 2 (default 5 = 2^5 = 32 slots, matching epoch size)
PENDING_DEPOSITS_WINDOW_POWER = 5


def get_pending_deposits_data(slot):
    """
    Query pending deposits for a single slot.
    Returns (pending_count, pending_eth) or (None, None) on failure.
    """
    url = f"{BEACON_NODE_URL}/eth/v1/beacon/states/{slot}/pending_deposits"
    resp = get_with_retries(url, timeout=120)

    if resp is None or resp.status_code != 200:
        print(f"Warning: Could not fetch pending deposits for state {slot}")
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
        print(f"Warning: Failed to parse pending deposits for state {slot}: {e}")
        return None, None


def find_pending_deposits_changes_recursive(slots, cache, depth=0):
    """
    Recursively find change points using binary search.
    Fills cache with queried slots only (change points).
    """
    indent = "    " * (depth + 1)  # Indentation for logging

    if PENDING_DEPOSITS_DEBUG_LOGGING:
        print(f"{indent}[depth={depth}] Processing range: slots {slots[0]} to {slots[-1]} ({len(slots)} slots)" if slots else f"{indent}[depth={depth}] Empty slots list")

    if len(slots) <= 1:
        if slots and slots[0] not in cache:
            data = get_pending_deposits_data(slots[0])
            cache[slots[0]] = data
            if PENDING_DEPOSITS_DEBUG_LOGGING:
                if data[0] is not None:
                    print(f"{indent}  -> QUERY slot {slots[0]}: count={data[0]}, eth={data[1]:.3f} (single slot)")
                else:
                    print(f"{indent}  -> QUERY slot {slots[0]}: FAILED (single slot)")
        elif slots and PENDING_DEPOSITS_DEBUG_LOGGING:
            print(f"{indent}  -> CACHED slot {slots[0]}: count={cache[slots[0]][0]} (already queried)")
        return

    first_slot = slots[0]
    last_slot = slots[-1]

    # Get data for first and last slots (use cache if available)
    if first_slot not in cache:
        data = get_pending_deposits_data(first_slot)
        cache[first_slot] = data
        if PENDING_DEPOSITS_DEBUG_LOGGING:
            if data[0] is not None:
                print(f"{indent}  -> QUERY first slot {first_slot}: count={data[0]}, eth={data[1]:.3f}")
            else:
                print(f"{indent}  -> QUERY first slot {first_slot}: FAILED")
    elif PENDING_DEPOSITS_DEBUG_LOGGING:
        print(f"{indent}  -> CACHED first slot {first_slot}: count={cache[first_slot][0]}")

    if last_slot not in cache:
        data = get_pending_deposits_data(last_slot)
        cache[last_slot] = data
        if PENDING_DEPOSITS_DEBUG_LOGGING:
            if data[0] is not None:
                print(f"{indent}  -> QUERY last slot {last_slot}: count={data[0]}, eth={data[1]:.3f}")
            else:
                print(f"{indent}  -> QUERY last slot {last_slot}: FAILED")
    elif PENDING_DEPOSITS_DEBUG_LOGGING:
        print(f"{indent}  -> CACHED last slot {last_slot}: count={cache[last_slot][0]}")

    first_data = cache[first_slot]
    last_data = cache[last_slot]

    # Handle failed queries - mark all slots in range as failed
    if first_data[0] is None or last_data[0] is None:
        if PENDING_DEPOSITS_DEBUG_LOGGING:
            print(f"{indent}  => Query failed, marking all {len(slots)} slots in range as failed")
        for s in slots:
            if s not in cache:
                cache[s] = (None, None)
        return

    first_count = first_data[0]
    last_count = last_data[0]

    # If same count, no changes in this range (don't recurse)
    if first_count == last_count:
        if PENDING_DEPOSITS_DEBUG_LOGGING:
            print(f"{indent}  => SAME count ({first_count}) at both ends - no changes in range, skipping recursion")
        return

    # If adjacent slots with different values, we've found the boundary
    if len(slots) == 2:
        if PENDING_DEPOSITS_DEBUG_LOGGING:
            print(f"{indent}  => BOUNDARY FOUND between slot {first_slot} (count={first_count}) and {last_slot} (count={last_count})")
        return

    # Different and not adjacent - query middle and recurse both halves
    mid_idx = len(slots) // 2
    mid_slot = slots[mid_idx]

    if PENDING_DEPOSITS_DEBUG_LOGGING:
        print(f"{indent}  => DIFFERENT counts (first={first_count}, last={last_count}) - splitting at middle slot {mid_slot}")

    if mid_slot not in cache:
        data = get_pending_deposits_data(mid_slot)
        cache[mid_slot] = data
        if PENDING_DEPOSITS_DEBUG_LOGGING:
            if data[0] is not None:
                print(f"{indent}  -> QUERY middle slot {mid_slot}: count={data[0]}, eth={data[1]:.3f}")
            else:
                print(f"{indent}  -> QUERY middle slot {mid_slot}: FAILED")
    elif PENDING_DEPOSITS_DEBUG_LOGGING:
        print(f"{indent}  -> CACHED middle slot {mid_slot}: count={cache[mid_slot][0]}")

    # Recurse on left half [first, mid] and right half [mid, last]
    if PENDING_DEPOSITS_DEBUG_LOGGING:
        print(f"{indent}  => Recursing LEFT: slots {first_slot} to {mid_slot}")
    find_pending_deposits_changes_recursive(slots[:mid_idx + 1], cache, depth + 1)

    if PENDING_DEPOSITS_DEBUG_LOGGING:
        print(f"{indent}  => Recursing RIGHT: slots {mid_slot} to {last_slot}")
    find_pending_deposits_changes_recursive(slots[mid_idx:], cache, depth + 1)


def find_pending_deposits_changes(slots):
    """
    Use binary search to find pending deposit changes within a list of slots.
    Returns a dict mapping slot -> (pending_count, pending_eth).
    """
    if not slots:
        return {}

    if PENDING_DEPOSITS_DEBUG_LOGGING:
        print(f"\n  === Pending Deposits Binary Search Start ===")
        print(f"  Input: {len(slots)} slots from {slots[0]} to {slots[-1]}")

    cache = {}

    # Find all change points via binary search
    find_pending_deposits_changes_recursive(slots, cache, depth=0)

    if PENDING_DEPOSITS_DEBUG_LOGGING:
        print(f"\n  === Filling in gaps ===")
        print(f"  Queried {len(cache)} slots: {sorted(cache.keys())}")

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
            if PENDING_DEPOSITS_DEBUG_LOGGING:
                print(f"    Slot {slot}: switching to queried value from slot {queried_slots[q_idx]} (count={current_data[0]})")
        result[slot] = current_data

    if PENDING_DEPOSITS_DEBUG_LOGGING:
        # Show the final mapping summary
        unique_values = {}
        for s, data in result.items():
            count = data[0] if data[0] is not None else "FAILED"
            if count not in unique_values:
                unique_values[count] = []
            unique_values[count].append(s)

        print(f"\n  === Result Summary ===")
        for count, slot_list in sorted(unique_values.items(), key=lambda x: (x[0] is None, x[0] or 0)):
            if len(slot_list) <= 6:
                print(f"    count={count}: slots {slot_list}")
            else:
                print(f"    count={count}: slots {slot_list[0]}-{slot_list[-1]} ({len(slot_list)} slots)")
        print(f"  === Pending Deposits Binary Search End ===\n")

    return result


def enrich_csv_with_pending_deposits(in_csv: str, out_csv: str, window_power: int = PENDING_DEPOSITS_WINDOW_POWER):
    """
    Reads the base CSV and writes an enriched CSV with pending deposit backlog.
    Uses binary search within windows of size 2^window_power slots.

    Args:
        in_csv: Input CSV file path
        out_csv: Output CSV file path
        window_power: Power of 2 for window size (default 5 = 32 slots)
    """
    window_size = 2 ** window_power

    rows = []
    with open(in_csv, newline="") as f:
        r = csv.DictReader(f)
        header_in = r.fieldnames or []
        for row in r:
            rows.append(row)

    if not rows:
        raise RuntimeError("No rows found in input CSV.")

    # Extract all slots
    all_slots = []
    for row in rows:
        slot = row.get("slot")
        if slot:
            all_slots.append(int(slot))

    if not all_slots:
        raise RuntimeError("No slots found in input CSV.")

    print(f"\n{'='*80}")
    print(f"Enriching CSV with pending deposits")
    print(f"Window size: 2^{window_power} = {window_size} slots")
    print(f"Total slots to process: {len(all_slots)}")
    print(f"{'='*80}")

    # Process slots in windows and build complete cache
    pending_cache = {}
    min_slot = min(all_slots)
    max_slot = max(all_slots)

    window_start = min_slot
    window_num = 0
    while window_start <= max_slot:
        window_end = window_start + window_size - 1
        window_slots = [s for s in all_slots if window_start <= s <= window_end]

        if window_slots:
            window_num += 1
            print(f"\n{'*'*80}")
            print(f"Window {window_num}: slots {window_start} to {min(window_end, max_slot)} ({len(window_slots)} slots in CSV)")
            print(f"{'*'*80}")

            window_result = find_pending_deposits_changes(window_slots)
            pending_cache.update(window_result)

        window_start = window_end + 1

    # Write output CSV
    header_out = header_in + [
        "pending_deposits_count",
        "pending_deposits_eth",
    ]

    with open(out_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header_out)
        w.writeheader()

        for row in rows:
            slot = row.get("slot")

            if not slot or int(slot) not in pending_cache:
                row["pending_deposits_count"] = ""
                row["pending_deposits_eth"] = ""
                w.writerow(row)
                continue

            pending_count, pending_eth = pending_cache[int(slot)]

            if pending_count is not None:
                row["pending_deposits_count"] = str(pending_count)
                row["pending_deposits_eth"] = f"{pending_eth:.9f}"
            else:
                row["pending_deposits_count"] = ""
                row["pending_deposits_eth"] = ""

            w.writerow(row)

    print(f"\n{'='*80}")
    print(f"Enrichment complete. Total API queries: {len(set(pending_cache.keys()))}")
    print(f"{'='*80}")


# -----------------------------
# Exit queue binary search optimization
# -----------------------------

# Set to True for detailed algorithm logging (for debugging/verification)
EXIT_QUEUE_DEBUG_LOGGING = True

def get_exit_queue_data(slot):
    """Query exit queue for a single slot, returns (count, eth)."""
    exit_queue = get_validators_by_status(state_id=slot, status="active_exiting,active_slashed")
    return len(exit_queue), sum_effective_eth(exit_queue)


def find_exit_queue_changes_recursive(slots, cache, depth=0):
    """
    Recursively find change points using binary search.
    Fills cache with queried slots only (change points).
    """
    indent = "    " * (depth + 1)  # Indentation for logging

    if EXIT_QUEUE_DEBUG_LOGGING:
        print(f"{indent}[depth={depth}] Processing range: slots {slots[0]} to {slots[-1]} ({len(slots)} slots)" if slots else f"{indent}[depth={depth}] Empty slots list")

    if len(slots) <= 1:
        if slots and slots[0] not in cache:
            data = get_exit_queue_data(slots[0])
            cache[slots[0]] = data
            if EXIT_QUEUE_DEBUG_LOGGING:
                print(f"{indent}  -> QUERY slot {slots[0]}: count={data[0]}, eth={data[1]:.3f} (single slot)")
        elif slots and EXIT_QUEUE_DEBUG_LOGGING:
            print(f"{indent}  -> CACHED slot {slots[0]}: count={cache[slots[0]][0]} (already queried)")
        return

    first_slot = slots[0]
    last_slot = slots[-1]

    # Get data for first and last slots (use cache if available)
    if first_slot not in cache:
        data = get_exit_queue_data(first_slot)
        cache[first_slot] = data
        if EXIT_QUEUE_DEBUG_LOGGING:
            print(f"{indent}  -> QUERY first slot {first_slot}: count={data[0]}, eth={data[1]:.3f}")
    elif EXIT_QUEUE_DEBUG_LOGGING:
        print(f"{indent}  -> CACHED first slot {first_slot}: count={cache[first_slot][0]}")

    if last_slot not in cache:
        data = get_exit_queue_data(last_slot)
        cache[last_slot] = data
        if EXIT_QUEUE_DEBUG_LOGGING:
            print(f"{indent}  -> QUERY last slot {last_slot}: count={data[0]}, eth={data[1]:.3f}")
    elif EXIT_QUEUE_DEBUG_LOGGING:
        print(f"{indent}  -> CACHED last slot {last_slot}: count={cache[last_slot][0]}")

    first_count = cache[first_slot][0]
    last_count = cache[last_slot][0]

    # If same count, no changes in this range (don't recurse)
    if first_count == last_count:
        if EXIT_QUEUE_DEBUG_LOGGING:
            print(f"{indent}  => SAME count ({first_count}) at both ends - no changes in range, skipping recursion")
        return

    # If adjacent slots with different values, we've found the boundary
    if len(slots) == 2:
        if EXIT_QUEUE_DEBUG_LOGGING:
            print(f"{indent}  => BOUNDARY FOUND between slot {first_slot} (count={first_count}) and {last_slot} (count={last_count})")
        return

    # Different and not adjacent - query middle and recurse both halves
    mid_idx = len(slots) // 2
    mid_slot = slots[mid_idx]

    if EXIT_QUEUE_DEBUG_LOGGING:
        print(f"{indent}  => DIFFERENT counts (first={first_count}, last={last_count}) - splitting at middle slot {mid_slot}")

    if mid_slot not in cache:
        data = get_exit_queue_data(mid_slot)
        cache[mid_slot] = data
        if EXIT_QUEUE_DEBUG_LOGGING:
            print(f"{indent}  -> QUERY middle slot {mid_slot}: count={data[0]}, eth={data[1]:.3f}")
    elif EXIT_QUEUE_DEBUG_LOGGING:
        print(f"{indent}  -> CACHED middle slot {mid_slot}: count={cache[mid_slot][0]}")

    # Recurse on left half [first, mid] and right half [mid, last]
    if EXIT_QUEUE_DEBUG_LOGGING:
        print(f"{indent}  => Recursing LEFT: slots {first_slot} to {mid_slot}")
    find_exit_queue_changes_recursive(slots[:mid_idx + 1], cache, depth + 1)

    if EXIT_QUEUE_DEBUG_LOGGING:
        print(f"{indent}  => Recursing RIGHT: slots {mid_slot} to {last_slot}")
    find_exit_queue_changes_recursive(slots[mid_idx:], cache, depth + 1)


def find_exit_queue_changes(slots):
    """
    Use binary search to find exit queue changes within a list of slots.
    Returns a dict mapping slot -> (exit_count, exit_eth).
    """
    if not slots:
        return {}

    if EXIT_QUEUE_DEBUG_LOGGING:
        print(f"\n  === Binary Search Algorithm Start ===")
        print(f"  Input: {len(slots)} slots from {slots[0]} to {slots[-1]}")

    cache = {}

    # Find all change points via binary search
    find_exit_queue_changes_recursive(slots, cache, depth=0)

    if EXIT_QUEUE_DEBUG_LOGGING:
        print(f"\n  === Filling in gaps ===")
        print(f"  Queried {len(cache)} slots: {sorted(cache.keys())}")

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
            if EXIT_QUEUE_DEBUG_LOGGING:
                print(f"    Slot {slot}: switching to queried value from slot {queried_slots[q_idx]} (count={current_data[0]})")
        result[slot] = current_data

    if EXIT_QUEUE_DEBUG_LOGGING:
        # Show the final mapping summary
        unique_values = {}
        for s, data in result.items():
            if data[0] not in unique_values:
                unique_values[data[0]] = []
            unique_values[data[0]].append(s)

        print(f"\n  === Result Summary ===")
        for count, slot_list in sorted(unique_values.items()):
            if len(slot_list) <= 6:
                print(f"    count={count}: slots {slot_list}")
            else:
                print(f"    count={count}: slots {slot_list[0]}-{slot_list[-1]} ({len(slot_list)} slots)")
        print(f"  === Binary Search Algorithm End ===\n")

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
    end_slot = 	13638943
    start_slot = end_slot - 5000

    base_csv = query_epochs(start_slot, end_slot, interval=1)

    # Step 2: enrich with pending deposits from beacon API
    enriched_csv = base_csv.replace(".csv", "_with_pending_deposits.csv")
    enrich_csv_with_pending_deposits(
        in_csv=base_csv,
        out_csv=enriched_csv,
    )
    print(f"✅ Enriched CSV written to {enriched_csv}")
