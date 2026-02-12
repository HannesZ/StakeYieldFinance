#!/usr/bin/env python3
import csv
import time

from config import EXECUTION_RPC_URL

from beacon import (
    get_validators_by_status,
    sum_effective_eth,
    get_beacon_eth1_snapshot,
    get_deposit_contract_address_from_beacon,
    get_exit_queue_data,
    get_pending_deposits_data,
    ZERO_HASH,
)

from beacon_helper import resolve_el_block_hash
from binary_search import find_changes
from csv_io import CSV_HEADER, csv_has_header, last_written_slot


# -----------------------------
# Debug logging (set to False to disable)
# -----------------------------
EXIT_QUEUE_DEBUG_LOGGING = True
PENDING_DEPOSITS_DEBUG_LOGGING = True

# Window size as power of 2 (default 5 = 2^5 = 32 slots, matching epoch size)
PENDING_DEPOSITS_WINDOW_POWER = 5


# -----------------------------
# Pending deposits CSV enrichment
# -----------------------------

def enrich_csv_with_pending_deposits(in_csv: str, out_csv: str, window_power: int = PENDING_DEPOSITS_WINDOW_POWER):
    """
    Reads the base CSV and writes an enriched CSV with pending deposit backlog.
    Uses binary search within windows of size 2^window_power slots.
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

            window_result = find_changes(
                window_slots,
                fetch_fn=get_pending_deposits_data,
                label="Pending Deposits",
                debug=PENDING_DEPOSITS_DEBUG_LOGGING,
            )
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
                    cur_eth1_block_hash != ZERO_HASH):
                    try:
                        cur_eth1_block_number, cur_eth1_block_timestamp = resolve_el_block_hash(
                            EXECUTION_RPC_URL, cur_eth1_block_hash
                        )
                    except Exception as e:
                        print(f"Warning: EL resolve failed at slot {boundary_slot}: {e}")

            # Use binary search to efficiently find exit queue changes within epoch
            print(f"  Finding exit queue changes for {len(epoch_slots)} slots...")
            exit_queue_cache = find_changes(
                epoch_slots,
                fetch_fn=get_exit_queue_data,
                label="Exit Queue",
                debug=EXIT_QUEUE_DEBUG_LOGGING,
            )
            queries_made = len(set(exit_queue_cache.values()))
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
    end_slot = 13673437
    start_slot = 13656524  # ~20 epochs back

    base_csv = query_epochs(start_slot, end_slot, interval=1)

    # Step 2: enrich with pending deposits from beacon API
    enriched_csv = base_csv.replace(".csv", "_with_pending_deposits.csv")
    enrich_csv_with_pending_deposits(
        in_csv=base_csv,
        out_csv=enriched_csv,
    )
    print(f"✅ Enriched CSV written to {enriched_csv}")
