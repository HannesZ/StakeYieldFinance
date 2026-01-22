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

        initialized_epoch_block = False

        # Carried snapshot values (epoch-granularity)
        cur_beacon_deposit_count = ""
        cur_eth1_block_hash = ""
        cur_eth1_block_number = ""
        cur_eth1_block_timestamp = ""

        # Counts/eth carried across epoch
        active_count = active_eth = 0
        entry_count = entry_eth = 0.0
        exit_count  = exit_eth  = 0.0

        for slot in range(resume_slot, end_slot + 1, interval):
            epoch_boundary = (slot % 32 == 0) or (not initialized_epoch_block)

            if epoch_boundary:
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

                # --- Snapshot only at epoch boundary ---
                cur_beacon_deposit_count = ""
                cur_eth1_block_hash = ""
                cur_eth1_block_number = ""
                cur_eth1_block_timestamp = ""

                snap = get_beacon_eth1_snapshot(state_id=slot)
                if snap:
                    cur_beacon_deposit_count, cur_eth1_block_hash = snap
                    # Check if block hash is not zero hash before trying to resolve it
                    if (EXECUTION_RPC_URL and cur_eth1_block_hash and
                        cur_eth1_block_hash != "0x0000000000000000000000000000000000000000000000000000000000000000"):
                        try:
                            cur_eth1_block_number, cur_eth1_block_timestamp = resolve_el_block_hash(
                                EXECUTION_RPC_URL, cur_eth1_block_hash
                            )
                        except Exception as e:
                            print(f"Warning: EL resolve failed at slot {slot}: {e}")

                initialized_epoch_block = True
            else:
                # per-slot updates
                exit_queue  = get_validators_by_status(state_id=slot, status="active_exiting,active_slashed")
                exit_count  = len(exit_queue)
                exit_eth    = sum_effective_eth(exit_queue)
            # --- one row per slot (snapshot columns repeated within epoch) ---
            writer.writerow([
                slot // 32, slot,
                active_count, f"{active_eth:.9f}",
                entry_count,  f"{entry_eth:.9f}",
                exit_count,   f"{exit_eth:.9f}",
                cur_beacon_deposit_count,
                cur_eth1_block_hash,
                cur_eth1_block_number,
                cur_eth1_block_timestamp,
            ])

            print(
                f"Slot {slot} - Active: {active_count} / {active_eth:.3f} ETH, "
                f"Entry(beacon): {entry_count} / {entry_eth:.3f} ETH, "
                f"Exit: {exit_count} / {exit_eth:.3f} ETH, "
                f"Beacon dep count: {cur_beacon_deposit_count}, "
                f"Eth1 block: {cur_eth1_block_number}"
            )

            if sleep_between:
                time.sleep(sleep_between)

    print(f"\n✅ Results saved/resumed in {filename}")
    print(f"Covered slots: {resume_slot} -> {end_slot}")
    return filename


if __name__ == "__main__":
    end_slot = 13523644
    start_slot = end_slot - 10000

    base_csv = query_epochs(start_slot, end_slot, interval=1)

    # Step 2: enrich with pending deposits from beacon API
    enriched_csv = base_csv.replace(".csv", "_with_pending_deposits.csv")
    enrich_csv_with_pending_deposits(
        in_csv=base_csv,
        out_csv=enriched_csv,
    )
    print(f"✅ Enriched CSV written to {enriched_csv}")
