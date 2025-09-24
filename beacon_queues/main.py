import csv, time
from .config import EXECUTION_RPC_URL, DEPOSIT_CONTRACT_CREATION_BLOCK
from .beacon import (
    get_validators_by_status, sum_effective_eth,
    get_beacon_processed_deposit_count, get_deposit_contract_address_from_beacon
)
from .el_backlog import get_el_backlog_precise
from .csv_io import CSV_HEADER, csv_has_header, last_written_slot

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
        active_count = active_eth = 0
        entry_count = entry_eth = 0.0
        exit_count  = exit_eth  = 0.0

        for slot in range(resume_slot, end_slot + 1, interval):
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
                                from_block_hint=DEPOSIT_CONTRACT_CREATION_BLOCK or None
                            )
                            el_backlog_events = pending_events
                            el_backlog_eth = pending_gwei / 1e9
                        except Exception as e:
                            print(f"Warning: precise EL backlog failed at slot {slot}: {e}")

                estimated_additional_entries = int((el_backlog_eth * 1e9) // 32_000_000_000)
                entry_total_estimated = entry_count + estimated_additional_entries
                initialized_epoch_block = True
            else:
                exit_queue  = get_validators_by_status(state_id=slot, status="active_exiting,active_slashed")
                exit_count  = len(exit_queue)
                exit_eth    = sum_effective_eth(exit_queue)

                active_ongoing = get_validators_by_status(state_id=slot, status="active_ongoing")
                active_count = len(active_ongoing)
                active_eth   = sum_effective_eth(active_ongoing)

            writer.writerow([
                slot // 32, slot, active_count, f"{active_eth:.9f}",
                entry_count, f"{entry_eth:.9f}",
                exit_count,  f"{exit_eth:.9f}",
                el_backlog_events, f"{el_backlog_eth:.9f}",
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

if __name__ == "__main__":
    end_slot = 12_659_090
    start_slot = end_slot - 20
    query_epochs(start_slot, end_slot, interval=1)
