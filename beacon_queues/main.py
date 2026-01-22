#!/usr/bin/env python3
import csv
import time
from array import array

from web3 import Web3

from config import (
    EXECUTION_RPC_URL,
    DEPOSIT_CONTRACT_CREATION_BLOCK,
)

from beacon import (
    get_validators_by_status,
    sum_effective_eth,
    get_beacon_eth1_snapshot,
    get_deposit_contract_address_from_beacon,
)

from beacon_helper import resolve_el_block_hash
from csv_io import CSV_HEADER, csv_has_header, last_written_slot


# -----------------------------
# EL helpers for enrichment
# -----------------------------

DEPOSIT_COUNT_ABI = [
    {
        "name": "get_deposit_count",
        "type": "function",
        "stateMutability": "view",
        "inputs": [],
        "outputs": [{"name": "", "type": "bytes8"}],
    }
]

DEPOSIT_EVENT_ABI = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": False, "name": "pubkey", "type": "bytes"},
            {"indexed": False, "name": "withdrawal_credentials", "type": "bytes"},
            {"indexed": False, "name": "amount", "type": "bytes"},
            {"indexed": False, "name": "signature", "type": "bytes"},
            {"indexed": False, "name": "index", "type": "bytes"},
        ],
        "name": "DepositEvent",
        "type": "event",
    }
]

DEPOSIT_EVENT_SIG = "DepositEvent(bytes,bytes,bytes,bytes,bytes)"
DEPOSIT_EVENT_TOPIC0 = Web3.keccak(text=DEPOSIT_EVENT_SIG).hex()  # "0x..."

# Some environments return without 0x; normalize:
if not DEPOSIT_EVENT_TOPIC0.startswith("0x"):
    DEPOSIT_EVENT_TOPIC0 = "0x" + DEPOSIT_EVENT_TOPIC0

assert len(DEPOSIT_EVENT_TOPIC0) == 66, (DEPOSIT_EVENT_TOPIC0, len(DEPOSIT_EVENT_TOPIC0))

def _le_bytes_to_int(b: bytes) -> int:
    return int.from_bytes(b, "little", signed=False)

def get_el_deposit_count_at_block(w3: Web3, deposit_contract: str, block_number: int) -> int:
    c = w3.eth.contract(address=Web3.to_checksum_address(deposit_contract), abi=DEPOSIT_COUNT_ABI)
    raw = c.functions.get_deposit_count().call(block_identifier=block_number)  # bytes8
    return _le_bytes_to_int(bytes(raw))

def build_deposit_prefix_gwei(
    w3: Web3,
    deposit_contract: str,
    from_block: int,
    to_block: int,
    chunk_size: int = 10_000,
):
    """
    prefix_gwei[i] = sum(amount_gwei) for deposits with index < i
    Also returns block_to_count: dict mapping block_number -> deposit count at that block
    """
    contract = w3.eth.contract(address=deposit_contract, abi=DEPOSIT_EVENT_ABI)
    ev = contract.events.DepositEvent()


    prefix = array("Q", [0])
    expected_index = 0
    block_to_count = {}  # Track deposit count at each block

    for start in range(from_block, to_block + 1, chunk_size):
        end = min(start + chunk_size - 1, to_block)
        logs = w3.eth.get_logs({
            "fromBlock": hex(start),
            "toBlock": hex(end),
            "address": deposit_contract,
            "topics": [DEPOSIT_EVENT_TOPIC0],
        })

        logs.sort(key=lambda lg: (
            lg["blockNumber"],
            lg.get("transactionIndex", 0),
            lg.get("logIndex", 0)
        ))

        for lg in logs:
            decoded = ev().process_log(lg)
            amt_gwei = _le_bytes_to_int(bytes(decoded["args"]["amount"]))
            idx = _le_bytes_to_int(bytes(decoded["args"]["index"]))

            if idx != expected_index:
                raise RuntimeError(
                    f"Non-contiguous deposit index: got {idx}, expected {expected_index}. "
                    f"Check creation_block={from_block} and that the RPC is consistent."
                )

            prefix.append(prefix[-1] + amt_gwei)
            expected_index += 1

            # Track the count at this block (count is index + 1 after processing)
            block_num = lg["blockNumber"]
            block_to_count[block_num] = expected_index

    return prefix, block_to_count

def enrich_csv_with_el_backlog(
    in_csv: str,
    out_csv: str,
    rpc_url: str,
    deposit_contract: str,
    creation_block: int,
    chunk_size: int = 10_000,
):
    """
    Reads the Step-1 CSV and writes an enriched CSV with EL deposit totals + backlog.
    Backlog is computed at each row's eth1_block_number.
    """
    rows = []
    blocks_needed = set()

    with open(in_csv, newline="") as f:
        r = csv.DictReader(f)
        header_in = r.fieldnames or []
        for row in r:
            rows.append(row)
            b = row.get("eth1_block_number")
            if b and b != "None":
                blocks_needed.add(int(b))

    if not blocks_needed:
        raise RuntimeError("No eth1_block_number values found in input CSV.")

    max_block = max(blocks_needed)
    w3 = Web3(Web3.HTTPProvider(rpc_url))

    prefix, block_to_count = build_deposit_prefix_gwei(
        w3=w3,
        deposit_contract=deposit_contract,
        from_block=creation_block,
        to_block=max_block,
        chunk_size=chunk_size,
    )

    # Build el_count_cache from block_to_count (derived from events)
    # For blocks without deposits, find the count from the previous block with deposits
    el_count_cache = {}
    sorted_blocks_with_deposits = sorted(block_to_count.keys())

    for b in blocks_needed:
        if b in block_to_count:
            # Block has deposits
            el_count_cache[b] = block_to_count[b]
        else:
            # Block has no deposits, find the last block before it that had deposits
            count = 0
            for deposit_block in sorted_blocks_with_deposits:
                if deposit_block <= b:
                    count = block_to_count[deposit_block]
                else:
                    break
            el_count_cache[b] = count

    header_out = header_in + [
        "el_deposit_count_at_eth1_block",
        "el_total_deposited_eth_at_eth1_block",
        "el_backlog_events",
        "el_backlog_eth",
    ]

    with open(out_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header_out)
        w.writeheader()

        for row in rows:
            b = row.get("eth1_block_number")
            bc = row.get("beacon_deposit_count")

            if not b or b == "None" or not bc or bc == "None":
                row["el_deposit_count_at_eth1_block"] = ""
                row["el_total_deposited_eth_at_eth1_block"] = ""
                row["el_backlog_events"] = ""
                row["el_backlog_eth"] = ""
                w.writerow(row)
                continue

            eth1_block = int(b)
            beacon_count = int(bc)
            el_count = el_count_cache[eth1_block]

            if el_count >= len(prefix):
                raise RuntimeError(
                    f"Prefix has {len(prefix)-1} deposits, but EL count at block {eth1_block} is {el_count}. "
                    f"Increase scan range or check creation_block."
                )

            el_total_gwei = prefix[el_count]
            backlog_events = max(0, el_count - beacon_count)

            backlog_gwei = 0
            if 0 <= beacon_count <= el_count:
                backlog_gwei = prefix[el_count] - prefix[beacon_count]

            row["el_deposit_count_at_eth1_block"] = str(el_count)
            row["el_total_deposited_eth_at_eth1_block"] = f"{el_total_gwei/1e9:.9f}"
            row["el_backlog_events"] = str(backlog_events)
            row["el_backlog_eth"] = f"{backlog_gwei/1e9:.9f}"

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

        # Counts/eth carried across epoch (your existing behavior)
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

                active_ongoing = get_validators_by_status(state_id=slot, status="active_ongoing")
                active_count = len(active_ongoing)
                active_eth   = sum_effective_eth(active_ongoing)

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
    end_slot = 	13521086
    start_slot = end_slot - 40

    base_csv = query_epochs(start_slot, end_slot, interval=1)

    # Step 2: enrich with EL backlog
    enriched_csv = base_csv.replace(".csv", "_with_el_backlog.csv")
    if EXECUTION_RPC_URL and deposit_contract:
        enrich_csv_with_el_backlog(
            in_csv=base_csv,
            out_csv=enriched_csv,
            rpc_url=EXECUTION_RPC_URL,
            deposit_contract=deposit_contract,
            creation_block=DEPOSIT_CONTRACT_CREATION_BLOCK,
            chunk_size=10_000,
        )
        print(f"✅ Enriched CSV written to {enriched_csv}")
    else:
        print("Skipping EL enrichment: missing EXECUTION_RPC_URL or deposit_contract")
