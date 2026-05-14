import csv
from web3 import Web3

def enrich_csv_with_el_backlog(
    in_csv: str,
    out_csv: str,
    rpc_url: str,
    deposit_contract: str,
    creation_block: int,
    chunk_size: int = 10_000,
):
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
        raise RuntimeError("No eth1_block_number found in input CSV.")

    max_block = max(blocks_needed)
    w3 = Web3(Web3.HTTPProvider(rpc_url))

    # 1) Build prefix sums once (up to max needed eth1 block)
    prefix = build_deposit_prefix_gwei(
        w3=w3,
        deposit_contract=deposit_contract,
        from_block=creation_block,
        to_block=max_block,
        chunk_size=chunk_size,
    )

    # 2) Cache EL deposit_count per unique eth1 block number (fast eth_call)
    el_count_cache = {}
    for b in sorted(blocks_needed):
        el_count_cache[b] = get_el_deposit_count_at_block(w3, deposit_contract, b)

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

            # prefix[i] exists for all scanned deposits. Ensure it covers el_count.
            if el_count >= len(prefix):
                raise RuntimeError(
                    f"Prefix only has {len(prefix)-1} deposits, but EL count at block {eth1_block} is {el_count}. "
                    f"Increase max_block (or ensure your scan reached that block)."
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



""" 
import json, pathlib
from web3 import Web3

DEPOSIT_EVENT_ABI = json.loads("""[{"anonymous":false,"inputs":[
 {"indexed":false,"internalType":"bytes","name":"pubkey","type":"bytes"},
 {"indexed":false,"internalType":"bytes","name":"withdrawal_credentials","type":"bytes"},
 {"indexed":false,"internalType":"bytes","name":"amount","type":"bytes"},
 {"indexed":false,"internalType":"bytes","name":"signature","type":"bytes"},
 {"indexed":false,"internalType":"bytes","name":"index","type":"bytes"}],
 "name":"DepositEvent","type":"event"}]
""")

def _decode_amount_bytes_to_gwei(b: bytes) -> int:
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
        return {"checkpoints": []}
    try:
        return json.loads(p.read_text())
    except Exception:
        return {"checkpoints": []}

def _save_cache(path: str, data: dict):
    pathlib.Path(path).write_text(json.dumps(data))

def _best_checkpoint(cache: dict, target_index: int) -> dict | None:
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
    addr = Web3.to_checksum_address(addr)
    curr = int(start_block); end = int(end_block); step_curr = int(step)
    while curr <= end:
        to_blk = min(curr + step_curr - 1, end)
        try:
            logs = w3.eth.get_logs({"fromBlock": curr, "toBlock": to_blk, "address": addr})
            yield logs
            curr = to_blk + 1
            if step_curr < 50_000:
                step_curr = min(50_000, step_curr + 2_000)
        except Exception:
            if step_curr > 2_000:
                step_curr = max(2_000, step_curr // 2)
            else:
                curr = to_blk + 1

def _find_boundary_and_pending(exec_url: str,
                               contract_address: str,
                               processed_count: int,
                               cache_path: str,
                               from_block_hint: int | None = None,
                               initial_step: int = 20_000):
    if processed_count is None:
        return 0, 0, 0, 0

    w3 = _w3(exec_url)
    latest_block = w3.eth.block_number
    cache = _load_cache(cache_path)

    cp = _best_checkpoint(cache, processed_count)
    if cp:
        start_block = max(0, int(cp["block"]))
        event_index_so_far = int(cp["event_index"])
        boundary_found = (event_index_so_far == processed_count)
        boundary_block = int(cp["block"]) if boundary_found else None
        boundary_log_index = int(cp["log_index"]) if boundary_found else None
    else:
        start_block = from_block_hint if from_block_hint is not None else 0
        event_index_so_far = 0
        boundary_found = (processed_count == 0)
        boundary_block = -1 if boundary_found else None
        boundary_log_index = -1 if boundary_found else None

    pending_events = pending_gwei = total_events = total_gwei = 0
    reached_boundary = boundary_found
    deposit_event = w3.eth.contract(abi=DEPOSIT_EVENT_ABI).events.DepositEvent

    for logs in _scan_logs(w3, contract_address, start_block, latest_block, initial_step):
        if not reached_boundary:
            for i, log in enumerate(logs):
                event_index_so_far += 1
                total_events += 1
                try:
                    decoded = deposit_event().process_log(log)
                    total_gwei += _decode_amount_bytes_to_gwei(decoded["args"]["amount"])
                except Exception:
                    pass

                if event_index_so_far == processed_count:
                    reached_boundary = True
                    boundary_block = log["blockNumber"]
                    boundary_log_index = log["logIndex"]
                    _push_checkpoint(cache, event_index_so_far, boundary_block, boundary_log_index)
                    for tlog in logs[i+1:]:
                        pending_events += 1
                        total_events += 1
                        try:
                            decoded2 = deposit_event().process_log(tlog)
                            amt = _decode_amount_bytes_to_gwei(decoded2["args"]["amount"])
                            pending_gwei += amt
                            total_gwei += amt
                        except Exception:
                            pass
                    break
            else:
                continue
        else:
            total_events += len(logs)
            for log in logs:
                pending_events += 1
                try:
                    decoded = deposit_event().process_log(log)
                    amt = _decode_amount_bytes_to_gwei(decoded["args"]["amount"])
                    pending_gwei += amt
                    total_gwei += amt
                except Exception:
                    pass

    if not reached_boundary and processed_count > event_index_so_far:
        print("Warning: EL node returned fewer deposit events than beacon's processed_count.")
        pending_events = pending_gwei = 0

    _save_cache(cache_path, cache)
    return pending_events, pending_gwei, total_events, total_gwei

def get_el_backlog_precise(exec_url: str,
                           contract_address: str,
                           processed_count: int,
                           cache_path: str = "deposit_index.cache.json",
                           from_block_hint: int | None = None):
    pe, pg, _te, _tg = _find_boundary_and_pending(
        exec_url, contract_address, int(processed_count) if processed_count is not None else 0,
        cache_path=cache_path, from_block_hint=from_block_hint, initial_step=20_000
    )
    return pe, pg
 """