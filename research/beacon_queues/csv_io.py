import os, csv


CSV_HEADER = [
    "epoch","slot",
    "active_count","active_eth",
    "entry_count","entry_eth",
    "exit_count","exit_eth",
    "beacon_deposit_count",
    "eth1_block_hash",
    "eth1_block_number",
    "eth1_block_timestamp",
]

def csv_has_header(path: str) -> bool:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return False
    with open(path, "r", newline="") as f:
        try:
            first = next(csv.reader(f))
            return first == CSV_HEADER
        except StopIteration:
            return False

def last_written_slot(path: str) -> int | None:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return None
    last_row = None
    with open(path, "r", newline="") as f:
        for row in csv.reader(f):
            last_row = row
    if last_row and last_row[0] != "epoch":
        try:
            return int(last_row[1])
        except (ValueError, IndexError):
            return None
    return None

