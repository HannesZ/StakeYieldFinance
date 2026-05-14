#!/usr/bin/env python3
import csv
from pathlib import Path
from datetime import datetime

from beacon_queues.main import query_epochs, enrich_csv_with_pending_deposits
from beacon_queues.history import historise

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "history.csv"
LOG = ROOT / "backfill.log"

CHUNK_EPOCHS = 200
CHUNK_SLOTS = CHUNK_EPOCHS * 32
TARGET_SLOT = 13823105
PAST_CHUNKS = 1


def min_max_slot(history_csv: Path):
    mn, mx = None, None
    with history_csv.open(newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            s = row.get("slot")
            if not s:
                continue
            s = int(s)
            mn = s if mn is None else min(mn, s)
            mx = s if mx is None else max(mx, s)
    if mn is None or mx is None:
        raise RuntimeError("No slots found in history.csv")
    return mn, mx


def run_chunk(start_slot: int, end_slot: int):
    base_csv = f"beacon_chain_queues_{start_slot}_{end_slot}.csv"
    enriched_csv = base_csv.replace(".csv", "_with_pending_deposits.csv")
    query_epochs(start_slot, end_slot, interval=1, filename=base_csv, sleep_between=0.0)
    enrich_csv_with_pending_deposits(base_csv, enriched_csv)
    historise(base_csv, enriched_csv, start_slot, end_slot)


def log(msg: str):
    line = f"[{datetime.now().isoformat(timespec='seconds')}] {msg}"
    print(line, flush=True)
    with LOG.open("a") as f:
        f.write(line + "\n")


def main():
    log("=== backfill_and_extend start ===")
    mn, mx = min_max_slot(DATA)
    log(f"history range before: {mn} -> {mx}")

    # 1) Catch up forward to target slot
    while mx < TARGET_SLOT:
        start = mx + 1
        end = min(start + CHUNK_SLOTS - 1, TARGET_SLOT)
        log(f"forward chunk: {start} -> {end}")
        run_chunk(start, end)
        mn, mx = min_max_slot(DATA)
        log(f"history range now: {mn} -> {mx}")

    # 2) Extend further into the past (older slots)
    for i in range(PAST_CHUNKS):
        end = mn - 1
        start = max(0, end - CHUNK_SLOTS + 1)
        if end < 0:
            break
        log(f"past chunk {i+1}/{PAST_CHUNKS}: {start} -> {end}")
        run_chunk(start, end)
        mn, mx = min_max_slot(DATA)
        log(f"history range now: {mn} -> {mx}")

    log("=== backfill_and_extend done ===")


if __name__ == "__main__":
    main()
