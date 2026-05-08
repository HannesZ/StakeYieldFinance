#!/usr/bin/env python3
"""Chunked backfill runner for StakeYieldFinance.

Phase 1: catch up forward from history max_slot to target_slot.
Phase 2: extend deeper into the past by N chunks.

Designed to be resumable: each chunk is historised immediately.
"""

import argparse
import csv
from pathlib import Path
from datetime import datetime

from beacon_queues.main import query_epochs, enrich_csv_with_pending_deposits
from beacon_queues.history import historise

ROOT = Path(__file__).resolve().parents[1]
HISTORY = ROOT / "data" / "history.csv"
LOG = ROOT / "backfill.log"


def min_max_slot(path: Path):
    mn, mx = None, None
    with path.open(newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            s = row.get("slot")
            if not s:
                continue
            s = int(s)
            mn = s if mn is None else min(mn, s)
            mx = s if mx is None else max(mx, s)
    if mn is None or mx is None:
        raise RuntimeError("history.csv has no slot values")
    return mn, mx


def log(msg: str):
    line = f"[{datetime.now().isoformat(timespec='seconds')}] {msg}"
    print(line, flush=True)
    with LOG.open("a") as f:
        f.write(line + "\n")


def run_chunk(start_slot: int, end_slot: int, sleep_between: float, interval: int = 1):
    base_csv = f"beacon_chain_queues_{start_slot}_{end_slot}.csv"
    enriched_csv = base_csv.replace(".csv", "_with_pending_deposits.csv")

    query_epochs(
        start_slot=start_slot,
        end_slot=end_slot,
        interval=interval,
        filename=base_csv,
        sleep_between=sleep_between,
    )
    enrich_csv_with_pending_deposits(base_csv, enriched_csv)
    historise(base_csv, enriched_csv, start_slot, end_slot)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--target-slot", type=int, required=True, help="Forward catch-up target slot")
    p.add_argument("--chunk-epochs", type=int, default=100, help="Epochs per chunk (default 100)")
    p.add_argument("--past-chunks", type=int, default=0, help="How many extra backward chunks to pull")
    p.add_argument("--sleep", type=float, default=0.0, help="sleep_between passed to query_epochs")
    p.add_argument("--interval", type=int, default=1, help="slot interval (32 = one per epoch)")
    args = p.parse_args()

    chunk_slots = args.chunk_epochs * 32

    log("=== backfill_pipeline start ===")
    mn, mx = min_max_slot(HISTORY)
    log(f"history before: {mn} -> {mx}")

    # Phase 1: forward catch-up
    while mx < args.target_slot:
        s = mx + 1
        e = min(s + chunk_slots - 1, args.target_slot)
        log(f"forward chunk: {s} -> {e}")
        run_chunk(s, e, args.sleep, args.interval)
        mn, mx = min_max_slot(HISTORY)
        log(f"history now: {mn} -> {mx}")

    # Phase 2: older history extension
    for i in range(args.past_chunks):
        e = mn - 1
        if e < 0:
            break
        s = max(0, e - chunk_slots + 1)
        log(f"past chunk {i+1}/{args.past_chunks}: {s} -> {e}")
        run_chunk(s, e, args.sleep, args.interval)
        mn, mx = min_max_slot(HISTORY)
        log(f"history now: {mn} -> {mx}")

    log("=== backfill_pipeline done ===")


if __name__ == "__main__":
    main()
