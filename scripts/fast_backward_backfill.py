#!/usr/bin/env python3
"""Fast backward backfill: 1 sample per day (every 225 epochs = 7200 slots).

Queries only the fields we need for the yield curve model:
- active_count, active_eth (from active_ongoing validators)
- entry_count (pending_queued)
- exit_count (active_exiting + active_slashed)
- pending_deposits (from beacon state)

Designed for long-range historical extension where per-slot resolution isn't needed.
"""

import csv
import json
import os
import sys
import time
import urllib.request
from pathlib import Path
from datetime import datetime

BEACON = os.environ.get("BEACON_NODE_URL_1", "http://192.168.1.123:5051")
ROOT = Path(__file__).resolve().parents[1]
OUTFILE = ROOT / "data" / "history_daily_backfill.csv"
LOGFILE = ROOT / "backfill_backward.log"
SLOTS_PER_DAY = 7200  # 225 epochs × 32 slots

HEADERS = ["epoch", "slot", "active_count", "active_eth",
           "entry_count", "entry_eth", "exit_count", "exit_eth",
           "pending_deposits_count", "pending_deposits_eth"]


def log(msg):
    line = f"[{datetime.now().isoformat(timespec='seconds')}] {msg}"
    print(line, flush=True)
    with open(LOGFILE, "a") as f:
        f.write(line + "\n")


def beacon_get(path, timeout=300):
    url = f"{BEACON}{path}"
    req = urllib.request.Request(url)
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read())
        except Exception as e:
            if attempt < 2:
                log(f"  retry {attempt+1}/3 for {path}: {e}")
                time.sleep(5)
            else:
                raise


def count_and_eth(slot, status):
    """Query validators by status, return (count, total_effective_eth)."""
    data = beacon_get(f"/eth/v1/beacon/states/{slot}/validators?status={status}")
    vals = data.get("data", [])
    count = len(vals)
    eth = sum(int(v["validator"]["effective_balance"]) for v in vals) / 1e9
    return count, eth


def get_pending_deposits(slot):
    """Get pending deposits from beacon state (Electra+)."""
    try:
        data = beacon_get(f"/eth/v2/beacon/states/{slot}/pending_deposits")
        deposits = data.get("data", [])
        count = len(deposits)
        eth = sum(int(d.get("amount", 0)) for d in deposits) / 1e9
        return count, eth
    except Exception:
        # Pre-Electra or endpoint not available
        return 0, 0.0


def query_slot(slot):
    """Query all needed data for a single slot. Returns dict or None on error."""
    epoch = slot // 32
    try:
        # Active validators (SLOW - ~10s)
        ac, ae = count_and_eth(slot, "active_ongoing")
        # Entry queue (fast)
        ec, ee = count_and_eth(slot, "pending_queued")
        # Exit queue: active_exiting + active_slashed
        xc1, xe1 = count_and_eth(slot, "active_exiting")
        xc2, xe2 = count_and_eth(slot, "active_slashed")
        xc, xe = xc1 + xc2, xe1 + xe2
        # Pending deposits
        pc, pe = get_pending_deposits(slot)

        return {
            "epoch": epoch, "slot": slot,
            "active_count": ac, "active_eth": f"{ae:.6f}",
            "entry_count": ec, "entry_eth": f"{ee:.6f}",
            "exit_count": xc, "exit_eth": f"{xe:.6f}",
            "pending_deposits_count": pc, "pending_deposits_eth": f"{pe:.6f}",
        }
    except Exception as e:
        log(f"ERROR at slot {slot}: {e}")
        return None


def get_last_slot():
    """Get the minimum slot already in the output file."""
    if not OUTFILE.exists():
        return None
    min_slot = None
    with open(OUTFILE, newline="") as f:
        for row in csv.DictReader(f):
            s = int(row["slot"])
            if min_slot is None or s < min_slot:
                min_slot = s
    return min_slot


def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--start-slot", type=int, required=True, help="Earliest slot to reach (go backward to here)")
    p.add_argument("--from-slot", type=int, default=None, help="Start going backward from this slot (default: min in history.csv)")
    args = p.parse_args()

    # Determine starting point
    if args.from_slot:
        current_slot = args.from_slot
    else:
        # Read from main history.csv
        history = ROOT / "data" / "history.csv"
        min_slot = None
        with open(history, newline="") as f:
            for row in csv.DictReader(f):
                s = int(row["slot"])
                if min_slot is None or s < min_slot:
                    min_slot = s
        current_slot = min_slot
        log(f"Starting from history.csv min slot: {current_slot}")

    # Check if we have partial progress
    last = get_last_slot()
    if last is not None and last < current_slot:
        current_slot = last
        log(f"Resuming from previous backfill at slot {current_slot}")

    # Write header if needed
    write_header = not OUTFILE.exists() or OUTFILE.stat().st_size == 0
    
    target = args.start_slot
    total_days = (current_slot - target) // SLOTS_PER_DAY
    log(f"Backward backfill: {current_slot} -> {target} ({total_days} daily samples)")

    done = 0
    with open(OUTFILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        if write_header:
            writer.writeheader()

        slot = current_slot - SLOTS_PER_DAY  # first backward step
        while slot >= target:
            t0 = time.time()
            row = query_slot(slot)
            elapsed = time.time() - t0
            if row:
                writer.writerow(row)
                f.flush()
                done += 1
                if done % 10 == 0:
                    log(f"  [{done}/{total_days}] slot={slot} epoch={slot//32} active={row['active_count']} pending={row['pending_deposits_count']} ({elapsed:.1f}s)")
            slot -= SLOTS_PER_DAY

    log(f"Backward backfill complete: {done} daily samples written to {OUTFILE}")


if __name__ == "__main__":
    main()
