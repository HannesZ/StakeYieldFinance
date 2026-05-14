#!/usr/bin/env python3
"""Calibrate Merton jump-diffusion + exit parameters from history.csv.

Outputs the parameter set for yield_curve_cli.py.
Uses a configurable trailing window (default: last 90 days of data).
"""

import csv
import math
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

DATA = Path(__file__).resolve().parents[1] / "data" / "history.csv"
GENESIS_TIME = 1606824023  # beacon chain genesis


def load_daily(history_csv: Path, window_days: int = 90):
    """Load history.csv, aggregate to daily, return last `window_days` days."""
    # Read all rows, group by date
    daily_data = defaultdict(list)
    with history_csv.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            slot = int(row["slot"])
            ts = GENESIS_TIME + slot * 12
            date_str = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
            daily_data[date_str].append(row)

    # Take last row per day (end-of-day state), filtering artifacts
    dates = sorted(daily_data.keys())

    # First pass: build all records to compute median for artifact detection
    all_records = []
    for d in dates:
        last = daily_data[d][-1]
        all_records.append({
            "date": d,
            "active_count": int(last["active_count"]),
            "entry_count": int(last.get("entry_count") or 0),
            "exit_count": int(last.get("exit_count") or 0),
            "pending_deposits_count": int(last.get("pending_deposits_count") or 0),
            "active_eth": float(last.get("active_eth") or 0),
        })

    # Filter out days where active_count drops to near-zero (API/node artifacts)
    active_vals = [r["active_count"] for r in all_records if r["active_count"] > 0]
    if active_vals:
        median_active = sorted(active_vals)[len(active_vals) // 2]
        threshold = median_active * 0.5
        clean = [r for r in all_records if r["active_count"] > threshold]
        n_dropped = len(all_records) - len(clean)
        if n_dropped > 0:
            print(f"  ⚠️  Dropped {n_dropped} artifact days (active_count < {threshold:,.0f})")
        all_records = clean

    if window_days and len(all_records) > window_days:
        all_records = all_records[-window_days:]

    return all_records


def calibrate(window_days: int = 90):
    print(f"Loading history.csv (trailing {window_days}-day window)...")
    daily = load_daily(DATA, window_days)
    print(f"  Days loaded: {len(daily)} ({daily[0]['date']} → {daily[-1]['date']})")

    # --- Current state (last row) ---
    last = daily[-1]
    print(f"\n📊 Current state ({last['date']}):")
    print(f"  Active validators: {last['active_count']:,}")
    print(f"  Active ETH:        {last['active_eth']:>14,.0f}")
    print(f"  Entry queue ETH:   {last['entry_count']:,} validators  (ETH not in CSV — see exit_eth)")
    print(f"  Exit queue ETH:    {last['exit_count']:,} validators")
    print(f"  Pending deposits:  {last['pending_deposits_count']:,} validators")

    # --- ETH-denominated daily flows ---
    delta_active_eth = []
    delta_pending_eth = []
    delta_exit_eth = []
    for i in range(1, len(daily)):
        delta_active_eth.append(daily[i]["active_eth"] - daily[i-1]["active_eth"])
        delta_pending_eth.append(daily[i]["pending_deposits_count"] - daily[i-1]["pending_deposits_count"])  # proxy
        delta_exit_eth.append(daily[i]["exit_count"] - daily[i-1]["exit_count"])  # proxy

    delta_active_eth = np.array(delta_active_eth, dtype=float)

    print(f"\n📈 Daily net active ETH flow (n={len(delta_active_eth)}):")
    print(f"  Mean:   {np.mean(delta_active_eth):>+12,.0f} ETH/day")
    print(f"  Median: {np.median(delta_active_eth):>+12,.0f} ETH/day")
    print(f"  Std:    {np.std(delta_active_eth):>12,.0f} ETH/day")
    print(f"  Min:    {np.min(delta_active_eth):>+12,.0f} ETH/day")
    print(f"  Max:    {np.max(delta_active_eth):>+12,.0f} ETH/day")
    print(f"  Entry days: {(delta_active_eth>0).sum()} | Exit days: {(delta_active_eth<0).sum()}")

    # --- APR check ---
    import math
    BASE_REWARD_FACTOR = 64
    EPOCHS_PER_YEAR = 82125
    cur_apr = BASE_REWARD_FACTOR * EPOCHS_PER_YEAR / math.sqrt(last["active_eth"] * 1e9)
    print(f"\n  Protocol APR (CL only): {cur_apr:.3%}")
    print(f"  With +0.5% EL premium:  {cur_apr+0.005:.3%}")
    print(f"  validatorqueue.com:     ~2.82%")

    # --- Estimate new daily deposits flowing INTO the system ---
    # New deposits = delta_pending_eth + ETH activated from pending that day
    # Approximation: new deposits ≈ delta_active_eth (when positive) + exit-related outflows
    # Better: use delta_pending + churn activations
    # For each day: new_deposits ≈ max(0, net_inflow + exits_that_day)
    # We use: est_new_deposits = max(0, delta_active_eth) + max(0, -delta_active_eth * 0)  
    # Simplest robust estimate: treat ALL positive daily net ETH gain + outflows as new deposits
    # Active ETH delta = new_activations - new_exits
    # new_activations ≈ daily_churn_capacity (when queues are present)
    # Churn capacity per day at current scale:
    churn_per_epoch = max(128.0, last["active_eth"] / 65536.0)
    churn_per_day = churn_per_epoch * 225  # ETH per day
    print(f"\n  Churn capacity: {churn_per_epoch:.0f} ETH/epoch = {churn_per_day:,.0f} ETH/day")
    print(f"  (Entry queue drained in ~{last['pending_deposits_count']*32 / churn_per_day:.0f} days at this rate)")

    # Calibrate deposit inflow: on days when active_eth is growing, activations ≈ churn_per_day
    # New gross deposits hitting pending ≈ delta_active_eth + gross_exits
    # Approximate: gross exits per day from exit-day data
    exit_days_eth = -delta_active_eth[delta_active_eth < 0]  # ETH lost on exit days
    entry_days_eth = delta_active_eth[delta_active_eth > 0]   # ETH gained on entry days

    # Estimate gross daily deposits entering pending
    # On entry days: delta_active ≈ activations - exits ≈ churn - small_exits → deposit_proxy = delta + exits
    # Use churn-rate to estimate total flow
    gross_daily_inflow = delta_active_eth + churn_per_day  # rough: net = inflow - churn_exits, so inflow = net + exits
    # but exits aren't always at full churn, so use:
    # Better proxy: est_new_deposits = delta_active_eth + est_exit_flow
    # For exit flow: use std of negative days
    mean_exit_flow = np.mean(exit_days_eth) if len(exit_days_eth) > 0 else 0

    # Simpler conservative estimate: model NET ETH flow directly
    # net_daily_eth_flow ~ N(drift, vol) + jumps
    net_flow = delta_active_eth

    # Jump detection on net flow
    flow_mean = np.mean(net_flow)
    flow_std = np.std(net_flow)
    jump_threshold = flow_mean + 2 * flow_std
    jump_mask = net_flow > jump_threshold
    jumps = net_flow[jump_mask]
    non_jumps = net_flow[~jump_mask]

    n_jumps = len(jumps)
    jump_lambda = n_jumps / len(net_flow)
    jump_mu = np.mean(jumps) if n_jumps > 0 else 0
    jump_sigma = np.std(jumps) if n_jumps > 1 else abs(jump_mu) * 0.25

    drift = np.mean(non_jumps)
    vol = np.std(non_jumps)

    # Exit calibration: on days with net exit, estimate gross exit ETH
    exit_drift = np.mean(exit_days_eth) if len(exit_days_eth) > 0 else 0
    exit_vol = np.std(exit_days_eth) if len(exit_days_eth) > 1 else exit_drift * 0.5

    print(f"\n🔧 Calibrated Parameters (ETH-denominated):")
    print(f"  ─── Net ETH flow (diffusion) ───")
    print(f"  drift_per_day:     {drift:>10,.0f} ETH/day")
    print(f"  vol_per_sqrt_day:  {vol:>10,.0f} ETH/day")
    print(f"  ─── Jumps (large deposit events) ───")
    print(f"  jump_lambda:       {jump_lambda:.4f} ({n_jumps} jumps in {len(net_flow)} days)")
    print(f"  jump_mu:           {jump_mu:>10,.0f} ETH")
    print(f"  jump_sigma:        {jump_sigma:>10,.0f} ETH")
    print(f"  ─── Exit side ───")
    print(f"  exit_drift:        {exit_drift:>10,.0f} ETH/day")
    print(f"  exit_vol:          {exit_vol:>10,.0f} ETH/day")
    print(f"  ─── Net ───")
    print(f"  net_daily:         {flow_mean:>+10,.0f} ETH/day")
    print(f"  regime:            {'exit-heavy ↑ yield' if flow_mean < 0 else 'entry-heavy ↓ yield (APR declines over time)'}")

    print(f"\n💻 yield_curve_cli.py command:")
    print(f"  python3 scripts/yield_curve_cli.py curve --term-years 3 \\")
    print(f"    --drift-per-day {drift:.0f} --vol-per-sqrt-day {vol:.0f} \\")
    print(f"    --jump-lambda {jump_lambda:.4f} --jump-mu {jump_mu:.0f} --jump-sigma {jump_sigma:.0f} \\")
    print(f"    --exit-drift {exit_drift:.0f} --exit-vol {exit_vol:.0f}")

    return {
        "drift_per_day": round(drift, 0),
        "vol_per_sqrt_day": round(vol, 0),
        "jump_lambda": round(jump_lambda, 4),
        "jump_mu": round(jump_mu, 0),
        "jump_sigma": round(jump_sigma, 0),
        "exit_drift": round(exit_drift, 0),
        "exit_vol": round(exit_vol, 0),
        "net_daily_eth": round(float(flow_mean), 0),
        "window_days": window_days,
        "data_range": f"{daily[0]['date']} → {daily[-1]['date']}",
        "current_active_eth": last["active_eth"],
    }


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--window", type=int, default=90, help="trailing window in days")
    args = ap.parse_args()
    calibrate(args.window)
