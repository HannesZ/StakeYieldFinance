#!/usr/bin/env python3
"""Pre-compute daily APR paths for the swap pricer GUI.

Outputs data/swap_surface.json with:
  - model_start: ISO date string
  - current_apr: current spot staking APR (%)
  - days: number of projection days
  - mean_apr: daily mean APR across MC paths (%)
  - p5/p25/p50/p75/p95: percentile APR paths (%)
  - cumulative_mean: cumulative average APR from day 0 to day d (for swap pricing)
"""

import json
import math
import random
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
HISTORY_CSV = DATA_DIR / "history.csv"
EPOCH_SUMMARY = DATA_DIR / "epoch_summary.csv"

# Beacon constants
BASE_REWARD_FACTOR = 64
EPOCHS_PER_YEAR = 82125
EPOCHS_PER_DAY = 225
EL_PREMIUM = 0.0013  # execution-layer premium

# Calibrated model parameters
DEPOSIT_DRIFT = 50_000.0
DEPOSIT_VOL = 40_000.0
JUMP_LAMBDA = 0.03
JUMP_MU = 200_000.0
JUMP_SIGMA = 50_000.0
EXIT_DRIFT = 20_000.0
EXIT_VOL = 30_000.0

TERM_DAYS = 3 * 365 + 1  # ~3 years
N_SIMS = 1000


def apr_from_active_eth(active_eth):
    cl = BASE_REWARD_FACTOR * EPOCHS_PER_YEAR / math.sqrt(max(active_eth, 1.0) * 1e9)
    return (cl + EL_PREMIUM) * 100  # percent


def eth_churn_per_epoch(active_eth):
    return max(128.0, active_eth / 65536.0)


def load_last_state():
    header = subprocess.check_output(["head", "-1", str(HISTORY_CSV)], text=True).strip().split(",")
    last = subprocess.check_output(["tail", "-1", str(HISTORY_CSV)], text=True).strip().split(",")
    state = dict(zip(header, last))
    return {
        "active_eth": float(state["active_eth"]),
        "entry_eth": float(state.get("entry_eth", 0)),
        "exit_eth": float(state.get("exit_eth", 0)),
        "pending_eth": float(state.get("pending_deposits_eth", 0)),
    }


def get_model_start_date():
    header = subprocess.check_output(["head", "-1", str(EPOCH_SUMMARY)], text=True).strip().split(",")
    last = subprocess.check_output(["tail", "-1", str(EPOCH_SUMMARY)], text=True).strip().split(",")
    state = dict(zip(header, last))
    ts = state.get("timestamp_utc", "2026-03-18 00:00")
    return datetime.strptime(ts[:10], "%Y-%m-%d")


def main():
    st = load_last_state()
    model_start = get_model_start_date()
    current_apr = apr_from_active_eth(st["active_eth"])

    print(f"Active ETH: {st['active_eth']:,.0f}")
    print(f"Current APR: {current_apr:.3f}%")
    print(f"Model start: {model_start.date()}")
    print(f"Running {N_SIMS} MC simulations over {TERM_DAYS} days...")

    # Deterministic queue drain
    active_eth = st["active_eth"]
    entry_eth = st["entry_eth"]
    exit_eth = st["exit_eth"]
    pending_eth = st["pending_eth"]

    # Find queue clear day
    ac, eq, xq, pq = active_eth, entry_eth, exit_eth, pending_eth
    queue_clear_day = TERM_DAYS
    for d in range(TERM_DAYS + 1):
        if eq <= 0 and xq <= 0 and pq <= 0:
            queue_clear_day = d
            break
        churn = eth_churn_per_epoch(ac)
        for _ in range(EPOCHS_PER_DAY):
            xq = max(0, xq - churn)
            can = min(churn, eq + pq)
            te = min(can, eq)
            eq -= te
            pq -= (can - te)
            ac += can

    # Deterministic path
    det = [active_eth]
    ac, eq, xq, pq = active_eth, entry_eth, exit_eth, pending_eth
    for _ in range(TERM_DAYS):
        churn = eth_churn_per_epoch(ac)
        for _ in range(EPOCHS_PER_DAY):
            drained = min(churn, xq)
            xq -= drained
            ac -= drained
            can = min(churn, eq + pq)
            fe = min(can, eq)
            eq -= fe
            pq -= (can - fe)
            ac += can
        det.append(ac)

    # Monte Carlo
    rng = random.Random(42)
    all_aprs = np.zeros((N_SIMS, TERM_DAYS + 1))

    for sim in range(N_SIMS):
        if sim % 100 == 0:
            print(f"  sim {sim}/{N_SIMS}...")
        eth_sim = det[min(queue_clear_day, TERM_DAYS)]
        pq_sim = eq_sim = xq_sim = 0.0

        for d in range(TERM_DAYS + 1):
            if d <= queue_clear_day:
                all_aprs[sim, d] = apr_from_active_eth(det[d])
            else:
                # Deposits
                z_dep = rng.gauss(0, 1)
                new_dep = max(0, DEPOSIT_DRIFT + DEPOSIT_VOL * z_dep)
                n_jumps = 0
                u = rng.random()
                p_acc = math.exp(-JUMP_LAMBDA)
                if u > p_acc:
                    n_jumps = 1
                    p_acc += JUMP_LAMBDA * math.exp(-JUMP_LAMBDA)
                    if u > p_acc:
                        n_jumps = 2
                for _ in range(n_jumps):
                    new_dep += max(0, rng.gauss(JUMP_MU, JUMP_SIGMA))
                pq_sim += new_dep

                # Exits
                z_exit = rng.gauss(0, 1)
                new_exits = max(0, EXIT_DRIFT + EXIT_VOL * z_exit)
                xq_sim += new_exits

                # Queue mechanics
                churn = eth_churn_per_epoch(eth_sim)
                for _ in range(EPOCHS_PER_DAY):
                    drained = min(churn, xq_sim)
                    xq_sim -= drained
                    eth_sim -= drained
                    can = min(churn, eq_sim + pq_sim)
                    fe = min(can, eq_sim)
                    eq_sim -= fe
                    pq_sim -= (can - fe)
                    eth_sim += can
                eth_sim = max(1.0, eth_sim)
                all_aprs[sim, d] = apr_from_active_eth(eth_sim)

    # Compute stats
    mean_apr = np.mean(all_aprs, axis=0).tolist()
    p5 = np.percentile(all_aprs, 5, axis=0).tolist()
    p25 = np.percentile(all_aprs, 25, axis=0).tolist()
    p50 = np.percentile(all_aprs, 50, axis=0).tolist()
    p75 = np.percentile(all_aprs, 75, axis=0).tolist()
    p95 = np.percentile(all_aprs, 95, axis=0).tolist()

    # Cumulative average APR for swap pricing:
    # swap_rate(start_day, end_day) = mean of mean_apr[start_day..end_day]
    # We store prefix sums so JS can compute any window quickly
    prefix_mean = np.cumsum(mean_apr).tolist()
    prefix_p5 = np.cumsum(p5).tolist()
    prefix_p25 = np.cumsum(p25).tolist()
    prefix_p50 = np.cumsum(p50).tolist()
    prefix_p75 = np.cumsum(p75).tolist()
    prefix_p95 = np.cumsum(p95).tolist()

    result = {
        "model_start": model_start.strftime("%Y-%m-%d"),
        "current_apr": round(current_apr, 4),
        "active_eth": st["active_eth"],
        "days": TERM_DAYS,
        "n_sims": N_SIMS,
        "mean_apr": [round(x, 4) for x in mean_apr],
        "p5": [round(x, 4) for x in p5],
        "p25": [round(x, 4) for x in p25],
        "p50": [round(x, 4) for x in p50],
        "p75": [round(x, 4) for x in p75],
        "p95": [round(x, 4) for x in p95],
        "prefix_mean": [round(x, 4) for x in prefix_mean],
        "prefix_p5": [round(x, 4) for x in prefix_p5],
        "prefix_p50": [round(x, 4) for x in prefix_p50],
        "prefix_p95": [round(x, 4) for x in prefix_p95],
    }

    out_path = DATA_DIR / "swap_surface.json"
    with open(out_path, "w") as f:
        json.dump(result, f)
    print(f"Saved to {out_path} ({out_path.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
