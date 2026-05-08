#!/usr/bin/env python3
"""Plot historical ETH staking APR (from DefiLlama Lido data) + modeled projection."""

import json
import math
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add parent for yield_curve_cli imports
sys.path.insert(0, str(Path(__file__).resolve().parent))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

# ── 1. Fetch historical Lido stETH APY from DefiLlama ──────────────────────
POOL_ID = "747c1d2a-c668-4682-b9f9-296708a3dd90"  # Lido stETH
URL = f"https://yields.llama.fi/chart/{POOL_ID}"

print("Fetching historical staking APR from DefiLlama...")
req = urllib.request.Request(URL, headers={"User-Agent": "Mozilla/5.0"})
with urllib.request.urlopen(req, timeout=30) as resp:
    payload = json.loads(resp.read())

raw = payload["data"]
print(f"  Got {len(raw)} daily observations")

# Filter to last ~3 years
cutoff = datetime.utcnow() - timedelta(days=3*365 + 60)
hist_dates = []
hist_apy = []
for row in raw:
    dt = datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00")).replace(tzinfo=None)
    apy = row.get("apyBase") or row.get("apy")
    if dt >= cutoff and apy is not None:
        hist_dates.append(dt)
        hist_apy.append(apy)

print(f"  Filtered to {len(hist_dates)} points from {hist_dates[0].date()} to {hist_dates[-1].date()}")

# ── 2. Load model projection from epoch_summary + yield_curve_cli ───────────
DATA_DIR = Path(__file__).resolve().parents[1] / "data"
HISTORY_CSV = DATA_DIR / "history.csv"

# Beacon constants
BASE_REWARD_FACTOR = 64
EPOCHS_PER_YEAR = 82125
EPOCHS_PER_DAY = 225
EL_PREMIUM = 0.0013  # ~0.13% execution layer premium

def apr_from_active_eth(active_eth, el_premium=EL_PREMIUM):
    cl_apr = BASE_REWARD_FACTOR * EPOCHS_PER_YEAR / math.sqrt(max(active_eth, 1.0) * 1e9)
    return (cl_apr + el_premium) * 100  # percent

def eth_churn_per_epoch(active_eth):
    return max(128.0, active_eth / 65536.0)

# Read last state from history.csv
import subprocess
header = subprocess.check_output(["head", "-1", str(HISTORY_CSV)], text=True).strip().split(",")
last = subprocess.check_output(["tail", "-1", str(HISTORY_CSV)], text=True).strip().split(",")
state = dict(zip(header, last))

active_eth = float(state["active_eth"])
entry_eth = float(state.get("entry_eth", 0))
exit_eth = float(state.get("exit_eth", 0))
pending_eth = float(state.get("pending_deposits_eth", 0))

# Get approximate date of last observation
epoch_summary = DATA_DIR / "epoch_summary.csv"
es_header = subprocess.check_output(["head", "-1", str(epoch_summary)], text=True).strip().split(",")
es_last = subprocess.check_output(["tail", "-1", str(epoch_summary)], text=True).strip().split(",")
es_state = dict(zip(es_header, es_last))
last_date_str = es_state.get("timestamp_utc", "2026-03-18 00:00")
model_start = datetime.strptime(last_date_str[:16], "%Y-%m-%d %H:%M")
print(f"  Model starts from: {model_start.date()}, active_eth={active_eth:,.0f}")

# ── 3. Run deterministic + stochastic projection (3 years forward) ──────────
TERM_DAYS = 3 * 365
N_SIMS = 500

# Calibrated parameters (from memo work)
DEPOSIT_DRIFT = 50000.0
DEPOSIT_VOL = 40000.0
JUMP_LAMBDA = 0.03
JUMP_MU = 200000.0
JUMP_SIGMA = 50000.0
EXIT_DRIFT = 20000.0
EXIT_VOL = 30000.0

import random

def deterministic_phase(active, entry, exit_q, pending, days):
    path = [active]
    for _ in range(days):
        churn = eth_churn_per_epoch(active)
        for _ in range(EPOCHS_PER_DAY):
            drained = min(churn, exit_q)
            exit_q -= drained
            active -= drained
            can_activate = min(churn, entry + pending)
            from_entry = min(can_activate, entry)
            entry -= from_entry
            pending -= (can_activate - from_entry)
            active += can_activate
        path.append(active)
    return path, entry, exit_q, pending

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
        te = min(can, eq); eq -= te; pq -= (can - te)
        ac += can

det_path, _, _, _ = deterministic_phase(active_eth, entry_eth, exit_eth, pending_eth, TERM_DAYS)

# Monte Carlo
print(f"  Running {N_SIMS} Monte Carlo simulations ({TERM_DAYS} days)...")
rng = random.Random(42)
all_aprs = np.zeros((N_SIMS, TERM_DAYS + 1))

for sim in range(N_SIMS):
    eth_sim = det_path[min(queue_clear_day, TERM_DAYS)]
    pq_sim = eq_sim = xq_sim = 0.0
    for d in range(TERM_DAYS + 1):
        if d <= queue_clear_day:
            all_aprs[sim, d] = apr_from_active_eth(det_path[d])
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

# Compute percentiles
p5  = np.percentile(all_aprs, 5, axis=0)
p25 = np.percentile(all_aprs, 25, axis=0)
p50 = np.percentile(all_aprs, 50, axis=0)
p75 = np.percentile(all_aprs, 75, axis=0)
p95 = np.percentile(all_aprs, 95, axis=0)

proj_dates = [model_start + timedelta(days=d) for d in range(TERM_DAYS + 1)]

# ── 4. Plot ─────────────────────────────────────────────────────────────────
print("  Plotting...")
fig, (ax, ax2) = plt.subplots(2, 1, figsize=(14, 9), height_ratios=[3, 2],
                               gridspec_kw={"hspace": 0.28})

# === TOP: full history + projection ===
hist_apy_arr = np.array(hist_apy)
window = 7
if len(hist_apy_arr) >= window:
    smoothed = np.convolve(hist_apy_arr, np.ones(window)/window, mode='valid')
    smooth_dates = hist_dates[window-1:]
else:
    smoothed = hist_apy_arr
    smooth_dates = hist_dates

ax.plot(smooth_dates, smoothed, color="#4A90D9", linewidth=1.2,
        label="Historical (Lido stETH, 7d avg)", alpha=0.9)
ax.fill_between(smooth_dates, 0, smoothed, alpha=0.08, color="#4A90D9")

# Transition line
if len(hist_dates) > 0 and len(proj_dates) > 0:
    ax.plot([hist_dates[-1], proj_dates[0]], [hist_apy[-1], p50[0]],
            color="gray", linewidth=1, linestyle="--", alpha=0.5)

# Model projection with explicit confidence lines
ax.plot(proj_dates, p50, color="#E74C3C", linewidth=1.8, label="Model median (p50)")
ax.plot(proj_dates, p5,  color="#E74C3C", linewidth=0.7, linestyle="--", alpha=0.6, label="Model p5 / p95")
ax.plot(proj_dates, p95, color="#E74C3C", linewidth=0.7, linestyle="--", alpha=0.6)
ax.plot(proj_dates, p25, color="#E74C3C", linewidth=0.5, linestyle=":", alpha=0.5, label="Model p25 / p75")
ax.plot(proj_dates, p75, color="#E74C3C", linewidth=0.5, linestyle=":", alpha=0.5)
ax.fill_between(proj_dates, p25, p75, alpha=0.25, color="#E74C3C")
ax.fill_between(proj_dates, p5, p95, alpha=0.10, color="#E74C3C")

# Vertical line at model start
ax.axvline(model_start, color="gray", linewidth=0.8, linestyle=":", alpha=0.7)
ax.text(model_start + timedelta(days=10), 6.2, "Model start",
        fontsize=8, color="gray", va="top")

# Key events
events = [
    (datetime(2023, 4, 12), "Shapella"),
    (datetime(2024, 3, 13), "Dencun"),
    (datetime(2025, 3, 11), "Pectra"),
]
for edate, ename in events:
    if hist_dates[0] <= edate <= proj_dates[-1]:
        ax.axvline(edate, color="#999", linewidth=0.6, linestyle="--", alpha=0.5)
        ax.text(edate + timedelta(days=5), 5.8, ename,
                fontsize=7, color="#666", rotation=90, va="top")

ax.set_ylabel("Staking APR (%)", fontsize=11)
ax.set_title("Ethereum Staking APR: Historical + Modeled Projection", fontsize=13, fontweight="bold")
ax.legend(loc="upper right", fontsize=9)
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
fig.autofmt_xdate(rotation=30)
ax.set_ylim(bottom=0, top=7.5)
ax.grid(True, alpha=0.3)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

# === BOTTOM: zoomed projection with visible confidence channels ===
ax2.plot(proj_dates, p50, color="#E74C3C", linewidth=1.8, label="Median (p50)")
ax2.plot(proj_dates, p5,  color="#C0392B", linewidth=1.0, linestyle="--", label="p5 / p95")
ax2.plot(proj_dates, p95, color="#C0392B", linewidth=1.0, linestyle="--")
ax2.plot(proj_dates, p25, color="#E67E22", linewidth=0.8, linestyle=":", label="p25 / p75")
ax2.plot(proj_dates, p75, color="#E67E22", linewidth=0.8, linestyle=":")
ax2.fill_between(proj_dates, p25, p75, alpha=0.30, color="#E74C3C")
ax2.fill_between(proj_dates, p5, p95, alpha=0.12, color="#E74C3C")

# Annotate spread at key horizons
for yr, label in [(365, "1Y"), (730, "2Y"), (1095, "3Y")]:
    if yr < len(proj_dates):
        spread = p95[yr] - p5[yr]
        mid = (p95[yr] + p5[yr]) / 2
        ax2.annotate(f"{label}: {spread*100:.0f}bp",
                     xy=(proj_dates[yr], p95[yr]),
                     xytext=(0, 8), textcoords="offset points",
                     fontsize=7.5, color="#333", ha="center",
                     arrowprops=dict(arrowstyle="-", color="#999", lw=0.5))

# Auto y-limits with padding
all_vals = np.concatenate([p5, p95])
ymin = all_vals.min() - 0.05
ymax = all_vals.max() + 0.1
ax2.set_ylim(ymin, ymax)

ax2.set_xlabel("Date", fontsize=11)
ax2.set_ylabel("Modeled APR (%)", fontsize=11)
ax2.set_title("Projection Detail — Confidence Channels (zoomed)", fontsize=11, fontstyle="italic")
ax2.legend(loc="upper right", fontsize=8)
ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
ax2.grid(True, alpha=0.3)
ax2.spines["top"].set_visible(False)
ax2.spines["right"].set_visible(False)

plt.tight_layout()
out_path = Path(__file__).resolve().parents[1] / "figures" / "apr_history_and_model.png"
out_path.parent.mkdir(exist_ok=True)
fig.savefig(str(out_path), dpi=180)
print(f"  Saved to {out_path}")

# Also print spread stats
for yr in [365, 730, 1095]:
    if yr < len(p50):
        print(f"  {yr//365}Y: p5={p5[yr]:.3f}% p50={p50[yr]:.3f}% p95={p95[yr]:.3f}% spread={100*(p95[yr]-p5[yr]):.1f}bp")
plt.close()
