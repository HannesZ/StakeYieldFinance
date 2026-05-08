"""
StakeYieldFinance — Preliminary Visualizations
Generated from epoch_summary.csv (~4k epochs, Jan–Apr 2026)
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

sns.set_theme(style="whitegrid", palette="muted")

OUT_DIR = Path("figures")
OUT_DIR.mkdir(exist_ok=True)

# ── Load data ──
# Use history.csv (slot-level) — much more data than epoch_summary
df = pd.read_csv("data/history.csv")
df = df.sort_values("slot").reset_index(drop=True)

# Derive timestamp from slot (beacon genesis: 1606824023, 12s/slot)
GENESIS_TIME = 1606824023
df["timestamp"] = pd.to_datetime(GENESIS_TIME + df["slot"] * 12, unit="s")
df["date"] = df["timestamp"].dt.date

# Filter out data artifacts (active_count dropping to near-zero)
median_active = df["active_count"].median()
df = df[df["active_count"] > median_active * 0.5].reset_index(drop=True)

# Epoch-level: take one row per epoch (first slot of each)
epoch_df = df.groupby("epoch").first().reset_index()

# Compute churn limit per epoch (ETH-based, post-Pectra)
CHURN_LIMIT_QUOTIENT = 65536
MIN_PER_EPOCH_CHURN_LIMIT_ETH = 128  # 128 ETH = 4 validators * 32 ETH
epoch_df["churn_limit_eth"] = np.maximum(
    MIN_PER_EPOCH_CHURN_LIMIT_ETH,
    epoch_df["active_eth"] / CHURN_LIMIT_QUOTIENT
)

# Daily aggregation for smoother plots
daily = epoch_df.groupby("date").agg(
    active_count=("active_count", "last"),
    active_eth=("active_eth", "last"),
    entry_count=("entry_count", "last"),
    entry_eth=("entry_eth", "last"),
    exit_count=("exit_count", "last"),
    exit_eth=("exit_eth", "last"),
    pending_deposits_count=("pending_deposits_count", "last"),
    pending_deposits_eth=("pending_deposits_eth", "last"),
    churn_limit_eth=("churn_limit_eth", "last"),
    delta_active=("active_count", lambda x: x.iloc[-1] - x.iloc[0] if len(x) > 1 else 0),
).reset_index()
daily["date"] = pd.to_datetime(daily["date"])
daily["max_future_active_eth"] = daily["active_eth"] + daily["pending_deposits_eth"]

print(f"Epochs: {len(df):,} | Days: {len(daily)} | Range: {daily['date'].min().date()} → {daily['date'].max().date()}")

# ── 1. Active Validators (count & ETH) ──
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True)
ax1.plot(daily["date"], daily["active_count"] / 1000, linewidth=1.5, color="#2c7bb6")
ax1.set_ylabel("Active Validators (thousands)")
ax1.set_title("Active Validator Set Over Time", fontsize=14, fontweight="bold")
ax1.ticklabel_format(useOffset=False, axis='y')

ax2.plot(daily["date"], daily["active_eth"] / 1e6, linewidth=1.5, color="#d7191c")
ax2.set_ylabel("Active ETH (millions)")
ax2.set_xlabel("")
fig.autofmt_xdate()
plt.tight_layout()
plt.savefig(OUT_DIR / "01_active_validators.png", dpi=150)
plt.close()
print("✓ 01_active_validators.png")

# ── 2. Entry & Exit Queues (ETH) ──
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True)
ax1.plot(daily["date"], daily["entry_eth"], linewidth=1.5, color="#1a9641", label="Entry Queue ETH")
ax1.set_ylabel("Entry Queue (ETH)")
ax1.set_title("Entry & Exit Queues", fontsize=14, fontweight="bold")
ax1.legend()

ax2.plot(daily["date"], daily["exit_eth"], linewidth=1.5, color="#d7191c", label="Exit Queue ETH")
ax2.set_ylabel("Exit Queue (ETH)")
ax2.legend()
fig.autofmt_xdate()
plt.tight_layout()
plt.savefig(OUT_DIR / "02_entry_exit_queues.png", dpi=150)
plt.close()
print("✓ 02_entry_exit_queues.png")

# ── 3. Pending Deposits ──
fig, ax = plt.subplots(figsize=(14, 5))
ax.plot(daily["date"], daily["pending_deposits_eth"] / 1e6, linewidth=1.5, color="#fdae61")
ax.set_ylabel("Pending Deposits (M ETH)")
ax.set_title("Pending Deposits Over Time", fontsize=14, fontweight="bold")
ax.fill_between(daily["date"], 0, daily["pending_deposits_eth"] / 1e6, alpha=0.3, color="#fdae61")
fig.autofmt_xdate()
plt.tight_layout()
plt.savefig(OUT_DIR / "03_pending_deposits.png", dpi=150)
plt.close()
print("✓ 03_pending_deposits.png")

# ── 4. Pending Activation Time Estimate ──
# Rough: pending_deposits_eth / (churn_limit_eth * 225 epochs/day)
daily["pend_activate_days"] = daily["pending_deposits_eth"] / (daily["churn_limit_eth"] * 225)
fig, ax = plt.subplots(figsize=(14, 5))
ax.plot(daily["date"], daily["pend_activate_days"], linewidth=1.5, color="#2c7bb6")
ax.set_ylabel("Estimated Days to Activate All Pending")
ax.set_title("Pending Deposit Activation Time Estimate", fontsize=14, fontweight="bold")
fig.autofmt_xdate()
plt.tight_layout()
plt.savefig(OUT_DIR / "04_activation_time_estimate.png", dpi=150)
plt.close()
print("✓ 04_activation_time_estimate.png")

# ── 5. Net Flow (daily delta active) ──
fig, ax = plt.subplots(figsize=(14, 5))
colors = ["#1a9641" if v >= 0 else "#d7191c" for v in daily["delta_active"]]
ax.bar(daily["date"], daily["delta_active"], color=colors, width=1, alpha=0.8)
ax.axhline(0, color="black", linewidth=0.5)
ax.set_ylabel("Daily Net Change in Active Validators")
ax.set_title("Net Validator Flow (Entries − Exits)", fontsize=14, fontweight="bold")
fig.autofmt_xdate()
plt.tight_layout()
plt.savefig(OUT_DIR / "05_net_flow.png", dpi=150)
plt.close()
print("✓ 05_net_flow.png")

# ── 6. Churn Limit ──
fig, ax = plt.subplots(figsize=(14, 5))
ax.plot(daily["date"], daily["churn_limit_eth"], linewidth=1.5, color="#7b3294")
ax.set_ylabel("Churn Limit (ETH per Epoch)")
ax.set_title("Epoch Churn Limit Over Time", fontsize=14, fontweight="bold")
fig.autofmt_xdate()
plt.tight_layout()
plt.savefig(OUT_DIR / "06_churn_limit.png", dpi=150)
plt.close()
print("✓ 06_churn_limit.png")

# ── 7. Max Future Active ETH (if all pending activate) ──
fig, ax = plt.subplots(figsize=(14, 5))
ax.plot(daily["date"], daily["active_eth"] / 1e6, linewidth=1.5, color="#2c7bb6", label="Current Active ETH")
ax.plot(daily["date"], daily["max_future_active_eth"] / 1e6, linewidth=1.5, color="#fdae61", linestyle="--", label="Max Future Active ETH")
ax.fill_between(daily["date"], daily["active_eth"] / 1e6, daily["max_future_active_eth"] / 1e6, alpha=0.2, color="#fdae61")
ax.set_ylabel("ETH (millions)")
ax.set_title("Current vs. Max Future Active ETH", fontsize=14, fontweight="bold")
ax.legend()
fig.autofmt_xdate()
plt.tight_layout()
plt.savefig(OUT_DIR / "07_current_vs_max_active.png", dpi=150)
plt.close()
print("✓ 07_current_vs_max_active.png")

# ── 8. Implied Staking Yield (rough estimate) ──
# Base reward: ~2.6% * sqrt(32e6) / sqrt(active_eth) annualized
# This is a rough approximation
daily["implied_yield_pct"] = 2.6 * np.sqrt(32e6) / np.sqrt(daily["active_eth"]) * 100 / 100
fig, ax = plt.subplots(figsize=(14, 5))
ax.plot(daily["date"], daily["implied_yield_pct"], linewidth=1.5, color="#1a9641")
ax.set_ylabel("Implied Base Yield (%)")
ax.set_title("Rough Implied Staking Yield (Base Reward Only)", fontsize=14, fontweight="bold")
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.2f}%"))
fig.autofmt_xdate()
plt.tight_layout()
plt.savefig(OUT_DIR / "08_implied_yield.png", dpi=150)
plt.close()
print("✓ 08_implied_yield.png")

print(f"\n✅ All figures saved to {OUT_DIR}/")
