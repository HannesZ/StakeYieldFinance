#!/usr/bin/env python3
"""Generate yield curve + key summary plots with recalibrated May 2026 parameters."""

import sys
import math
import random
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[0]))
from yield_curve_cli import (
    load_last_state, build_yield_paths, discount_factor_from_apr_path,
    apr_from_active_eth, DATA
)

sns.set_theme(style="whitegrid", palette="muted")
OUT = Path(__file__).resolve().parents[1] / "figures"
OUT.mkdir(exist_ok=True)

# Recalibrated parameters (May 2026, ETH-denominated, 90-day window)
PARAMS = {
    "drift_per_day": 31797.0,
    "vol_per_sqrt_day": 51622.0,
    "jump_lambda": 0.0,
    "jump_mu": 0.0,
    "jump_sigma": 1.0,
    "exit_drift": 35058.0,
    "exit_vol": 37037.0,
}

# Old params (March 2026, validator-count based) — kept for comparison shape only
# Note: these used validator counts, not ETH, so the absolute rate level is not comparable
OLD_PARAMS = {
    "drift_per_day": 1702.0,
    "vol_per_sqrt_day": 703.0,
    "jump_lambda": 0.0444,
    "jump_mu": 1981.0,
    "jump_sigma": 500.0,
    "exit_drift": 2374.0,
    "exit_vol": 1202.0,
}

N_SIMS = 500
TERM_YEARS = 3
TERM_DAYS = int(TERM_YEARS * 365)
TENORS = [7, 14, 30, 60, 90, 180, 365, 548, 730, 912, 1095]


def compute_curve(params, n_sims=N_SIMS, term_days=TERM_DAYS):
    """Build yield paths and extract percentiles at each tenor."""
    paths, q_day = build_yield_paths(
        term_days, n_sims,
        params["drift_per_day"], params["vol_per_sqrt_day"],
        params["jump_lambda"], params["jump_mu"], params["jump_sigma"],
        params["exit_drift"], params["exit_vol"],
    )

    tenors = [t for t in TENORS if t <= term_days]
    results = {"tenor": [], "zr_p5": [], "zr_p25": [], "zr_p50": [], "zr_p75": [], "zr_p95": []}

    for t in tenors:
        dfs = sorted([discount_factor_from_apr_path(p, t) for p in paths])
        zrs = [-math.log(df) / (t / 365.0) * 100 for df in dfs]  # in %
        zrs.sort()
        n = len(zrs)
        results["tenor"].append(t)
        results["zr_p5"].append(zrs[int(0.05 * (n - 1))])
        results["zr_p25"].append(zrs[int(0.25 * (n - 1))])
        results["zr_p50"].append(zrs[n // 2])
        results["zr_p75"].append(zrs[int(0.75 * (n - 1))])
        results["zr_p95"].append(zrs[int(0.95 * (n - 1))])

    return results, paths, q_day


def plot_yield_curve(new_results, old_results):
    """Plot yield curve with confidence bands, comparing old vs new calibration."""
    fig, ax = plt.subplots(figsize=(14, 7))

    t_new = np.array(new_results["tenor"]) / 365.0  # years
    t_old = np.array(old_results["tenor"]) / 365.0

    # New calibration — bands
    ax.fill_between(t_new, new_results["zr_p5"], new_results["zr_p95"],
                    alpha=0.15, color="#2c7bb6", label="May 2026 (ETH-based): 5th–95th pctl")
    ax.fill_between(t_new, new_results["zr_p25"], new_results["zr_p75"],
                    alpha=0.3, color="#2c7bb6", label="May 2026 (ETH-based): 25th–75th pctl")
    ax.plot(t_new, new_results["zr_p50"], "o-", color="#2c7bb6", linewidth=2.5,
            markersize=6, label="May 2026: median", zorder=5)

    # Reference: validatorqueue.com spot rate
    ax.axhline(2.82, color="#1a9641", linewidth=1.5, linestyle=":",
               label="validatorqueue.com spot APR (2.82%)")

    ax.set_xlabel("Term (years)", fontsize=12)
    ax.set_ylabel("Zero-Coupon Rate (%)", fontsize=12)
    ax.set_title("Ethereum Staking Yield Curve — Recalibrated May 2026\n"
                 "ETH-denominated flows, protocol APR formula, 500 Monte Carlo paths",
                 fontsize=14, fontweight="bold")
    ax.legend(loc="best", fontsize=10)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.2f}%"))
    plt.tight_layout()
    plt.savefig(OUT / "09_yield_curve_recalibrated.png", dpi=150)
    plt.close()
    print("✓ 09_yield_curve_recalibrated.png")


def plot_apr_fan(paths):
    """Fan chart of simulated APR paths over 3 years."""
    term_days = len(paths[0]) - 1
    days = np.arange(term_days + 1)
    years = days / 365.0

    all_aprs = np.array(paths) * 100  # to percent
    p5 = np.percentile(all_aprs, 5, axis=0)
    p25 = np.percentile(all_aprs, 25, axis=0)
    p50 = np.percentile(all_aprs, 50, axis=0)
    p75 = np.percentile(all_aprs, 75, axis=0)
    p95 = np.percentile(all_aprs, 95, axis=0)

    fig, ax = plt.subplots(figsize=(14, 7))
    ax.fill_between(years, p5, p95, alpha=0.15, color="#1a9641", label="5th–95th pctl")
    ax.fill_between(years, p25, p75, alpha=0.3, color="#1a9641", label="25th–75th pctl")
    ax.plot(years, p50, color="#1a9641", linewidth=2, label="Median APR")

    ax.set_xlabel("Years from now", fontsize=12)
    ax.set_ylabel("Staking APR (%)", fontsize=12)
    ax.set_title("Projected Staking APR — Fan Chart (500 MC paths)\n"
                 "Entry-heavy regime: net +31,797 ETH/day → APR declines gradually",
                 fontsize=14, fontweight="bold")
    ax.legend(loc="best", fontsize=10)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.2f}%"))
    plt.tight_layout()
    plt.savefig(OUT / "10_apr_fan_chart.png", dpi=150)
    plt.close()
    print("✓ 10_apr_fan_chart.png")


def plot_discount_factors(new_results):
    """Plot discount factor term structure."""
    # Recompute DFs from zero rates
    tenors = np.array(new_results["tenor"])
    years = tenors / 365.0

    df_p50 = [math.exp(-new_results["zr_p50"][i] / 100 * years[i]) for i in range(len(tenors))]
    df_p5 = [math.exp(-new_results["zr_p95"][i] / 100 * years[i]) for i in range(len(tenors))]  # note: higher rate = lower DF
    df_p95 = [math.exp(-new_results["zr_p5"][i] / 100 * years[i]) for i in range(len(tenors))]

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.fill_between(years, df_p5, df_p95, alpha=0.2, color="#7b3294")
    ax.plot(years, df_p50, "o-", color="#7b3294", linewidth=2, markersize=5, label="Median DF")
    ax.set_xlabel("Term (years)", fontsize=12)
    ax.set_ylabel("Discount Factor", fontsize=12)
    ax.set_title("Staking Discount Factor Curve", fontsize=14, fontweight="bold")
    ax.legend()
    plt.tight_layout()
    plt.savefig(OUT / "11_discount_factors.png", dpi=150)
    plt.close()
    print("✓ 11_discount_factors.png")


if __name__ == "__main__":
    print("Building new calibration yield paths...")
    new_results, paths, q_day = compute_curve(PARAMS)
    print(f"  Queue clear day: {q_day}")
    print(f"  Median 1Y zero rate: {new_results['zr_p50'][TENORS.index(365) if 365 in new_results['tenor'] else -1]:.2f}%")

    print("Building old calibration yield paths...")
    old_results, _, _ = compute_curve(OLD_PARAMS)

    print("\nGenerating plots...")
    plot_yield_curve(new_results, old_results)
    plot_apr_fan(paths)
    plot_discount_factors(new_results)
    print("\n✅ Done!")
