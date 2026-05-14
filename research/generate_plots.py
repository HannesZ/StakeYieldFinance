#!/usr/bin/env python3
"""Generate all yield-curve memo plots from history.csv.

Outputs PNGs to Theory/plots/.
"""

import csv
import math
import random
from pathlib import Path
from collections import defaultdict

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from scipy import stats

sns.set_theme(style="whitegrid", palette="deep", font_scale=1.1)
plt.rcParams["figure.dpi"] = 150
plt.rcParams["savefig.bbox"] = "tight"

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "history.csv"
PLOTS = ROOT / "Theory" / "plots"
PLOTS.mkdir(parents=True, exist_ok=True)

# ── Constants ──────────────────────────────────────────────────────────────
EPOCHS_PER_DAY = 225
BASE_REWARD_FACTOR = 64
EFFECTIVE_BALANCE_INCREMENT = 1e9  # Gwei = 1 ETH
SECONDS_PER_SLOT = 12
SLOTS_PER_EPOCH = 32

# ── 1. Load history ───────────────────────────────────────────────────────

def load_history():
    """Load history.csv, deduplicate to one row per epoch (last slot wins).
    Filters out rows with zero active_eth (data artefacts from missing slots).
    """
    raw = []
    with DATA.open(newline="") as f:
        for row in csv.DictReader(f):
            # Skip artefact rows with zero active balance
            if float(row["active_eth"]) == 0:
                continue
            raw.append(row)

    # Keep last slot per epoch
    by_epoch = {}
    for row in raw:
        ep = int(row["epoch"])
        by_epoch[ep] = row

    epochs = sorted(by_epoch.keys())
    out = {
        "epoch": np.array([int(by_epoch[e]["epoch"]) for e in epochs]),
        "slot": np.array([int(by_epoch[e]["slot"]) for e in epochs]),
        "active_count": np.array([int(by_epoch[e]["active_count"]) for e in epochs]),
        "active_eth": np.array([float(by_epoch[e]["active_eth"]) for e in epochs]),
        "entry_count": np.array([int(by_epoch[e].get("entry_count") or 0) for e in epochs]),
        "exit_count": np.array([int(by_epoch[e].get("exit_count") or 0) for e in epochs]),
        "pending_count": np.array([int(by_epoch[e].get("pending_deposits_count") or 0) for e in epochs]),
        "pending_eth": np.array([float(by_epoch[e].get("pending_deposits_eth") or 0) for e in epochs]),
        "entry_eth": np.array([float(by_epoch[e].get("entry_eth") or 0) for e in epochs]),
        "exit_eth": np.array([float(by_epoch[e].get("exit_eth") or 0) for e in epochs]),
    }
    return out


def epoch_to_approx_date(epoch, ref_epoch, ref_ts=None):
    """Convert epoch number to approximate days-since-start."""
    return (epoch - ref_epoch) * SECONDS_PER_SLOT * SLOTS_PER_EPOCH / 86400.0


# ── 2. APR computation ────────────────────────────────────────────────────

def compute_apr(active_eth, participation=0.99):
    """APR from total active balance in ETH.
    r = p * BASE_REWARD_FACTOR * 82125 / sqrt(B_total_gwei)
    B_total_gwei = active_eth * 1e9
    """
    b_total_gwei = active_eth * 1e9
    return participation * BASE_REWARD_FACTOR * 82125.0 / np.sqrt(b_total_gwei)


# ── 3. Queue flow derivation ─────────────────────────────────────────────

def compute_flows(hist):
    """Compute epoch-over-epoch changes in queues and active set."""
    d_active = np.diff(hist["active_count"])
    d_entry = np.diff(hist["entry_count"])
    d_exit = np.diff(hist["exit_count"])
    d_pending = np.diff(hist["pending_count"])
    return d_active, d_entry, d_exit, d_pending


# ── 4. Churn limit ────────────────────────────────────────────────────────

def churn_limit(active_count, epoch):
    """Compute churn limit per epoch."""
    base = max(4, active_count // 65536)
    DENEB_EPOCH = 269568
    if epoch >= DENEB_EPOCH:
        return min(8, base)
    return base


# ── 5. Deterministic queue unwind ─────────────────────────────────────────

def deterministic_unwind(active, entry, exit_q, pending, days,
                         activation_rate=8, drain_rate=8):
    out_active = [active]
    ac, eq, xq, pq = active, entry, exit_q, pending
    for _ in range(days):
        for _ in range(EPOCHS_PER_DAY):
            xq = max(0, xq - drain_rate)
            acts = min(activation_rate, eq + pq)
            from_eq = min(acts, eq)
            from_pq = acts - from_eq
            eq -= from_eq
            pq -= from_pq
            ac += acts
        out_active.append(ac)
    return out_active, eq, xq, pq


# ── 6. Monte Carlo yield paths ───────────────────────────────────────────

def build_yield_paths(term_days, n_sims, drift, vol, seed=42):
    """Build MC APR paths. Returns (paths_apr, queue_clear_day)."""
    raw = load_history()
    last_active = int(raw["active_count"][-1])
    last_entry = int(raw["entry_count"][-1])
    last_exit = int(raw["exit_count"][-1])
    last_pending = int(raw["pending_count"][-1])
    last_active_eth = float(raw["active_eth"][-1])

    # Deterministic phase
    det_active, eq_end, xq_end, pq_end = deterministic_unwind(
        last_active, last_entry, last_exit, last_pending, term_days)

    # Find queue clear day
    queue_clear_day = 0
    ac, e, x, p = last_active, last_entry, last_exit, last_pending
    for d in range(term_days + 1):
        if e == 0 and x == 0 and p == 0:
            queue_clear_day = d
            break
        for _ in range(EPOCHS_PER_DAY):
            x = max(0, x - 8)
            acts = min(8, e + p)
            take_e = min(acts, e)
            e -= take_e
            p -= (acts - take_e)
            ac += acts
    else:
        queue_clear_day = term_days

    # Estimate ETH per validator ratio for APR calc
    eth_per_val = last_active_eth / max(last_active, 1)

    rng = random.Random(seed)
    paths_apr = []
    paths_active = []
    for _ in range(n_sims):
        aprs = []
        actives = []
        n = float(det_active[min(queue_clear_day, term_days)])
        for d in range(term_days + 1):
            if d <= queue_clear_day:
                n_d = det_active[d]
            else:
                z = rng.gauss(0.0, 1.0)
                n = max(1.0, n + drift + vol * z)
                n_d = n
            active_eth_est = n_d * eth_per_val
            aprs.append(compute_apr(active_eth_est))
            actives.append(n_d)
        paths_apr.append(aprs)
        paths_active.append(actives)
    return paths_apr, paths_active, queue_clear_day


def discount_factor(aprs, term_days):
    acc = 0.0
    for d in range(1, term_days + 1):
        acc += aprs[d] / 365.0
    return math.exp(-acc)


# ══════════════════════════════════════════════════════════════════════════
# PLOTTING
# ══════════════════════════════════════════════════════════════════════════

def plot_91_historical_state(hist):
    """Section 9.1: Active validators, queues, churn limit over time."""
    epochs = hist["epoch"]
    days = (epochs - epochs[0]) / EPOCHS_PER_DAY

    fig, axes = plt.subplots(4, 1, figsize=(14, 14), sharex=True)

    # 9.1a: Active validator count
    ax = axes[0]
    ax.plot(days, hist["active_count"] / 1e3, color=sns.color_palette()[0], linewidth=1.2)
    ax.set_ylabel("Active Validators (thousands)")
    ax.set_title("Active Validator Count")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}k"))

    # 9.1b: Entry / Exit queues (without pending — different scale)
    ax = axes[1]
    ax.plot(days, hist["entry_count"], label="Entry queue", linewidth=1.0)
    ax.plot(days, hist["exit_count"], label="Exit queue", linewidth=1.0)
    ax.set_ylabel("Queue Size (validators)")
    ax.set_title("Entry & Exit Queues")
    ax.legend(loc="upper right")

    # 9.1c: Pending deposits (separate panel due to different scale)
    ax = axes[2]
    ax.plot(days, hist["pending_count"], color=sns.color_palette()[2], linewidth=1.0,
            label="Pending deposits")
    ax2 = ax.twinx()
    ax2.plot(days, hist["pending_eth"] / 1e6, color=sns.color_palette()[3],
             linewidth=1.0, alpha=0.7, label="Pending ETH (M)")
    ax.set_ylabel("Pending Deposits (validators)")
    ax2.set_ylabel("Pending ETH (millions)")
    ax.set_title("Pending Deposit Queue")
    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labels1 + labels2, loc="upper right")

    # 9.1d: Churn limit (constant at 8 post-Deneb, annotated)
    ax = axes[3]
    churn = np.array([churn_limit(int(a), int(e))
                      for a, e in zip(hist["active_count"], epochs)])
    ax.plot(days, churn, color=sns.color_palette()[3], linewidth=1.5)
    ax.set_ylabel("Churn Limit (validators/epoch)")
    ax.set_title("Activation Churn Limit")
    ax.set_xlabel("Days Since Start of Dataset")
    ax.set_ylim(0, 12)
    ax.axhline(8, color="gray", linestyle=":", linewidth=1, alpha=0.5)
    ax.annotate("Post-Deneb cap = 8", xy=(days[-1]*0.5, 8.3),
                fontsize=10, color="gray")

    fig.suptitle("Section 9.1 — Historical Beacon Chain State", fontsize=14, y=1.01)
    plt.tight_layout()
    fig.savefig(PLOTS / "9_1_historical_state.png")
    plt.close(fig)
    print("  ✓ 9.1 Historical state")


def plot_92_historical_apr(hist):
    """Section 9.2: Historical implied APR."""
    epochs = hist["epoch"]
    days = (epochs - epochs[0]) / EPOCHS_PER_DAY
    apr = compute_apr(hist["active_eth"]) * 100  # percent

    fig, axes = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

    # 9.2a: APR over time
    ax = axes[0]
    ax.plot(days, apr, color=sns.color_palette()[1], linewidth=1.2)
    ax.set_ylabel("Implied APR (%)")
    ax.set_title("Historical Consensus-Layer Staking APR")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.2f}%"))

    # 9.2b: APR vs total active ETH (scatter) — zoomed to data region with wider model
    ax = axes[1]
    # Subsample for scatter
    idx = np.arange(0, len(hist["active_eth"]), max(1, len(hist["active_eth"]) // 500))
    ax.scatter(hist["active_eth"][idx] / 1e6, apr[idx],
               alpha=0.4, s=12, color=sns.color_palette()[2], zorder=5)
    # Model line — extend ±20% around data range for context
    eth_min = hist["active_eth"].min() * 0.8
    eth_max = hist["active_eth"].max() * 1.2
    eth_range = np.linspace(eth_min, eth_max, 200)
    model_apr = compute_apr(eth_range) * 100
    ax.plot(eth_range / 1e6, model_apr, color=sns.color_palette()[3],
            linewidth=2, label=r"$r = p \cdot 5{,}256{,}000 \;/\; \sqrt{B_{total}}$")
    ax.set_xlim(eth_min / 1e6, eth_max / 1e6)
    ax.set_xlabel("Total Active Balance (million ETH)")
    ax.set_ylabel("APR (%)")
    ax.set_title("APR vs Active Balance — Model Fit (zoomed)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.2f}%"))
    ax.legend()

    fig.suptitle("Section 9.2 — Staking APR", fontsize=14, y=1.01)
    plt.tight_layout()
    fig.savefig(PLOTS / "9_2_historical_apr.png")
    plt.close(fig)
    print("  ✓ 9.2 Historical APR")


def plot_93_yield_curve():
    """Section 9.3: Zero-coupon rate term structure with MC bands."""
    term_days = 3 * 365  # 3 years
    n_sims = 2000
    tenors = [30, 90, 180, 365, 730, 1095]

    scenarios = [
        {"drift": 8,  "vol": 60,  "label": "Base (μ=+8, σ=60)"},
        {"drift": 0,  "vol": 60,  "label": "No growth (μ=0, σ=60)"},
        {"drift": 16, "vol": 60,  "label": "High growth (μ=+16, σ=60)"},
        {"drift": 8,  "vol": 30,  "label": "Low vol (μ=+8, σ=30)"},
        {"drift": 8,  "vol": 120, "label": "High vol (μ=+8, σ=120)"},
    ]

    fig, axes = plt.subplots(2, 1, figsize=(14, 10))

    # 9.3a: Zero rates with bands (base scenario)
    ax = axes[0]
    paths_apr, paths_active, q_day = build_yield_paths(
        term_days, n_sims, drift=8, vol=60)

    zr_p5, zr_p50, zr_p95 = [], [], []
    df_p5, df_p50, df_p95 = [], [], []
    for t in tenors:
        dfs = sorted([discount_factor(p, t) for p in paths_apr])
        n = len(dfs)
        d5, d50, d95 = dfs[int(0.05*(n-1))], dfs[n//2], dfs[int(0.95*(n-1))]
        df_p5.append(d5); df_p50.append(d50); df_p95.append(d95)
        ty = t / 365.0
        zr_p5.append(-math.log(d95) / ty * 100)   # note: lower DF = higher rate
        zr_p50.append(-math.log(d50) / ty * 100)
        zr_p95.append(-math.log(d5) / ty * 100)

    tenor_years = [t/365 for t in tenors]
    ax.fill_between(tenor_years, zr_p5, zr_p95, alpha=0.25, color=sns.color_palette()[0])
    ax.plot(tenor_years, zr_p50, "o-", color=sns.color_palette()[0],
            linewidth=2, markersize=6, label="Median")
    ax.plot(tenor_years, zr_p5, "--", color=sns.color_palette()[0],
            linewidth=0.8, alpha=0.6, label="5th / 95th pctile")
    ax.plot(tenor_years, zr_p95, "--", color=sns.color_palette()[0],
            linewidth=0.8, alpha=0.6)
    ax.set_xlabel("Tenor (years)")
    ax.set_ylabel("Zero Rate (%)")
    ax.set_title(f"Staking-Implied Zero Rate Term Structure (queue clear day {q_day})")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.2f}%"))
    ax.legend()

    # 9.3b: Comparison across scenarios
    ax = axes[1]
    scenario_colors = sns.color_palette("bright", len(scenarios))
    for i, sc in enumerate(scenarios):
        paths_apr_sc, _, q_day_sc = build_yield_paths(
            term_days, n_sims, drift=sc["drift"], vol=sc["vol"], seed=42+i)
        zr_meds = []
        for t in tenors:
            dfs = sorted([discount_factor(p, t) for p in paths_apr_sc])
            d50 = dfs[len(dfs)//2]
            zr_meds.append(-math.log(d50) / (t/365) * 100)
        ax.plot(tenor_years, zr_meds, "o-", linewidth=1.8, markersize=5,
                color=scenario_colors[i], label=sc["label"])

    ax.set_xlabel("Tenor (years)")
    ax.set_ylabel("Median Zero Rate (%)")
    ax.set_title("Yield Curve Sensitivity to Drift & Volatility")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.2f}%"))
    ax.legend(fontsize=9)

    fig.suptitle("Section 9.3 — Yield Curve", fontsize=14, y=1.01)
    plt.tight_layout()
    fig.savefig(PLOTS / "9_3_yield_curve.png")
    plt.close(fig)
    print("  ✓ 9.3 Yield curve")

    return df_p5, df_p50, df_p95, tenors, paths_apr


def plot_94_queue_distributions(hist):
    """Section 9.4: Queue flow distributions with fitted parametric curves."""
    d_active, d_entry, d_exit, d_pending = compute_flows(hist)

    # Net new entries = increase in entry queue + activations that went through
    # Simplification: use -d_entry as proxy for "newly queued entries consumed"
    # and d_exit for "newly queued exits"
    # More meaningful: look at positive changes in pending + entry queues
    new_entry_flow = np.maximum(-d_entry, 0)  # entries leaving queue (activated)
    new_exit_flow = np.maximum(d_exit, 0)     # new exits entering queue

    # Filter out zeros for fitting
    entry_nz = new_entry_flow[new_entry_flow > 0]
    exit_nz = new_exit_flow[new_exit_flow > 0]

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    for col, (data, name) in enumerate([(entry_nz, "Entry Queue Activations"),
                                         (exit_nz, "New Exit Queue Additions")]):
        if len(data) < 10:
            axes[0, col].text(0.5, 0.5, f"Insufficient data\n({len(data)} points)",
                              ha="center", va="center", transform=axes[0, col].transAxes)
            axes[1, col].text(0.5, 0.5, "N/A", ha="center", va="center",
                              transform=axes[1, col].transAxes)
            continue

        # Histogram with KDE
        ax = axes[0, col]
        use_log = (data.max() / max(data.min(), 1)) > 50  # use log if very skewed
        if use_log:
            log_data = np.log1p(data)
            sns.histplot(log_data, bins=min(50, max(10, len(data)//20)), kde=True,
                         ax=ax, stat="density", alpha=0.5)
            ax.set_xlabel("log(1 + validators per epoch)")
        else:
            sns.histplot(data, bins=min(50, max(10, len(data)//20)), kde=True,
                         ax=ax, stat="density", alpha=0.5)

        # Fit distributions (on log-transformed data if skewed)
        fit_data = np.log1p(data) if use_log else data
        fits = {}
        x = np.linspace(fit_data.min(), np.percentile(fit_data, 99), 200)

        # Normal
        mu, sigma = stats.norm.fit(fit_data)
        fits["Normal"] = (stats.norm.pdf(x, mu, sigma),
                          stats.kstest(fit_data, "norm", args=(mu, sigma)).pvalue)

        # Gamma (only for positive data)
        if fit_data.min() > 0:
            try:
                a_g, loc_g, scale_g = stats.gamma.fit(fit_data, floc=0)
                fits["Gamma"] = (stats.gamma.pdf(x, a_g, loc=0, scale=scale_g),
                                 stats.kstest(fit_data, "gamma", args=(a_g, 0, scale_g)).pvalue)
            except Exception:
                pass

        # Lognormal (only for positive data)
        if fit_data.min() > 0:
            try:
                s_ln, loc_ln, scale_ln = stats.lognorm.fit(fit_data, floc=0)
                fits["Lognormal"] = (stats.lognorm.pdf(x, s_ln, loc=0, scale=scale_ln),
                                     stats.kstest(fit_data, "lognorm", args=(s_ln, 0, scale_ln)).pvalue)
            except Exception:
                pass

        colors = sns.color_palette("Set2", len(fits))
        for i, (dist_name, (pdf, pval)) in enumerate(fits.items()):
            ax.plot(x, pdf, linewidth=1.5, color=colors[i],
                    label=f"{dist_name} (KS p={pval:.3f})")

        ax.set_title(f"{name} — Histogram + Fits")
        ax.set_xlabel("Validators per Epoch")
        ax.legend(fontsize=8)

        # QQ plot for best fit
        ax = axes[1, col]
        best = max(fits, key=lambda k: fits[k][1])
        if best == "Normal":
            dist = stats.norm(mu, sigma)
        elif best == "Gamma":
            dist = stats.gamma(a_g, loc=0, scale=scale_g)
        else:
            dist = stats.lognorm(s_ln, loc=0, scale=scale_ln)

        theoretical = dist.ppf(np.linspace(0.01, 0.99, len(fit_data)))
        empirical = np.sort(fit_data)[:len(theoretical)]
        ax.scatter(theoretical, empirical, s=4, alpha=0.5)
        lims = [min(theoretical.min(), empirical.min()),
                max(theoretical.max(), empirical.max())]
        ax.plot(lims, lims, "r--", linewidth=1)
        ax.set_xlabel(f"Theoretical ({best})")
        ax.set_ylabel("Empirical")
        ax.set_title(f"QQ-Plot — Best Fit: {best}")

    fig.suptitle("Section 9.4 — Queue Flow Distributions", fontsize=14, y=1.01)
    plt.tight_layout()
    fig.savefig(PLOTS / "9_4_queue_distributions.png")
    plt.close(fig)
    print("  ✓ 9.4 Queue distributions")


def plot_95_pv_analysis(paths_apr, tenors):
    """Section 9.5: PV of 100 ETH, distribution, sensitivity."""
    cashflow = 100.0

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # 9.5a: PV as function of tenor (with bands)
    ax = axes[0, 0]
    pv_p5, pv_p50, pv_p95 = [], [], []
    fine_tenors = list(range(30, 1096, 30))
    for t in fine_tenors:
        dfs = sorted([discount_factor(p, t) for p in paths_apr])
        n = len(dfs)
        pv_p5.append(cashflow * dfs[int(0.05*(n-1))])
        pv_p50.append(cashflow * dfs[n//2])
        pv_p95.append(cashflow * dfs[int(0.95*(n-1))])

    tenor_y = [t/365 for t in fine_tenors]
    ax.fill_between(tenor_y, pv_p5, pv_p95, alpha=0.2, color=sns.color_palette()[0])
    ax.plot(tenor_y, pv_p50, linewidth=2, color=sns.color_palette()[0], label="Median PV")
    ax.axhline(cashflow, color="gray", linestyle=":", linewidth=1, label="Undiscounted")
    ax.set_xlabel("Tenor (years)")
    ax.set_ylabel("Present Value (ETH)")
    ax.set_title("PV of 100 ETH — Staking Discount")
    ax.legend()

    # 9.5b: PV distribution at 1 year
    ax = axes[0, 1]
    dfs_1y = [discount_factor(p, 365) for p in paths_apr]
    pvs_1y = [cashflow * df for df in dfs_1y]
    sns.histplot(pvs_1y, bins=40, kde=True, ax=ax, color=sns.color_palette()[1])
    ax.axvline(np.median(pvs_1y), color="red", linestyle="--", label=f"Median: {np.median(pvs_1y):.2f}")
    ax.set_xlabel("PV (ETH)")
    ax.set_title("PV Distribution — 1 Year Tenor")
    ax.legend()

    # 9.5c: PV distribution at 3 years
    ax = axes[1, 0]
    dfs_3y = [discount_factor(p, 1095) for p in paths_apr]
    pvs_3y = [cashflow * df for df in dfs_3y]
    sns.histplot(pvs_3y, bins=40, kde=True, ax=ax, color=sns.color_palette()[2])
    ax.axvline(np.median(pvs_3y), color="red", linestyle="--", label=f"Median: {np.median(pvs_3y):.2f}")
    ax.set_xlabel("PV (ETH)")
    ax.set_title("PV Distribution — 3 Year Tenor")
    ax.legend()

    # 9.5d: Sensitivity to drift and vol
    ax = axes[1, 1]
    drifts = [0, 4, 8, 12, 16]
    vols = [30, 60, 120]
    tenor_1y = 365
    results = {}
    for v in vols:
        medians = []
        for dr in drifts:
            pa, _, _ = build_yield_paths(tenor_1y, 1000, drift=dr, vol=v, seed=99)
            dfs = sorted([discount_factor(p, tenor_1y) for p in pa])
            medians.append(cashflow * dfs[len(dfs)//2])
        results[v] = medians

    for i, v in enumerate(vols):
        ax.plot(drifts, results[v], "o-", linewidth=1.5, label=f"σ={v}")
    ax.set_xlabel("Drift (validators/day)")
    ax.set_ylabel("Median PV of 100 ETH at 1Y (ETH)")
    ax.set_title("PV Sensitivity — Drift × Volatility")
    ax.yaxis.get_major_formatter().set_useOffset(False)  # no scientific offset
    ax.ticklabel_format(style="plain", axis="y")
    ax.legend()

    fig.suptitle("Section 9.5 — Present Value Analysis", fontsize=14, y=1.01)
    plt.tight_layout()
    fig.savefig(PLOTS / "9_5_pv_analysis.png")
    plt.close(fig)
    print("  ✓ 9.5 PV analysis")


# ══════════════════════════════════════════════════════════════════════════

def main():
    print("Loading history.csv ...")
    hist = load_history()
    n_epochs = len(hist["epoch"])
    print(f"  {n_epochs} unique epochs loaded "
          f"(epoch {hist['epoch'][0]}–{hist['epoch'][-1]})")
    print(f"  Active: {hist['active_count'][-1]:,} validators, "
          f"{hist['active_eth'][-1]:,.0f} ETH")
    print()

    print("Generating plots:")
    plot_91_historical_state(hist)
    plot_92_historical_apr(hist)
    df_p5, df_p50, df_p95, tenors, paths_apr = plot_93_yield_curve()
    plot_94_queue_distributions(hist)
    plot_95_pv_analysis(paths_apr, tenors)

    print()
    print(f"All plots saved to {PLOTS}/")

    # Print summary table
    print()
    print("── Yield Curve Summary (Base Scenario) ──")
    print(f"{'Tenor':>8s}  {'DF (p50)':>10s}  {'Zero Rate':>10s}")
    for t, d in zip(tenors, df_p50):
        ty = t / 365.0
        zr = -math.log(d) / ty * 100
        print(f"{t:>6d}d  {d:>10.6f}  {zr:>9.2f}%")


if __name__ == "__main__":
    main()
