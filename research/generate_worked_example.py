#!/usr/bin/env python3
"""Generate a fully worked PV example with transparent APR breakdown.

Tenor: 90 days (beyond the ~25-day queue clear)
Cash flow: 100 ETH at day 90
Shows: deterministic phase → stochastic phase, day-by-day APR, cumulative DF
"""

import math
import random
from pathlib import Path

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.ticker as mticker
import seaborn as sns

sns.set_theme(style="whitegrid", palette="deep", font_scale=1.1)
plt.rcParams["figure.dpi"] = 150
plt.rcParams["savefig.bbox"] = "tight"

ROOT = Path(__file__).resolve().parents[1]
PLOTS = ROOT / "Theory" / "plots"
PLOTS.mkdir(parents=True, exist_ok=True)

# Import from generate_plots
import sys
sys.path.insert(0, str(ROOT / "scripts"))
from generate_plots import (
    load_history, deterministic_unwind, compute_apr, discount_factor,
    EPOCHS_PER_DAY
)


def main():
    TENOR = 90  # days
    CASHFLOW = 100.0  # ETH
    N_SIMS = 2000
    DRIFT = 8.0
    VOL = 60.0
    SEED = 42
    P = 0.99  # participation rate

    # ── Load last state ───────────────────────────────────────────────────
    hist = load_history()
    last_active = int(hist["active_count"][-1])
    last_entry = int(hist["entry_count"][-1])
    last_exit = int(hist["exit_count"][-1])
    last_pending = int(hist["pending_count"][-1])
    last_active_eth = float(hist["active_eth"][-1])
    eth_per_val = last_active_eth / max(last_active, 1)

    print(f"Starting state:")
    print(f"  Active: {last_active:,} validators ({last_active_eth:,.0f} ETH)")
    print(f"  Queues: entry={last_entry}, exit={last_exit}, pending={last_pending:,}")
    print(f"  ETH/validator: {eth_per_val:.4f}")
    print()

    # ── Phase 1: Deterministic queue unwind ────────────────────────────────
    det_active, eq_end, xq_end, pq_end = deterministic_unwind(
        last_active, last_entry, last_exit, last_pending, TENOR)

    # Find queue clear day
    ac, e, x, p_q = last_active, last_entry, last_exit, last_pending
    queue_clear_day = TENOR
    for d in range(TENOR + 1):
        if e == 0 and x == 0 and p_q == 0:
            queue_clear_day = d
            break
        for _ in range(EPOCHS_PER_DAY):
            x = max(0, x - 8)
            acts = min(8, e + p_q)
            take_e = min(acts, e)
            e -= take_e
            p_q -= (acts - take_e)
            ac += acts

    print(f"Queue clear day: {queue_clear_day}")
    print(f"  Active at queue clear: {det_active[queue_clear_day]:,}")
    print()

    # ── Compute single deterministic path APR (median sim) ─────────────────
    # For the worked example, show one representative path
    rng = random.Random(SEED)
    det_aprs = []
    det_actives = []
    n_stoch = float(det_active[queue_clear_day])
    for d in range(TENOR + 1):
        if d <= queue_clear_day:
            n_d = det_active[d]
        else:
            z = rng.gauss(0.0, 1.0)
            n_stoch = max(1.0, n_stoch + DRIFT + VOL * z)
            n_d = n_stoch
        active_eth_est = n_d * eth_per_val
        apr = compute_apr(active_eth_est, participation=P)
        det_aprs.append(apr)
        det_actives.append(n_d)

    # ── Compute cumulative discount factor day by day ──────────────────────
    cum_log_df = [0.0]  # day 0: DF = 1
    for d in range(1, TENOR + 1):
        cum_log_df.append(cum_log_df[-1] - det_aprs[d] / 365.0)
    cum_df = [math.exp(x) for x in cum_log_df]

    final_df = cum_df[-1]
    final_pv = CASHFLOW * final_df
    zero_rate = -math.log(final_df) / (TENOR / 365.0) * 100

    print(f"── Single Path Result (seed={SEED}) ──")
    print(f"  Discount factor P(0, {TENOR}d) = {final_df:.6f}")
    print(f"  Zero rate z({TENOR/365:.4f}y) = {zero_rate:.4f}%")
    print(f"  PV of {CASHFLOW} ETH at day {TENOR} = {final_pv:.4f} ETH")
    print()

    # ── Monte Carlo for confidence bands ──────────────────────────────────
    all_aprs = []
    all_actives = []
    all_dfs = []
    for sim in range(N_SIMS):
        rng_mc = random.Random(SEED + sim)
        aprs_mc = []
        n_mc = float(det_active[queue_clear_day])
        for d in range(TENOR + 1):
            if d <= queue_clear_day:
                n_d = det_active[d]
            else:
                z = rng_mc.gauss(0.0, 1.0)
                n_mc = max(1.0, n_mc + DRIFT + VOL * z)
                n_d = n_mc
            aprs_mc.append(compute_apr(n_d * eth_per_val, participation=P))
        all_aprs.append(aprs_mc)

        # Cumulative DF for this path
        path_dfs = [1.0]
        log_acc = 0.0
        for d in range(1, TENOR + 1):
            log_acc -= aprs_mc[d] / 365.0
            path_dfs.append(math.exp(log_acc))
        all_dfs.append(path_dfs)

    # Percentile bands
    apr_arr = np.array(all_aprs)  # (N_SIMS, TENOR+1)
    df_arr = np.array(all_dfs)
    apr_p5 = np.percentile(apr_arr, 5, axis=0)
    apr_p50 = np.percentile(apr_arr, 50, axis=0)
    apr_p95 = np.percentile(apr_arr, 95, axis=0)
    df_p5 = np.percentile(df_arr, 5, axis=0)
    df_p50 = np.percentile(df_arr, 50, axis=0)
    df_p95 = np.percentile(df_arr, 95, axis=0)

    final_dfs = df_arr[:, -1]
    pvs = CASHFLOW * final_dfs
    print(f"── Monte Carlo ({N_SIMS} sims) ──")
    print(f"  DF p5/p50/p95: {np.percentile(final_dfs,5):.6f} / {np.median(final_dfs):.6f} / {np.percentile(final_dfs,95):.6f}")
    print(f"  PV p5/p50/p95: {np.percentile(pvs,5):.4f} / {np.median(pvs):.4f} / {np.percentile(pvs,95):.4f}")

    # ── Print day-by-day table (every 5 days + boundaries) ─────────────────
    print()
    print(f"── Day-by-Day Breakdown (single path, seed={SEED}) ──")
    print(f"{'Day':>4s}  {'Phase':>12s}  {'N_active':>10s}  {'APR':>8s}  {'Daily DF':>10s}  {'Cum DF':>10s}")
    print("-" * 68)
    show_days = sorted(set(
        list(range(0, TENOR + 1, 5)) +
        [queue_clear_day - 1, queue_clear_day, queue_clear_day + 1, TENOR]
    ))
    for d in show_days:
        if d < 0 or d > TENOR:
            continue
        phase = "Determ." if d <= queue_clear_day else "Stochastic"
        daily_df = math.exp(-det_aprs[d] / 365.0) if d > 0 else 1.0
        print(f"{d:>4d}  {phase:>12s}  {det_actives[d]:>10,.0f}  "
              f"{det_aprs[d]*100:>7.4f}%  {daily_df:>10.8f}  {cum_df[d]:>10.8f}")

    # ══════════════════════════════════════════════════════════════════════
    # PLOTTING
    # ══════════════════════════════════════════════════════════════════════

    days = np.arange(TENOR + 1)

    fig, axes = plt.subplots(4, 1, figsize=(14, 18))

    pal = sns.color_palette()
    det_color = pal[0]   # blue
    stoch_color = pal[1]  # orange
    band_color = pal[2]   # green

    # ── Panel 1: Active Validator Count ────────────────────────────────────
    ax = axes[0]
    # Deterministic phase
    det_days = days[:queue_clear_day + 1]
    stoch_days = days[queue_clear_day:]
    ax.plot(det_days, np.array(det_actives[:queue_clear_day + 1]) / 1e3,
            color=det_color, linewidth=2, label="Deterministic (queue unwind)")
    # Stochastic phase — show MC bands
    stoch_active_arr = np.array([
        [all_aprs[s][d] for d in range(queue_clear_day, TENOR + 1)]
        for s in range(N_SIMS)
    ])
    # Actually need actives, not APRs. Recompute from APR → active
    # Simpler: just show the single path + note MC
    ax.plot(stoch_days, np.array(det_actives[queue_clear_day:]) / 1e3,
            color=stoch_color, linewidth=2, label="Stochastic (single path)")
    ax.axvline(queue_clear_day, color="gray", linestyle="--", linewidth=1.5, alpha=0.7)
    ax.set_ylabel("Active Validators (thousands)")
    ax.set_title("Validator Count Projection")
    ax.legend(loc="lower right")
    # Phase labels on background
    ax.text(queue_clear_day * 0.4, ax.get_ylim()[1] * 0.99,
            "DETERMINISTIC\n(queue unwind)", ha="center", va="top",
            fontsize=9, color=det_color, alpha=0.6, fontweight="bold")
    ax.text(queue_clear_day + (TENOR - queue_clear_day) * 0.5, ax.get_ylim()[1] * 0.99,
            "STOCHASTIC\n(diffusion)", ha="center", va="top",
            fontsize=9, color=stoch_color, alpha=0.6, fontweight="bold")

    # ── Panel 2: APR over time with phase coloring ─────────────────────────
    ax = axes[1]
    apr_pct = np.array(det_aprs) * 100

    # Background shading for phases
    ax.axvspan(0, queue_clear_day, alpha=0.08, color=det_color, label="_")
    ax.axvspan(queue_clear_day, TENOR, alpha=0.08, color=stoch_color, label="_")

    # MC bands (stochastic phase only)
    ax.fill_between(days[queue_clear_day:],
                    apr_p5[queue_clear_day:] * 100,
                    apr_p95[queue_clear_day:] * 100,
                    alpha=0.2, color=band_color, label="5th–95th pctile (MC)")

    # Single path
    ax.plot(det_days, apr_pct[:queue_clear_day + 1],
            color=det_color, linewidth=2, label="Deterministic APR")
    ax.plot(stoch_days, apr_pct[queue_clear_day:],
            color=stoch_color, linewidth=2, label="Stochastic APR (single path)")
    ax.plot(days, apr_p50 * 100, "--", color="gray", linewidth=1, alpha=0.6,
            label="Median APR (MC)")

    ax.axvline(queue_clear_day, color="gray", linestyle="--", linewidth=1.5, alpha=0.7)
    ax.set_ylabel("APR (%)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.3f}%"))
    ax.set_title(f"Epoch-by-Epoch APR Applied to Discount (p={P})")
    ax.legend(loc="upper right", fontsize=9)

    # Annotate key APR values
    ax.annotate(f"Day 0: {apr_pct[0]:.3f}%",
                xy=(0, apr_pct[0]), xytext=(3, apr_pct[0] + 0.01),
                fontsize=9, color=det_color)
    ax.annotate(f"Day {queue_clear_day}: {apr_pct[queue_clear_day]:.3f}%",
                xy=(queue_clear_day, apr_pct[queue_clear_day]),
                xytext=(queue_clear_day + 3, apr_pct[queue_clear_day] + 0.01),
                fontsize=9, color=det_color)
    ax.annotate(f"Day {TENOR}: {apr_pct[-1]:.3f}%",
                xy=(TENOR, apr_pct[-1]),
                xytext=(TENOR - 25, apr_pct[-1] + 0.02),
                fontsize=9, color=stoch_color,
                arrowprops=dict(arrowstyle="->", color=stoch_color, lw=0.8))

    # ── Panel 3: Cumulative Discount Factor ────────────────────────────────
    ax = axes[2]

    # Background shading
    ax.axvspan(0, queue_clear_day, alpha=0.08, color=det_color)
    ax.axvspan(queue_clear_day, TENOR, alpha=0.08, color=stoch_color)

    # MC bands
    ax.fill_between(days, df_p5, df_p95, alpha=0.15, color=band_color,
                    label="5th–95th pctile (MC)")

    # Single path
    ax.plot(det_days, cum_df[:queue_clear_day + 1],
            color=det_color, linewidth=2, label="Deterministic phase")
    ax.plot(stoch_days, cum_df[queue_clear_day:],
            color=stoch_color, linewidth=2, label="Stochastic phase")
    ax.plot(days, df_p50, "--", color="gray", linewidth=1, alpha=0.6,
            label="Median DF (MC)")

    ax.axvline(queue_clear_day, color="gray", linestyle="--", linewidth=1.5, alpha=0.7)
    ax.set_ylabel("Discount Factor P(0, d)")
    ax.set_title("Cumulative Discount Factor — How 1 ETH Depreciates Over Time")
    ax.legend(loc="lower left", fontsize=9)

    # Annotate key points
    ax.annotate(f"P(0, 0) = 1.000000",
                xy=(0, 1.0), xytext=(3, 0.9995),
                fontsize=9, color="black")
    ax.annotate(f"P(0, {queue_clear_day}) = {cum_df[queue_clear_day]:.6f}",
                xy=(queue_clear_day, cum_df[queue_clear_day]),
                xytext=(queue_clear_day + 3, cum_df[queue_clear_day] + 0.0003),
                fontsize=9, arrowprops=dict(arrowstyle="->", color="gray"),
                color=det_color)
    ax.annotate(f"P(0, {TENOR}) = {cum_df[TENOR]:.6f}\n"
                f"→ PV = {CASHFLOW} × {cum_df[TENOR]:.6f} = {final_pv:.4f} ETH",
                xy=(TENOR, cum_df[TENOR]),
                xytext=(TENOR - 35, cum_df[TENOR] + 0.001),
                fontsize=10, fontweight="bold",
                arrowprops=dict(arrowstyle="->", color="red", lw=1.5),
                color="red",
                bbox=dict(boxstyle="round,pad=0.3", facecolor="lightyellow", edgecolor="red", alpha=0.9))

    # ── Panel 4: PV Distribution (MC) ──────────────────────────────────────
    ax = axes[3]
    sns.histplot(pvs, bins=50, kde=True, ax=ax, color=pal[4], alpha=0.6)
    ax.axvline(np.median(pvs), color="red", linestyle="--", linewidth=2,
               label=f"Median: {np.median(pvs):.4f} ETH")
    ax.axvline(np.percentile(pvs, 5), color="gray", linestyle=":",
               label=f"5th pctile: {np.percentile(pvs,5):.4f}")
    ax.axvline(np.percentile(pvs, 95), color="gray", linestyle=":",
               label=f"95th pctile: {np.percentile(pvs,95):.4f}")
    ax.set_xlabel("Present Value (ETH)")
    ax.set_title(f"Distribution of PV({CASHFLOW} ETH at day {TENOR}) — {N_SIMS} Simulations")
    ax.xaxis.get_major_formatter().set_useOffset(False)
    ax.ticklabel_format(style="plain", axis="x")
    ax.legend(fontsize=9)

    # Add text box with summary
    textstr = (
        f"Cash flow: {CASHFLOW:.0f} ETH at day {TENOR}\n"
        f"Queue clear: day {queue_clear_day}\n"
        f"Deterministic: days 0–{queue_clear_day} (N grows {last_active:,}→{det_active[queue_clear_day]:,})\n"
        f"Stochastic: days {queue_clear_day}–{TENOR} (μ={DRIFT:+.0f}, σ={VOL:.0f} val/day)\n"
        f"Participation: p={P}\n"
        f"Median DF: {np.median(final_dfs):.6f}  →  Median PV: {np.median(pvs):.4f} ETH\n"
        f"Zero rate: {-math.log(np.median(final_dfs))/(TENOR/365)*100:.4f}%"
    )
    props = dict(boxstyle="round", facecolor="white", alpha=0.9, edgecolor="gray")
    ax.text(0.98, 0.95, textstr, transform=ax.transAxes, fontsize=9,
            verticalalignment="top", horizontalalignment="right", bbox=props,
            family="monospace")

    fig.suptitle(
        f"Worked Example — PV of {CASHFLOW:.0f} ETH at Day {TENOR}\n"
        f"(Deterministic Queue Unwind + Stochastic Diffusion)",
        fontsize=14, y=1.01, fontweight="bold")
    plt.tight_layout()
    fig.savefig(PLOTS / "worked_example_90d.png")
    plt.close(fig)
    print()
    print(f"Plot saved to {PLOTS / 'worked_example_90d.png'}")


if __name__ == "__main__":
    main()
