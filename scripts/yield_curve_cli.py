#!/usr/bin/env python3
"""Deterministic + stochastic validator-based yield curve and pricing CLI.

Usage examples:
  python3 scripts/yield_curve_cli.py curve --term-years 3
  python3 scripts/yield_curve_cli.py pv --term-years 2 --cashflow 100000
  python3 scripts/yield_curve_cli.py pv --schedule cashflows.csv
"""

import argparse
import csv
import math
import random
from pathlib import Path

DATA = Path(__file__).resolve().parents[1] / "data" / "history.csv"

# Beacon constants
SECONDS_PER_EPOCH = 384
EPOCHS_PER_DAY = 225
EPOCHS_PER_YEAR = 82125
BASE_REWARD_FACTOR = 64  # EIP-7251 / post-Pectra


def load_last_state(history_csv: Path):
    """Read just the header and last line of history.csv (fast, no full scan)."""
    import subprocess
    header_line = subprocess.check_output(["head", "-1", str(history_csv)], text=True).strip()
    last_line = subprocess.check_output(["tail", "-1", str(history_csv)], text=True).strip()
    if not last_line or last_line == header_line:
        raise RuntimeError("history.csv is empty")
    headers = header_line.split(",")
    values = last_line.split(",")
    last = dict(zip(headers, values))
    return {
        "slot": int(last["slot"]),
        "active_eth": float(last.get("active_eth") or 0),
        "entry_eth": float(last.get("entry_eth") or 0),
        "exit_eth": float(last.get("exit_eth") or 0),
        "pending_eth": float(last.get("pending_deposits_eth") or 0),
        # keep count for reference
        "active": int(last["active_count"]),
    }


def eth_churn_per_epoch(active_eth: float) -> float:
    """Post-Pectra ETH-denominated churn limit per epoch.
    EIP-7251: MIN_PER_EPOCH_CHURN_LIMIT_ETH=128, CHURN_LIMIT_QUOTIENT=65536
    """
    return max(128.0, active_eth / 65536.0)


def deterministic_daily_active_eth(active_eth, entry_eth, exit_eth, pending_eth, days):
    """Deterministic queue unwind in ETH terms. Returns active_eth per day."""
    out = [active_eth]
    ac, eq, xq, pq = active_eth, entry_eth, exit_eth, pending_eth
    for _ in range(days):
        churn = eth_churn_per_epoch(ac)
        for _ep in range(EPOCHS_PER_DAY):
            # Drain exit queue
            drained = min(churn, xq)
            xq -= drained
            ac -= drained
            # Activate from entry + pending
            can_activate = min(churn, eq + pq)
            from_eq = min(can_activate, eq)
            from_pq = can_activate - from_eq
            eq -= from_eq
            pq -= from_pq
            ac += can_activate
        out.append(ac)
    return out, eq, xq, pq


def apr_from_active_eth(active_eth: float, el_premium: float = 0.0013) -> float:
    """Protocol-derived APR from total active ETH (consensus layer only).
    Formula: APR = BASE_REWARD_FACTOR * EPOCHS_PER_YEAR / sqrt(active_eth * 1e9)
    el_premium: execution layer rewards on top (MEV/tips), default 0.5%
    """
    cl_apr = BASE_REWARD_FACTOR * EPOCHS_PER_YEAR / math.sqrt(max(active_eth, 1.0) * 1e9)
    return cl_apr + el_premium


def simulate_day_eth(active_eth, pending_eth, entry_eth, exit_eth,
                     deposit_drift, deposit_vol,
                     jump_lambda, jump_mu, jump_sigma,
                     exit_drift, exit_vol, rng):
    """Simulate one day in ETH terms: new deposits + exits, then queue mechanics."""
    # --- New deposits into pending queue (diffusion + Poisson jumps, in ETH) ---
    z_dep = rng.gauss(0.0, 1.0)
    new_deposits = max(0.0, deposit_drift + deposit_vol * z_dep)

    n_jumps = 0
    u = rng.random()
    p_acc = math.exp(-jump_lambda)
    if u > p_acc:
        n_jumps = 1
        p_acc += jump_lambda * math.exp(-jump_lambda)
        if u > p_acc:
            n_jumps = 2
    for _ in range(n_jumps):
        new_deposits += max(0.0, rng.gauss(jump_mu, jump_sigma))

    pending_eth += new_deposits

    # --- New exits entering exit queue (ETH) ---
    z_exit = rng.gauss(0.0, 1.0)
    new_exits = max(0.0, exit_drift + exit_vol * z_exit)
    exit_eth += new_exits

    # --- Epoch-by-epoch queue mechanics (ETH churn) ---
    churn = eth_churn_per_epoch(active_eth)
    for _ in range(EPOCHS_PER_DAY):
        drained = min(churn, exit_eth)
        exit_eth -= drained
        active_eth -= drained
        can_activate = min(churn, entry_eth + pending_eth)
        from_entry = min(can_activate, entry_eth)
        entry_eth -= from_entry
        pending_eth -= (can_activate - from_entry)
        active_eth += can_activate

    active_eth = max(1.0, active_eth)
    return active_eth, pending_eth, entry_eth, exit_eth


def build_yield_paths(term_days, n_sims, drift_per_day, vol_per_sqrt_day,
                      jump_lambda=0.02, jump_mu=50000, jump_sigma=10000,
                      exit_drift=5000, exit_vol=10000, seed=42):
    """Build Monte Carlo APR paths in ETH terms."""
    st = load_last_state(DATA)
    det_active_eth, eq, xq, pq = deterministic_daily_active_eth(
        st["active_eth"], st["entry_eth"], st["exit_eth"], st["pending_eth"], term_days
    )

    # Find queue-clear day from deterministic phase
    queue_clear_day = 0
    if st["entry_eth"] + st["exit_eth"] + st["pending_eth"] > 0:
        ac, e, x, p = st["active_eth"], st["entry_eth"], st["exit_eth"], st["pending_eth"]
        for d in range(term_days + 1):
            if e <= 0 and x <= 0 and p <= 0:
                queue_clear_day = d
                break
            churn = eth_churn_per_epoch(ac)
            for _ in range(EPOCHS_PER_DAY):
                x = max(0.0, x - churn)
                can = min(churn, e + p)
                te = min(can, e); e -= te; p -= (can - te)
                ac += can
        else:
            queue_clear_day = term_days

    rng = random.Random(seed)
    paths_apr = []
    for _ in range(n_sims):
        aprs = []
        # Deterministic phase: drain existing queues
        eth_sim = det_active_eth[min(queue_clear_day, term_days)]
        pq_sim = 0.0
        eq_sim = 0.0
        xq_sim = 0.0
        for d in range(term_days + 1):
            if d <= queue_clear_day:
                aprs.append(apr_from_active_eth(det_active_eth[d]))
            else:
                eth_sim, pq_sim, eq_sim, xq_sim = simulate_day_eth(
                    eth_sim, pq_sim, eq_sim, xq_sim,
                    drift_per_day, vol_per_sqrt_day,
                    jump_lambda, jump_mu, jump_sigma,
                    exit_drift, exit_vol, rng
                )
                aprs.append(apr_from_active_eth(eth_sim))
        paths_apr.append(aprs)
    return paths_apr, queue_clear_day


def discount_factor_from_apr_path(aprs, term_days):
    acc = 0.0
    for d in range(1, term_days + 1):
        r = aprs[d]
        acc += r / 365.0
    return math.exp(-acc)


def price_single_cashflow(cashflow, term_days, paths_apr):
    dfs = [discount_factor_from_apr_path(p, term_days) for p in paths_apr]
    pvs = [cashflow * df for df in dfs]
    dfs.sort(); pvs.sort()
    mid = len(dfs) // 2
    return {
        "df_p5": dfs[int(0.05 * (len(dfs)-1))],
        "df_p50": dfs[mid],
        "df_p95": dfs[int(0.95 * (len(dfs)-1))],
        "pv_p5": pvs[int(0.05 * (len(pvs)-1))],
        "pv_p50": pvs[mid],
        "pv_p95": pvs[int(0.95 * (len(pvs)-1))],
    }


def price_schedule(schedule_csv, paths_apr):
    rows = []
    with open(schedule_csv, newline="") as f:
        for r in csv.DictReader(f):
            rows.append((int(r["day"]), float(r["cashflow"])))

    totals = []
    for p in paths_apr:
        pv = 0.0
        for day, cf in rows:
            pv += cf * discount_factor_from_apr_path(p, day)
        totals.append(pv)
    totals.sort()
    n = len(totals)
    return {
        "pv_p5": totals[int(0.05 * (n-1))],
        "pv_p50": totals[n // 2],
        "pv_p95": totals[int(0.95 * (n-1))],
    }


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    base = argparse.ArgumentParser(add_help=False)
    base.add_argument("--term-years", type=float, default=2.0)
    base.add_argument("--sims", type=int, default=2000)
    base.add_argument("--drift-per-day", type=float, default=50000.0, help="mean daily new deposits to pending queue (ETH)")
    base.add_argument("--vol-per-sqrt-day", type=float, default=40000.0, help="daily deposit volatility (ETH)")
    base.add_argument("--jump-lambda", type=float, default=0.03, help="jump intensity (per day)")
    base.add_argument("--jump-mu", type=float, default=200000.0, help="mean jump size (ETH)")
    base.add_argument("--jump-sigma", type=float, default=50000.0, help="jump size std dev (ETH)")
    base.add_argument("--exit-drift", type=float, default=20000.0, help="mean daily new exits (ETH)")
    base.add_argument("--exit-vol", type=float, default=30000.0, help="daily exit volatility (ETH)")

    curve = sub.add_parser("curve", parents=[base])
    pv = sub.add_parser("pv", parents=[base])
    pv.add_argument("--cashflow", type=float)
    pv.add_argument("--schedule", type=str)

    args = ap.parse_args()
    term_days = max(1, int(round(args.term_years * 365)))

    paths, q_day = build_yield_paths(term_days, args.sims, args.drift_per_day, args.vol_per_sqrt_day,
                                     args.jump_lambda, args.jump_mu, args.jump_sigma,
                                     args.exit_drift, args.exit_vol)

    if args.cmd == "curve":
        tenors = [30, 90, 180, 365, 730, min(term_days, 1825)]
        tenors = sorted(set([t for t in tenors if t <= term_days]))
        print(f"queue_clear_day={q_day}")
        print("tenor_days,df_p50,zero_rate_p50")
        for t in tenors:
            dfs = sorted([discount_factor_from_apr_path(p, t) for p in paths])
            df = dfs[len(dfs)//2]
            zr = -math.log(df) / (t / 365.0)
            print(f"{t},{df:.6f},{zr:.6%}")

    elif args.cmd == "pv":
        if args.cashflow is None and not args.schedule:
            raise SystemExit("Provide --cashflow or --schedule")
        if args.cashflow is not None:
            out = price_single_cashflow(args.cashflow, term_days, paths)
        else:
            out = price_schedule(args.schedule, paths)
        print(f"queue_clear_day={q_day}")
        for k, v in out.items():
            print(f"{k}={v:.6f}")


if __name__ == "__main__":
    main()
