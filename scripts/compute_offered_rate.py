#!/usr/bin/env python3
"""Compute the model-derived offered fixed rate for each active series.

Pipeline:
  1. Calibrate jump-diffusion parameters from latest history.csv
  2. For each series tenor, run Monte Carlo yield curve
  3. Compute the weighted-average projected APR (p50) over the tenor
  4. Output the result as JSON for the on-chain push script

Also outputs the current spot staking APR from latest data.

Usage:
  python3 scripts/compute_offered_rate.py
  python3 scripts/compute_offered_rate.py --series 2026Q4 --maturity 2026-12-31
  python3 scripts/compute_offered_rate.py --sims 5000 --window 90
"""

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path for imports
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from calibrate import calibrate, load_daily, DATA
from yield_curve_cli import (
    load_last_state, build_yield_paths, apr_from_active_eth,
    EPOCHS_PER_YEAR, BASE_REWARD_FACTOR
)

OUTPUT_FILE = ROOT / "data" / "model_rate_output.json"


def compute_spot_apr() -> float:
    """Current spot staking APR from latest active_eth in history.csv."""
    st = load_last_state(DATA)
    return apr_from_active_eth(st["active_eth"])


def compute_projected_apr(term_days: int, params: dict, sims: int = 2000) -> dict:
    """Run Monte Carlo, return projected APR statistics over the tenor.
    
    Returns the time-weighted average APR across the tenor for each percentile.
    This is what a depositor locking in for `term_days` would experience on average.
    """
    paths, q_day = build_yield_paths(
        term_days=term_days,
        n_sims=sims,
        drift_per_day=params["drift_per_day"],
        vol_per_sqrt_day=params["vol_per_sqrt_day"],
        jump_lambda=params["jump_lambda"],
        jump_mu=params["jump_mu"],
        jump_sigma=params["jump_sigma"],
        exit_drift=params["exit_drift"],
        exit_vol=params["exit_vol"],
    )

    # For each simulation path, compute the time-weighted average APR over the tenor
    avg_aprs = []
    for path in paths:
        # path[0] = today, path[1] = day 1, ..., path[term_days] = maturity
        # Time-weighted average: simple mean of daily APRs
        daily_aprs = path[1:term_days + 1] if len(path) > term_days else path[1:]
        if daily_aprs:
            avg_aprs.append(sum(daily_aprs) / len(daily_aprs))

    avg_aprs.sort()
    n = len(avg_aprs)

    return {
        "p5":  avg_aprs[int(0.05 * (n - 1))],
        "p25": avg_aprs[int(0.25 * (n - 1))],
        "p50": avg_aprs[n // 2],
        "p75": avg_aprs[int(0.75 * (n - 1))],
        "p95": avg_aprs[int(0.95 * (n - 1))],
        "mean": sum(avg_aprs) / n,
        "queue_clear_day": q_day,
        "term_days": term_days,
        "n_sims": sims,
    }


def main():
    ap = argparse.ArgumentParser(description="Compute model-derived offered rate")
    ap.add_argument("--series", type=str, default="2026Q4", help="Series label")
    ap.add_argument("--maturity", type=str, default="2026-12-31",
                    help="Maturity date (YYYY-MM-DD)")
    ap.add_argument("--sims", type=int, default=2000, help="Monte Carlo simulations")
    ap.add_argument("--window", type=int, default=90, help="Calibration window (days)")
    ap.add_argument("--output", type=str, default=str(OUTPUT_FILE), help="Output JSON path")
    args = ap.parse_args()

    print("=" * 60)
    print("  StakeYield Finance — Model Rate Computation")
    print("=" * 60)

    # 1. Current spot APR
    spot_apr = compute_spot_apr()
    print(f"\n📊 Current spot staking APR: {spot_apr:.4%}")
    print(f"   (from latest active_eth in history.csv)")

    # 2. Calibrate parameters
    print(f"\n🔧 Calibrating from {args.window}-day window...")
    params = calibrate(args.window)

    # 3. Compute tenor
    maturity_dt = datetime.strptime(args.maturity, "%Y-%m-%d").replace(
        tzinfo=timezone.utc
    )
    now = datetime.now(timezone.utc)
    term_days = max(1, (maturity_dt - now).days)
    print(f"\n📅 Series {args.series}: {term_days} days to maturity ({args.maturity})")

    # 4. Run yield curve projection
    print(f"\n🎲 Running {args.sims} Monte Carlo simulations...")
    projection = compute_projected_apr(term_days, params, args.sims)

    print(f"\n📈 Projected average staking APR over {term_days}-day tenor:")
    print(f"   p5:   {projection['p5']:.4%}")
    print(f"   p25:  {projection['p25']:.4%}")
    print(f"   p50:  {projection['p50']:.4%}  ← used for pricing")
    print(f"   p75:  {projection['p75']:.4%}")
    print(f"   p95:  {projection['p95']:.4%}")
    print(f"   mean: {projection['mean']:.4%}")
    print(f"   Queue clear day: {projection['queue_clear_day']}")

    # 5. The projected APR (p50) is what we push to the contract as stakingAPR.
    #    The contract then computes: offeredRate = stakingAPR - spread(κ)
    model_apr = projection["p50"]

    # Convert to 1e18-scaled uint256 for Solidity
    # 2.85% = 0.0285 → 0.0285 * 1e18 = 28500000000000000
    model_apr_e18 = int(model_apr * 1e18)

    print(f"\n✅ Model output:")
    print(f"   stakingAPR (for contract): {model_apr:.4%}  ({model_apr_e18} wei)")
    print(f"   spot APR (for display):    {spot_apr:.4%}")

    # 6. Write output
    output = {
        "timestamp": now.isoformat(),
        "series": args.series,
        "maturity": args.maturity,
        "term_days": term_days,
        "calibration_window_days": args.window,
        "calibration_data_range": params["data_range"],
        "current_active_eth": params["current_active_eth"],
        "spot_apr": round(spot_apr, 6),
        "spot_apr_pct": round(spot_apr * 100, 4),
        "model_projected_apr": round(model_apr, 6),
        "model_projected_apr_pct": round(model_apr * 100, 4),
        "model_projected_apr_e18": str(model_apr_e18),
        "projection_percentiles": {
            k: round(v, 6) if isinstance(v, float) else v
            for k, v in projection.items()
        },
        "calibrated_params": {
            k: v for k, v in params.items()
            if k not in ("data_range", "current_active_eth")
        },
        "n_sims": args.sims,
    }

    output_path = Path(args.output)
    output_path.write_text(json.dumps(output, indent=2) + "\n")
    print(f"\n💾 Saved to {output_path}")

    return output


if __name__ == "__main__":
    main()
