"use client";

import { timeUntil } from "@/lib/utils";

export interface Position {
  seriesId: string;
  seriesLabel: string;
  balance: number; // syLST balance (= wstETH deposited)
  fixedRate: number; // annualized %
  maturity: number; // unix timestamp
  accruedInterest: number; // stETH (≈ ETH)
  claimAtMaturity: number; // stETH total payout (≈ ETH)
  /** ETH-equivalent of the original wstETH deposit (via stEthPerToken at deposit time) */
  ethDeposited: number;
}

interface PositionCardProps {
  position: Position;
  /** Current wstETH→ETH rate for display */
  exchangeRate: number;
  /** Decimal places for display (testnet precision toggle) */
  decimals?: number;
  onRedeem?: () => void;
}

export function PositionCard({ position, exchangeRate, decimals: d = 4, onRedeem }: PositionCardProps) {
  const t = timeUntil(position.maturity);
  const maturityDate = new Date(position.maturity * 1000).toLocaleDateString(
    "en-US",
    { year: "numeric", month: "short", day: "numeric" }
  );

  // ETH-equivalent figures (stETH ≈ ETH)
  const ethDeposited = position.ethDeposited;
  const ethClaim = position.claimAtMaturity; // already in stETH ≈ ETH
  const ethYield = ethClaim - ethDeposited;
  const yieldPct = ethDeposited > 0 ? (ethYield / ethDeposited) * 100 : 0;

  return (
    <div className="rounded-2xl border border-white/10 bg-gradient-to-br from-white/[0.04] to-white/[0.01] p-6 transition hover:border-white/15">
      {/* Header */}
      <div className="mb-5 flex items-center justify-between">
        <div>
          <h3 className="text-lg font-semibold text-white">
            {position.seriesLabel}
          </h3>
          <p className="text-xs text-slate-400">Matures {maturityDate}</p>
        </div>
        <div className="text-right">
          <div className="text-2xl font-bold text-[#4EC9B0]">
            {position.fixedRate.toFixed(2)}%
          </div>
          <div className="text-xs text-slate-400">Fixed APR</div>
        </div>
      </div>

      {/* Main ETH-equivalent stats */}
      <div className="mb-4 grid grid-cols-3 gap-3">
        <div className="rounded-xl bg-white/[0.03] p-3">
          <div className="text-xs text-slate-400">You Deposited</div>
          <div className="mt-1 font-mono text-sm font-semibold text-white">
            ≈ {ethDeposited.toFixed(d)} ETH
          </div>
          <div className="mt-0.5 font-mono text-xs text-slate-500">
            {position.balance.toFixed(d)} wstETH
          </div>
        </div>
        <div className="rounded-xl bg-white/[0.03] p-3">
          <div className="text-xs text-slate-400">You&apos;ll Receive</div>
          <div className="mt-1 font-mono text-sm font-semibold text-white">
            ≈ {ethClaim.toFixed(d)} ETH
          </div>
          <div className="mt-0.5 font-mono text-xs text-slate-500">
            {position.claimAtMaturity.toFixed(d)} stETH
          </div>
        </div>
        <div className="rounded-xl bg-white/[0.03] p-3">
          <div className="text-xs text-slate-400">Your Yield</div>
          <div className="mt-1 font-mono text-sm font-semibold text-[#4EC9B0]">
            ≈ +{ethYield.toFixed(d)} ETH
          </div>
          <div className="mt-0.5 font-mono text-xs text-[#4EC9B0]/60">
            +{yieldPct.toFixed(2)}%
          </div>
        </div>
      </div>

      {/* Secondary stats */}
      <div className="mb-4 grid grid-cols-2 gap-3">
        <div className="rounded-xl bg-white/[0.03] p-3">
          <div className="text-xs text-slate-400">Accrued So Far</div>
          <div className="mt-0.5 font-mono text-sm font-medium text-[#4EC9B0]">
            ≈ +{position.accruedInterest.toFixed(d)} ETH
          </div>
        </div>
        <div className="rounded-xl bg-white/[0.03] p-3">
          <div className="text-xs text-slate-400">Time Remaining</div>
          <div className="mt-0.5 text-sm font-medium text-white">
            {t.isPast ? (
              <span className="text-amber-400">Matured ✓</span>
            ) : (
              `${t.days}d ${t.hours}h ${t.minutes}m`
            )}
          </div>
        </div>
      </div>

      {/* Countdown bar */}
      {!t.isPast && (
        <div className="mb-4">
          <div className="h-1.5 w-full overflow-hidden rounded-full bg-white/5">
            <div
              className="h-full rounded-full bg-gradient-to-r from-[#637DEA] to-[#4EC9B0] transition-all duration-1000"
              style={{
                width: `${Math.max(5, 100 - (t.days / 365) * 100)}%`,
              }}
            />
          </div>
        </div>
      )}

      {/* Redeem button */}
      <button
        onClick={onRedeem}
        disabled={!t.isPast}
        className={`w-full rounded-xl py-2.5 text-sm font-semibold transition ${
          t.isPast
            ? "bg-[#4EC9B0] text-[#0F172A] hover:bg-[#3db89f] hover:shadow-lg hover:shadow-[#4EC9B0]/20"
            : "cursor-not-allowed bg-white/5 text-slate-500"
        }`}
      >
        {t.isPast ? "Redeem" : `Redeemable in ${t.days}d`}
      </button>
    </div>
  );
}
