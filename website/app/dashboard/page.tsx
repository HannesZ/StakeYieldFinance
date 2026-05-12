"use client";

import { useAccount, useReadContract } from "wagmi";
import { formatEther } from "viem";
import { ConnectButton } from "@/components/ConnectButton";
import { PositionCard, type Position } from "@/components/PositionCard";
import { DEMO_SERIES, SERIES_2026Q4_ID, timeUntil } from "@/lib/utils";
import { ADDRESSES, SY_LST_ABI, STABLE_YIELD_VAULT_ABI, ERC20_ABI } from "@/lib/contracts";
import { useSeries } from "@/hooks/useSeries";
import { useActivityHistory } from "@/hooks/useActivityHistory";
import { ActivityHistory } from "@/components/ActivityHistory";
import { useEffect, useState } from "react";

const addresses = ADDRESSES.hoodi;
const series = DEMO_SERIES[0]; // 2026Q4

export default function DashboardPage() {
  const { address, isConnected } = useAccount();
  const seriesInfo = useSeries();

  // Read syLST balance for 2026Q4
  const tokenId = BigInt(SERIES_2026Q4_ID);
  const { data: syLstBalance } = useReadContract({
    address: addresses.syLST,
    abi: SY_LST_ABI,
    functionName: "balanceOf",
    args: address ? [address, tokenId] : undefined,
    query: { enabled: !!address },
  });

  // Read wstETH balance
  const { data: wstETHBalance } = useReadContract({
    address: addresses.wstETH,
    abi: ERC20_ABI,
    functionName: "balanceOf",
    args: address ? [address] : undefined,
    query: { enabled: !!address },
  });

  // Read series data from vault
  const { data: seriesData } = useReadContract({
    address: addresses.stableYieldVault,
    abi: STABLE_YIELD_VAULT_ABI,
    functionName: "getSeries",
    args: [SERIES_2026Q4_ID],
  });

  // Read per-deposit records to compute accrued interest
  const { data: deposits } = useReadContract({
    address: addresses.stableYieldVault,
    abi: STABLE_YIELD_VAULT_ABI,
    functionName: "getDeposits",
    args: address ? [SERIES_2026Q4_ID, address] : undefined,
    query: { enabled: !!address },
  });

  // Live-updating accrued interest (recalculates every second)
  const [accruedInterest, setAccruedInterest] = useState(0);
  useEffect(() => {
    if (!deposits || deposits.length === 0) {
      setAccruedInterest(0);
      return;
    }
    const tick = () => {
      const nowSec = Math.floor(Date.now() / 1000);
      const SECONDS_PER_YEAR = 365 * 86400;
      let total = 0;
      for (const d of deposits) {
        const principal = Number(formatEther(d.wstEthAmount));
        const rate = Number(d.fixedRateE18) / 1e18; // annualised rate as decimal
        const elapsed = nowSec - Number(d.depositTimestamp);
        if (elapsed > 0) {
          total += principal * rate * elapsed / SECONDS_PER_YEAR;
        }
      }
      setAccruedInterest(total);
    };
    tick();
    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, [deposits]);

  // Activity history from on-chain events
  const { activities, isLoading: activitiesLoading } = useActivityHistory(address);

  const hasPosition = syLstBalance !== undefined && syLstBalance > BigInt(0);
  const remaining = timeUntil(series.maturity);

  const userPosition: Position | null = hasPosition ? (() => {
    const bal = Number(formatEther(syLstBalance!));
    const tenorYears = (series.maturity - Math.floor(Date.now() / 1000)) / (365 * 86400);
    const payout = bal * (1 + (seriesInfo.fixedRate / 100) * tenorYears);
    return {
      seriesId: series.seriesId,
      seriesLabel: series.id,
      balance: bal,
      fixedRate: seriesInfo.fixedRate,
      maturity: series.maturity,
      accruedInterest,
      claimAtMaturity: payout,
    };
  })() : null;

  if (!isConnected) {
    return (
      <div className="flex min-h-[60vh] flex-col items-center justify-center text-center">
        <div className="mb-6 text-6xl">📊</div>
        <h1 className="mb-3 text-3xl font-bold text-white">Your Dashboard</h1>
        <p className="mb-8 max-w-md text-slate-400">
          Connect your wallet to view your fixed-rate positions.
        </p>
        <ConnectButton />
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-4xl space-y-8">
      <div>
        <h1 className="text-3xl font-bold text-white">Dashboard</h1>
        <p className="mt-2 text-slate-400">Your StakeYield positions and balances.</p>
      </div>

      {/* Wallet Overview */}
      <div className="grid gap-4 sm:grid-cols-2">
        <div className="rounded-xl border border-white/10 bg-white/[0.02] p-5">
          <div className="text-sm text-slate-400">wstETH Balance</div>
          <div className="mt-1 text-2xl font-bold font-mono text-white">
            {wstETHBalance !== undefined ? Number(formatEther(wstETHBalance)).toFixed(6) : "—"}
          </div>
        </div>
        <div className="rounded-xl border border-white/10 bg-white/[0.02] p-5">
          <div className="text-sm text-slate-400">syLST Balance (2026Q4)</div>
          <div className="mt-1 text-2xl font-bold font-mono text-[#4EC9B0]">
            {syLstBalance !== undefined ? Number(formatEther(syLstBalance)).toFixed(6) : "—"}
          </div>
        </div>
      </div>

      {/* Positions */}
      <div>
        <h2 className="mb-4 text-xl font-semibold text-white">Your Positions</h2>
        {userPosition ? (
          <PositionCard position={userPosition} />
        ) : (
          <div className="rounded-2xl border border-white/10 bg-white/[0.02] p-8 text-center">
            <div className="mb-3 text-4xl">🏦</div>
            <p className="text-slate-400">No positions yet.</p>
            <a
              href="/deposit"
              className="mt-4 inline-block rounded-lg bg-gradient-to-r from-[#637DEA] to-[#4EC9B0] px-6 py-2 text-sm font-semibold text-white transition hover:shadow-lg"
            >
              Make Your First Deposit →
            </a>
          </div>
        )}
      </div>

      {/* Series Info */}
      <div>
        <h2 className="mb-4 text-xl font-semibold text-white">Active Series</h2>
        <div className="rounded-xl border border-white/10 bg-white/[0.02] p-5">
          <div className="flex items-center justify-between">
            <div>
              <div className="text-lg font-semibold text-white">2026Q4</div>
              <div className="text-sm text-slate-400">
                Maturity: December 31, 2026
              </div>
            </div>
            <div className="text-right">
              <div className="text-2xl font-bold text-[#4EC9B0]">{seriesInfo.fixedRate.toFixed(2)}%</div>
              <div className="text-xs text-slate-400">Fixed APR</div>
            </div>
          </div>
          <div className="mt-4 grid grid-cols-3 gap-4 border-t border-white/10 pt-4 text-center">
            <div>
              <div className="text-xs text-slate-400">Total Deposited</div>
              <div className="mt-1 font-mono text-sm text-white">
                {seriesData ? Number(formatEther(seriesData.totalDeposited)).toFixed(4) : "—"} wstETH
              </div>
            </div>
            <div>
              <div className="text-xs text-slate-400">Time Remaining</div>
              <div className="mt-1 font-mono text-sm text-white">
                {remaining.isPast ? "Matured" : `${remaining.days}d ${remaining.hours}h`}
              </div>
            </div>
            <div>
              <div className="text-xs text-slate-400">Status</div>
              <div className="mt-1 text-sm font-semibold text-[#4EC9B0]">
                {seriesData?.isOpen ? "Open" : "Closed"}
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Activity History */}
      <div>
        <h2 className="mb-4 text-xl font-semibold text-white">Activity History</h2>
        <ActivityHistory activities={activities} isLoading={activitiesLoading} />
      </div>
    </div>
  );
}
