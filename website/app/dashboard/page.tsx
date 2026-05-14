"use client";

import { useAccount, useReadContract, useBalance } from "wagmi";
import { formatEther } from "viem";
import { ConnectButton } from "@/components/ConnectButton";
import { PositionCard, type Position } from "@/components/PositionCard";
import { DEMO_SERIES, SERIES_2026Q4_ID, timeUntil } from "@/lib/utils";
import { ADDRESSES, SY_LST_ABI, STABLE_YIELD_VAULT_ABI, ERC20_ABI } from "@/lib/contracts";
import { useSeries } from "@/hooks/useSeries";
import { useExchangeRate } from "@/hooks/useExchangeRate";
import { useActivityHistory } from "@/hooks/useActivityHistory";
import { ActivityHistory } from "@/components/ActivityHistory";
import { useEffect, useState } from "react";
import { usePrecision } from "@/hooks/usePrecision";
import { PrecisionToggle } from "@/components/PrecisionToggle";

const addresses = ADDRESSES.hoodi;
const series = DEMO_SERIES[0]; // 2026Q4

export default function DashboardPage() {
  const { address, isConnected } = useAccount();
  const seriesInfo = useSeries();
  const { stEthPerWstEth, toEth } = useExchangeRate();
  const { decimals: d, extended, isTestnet, toggle } = usePrecision();

  // Read native ETH balance
  const { data: ethBalanceData } = useBalance({
    address,
    query: { enabled: !!address },
  });

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

  // Read per-deposit records to compute accrued interest + ETH deposited
  const { data: deposits } = useReadContract({
    address: addresses.stableYieldVault,
    abi: STABLE_YIELD_VAULT_ABI,
    functionName: "getDeposits",
    args: address ? [SERIES_2026Q4_ID, address] : undefined,
    query: { enabled: !!address },
  });

  // Live-updating accrued interest in stETH ≈ ETH (recalculates every second)
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
        const stEthPrincipal = Number(formatEther(d.stEthValue));
        const rate = Number(d.fixedRateE18) / 1e18;
        const elapsed = nowSec - Number(d.depositTimestamp);
        if (elapsed > 0) {
          total += stEthPrincipal * rate * elapsed / SECONDS_PER_YEAR;
        }
      }
      setAccruedInterest(total);
    };
    tick();
    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, [deposits]);

  // Sum ETH deposited (stEthValue at deposit time) and total claim
  const ethDeposited = deposits
    ? deposits.reduce((sum, d) => sum + Number(formatEther(d.stEthValue)), 0)
    : 0;
  const totalClaimStEth = deposits
    ? deposits.reduce((sum, d) => sum + Number(formatEther(d.claimAtMaturityStEth)), 0)
    : 0;

  // Activity history from on-chain events
  const { activities, isLoading: activitiesLoading } = useActivityHistory(address);

  const hasPosition = syLstBalance !== undefined && syLstBalance > BigInt(0);
  const remaining = timeUntil(series.maturity);

  // Wallet balances in human-readable numbers
  const wstEthBal = wstETHBalance !== undefined ? Number(formatEther(wstETHBalance)) : undefined;
  const syLstBal = syLstBalance !== undefined ? Number(formatEther(syLstBalance)) : undefined;

  const userPosition: Position | null = hasPosition ? {
    seriesId: series.seriesId,
    seriesLabel: series.id,
    balance: syLstBal!,
    fixedRate: seriesInfo.fixedRate,
    maturity: series.maturity,
    accruedInterest,
    claimAtMaturity: totalClaimStEth,
    ethDeposited,
  } : null;

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

  // Total deposited (series level) in ETH
  const totalDepositedWstEth = seriesData
    ? Number(formatEther(seriesData.totalDeposited))
    : 0;

  return (
    <div className="mx-auto max-w-4xl space-y-8">
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-3xl font-bold text-white">Dashboard</h1>
          <p className="mt-2 text-slate-400">Your StakeYield positions and balances.</p>
        </div>
        {isTestnet && toggle && (
          <PrecisionToggle extended={extended} onToggle={toggle} />
        )}
      </div>

      {/* Wallet Overview — ETH-equivalent primary */}
      <div className="grid gap-4 sm:grid-cols-2">
        <div className="rounded-xl border border-white/10 bg-white/[0.02] p-5">
          <div className="text-sm text-slate-400">Wallet Balance</div>
          {(() => {
            const ethBal = ethBalanceData ? Number(formatEther(ethBalanceData.value)) : undefined;
            const wstEthInEth = wstEthBal !== undefined ? toEth(wstEthBal) : 0;
            const total = (ethBal ?? 0) + wstEthInEth;
            const hasAny = ethBal !== undefined || wstEthBal !== undefined;
            return (
              <>
                <div className="mt-1 text-2xl font-bold font-mono text-white">
                  {hasAny ? `${total.toFixed(d)} ETH` : "—"}
                </div>
                <div className="mt-0.5 space-y-0.5 font-mono text-xs text-slate-500">
                  {ethBal !== undefined && ethBal > 0 && (
                    <div>{ethBal.toFixed(d)} ETH</div>
                  )}
                  {wstEthBal !== undefined && wstEthBal > 0 && (
                    <div>{wstEthBal.toFixed(d)} wstETH (≈ {wstEthInEth.toFixed(d)} ETH)</div>
                  )}
                </div>
              </>
            );
          })()}
        </div>
        <div className="rounded-xl border border-white/10 bg-white/[0.02] p-5">
          <div className="text-sm text-slate-400">Locked in StakeYield</div>
          <div className="mt-1 text-2xl font-bold font-mono text-[#4EC9B0]">
            {syLstBal !== undefined && syLstBal > 0
              ? `≈ ${toEth(syLstBal).toFixed(d)} ETH`
              : syLstBal !== undefined ? `0.${'0'.repeat(d)} ETH` : "—"}
          </div>
          <div className="mt-0.5 font-mono text-xs text-slate-500">
            {syLstBal !== undefined ? `${syLstBal.toFixed(d)} syLST (2026Q4)` : ""}
          </div>
        </div>
      </div>

      {/* Positions */}
      <div>
        <h2 className="mb-4 text-xl font-semibold text-white">Your Positions</h2>
        {userPosition ? (
          <PositionCard position={userPosition} exchangeRate={stEthPerWstEth} decimals={d} />
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
                ≈ {toEth(totalDepositedWstEth).toFixed(d)} ETH
              </div>
              <div className="font-mono text-xs text-slate-500">
                {totalDepositedWstEth.toFixed(d)} wstETH
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

      {/* Exchange Rate — subtle display */}
      <div className="flex items-center justify-center gap-2 text-xs text-slate-500">
        <span>1 wstETH = {stEthPerWstEth.toFixed(d)} stETH ≈ {stEthPerWstEth.toFixed(d)} ETH</span>
        <span className="text-slate-600">·</span>
        <span>via Lido</span>
      </div>

      {/* Activity History */}
      <div>
        <h2 className="mb-4 text-xl font-semibold text-white">Activity History</h2>
        <ActivityHistory
          activities={activities}
          isLoading={activitiesLoading}
          stEthPerWstEth={stEthPerWstEth}
          decimals={d}
        />
      </div>
    </div>
  );
}
