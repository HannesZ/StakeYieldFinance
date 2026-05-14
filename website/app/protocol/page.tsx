"use client";

import { useReadContract } from "wagmi";
import { formatEther } from "viem";
import { SolvencyGauge } from "@/components/SolvencyGauge";
import { SpreadCurve } from "@/components/SpreadCurve";
import { SeriesTable } from "@/components/SeriesTable";
import { ADDRESSES, RESERVE_MANAGER_ABI, SPREAD_CALCULATOR_ABI, STABLE_YIELD_VAULT_ABI } from "@/lib/contracts";
import { DEMO_SERIES, SERIES_2026Q4_ID, timeUntil } from "@/lib/utils";
import { useSeries } from "@/hooks/useSeries";
import { useSpotAPR } from "@/hooks/useSpotAPR";
import { usePrecision } from "@/hooks/usePrecision";
import { PrecisionToggle } from "@/components/PrecisionToggle";

const addresses = ADDRESSES.hoodi;

export default function ProtocolPage() {
  const seriesInfo = useSeries();
  const { spotAPR, source: aprSource } = useSpotAPR();
  const { decimals: d, extended, isTestnet, toggle } = usePrecision();

  // Reserve data
  const { data: totalReserve } = useReadContract({
    address: addresses.reserveManager,
    abi: RESERVE_MANAGER_ABI,
    functionName: "totalReserve",
  });

  const { data: totalLiabilities } = useReadContract({
    address: addresses.reserveManager,
    abi: RESERVE_MANAGER_ABI,
    functionName: "totalLiabilities",
  });

  const { data: kappaRaw } = useReadContract({
    address: addresses.reserveManager,
    abi: RESERVE_MANAGER_ABI,
    functionName: "kappa",
  });

  const { data: kappaTarget } = useReadContract({
    address: addresses.reserveManager,
    abi: RESERVE_MANAGER_ABI,
    functionName: "kappaTarget",
  });

  const { data: isEmergency } = useReadContract({
    address: addresses.reserveManager,
    abi: RESERVE_MANAGER_ABI,
    functionName: "isEmergency",
  });

  const { data: kappaEmergencyRaw } = useReadContract({
    address: addresses.reserveManager,
    abi: RESERVE_MANAGER_ABI,
    functionName: "kappaEmergency",
  });

  // Spread data
  const { data: currentSpread } = useReadContract({
    address: addresses.spreadCalculator,
    abi: SPREAD_CALCULATOR_ABI,
    functionName: "currentSpread",
  });

  const { data: spreadParams } = useReadContract({
    address: addresses.spreadCalculator,
    abi: SPREAD_CALCULATOR_ABI,
    functionName: "getParameters",
  });

  // Series data
  const { data: seriesData } = useReadContract({
    address: addresses.stableYieldVault,
    abi: STABLE_YIELD_VAULT_ABI,
    functionName: "getSeries",
    args: [SERIES_2026Q4_ID],
  });

  // Format values
  const reserve = totalReserve !== undefined ? Number(formatEther(totalReserve)) : null;
  const liabilities = totalLiabilities !== undefined ? Number(formatEther(totalLiabilities)) : null;
  const kappaNum = kappaRaw !== undefined ? Number(formatEther(kappaRaw)) : null;
  // When liabilities are 0, kappa is effectively infinite — cap for display
  const kappa = kappaNum !== null && kappaNum > 100 ? null : kappaNum;
  const kappaTargetNum = kappaTarget !== undefined ? Number(formatEther(kappaTarget)) : 1.5;
  const kappaEmergencyNum = kappaEmergencyRaw !== undefined ? Number(formatEther(kappaEmergencyRaw)) : 0.3;
  const spreadBps = currentSpread !== undefined ? Number(currentSpread) : null;

  // Build series table
  const seriesRows = [{
    id: "2026Q4",
    fixedRate: seriesInfo.fixedRate,
    maturity: seriesData ? Number(seriesData.maturity) : 1798761599,
    totalDeposited: seriesData ? Number(formatEther(seriesData.totalDeposited)) : 0,
    isOpen: seriesData?.isOpen ?? true,
  }];

  return (
    <div className="mx-auto max-w-5xl space-y-8">
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-3xl font-bold text-white">Protocol Health</h1>
          <p className="mt-2 text-slate-400">
            Real-time on-chain data from StableYield on Hoodi testnet.
          </p>
        </div>
        {isTestnet && toggle && (
          <PrecisionToggle extended={extended} onToggle={toggle} />
        )}
      </div>
      {isEmergency && (
        <div className="rounded-lg border border-red-500/30 bg-red-500/10 px-4 py-2 text-sm text-red-400">
          ⚠️ Emergency mode active — solvency below critical threshold
        </div>
      )}

      {/* Key Metrics */}
      <div className="grid gap-4 sm:grid-cols-3 lg:grid-cols-5">
        <div className="rounded-xl border border-white/10 bg-white/[0.02] p-5">
          <div className="text-xs text-slate-400 uppercase tracking-wider">Reserve</div>
          <div className="mt-2 text-2xl font-bold font-mono text-white">
            {reserve !== null ? reserve.toFixed(d) : "—"}
          </div>
          <div className="text-xs text-slate-500">wstETH</div>
        </div>
        <div className="rounded-xl border border-white/10 bg-white/[0.02] p-5">
          <div className="text-xs text-slate-400 uppercase tracking-wider">Liabilities</div>
          <div className="mt-2 text-2xl font-bold font-mono text-white">
            {liabilities !== null ? liabilities.toFixed(d) : "—"}
          </div>
          <div className="text-xs text-slate-500">wstETH owed</div>
        </div>
        <div className="rounded-xl border border-white/10 bg-white/[0.02] p-5">
          <div className="text-xs text-slate-400 uppercase tracking-wider">Solvency κ</div>
          <div className={`mt-2 text-2xl font-bold font-mono ${
            kappa !== null && kappa >= kappaTargetNum ? "text-[#4EC9B0]" :
            kappa !== null && kappa >= 1 ? "text-yellow-400" : "text-red-400"
          }`}>
            {kappa !== null ? (kappa * 100).toFixed(1) + "%" : "—"}
          </div>
          <div className="text-xs text-slate-500">target: {(kappaTargetNum * 100).toFixed(0)}%</div>
        </div>
        <div className="rounded-xl border border-white/10 bg-white/[0.02] p-5">
          <div className="text-xs text-slate-400 uppercase tracking-wider">Current Spread</div>
          <div className="mt-2 text-2xl font-bold font-mono text-[#637DEA]">
            {spreadBps !== null ? spreadBps + " bp" : "—"}
          </div>
          <div className="text-xs text-slate-500">
            base: {spreadParams ? Number(spreadParams.sBaseBps) + " bp" : "—"}
          </div>
        </div>
        <div className="rounded-xl border border-white/10 bg-white/[0.02] p-5">
          <div className="text-xs text-slate-400 uppercase tracking-wider">Staking APR</div>
          <div className="mt-2 text-2xl font-bold font-mono text-amber-400">
            {spotAPR !== null ? spotAPR.toFixed(2) + "%" : "—"}
          </div>
          <div className="text-xs text-slate-500">
            {aprSource}
          </div>
        </div>
      </div>

      {/* Solvency Gauge + Spread Curve */}
      <div className="grid gap-6 lg:grid-cols-2">
        <div className="rounded-2xl border border-white/10 bg-white/[0.02] p-6">
          <h2 className="mb-4 text-lg font-semibold text-white">Solvency Gauge</h2>
          <SolvencyGauge
            kappa={kappa ?? 0}
            kappaTarget={kappaTargetNum}
            kappaEmergency={kappaEmergencyNum}
          />
          <div className="mt-4 space-y-1 text-xs text-slate-400">
            <div className="flex justify-between">
              <span>κ_target</span>
              <span className="font-mono text-[#4EC9B0]">{(kappaTargetNum * 100).toFixed(0)}%</span>
            </div>
            <div className="flex justify-between">
              <span>κ_emergency</span>
              <span className="font-mono text-red-400">{(kappaEmergencyNum * 100).toFixed(0)}%</span>
            </div>
          </div>
        </div>
        <div className="rounded-2xl border border-white/10 bg-white/[0.02] p-6">
          <h2 className="mb-4 text-lg font-semibold text-white">Spread Curve</h2>
          <SpreadCurve
            sBase={spreadParams ? Number(spreadParams.sBaseBps) : 25}
            alpha={spreadParams ? Number(formatEther(spreadParams.alphaE18)) : 10}
            beta={spreadParams ? Number(formatEther(spreadParams.betaE18)) : 3}
            kappaTarget={kappaTargetNum}
            currentKappa={kappa ?? undefined}
          />
        </div>
      </div>

      {/* Series Table */}
      <div className="rounded-2xl border border-white/10 bg-white/[0.02] p-6">
        <h2 className="mb-4 text-lg font-semibold text-white">Active Series</h2>
        <SeriesTable series={seriesRows} />
      </div>

      {/* Contract Addresses */}
      <div className="rounded-2xl border border-white/10 bg-white/[0.02] p-6">
        <h2 className="mb-4 text-lg font-semibold text-white">Contract Addresses</h2>
        <div className="space-y-2 text-sm">
          {[
            ["StableYieldVault", addresses.stableYieldVault],
            ["SyLST (ERC-1155)", addresses.syLST],
            ["ReserveManager", addresses.reserveManager],
            ["SpreadCalculator", addresses.spreadCalculator],
            ["SYLD Token", addresses.syldToken],
            ["wstETH", addresses.wstETH],
          ].map(([name, addr]) => (
            <div key={name} className="flex items-center justify-between">
              <span className="text-slate-400">{name}</span>
              <a
                href={`https://hoodi.etherscan.io/address/${addr}`}
                target="_blank"
                rel="noopener noreferrer"
                className="font-mono text-[#637DEA] hover:underline"
              >
                {addr}
              </a>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
