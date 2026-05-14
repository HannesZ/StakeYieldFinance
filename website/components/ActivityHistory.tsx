"use client";

import { type Activity } from "@/hooks/useActivityHistory";

const EXPLORER = "https://hoodi.etherscan.io";

function truncateHash(hash: string): string {
  return `${hash.slice(0, 6)}…${hash.slice(-4)}`;
}

function formatTimestamp(unix: number): string {
  if (unix === 0) return "—";
  const d = new Date(unix * 1000);
  return d.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  }) + " " + d.toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

interface ActivityHistoryProps {
  activities: Activity[];
  isLoading: boolean;
  /** Current wstETH→stETH rate for ETH-equivalent display */
  stEthPerWstEth: number;
  /** Decimal places for display (testnet precision toggle) */
  decimals?: number;
}

export function ActivityHistory({ activities, isLoading, stEthPerWstEth, decimals: d = 4 }: ActivityHistoryProps) {
  if (isLoading) {
    return (
      <div className="rounded-2xl border border-white/10 bg-white/[0.02] p-8 text-center">
        <div className="mb-2 text-2xl animate-pulse">⏳</div>
        <p className="text-sm text-slate-400">Loading activity…</p>
      </div>
    );
  }

  if (activities.length === 0) {
    return (
      <div className="rounded-2xl border border-white/10 bg-white/[0.02] p-8 text-center">
        <div className="mb-3 text-4xl">📜</div>
        <p className="text-slate-400">No activity yet.</p>
      </div>
    );
  }

  return (
    <div className="overflow-hidden rounded-2xl border border-white/10 bg-white/[0.02]">
      <table className="w-full text-left text-sm">
        <thead>
          <tr className="border-b border-white/10 text-xs uppercase tracking-wider text-slate-400">
            <th className="px-5 py-3 font-medium">Action</th>
            <th className="px-5 py-3 font-medium">Deposited</th>
            <th className="px-5 py-3 font-medium hidden md:table-cell">Claim at Maturity</th>
            <th className="px-5 py-3 font-medium hidden sm:table-cell">Yield</th>
            <th className="px-5 py-3 font-medium">Date</th>
            <th className="px-5 py-3 font-medium text-right">Tx</th>
          </tr>
        </thead>
        <tbody>
          {activities.map((a, i) => {
            // ETH-equivalent: wstETH * rate for deposited, claim is already stETH ≈ ETH
            const ethDeposited = a.amount * stEthPerWstEth;
            const ethClaim = a.claimAtMaturityStEth ?? 0;
            const ethYield = ethClaim - ethDeposited;
            const yieldPct = ethDeposited > 0 ? (ethYield / ethDeposited) * 100 : 0;

            return (
              <tr
                key={`${a.txHash}-${i}`}
                className="border-b border-white/5 transition hover:bg-white/[0.02]"
              >
                {/* Action */}
                <td className="px-5 py-3.5">
                  <span
                    className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-xs font-semibold ${
                      a.type === "deposit"
                        ? "bg-[#4EC9B0]/10 text-[#4EC9B0]"
                        : "bg-amber-400/10 text-amber-400"
                    }`}
                  >
                    {a.type === "deposit" ? "⬇ Deposit" : "⬆ Redeem"}
                  </span>
                </td>

                {/* Deposited — ETH primary, wstETH secondary */}
                <td className="px-5 py-3.5">
                  <div className="font-mono text-white">
                    ≈ {ethDeposited.toFixed(d)} <span className="text-slate-400">ETH</span>
                  </div>
                  <div className="font-mono text-xs text-slate-500">
                    {a.amount.toFixed(d)} wstETH
                  </div>
                </td>

                {/* Claim at maturity — ETH primary, stETH secondary */}
                <td className="px-5 py-3.5 hidden md:table-cell">
                  {a.type === "deposit" && ethClaim > 0 ? (
                    <>
                      <div className="font-mono text-white">
                        ≈ {ethClaim.toFixed(d)} <span className="text-slate-400">ETH</span>
                      </div>
                      <div className="font-mono text-xs text-slate-500">
                        {ethClaim.toFixed(d)} stETH
                      </div>
                    </>
                  ) : (
                    <span className="text-slate-500">—</span>
                  )}
                </td>

                {/* Yield */}
                <td className="px-5 py-3.5 hidden sm:table-cell">
                  {a.type === "deposit" && ethClaim > 0 ? (
                    <>
                      <div className="font-mono text-[#4EC9B0]">
                        +{ethYield.toFixed(d)} ETH
                      </div>
                      <div className="font-mono text-xs text-[#4EC9B0]/60">
                        +{yieldPct.toFixed(2)}%
                      </div>
                    </>
                  ) : (
                    <span className="text-slate-500">—</span>
                  )}
                </td>

                {/* Date */}
                <td className="px-5 py-3.5 text-slate-300">
                  {formatTimestamp(a.timestamp)}
                </td>

                {/* Tx link */}
                <td className="px-5 py-3.5 text-right">
                  <a
                    href={`${EXPLORER}/tx/${a.txHash}`}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-1 font-mono text-xs text-[#637DEA] transition hover:text-[#4EC9B0] hover:underline"
                  >
                    {truncateHash(a.txHash)}
                    <svg
                      className="h-3 w-3"
                      fill="none"
                      viewBox="0 0 24 24"
                      stroke="currentColor"
                      strokeWidth={2}
                    >
                      <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"
                      />
                    </svg>
                  </a>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
