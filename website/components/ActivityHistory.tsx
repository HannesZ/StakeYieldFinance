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
}

export function ActivityHistory({ activities, isLoading }: ActivityHistoryProps) {
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
            <th className="px-5 py-3 font-medium">Amount</th>
            <th className="px-5 py-3 font-medium hidden sm:table-cell">syLST</th>
            <th className="px-5 py-3 font-medium hidden md:table-cell">Claim (stETH)</th>
            <th className="px-5 py-3 font-medium">Date</th>
            <th className="px-5 py-3 font-medium text-right">Tx</th>
          </tr>
        </thead>
        <tbody>
          {activities.map((a, i) => (
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

              {/* wstETH amount */}
              <td className="px-5 py-3.5 font-mono text-white">
                {a.amount.toFixed(4)} <span className="text-slate-400">wstETH</span>
              </td>

              {/* syLST amount */}
              <td className="px-5 py-3.5 font-mono text-slate-300 hidden sm:table-cell">
                {a.syLstAmount.toFixed(4)}
              </td>

              {/* Claim at maturity (stETH) */}
              <td className="px-5 py-3.5 font-mono text-slate-300 hidden md:table-cell">
                {a.type === "deposit" && a.claimAtMaturityStEth
                  ? `${a.claimAtMaturityStEth.toFixed(4)} stETH`
                  : "—"}
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
          ))}
        </tbody>
      </table>
    </div>
  );
}
