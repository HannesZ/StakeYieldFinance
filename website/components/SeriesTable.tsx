"use client";

import { timeUntil } from "@/lib/utils";

export interface SeriesRow {
  id: string;
  fixedRate: number;
  maturity: number; // unix timestamp
  totalDeposited: number;
  isOpen: boolean;
}

interface SeriesTableProps {
  series: SeriesRow[];
  onSelect?: (id: string) => void;
}

export function SeriesTable({ series, onSelect }: SeriesTableProps) {
  return (
    <div className="overflow-x-auto rounded-xl border border-white/10 bg-white/[0.02]">
      <table className="w-full text-left text-sm">
        <thead>
          <tr className="border-b border-white/10 text-xs uppercase tracking-wider text-slate-400">
            <th className="px-5 py-3">Series</th>
            <th className="px-5 py-3">Fixed Rate</th>
            <th className="px-5 py-3">Maturity</th>
            <th className="px-5 py-3">Time Left</th>
            <th className="px-5 py-3">TVL (wstETH)</th>
            <th className="px-5 py-3">Status</th>
          </tr>
        </thead>
        <tbody>
          {series.map((s) => {
            const t = timeUntil(s.maturity);
            const maturityDate = new Date(s.maturity * 1000).toLocaleDateString(
              "en-US",
              { year: "numeric", month: "short", day: "numeric" }
            );
            return (
              <tr
                key={s.id}
                className="border-b border-white/5 transition hover:bg-white/[0.03] cursor-pointer"
                onClick={() => onSelect?.(s.id)}
              >
                <td className="px-5 py-4 font-medium text-white">{s.id}</td>
                <td className="px-5 py-4 font-mono text-[#4EC9B0]">
                  {s.fixedRate.toFixed(2)}%
                </td>
                <td className="px-5 py-4 text-slate-300">{maturityDate}</td>
                <td className="px-5 py-4 text-slate-300">
                  {t.isPast ? (
                    <span className="text-amber-400">Matured</span>
                  ) : (
                    `${t.days}d ${t.hours}h`
                  )}
                </td>
                <td className="px-5 py-4 font-mono text-slate-200">
                  {s.totalDeposited.toLocaleString()}
                </td>
                <td className="px-5 py-4">
                  {s.isOpen ? (
                    <span className="inline-flex items-center gap-1.5 rounded-full bg-emerald-500/10 px-2.5 py-0.5 text-xs font-medium text-emerald-400">
                      <span className="h-1.5 w-1.5 rounded-full bg-emerald-400" />
                      Open
                    </span>
                  ) : (
                    <span className="inline-flex items-center gap-1.5 rounded-full bg-slate-500/10 px-2.5 py-0.5 text-xs font-medium text-slate-400">
                      Closed
                    </span>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
