"use client";

import { useState } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ReferenceLine,
  ResponsiveContainer,
} from "recharts";

interface SpreadCurveProps {
  sBase?: number;
  alpha?: number;
  beta?: number;
  kappaTarget?: number;
  currentKappa?: number;
}

/** Generate spread data points for the chart. */
function generateSpreadData(
  sBase: number,
  alpha: number,
  beta: number,
  kappaTarget: number
) {
  const points = [];
  for (let k = 0.2; k <= 2.01; k += 0.02) {
    const excess = Math.max(0, kappaTarget / k - 1);
    const spread = sBase * (1 + alpha * Math.pow(excess, beta));
    points.push({
      kappa: Number(k.toFixed(2)),
      spread: Math.min(spread, 10000), // cap at 100%
    });
  }
  return points;
}

export function SpreadCurve({
  sBase = 50,
  alpha = 10,
  beta = 3,
  kappaTarget = 1.5,
  currentKappa = 1.5,
}: SpreadCurveProps) {
  const [logScale, setLogScale] = useState(false);
  const data = generateSpreadData(sBase, alpha, beta, kappaTarget);

  return (
    <div>
      {/* Toggle */}
      <div className="mb-3 flex items-center justify-end">
        <button
          onClick={() => setLogScale((v) => !v)}
          className={`flex items-center gap-1.5 rounded-lg px-3 py-1 text-xs font-medium transition ${
            logScale
              ? "bg-[#637DEA]/15 text-[#637DEA]"
              : "bg-white/5 text-slate-400 hover:bg-white/10 hover:text-slate-300"
          }`}
        >
          <svg className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M3 21h18M3 21V3m0 18l4-4c1-1 2-6 4-8s3-2 4-2 2 1 4 5l2 3" />
          </svg>
          {logScale ? "Log scale" : "Linear scale"}
        </button>
      </div>

      <div className="h-[300px] w-full">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} margin={{ top: 5, right: 20, bottom: 20, left: 10 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
            <XAxis
              dataKey="kappa"
              stroke="#64748b"
              fontSize={12}
              label={{ value: "κ (Solvency Ratio)", position: "insideBottom", offset: -10, fill: "#94a3b8", fontSize: 12 }}
              tickFormatter={(v: number) => v.toFixed(1)}
            />
            <YAxis
              stroke="#64748b"
              fontSize={12}
              scale={logScale ? "log" : "auto"}
              domain={logScale ? [1, "auto"] : [0, "auto"]}
              allowDataOverflow={logScale}
              label={{ value: "Spread (bp)", angle: -90, position: "insideLeft", fill: "#94a3b8", fontSize: 12 }}
              tickFormatter={(v: number) =>
                logScale && v >= 1000
                  ? `${(v / 1000).toFixed(0)}k`
                  : v.toFixed(0)
              }
            />
            <Tooltip
              contentStyle={{
                backgroundColor: "#1e293b",
                border: "1px solid rgba(255,255,255,0.1)",
                borderRadius: "8px",
                color: "#e2e8f0",
                fontSize: 13,
              }}
              // eslint-disable-next-line @typescript-eslint/no-explicit-any
              formatter={((value: any) => [`${Number(value).toFixed(0)} bp`, "Spread"]) as any}
              // eslint-disable-next-line @typescript-eslint/no-explicit-any
              labelFormatter={((label: any) => `κ = ${label}`) as any}
            />
            <ReferenceLine
              x={kappaTarget}
              stroke="#4EC9B0"
              strokeDasharray="5 5"
              label={{ value: "κ target", fill: "#4EC9B0", fontSize: 11, position: "top" }}
            />
            {currentKappa < 2 && (
              <ReferenceLine
                x={currentKappa}
                stroke="#637DEA"
                strokeDasharray="3 3"
                label={{ value: "Current", fill: "#637DEA", fontSize: 11, position: "top" }}
              />
            )}
            <Line
              type="monotone"
              dataKey="spread"
              stroke="#637DEA"
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4, fill: "#637DEA" }}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
