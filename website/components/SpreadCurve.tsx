"use client";

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
  const data = generateSpreadData(sBase, alpha, beta, kappaTarget);

  return (
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
            label={{ value: "Spread (bp)", angle: -90, position: "insideLeft", fill: "#94a3b8", fontSize: 12 }}
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
  );
}
