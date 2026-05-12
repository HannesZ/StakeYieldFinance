"use client";

interface SolvencyGaugeProps {
  kappa: number; // e.g. 1.5 for 150%
  kappaTarget: number;
  kappaEmergency: number;
}

export function SolvencyGauge({
  kappa,
  kappaTarget,
  kappaEmergency,
}: SolvencyGaugeProps) {
  // Map kappa 0..2 to 0..100% arc
  const cappedKappa = Math.min(Math.max(kappa, 0), 2);
  const angle = (cappedKappa / 2) * 180; // 0 to 180 degrees
  const targetAngle = (kappaTarget / 2) * 180;
  const emergencyAngle = (kappaEmergency / 2) * 180;

  // Determine color based on solvency thresholds
  let color = "#4EC9B0"; // teal/mint — healthy (κ ≥ target)
  if (kappa < kappaTarget && kappa >= kappaEmergency) color = "#f59e0b"; // amber — stressed
  if (kappa < kappaEmergency) color = "#ef4444"; // red — emergency

  const status =
    kappa >= kappaTarget
      ? "Healthy"
      : kappa >= kappaEmergency
        ? "Stressed"
        : "Emergency";

  return (
    <div className="flex flex-col items-center">
      <div className="relative h-[140px] w-[260px]">
        <svg viewBox="0 0 260 140" className="h-full w-full">
          {/* Background arc */}
          <path
            d="M 20 130 A 110 110 0 0 1 240 130"
            fill="none"
            stroke="rgba(255,255,255,0.08)"
            strokeWidth="16"
            strokeLinecap="round"
          />
          {/* Emergency zone marker */}
          <path
            d={describeArc(130, 130, 110, 180, 180 - emergencyAngle)}
            fill="none"
            stroke="rgba(239,68,68,0.2)"
            strokeWidth="16"
            strokeLinecap="round"
          />
          {/* Target marker */}
          <line
            x1={130 + 110 * Math.cos((Math.PI * (180 - targetAngle)) / 180)}
            y1={130 - 110 * Math.sin((Math.PI * (180 - targetAngle)) / 180)}
            x2={130 + 94 * Math.cos((Math.PI * (180 - targetAngle)) / 180)}
            y2={130 - 94 * Math.sin((Math.PI * (180 - targetAngle)) / 180)}
            stroke="rgba(255,255,255,0.3)"
            strokeWidth="2"
          />
          {/* Value arc */}
          <path
            d={describeArc(130, 130, 110, 180, 180 - angle)}
            fill="none"
            stroke={color}
            strokeWidth="16"
            strokeLinecap="round"
          />
          {/* Needle dot */}
          <circle
            cx={130 + 110 * Math.cos((Math.PI * (180 - angle)) / 180)}
            cy={130 - 110 * Math.sin((Math.PI * (180 - angle)) / 180)}
            r="6"
            fill={color}
          />
        </svg>
      </div>
      <div className="mt-1 text-center">
        <div className="text-3xl font-bold" style={{ color }}>
          {(kappa * 100).toFixed(0)}%
        </div>
        <div className="text-xs text-slate-400">
          κ = {kappa.toFixed(2)} · {status}
        </div>
      </div>
    </div>
  );
}

function describeArc(
  cx: number,
  cy: number,
  r: number,
  startAngle: number,
  endAngle: number
): string {
  const start = polarToCartesian(cx, cy, r, endAngle);
  const end = polarToCartesian(cx, cy, r, startAngle);
  const largeArcFlag = startAngle - endAngle > 180 ? 1 : 0;
  return `M ${start.x} ${start.y} A ${r} ${r} 0 ${largeArcFlag} 0 ${end.x} ${end.y}`;
}

function polarToCartesian(
  cx: number,
  cy: number,
  r: number,
  angleDeg: number
) {
  const rad = ((angleDeg - 180) * Math.PI) / 180;
  return { x: cx + r * Math.cos(rad), y: cy - r * Math.sin(rad) };
}
