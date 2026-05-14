"use client";

interface PrecisionToggleProps {
  extended: boolean;
  onToggle: () => void;
}

/**
 * Small testnet-only toggle for switching between standard and extended decimal precision.
 */
export function PrecisionToggle({ extended, onToggle }: PrecisionToggleProps) {
  return (
    <button
      onClick={onToggle}
      className="inline-flex items-center gap-1.5 rounded-lg border border-white/10 bg-white/[0.03] px-2.5 py-1 text-xs text-slate-400 transition hover:border-white/20 hover:text-slate-300"
      title={extended ? "Show standard precision" : "Show extended precision (testnet)"}
    >
      <span className="font-mono">.{'0'.repeat(extended ? 10 : 4)}</span>
      <span className={`inline-block h-3 w-6 rounded-full transition ${extended ? 'bg-[#4EC9B0]' : 'bg-white/10'}`}>
        <span className={`block h-3 w-3 rounded-full bg-white transition-transform ${extended ? 'translate-x-3' : 'translate-x-0'}`} />
      </span>
    </button>
  );
}
