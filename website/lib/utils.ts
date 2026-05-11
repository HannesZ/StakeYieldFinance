import { formatUnits } from "viem";

/** Format a 1e18-scaled uint to a human-readable percentage string. */
export function formatRatePercent(rateE18: bigint, decimals = 2): string {
  const pct = Number(formatUnits(rateE18, 16)); // 1e18 → 100%, so /1e16 → %
  return pct.toFixed(decimals) + "%";
}

/** Format a 1e18-scaled uint to a plain number. */
export function formatE18(value: bigint, decimals = 4): string {
  return Number(formatUnits(value, 18)).toFixed(decimals);
}

/** Format basis points to percentage string. */
export function bpsToPercent(bps: number, decimals = 2): string {
  return (bps / 100).toFixed(decimals) + "%";
}

/** Get time remaining until a unix timestamp. */
export function timeUntil(unixTimestamp: number): {
  days: number;
  hours: number;
  minutes: number;
  isPast: boolean;
} {
  const now = Math.floor(Date.now() / 1000);
  const diff = unixTimestamp - now;
  if (diff <= 0) return { days: 0, hours: 0, minutes: 0, isPast: true };
  return {
    days: Math.floor(diff / 86400),
    hours: Math.floor((diff % 86400) / 3600),
    minutes: Math.floor((diff % 3600) / 60),
    isPast: false,
  };
}

/** Truncate an address for display. */
export function truncateAddress(address: string, chars = 4): string {
  return `${address.slice(0, chars + 2)}…${address.slice(-chars)}`;
}

/** Live series — 2026Q4 deployed on Hoodi testnet. */
export const SERIES_2026Q4_ID = "0xfe072b35dd5e8a2b9cd745db0bbedbd13b57467ac4e187c797255d08a751353a" as `0x${string}`;

export const DEMO_SERIES = [
  {
    id: "2026Q4",
    seriesId: SERIES_2026Q4_ID,
    maturity: 1798761599, // 2026-12-31T23:59:59Z
    totalDeposited: 0,
    isOpen: true,
  },
];
