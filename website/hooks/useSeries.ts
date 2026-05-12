import { useReadContract } from "wagmi";
import { formatUnits } from "viem";
import { ADDRESSES, STABLE_YIELD_VAULT_ABI } from "@/lib/contracts";
import { SERIES_2026Q4_ID } from "@/lib/utils";

const addresses = ADDRESSES.hoodi;

export interface SeriesInfo {
  maturity: number;
  fixedRate: number; // percentage, e.g. 2.5
  fixedRateE18: bigint;
  totalDeposited: bigint;
  totalClaimsStEth: bigint;
  totalSyLst: bigint;
  isOpen: boolean;
  isSettled: boolean;
  isLoading: boolean;
}

export function useSeries(seriesId: `0x${string}` = SERIES_2026Q4_ID): SeriesInfo {
  const { data: seriesData, isLoading: isSeriesLoading } = useReadContract({
    address: addresses.stableYieldVault,
    abi: STABLE_YIELD_VAULT_ABI,
    functionName: "getSeries",
    args: [seriesId],
  });

  // Read the model-computed fixed rate: stakingAPR - spread(κ)
  const { data: computedRateE18, isLoading: isRateLoading } = useReadContract({
    address: addresses.stableYieldVault,
    abi: STABLE_YIELD_VAULT_ABI,
    functionName: "computeFixedRate",
  });

  const isLoading = isSeriesLoading || isRateLoading;

  // Convert 1e18-scaled rate to percentage: 1e18 = 100%, so /1e16 gives %
  const fixedRateE18 = computedRateE18 ?? 0n;
  const fixedRate = computedRateE18 ? Number(formatUnits(fixedRateE18, 16)) : 0;

  if (!seriesData) {
    return {
      maturity: 1798761599,
      fixedRate,
      fixedRateE18,
      totalDeposited: 0n,
      totalClaimsStEth: 0n,
      totalSyLst: 0n,
      isOpen: true,
      isSettled: false,
      isLoading,
    };
  }

  return {
    maturity: Number(seriesData.maturity),
    fixedRate,
    fixedRateE18,
    totalDeposited: seriesData.totalDeposited,
    totalClaimsStEth: seriesData.totalClaimsStEth,
    totalSyLst: seriesData.totalSyLst,
    isOpen: seriesData.isOpen,
    isSettled: seriesData.isSettled,
    isLoading,
  };
}
