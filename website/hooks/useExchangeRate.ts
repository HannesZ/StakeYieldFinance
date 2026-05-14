import { useReadContract } from "wagmi";
import { formatEther } from "viem";
import { ADDRESSES, WSTETH_ABI } from "@/lib/contracts";

const addresses = ADDRESSES.hoodi;

export interface ExchangeRate {
  /** stETH per 1 wstETH (e.g. 1.1234) — stETH ≈ ETH for display purposes */
  stEthPerWstEth: number;
  /** Raw 1e18-scaled value from contract */
  stEthPerTokenRaw: bigint | undefined;
  isLoading: boolean;
  /** Convert a wstETH amount to ETH-equivalent (via stETH ≈ ETH) */
  toEth: (wstEthAmount: number) => number;
  /** Convert an stETH amount to wstETH-equivalent */
  toWstEth: (stEthAmount: number) => number;
}

/**
 * Reads the wstETH/stETH exchange rate from the Lido contract.
 * Since stETH ≈ ETH, we treat stETH values as ETH-equivalent for display.
 */
export function useExchangeRate(): ExchangeRate {
  const { data: stEthPerToken, isLoading } = useReadContract({
    address: addresses.wstETH,
    abi: WSTETH_ABI,
    functionName: "stEthPerToken",
  });

  const stEthPerWstEth = stEthPerToken
    ? Number(formatEther(stEthPerToken))
    : 1;

  return {
    stEthPerWstEth,
    stEthPerTokenRaw: stEthPerToken,
    isLoading,
    toEth: (wstEthAmount: number) => wstEthAmount * stEthPerWstEth,
    toWstEth: (stEthAmount: number) =>
      stEthPerWstEth > 0 ? stEthAmount / stEthPerWstEth : 0,
  };
}
