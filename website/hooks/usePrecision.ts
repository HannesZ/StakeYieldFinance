import { useState } from "react";
import { useChainId } from "wagmi";
import { hoodi } from "@/lib/wagmi";

/**
 * Precision toggle for testnet debugging.
 * Returns the number of decimal places and a toggle button component.
 * Only active on testnets; on mainnet returns fixed 4 decimals and no toggle.
 */
export function usePrecision(defaultDecimals = 4, extendedDecimals = 10) {
  const chainId = useChainId();
  const isTestnet = chainId === hoodi.id; // extend with other testnets if needed
  const [extended, setExtended] = useState(false);

  const decimals = isTestnet && extended ? extendedDecimals : defaultDecimals;

  const toggle = isTestnet ? () => setExtended((v) => !v) : undefined;

  return { decimals, extended: isTestnet && extended, isTestnet, toggle };
}
