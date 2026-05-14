import { useGasPrice, useChainId } from "wagmi";
import { useEffect, useState } from "react";
import { formatGwei } from "viem";

const ZAP_EXTRA_GAS = 125_000n;

/** Mainnet chain id */
const MAINNET_CHAIN_ID = 1;

/**
 * Estimates the extra gas cost of the ZapRouter (ETH→stETH→wstETH wrapping)
 * using the current chain's gas price and a live ETH/USD price feed.
 */
export function useZapGasCost() {
  const chainId = useChainId();
  const { data: gasPrice } = useGasPrice();
  const [ethUsd, setEthUsd] = useState<number | null>(null);

  const isMainnet = chainId === MAINNET_CHAIN_ID;

  // Fetch ETH/USD from CoinGecko (lightweight, no API key needed)
  useEffect(() => {
    let cancelled = false;
    async function fetchPrice() {
      try {
        const res = await fetch(
          "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd"
        );
        const data = await res.json();
        if (!cancelled && data?.ethereum?.usd) {
          setEthUsd(data.ethereum.usd);
        }
      } catch {
        // silently fail — USD estimate just won't show
      }
    }
    fetchPrice();
    return () => { cancelled = true; };
  }, []);

  if (!gasPrice) {
    return { ready: false as const, text: "" };
  }

  // Extra cost in ETH
  const costWei = gasPrice * ZAP_EXTRA_GAS;
  const costEth = Number(costWei) / 1e18;

  // Build display string
  let text: string;
  if (ethUsd && isMainnet) {
    const costUsd = costEth * ethUsd;
    text = costUsd < 0.01
      ? "<$0.01"
      : `~$${costUsd.toFixed(2)}`;
  } else if (ethUsd) {
    // Testnet: show both ETH cost and what it *would* cost on mainnet
    const costUsd = costEth * ethUsd;
    if (costUsd < 0.01) {
      text = `${costEth.toFixed(6)} ETH (<$0.01 at mainnet prices)`;
    } else {
      text = `${costEth.toFixed(6)} ETH (~$${costUsd.toFixed(2)} at mainnet prices)`;
    }
  } else {
    text = `${costEth.toFixed(6)} ETH`;
  }

  return {
    ready: true as const,
    costEth,
    costUsd: ethUsd ? costEth * ethUsd : null,
    gasPriceGwei: formatGwei(gasPrice),
    isMainnet,
    text,
  };
}
