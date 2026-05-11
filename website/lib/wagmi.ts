import { http, createConfig } from "wagmi";
import { injected } from "wagmi/connectors";
import { type Chain } from "viem";
import { mainnet } from "wagmi/chains";

/** Hoodi testnet (chainId 560048) */
export const hoodi: Chain = {
  id: 560048,
  name: "Hoodi Testnet",
  nativeCurrency: { name: "Ether", symbol: "ETH", decimals: 18 },
  rpcUrls: {
    default: { http: ["https://rpc.hoodi.ethpandaops.io"] },
  },
  blockExplorers: {
    default: { name: "Etherscan", url: "https://hoodi.etherscan.io" },
  },
  testnet: true,
};

export const config = createConfig({
  chains: [hoodi, mainnet],
  connectors: [
    injected(),
  ],
  transports: {
    [hoodi.id]: http("https://rpc.hoodi.ethpandaops.io"),
    [mainnet.id]: http(),
  },
  ssr: true,
});
