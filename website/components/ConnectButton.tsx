"use client";

import { useAccount, useConnect, useDisconnect } from "wagmi";
import { truncateAddress } from "@/lib/utils";
import { useEffect } from "react";

export function ConnectButton() {
  const { address, isConnected, chain } = useAccount();
  const { connect, connectors, isPending, error } = useConnect();
  const { disconnect } = useDisconnect();

  // Debug: log connector state
  useEffect(() => {
    console.log("[ConnectButton] connectors:", connectors.map(c => ({ name: c.name, id: c.id, type: c.type })));
    if (error) console.error("[ConnectButton] connect error:", error);
  }, [connectors, error]);

  if (isConnected && address) {
    return (
      <div className="flex items-center gap-2">
        {chain && (
          <span className="rounded-lg bg-white/5 px-3 py-2 text-xs text-slate-400">
            {chain.name}
          </span>
        )}
        <button
          onClick={() => disconnect()}
          className="rounded-xl bg-white/5 px-4 py-2 text-sm font-medium text-white transition hover:bg-white/10"
          title={address}
        >
          {truncateAddress(address)}
        </button>
      </div>
    );
  }

  // Use the first available connector (injected / MetaMask)
  const connector = connectors[0];

  const handleConnect = () => {
    console.log("[ConnectButton] clicked! connector:", connector?.name, connector?.id);
    if (connector) {
      connect({ connector });
    } else {
      console.warn("[ConnectButton] no connectors available");
      // Fallback: try window.ethereum directly
      if (typeof window !== "undefined" && window.ethereum) {
        console.log("[ConnectButton] trying window.ethereum.request directly");
        window.ethereum.request({ method: "eth_requestAccounts" }).catch(console.error);
      }
    }
  };

  return (
    <button
      onClick={handleConnect}
      disabled={isPending}
      className="rounded-xl bg-indigo-500 px-5 py-2.5 text-sm font-semibold text-white transition-all hover:bg-indigo-400 hover:shadow-lg hover:shadow-indigo-500/25 disabled:opacity-50"
    >
      {isPending ? "Connecting…" : "Connect Wallet"}
    </button>
  );
}
