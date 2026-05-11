"use client";

import { useAccount, useSwitchChain } from "wagmi";
import { hoodi } from "@/lib/wagmi";

/**
 * Shows a banner + switch button when the user is connected but on the wrong chain.
 * Wraps page content — if wrong chain, content is dimmed behind the banner.
 */
export function NetworkGuard({ children }: { children: React.ReactNode }) {
  const { isConnected, chain } = useAccount();
  const { switchChain, isPending } = useSwitchChain();

  const isWrongChain = isConnected && chain && chain.id !== hoodi.id;

  if (!isWrongChain) return <>{children}</>;

  return (
    <div className="relative">
      {/* Banner */}
      <div className="sticky top-16 z-40 mx-auto mb-6 max-w-2xl rounded-xl border border-amber-500/30 bg-amber-500/10 p-4 text-center backdrop-blur">
        <p className="mb-3 text-sm text-amber-200">
          You&apos;re connected to <strong>{chain.name}</strong>. StakeYield runs on{" "}
          <strong>Hoodi Testnet</strong>.
        </p>
        <button
          onClick={() => switchChain({ chainId: hoodi.id })}
          disabled={isPending}
          className="rounded-lg bg-amber-500 px-5 py-2 text-sm font-semibold text-black transition hover:bg-amber-400 disabled:opacity-50"
        >
          {isPending ? "Switching…" : "Switch to Hoodi Testnet"}
        </button>
      </div>
      {/* Dimmed content */}
      <div className="pointer-events-none opacity-40">{children}</div>
    </div>
  );
}
