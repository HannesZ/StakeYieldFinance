"use client";

import Link from "next/link";
import { DEMO_SERIES, SERIES_2026Q4_ID } from "@/lib/utils";
import { ADDRESSES, STABLE_YIELD_VAULT_ABI } from "@/lib/contracts";
import { useSeries } from "@/hooks/useSeries";
import { useSpotAPR } from "@/hooks/useSpotAPR";
import { useReadContract } from "wagmi";
import { formatEther } from "viem";

const STEPS = [
  {
    num: "01",
    title: "Connect & Choose",
    desc: "Connect your wallet and select a maturity series — Q3 2026, Q4 2026, and beyond. Each series offers a transparent, model-derived fixed rate.",
    icon: "🔗",
  },
  {
    num: "02",
    title: "Deposit wstETH",
    desc: "Approve and deposit wrapped staked ETH. You receive syLST tokens — transferable fixed-rate claims backed by the protocol's actuarial reserve.",
    icon: "📥",
  },
  {
    num: "03",
    title: "Redeem at Maturity",
    desc: "At maturity, burn your syLST to receive your principal plus the guaranteed fixed yield. No surprises, no impermanent loss.",
    icon: "💰",
  },
];

const addresses = ADDRESSES.hoodi;

export default function LandingPage() {
  const seriesInfo = useSeries();
  const { spotAPR } = useSpotAPR();

  // Read actual on-chain totalDeposited for TVL display
  const { data: seriesData } = useReadContract({
    address: addresses.stableYieldVault,
    abi: STABLE_YIELD_VAULT_ABI,
    functionName: "getSeries",
    args: [SERIES_2026Q4_ID],
  });
  const tvl = seriesData ? Number(formatEther(seriesData.totalDeposited)) : null;

  return (
    <div className="space-y-24">
      {/* ── Hero ─────────────────────────────────────────────────────── */}
      <section className="relative flex flex-col items-center pt-16 text-center">
        {/* Glow effect */}
        <div className="pointer-events-none absolute -top-20 h-[500px] w-[500px] rounded-full bg-[#637DEA]/10 blur-[120px]" />

        <div className="relative">
          <div className="mb-5 inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/5 px-4 py-1.5 text-sm text-slate-300">
            <span className="h-2 w-2 rounded-full bg-[#4EC9B0] animate-pulse" />
            Live on Hoodi Testnet
          </div>
          <h1 className="max-w-4xl text-5xl font-bold leading-tight tracking-tight text-white md:text-7xl">
            Fixed-Rate Yield on{" "}
            <span className="bg-gradient-to-r from-[#637DEA] to-[#4EC9B0] bg-clip-text text-transparent">
              Ethereum Staking
            </span>
          </h1>
          <p className="mx-auto mt-6 max-w-2xl text-lg text-slate-400">
            Convert variable staking yield into predictable, fixed-rate
            instruments. Backed by actuarial-grade reserve management —
            transparent risk pricing, not black-box AMMs.
          </p>
          <div className="mt-10 flex flex-col items-center gap-4 sm:flex-row sm:justify-center">
            <Link
              href="/deposit"
              className="rounded-xl bg-gradient-to-r from-[#637DEA] to-[#4EC9B0] px-8 py-3.5 text-base font-semibold text-white shadow-lg shadow-[#637DEA]/20 transition hover:shadow-xl hover:shadow-[#637DEA]/30"
            >
              Start Earning Fixed Yield →
            </Link>
            <Link
              href="/protocol"
              className="rounded-xl border border-white/10 bg-white/5 px-8 py-3.5 text-base font-semibold text-slate-300 transition hover:bg-white/10"
            >
              View Protocol Health
            </Link>
          </div>
        </div>
      </section>

      {/* ── How it Works ─────────────────────────────────────────────── */}
      <section>
        <h2 className="mb-12 text-center text-3xl font-bold text-white">
          How It Works
        </h2>
        <div className="grid gap-6 md:grid-cols-3">
          {STEPS.map((step) => (
            <div
              key={step.num}
              className="group relative overflow-hidden rounded-2xl border border-white/10 bg-gradient-to-b from-white/[0.04] to-transparent p-8 transition hover:border-white/20"
            >
              <div className="absolute right-4 top-4 font-mono text-6xl font-bold text-white/[0.03] transition group-hover:text-white/[0.06]">
                {step.num}
              </div>
              <div className="mb-4 text-3xl">{step.icon}</div>
              <h3 className="mb-2 text-xl font-semibold text-white">
                {step.title}
              </h3>
              <p className="text-sm leading-relaxed text-slate-400">
                {step.desc}
              </p>
            </div>
          ))}
        </div>
      </section>

      {/* ── Current Rates ────────────────────────────────────────────── */}
      <section>
        <h2 className="mb-2 text-center text-3xl font-bold text-white">
          Current Rates
        </h2>
        <p className="mb-10 text-center text-slate-400">
          Fixed yields across maturities — lock in your rate today
        </p>
        <div className="grid gap-4 md:grid-cols-3">
          {DEMO_SERIES.map((s) => {
            const matDate = new Date(s.maturity * 1000).toLocaleDateString(
              "en-US",
              { year: "numeric", month: "short" }
            );
            return (
              <div
                key={s.id}
                className="rounded-2xl border border-white/10 bg-white/[0.02] p-6 text-center transition hover:border-[#637DEA]/30 hover:bg-white/[0.04]"
              >
                <div className="mb-1 text-sm text-slate-400">{s.id}</div>
                <div className="text-4xl font-bold text-[#4EC9B0]">
                  {seriesInfo.fixedRate.toFixed(2)}%
                </div>
                <div className="mb-4 text-xs text-slate-500">Fixed APR</div>
                <div className="text-xs text-slate-400">
                  Matures {matDate}{tvl !== null ? ` · ${tvl.toFixed(2)} wstETH TVL` : ""}
                </div>
                <Link
                  href="/deposit"
                  className="mt-4 inline-block rounded-lg bg-[#637DEA]/10 px-5 py-2 text-sm font-medium text-[#637DEA] transition hover:bg-[#637DEA]/20"
                >
                  Deposit →
                </Link>
              </div>
            );
          })}
        </div>
      </section>

      {/* ── Stats Bar ────────────────────────────────────────────────── */}
      <section className="rounded-2xl border border-white/10 bg-gradient-to-r from-[#637DEA]/5 to-[#4EC9B0]/5 p-8">
        <div className="grid grid-cols-2 gap-8 md:grid-cols-4">
          {[
            { label: "Total Value Locked", value: "Testnet" },
            { label: "Current Staking APR", value: spotAPR ? spotAPR.toFixed(2) + "%" : "—" },
            { label: "Offered Fixed Rate", value: seriesInfo.fixedRate.toFixed(2) + "%" },
            { label: "Network", value: "Hoodi" },
          ].map((stat) => (
            <div key={stat.label} className="text-center">
              <div className="text-2xl font-bold text-white md:text-3xl">
                {stat.value}
              </div>
              <div className="mt-1 text-xs text-slate-400">{stat.label}</div>
            </div>
          ))}
        </div>
      </section>

      {/* ── CTA ──────────────────────────────────────────────────────── */}
      <section className="text-center pb-8">
        <h2 className="text-3xl font-bold text-white">
          Ready for Predictable Yield?
        </h2>
        <p className="mx-auto mt-3 max-w-lg text-slate-400">
          No impermanent loss. No rate uncertainty. Just actuarially-managed fixed returns on your staked ETH.
        </p>
        <Link
          href="/deposit"
          className="mt-8 inline-block rounded-xl bg-gradient-to-r from-[#637DEA] to-[#4EC9B0] px-10 py-4 text-lg font-semibold text-white shadow-lg shadow-[#637DEA]/20 transition hover:shadow-xl hover:shadow-[#637DEA]/30"
        >
          Launch App →
        </Link>
      </section>
    </div>
  );
}
