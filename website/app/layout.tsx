import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import { Providers } from "@/components/Providers";
import { ConnectButton } from "@/components/ConnectButton";
import { NetworkGuard } from "@/components/NetworkGuard";
import Link from "next/link";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "StakeYield Finance — Fixed-Rate Yield on Ethereum Staking",
  description:
    "Convert variable Ethereum staking yield into fixed-rate instruments. Transparent, actuarial-grade risk management on-chain.",
};

const NAV_LINKS = [
  { href: "/", label: "Home" },
  { href: "/deposit", label: "Deposit" },
  { href: "/dashboard", label: "Dashboard" },
  { href: "/protocol", label: "Protocol" },
];

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark">
      <body
        className={`${inter.className} min-h-screen bg-[#0F172A] text-slate-100 antialiased`}
      >
        <Providers>
          {/* ── Navigation ─────────────────────────────────────────────── */}
          <nav className="sticky top-0 z-50 border-b border-white/5 bg-[#0F172A]/80 backdrop-blur-xl">
            <div className="mx-auto flex max-w-7xl items-center justify-between px-6 py-4">
              {/* Logo */}
              <Link href="/" className="flex items-center gap-2.5">
                <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-gradient-to-br from-[#637DEA] to-[#4EC9B0]">
                  <span className="text-sm font-bold text-white">SY</span>
                </div>
                <span className="text-lg font-bold text-white">
                  StakeYield
                </span>
              </Link>

              {/* Links */}
              <div className="hidden items-center gap-1 md:flex">
                {NAV_LINKS.map((link) => (
                  <Link
                    key={link.href}
                    href={link.href}
                    className="rounded-lg px-3.5 py-2 text-sm text-slate-300 transition hover:bg-white/5 hover:text-white"
                  >
                    {link.label}
                  </Link>
                ))}
              </div>

              {/* Wallet */}
              <ConnectButton />
            </div>
          </nav>

          {/* ── Main ───────────────────────────────────────────────────── */}
          <main className="mx-auto max-w-7xl px-6 py-10">
            <NetworkGuard>{children}</NetworkGuard>
          </main>

          {/* ── Footer ─────────────────────────────────────────────────── */}
          <footer className="border-t border-white/5 bg-[#0a1020]">
            <div className="mx-auto flex max-w-7xl flex-col items-center justify-between gap-4 px-6 py-8 md:flex-row">
              <div className="flex items-center gap-2 text-sm text-slate-500">
                <div className="flex h-5 w-5 items-center justify-center rounded bg-gradient-to-br from-[#637DEA] to-[#4EC9B0]">
                  <span className="text-[8px] font-bold text-white">SY</span>
                </div>
                StakeYield Finance · Actuarial-grade DeFi
              </div>
              <div className="flex gap-6 text-sm text-slate-500">
                <a href="https://github.com" className="transition hover:text-slate-300">
                  GitHub
                </a>
                <a href="#" className="transition hover:text-slate-300">
                  Docs
                </a>
                <a href="#" className="transition hover:text-slate-300">
                  Discord
                </a>
              </div>
            </div>
          </footer>
        </Providers>
      </body>
    </html>
  );
}
