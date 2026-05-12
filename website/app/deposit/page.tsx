"use client";

import { useState, useEffect } from "react";
import { useAccount, useReadContract, useWriteContract, useWaitForTransactionReceipt } from "wagmi";
import { parseEther, formatEther } from "viem";
import { ConnectButton } from "@/components/ConnectButton";
import { DEMO_SERIES } from "@/lib/utils";
import { ADDRESSES, STABLE_YIELD_VAULT_ABI, ERC20_ABI, WSTETH_ABI } from "@/lib/contracts";
import { useSeries } from "@/hooks/useSeries";

const addresses = ADDRESSES.hoodi;

export default function DepositPage() {
  const { address, isConnected } = useAccount();
  const [selectedSeries, setSelectedSeries] = useState(DEMO_SERIES[0]);
  const [amount, setAmount] = useState("");
  const [step, setStep] = useState<"input" | "approving" | "approved" | "depositing" | "done">("input");
  const seriesInfo = useSeries(selectedSeries.seriesId);

  const parsedAmount = parseFloat(amount) || 0;
  const tenor = (selectedSeries.maturity - Math.floor(Date.now() / 1000)) / (365 * 86400);
  const effectiveRate = seriesInfo.fixedRate;

  // Read wstETH balance
  const { data: wstETHBalance } = useReadContract({
    address: addresses.wstETH,
    abi: ERC20_ABI,
    functionName: "balanceOf",
    args: address ? [address] : undefined,
    query: { enabled: !!address },
  });

  // Read stEthPerToken for conversion (WAD-scaled, 1e18)
  const { data: stEthPerToken } = useReadContract({
    address: addresses.wstETH,
    abi: WSTETH_ABI,
    functionName: "stEthPerToken",
  });

  // Compute stETH value and expected payout in stETH
  const stEthPerTokenNum = stEthPerToken ? Number(stEthPerToken) / 1e18 : 1;
  const stEthValue = parsedAmount * stEthPerTokenNum;
  const expectedPayoutStEth = stEthValue * (1 + (effectiveRate / 100) * tenor);
  const yieldAmountStEth = expectedPayoutStEth - stEthValue;
  // Approximate wstETH equivalent at current rate (for user reference)
  const expectedPayoutWstEth = stEthPerTokenNum > 0 ? expectedPayoutStEth / stEthPerTokenNum : 0;

  const maturityDate = new Date(selectedSeries.maturity * 1000).toLocaleDateString(
    "en-US",
    { year: "numeric", month: "long", day: "numeric" }
  );

  // Read current allowance
  const { data: allowance, refetch: refetchAllowance } = useReadContract({
    address: addresses.wstETH,
    abi: ERC20_ABI,
    functionName: "allowance",
    args: address ? [address, addresses.stableYieldVault] : undefined,
    query: { enabled: !!address },
  });

  // Approve tx
  const {
    writeContract: writeApprove,
    data: approveTxHash,
    isPending: isApprovePending,
  } = useWriteContract();

  const { isSuccess: isApproveConfirmed } = useWaitForTransactionReceipt({
    hash: approveTxHash,
  });

  // Deposit tx
  const {
    writeContract: writeDeposit,
    data: depositTxHash,
    isPending: isDepositPending,
  } = useWriteContract();

  const { isSuccess: isDepositConfirmed } = useWaitForTransactionReceipt({
    hash: depositTxHash,
  });

  // Handle approve confirmation
  useEffect(() => {
    if (isApproveConfirmed) {
      refetchAllowance();
      setStep("approved");
    }
  }, [isApproveConfirmed, refetchAllowance]);

  // Handle deposit confirmation
  useEffect(() => {
    if (isDepositConfirmed) {
      setStep("done");
    }
  }, [isDepositConfirmed]);

  const handleApprove = () => {
    if (parsedAmount <= 0) return;
    setStep("approving");
    writeApprove({
      address: addresses.wstETH,
      abi: ERC20_ABI,
      functionName: "approve",
      args: [addresses.stableYieldVault, parseEther(amount)],
    });
  };

  const handleDeposit = () => {
    if (parsedAmount <= 0) return;
    setStep("depositing");
    writeDeposit({
      address: addresses.stableYieldVault,
      abi: STABLE_YIELD_VAULT_ABI,
      functionName: "deposit",
      args: [selectedSeries.seriesId, parseEther(amount)],
    });
  };

  // Check if already approved enough
  const amountWei = parsedAmount > 0 ? parseEther(amount || "0") : 0n;
  const hasAllowance = allowance !== undefined && amountWei > 0n && allowance >= amountWei;

  if (!isConnected) {
    return (
      <div className="flex min-h-[60vh] flex-col items-center justify-center text-center">
        <div className="mb-6 text-6xl">🔒</div>
        <h1 className="mb-3 text-3xl font-bold text-white">
          Connect Your Wallet
        </h1>
        <p className="mb-8 max-w-md text-slate-400">
          Connect your wallet to deposit wstETH and start earning fixed-rate yield.
        </p>
        <ConnectButton />
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-2xl space-y-8">
      <div>
        <h1 className="text-3xl font-bold text-white">Deposit wstETH</h1>
        <p className="mt-2 text-slate-400">
          Lock your wstETH for a fixed yield on its stETH value. Choose a maturity,
          enter an amount, and confirm.
        </p>
      </div>

      {/* Wallet Balance */}
      <div className="rounded-xl border border-white/10 bg-white/[0.02] p-4 text-sm">
        <span className="text-slate-400">Your wstETH balance: </span>
        <span className="font-mono text-white">
          {wstETHBalance !== undefined ? Number(formatEther(wstETHBalance)).toFixed(6) : "—"} wstETH
        </span>
      </div>

      {/* Series Selector */}
      <div className="rounded-2xl border border-white/10 bg-white/[0.02] p-6">
        <label className="mb-3 block text-sm font-medium text-slate-300">
          Select Maturity Series
        </label>
        <div className="grid gap-3 sm:grid-cols-3">
          {DEMO_SERIES.map((s) => (
            <button
              key={s.id}
              onClick={() => setSelectedSeries(s)}
              className={`rounded-xl border p-4 text-center transition ${
                selectedSeries.id === s.id
                  ? "border-[#637DEA] bg-[#637DEA]/10"
                  : "border-white/10 bg-white/[0.02] hover:border-white/20"
              }`}
            >
              <div className="text-sm font-semibold text-white">{s.id}</div>
              <div className="mt-1 text-2xl font-bold text-[#4EC9B0]">
                {seriesInfo.fixedRate.toFixed(2)}%
              </div>
              <div className="text-xs text-slate-400">Fixed APR</div>
            </button>
          ))}
        </div>
      </div>

      {/* Amount Input */}
      <div className="rounded-2xl border border-white/10 bg-white/[0.02] p-6">
        <label className="mb-3 block text-sm font-medium text-slate-300">
          Amount (wstETH)
        </label>
        <div className="relative">
          <input
            type="number"
            value={amount}
            onChange={(e) => { setAmount(e.target.value); setStep("input"); }}
            placeholder="0.0"
            min="0"
            step="0.01"
            className="w-full rounded-xl border border-white/10 bg-white/5 px-5 py-4 text-2xl font-mono text-white placeholder-slate-600 outline-none transition focus:border-[#637DEA]/50 focus:ring-1 focus:ring-[#637DEA]/30"
          />
          <div className="absolute right-4 top-1/2 -translate-y-1/2 text-sm text-slate-400">
            wstETH
          </div>
        </div>
        <div className="mt-2 flex gap-2">
          {["0.1", "0.5", "1"].map((v) => (
            <button
              key={v}
              onClick={() => { setAmount(v); setStep("input"); }}
              className="rounded-lg bg-white/5 px-3 py-1 text-xs text-slate-400 transition hover:bg-white/10 hover:text-white"
            >
              {v}
            </button>
          ))}
          {wstETHBalance !== undefined && (
            <button
              onClick={() => { setAmount(formatEther(wstETHBalance)); setStep("input"); }}
              className="rounded-lg bg-white/5 px-3 py-1 text-xs text-slate-400 transition hover:bg-white/10 hover:text-white"
            >
              Max
            </button>
          )}
        </div>
      </div>

      {/* Rate Display */}
      {parsedAmount > 0 && (
        <div className="rounded-2xl border border-[#4EC9B0]/20 bg-[#4EC9B0]/5 p-6">
          <h3 className="mb-4 text-sm font-medium text-[#4EC9B0]">
            Deposit Summary
          </h3>
          <div className="space-y-3">
            <div className="flex justify-between text-sm">
              <span className="text-slate-400">Series</span>
              <span className="text-white">{selectedSeries.id}</span>
            </div>
            <div className="flex justify-between text-sm">
              <span className="text-slate-400">Fixed Rate</span>
              <span className="font-mono text-[#4EC9B0]">
                {effectiveRate.toFixed(2)}% APR
              </span>
            </div>
            <div className="flex justify-between text-sm">
              <span className="text-slate-400">Maturity Date</span>
              <span className="text-white">{maturityDate}</span>
            </div>
            <div className="flex justify-between text-sm">
              <span className="text-slate-400">Tenor</span>
              <span className="text-white">
                {Math.round(tenor * 365)} days
              </span>
            </div>
            <div className="border-t border-white/10 pt-3" />
            <div className="flex justify-between text-sm">
              <span className="text-slate-400">You Deposit</span>
              <span className="font-mono text-white">
                {parsedAmount.toFixed(4)} wstETH
                <span className="text-slate-500 ml-1">
                  ≈ {stEthValue.toFixed(4)} stETH
                </span>
              </span>
            </div>
            <div className="flex justify-between text-sm">
              <span className="text-slate-400">Fixed Yield</span>
              <span className="font-mono text-[#4EC9B0]">
                +{yieldAmountStEth.toFixed(4)} stETH
              </span>
            </div>
            <div className="flex justify-between text-base font-semibold">
              <span className="text-slate-200">You Receive at Maturity</span>
              <span className="font-mono text-white">
                {expectedPayoutStEth.toFixed(4)} stETH
              </span>
            </div>
            <div className="flex justify-between text-xs">
              <span className="text-slate-500">≈ wstETH at current rate</span>
              <span className="font-mono text-slate-500">
                ≈ {expectedPayoutWstEth.toFixed(4)} wstETH
              </span>
            </div>
          </div>
        </div>
      )}

      {/* Action Buttons */}
      <div className="space-y-3">
        {step === "input" && !hasAllowance && (
          <button
            onClick={handleApprove}
            disabled={parsedAmount <= 0}
            className={`w-full rounded-xl py-4 text-base font-semibold transition ${
              parsedAmount > 0
                ? "bg-gradient-to-r from-[#637DEA] to-[#4EC9B0] text-white shadow-lg shadow-[#637DEA]/20 hover:shadow-xl"
                : "cursor-not-allowed bg-white/5 text-slate-500"
            }`}
          >
            {parsedAmount > 0 ? "1/2 — Approve wstETH" : "Enter an amount"}
          </button>
        )}
        {step === "input" && hasAllowance && (
          <button
            onClick={handleDeposit}
            className="w-full rounded-xl bg-gradient-to-r from-[#637DEA] to-[#4EC9B0] py-4 text-base font-semibold text-white shadow-lg shadow-[#637DEA]/20 transition hover:shadow-xl"
          >
            Deposit {parsedAmount.toFixed(4)} wstETH
          </button>
        )}
        {step === "approving" && (
          <button disabled className="w-full rounded-xl bg-[#637DEA]/60 py-4 text-base font-semibold text-white/80">
            {isApprovePending ? "⏳ Confirm in wallet…" : "⏳ Waiting for confirmation…"}
          </button>
        )}
        {step === "approved" && (
          <button
            onClick={handleDeposit}
            className="w-full rounded-xl bg-gradient-to-r from-[#637DEA] to-[#4EC9B0] py-4 text-base font-semibold text-white shadow-lg shadow-[#637DEA]/20 transition hover:shadow-xl"
          >
            2/2 — Confirm Deposit
          </button>
        )}
        {step === "depositing" && (
          <button disabled className="w-full rounded-xl bg-[#637DEA]/60 py-4 text-base font-semibold text-white/80">
            {isDepositPending ? "⏳ Confirm in wallet…" : "⏳ Depositing…"}
          </button>
        )}
        {step === "done" && (
          <div className="rounded-xl border border-[#4EC9B0]/30 bg-[#4EC9B0]/10 p-6 text-center">
            <div className="mb-2 text-3xl">✅</div>
            <div className="text-lg font-semibold text-white">
              Deposit Successful
            </div>
            <p className="mt-1 text-sm text-slate-400">
              You received {parsedAmount.toFixed(4)} syLST for series{" "}
              {selectedSeries.id}. View your position on the{" "}
              <a href="/dashboard" className="text-[#637DEA] underline">
                Dashboard
              </a>
              .
            </p>
            {depositTxHash && (
              <a
                href={`https://hoodi.etherscan.io/tx/${depositTxHash}`}
                target="_blank"
                rel="noopener noreferrer"
                className="mt-2 inline-block text-sm text-[#637DEA] underline"
              >
                View on Etherscan ↗
              </a>
            )}
            <button
              onClick={() => { setStep("input"); setAmount(""); }}
              className="mt-4 block w-full rounded-lg bg-white/5 px-4 py-2 text-sm text-slate-300 transition hover:bg-white/10"
            >
              Make Another Deposit
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
