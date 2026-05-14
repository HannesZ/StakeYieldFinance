"use client";

import { useState, useEffect } from "react";
import { useAccount, useReadContract, useWriteContract, useWaitForTransactionReceipt, useBalance } from "wagmi";
import { parseEther, formatEther } from "viem";
import { ConnectButton } from "@/components/ConnectButton";
import { DEMO_SERIES } from "@/lib/utils";
import { ADDRESSES, STABLE_YIELD_VAULT_ABI, ERC20_ABI, ZAP_ROUTER_ABI } from "@/lib/contracts";
import { useSeries } from "@/hooks/useSeries";
import { useExchangeRate } from "@/hooks/useExchangeRate";
import { useZapGasCost } from "@/hooks/useZapGasCost";

const addresses = ADDRESSES.hoodi;
const zapRouterDeployed = addresses.zapRouter !== "0x0000000000000000000000000000000000000000";

type DepositMode = "eth" | "wsteth";

export default function DepositPage() {
  const { address, isConnected } = useAccount();
  const [selectedSeries, setSelectedSeries] = useState(DEMO_SERIES[0]);
  const [amount, setAmount] = useState("");
  const [mode, setMode] = useState<DepositMode>(zapRouterDeployed ? "eth" : "wsteth");
  const [step, setStep] = useState<"input" | "approving" | "approved" | "depositing" | "done">("input");
  const seriesInfo = useSeries(selectedSeries.seriesId);
  const { stEthPerWstEth, toEth, toWstEth } = useExchangeRate();
  const zapGas = useZapGasCost();

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

  // Read native ETH balance
  const { data: ethBalanceData } = useBalance({
    address,
    query: { enabled: !!address },
  });

  // ETH-equivalent calculations depending on mode
  const ethDeposit = mode === "eth"
    ? parsedAmount                               // user typed ETH directly
    : toEth(parsedAmount);                       // wstETH → ETH
  const wstEthEquivalent = mode === "eth"
    ? toWstEth(parsedAmount)                     // ETH → wstETH (approximate)
    : parsedAmount;                              // user typed wstETH directly

  const ethPayout = ethDeposit * (1 + (effectiveRate / 100) * tenor);
  const ethYield = ethPayout - ethDeposit;

  const maturityDate = new Date(selectedSeries.maturity * 1000).toLocaleDateString(
    "en-US",
    { year: "numeric", month: "long", day: "numeric" }
  );

  // Read current allowance (only relevant for wstETH mode)
  const { data: allowance, refetch: refetchAllowance } = useReadContract({
    address: addresses.wstETH,
    abi: ERC20_ABI,
    functionName: "allowance",
    args: address ? [address, addresses.stableYieldVault] : undefined,
    query: { enabled: !!address && mode === "wsteth" },
  });

  // Approve tx (wstETH mode only)
  const {
    writeContract: writeApprove,
    data: approveTxHash,
    isPending: isApprovePending,
  } = useWriteContract();

  const { isSuccess: isApproveConfirmed } = useWaitForTransactionReceipt({
    hash: approveTxHash,
  });

  // Deposit tx (both modes)
  const {
    writeContract: writeDeposit,
    data: depositTxHash,
    isPending: isDepositPending,
  } = useWriteContract();

  const { isSuccess: isDepositConfirmed } = useWaitForTransactionReceipt({
    hash: depositTxHash,
  });

  useEffect(() => {
    if (isApproveConfirmed) {
      refetchAllowance();
      setStep("approved");
    }
  }, [isApproveConfirmed, refetchAllowance]);

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

    if (mode === "eth") {
      // Zap: send ETH to ZapRouter
      writeDeposit({
        address: addresses.zapRouter,
        abi: ZAP_ROUTER_ABI,
        functionName: "depositETH",
        args: [selectedSeries.seriesId],
        value: parseEther(amount),
      });
    } else {
      // Direct wstETH deposit
      writeDeposit({
        address: addresses.stableYieldVault,
        abi: STABLE_YIELD_VAULT_ABI,
        functionName: "deposit",
        args: [selectedSeries.seriesId, parseEther(amount)],
      });
    }
  };

  // Check if already approved enough (wstETH mode only)
  const amountWei = parsedAmount > 0 ? parseEther(amount || "0") : 0n;
  const hasAllowance = mode === "wsteth"
    ? (allowance !== undefined && amountWei > 0n && allowance >= amountWei)
    : true; // ETH mode doesn't need approval

  // Wallet balance helpers
  const wstEthBal = wstETHBalance !== undefined ? Number(formatEther(wstETHBalance)) : undefined;
  const ethBal = ethBalanceData ? Number(formatEther(ethBalanceData.value)) : undefined;

  // Active balance for the selected mode
  const activeBal = mode === "eth" ? ethBal : wstEthBal;
  const activeEthEquiv = activeBal !== undefined
    ? (mode === "eth" ? activeBal : toEth(activeBal))
    : undefined;
  const activeUnit = mode === "eth" ? "ETH" : "wstETH";

  if (!isConnected) {
    return (
      <div className="flex min-h-[60vh] flex-col items-center justify-center text-center">
        <div className="mb-6 text-6xl">🔒</div>
        <h1 className="mb-3 text-3xl font-bold text-white">
          Connect Your Wallet
        </h1>
        <p className="mb-8 max-w-md text-slate-400">
          Connect your wallet to deposit and start earning fixed-rate yield.
        </p>
        <ConnectButton />
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-2xl space-y-8">
      <div>
        <h1 className="text-3xl font-bold text-white">Deposit</h1>
        <p className="mt-2 text-slate-400">
          Lock your assets for a fixed yield. Choose how to deposit, pick a maturity, and confirm.
        </p>
      </div>

      {/* Deposit Mode Toggle */}
      <div className="flex items-center gap-2 rounded-xl border border-white/10 bg-white/[0.02] p-1.5">
        <button
          onClick={() => { setMode("eth"); setAmount(""); setStep("input"); }}
          disabled={!zapRouterDeployed}
          className={`flex-1 rounded-lg px-4 py-2.5 text-sm font-medium transition ${
            mode === "eth"
              ? "bg-gradient-to-r from-[#637DEA]/20 to-[#4EC9B0]/20 text-white border border-white/10"
              : "text-slate-400 hover:text-white"
          } ${!zapRouterDeployed ? "cursor-not-allowed opacity-40" : ""}`}
        >
          <span className="text-base mr-1.5">Ξ</span> Deposit ETH
          {!zapRouterDeployed && <span className="ml-1 text-xs text-slate-500">(soon)</span>}
        </button>
        <button
          onClick={() => { setMode("wsteth"); setAmount(""); setStep("input"); }}
          className={`flex-1 rounded-lg px-4 py-2.5 text-sm font-medium transition ${
            mode === "wsteth"
              ? "bg-gradient-to-r from-[#637DEA]/20 to-[#4EC9B0]/20 text-white border border-white/10"
              : "text-slate-400 hover:text-white"
          }`}
        >
          <span className="text-base mr-1.5">🔷</span> Deposit wstETH
        </button>
      </div>

      {/* Wallet Balance — ETH-equivalent primary */}
      <div className="rounded-xl border border-white/10 bg-white/[0.02] p-4 text-sm">
        <span className="text-slate-400">Your {activeUnit} balance: </span>
        <span className="font-mono text-white">
          {activeBal !== undefined
            ? mode === "eth"
              ? `${activeBal.toFixed(4)} ETH`
              : `≈ ${activeEthEquiv!.toFixed(4)} ETH`
            : "—"}
        </span>
        {activeBal !== undefined && mode === "wsteth" && (
          <span className="ml-2 font-mono text-xs text-slate-500">
            ({activeBal.toFixed(6)} wstETH)
          </span>
        )}
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
          Amount ({activeUnit})
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
            {activeUnit}
          </div>
        </div>
        {/* Live ETH-equivalent below input (wstETH mode only) */}
        {parsedAmount > 0 && mode === "wsteth" && (
          <div className="mt-1.5 text-right font-mono text-xs text-slate-500">
            ≈ {ethDeposit.toFixed(4)} ETH
          </div>
        )}
        {/* Conversion note for ETH mode */}
        {parsedAmount > 0 && mode === "eth" && (
          <div className="mt-1.5 text-right font-mono text-xs text-slate-500">
            ≈ {wstEthEquivalent.toFixed(4)} wstETH after wrapping
          </div>
        )}
        <div className="mt-2 flex gap-2">
          {(mode === "eth" ? ["0.1", "0.5", "1"] : ["0.1", "0.5", "1"]).map((v) => (
            <button
              key={v}
              onClick={() => { setAmount(v); setStep("input"); }}
              className="rounded-lg bg-white/5 px-3 py-1 text-xs text-slate-400 transition hover:bg-white/10 hover:text-white"
            >
              {v}
            </button>
          ))}
          {activeBal !== undefined && (
            <button
              onClick={() => {
                if (mode === "eth" && ethBalanceData) {
                  // Leave some gas buffer (0.01 ETH)
                  const maxEth = Number(formatEther(ethBalanceData.value)) - 0.01;
                  setAmount(maxEth > 0 ? maxEth.toFixed(6) : "0");
                } else if (mode === "wsteth" && wstETHBalance !== undefined) {
                  setAmount(formatEther(wstETHBalance));
                }
                setStep("input");
              }}
              className="rounded-lg bg-white/5 px-3 py-1 text-xs text-slate-400 transition hover:bg-white/10 hover:text-white"
            >
              Max
            </button>
          )}
        </div>
      </div>

      {/* Deposit Summary — ETH-denominated */}
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

            {/* ETH-equivalent primary display */}
            <div className="flex justify-between text-sm">
              <span className="text-slate-400">You Deposit</span>
              <div className="text-right">
                <span className="font-mono text-white">
                  {mode === "eth" ? "" : "≈ "}{ethDeposit.toFixed(4)} ETH
                </span>
                {mode === "wsteth" && (
                  <div className="font-mono text-xs text-slate-500">
                    {parsedAmount.toFixed(4)} wstETH
                  </div>
                )}
                {mode === "eth" && (
                  <div className="font-mono text-xs text-slate-500">
                    ≈ {wstEthEquivalent.toFixed(4)} wstETH after wrapping
                  </div>
                )}
              </div>
            </div>
            <div className="flex justify-between text-sm">
              <span className="text-slate-400">Fixed Yield</span>
              <span className="font-mono text-[#4EC9B0]">
                +{ethYield.toFixed(4)} ETH
              </span>
            </div>
            <div className="flex justify-between text-base font-semibold">
              <span className="text-slate-200">You Receive at Maturity</span>
              <div className="text-right">
                <span className="font-mono text-white">
                  ≈ {ethPayout.toFixed(4)} ETH
                </span>
                <div className="font-mono text-xs font-normal text-slate-500">
                  {ethPayout.toFixed(4)} stETH
                </div>
              </div>
            </div>

            {/* ETH mode: extra gas note */}
            {mode === "eth" && (
              <p className="mt-2 rounded-lg bg-white/[0.03] px-3 py-2 text-xs text-slate-500">
                Depositing ETH wraps to wstETH via Lido, adding ~125k gas to the transaction{zapGas.ready ? ` (${zapGas.text})` : ""}.
                This is paid by you as part of the normal tx fee.
              </p>
            )}
          </div>
        </div>
      )}

      {/* Exchange rate — subtle */}
      {parsedAmount > 0 && (
        <div className="text-center font-mono text-xs text-slate-500">
          1 wstETH = {stEthPerWstEth.toFixed(4)} stETH ≈ {stEthPerWstEth.toFixed(4)} ETH
        </div>
      )}

      {/* Action Buttons */}
      <div className="space-y-3">
        {step === "input" && mode === "eth" && (
          <button
            onClick={handleDeposit}
            disabled={parsedAmount <= 0 || !zapRouterDeployed}
            className={`w-full rounded-xl py-4 text-base font-semibold transition ${
              parsedAmount > 0 && zapRouterDeployed
                ? "bg-gradient-to-r from-[#637DEA] to-[#4EC9B0] text-white shadow-lg shadow-[#637DEA]/20 hover:shadow-xl"
                : "cursor-not-allowed bg-white/5 text-slate-500"
            }`}
          >
            {parsedAmount > 0
              ? `Deposit ${parsedAmount.toFixed(4)} ETH`
              : "Enter an amount"}
          </button>
        )}
        {step === "input" && mode === "wsteth" && !hasAllowance && (
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
        {step === "input" && mode === "wsteth" && hasAllowance && (
          <button
            onClick={handleDeposit}
            className="w-full rounded-xl bg-gradient-to-r from-[#637DEA] to-[#4EC9B0] py-4 text-base font-semibold text-white shadow-lg shadow-[#637DEA]/20 transition hover:shadow-xl"
          >
            Deposit ≈ {ethDeposit.toFixed(4)} ETH
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
              You deposited ≈ {ethDeposit.toFixed(4)} ETH
              {mode === "wsteth" && ` (${parsedAmount.toFixed(4)} wstETH)`}
              {mode === "eth" && ` (wrapped to ≈ ${wstEthEquivalent.toFixed(4)} wstETH)`}
              {" "}into series {selectedSeries.id}. View your position on the{" "}
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
