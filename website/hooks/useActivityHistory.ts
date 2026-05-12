import { useEffect, useState } from "react";
import { usePublicClient } from "wagmi";
import { formatEther, type Log, parseAbiItem } from "viem";
import { ADDRESSES } from "@/lib/contracts";

const addresses = ADDRESSES.hoodi;

/** Hoodi deployment block — skip scanning ancient history. */
const DEPLOY_BLOCK = 0n; // TODO: set to actual deploy block to speed up queries

const DEPOSITED_EVENT = parseAbiItem(
  "event Deposited(bytes32 indexed seriesId, address indexed depositor, uint256 wstEthAmount, uint256 syLstMinted, uint256 claimAtMaturity)"
);
const REDEEMED_EVENT = parseAbiItem(
  "event Redeemed(bytes32 indexed seriesId, address indexed redeemer, uint256 syLstBurned, uint256 wstEthReturned)"
);

export type ActivityType = "deposit" | "redeem";

export interface Activity {
  type: ActivityType;
  txHash: `0x${string}`;
  blockNumber: bigint;
  timestamp: number; // unix seconds
  /** wstETH amount (human-readable) */
  amount: number;
  /** syLST amount (human-readable) */
  syLstAmount: number;
  /** For deposits: claim at maturity (human-readable) */
  claimAtMaturity?: number;
}

export function useActivityHistory(userAddress: `0x${string}` | undefined) {
  const client = usePublicClient({ chainId: 560048 });
  const [activities, setActivities] = useState<Activity[]>([]);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    if (!client || !userAddress) {
      setActivities([]);
      return;
    }

    let cancelled = false;

    async function fetchLogs() {
      setIsLoading(true);
      try {
        // Fetch deposit and redeem logs in parallel
        const [depositLogs, redeemLogs] = await Promise.all([
          client!.getLogs({
            address: addresses.stableYieldVault,
            event: DEPOSITED_EVENT,
            args: { depositor: userAddress },
            fromBlock: DEPLOY_BLOCK,
            toBlock: "latest",
          }),
          client!.getLogs({
            address: addresses.stableYieldVault,
            event: REDEEMED_EVENT,
            args: { redeemer: userAddress },
            fromBlock: DEPLOY_BLOCK,
            toBlock: "latest",
          }),
        ]);

        if (cancelled) return;

        // Collect unique block numbers to batch-fetch timestamps
        const blockNumbers = new Set<bigint>();
        for (const l of [...depositLogs, ...redeemLogs]) {
          if (l.blockNumber != null) blockNumbers.add(l.blockNumber);
        }

        const blockTimestamps = new Map<bigint, number>();
        await Promise.all(
          [...blockNumbers].map(async (bn) => {
            const block = await client!.getBlock({ blockNumber: bn });
            blockTimestamps.set(bn, Number(block.timestamp));
          })
        );

        if (cancelled) return;

        const items: Activity[] = [];

        for (const log of depositLogs) {
          const args = log.args;
          items.push({
            type: "deposit",
            txHash: log.transactionHash!,
            blockNumber: log.blockNumber!,
            timestamp: blockTimestamps.get(log.blockNumber!) ?? 0,
            amount: Number(formatEther(args.wstEthAmount ?? 0n)),
            syLstAmount: Number(formatEther(args.syLstMinted ?? 0n)),
            claimAtMaturity: Number(formatEther(args.claimAtMaturity ?? 0n)),
          });
        }

        for (const log of redeemLogs) {
          const args = log.args;
          items.push({
            type: "redeem",
            txHash: log.transactionHash!,
            blockNumber: log.blockNumber!,
            timestamp: blockTimestamps.get(log.blockNumber!) ?? 0,
            amount: Number(formatEther(args.wstEthReturned ?? 0n)),
            syLstAmount: Number(formatEther(args.syLstBurned ?? 0n)),
          });
        }

        // Sort newest first
        items.sort((a, b) => Number(b.blockNumber - a.blockNumber));

        if (!cancelled) setActivities(items);
      } catch (err) {
        console.error("Failed to fetch activity history:", err);
        if (!cancelled) setActivities([]);
      } finally {
        if (!cancelled) setIsLoading(false);
      }
    }

    fetchLogs();
    return () => { cancelled = true; };
  }, [client, userAddress]);

  return { activities, isLoading };
}
