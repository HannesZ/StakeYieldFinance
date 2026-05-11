"use client";

import { useState, useEffect } from "react";

/**
 * Fetch the current ETH staking APR from Lido's API (7-day SMA).
 * Falls back to null if unavailable.
 */
export function useSpotAPR(): { spotAPR: number | null; isLoading: boolean; source: string } {
  const [spotAPR, setSpotAPR] = useState<number | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [source, setSource] = useState("loading");

  useEffect(() => {
    let cancelled = false;

    async function fetchAPR() {
      try {
        // Lido's published 7-day simple moving average APR
        const res = await fetch("https://eth-api.lido.fi/v1/protocol/steth/apr/sma", {
          next: { revalidate: 300 }, // cache 5 min
        });
        if (!cancelled && res.ok) {
          const data = await res.json();
          // Response: { data: { smaApr: "2.8", ... } }
          const apr = parseFloat(data?.data?.smaApr);
          if (!isNaN(apr)) {
            setSpotAPR(apr);
            setSource("Lido 7d SMA");
            setIsLoading(false);
            return;
          }
        }
      } catch {
        // Lido API unavailable — try fallback
      }

      try {
        // Fallback: Lido v1 last APR
        const res = await fetch("https://eth-api.lido.fi/v1/protocol/steth/apr/last");
        if (!cancelled && res.ok) {
          const data = await res.json();
          const apr = parseFloat(data?.data?.apr);
          if (!isNaN(apr)) {
            setSpotAPR(apr);
            setSource("Lido last APR");
            setIsLoading(false);
            return;
          }
        }
      } catch {
        // Both failed
      }

      if (!cancelled) {
        setIsLoading(false);
        setSource("unavailable");
      }
    }

    fetchAPR();
    return () => { cancelled = true; };
  }, []);

  return { spotAPR, isLoading, source };
}
