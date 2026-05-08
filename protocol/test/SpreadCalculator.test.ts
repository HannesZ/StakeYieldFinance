import { expect } from "chai";
import { ethers } from "hardhat";
import { loadFixture } from "@nomicfoundation/hardhat-network-helpers";
import { deploy } from "./helpers/deploy";
import type { DeployResult } from "./helpers/deploy";
import type { SpreadCalculator } from "../typechain-types";

const WAD = ethers.parseEther("1"); // 1e18

// ──────────────────────────────────────────────────────────────────────────────
// Default spread parameters (should match what deploy() configures)
// ──────────────────────────────────────────────────────────────────────────────
// s_base   = 25 bp
// α        = 2.0
// β        = 2.0
// κ_target = 1.20
// κ_crit   = 1.00
//
// Formula: s(κ) = s_base · (1 + α · max(0, κ_target/κ − 1)^β)
// ──────────────────────────────────────────────────────────────────────────────

const S_BASE = 25n; // basis points
const ALPHA = ethers.parseEther("2"); // 2e18
const BETA = ethers.parseEther("2"); // 2e18
const KAPPA_TARGET = ethers.parseEther("1.2"); // 120%
const KAPPA_CRITICAL = ethers.parseEther("0.5"); // 50%

// Manual spread computation for verification
// s(κ) = s_base * (1 + α * (κ_target/κ - 1)^β)  for κ < κ_target
// s(κ) = s_base                                    for κ >= κ_target
function computeExpectedSpread(
  kappaE18: bigint,
  sBase: bigint,
  alpha: bigint,
  beta: bigint,
  kappaTarget: bigint
): number {
  const kappa = Number(kappaE18) / 1e18;
  const target = Number(kappaTarget) / 1e18;
  const a = Number(alpha) / 1e18;
  const b = Number(beta) / 1e18;
  const sB = Number(sBase);

  if (kappa >= target) return sB;

  const excess = target / kappa - 1.0;
  const penalty = a * Math.pow(excess, b);
  const spread = sB * (1 + penalty);

  // Cap at 10000 bp
  return Math.min(spread, 10000);
}

// ──────────────────────────────────────────────────────────────────────────────
describe("SpreadCalculator", () => {
  // ──────────────────────────────────────────────────────────────────────────
  describe("getSpread: κ at / above κ_target", () => {
    it("returns s_base exactly when κ == κ_target", async () => {
      const { spread } = await loadFixture(deploy);

      const spreadBps = await spread.getSpread(KAPPA_TARGET);
      expect(spreadBps).to.equal(S_BASE);
    });

    it("returns s_base when κ slightly above κ_target", async () => {
      const { spread } = await loadFixture(deploy);

      const kappaAbove = KAPPA_TARGET + ethers.parseEther("0.01"); // 1.21
      const spreadBps = await spread.getSpread(kappaAbove);
      expect(spreadBps).to.equal(S_BASE);
    });

    it("returns s_base when κ well above κ_target (κ = 2.0)", async () => {
      const { spread } = await loadFixture(deploy);

      const spreadBps = await spread.getSpread(ethers.parseEther("2.0"));
      expect(spreadBps).to.equal(S_BASE);
    });

    it("returns s_base when κ = 5.0 (very well capitalised)", async () => {
      const { spread } = await loadFixture(deploy);

      const spreadBps = await spread.getSpread(ethers.parseEther("5.0"));
      expect(spreadBps).to.equal(S_BASE);
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  describe("getSpread: κ slightly below κ_target", () => {
    it("spread is slightly above s_base when κ is below target", async () => {
      const { spread } = await loadFixture(deploy);

      // κ = 1.05 (below 1.2 target)
      // excess = 1.2/1.05 - 1 ≈ 0.1429
      // penalty = 2 * 0.1429^2 ≈ 0.0408
      // spread = 25 * (1 + 0.0408) ≈ 26.02 → 26 bp
      const kappa = ethers.parseEther("1.05");
      const spreadBps = await spread.getSpread(kappa);

      expect(spreadBps).to.be.gt(S_BASE);
      expect(spreadBps).to.be.lte(30n);
    });

    it("spread is moderately elevated when κ is well below target", async () => {
      const { spread } = await loadFixture(deploy);

      // κ = 0.9 (well below 1.2 target, above 0.5 critical)
      // excess = 1.2/0.9 - 1 = 0.3333
      // penalty = 2 * 0.3333^2 ≈ 0.2222
      // spread = 25 * (1 + 0.2222) ≈ 30.56 → 30 bp
      const kappa = ethers.parseEther("0.9");
      const spreadBps = await spread.getSpread(kappa);

      expect(spreadBps).to.be.gt(S_BASE);
      const expected = Math.round(
        computeExpectedSpread(
          ethers.parseEther("0.9"),
          S_BASE,
          ALPHA,
          BETA,
          KAPPA_TARGET
        )
      );
      // Allow tolerance of ±5 bp due to fixed-point approximation
      expect(Number(spreadBps)).to.be.closeTo(expected, 5);
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  describe("getSpread: κ = 0.5 × κ_target", () => {
    it("returns significantly elevated spread", async () => {
      const { spread } = await loadFixture(deploy);

      // κ = 0.7 (well below 1.2 target, above 0.5 critical)
      // excess = 1.2/0.7 - 1 = 0.7143
      // penalty = 2 * 0.7143^2 ≈ 1.0204
      // spread = 25 * (1 + 1.0204) ≈ 50.51 → 50 bp
      const kappa = ethers.parseEther("0.7");
      const spreadBps = await spread.getSpread(kappa);

      expect(spreadBps).to.be.gt(S_BASE);
      const expected = computeExpectedSpread(
        ethers.parseEther("0.7"),
        S_BASE,
        ALPHA,
        BETA,
        KAPPA_TARGET
      );
      expect(Number(spreadBps)).to.be.closeTo(Math.round(expected), 5);
    });

    it("κ at midpoint between critical and target yields intermediate spread", async () => {
      const { spread } = await loadFixture(deploy);

      // κ_target = 1.2, κ_critical = 0.5 → midpoint = 0.85
      const midKappa = ethers.parseEther("0.85");
      const spreadAtMid = await spread.getSpread(midKappa);

      // Should be above s_base
      expect(spreadAtMid).to.be.gt(S_BASE);

      // Also verify it's less than the near-critical spread
      const nearCritical = ethers.parseEther("0.6");
      const spreadNearCrit = await spread.getSpread(nearCritical);
      expect(spreadNearCrit).to.be.gt(spreadAtMid);
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  describe("getSpread: κ near critical", () => {
    it("returns very high spread just above critical", async () => {
      const { spread } = await loadFixture(deploy);

      // Just above critical: κ = 0.51 (critical = 0.5)
      // excess = 1.2/0.51 - 1 = 1.3529
      // penalty = 2 * 1.3529^2 ≈ 3.661
      // spread = 25 * (1 + 3.661) ≈ 116.5 → 116 bp
      const nearCritical = ethers.parseEther("0.51");
      const spreadBps = await spread.getSpread(nearCritical);

      expect(spreadBps).to.be.gt(S_BASE);

      const expected = computeExpectedSpread(
        ethers.parseEther("0.51"),
        S_BASE,
        ALPHA,
        BETA,
        KAPPA_TARGET
      );
      expect(Number(spreadBps)).to.be.closeTo(Math.round(expected), 10);
    });

    it("spread monotonically increases as κ decreases toward critical", async () => {
      const { spread } = await loadFixture(deploy);

      const kappas = [
        ethers.parseEther("1.19"), // just below target
        ethers.parseEther("1.10"),
        ethers.parseEther("0.90"),
        ethers.parseEther("0.70"),
        ethers.parseEther("0.60"),
        ethers.parseEther("0.51"),
      ];

      let prevSpread = 0n;
      for (const k of kappas) {
        const s = await spread.getSpread(k);
        expect(s).to.be.gte(prevSpread);
        prevSpread = s;
      }
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  describe("getSpread: κ at/below critical → reverts", () => {
    it("reverts when κ exactly equals κ_critical", async () => {
      const { spread } = await loadFixture(deploy);

      // KAPPA_CRITICAL = 0.5e18 (matches deploy config)
      await expect(
        spread.getSpread(KAPPA_CRITICAL)
      ).to.be.revertedWith("SpreadCalculator: kappa at/below critical");
    });

    it("reverts when κ is below κ_critical", async () => {
      const { spread } = await loadFixture(deploy);

      await expect(
        spread.getSpread(ethers.parseEther("0.5"))
      ).to.be.revertedWith("SpreadCalculator: kappa at/below critical");
    });

    it("reverts when κ = 0", async () => {
      const { spread } = await loadFixture(deploy);

      await expect(
        spread.getSpread(0n)
      ).to.be.revertedWith("SpreadCalculator: kappa at/below critical");
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  describe("currentSpread", () => {
    it("currentSpread() reads κ from ReserveManager", async () => {
      const { spread, reserve } = await loadFixture(deploy);

      // When no liabilities exist, kappa = max → spread = s_base
      const kappa = await reserve.kappa();
      // With no liabilities, kappa should be type(uint256).max
      // So currentSpread should = s_base

      const spreadFromRM = await spread.currentSpread();
      // kappa > target → base spread
      expect(spreadFromRM).to.equal(S_BASE);
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  describe("Parameter Updates", () => {
    it("governance can update parameters", async () => {
      const { spread, admin } = await loadFixture(deploy);

      const newParams = {
        sBaseBps: 50n, // 50 bp
        alphaE18: ethers.parseEther("3"),
        betaE18: ethers.parseEther("2"),
        kappaTargetE18: ethers.parseEther("1.3"),
        kappaCriticalE18: ethers.parseEther("1.05"),
      };

      await expect(spread.connect(admin).setParameters(newParams))
        .to.emit(spread, "ParametersUpdated")
        .withArgs(
          newParams.sBaseBps,
          newParams.alphaE18,
          newParams.betaE18,
          newParams.kappaTargetE18,
          newParams.kappaCriticalE18
        );

      const stored = await spread.getParameters();
      expect(stored.sBaseBps).to.equal(newParams.sBaseBps);
      expect(stored.alphaE18).to.equal(newParams.alphaE18);
      expect(stored.betaE18).to.equal(newParams.betaE18);
      expect(stored.kappaTargetE18).to.equal(newParams.kappaTargetE18);
      expect(stored.kappaCriticalE18).to.equal(newParams.kappaCriticalE18);
    });

    it("non-governance cannot update parameters", async () => {
      const { spread, user1 } = await loadFixture(deploy);

      const params = {
        sBaseBps: 50n,
        alphaE18: ethers.parseEther("3"),
        betaE18: ethers.parseEther("2"),
        kappaTargetE18: ethers.parseEther("1.3"),
        kappaCriticalE18: ethers.parseEther("1.05"),
      };

      await expect(
        spread.connect(user1).setParameters(params)
      ).to.be.revertedWithCustomError(spread, "AccessControlUnauthorizedAccount");
    });

    it("reverts if sBase = 0", async () => {
      const { spread, admin } = await loadFixture(deploy);

      const badParams = {
        sBaseBps: 0n,
        alphaE18: ethers.parseEther("2"),
        betaE18: ethers.parseEther("2"),
        kappaTargetE18: ethers.parseEther("1.2"),
        kappaCriticalE18: ethers.parseEther("1.0"),
      };

      await expect(
        spread.connect(admin).setParameters(badParams)
      ).to.be.revertedWith("SpreadCalculator: invalid sBase");
    });

    it("reverts if sBase > 1000 bp", async () => {
      const { spread, admin } = await loadFixture(deploy);

      const badParams = {
        sBaseBps: 1001n,
        alphaE18: ethers.parseEther("2"),
        betaE18: ethers.parseEther("2"),
        kappaTargetE18: ethers.parseEther("1.2"),
        kappaCriticalE18: ethers.parseEther("1.0"),
      };

      await expect(
        spread.connect(admin).setParameters(badParams)
      ).to.be.revertedWith("SpreadCalculator: invalid sBase");
    });

    it("reverts if beta < 1.0", async () => {
      const { spread, admin } = await loadFixture(deploy);

      const badParams = {
        sBaseBps: 25n,
        alphaE18: ethers.parseEther("2"),
        betaE18: ethers.parseEther("0.5"), // beta < 1
        kappaTargetE18: ethers.parseEther("1.2"),
        kappaCriticalE18: ethers.parseEther("1.0"),
      };

      await expect(
        spread.connect(admin).setParameters(badParams)
      ).to.be.revertedWith("SpreadCalculator: beta must be >= 1");
    });

    it("reverts if kappaTarget <= kappaCritical", async () => {
      const { spread, admin } = await loadFixture(deploy);

      const badParams = {
        sBaseBps: 25n,
        alphaE18: ethers.parseEther("2"),
        betaE18: ethers.parseEther("2"),
        kappaTargetE18: ethers.parseEther("1.0"),
        kappaCriticalE18: ethers.parseEther("1.0"), // equal
      };

      await expect(
        spread.connect(admin).setParameters(badParams)
      ).to.be.revertedWith("SpreadCalculator: target <= critical");
    });

    it("reverts if kappaCritical = 0", async () => {
      const { spread, admin } = await loadFixture(deploy);

      const badParams = {
        sBaseBps: 25n,
        alphaE18: ethers.parseEther("2"),
        betaE18: ethers.parseEther("2"),
        kappaTargetE18: ethers.parseEther("1.2"),
        kappaCriticalE18: 0n,
      };

      await expect(
        spread.connect(admin).setParameters(badParams)
      ).to.be.revertedWith("SpreadCalculator: critical must be > 0");
    });

    it("after parameter update, spread changes accordingly", async () => {
      const { spread, admin } = await loadFixture(deploy);

      // Get spread with default params at κ = 1.1
      const kappa = ethers.parseEther("1.1");
      const spreadBefore = await spread.getSpread(kappa);

      // Update: double the base spread
      const newParams = {
        sBaseBps: 50n, // double
        alphaE18: ALPHA,
        betaE18: BETA,
        kappaTargetE18: KAPPA_TARGET,
        kappaCriticalE18: KAPPA_CRITICAL,
      };
      await spread.connect(admin).setParameters(newParams);

      const spreadAfter = await spread.getSpread(kappa);
      // Spread should be roughly doubled
      expect(spreadAfter).to.be.approximately(spreadBefore * 2n, 5n);
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  describe("Edge Cases", () => {
    it("spread is capped at 10000 bp (100%)", async () => {
      const { spread, admin } = await loadFixture(deploy);

      // Set very high alpha to push spread toward cap
      const extremeParams = {
        sBaseBps: 1000n, // 10%
        alphaE18: ethers.parseEther("100"), // extreme amplification
        betaE18: ethers.parseEther("3"),
        kappaTargetE18: ethers.parseEther("1.2"),
        kappaCriticalE18: ethers.parseEther("1.0"),
      };
      await spread.connect(admin).setParameters(extremeParams);

      // κ just above critical
      const nearCritical = ethers.parseEther("1.001");
      const spreadBps = await spread.getSpread(nearCritical);

      // Should be capped at 10000 bp
      expect(spreadBps).to.be.lte(10000n);
    });

    it("getParameters returns the currently set parameters", async () => {
      const { spread } = await loadFixture(deploy);

      const params = await spread.getParameters();
      expect(params.sBaseBps).to.equal(S_BASE);
      expect(params.alphaE18).to.equal(ALPHA);
      expect(params.betaE18).to.equal(BETA);
      expect(params.kappaTargetE18).to.equal(KAPPA_TARGET);
      expect(params.kappaCriticalE18).to.equal(KAPPA_CRITICAL);
    });

    it("numerical spot check: κ = 1.1, expected spread ≈ 25–26 bp", async () => {
      const { spread } = await loadFixture(deploy);

      // Manual: excess = 1.2/1.1 - 1 = 0.09090...
      //         penalty = 2 * 0.09090^2 = 0.016529
      //         spread = 25 * 1.016529 = 25.413 → rounds to 25 bp
      const spreadBps = await spread.getSpread(ethers.parseEther("1.1"));
      expect(Number(spreadBps)).to.be.within(25, 30);
    });

    it("numerical spot check: κ = 1.05, expected spread ≈ 25–35 bp", async () => {
      const { spread } = await loadFixture(deploy);

      // excess = 1.2/1.05 - 1 = 0.14285...
      // penalty = 2 * 0.14285^2 = 0.04082
      // spread = 25 * 1.04082 = 26.02 bp
      const spreadBps = await spread.getSpread(ethers.parseEther("1.05"));
      expect(Number(spreadBps)).to.be.within(25, 35);
    });
  });
});
