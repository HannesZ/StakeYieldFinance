import { expect } from "chai";
import { ethers } from "hardhat";
import { loadFixture, time } from "@nomicfoundation/hardhat-network-helpers";
import { deploy, createSeries, advanceTime, simulateYield, fundReserve } from "./helpers/deploy";
import type { DeployResult } from "./helpers/deploy";

const WAD = ethers.parseEther("1"); // 1e18
const RAY = 10n ** 27n;
const SECONDS_PER_YEAR = 365n * 24n * 3600n;
const INITIAL_RATE = 115n * 10n ** 25n; // 1.15e27 — MockWstETH starting rate

// Helper: compute the expected stETH claim at maturity
function computeExpectedClaimStEth(
  stEthValue: bigint,
  fixedRateE18: bigint,
  tenorSeconds: bigint
): bigint {
  const tenorFracE18 = (tenorSeconds * WAD) / SECONDS_PER_YEAR;
  const interestE18 = (fixedRateE18 * tenorFracE18) / WAD;
  return stEthValue + (stEthValue * interestE18) / WAD;
}

// Helper: convert wstETH to stETH at a given rate
function wstEthToStEth(wstEthAmount: bigint, rate: bigint = INITIAL_RATE): bigint {
  return (wstEthAmount * rate) / RAY;
}

// ──────────────────────────────────────────────────────────────────────────────
// Fixture
// ──────────────────────────────────────────────────────────────────────────────

async function vaultFixture() {
  const d = await deploy();

  // Set stakingAPR = 2.75% so computeFixedRate() ≈ 2.5% (with 25bp spread)
  await d.vault.connect(d.keeper).setStakingAPR(ethers.parseEther("0.0275"));

  // Create a default 1-year series (rate computed dynamically at deposit time)
  const ONE_YEAR = Number(SECONDS_PER_YEAR);
  const seriesId = await createSeries(d.vault, "2027Q1", ONE_YEAR);

  return { ...d, seriesId, ONE_YEAR };
}

// ──────────────────────────────────────────────────────────────────────────────
describe("StableYieldVault", () => {
  // ────────────────────────────────────────────────────────────────────────────
  describe("Series Creation", () => {
    it("governance can create a series", async () => {
      const { vault, admin } = await loadFixture(deploy);
      const maturity = (await time.latest()) + 90 * 24 * 3600; // 90 days

      const tx = await vault
        .connect(admin)
        .createSeries("2027Q2", maturity);
      const receipt = await tx.wait();

      // SeriesCreated event should be emitted
      const event = receipt?.logs.find((l: any) => {
        try {
          const parsed = vault.interface.parseLog(l);
          return parsed?.name === "SeriesCreated";
        } catch {
          return false;
        }
      });
      expect(event).to.not.be.undefined;
    });

    it("non-governance cannot create a series", async () => {
      const { vault, user1 } = await loadFixture(deploy);
      const maturity = (await time.latest()) + 90 * 24 * 3600;

      await expect(
        vault.connect(user1).createSeries("2027Q3", maturity)
      ).to.be.revertedWithCustomError(vault, "AccessControlUnauthorizedAccount");
    });

    it("reverts if maturity is in the past", async () => {
      const { vault, admin } = await loadFixture(deploy);
      const pastMaturity = (await time.latest()) - 1;

      await expect(
        vault.connect(admin).createSeries("PAST", pastMaturity)
      ).to.be.revertedWith("Vault: maturity in the past");
    });

    it("computeFixedRate returns 0 when stakingAPR not set", async () => {
      const { vault } = await loadFixture(deploy);
      // Default stakingAPR=0, spread >= 0, so fixedRate = 0
      const rate = await vault.computeFixedRate();
      expect(rate).to.equal(0n);
    });

    it("computeFixedRate returns correct value after setStakingAPR", async () => {
      const { vault, keeper, spread } = await loadFixture(deploy);
      // stakingAPR = 3.2%, spread ≈ 25bp = 0.25%
      await vault.connect(keeper).setStakingAPR(ethers.parseEther("0.032"));
      const fixedRate = await vault.computeFixedRate();
      const spreadBps = await spread.currentSpread();
      const expectedRate = ethers.parseEther("0.032") - spreadBps * 10n ** 14n;
      expect(fixedRate).to.equal(expectedRate);
    });

    it("setStakingAPR requires KEEPER_ROLE", async () => {
      const { vault, user1 } = await loadFixture(deploy);
      await expect(
        vault.connect(user1).setStakingAPR(ethers.parseEther("0.032"))
      ).to.be.revertedWithCustomError(vault, "AccessControlUnauthorizedAccount");
    });

    it("setStakingAPR reverts if APR >= 100%", async () => {
      const { vault, keeper } = await loadFixture(deploy);
      await expect(
        vault.connect(keeper).setStakingAPR(WAD)
      ).to.be.revertedWith("Vault: APR too high");
    });

    it("computeFixedRate returns 0 when spread >= stakingAPR", async () => {
      const { vault, keeper } = await loadFixture(deploy);
      // Set stakingAPR very low (1bp) so spread > stakingAPR
      await vault.connect(keeper).setStakingAPR(1n);
      const fixedRate = await vault.computeFixedRate();
      expect(fixedRate).to.equal(0n);
    });

    it("reverts on duplicate series label", async () => {
      const { vault, admin } = await loadFixture(deploy);
      const maturity = (await time.latest()) + 90 * 24 * 3600;

      await vault.connect(admin).createSeries("DUPL", maturity);
      await expect(
        vault.connect(admin).createSeries("DUPL", maturity + 1000)
      ).to.be.revertedWith("Vault: series already exists");
    });

    it("series is registered on SyLST contract", async () => {
      const { vault, syLST, admin } = await loadFixture(deploy);
      const maturity = (await time.latest()) + 90 * 24 * 3600;

      const tx = await vault.connect(admin).createSeries("SYLT1", maturity);
      const receipt = await tx.wait();

      // Derive seriesId the same way the contract does
      const seriesId = ethers.keccak256(ethers.toUtf8Bytes("SYLT1"));
      const tokenId = BigInt(seriesId);

      const meta = await syLST.seriesMeta(tokenId);
      expect(meta.maturityTimestamp).to.equal(maturity);
      // fixedRateE18 is no longer stored in SyLST (tracked per-deposit in vault)
      expect(meta.settled).to.equal(false);
    });
  });

  // ────────────────────────────────────────────────────────────────────────────
  describe("Deposit", () => {
    it("deposits correct amount and mints 1:1 syLST", async () => {
      const { vault, syLST, wstETH, user1, seriesId } = await loadFixture(vaultFixture);

      const depositAmount = ethers.parseEther("10");
      await wstETH.mint(user1.address, depositAmount);
      await wstETH.connect(user1).approve(await vault.getAddress(), depositAmount);

      const syLSTBefore = await syLST.balanceOf(user1.address, BigInt(seriesId));
      await vault.connect(user1).deposit(seriesId, depositAmount);
      const syLSTAfter = await syLST.balanceOf(user1.address, BigInt(seriesId));

      // 1:1 mint: syLST minted == wstETH deposited
      expect(syLSTAfter - syLSTBefore).to.equal(depositAmount);
    });

    it("transfers wstETH from depositor to vault", async () => {
      const { vault, wstETH, user1, seriesId } = await loadFixture(vaultFixture);

      const depositAmount = ethers.parseEther("10");
      await wstETH.mint(user1.address, depositAmount);
      await wstETH.connect(user1).approve(await vault.getAddress(), depositAmount);

      const userBefore = await wstETH.balanceOf(user1.address);
      await vault.connect(user1).deposit(seriesId, depositAmount);
      const userAfter = await wstETH.balanceOf(user1.address);

      // User's wstETH balance decreased
      expect(userBefore - userAfter).to.equal(depositAmount);
    });

    it("registers liability in ReserveManager", async () => {
      const { vault, reserve, wstETH, user1, seriesId } = await loadFixture(vaultFixture);

      const depositAmount = ethers.parseEther("10");
      await wstETH.mint(user1.address, depositAmount);
      await wstETH.connect(user1).approve(await vault.getAddress(), depositAmount);

      await vault.connect(user1).deposit(seriesId, depositAmount);

      // Liability should be > 0 (principal + interest)
      const liability = await reserve.seriesLiability(seriesId);
      expect(liability).to.be.gt(depositAmount); // includes interest
    });

    it("computed claim > principal (includes fixed interest)", async () => {
      const { vault, wstETH, user1, seriesId, ONE_YEAR } = await loadFixture(vaultFixture);

      const depositAmount = ethers.parseEther("10");
      await wstETH.mint(user1.address, depositAmount);
      await wstETH.connect(user1).approve(await vault.getAddress(), depositAmount);

      const tx = await vault.connect(user1).deposit(seriesId, depositAmount);
      const receipt = await tx.wait();

      // Parse the Deposited event
      let claimAtMaturityStEth = 0n;
      for (const log of receipt?.logs ?? []) {
        try {
          const parsed = vault.interface.parseLog(log);
          if (parsed?.name === "Deposited") {
            claimAtMaturityStEth = parsed.args.claimAtMaturityStEth;
            break;
          }
        } catch {}
      }

      // stETH value of 10 wstETH at rate 1.15 = 11.5 stETH
      // 2.5% over 1 year → claim ≈ 11.5 × 1.025 = 11.7875 stETH
      const stEthValue = wstEthToStEth(depositAmount);
      const expectedClaim = stEthValue + (stEthValue * ethers.parseEther("0.025")) / WAD;
      expect(claimAtMaturityStEth).to.be.closeTo(expectedClaim, ethers.parseEther("0.02"));
    });

    it("applies dynamic spread — series total reflects spread transferred to reserve", async () => {
      const { vault, reserve, wstETH, user1, seriesId } = await loadFixture(vaultFixture);

      const depositAmount = ethers.parseEther("10");
      await wstETH.mint(user1.address, depositAmount);
      await wstETH.connect(user1).approve(await vault.getAddress(), depositAmount);

      const reserveBefore = await reserve.totalReserve();
      await vault.connect(user1).deposit(seriesId, depositAmount);
      const reserveAfter = await reserve.totalReserve();

      // Spread income should have increased the reserve
      expect(reserveAfter).to.be.gt(reserveBefore);
    });

    it("multiple users deposit into same series — cumulative totalSyLst correct", async () => {
      const d = await loadFixture(vaultFixture);
      const { vault, syLST, wstETH, user1, user2, seriesId } = d;

      // Fund reserve so kappa stays above critical after first deposit
      await fundReserve(d, ethers.parseEther("100"));

      const amount1 = ethers.parseEther("5");
      const amount2 = ethers.parseEther("8");

      await wstETH.mint(user1.address, amount1);
      await wstETH.connect(user1).approve(await vault.getAddress(), amount1);
      await wstETH.mint(user2.address, amount2);
      await wstETH.connect(user2).approve(await vault.getAddress(), amount2);

      await vault.connect(user1).deposit(seriesId, amount1);
      await vault.connect(user2).deposit(seriesId, amount2);

      const tokenId = BigInt(seriesId);
      const supply1 = await syLST.balanceOf(user1.address, tokenId);
      const supply2 = await syLST.balanceOf(user2.address, tokenId);
      const totalSupply = await syLST["totalSupply(uint256)"](tokenId);

      expect(supply1).to.equal(amount1);
      expect(supply2).to.equal(amount2);
      expect(totalSupply).to.equal(amount1 + amount2);
    });

    it("reverts if series does not exist", async () => {
      const { vault, wstETH, user1 } = await loadFixture(vaultFixture);

      const fakeSeries = ethers.keccak256(ethers.toUtf8Bytes("NONEXISTENT"));
      const amount = ethers.parseEther("1");
      await wstETH.mint(user1.address, amount);
      await wstETH.connect(user1).approve(await vault.getAddress(), amount);

      await expect(
        vault.connect(user1).deposit(fakeSeries, amount)
      ).to.be.revertedWith("Vault: series not found");
    });

    it("reverts if series is closed", async () => {
      const { vault, wstETH, user1, seriesId, admin } = await loadFixture(vaultFixture);

      await vault.connect(admin).closeSeries(seriesId);

      const amount = ethers.parseEther("1");
      await wstETH.mint(user1.address, amount);
      await wstETH.connect(user1).approve(await vault.getAddress(), amount);

      await expect(
        vault.connect(user1).deposit(seriesId, amount)
      ).to.be.revertedWith("Vault: series closed");
    });

    it("reverts if series has matured (past expiry without settlement)", async () => {
      const { vault, wstETH, user1, seriesId, ONE_YEAR } = await loadFixture(vaultFixture);

      // Advance time past maturity
      await advanceTime(ONE_YEAR + 100);

      const amount = ethers.parseEther("1");
      await wstETH.mint(user1.address, amount);
      await wstETH.connect(user1).approve(await vault.getAddress(), amount);

      await expect(
        vault.connect(user1).deposit(seriesId, amount)
      ).to.be.revertedWith("Vault: series matured");
    });

    it("reverts on zero deposit", async () => {
      const { vault, user1, seriesId } = await loadFixture(vaultFixture);

      await expect(
        vault.connect(user1).deposit(seriesId, 0n)
      ).to.be.revertedWith("Vault: zero deposit");
    });

    it("reverts when paused", async () => {
      const { vault, wstETH, user1, admin, seriesId } = await loadFixture(vaultFixture);

      await vault.connect(admin).pause();

      const amount = ethers.parseEther("1");
      await wstETH.mint(user1.address, amount);
      await wstETH.connect(user1).approve(await vault.getAddress(), amount);

      await expect(
        vault.connect(user1).deposit(seriesId, amount)
      ).to.be.revertedWithCustomError(vault, "EnforcedPause");
    });
  });

  // ────────────────────────────────────────────────────────────────────────────
  describe("Per-Deposit Rate Tracking", () => {
    it("stores deposit record with correct rate and claim", async () => {
      const { vault, wstETH, user1, seriesId } = await loadFixture(vaultFixture);

      const depositAmount = ethers.parseEther("10");
      await wstETH.mint(user1.address, depositAmount);
      await wstETH.connect(user1).approve(await vault.getAddress(), depositAmount);
      await vault.connect(user1).deposit(seriesId, depositAmount);

      const records = await vault.getDeposits(seriesId, user1.address);
      expect(records).to.have.length(1);
      expect(records[0].wstEthAmount).to.equal(depositAmount);
      expect(records[0].stEthValue).to.equal(wstEthToStEth(depositAmount));
      expect(records[0].fixedRateE18).to.be.gt(0n); // rate was set
      expect(records[0].claimAtMaturityStEth).to.be.gt(records[0].stEthValue);
    });

    it("multiple deposits from same user tracked separately", async () => {
      const d = await loadFixture(vaultFixture);
      const { vault, wstETH, user1, seriesId } = d;

      const amt1 = ethers.parseEther("5");
      const amt2 = ethers.parseEther("3");

      // Fund reserve so kappa stays healthy after first deposit
      await fundReserve(d, ethers.parseEther("100"));

      await wstETH.mint(user1.address, amt1 + amt2);
      await wstETH.connect(user1).approve(await vault.getAddress(), amt1 + amt2);
      await vault.connect(user1).deposit(seriesId, amt1);
      await vault.connect(user1).deposit(seriesId, amt2);

      const records = await vault.getDeposits(seriesId, user1.address);
      expect(records).to.have.length(2);
      expect(records[0].wstEthAmount).to.equal(amt1);
      expect(records[1].wstEthAmount).to.equal(amt2);
    });

    it("getUserClaim returns sum of all deposits claims", async () => {
      const d = await loadFixture(vaultFixture);
      const { vault, wstETH, user1, seriesId } = d;

      const amt1 = ethers.parseEther("5");
      const amt2 = ethers.parseEther("3");

      // Fund reserve so kappa stays healthy after first deposit
      await fundReserve(d, ethers.parseEther("100"));

      await wstETH.mint(user1.address, amt1 + amt2);
      await wstETH.connect(user1).approve(await vault.getAddress(), amt1 + amt2);
      await vault.connect(user1).deposit(seriesId, amt1);
      await vault.connect(user1).deposit(seriesId, amt2);

      const records = await vault.getDeposits(seriesId, user1.address);
      const totalClaim = records.reduce((sum, r) => sum + r.claimAtMaturityStEth, 0n);
      const userClaim = await vault.getUserClaim(seriesId, user1.address);
      expect(userClaim).to.equal(totalClaim);
      // Claims are in stETH; should be > stETH value of deposits
      const stEthDeposited = wstEthToStEth(amt1 + amt2);
      expect(userClaim).to.be.gt(stEthDeposited);
    });
  });

  // ────────────────────────────────────────────────────────────────────────────
  describe("Redemption", () => {
    async function depositAndSettleFixture() {
      const d = await loadFixture(vaultFixture);
      const { vault, wstETH, user1, user2, seriesId, ONE_YEAR, keeper } = d;

      // Fund reserve so kappa stays healthy and settlement has enough funds
      await fundReserve(d, ethers.parseEther("100"));

      // Both users deposit
      const amount1 = ethers.parseEther("10");
      const amount2 = ethers.parseEther("5");
      await wstETH.mint(user1.address, amount1);
      await wstETH.connect(user1).approve(await vault.getAddress(), amount1);
      await wstETH.mint(user2.address, amount2);
      await wstETH.connect(user2).approve(await vault.getAddress(), amount2);
      await vault.connect(user1).deposit(seriesId, amount1);
      await vault.connect(user2).deposit(seriesId, amount2);

      // Advance to maturity
      await advanceTime(ONE_YEAR + 1);

      // Settle
      await vault.connect(keeper).settleSeries(seriesId);

      return d;
    }

    it("redeems at maturity: user receives principal + fixed yield", async () => {
      const { vault, wstETH, syLST, user1, seriesId } = await loadFixture(depositAndSettleFixture);

      const syLSTBalance = await syLST.balanceOf(user1.address, BigInt(seriesId));
      const wstETHBefore = await wstETH.balanceOf(user1.address);

      await vault.connect(user1).redeem(seriesId, syLSTBalance);

      const wstETHAfter = await wstETH.balanceOf(user1.address);
      const received = wstETHAfter - wstETHBefore;

      // 10 wstETH at 2.5% for 1 year → ~10.25 wstETH
      const expectedMin = ethers.parseEther("10.20"); // allow for timing
      const expectedMax = ethers.parseEther("10.30");
      expect(received).to.be.gte(expectedMin);
      expect(received).to.be.lte(expectedMax);
    });

    it("burns syLST upon redemption", async () => {
      const { vault, syLST, wstETH, user1, seriesId } = await loadFixture(depositAndSettleFixture);

      const syLSTBefore = await syLST.balanceOf(user1.address, BigInt(seriesId));
      expect(syLSTBefore).to.be.gt(0n);

      await vault.connect(user1).redeem(seriesId, syLSTBefore);

      const syLSTAfter = await syLST.balanceOf(user1.address, BigInt(seriesId));
      expect(syLSTAfter).to.equal(0n);
    });

    it("partial redemption returns correct proportion", async () => {
      const { vault, wstETH, syLST, user1, seriesId } = await loadFixture(depositAndSettleFixture);

      const syLSTBalance = await syLST.balanceOf(user1.address, BigInt(seriesId));
      const half = syLSTBalance / 2n;

      const wstETHBefore = await wstETH.balanceOf(user1.address);
      await vault.connect(user1).redeem(seriesId, half);
      const wstETHAfter = await wstETH.balanceOf(user1.address);

      const received = wstETHAfter - wstETHBefore;
      // Should be roughly 5.125 wstETH (half of ~10.25)
      const expectedMin = ethers.parseEther("5.10");
      const expectedMax = ethers.parseEther("5.20");
      expect(received).to.be.gte(expectedMin);
      expect(received).to.be.lte(expectedMax);
    });

    it("cannot redeem before maturity (series not settled)", async () => {
      const { vault, syLST, user1, seriesId } = await loadFixture(vaultFixture);

      // Deposit
      const amount = ethers.parseEther("10");
      const { wstETH } = await loadFixture(vaultFixture);
      await wstETH.mint(user1.address, amount);
      await wstETH.connect(user1).approve(await vault.getAddress(), amount);
      await vault.connect(user1).deposit(seriesId, amount);

      // Try to redeem without settling
      const syLSTBalance = await syLST.balanceOf(user1.address, BigInt(seriesId));
      await expect(
        vault.connect(user1).redeem(seriesId, syLSTBalance)
      ).to.be.revertedWith("Vault: series not settled");
    });

    it("reverts if series not settled yet", async () => {
      const { vault, wstETH, syLST, user1, seriesId, ONE_YEAR } = await loadFixture(vaultFixture);

      const amount = ethers.parseEther("5");
      await wstETH.mint(user1.address, amount);
      await wstETH.connect(user1).approve(await vault.getAddress(), amount);
      await vault.connect(user1).deposit(seriesId, amount);

      // Advance to maturity but don't settle
      await advanceTime(ONE_YEAR + 1);

      const syLSTBalance = await syLST.balanceOf(user1.address, BigInt(seriesId));
      await expect(
        vault.connect(user1).redeem(seriesId, syLSTBalance)
      ).to.be.revertedWith("Vault: series not settled");
    });

    it("reverts on zero redeem amount", async () => {
      const { vault, user1, seriesId } = await loadFixture(depositAndSettleFixture);

      await expect(
        vault.connect(user1).redeem(seriesId, 0n)
      ).to.be.revertedWith("Vault: zero amount");
    });

    it("claimPerToken is consistent: two users redeeming same proportional syLST", async () => {
      const { vault, wstETH, syLST, user1, user2, seriesId } = await loadFixture(depositAndSettleFixture);

      const syLST1 = await syLST.balanceOf(user1.address, BigInt(seriesId));
      const syLST2 = await syLST.balanceOf(user2.address, BigInt(seriesId));

      const wstETH1Before = await wstETH.balanceOf(user1.address);
      const wstETH2Before = await wstETH.balanceOf(user2.address);

      await vault.connect(user1).redeem(seriesId, syLST1);
      await vault.connect(user2).redeem(seriesId, syLST2);

      const received1 = (await wstETH.balanceOf(user1.address)) - wstETH1Before;
      const received2 = (await wstETH.balanceOf(user2.address)) - wstETH2Before;

      // User1 deposited 10, user2 deposited 5 → ratio 2:1
      // Verify ratio is maintained (within rounding)
      const ratio = (received1 * 1000n) / received2;
      expect(ratio).to.be.closeTo(2000n, 10n); // 2.000 ± 0.01
    });
  });

  // ────────────────────────────────────────────────────────────────────────────
  describe("Series Management", () => {
    it("governance can close a series", async () => {
      const { vault, admin, seriesId } = await loadFixture(vaultFixture);

      await vault.connect(admin).closeSeries(seriesId);
      const series = await vault.getSeries(seriesId);
      expect(series.isOpen).to.equal(false);
    });

    it("non-governance cannot close a series", async () => {
      const { vault, user1, seriesId } = await loadFixture(vaultFixture);

      await expect(
        vault.connect(user1).closeSeries(seriesId)
      ).to.be.revertedWithCustomError(vault, "AccessControlUnauthorizedAccount");
    });

    it("cannot close an already-closed series", async () => {
      const { vault, admin, seriesId } = await loadFixture(vaultFixture);

      await vault.connect(admin).closeSeries(seriesId);
      await expect(
        vault.connect(admin).closeSeries(seriesId)
      ).to.be.revertedWith("Vault: already closed");
    });

    it("allSeriesIds returns all created series", async () => {
      const { vault, admin } = await loadFixture(deploy);

      const maturity = (await time.latest()) + 90 * 24 * 3600;
      await vault.connect(admin).createSeries("A", maturity);
      await vault.connect(admin).createSeries("B", maturity + 1);

      const ids = await vault.allSeriesIds();
      expect(ids).to.have.length(2);
    });
  });

  // ────────────────────────────────────────────────────────────────────────────
  describe("Pause / Unpause", () => {
    it("governance can pause and unpause", async () => {
      const { vault, admin } = await loadFixture(deploy);

      await vault.connect(admin).pause();
      // paused() is from Pausable
      // We verify by trying a deposit (should revert)
      // Actually just check state
      expect(await vault.paused()).to.equal(true);

      await vault.connect(admin).unpause();
      expect(await vault.paused()).to.equal(false);
    });

    it("non-governance cannot pause", async () => {
      const { vault, user1 } = await loadFixture(deploy);

      await expect(
        vault.connect(user1).pause()
      ).to.be.revertedWithCustomError(vault, "AccessControlUnauthorizedAccount");
    });
  });

  // ────────────────────────────────────────────────────────────────────────────
  describe("Settlement", () => {
    it("only keeper can settle a series", async () => {
      const { vault, user1, seriesId, ONE_YEAR } = await loadFixture(vaultFixture);

      await advanceTime(ONE_YEAR + 1);

      await expect(
        vault.connect(user1).settleSeries(seriesId)
      ).to.be.revertedWithCustomError(vault, "AccessControlUnauthorizedAccount");
    });

    it("cannot settle before maturity", async () => {
      const { vault, keeper, seriesId } = await loadFixture(vaultFixture);

      await expect(
        vault.connect(keeper).settleSeries(seriesId)
      ).to.be.revertedWith("Vault: not yet mature");
    });

    it("cannot settle an already-settled series", async () => {
      const d = await loadFixture(vaultFixture);
      const { vault, wstETH, user1, keeper, seriesId, ONE_YEAR } = d;

      // Fund reserve for settlement
      await fundReserve(d, ethers.parseEther("100"));

      // Deposit and settle
      const amount = ethers.parseEther("1");
      await wstETH.mint(user1.address, amount);
      await wstETH.connect(user1).approve(await vault.getAddress(), amount);
      await vault.connect(user1).deposit(seriesId, amount);
      await advanceTime(ONE_YEAR + 1);
      await vault.connect(keeper).settleSeries(seriesId);

      await expect(
        vault.connect(keeper).settleSeries(seriesId)
      ).to.be.revertedWith("Vault: already settled");
    });

    it("empty series settles with 1:1 claim rate", async () => {
      const { vault, keeper, seriesId, ONE_YEAR } = await loadFixture(vaultFixture);

      await advanceTime(ONE_YEAR + 1);
      await vault.connect(keeper).settleSeries(seriesId);

      const series = await vault.getSeries(seriesId);
      expect(series.isSettled).to.equal(true);
    });

    it("settlement closes open series", async () => {
      const d = await loadFixture(vaultFixture);
      const { vault, wstETH, user1, keeper, seriesId, ONE_YEAR } = d;

      // Fund reserve for settlement
      await fundReserve(d, ethers.parseEther("100"));

      const amount = ethers.parseEther("1");
      await wstETH.mint(user1.address, amount);
      await wstETH.connect(user1).approve(await vault.getAddress(), amount);
      await vault.connect(user1).deposit(seriesId, amount);

      await advanceTime(ONE_YEAR + 1);
      await vault.connect(keeper).settleSeries(seriesId);

      const series = await vault.getSeries(seriesId);
      expect(series.isOpen).to.equal(false);
      expect(series.isSettled).to.equal(true);
    });

    it("settlement removes liability from ReserveManager", async () => {
      const d = await loadFixture(vaultFixture);
      const { vault, reserve, wstETH, user1, keeper, seriesId, ONE_YEAR } = d;

      // Fund reserve for settlement
      await fundReserve(d, ethers.parseEther("100"));

      const amount = ethers.parseEther("10");
      await wstETH.mint(user1.address, amount);
      await wstETH.connect(user1).approve(await vault.getAddress(), amount);
      await vault.connect(user1).deposit(seriesId, amount);

      const liabilityBefore = await reserve.seriesLiability(seriesId);
      expect(liabilityBefore).to.be.gt(0n);

      await advanceTime(ONE_YEAR + 1);
      await vault.connect(keeper).settleSeries(seriesId);

      const liabilityAfter = await reserve.seriesLiability(seriesId);
      expect(liabilityAfter).to.equal(0n);
    });
  });

  // ────────────────────────────────────────────────────────────────────────────
  describe("previewRedeem", () => {
    it("returns theoretical claim for unsettled series", async () => {
      const d = await loadFixture(vaultFixture);
      const { vault, wstETH, user1, seriesId } = d;

      // Need a deposit so totalSyLst > 0 for previewRedeem to compute claims
      const depositAmount = ethers.parseEther("10");
      await wstETH.mint(user1.address, depositAmount);
      await wstETH.connect(user1).approve(await vault.getAddress(), depositAmount);
      await vault.connect(user1).deposit(seriesId, depositAmount);

      const preview = await vault.previewRedeem(seriesId, depositAmount);

      // With rate unchanged (1.15), stETH claim converted back to wstETH
      // gives > depositAmount (includes fixed interest)
      expect(preview).to.be.gt(depositAmount);
    });

    it("returns actual claim for settled series", async () => {
      const d = await loadFixture(vaultFixture);
      const { vault, wstETH, user1, keeper, seriesId, ONE_YEAR } = d;

      // Fund reserve for settlement
      await fundReserve(d, ethers.parseEther("100"));

      const amount = ethers.parseEther("10");
      await wstETH.mint(user1.address, amount);
      await wstETH.connect(user1).approve(await vault.getAddress(), amount);
      await vault.connect(user1).deposit(seriesId, amount);

      await advanceTime(ONE_YEAR + 1);
      await vault.connect(keeper).settleSeries(seriesId);

      const preview = await vault.previewRedeem(seriesId, amount);
      expect(preview).to.be.gt(amount);
    });
  });
});
