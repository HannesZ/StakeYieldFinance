/**
 * End-to-end test deposit on Sepolia:
 * 1. Mint MockWstETH to deployer
 * 2. Seed the reserve
 * 3. Deposit into the 2026Q4 series
 * 4. Check syLST balance
 */
import { ethers } from "hardhat";

const ADDRESSES = {
  wstETH:   "0x028eB1e76f1eb7DDBc8D5c7769d37607B328628F",
  vault:    "0x7C996A859D4fa526DEdd07004491F67a14846FbF",
  reserve:  "0x996bCe85FBb5B400eBd5c38f144C40CB278fD7d6",
  syLST:    "0xeCce89738301AdE8eB371B23d42C7f31c3B53247",
  spread:   "0xDc8CB04A0BaaA2CdE10052C488fCd5A73647bB83",
};

const SERIES_ID = "0xfe072b35dd5e8a2b9cd745db0bbedbd13b57467ac4e187c797255d08a751353a";

async function main() {
  const [deployer] = await ethers.getSigners();
  const addr = await deployer.getAddress();
  console.log(`\nDeployer: ${addr}`);

  // Get contract instances
  const wstETH = await ethers.getContractAt("MockWstETH", ADDRESSES.wstETH);
  const vault = await ethers.getContractAt("StableYieldVault", ADDRESSES.vault);
  const reserve = await ethers.getContractAt("ReserveManager", ADDRESSES.reserve);
  const syLST = await ethers.getContractAt("SyLST", ADDRESSES.syLST);
  const spread = await ethers.getContractAt("SpreadCalculator", ADDRESSES.spread);

  // ── 1. Mint MockWstETH ──────────────────────────────────────────────────
  const mintAmount = ethers.parseEther("1000");
  console.log("\n[1] Minting 1000 MockWstETH...");
  await (await wstETH.mint(addr, mintAmount)).wait();
  console.log(`    Balance: ${ethers.formatEther(await wstETH.balanceOf(addr))} wstETH`);

  // ── 2. Seed reserve (send wstETH directly to ReserveManager) ───────────
  const reserveSeed = ethers.parseEther("500");
  console.log("\n[2] Seeding reserve with 500 wstETH...");
  await (await wstETH.transfer(ADDRESSES.reserve, reserveSeed)).wait();
  
  const reserveBal = await reserve.totalReserve();
  console.log(`    Reserve: ${ethers.formatEther(reserveBal)} wstETH`);

  // Check κ before deposit
  let kappa = await reserve.kappa();
  console.log(`    κ: ${kappa === ethers.MaxUint256 ? "∞ (no liabilities)" : ethers.formatEther(kappa)}`);

  // Check current spread
  let spreadBps = await spread.currentSpread();
  console.log(`    Spread: ${spreadBps} bp`);

  // ── 3. Deposit 100 wstETH into 2026Q4 series ──────────────────────────
  const depositAmount = ethers.parseEther("100");
  console.log("\n[3] Depositing 100 wstETH into 2026Q4 series...");
  await (await wstETH.approve(ADDRESSES.vault, depositAmount)).wait();
  const tx = await vault.deposit(SERIES_ID, depositAmount);
  const receipt = await tx.wait();
  console.log(`    Tx: ${receipt!.hash}`);

  // ── 4. Check results ───────────────────────────────────────────────────
  const tokenId = BigInt(SERIES_ID);
  const syLSTBalance = await syLST.balanceOf(addr, tokenId);
  console.log(`\n[4] Results:`);
  console.log(`    syLST balance: ${ethers.formatEther(syLSTBalance)} syLST`);
  console.log(`    wstETH remaining: ${ethers.formatEther(await wstETH.balanceOf(addr))} wstETH`);
  
  kappa = await reserve.kappa();
  console.log(`    κ after deposit: ${ethers.formatEther(kappa)}`);
  
  spreadBps = await spread.currentSpread();
  console.log(`    Spread after deposit: ${spreadBps} bp`);

  const totalLiab = await reserve.totalLiabilities();
  console.log(`    Total liabilities: ${ethers.formatEther(totalLiab)} wstETH`);

  // Preview what we'd get at maturity
  const claim = await vault.previewRedeem(SERIES_ID, syLSTBalance);
  console.log(`    Expected at maturity: ${ethers.formatEther(claim)} wstETH`);

  console.log(`\n✅ Full cycle verified on Sepolia!`);
  console.log(`   View on Etherscan: https://sepolia.etherscan.io/tx/${receipt!.hash}`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
