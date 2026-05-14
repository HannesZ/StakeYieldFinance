/**
 * Fix the v2 migration: redeploy SyLST, re-wire, create series, deploy ZapRouter.
 *
 * The previous migration deployed the new vault (0xbd7C…) but createSeries failed
 * because the old SyLST already had 2026Q4 registered. This script deploys a fresh
 * SyLST and finishes the migration.
 *
 * Usage:
 *   npx hardhat run scripts/migrate-vault-v2-fix.ts --network hoodi
 */

import { ethers } from "hardhat";
import * as fs from "fs";
import * as path from "path";

const HOODI_STETH  = "0x3508A952176b3c15387C97BE809eaffB1982176a";
const HOODI_WSTETH = "0x7E99eE3C66636DE415D2d7C880938F2f40f94De4";

const NEW_VAULT     = "0xbd7C28449034Fbb636327746AfE7b574058C51E5";
const RESERVE_ADDR  = "0xADf826DF9f5d260FA60202c6520f3ECB530a0a72";
const SPREAD_ADDR   = "0x883Af902FeBEd81fD03F93d1B0aDA6A53e3DeF1a";

const SERIES_LABEL   = "2026Q4";
const SERIES_MATURITY = Math.floor(new Date("2026-12-31T23:59:59Z").getTime() / 1000);
const STAKING_APR     = ethers.parseEther("0.035");

async function main() {
  const [deployer] = await ethers.getSigners();
  const deployerAddr = await deployer.getAddress();

  console.log("═══════════════════════════════════════════════════════════");
  console.log("  Vault v2 Migration — Fix (redeploy SyLST)");
  console.log("═══════════════════════════════════════════════════════════");
  console.log(`  Deployer: ${deployerAddr}`);
  console.log(`  Balance:  ${ethers.formatEther(await ethers.provider.getBalance(deployerAddr))} ETH`);
  console.log("");

  const VAULT_ROLE  = ethers.keccak256(ethers.toUtf8Bytes("VAULT_ROLE"));
  const KEEPER_ROLE = ethers.keccak256(ethers.toUtf8Bytes("KEEPER_ROLE"));

  // ── 1. Deploy fresh SyLST ─────────────────────────────────────────────────

  console.log("  [1/6] Deploying fresh SyLST...");
  const SyLST = await ethers.getContractFactory("SyLST");
  const syLST = await SyLST.deploy(
    deployerAddr,
    "https://api.stakeyield.finance/syLST/{id}.json"
  );
  await syLST.waitForDeployment();
  const syLSTAddr = await syLST.getAddress();
  console.log(`         SyLST: ${syLSTAddr}`);

  // ── 2. Grant VAULT_ROLE on new SyLST to the new vault ─────────────────────

  console.log("  [2/6] Granting VAULT_ROLE on new SyLST...");
  await (await syLST.grantRole(VAULT_ROLE, NEW_VAULT)).wait();
  console.log("         SyLST.VAULT_ROLE → vault ✓");

  // ── 3. Redeploy vault pointing to new SyLST ───────────────────────────────
  //    The vault at 0xbd7C has the OLD SyLST address baked in as immutable.
  //    We need a fresh vault pointing to the new SyLST.

  console.log("  [3/6] Deploying StableYieldVault v2 (with new SyLST)...");

  // First revoke VAULT_ROLE from partially-migrated vault on ReserveManager
  const reserve = await ethers.getContractAt("ReserveManager", RESERVE_ADDR);
  try {
    await (await reserve.revokeRole(VAULT_ROLE, NEW_VAULT)).wait();
    console.log("         Revoked VAULT_ROLE from partial vault on ReserveManager ✓");
  } catch {
    console.log("         (partial vault didn't have VAULT_ROLE on ReserveManager, skipping)");
  }

  const Vault = await ethers.getContractFactory("StableYieldVault");
  const vault = await Vault.deploy(
    deployerAddr,
    HOODI_WSTETH,
    syLSTAddr,      // ← new SyLST
    RESERVE_ADDR,
    SPREAD_ADDR
  );
  await vault.waitForDeployment();
  const vaultAddr = await vault.getAddress();
  console.log(`         Vault: ${vaultAddr}`);

  // ── 4. Wire roles ─────────────────────────────────────────────────────────

  console.log("  [4/6] Wiring roles...");
  await (await reserve.grantRole(VAULT_ROLE, vaultAddr)).wait();
  console.log("         ReserveManager.VAULT_ROLE → vault ✓");
  await (await vault.grantRole(KEEPER_ROLE, deployerAddr)).wait();
  console.log("         Vault.KEEPER_ROLE → deployer ✓");

  // ── 5. Set staking APR + create series ─────────────────────────────────────

  console.log("  [5/6] Setting staking APR and creating series...");
  await (await vault.setStakingAPR(STAKING_APR)).wait();
  const computedRate = await vault.computeFixedRate();
  console.log(`         Staking APR: 3.5%  →  Fixed rate: ${(Number(computedRate) / 1e16).toFixed(2)}%`);

  await (await vault.createSeries(SERIES_LABEL, SERIES_MATURITY)).wait();
  const seriesId = ethers.keccak256(ethers.toUtf8Bytes(SERIES_LABEL));
  console.log(`         Series "${SERIES_LABEL}": ${seriesId} ✓`);

  // ── 6. Deploy ZapRouter ────────────────────────────────────────────────────

  console.log("  [6/6] Deploying ZapRouter...");
  const ZapRouter = await ethers.getContractFactory("ZapRouter");
  const zap = await ZapRouter.deploy(HOODI_STETH, HOODI_WSTETH, vaultAddr);
  await zap.waitForDeployment();
  const zapAddr = await zap.getAddress();
  console.log(`         ZapRouter: ${zapAddr}`);

  // ── Save ───────────────────────────────────────────────────────────────────

  const deploymentPath = path.join(__dirname, "..", "deployments", "hoodi.json");
  const deployment = JSON.parse(fs.readFileSync(deploymentPath, "utf-8"));
  deployment.SyLST_v1 = deployment.SyLST;
  deployment.SyLST = syLSTAddr;
  deployment.StableYieldVault_v1 = deployment.StableYieldVault_v1 || deployment.StableYieldVault;
  deployment.StableYieldVault_v2_partial = NEW_VAULT;
  deployment.StableYieldVault = vaultAddr;
  deployment.ZapRouter = zapAddr;
  deployment.migratedAt = new Date().toISOString();
  fs.writeFileSync(deploymentPath, JSON.stringify(deployment, null, 2) + "\n");
  console.log("         deployments/hoodi.json updated ✓");

  console.log("");
  console.log("═══════════════════════════════════════════════════════════");
  console.log("  Migration Complete");
  console.log("═══════════════════════════════════════════════════════════");
  console.log(`
  New addresses (update website/lib/contracts.ts):
    stableYieldVault: "${vaultAddr}"
    syLST:            "${syLSTAddr}"
    zapRouter:        "${zapAddr}"
  `);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
