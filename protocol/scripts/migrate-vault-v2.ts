/**
 * Migrate StableYieldVault to v2 (adds depositFor) on Hoodi testnet.
 *
 * Deploys a new vault, re-wires roles, recreates the series, and deploys a new ZapRouter.
 * Existing contracts (SyLST, ReserveManager, SpreadCalculator, SYLD) are reused.
 *
 * Usage:
 *   npx hardhat run scripts/migrate-vault-v2.ts --network hoodi
 */

import { ethers } from "hardhat";
import * as fs from "fs";
import * as path from "path";

const HOODI_STETH  = "0x3508A952176b3c15387C97BE809eaffB1982176a";
const HOODI_WSTETH = "0x7E99eE3C66636DE415D2d7C880938F2f40f94De4";

// Series config (must match original deployment)
const SERIES_LABEL   = "2026Q4";
const SERIES_MATURITY = Math.floor(new Date("2026-12-31T23:59:59Z").getTime() / 1000);
const STAKING_APR     = ethers.parseEther("0.035"); // 3.5%

async function main() {
  const [deployer] = await ethers.getSigners();
  const deployerAddr = await deployer.getAddress();

  console.log("═══════════════════════════════════════════════════════════");
  console.log("  Vault v2 Migration — Hoodi");
  console.log("═══════════════════════════════════════════════════════════");
  console.log(`  Deployer: ${deployerAddr}`);
  console.log(`  Balance:  ${ethers.formatEther(await ethers.provider.getBalance(deployerAddr))} ETH`);
  console.log("");

  // Load existing deployment
  const deploymentPath = path.join(__dirname, "..", "deployments", "hoodi.json");
  if (!fs.existsSync(deploymentPath)) {
    throw new Error("No deployment found at deployments/hoodi.json");
  }
  const deployment = JSON.parse(fs.readFileSync(deploymentPath, "utf-8"));

  const syLSTAddr   = deployment.SyLST;
  const reserveAddr = deployment.ReserveManager;
  const spreadAddr  = deployment.SpreadCalculator;
  const oldVaultAddr = deployment.StableYieldVault;

  console.log("  Existing contracts:");
  console.log(`    SyLST:            ${syLSTAddr}`);
  console.log(`    ReserveManager:   ${reserveAddr}`);
  console.log(`    SpreadCalculator: ${spreadAddr}`);
  console.log(`    Old Vault:        ${oldVaultAddr}`);
  console.log("");

  const VAULT_ROLE  = ethers.keccak256(ethers.toUtf8Bytes("VAULT_ROLE"));
  const KEEPER_ROLE = ethers.keccak256(ethers.toUtf8Bytes("KEEPER_ROLE"));

  // ── 1. Revoke VAULT_ROLE from old vault ────────────────────────────────────

  console.log("  [1/7] Revoking VAULT_ROLE from old vault...");
  const syLST = await ethers.getContractAt("SyLST", syLSTAddr);
  const reserve = await ethers.getContractAt("ReserveManager", reserveAddr);

  await (await syLST.revokeRole(VAULT_ROLE, oldVaultAddr)).wait();
  console.log("         SyLST: revoked ✓");
  await (await reserve.revokeRole(VAULT_ROLE, oldVaultAddr)).wait();
  console.log("         ReserveManager: revoked ✓");

  // ── 2. Deploy new StableYieldVault ─────────────────────────────────────────

  console.log("  [2/7] Deploying StableYieldVault v2...");
  const Vault = await ethers.getContractFactory("StableYieldVault");
  const vault = await Vault.deploy(
    deployerAddr,
    HOODI_WSTETH,
    syLSTAddr,
    reserveAddr,
    spreadAddr
  );
  await vault.waitForDeployment();
  const vaultAddr = await vault.getAddress();
  console.log(`         New Vault: ${vaultAddr}`);

  // ── 3. Grant VAULT_ROLE to new vault ───────────────────────────────────────

  console.log("  [3/7] Granting VAULT_ROLE to new vault...");
  await (await syLST.grantRole(VAULT_ROLE, vaultAddr)).wait();
  console.log("         SyLST: granted ✓");
  await (await reserve.grantRole(VAULT_ROLE, vaultAddr)).wait();
  console.log("         ReserveManager: granted ✓");

  // ── 4. Grant KEEPER_ROLE to deployer on new vault ──────────────────────────

  console.log("  [4/7] Granting KEEPER_ROLE on new vault...");
  await (await vault.grantRole(KEEPER_ROLE, deployerAddr)).wait();
  console.log("         Vault.KEEPER_ROLE → deployer ✓");

  // ── 5. Set staking APR and create series ───────────────────────────────────

  console.log("  [5/7] Setting staking APR and creating series...");
  await (await vault.setStakingAPR(STAKING_APR)).wait();
  const computedRate = await vault.computeFixedRate();
  console.log(`         Staking APR: 3.5%  →  Fixed rate: ${(Number(computedRate) / 1e16).toFixed(2)}%`);

  await (await vault.createSeries(SERIES_LABEL, SERIES_MATURITY)).wait();
  const seriesId = ethers.keccak256(ethers.toUtf8Bytes(SERIES_LABEL));
  console.log(`         Series "${SERIES_LABEL}": ${seriesId}`);

  // ── 6. Deploy new ZapRouter ────────────────────────────────────────────────

  console.log("  [6/7] Deploying ZapRouter...");
  const ZapRouter = await ethers.getContractFactory("ZapRouter");
  const zap = await ZapRouter.deploy(HOODI_STETH, HOODI_WSTETH, vaultAddr);
  await zap.waitForDeployment();
  const zapAddr = await zap.getAddress();
  console.log(`         ZapRouter: ${zapAddr}`);

  // ── 7. Update deployment file ──────────────────────────────────────────────

  console.log("  [7/7] Updating deployment file...");
  deployment.StableYieldVault_v1 = oldVaultAddr;
  deployment.StableYieldVault = vaultAddr;
  deployment.ZapRouter = zapAddr;
  deployment.migratedAt = new Date().toISOString();
  fs.writeFileSync(deploymentPath, JSON.stringify(deployment, null, 2) + "\n");
  console.log("         deployments/hoodi.json updated ✓");

  // ── Summary ────────────────────────────────────────────────────────────────

  console.log("");
  console.log("═══════════════════════════════════════════════════════════");
  console.log("  Migration Complete");
  console.log("═══════════════════════════════════════════════════════════");
  console.log(`
  New addresses:
    StableYieldVault: ${vaultAddr}
    ZapRouter:        ${zapAddr}

  Update website/lib/contracts.ts:
    stableYieldVault: "${vaultAddr}"
    zapRouter:        "${zapAddr}"
  `);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
