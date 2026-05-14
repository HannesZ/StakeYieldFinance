/**
 * Finish v2 migration: grant VAULT_ROLE to the correct vault, create series, deploy ZapRouter.
 *
 * State after the failed fix script:
 *   - New SyLST:  0xD787DD8Db0a1F40D2CAC9d2da138F26B4159C398  (fresh, no series registered)
 *   - New Vault:   0x18849aDE3838DA311bfD79e55F3bB0F4Ee470E01  (deployed, APR set, NO VAULT_ROLE on SyLST)
 *   - Reserve:     0xADf826DF9f5d260FA60202c6520f3ECB530a0a72  (VAULT_ROLE granted to new vault ✓)
 *
 * This script:
 *   1. Revokes VAULT_ROLE from the partial vault on the new SyLST
 *   2. Grants VAULT_ROLE to the correct new vault on the new SyLST
 *   3. Creates the 2026Q4 series
 *   4. Deploys ZapRouter
 *
 * Usage:
 *   npx hardhat run scripts/migrate-vault-v2-finish.ts --network hoodi
 */

import { ethers } from "hardhat";
import * as fs from "fs";
import * as path from "path";

const HOODI_STETH   = "0x3508A952176b3c15387C97BE809eaffB1982176a";
const HOODI_WSTETH  = "0x7E99eE3C66636DE415D2d7C880938F2f40f94De4";

const NEW_SYLST     = "0xD787DD8Db0a1F40D2CAC9d2da138F26B4159C398";
const NEW_VAULT     = "0x18849aDE3838DA311bfD79e55F3bB0F4Ee470E01";
const PARTIAL_VAULT = "0xbd7C28449034Fbb636327746AfE7b574058C51E5";

const SERIES_LABEL   = "2026Q4";
const SERIES_MATURITY = Math.floor(new Date("2026-12-31T23:59:59Z").getTime() / 1000);

async function main() {
  const [deployer] = await ethers.getSigners();
  const deployerAddr = await deployer.getAddress();

  console.log("═══════════════════════════════════════════════════════════");
  console.log("  Vault v2 Migration — Finish");
  console.log("═══════════════════════════════════════════════════════════");
  console.log(`  Deployer: ${deployerAddr}`);
  console.log(`  Balance:  ${ethers.formatEther(await ethers.provider.getBalance(deployerAddr))} ETH`);
  console.log("");

  const VAULT_ROLE = ethers.keccak256(ethers.toUtf8Bytes("VAULT_ROLE"));
  const syLST = await ethers.getContractAt("SyLST", NEW_SYLST);
  const vault = await ethers.getContractAt("StableYieldVault", NEW_VAULT);

  // ── 1. Fix VAULT_ROLE on new SyLST ────────────────────────────────────────

  console.log("  [1/4] Fixing VAULT_ROLE on SyLST...");

  // Revoke from partial vault (was granted in error)
  const partialHasRole = await syLST.hasRole(VAULT_ROLE, PARTIAL_VAULT);
  if (partialHasRole) {
    await (await syLST.revokeRole(VAULT_ROLE, PARTIAL_VAULT)).wait();
    console.log(`         Revoked from partial vault (${PARTIAL_VAULT}) ✓`);
  }

  // Grant to the correct new vault
  const newHasRole = await syLST.hasRole(VAULT_ROLE, NEW_VAULT);
  if (!newHasRole) {
    await (await syLST.grantRole(VAULT_ROLE, NEW_VAULT)).wait();
    console.log(`         Granted to new vault (${NEW_VAULT}) ✓`);
  } else {
    console.log(`         New vault already has VAULT_ROLE ✓`);
  }

  // ── 2. Create series ──────────────────────────────────────────────────────

  console.log("  [2/4] Creating series...");
  await (await vault.createSeries(SERIES_LABEL, SERIES_MATURITY)).wait();
  const seriesId = ethers.keccak256(ethers.toUtf8Bytes(SERIES_LABEL));
  console.log(`         Series "${SERIES_LABEL}": ${seriesId} ✓`);

  // ── 3. Deploy ZapRouter ────────────────────────────────────────────────────

  console.log("  [3/4] Deploying ZapRouter...");
  const ZapRouter = await ethers.getContractFactory("ZapRouter");
  const zap = await ZapRouter.deploy(HOODI_STETH, HOODI_WSTETH, NEW_VAULT);
  await zap.waitForDeployment();
  const zapAddr = await zap.getAddress();
  console.log(`         ZapRouter: ${zapAddr} ✓`);

  // ── 4. Update deployment file ──────────────────────────────────────────────

  console.log("  [4/4] Updating deployment file...");
  const deploymentPath = path.join(__dirname, "..", "deployments", "hoodi.json");
  const deployment = JSON.parse(fs.readFileSync(deploymentPath, "utf-8"));
  deployment.SyLST = NEW_SYLST;
  deployment.StableYieldVault = NEW_VAULT;
  deployment.ZapRouter = zapAddr;
  deployment.migratedAt = new Date().toISOString();
  fs.writeFileSync(deploymentPath, JSON.stringify(deployment, null, 2) + "\n");
  console.log("         deployments/hoodi.json updated ✓");

  console.log("");
  console.log("═══════════════════════════════════════════════════════════");
  console.log("  Migration Complete! ✓");
  console.log("═══════════════════════════════════════════════════════════");
  console.log(`
  Final addresses (update website/lib/contracts.ts):
    stableYieldVault: "${NEW_VAULT}"
    syLST:            "${NEW_SYLST}"
    zapRouter:        "${zapAddr}"
  `);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
