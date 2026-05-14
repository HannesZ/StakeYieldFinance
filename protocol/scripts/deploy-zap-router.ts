/**
 * Deploy ZapRouter to Hoodi testnet.
 *
 * Usage:
 *   npx hardhat run scripts/deploy-zap-router.ts --network hoodi
 *
 * Reads vault address from deployments/hoodi.json.
 */

import { ethers } from "hardhat";
import * as fs from "fs";
import * as path from "path";

const HOODI_STETH  = "0x3508A952176b3c15387C97BE809eaffB1982176a";
const HOODI_WSTETH = "0x7E99eE3C66636DE415D2d7C880938F2f40f94De4";

async function main() {
  const [deployer] = await ethers.getSigners();
  const deployerAddr = await deployer.getAddress();

  console.log("═══════════════════════════════════════════════════════════");
  console.log("  ZapRouter — Hoodi Deployment");
  console.log("═══════════════════════════════════════════════════════════");
  console.log(`  Deployer: ${deployerAddr}`);
  console.log(`  Balance:  ${ethers.formatEther(await ethers.provider.getBalance(deployerAddr))} ETH`);
  console.log("");

  // Load existing deployment
  const deploymentPath = path.join(__dirname, "..", "deployments", "hoodi.json");
  if (!fs.existsSync(deploymentPath)) {
    throw new Error("No deployment found at deployments/hoodi.json — run deploy-hoodi.ts first.");
  }
  const deployment = JSON.parse(fs.readFileSync(deploymentPath, "utf-8"));
  const vaultAddr = deployment.StableYieldVault;

  console.log(`  Vault:   ${vaultAddr}`);
  console.log(`  stETH:   ${HOODI_STETH}`);
  console.log(`  wstETH:  ${HOODI_WSTETH}`);
  console.log("");

  // Deploy
  console.log("  Deploying ZapRouter...");
  const ZapRouter = await ethers.getContractFactory("ZapRouter");
  const zap = await ZapRouter.deploy(HOODI_STETH, HOODI_WSTETH, vaultAddr);
  await zap.waitForDeployment();
  const zapAddr = await zap.getAddress();
  console.log(`  ZapRouter: ${zapAddr}`);

  // Update deployment file
  deployment.ZapRouter = zapAddr;
  deployment.zapDeployedAt = new Date().toISOString();
  fs.writeFileSync(deploymentPath, JSON.stringify(deployment, null, 2) + "\n");
  console.log(`  Updated deployments/hoodi.json`);

  console.log("");
  console.log("  Done. Users can now call ZapRouter.depositETH(seriesId) with ETH.");
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
