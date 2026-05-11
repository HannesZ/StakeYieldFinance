/**
 * Verify deployed contracts on Hoodi block explorer.
 *
 * Usage:
 *   npx hardhat run scripts/verify-hoodi.ts --network hoodi
 *
 * Reads addresses from deployments/hoodi.json (created by deploy-hoodi.ts).
 *
 * Note: Hoodi may not have a verified Etherscan-compatible explorer yet.
 *       This script is ready for when one becomes available. In the meantime,
 *       it prints the verify commands for manual execution.
 */

import { ethers, run } from "hardhat";
import * as fs from "fs";
import * as path from "path";

// ─── Deployment Parameters (must match deploy-hoodi.ts) ──────────────────────

const INITIAL_SYLD_SUPPLY = ethers.parseEther("10000000");

const SPREAD_PARAMS = {
  sBaseBps: 25n,
  alphaE18: ethers.parseEther("10"),
  betaE18: ethers.parseEther("3"),
  kappaTargetE18: ethers.parseEther("1.5"),
  kappaCriticalE18: ethers.parseEther("0.3"),
};

const KAPPA_TARGET = ethers.parseEther("1.5");
const KAPPA_EMERGENCY = ethers.parseEther("1.05");

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const deploymentPath = path.join(__dirname, "..", "deployments", "hoodi.json");

  if (!fs.existsSync(deploymentPath)) {
    console.error("  ✗ No deployment found at deployments/hoodi.json");
    console.error("  Run deploy-hoodi.ts first.");
    process.exitCode = 1;
    return;
  }

  const deployment = JSON.parse(fs.readFileSync(deploymentPath, "utf-8"));
  console.log("═══════════════════════════════════════════════════════════");
  console.log("  StableYield Protocol — Hoodi Contract Verification");
  console.log("═══════════════════════════════════════════════════════════");
  console.log(`  Deployed: ${deployment.deployedAt}`);
  console.log("");

  const contracts = [
    {
      name: "SYLDToken",
      address: deployment.SYLDToken,
      args: [deployment.deployer, deployment.deployer, INITIAL_SYLD_SUPPLY],
    },
    {
      name: "SyLST",
      address: deployment.SyLST,
      args: [deployment.deployer, "https://api.stakeyield.finance/syLST/{id}.json"],
    },
    {
      name: "ReserveManager",
      address: deployment.ReserveManager,
      args: [
        deployment.deployer,
        deployment.wstETH,
        deployment.SYLDToken,
        deployment.deployer, // syldStaking placeholder
        KAPPA_TARGET,
        KAPPA_EMERGENCY,
      ],
    },
    {
      name: "SpreadCalculator",
      address: deployment.SpreadCalculator,
      args: [deployment.deployer, deployment.ReserveManager, SPREAD_PARAMS],
    },
    {
      name: "StableYieldVault",
      address: deployment.StableYieldVault,
      args: [
        deployment.deployer,
        deployment.wstETH,
        deployment.SyLST,
        deployment.ReserveManager,
        deployment.SpreadCalculator,
      ],
    },
  ];

  // Add MockWstETH if it was deployed
  if (deployment.usedMockWstETH === "true") {
    contracts.unshift({
      name: "MockWstETH",
      address: deployment.wstETH,
      args: [],
    });
  }

  for (const contract of contracts) {
    console.log(`  Verifying ${contract.name} at ${contract.address}...`);
    try {
      await run("verify:verify", {
        address: contract.address,
        constructorArguments: contract.args,
      });
      console.log(`  ✓ ${contract.name} verified`);
    } catch (error: any) {
      if (error.message?.includes("Already Verified")) {
        console.log(`  ✓ ${contract.name} already verified`);
      } else {
        console.log(`  ✗ ${contract.name}: ${error.message}`);
        console.log(`    (This is expected if Hoodi explorer is not yet available)`);
      }
    }
    console.log("");
  }

  console.log("═══════════════════════════════════════════════════════════");
  console.log("  Verification complete");
  console.log("═══════════════════════════════════════════════════════════");
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
