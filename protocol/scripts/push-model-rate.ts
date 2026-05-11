/**
 * Push model-computed staking APR to the StableYieldVault on-chain.
 *
 * Reads the output from compute_offered_rate.py and calls setStakingAPR().
 *
 * Usage:
 *   npx hardhat run scripts/push-model-rate.ts --network hoodi
 *
 * Reads: ../data/model_rate_output.json
 */

import { ethers } from "hardhat";
import * as fs from "fs";
import * as path from "path";

// Load deployment addresses
const DEPLOYMENTS_PATH = path.join(__dirname, "..", "deployments", "hoodi.json");
const MODEL_OUTPUT_PATH = path.join(__dirname, "..", "..", "data", "model_rate_output.json");

async function main() {
  // Read deployment addresses
  if (!fs.existsSync(DEPLOYMENTS_PATH)) {
    throw new Error(`Deployment file not found: ${DEPLOYMENTS_PATH}`);
  }
  const deployment = JSON.parse(fs.readFileSync(DEPLOYMENTS_PATH, "utf-8"));
  const vaultAddress = deployment.StableYieldVault;

  // Read model output
  if (!fs.existsSync(MODEL_OUTPUT_PATH)) {
    throw new Error(
      `Model output not found: ${MODEL_OUTPUT_PATH}\n` +
      `Run: python3 scripts/compute_offered_rate.py`
    );
  }
  const modelOutput = JSON.parse(fs.readFileSync(MODEL_OUTPUT_PATH, "utf-8"));
  const modelAPR_e18 = BigInt(modelOutput.model_projected_apr_e18);
  const modelAPR_pct = modelOutput.model_projected_apr_pct;
  const spotAPR_pct = modelOutput.spot_apr_pct;

  console.log("═══════════════════════════════════════════════════════════");
  console.log("  StakeYield — Push Model Rate On-Chain");
  console.log("═══════════════════════════════════════════════════════════");
  console.log(`  Vault:              ${vaultAddress}`);
  console.log(`  Series:             ${modelOutput.series}`);
  console.log(`  Model projected APR: ${modelAPR_pct}%  (${modelAPR_e18} wei)`);
  console.log(`  Current spot APR:    ${spotAPR_pct}%`);
  console.log(`  Data range:          ${modelOutput.calibration_data_range}`);
  console.log(`  Computed at:         ${modelOutput.timestamp}`);
  console.log("");

  // Connect to vault
  const [signer] = await ethers.getSigners();
  const vault = await ethers.getContractAt("StableYieldVault", vaultAddress, signer);

  // Read current on-chain value
  const currentAPR = await vault.stakingAPR();
  const currentPct = Number(currentAPR) / 1e16;
  console.log(`  Current on-chain stakingAPR: ${currentPct.toFixed(4)}%`);

  if (currentAPR === modelAPR_e18) {
    console.log("  ✅ Already up to date — no transaction needed.");
    return;
  }

  // Push new rate
  console.log(`  Updating: ${currentPct.toFixed(4)}% → ${modelAPR_pct}%`);
  const tx = await vault.setStakingAPR(modelAPR_e18);
  console.log(`  Tx: ${tx.hash}`);
  await tx.wait();
  console.log("  ✅ Rate updated on-chain.");

  // Verify
  const newAPR = await vault.stakingAPR();
  const computedRate = await vault.computeFixedRate();
  const offeredPct = Number(computedRate) / 1e16;
  console.log("");
  console.log(`  New stakingAPR:     ${Number(newAPR) / 1e16}%`);
  console.log(`  Offered fixed rate: ${offeredPct.toFixed(4)}%  (after spread deduction)`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
