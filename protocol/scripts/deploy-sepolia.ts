/**
 * Deploy StableYield protocol to Sepolia testnet.
 *
 * Usage:
 *   npx hardhat run scripts/deploy-sepolia.ts --network sepolia
 *
 * Env vars:
 *   DEPLOYER_KEY         — private key for deployment
 *   ETHERSCAN_API_KEY    — optional, for contract verification
 *   USE_MOCK_WSTETH      — set "true" to deploy MockWstETH instead of using real Sepolia wstETH
 */

import { ethers } from "hardhat";

// ─── Sepolia Addresses ────────────────────────────────────────────────────────

const SEPOLIA_WSTETH = "0x0000000000000000000000000000000000000000";

// ─── Deployment Parameters ────────────────────────────────────────────────────

const INITIAL_SYLD_SUPPLY = ethers.parseEther("10000000"); // 10M SYLD

// SpreadCalculator: s_base=25bp, α=10, β=3, κ_target=1.5, κ_critical=0.3
const SPREAD_PARAMS = {
  sBaseBps: 25n,
  alphaE18: ethers.parseEther("10"),
  betaE18: ethers.parseEther("3"),
  kappaTargetE18: ethers.parseEther("1.5"),
  kappaCriticalE18: ethers.parseEther("0.3"),
};

// ReserveManager: κ_target=150%, κ_emergency=105%
const KAPPA_TARGET = ethers.parseEther("1.5");
const KAPPA_EMERGENCY = ethers.parseEther("1.05");

// Initial series: 2026Q4 maturing Dec 31, 2026 at 2.5%
const INITIAL_SERIES_LABEL = "2026Q4";
const INITIAL_SERIES_MATURITY = Math.floor(
  new Date("2026-12-31T23:59:59Z").getTime() / 1000
);
const INITIAL_SERIES_RATE = ethers.parseEther("0.025");

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const [deployer] = await ethers.getSigners();
  const deployerAddr = await deployer.getAddress();

  console.log("═══════════════════════════════════════════════════════════");
  console.log("  StableYield Protocol — Sepolia Deployment");
  console.log("═══════════════════════════════════════════════════════════");
  console.log(`  Deployer:  ${deployerAddr}`);
  console.log(`  Network:   ${(await ethers.provider.getNetwork()).name}`);
  console.log(`  Balance:   ${ethers.formatEther(await ethers.provider.getBalance(deployerAddr))} ETH`);
  console.log("");

  // ── 1. wstETH ──────────────────────────────────────────────────────────────

  let wstETHAddress: string;
  if (process.env.USE_MOCK_WSTETH === "true") {
    console.log("  [1/7] Deploying MockWstETH...");
    const MockWstETH = await ethers.getContractFactory("MockWstETH");
    const mock = await MockWstETH.deploy();
    await mock.waitForDeployment();
    wstETHAddress = await mock.getAddress();
    console.log(`         MockWstETH: ${wstETHAddress}`);
  } else {
    wstETHAddress = SEPOLIA_WSTETH;
    console.log(`  [1/7] Using Sepolia wstETH: ${wstETHAddress}`);
  }

  // ── 2. SYLDToken ───────────────────────────────────────────────────────────

  console.log("  [2/7] Deploying SYLDToken...");
  const SYLDToken = await ethers.getContractFactory("SYLDToken");
  const syld = await SYLDToken.deploy(deployerAddr, deployerAddr, INITIAL_SYLD_SUPPLY);
  await syld.waitForDeployment();
  const syldAddr = await syld.getAddress();
  console.log(`         SYLDToken: ${syldAddr}`);

  // ── 3. SyLST ──────────────────────────────────────────────────────────────

  console.log("  [3/7] Deploying SyLST (ERC-1155)...");
  const SyLST = await ethers.getContractFactory("SyLST");
  const syLST = await SyLST.deploy(
    deployerAddr,
    "https://api.stakeyield.finance/syLST/{id}.json"
  );
  await syLST.waitForDeployment();
  const syLSTAddr = await syLST.getAddress();
  console.log(`         SyLST:    ${syLSTAddr}`);

  // ── 4. ReserveManager ──────────────────────────────────────────────────────

  console.log("  [4/7] Deploying ReserveManager...");
  const ReserveManager = await ethers.getContractFactory("ReserveManager");
  const reserve = await ReserveManager.deploy(
    deployerAddr,
    wstETHAddress,
    syldAddr,
    deployerAddr, // syldStaking placeholder — update after staking contract deployed
    KAPPA_TARGET,
    KAPPA_EMERGENCY
  );
  await reserve.waitForDeployment();
  const reserveAddr = await reserve.getAddress();
  console.log(`         Reserve:   ${reserveAddr}`);

  // ── 5. SpreadCalculator ────────────────────────────────────────────────────

  console.log("  [5/7] Deploying SpreadCalculator...");
  const SpreadCalculator = await ethers.getContractFactory("SpreadCalculator");
  const spread = await SpreadCalculator.deploy(
    deployerAddr,
    reserveAddr,
    SPREAD_PARAMS
  );
  await spread.waitForDeployment();
  const spreadAddr = await spread.getAddress();
  console.log(`         Spread:    ${spreadAddr}`);

  // ── 6. StableYieldVault ────────────────────────────────────────────────────

  console.log("  [6/7] Deploying StableYieldVault...");
  const Vault = await ethers.getContractFactory("StableYieldVault");
  const vault = await Vault.deploy(
    deployerAddr,
    wstETHAddress,
    syLSTAddr,
    reserveAddr,
    spreadAddr
  );
  await vault.waitForDeployment();
  const vaultAddr = await vault.getAddress();
  console.log(`         Vault:     ${vaultAddr}`);

  // ── 7. Role wiring ────────────────────────────────────────────────────────

  console.log("  [7/7] Wiring roles...");
  const VAULT_ROLE = ethers.keccak256(ethers.toUtf8Bytes("VAULT_ROLE"));
  const KEEPER_ROLE = ethers.keccak256(ethers.toUtf8Bytes("KEEPER_ROLE"));
  const MINTER_ROLE = ethers.keccak256(ethers.toUtf8Bytes("MINTER_ROLE"));

  // SyLST: vault can mint/burn/register/settle
  await (await syLST.grantRole(VAULT_ROLE, vaultAddr)).wait();
  console.log("         SyLST.VAULT_ROLE → vault ✓");

  // ReserveManager: vault can deposit/withdraw/update liabilities
  await (await reserve.grantRole(VAULT_ROLE, vaultAddr)).wait();
  console.log("         Reserve.VAULT_ROLE → vault ✓");

  // ReserveManager: deployer as keeper
  await (await reserve.grantRole(KEEPER_ROLE, deployerAddr)).wait();
  console.log("         Reserve.KEEPER_ROLE → deployer ✓");

  // Vault: deployer as keeper
  await (await vault.grantRole(KEEPER_ROLE, deployerAddr)).wait();
  console.log("         Vault.KEEPER_ROLE → deployer ✓");

  // SYLDToken: reserve can mint in emergencies
  await (await syld.grantRole(MINTER_ROLE, reserveAddr)).wait();
  console.log("         SYLD.MINTER_ROLE → reserve ✓");

  // ── Create initial series ──────────────────────────────────────────────────

  console.log("");
  console.log("  Creating initial series...");
  const tx = await vault.createSeries(
    INITIAL_SERIES_LABEL,
    INITIAL_SERIES_MATURITY,
    INITIAL_SERIES_RATE
  );
  await tx.wait();
  const seriesId = ethers.keccak256(
    ethers.toUtf8Bytes(INITIAL_SERIES_LABEL)
  );
  console.log(`  Series "${INITIAL_SERIES_LABEL}": ${seriesId}`);
  console.log(`  Fixed rate: 2.50%  |  Maturity: 2026-12-31`);

  // ── Summary ────────────────────────────────────────────────────────────────

  console.log("");
  console.log("═══════════════════════════════════════════════════════════");
  console.log("  Deployment Complete");
  console.log("═══════════════════════════════════════════════════════════");
  console.log(`
  Addresses:
    wstETH:          ${wstETHAddress}
    SYLDToken:       ${syldAddr}
    SyLST:           ${syLSTAddr}
    ReserveManager:  ${reserveAddr}
    SpreadCalculator:${spreadAddr}
    StableYieldVault:${vaultAddr}

  Series:
    ${INITIAL_SERIES_LABEL}: ${seriesId}

  Next steps:
    1. Seed reserve: transfer wstETH to ReserveManager (${reserveAddr})
    2. Update GUI with deployed vault address
    3. Verify contracts on Etherscan:
       npx hardhat verify --network sepolia ${vaultAddr} ${deployerAddr} ${wstETHAddress} ${syLSTAddr} ${reserveAddr} ${spreadAddr}
  `);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
