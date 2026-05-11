import { ethers } from "hardhat";
import type { HardhatEthersSigner } from "@nomicfoundation/hardhat-ethers/signers";
import type {
  MockWstETH,
  SYLDToken,
  SyLST,
  ReserveManager,
  SpreadCalculator,
  StableYieldVault,
} from "../../typechain-types";

// ─── Result Type ──────────────────────────────────────────────────────────────

export interface DeployResult {
  wstETH: MockWstETH;
  syld: SYLDToken;
  syLST: SyLST;
  reserve: ReserveManager;
  /** Alias for spreadCalculator */
  spread: SpreadCalculator;
  spreadCalculator: SpreadCalculator;
  vault: StableYieldVault;
  admin: HardhatEthersSigner;
  keeper: HardhatEthersSigner;
  user1: HardhatEthersSigner;
  user2: HardhatEthersSigner;
}

// ─── Core Deploy ──────────────────────────────────────────────────────────────

/**
 * Deploy all StableYield protocol contracts and wire them together.
 *
 * Signers allocated:
 *   [0] admin   — holds DEFAULT_ADMIN_ROLE, GOVERNANCE_ROLE, KEEPER_ROLE everywhere
 *   [1] keeper  — holds KEEPER_ROLE on vault + reserveManager
 *   [2] user1   — unprivileged test user
 *   [3] user2   — unprivileged test user
 *
 * Deployment order:
 *   1. MockWstETH          — mock Lido wstETH (ERC-20 + stEthPerToken)
 *   2. SYLDToken           — governance token; initial supply minted to admin
 *   3. SyLST               — ERC-1155 series claims
 *   4. ReserveManager      — holds reserve, tracks κ
 *   5. SpreadCalculator    — reads kappa from ReserveManager
 *   6. StableYieldVault    — core vault
 *
 * Role wiring:
 *   SyLST.VAULT_ROLE          → vault
 *   ReserveManager.VAULT_ROLE → vault
 *   ReserveManager.KEEPER_ROLE → admin, keeper
 *   StableYieldVault.KEEPER_ROLE → admin, keeper
 *   SYLDToken.MINTER_ROLE     → reserveManager
 */
export async function deploy(): Promise<DeployResult> {
  const [adminSigner, keeperSigner, user1Signer, user2Signer] =
    (await ethers.getSigners()) as HardhatEthersSigner[];

  const adminAddress  = await adminSigner.getAddress();
  const keeperAddress = await keeperSigner.getAddress();

  // 1. MockWstETH — starts at 1.15e27 (15% appreciation since genesis)
  const wstETH = (await (
    await ethers.getContractFactory("MockWstETH", adminSigner)
  ).deploy()) as unknown as MockWstETH;
  await wstETH.waitForDeployment();

  // 2. SYLDToken — 10M initial supply to admin
  const initialSupply = ethers.parseEther("10000000");
  const syld = (await (
    await ethers.getContractFactory("SYLDToken", adminSigner)
  ).deploy(adminAddress, adminAddress, initialSupply)) as unknown as SYLDToken;
  await syld.waitForDeployment();

  // 3. SyLST ERC-1155
  const syLST = (await (
    await ethers.getContractFactory("SyLST", adminSigner)
  ).deploy(
    adminAddress,
    "https://api.stakeyield.finance/syLST/{id}.json"
  )) as unknown as SyLST;
  await syLST.waitForDeployment();

  // 4. ReserveManager — κ_target = 120%, κ_emergency = 105%
  const kappaTarget    = ethers.parseEther("1.2");
  const kappaEmergency = ethers.parseEther("1.05");

  const reserve = (await (
    await ethers.getContractFactory("ReserveManager", adminSigner)
  ).deploy(
    adminAddress,
    await wstETH.getAddress(),
    await syld.getAddress(),
    adminAddress, // syldStaking placeholder
    kappaTarget,
    kappaEmergency
  )) as unknown as ReserveManager;
  await reserve.waitForDeployment();

  // 5. SpreadCalculator
  //    sBaseBps = 25, α = 2.0, β = 2.0, κ_target = 1.2, κ_critical = 0.5
  const spreadParams = {
    sBaseBps:         25n,
    alphaE18:         ethers.parseEther("2"),
    betaE18:          ethers.parseEther("2"),
    kappaTargetE18:   ethers.parseEther("1.2"),
    kappaCriticalE18: ethers.parseEther("0.5"),
  };

  const spreadCalculator = (await (
    await ethers.getContractFactory("SpreadCalculator", adminSigner)
  ).deploy(
    adminAddress,
    await reserve.getAddress(),
    spreadParams
  )) as unknown as SpreadCalculator;
  await spreadCalculator.waitForDeployment();

  // 6. StableYieldVault
  const vault = (await (
    await ethers.getContractFactory("StableYieldVault", adminSigner)
  ).deploy(
    adminAddress,
    await wstETH.getAddress(),
    await syLST.getAddress(),
    await reserve.getAddress(),
    await spreadCalculator.getAddress()
  )) as unknown as StableYieldVault;
  await vault.waitForDeployment();

  const vaultAddress   = await vault.getAddress();
  const reserveAddress = await reserve.getAddress();

  // ─── Role wiring ──────────────────────────────────────────────────────────────

  const VAULT_ROLE  = ethers.keccak256(ethers.toUtf8Bytes("VAULT_ROLE"));
  const KEEPER_ROLE = ethers.keccak256(ethers.toUtf8Bytes("KEEPER_ROLE"));
  const MINTER_ROLE = ethers.keccak256(ethers.toUtf8Bytes("MINTER_ROLE"));

  // Vault can mint/burn/register/settle on syLST
  await syLST.grantRole(VAULT_ROLE, vaultAddress);

  // Vault can manage reserve (deposit, withdraw, update liabilities)
  await reserve.grantRole(VAULT_ROLE, vaultAddress);

  // Keepers — both admin and keeper signer can call keeper-only functions
  await reserve.grantRole(KEEPER_ROLE, adminAddress);
  await reserve.grantRole(KEEPER_ROLE, keeperAddress);
  await vault.grantRole(KEEPER_ROLE, adminAddress);
  await vault.grantRole(KEEPER_ROLE, keeperAddress);

  // ReserveManager can mint SYLD in emergencies
  await syld.grantRole(MINTER_ROLE, reserveAddress);

  return {
    wstETH,
    syld,
    syLST,
    reserve,
    spreadCalculator,
    spread: spreadCalculator, // alias used by SpreadCalculator tests
    vault,
    admin:  adminSigner,
    keeper: keeperSigner,
    user1:  user1Signer,
    user2:  user2Signer,
  };
}

// ─── Series Helper ────────────────────────────────────────────────────────────

/**
 * Create a series on the vault.
 *
 * @param vault             Deployed vault; caller (admin) must hold GOVERNANCE_ROLE.
 * @param label             Series label, e.g. "2027Q1". Determines seriesId.
 * @param maturityOffsetSec Seconds from now until maturity.
 * @param fixedRateE18      Annualised fixed rate, 1e18-scaled (e.g. 2.5% = 0.025e18).
 * @returns                 seriesId = keccak256(label) as bytes32 hex string.
 */
export async function createSeries(
  vault: StableYieldVault,
  label: string,
  maturityOffsetSec: number | bigint
): Promise<string> {
  const now = (await ethers.provider.getBlock("latest"))!.timestamp;
  const maturity = BigInt(now) + BigInt(maturityOffsetSec as number);

  const tx = await vault.createSeries(label, maturity);
  await tx.wait();

  return ethers.keccak256(ethers.toUtf8Bytes(label));
}

// ─── Time Helpers ─────────────────────────────────────────────────────────────

/**
 * Advance EVM time by `seconds` and mine a block.
 */
export async function advanceTime(seconds: number): Promise<void> {
  await ethers.provider.send("evm_increaseTime", [seconds]);
  await ethers.provider.send("evm_mine", []);
}

/**
 * Set the next block's timestamp and mine.
 */
export async function setTimestamp(timestamp: number): Promise<void> {
  await ethers.provider.send("evm_setNextBlockTimestamp", [timestamp]);
  await ethers.provider.send("evm_mine", []);
}

// ─── wstETH Yield Simulation ──────────────────────────────────────────────────

/**
 * Simulate wstETH yield accrual by increasing the stEthPerToken rate.
 *
 * @param wstETH          MockWstETH contract.
 * @param annualRateBps   Annual staking yield in basis points (e.g. 350 = 3.5%).
 * @param elapsedDays     Number of days of yield to simulate.
 */
export async function simulateYield(
  wstETH: MockWstETH,
  annualRateBps: number,
  elapsedDays: number
): Promise<void> {
  const currentRate = await wstETH.stEthPerToken();
  const RAY = 10n ** 27n;
  const annualBps = BigInt(annualRateBps);
  const days = BigInt(elapsedDays);

  // yieldFraction (ray-scaled) = annualRateBps * days / (10000 * 365) * RAY
  const yieldFraction = (annualBps * days * RAY) / (10000n * 365n);
  const newRate = currentRate + (currentRate * yieldFraction) / RAY;
  await wstETH.setStEthPerToken(newRate);
}

/**
 * Directly set the wstETH exchange rate.
 *
 * @param wstETH    MockWstETH contract.
 * @param newRate   New stETH-per-wstETH rate, ray-scaled (1e27).
 */
export async function setWstEthRate(
  wstETH: MockWstETH,
  newRate: bigint
): Promise<void> {
  await wstETH.setStEthPerToken(newRate);
}

/**
 * Fund the reserve with wstETH so that kappa stays healthy.
 * Mints wstETH directly to the ReserveManager address.
 *
 * @param d      Deploy result.
 * @param amount Amount of wstETH to send to reserve.
 */
export async function fundReserve(
  d: DeployResult,
  amount: bigint
): Promise<void> {
  const reserveAddr = await d.reserve.getAddress();
  await d.wstETH.mint(reserveAddr, amount);
}
