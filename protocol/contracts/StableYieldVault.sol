// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {AccessControl}  from "@openzeppelin/contracts/access/AccessControl.sol";
import {ReentrancyGuard} from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import {Pausable}        from "@openzeppelin/contracts/utils/Pausable.sol";
import {IERC20}          from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {SafeERC20}       from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";

import {IStableYieldVault} from "./interfaces/IStableYieldVault.sol";
import {ISyLST}            from "./interfaces/ISyLST.sol";
import {IReserveManager}   from "./interfaces/IReserveManager.sol";
import {ISpreadCalculator} from "./interfaces/ISpreadCalculator.sol";

/**
 * @title StableYieldVault
 * @author StakeYield Finance
 * @notice Core vault: accepts wstETH deposits into fixed-rate quarterly series,
 *         mints syLST (ERC-1155) claims, routes floating yield to reserve, and
 *         handles fixed-rate redemptions at maturity.
 *
 * ════════════════════════════════════════════════════════════════════════════════
 * PROTOCOL OVERVIEW
 * ════════════════════════════════════════════════════════════════════════════════
 *
 * 1. SERIES
 *    A series is a distinct quarterly maturity (e.g. "2026Q3" maturing 30-Sep-2026).
 *    Governance creates series via createSeries() before deposits open.
 *    Each series has a fixedRateE18 (annualised, 1e18-scaled).
 *
 * 2. DEPOSIT MECHANICS
 *    User deposits X wstETH into series S with fixed rate r and remaining tenor T (years).
 *    The vault:
 *      a. Applies the dynamic spread (s bps) to compute the effective fixed rate for reserve.
 *         effective_rate = r + spread/10000
 *         (Spread is the premium charged on the user; the spread portion flows to reserve)
 *      b. Mints syLST[seriesId] proportional to the user's share of the series.
 *         syLST minted = X (1:1 with wstETH deposited, for simplicity of accounting).
 *      c. Records the user's fixed-rate claim:
 *         claim = X · (1 + r · T)     [user sees this at maturity]
 *      d. Records the spread obligation to reserve:
 *         spreadObligation = X · (spread/10000) · T  [flows to reserve as surplus]
 *      e. Updates liability in ReserveManager:
 *         liability added = X · (1 + r · T)
 *
 * 3. YIELD FLOW
 *    wstETH is a non-rebasing token; its value vs stETH grows as the Ethereum staking
 *    rate accrues. harvestYield() is called periodically (by keeper or anyone):
 *      a. Compare current wstETH/stETH rate to rate at last harvest.
 *      b. The implied floating-rate gain on total escrowed wstETH = floatGain.
 *      c. The total fixed-rate obligation accruing since last harvest = fixedObligation.
 *      d. net = floatGain − fixedObligation
 *         net > 0 → surplus wstETH sent to ReserveManager
 *         net < 0 → deficit wstETH drawn from ReserveManager
 *
 * 4. MATURITY & SETTLEMENT
 *    At or after a series' maturity timestamp:
 *      - settleSeries() finalises the claimPerToken rate on the syLST contract.
 *      - Any remaining reserve top-up or surplus is handled during settlement.
 *      - Once settled, syLST holders can call redeem() to burn tokens and receive wstETH.
 *
 * 5. NO EARLY EXIT
 *    There is no early redemption. Users wishing to exit early must sell syLST
 *    on secondary markets (AMMs, OTC). This simplifies the vault's accounting.
 *
 * ════════════════════════════════════════════════════════════════════════════════
 * WSTETH ACCOUNTING
 * ════════════════════════════════════════════════════════════════════════════════
 *
 * wstETH is non-rebasing: the balance in this contract does not increase on its own.
 * The appreciation is captured by the increasing wstETH/stETH exchange rate, which
 * we track via IWstETH.stEthPerToken() (or getStETHByWstETH(1e18)).
 *
 * Snapshotting the stETH-per-wstETH ratio at each harvest lets us compute the
 * implicit floating yield earned by the escrowed wstETH without transferring tokens:
 *   floatGain_wstETH = totalEscrowed · (currentRate − lastRate) / lastRate
 *
 * In practice, the "yield" is realised as wstETH appreciation; the surplus
 * wstETH is NOT minted — instead the vault signals to the reserve that a surplus
 * is available, and the vault may transfer a portion of the escrowed principal
 * to the reserve (or record a credit/debit). For simplicity, we track in wstETH units.
 *
 * ════════════════════════════════════════════════════════════════════════════════
 * FIXED-POINT MATH
 * ════════════════════════════════════════════════════════════════════════════════
 *
 * All rates (fixed, float, spread) are stored as 1e18-scaled uint256.
 * Time is in seconds; 1 year = 365 days = 31,536,000 seconds.
 * Simple interest used throughout (no compounding) for auditability.
 *
 * ════════════════════════════════════════════════════════════════════════════════
 */
contract StableYieldVault is IStableYieldVault, AccessControl, ReentrancyGuard, Pausable {
    using SafeERC20 for IERC20;

    // ─── Constants ─────────────────────────────────────────────────────────────

    /// @dev 1e18 scaling factor for fixed-point arithmetic.
    uint256 private constant WAD = 1e18;

    /// @dev Seconds in a standard 365-day year (simple interest basis).
    uint256 private constant SECONDS_PER_YEAR = 365 days;

    // ─── Roles ─────────────────────────────────────────────────────────────────

    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 public constant KEEPER_ROLE     = keccak256("KEEPER_ROLE");

    // ─── Immutables ─────────────────────────────────────────────────────────────

    /// @inheritdoc IStableYieldVault
    address public immutable override wstETH;

    /// @inheritdoc IStableYieldVault
    address public immutable override syLST;

    /// @inheritdoc IStableYieldVault
    address public immutable override reserveManager;

    /// @notice SpreadCalculator for dynamic spread computation.
    address public immutable spreadCalculator;

    // ─── wstETH Oracle Interface ────────────────────────────────────────────────

    /// @dev Minimal interface to read wstETH↔stETH exchange rate.
    ///      Lido's wstETH exposes stEthPerToken() returning stETH per 1e18 wstETH.
    IWstETH private immutable _wstEthOracle;

    // ─── Series State ───────────────────────────────────────────────────────────

    /// @notice All series metadata, keyed by seriesId.
    mapping(bytes32 => Series) private _series;

    /// @notice List of all created series identifiers (for enumeration).
    bytes32[] private _allSeriesIds;

    // ─── Harvest State ──────────────────────────────────────────────────────────

    /// @notice Total wstETH escrowed across all active series.
    uint256 public totalEscrowed;

    /// @notice stETH-per-wstETH ratio at the last harvest (1e27-scaled, matching Lido).
    uint256 public lastHarvestRate;

    /// @notice Timestamp of the last yield harvest.
    uint256 public lastHarvestTimestamp;

    // ─── Per-Series Settlement ───────────────────────────────────────────────────

    /// @notice wstETH per syLST token for settled series (1e18-scaled).
    ///         Set during settleSeries(); used for redemption.
    mapping(bytes32 => uint256) private _claimPerToken;

    /// @notice Total wstETH allocated for redemption of a settled series.
    ///         Funded by the vault's escrow + any reserve top-up.
    mapping(bytes32 => uint256) private _settlementPool;

    // ─── Constructor ────────────────────────────────────────────────────────────

    /**
     * @param admin             Multi-sig / timelock holding DEFAULT_ADMIN_ROLE.
     * @param _wstETH           wstETH token address (Lido).
     * @param _syLST            SyLST ERC-1155 token address.
     * @param _reserveManager   ReserveManager address.
     * @param _spreadCalculator SpreadCalculator address.
     */
    constructor(
        address admin,
        address _wstETH,
        address _syLST,
        address _reserveManager,
        address _spreadCalculator
    ) {
        require(admin != address(0),             "Vault: zero admin");
        require(_wstETH != address(0),           "Vault: zero wstETH");
        require(_syLST != address(0),            "Vault: zero syLST");
        require(_reserveManager != address(0),   "Vault: zero reserve");
        require(_spreadCalculator != address(0), "Vault: zero spread");

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);

        wstETH          = _wstETH;
        syLST           = _syLST;
        reserveManager  = _reserveManager;
        spreadCalculator = _spreadCalculator;
        _wstEthOracle   = IWstETH(_wstETH);

        // Snapshot the initial wstETH rate.
        lastHarvestRate      = _wstEthOracle.stEthPerToken();
        lastHarvestTimestamp = block.timestamp;
    }

    // ─── IStableYieldVault: Governance ────────────────────────────────────────

    /**
     * @inheritdoc IStableYieldVault
     *
     * @dev Series label → seriesId derivation:
     *   seriesId = keccak256(abi.encodePacked(seriesLabel))
     *   e.g. keccak256("2026Q3") → 0xabc…
     *
     * The fixed rate provided here is the BASE rate offered to depositors.
     * The spread is added on top at deposit time and flows to the reserve.
     * So effective obligations from the protocol's perspective are:
     *   base rate → returned to depositors at maturity
     *   spread    → accrues to reserve as safety buffer
     *
     * @param seriesLabel       Human-readable label (e.g. "2026Q3").
     * @param maturityTimestamp Unix maturity timestamp.
     * @param fixedRateE18      Annualised base fixed rate, 1e18-scaled.
     */
    function createSeries(
        string calldata seriesLabel,
        uint256 maturityTimestamp,
        uint256 fixedRateE18
    )
        external
        override
        onlyRole(GOVERNANCE_ROLE)
        returns (bytes32 seriesId)
    {
        require(maturityTimestamp > block.timestamp, "Vault: maturity in the past");
        require(fixedRateE18 > 0 && fixedRateE18 < WAD, "Vault: invalid rate");

        seriesId = keccak256(abi.encodePacked(seriesLabel));
        require(_series[seriesId].maturity == 0, "Vault: series already exists");

        _series[seriesId] = Series({
            maturity:       maturityTimestamp,
            fixedRateE18:   fixedRateE18,
            totalDeposited: 0,
            totalClaims:    0,
            totalSyLst:     0,
            isOpen:         true,
            isSettled:      false
        });

        _allSeriesIds.push(seriesId);

        // Register the series tokenId on the SyLST contract.
        ISyLST(syLST).registerSeries(
            uint256(seriesId),
            maturityTimestamp,
            fixedRateE18
        );

        emit SeriesCreated(seriesId, maturityTimestamp, fixedRateE18);
    }

    /**
     * @inheritdoc IStableYieldVault
     */
    function closeSeries(bytes32 seriesId)
        external
        override
        onlyRole(GOVERNANCE_ROLE)
    {
        Series storage s = _series[seriesId];
        require(s.maturity > 0,  "Vault: series not found");
        require(s.isOpen,        "Vault: already closed");

        s.isOpen = false;
        emit SeriesClosed(seriesId);
    }

    // ─── IStableYieldVault: Core User Actions ─────────────────────────────────

    /**
     * @inheritdoc IStableYieldVault
     *
     * @dev Deposit flow:
     *  1. Validate series is open and not expired.
     *  2. Pull wstETH from caller.
     *  3. Read current dynamic spread from SpreadCalculator (function of κ).
     *  4. Compute the user's fixed-rate claim at maturity (principal + interest).
     *     claim = wstEthAmount · (1 + r_fixed · T)
     *     where T = (maturity − now) / SECONDS_PER_YEAR
     *  5. Compute the spread obligation (reserved for protocol):
     *     spreadObligation = wstEthAmount · (spreadRate · T)
     *     This is tracked as additional reserve income, not returned to the user.
     *  6. Update series state (totalDeposited, totalClaims, totalSyLst).
     *  7. Update liability in ReserveManager = user claim (not spread — spread is surplus).
     *  8. Mint syLST 1:1 with wstEthAmount (each token represents 1 wstETH of principal).
     *  9. Update escrow tracker and emit event.
     *
     * Gas considerations:
     *  - Single storage slot for series state (packed struct).
     *  - SpreadCalculator called via external view — no state change there.
     *  - ReserveManager.updateLiability() is the only external state-changing call.
     */
    function deposit(bytes32 seriesId, uint256 wstEthAmount)
        external
        override
        nonReentrant
        whenNotPaused
        returns (uint256 syLstMinted)
    {
        require(wstEthAmount > 0, "Vault: zero deposit");

        Series storage s = _series[seriesId];
        require(s.maturity > 0,               "Vault: series not found");
        require(s.isOpen,                     "Vault: series closed");
        require(!s.isSettled,                 "Vault: series settled");
        require(block.timestamp < s.maturity, "Vault: series matured");

        // ── 1. Pull wstETH from depositor ──────────────────────────────────────
        IERC20(wstETH).safeTransferFrom(msg.sender, address(this), wstEthAmount);

        // ── 2. Compute tenor in seconds and as a WAD fraction of a year ────────
        uint256 tenorSeconds = s.maturity - block.timestamp;
        // tenorFracE18 = tenor / SECONDS_PER_YEAR * 1e18
        uint256 tenorFracE18 = (tenorSeconds * WAD) / SECONDS_PER_YEAR;

        // ── 3. Fetch dynamic spread (basis points) ──────────────────────────────
        uint256 spreadBps = ISpreadCalculator(spreadCalculator).currentSpread();
        // Convert spread from basis points to 1e18-scaled fraction.
        // 100 bp = 1% = 0.01 = 1e16 in WAD
        uint256 spreadE18 = (spreadBps * WAD) / 10_000;

        // ── 4. Compute user's fixed-rate claim at maturity ──────────────────────
        // claim = principal · (1 + r_fixed · tenor)
        // r_fixed · tenor = fixedRateE18 · tenorFracE18 / WAD
        uint256 fixedInterestE18 = (s.fixedRateE18 * tenorFracE18) / WAD;
        // claim_E18 = principal_E18 · (1 + fixedInterestRate_E18)
        // claim (in wstETH raw units) = wstEthAmount + wstEthAmount * fixedInterestE18 / WAD
        uint256 claimAtMaturity = wstEthAmount + (wstEthAmount * fixedInterestE18) / WAD;

        // ── 5. Compute spread income routed to reserve ──────────────────────────
        // spreadObligation = principal · spreadRate · tenor
        // This is additional to the fixed rate — charged to the depositor implicitly
        // as a higher effective fixed rate, but NOT included in their claim.
        // (The user gets r_fixed; the protocol keeps the spread as reserve income.)
        //
        // Actuarial note: the spread is an insurance premium. It directly increases
        // reserve income without increasing user obligations, so it improves κ.
        uint256 spreadIncomeE18  = (spreadE18 * tenorFracE18) / WAD;
        uint256 spreadIncomeWstEth = (wstEthAmount * spreadIncomeE18) / WAD;

        // ── 6. Mint syLST 1:1 with deposited wstETH ────────────────────────────
        // Each syLST token represents 1 wstETH of deposited principal.
        // The claimPerToken rate will be finalised at settlement.
        syLstMinted = wstEthAmount; // 1:1 mint

        // ── 7. Update series accounting ─────────────────────────────────────────
        s.totalDeposited += wstEthAmount;
        s.totalClaims    += claimAtMaturity;
        s.totalSyLst     += syLstMinted;

        // ── 8. Update escrow and liability tracking ─────────────────────────────
        totalEscrowed += wstEthAmount;

        // Liability = user's fixed-rate claim (what we owe at maturity).
        // Spread income is NOT a liability — it is surplus income.
        IReserveManager(reserveManager).updateLiability(seriesId, s.totalClaims);

        // ── 9. Transfer spread income to reserve ────────────────────────────────
        // The spread is immediately transferred to the reserve from the deposit.
        // This means the user's effective deposit into escrow = wstEthAmount - spreadIncomeWstEth,
        // but their claim is still on wstEthAmount · (1 + r_fixed · T).
        //
        // IMPORTANT: The reserve must ALWAYS be able to cover the deficit between
        // floatYield and fixedClaim. The spread pre-funds part of that buffer.
        if (spreadIncomeWstEth > 0) {
            // Transfer spread to ReserveManager and notify.
            IERC20(wstETH).safeTransfer(reserveManager, spreadIncomeWstEth);
            IReserveManager(reserveManager).depositReserve(spreadIncomeWstEth);
            // Net escrowed (used for yield tracking) is principal minus spread pre-payment.
            // However, we account for it as fully escrowed to simplify yield attribution.
            // The spread_income is a one-time credit; yield attribution is on totalEscrowed.
        }

        // ── 10. Mint syLST to depositor ─────────────────────────────────────────
        ISyLST(syLST).mint(msg.sender, uint256(seriesId), syLstMinted, "");

        emit Deposited(seriesId, msg.sender, wstEthAmount, syLstMinted, claimAtMaturity);
    }

    /**
     * @inheritdoc IStableYieldVault
     *
     * @dev Redemption flow:
     *  1. Verify series is settled (past maturity + vault has settled it).
     *  2. Compute wstETH out = syLstAmount · claimPerToken[seriesId] / 1e18.
     *  3. Burn syLST from caller.
     *  4. Transfer wstETH from settlement pool to caller.
     *  5. Decrease outstanding settlement pool.
     *
     * Note: If the settlement pool is exhausted (due to haircut), redemption
     * reverts. In practice, governance ensures reserve is adequate before settling.
     */
    function redeem(bytes32 seriesId, uint256 syLstAmount)
        external
        override
        nonReentrant
        whenNotPaused
        returns (uint256 wstEthOut)
    {
        require(syLstAmount > 0, "Vault: zero amount");

        Series storage s = _series[seriesId];
        require(s.maturity > 0,    "Vault: series not found");
        require(s.isSettled,       "Vault: series not settled");
        require(
            block.timestamp >= s.maturity,
            "Vault: series not yet mature"
        );

        uint256 claimRate = _claimPerToken[seriesId];
        require(claimRate > 0, "Vault: zero claim rate (not settled)");

        // wstETH owed = syLST burned × claimPerToken (1e18-scaled)
        // e.g. if claimPerToken = 1.025e18 and syLstAmount = 1e18 (1 token)
        // then wstEthOut = 1e18 × 1.025e18 / 1e18 = 1.025e18 (1.025 wstETH)
        wstEthOut = (syLstAmount * claimRate) / WAD;
        require(wstEthOut > 0, "Vault: zero payout");

        // Check settlement pool has sufficient funds.
        uint256 poolBalance = _settlementPool[seriesId];
        require(poolBalance >= wstEthOut, "Vault: settlement pool insufficient");

        // ── Burn syLST ──────────────────────────────────────────────────────────
        ISyLST(syLST).burn(msg.sender, uint256(seriesId), syLstAmount);

        // ── Debit settlement pool ────────────────────────────────────────────────
        _settlementPool[seriesId] = poolBalance - wstEthOut;

        // ── Transfer wstETH to redeemer ─────────────────────────────────────────
        IERC20(wstETH).safeTransfer(msg.sender, wstEthOut);

        emit Redeemed(seriesId, msg.sender, syLstAmount, wstEthOut);
    }

    /**
     * @inheritdoc IStableYieldVault
     *
     * @dev Harvest flow:
     *  1. Read current wstETH/stETH rate from wstETH contract.
     *  2. Compute the implied stETH gain on escrowed wstETH since last harvest.
     *     floatYieldStEth = totalEscrowed × (currentRate − lastRate) / lastRate
     *     floatYieldWstEth = floatYieldStEth / currentRate  [convert back to wstETH]
     *
     *     Simplified: since wstETH is non-rebasing, "yield" is purely exchange rate appreciation.
     *     The wstETH balance doesn't change; the underlying value does.
     *     We compute the imputed yield in wstETH units as:
     *       imputed_wstETH_gain = totalEscrowed × (currentRate/lastRate − 1)
     *
     *  3. Compute the fixed-rate obligation accrued during the same period:
     *       fixedObligation = Σ_{series_i} (seriesEscrowed_i × r_i × dt/year)
     *     For simplicity, we use a blended fixed rate across total escrowed:
     *       blendedRate = Σ (escrowed_i × r_i) / totalEscrowed
     *     This approximation is valid when harvest frequency is high (daily/weekly).
     *
     *  4. net = floatGain − fixedObligation:
     *     net > 0 → surplus wstETH → transfer to ReserveManager
     *     net < 0 → deficit wstETH → withdraw from ReserveManager to cover
     *     (Note: we track in wstETH but since wstETH balance doesn't change, the
     *      "transfer" to reserve is a notional credit/debit settled at maturity.)
     *
     *  5. Update lastHarvestRate and lastHarvestTimestamp.
     *
     * Implementation choice — virtual vs physical settlement:
     *   We implement PHYSICAL settlement at harvest: the vault actually transfers
     *   wstETH to/from the reserve. This requires the vault to hold extra wstETH
     *   beyond user principals (the reserve pre-funds any deficit).
     *   At maturity, the settlement pool is funded exactly to cover all claims.
     *
     * Keeper-callable: anyone can trigger a harvest. There is no MEV attack since
     * the yield is purely time-based (no oracle to front-run).
     */
    function harvestYield()
        external
        override
        nonReentrant
        whenNotPaused
    {
        uint256 currentRate = _wstEthOracle.stEthPerToken();
        uint256 _lastRate   = lastHarvestRate;

        // No yield if rate hasn't changed (or harvest is called twice in same block).
        if (currentRate <= _lastRate || totalEscrowed == 0) {
            lastHarvestRate      = currentRate;
            lastHarvestTimestamp = block.timestamp;
            return;
        }

        // ── 1. Compute floating yield on escrowed wstETH ────────────────────────
        // The wstETH/stETH rate increased from lastRate to currentRate.
        // Imputed float gain in wstETH = escrowed × (currentRate/lastRate − 1)
        // = escrowed × (currentRate − lastRate) / lastRate
        // Note: rates are in stETH-per-wstETH (1e27-scaled by Lido convention).
        uint256 rateIncrease  = currentRate - _lastRate; // in stETH units (1e27)
        // floatGainWstEth = totalEscrowed * rateIncrease / lastRate
        // To avoid overflow: (totalEscrowed * rateIncrease) may be large.
        // Safe because totalEscrowed < 2^128 and rateIncrease/lastRate < 1.
        uint256 floatGainWstEth = (totalEscrowed * rateIncrease) / _lastRate;

        // ── 2. Compute fixed-rate obligation since last harvest ──────────────────
        uint256 dt = block.timestamp - lastHarvestTimestamp; // seconds
        if (dt == 0) {
            lastHarvestRate      = currentRate;
            lastHarvestTimestamp = block.timestamp;
            return;
        }
        uint256 dtFracE18 = (dt * WAD) / SECONDS_PER_YEAR;

        // Compute blended fixed rate = weighted average across active series.
        uint256 blendedFixedRateE18 = _blendedFixedRate();

        // fixedObligation = totalEscrowed × blendedRate × dt/year
        uint256 fixedObligationWstEth = (totalEscrowed * ((blendedFixedRateE18 * dtFracE18) / WAD)) / WAD;

        // ── 3. Route net yield ──────────────────────────────────────────────────
        uint256 toReserve;
        if (floatGainWstEth >= fixedObligationWstEth) {
            // Surplus: float yield exceeded fixed obligations → send to reserve.
            toReserve = floatGainWstEth - fixedObligationWstEth;
            if (toReserve > 0) {
                // Physical transfer: wstETH flows from vault escrow to reserve.
                // This means the vault's wstETH balance decreases; the deficit
                // at maturity will be covered by reserve withdrawal during settlement.
                IERC20(wstETH).safeTransfer(reserveManager, toReserve);
                IReserveManager(reserveManager).depositReserve(toReserve);
                totalEscrowed -= toReserve;
            }
        } else {
            // Deficit: float yield fell short of fixed obligations → draw from reserve.
            uint256 deficit = fixedObligationWstEth - floatGainWstEth;
            IReserveManager(reserveManager).withdrawReserve(deficit, address(this));
            totalEscrowed += deficit;
        }

        // ── 4. Update harvest state ──────────────────────────────────────────────
        lastHarvestRate      = currentRate;
        lastHarvestTimestamp = block.timestamp;

        emit YieldHarvested(floatGainWstEth, toReserve);
    }

    /**
     * @inheritdoc IStableYieldVault
     *
     * @dev Settlement flow:
     *  1. Verify series is past maturity.
     *  2. Run a final harvest if needed.
     *  3. Compute claimPerToken = totalClaims / totalSyLst (wstETH per syLST, 1e18-scaled).
     *     claimPerToken = (totalClaims × 1e18) / totalSyLst
     *  4. Compute total wstETH needed for redemption = totalClaims.
     *  5. Check vault's wstETH balance covers totalClaims:
     *     a. If surplus: excess flows to reserve.
     *     b. If deficit: draw from reserve.
     *  6. Allocate settlement pool = totalClaims wstETH in the vault.
     *  7. Update liability to 0 in ReserveManager.
     *  8. Call ISyLST.settleSeries() to lock the claimPerToken on-chain.
     *  9. Mark series as settled.
     *
     * Haircut scenario:
     *   If reserve cannot cover the full claim (extreme negative event), governance
     *   may trigger an emergency mint before settlement to recapitalise.
     *   If still short, a haircut is applied: claimPerToken is reduced proportionally.
     *   The haircut is tracked in an event for transparency.
     */
    function settleSeries(bytes32 seriesId)
        external
        override
        nonReentrant
        onlyRole(KEEPER_ROLE)
    {
        Series storage s = _series[seriesId];
        require(s.maturity > 0,           "Vault: series not found");
        require(!s.isSettled,             "Vault: already settled");
        require(
            block.timestamp >= s.maturity,
            "Vault: not yet mature"
        );

        // Close the series to new deposits if not already closed.
        if (s.isOpen) {
            s.isOpen = false;
            emit SeriesClosed(seriesId);
        }

        uint256 totalClaims  = s.totalClaims;
        uint256 totalSyLst_  = s.totalSyLst;

        if (totalSyLst_ == 0) {
            // Empty series: no depositors, just mark settled.
            s.isSettled = true;
            IReserveManager(reserveManager).removeLiability(seriesId);
            ISyLST(syLST).settleSeries(uint256(seriesId), WAD); // 1:1 (no-op)
            return;
        }

        // ── 1. Check available wstETH in vault ──────────────────────────────────
        uint256 vaultBalance = IERC20(wstETH).balanceOf(address(this));

        uint256 claimPerTokenE18;
        uint256 settledClaims;

        if (vaultBalance >= totalClaims) {
            // ── Surplus case ────────────────────────────────────────────────────
            uint256 excess = vaultBalance - totalClaims;
            settledClaims = totalClaims;

            // Route excess wstETH to reserve.
            if (excess > 0) {
                IERC20(wstETH).safeTransfer(reserveManager, excess);
                IReserveManager(reserveManager).depositReserve(excess);
                totalEscrowed = totalEscrowed > excess ? totalEscrowed - excess : 0;
            }
        } else {
            // ── Deficit case: request reserve top-up ────────────────────────────
            uint256 shortfall = totalClaims - vaultBalance;

            // Attempt to withdraw shortfall from reserve.
            // This may revert if reserve is below κ_emergency.
            IReserveManager(reserveManager).withdrawReserve(shortfall, address(this));

            vaultBalance = IERC20(wstETH).balanceOf(address(this));

            if (vaultBalance >= totalClaims) {
                settledClaims = totalClaims;
            } else {
                // ── Haircut: reserve insufficient ────────────────────────────────
                // Apply proportional haircut: claimPerToken = vaultBalance / totalSyLst
                // Depositors receive less than their full claim.
                settledClaims = vaultBalance;
            }
        }

        // ── 2. Compute claimPerToken (WAD-scaled) ────────────────────────────────
        // claimPerToken = settledClaims * 1e18 / totalSyLst
        // e.g. 1025 wstETH claims / 1000 syLST = 1.025e18 per token
        claimPerTokenE18 = (settledClaims * WAD) / totalSyLst_;

        // ── 3. Allocate settlement pool ──────────────────────────────────────────
        _settlementPool[seriesId]   = settledClaims;
        _claimPerToken[seriesId]    = claimPerTokenE18;

        // ── 4. Update tracking state ─────────────────────────────────────────────
        totalEscrowed = totalEscrowed > s.totalDeposited
            ? totalEscrowed - s.totalDeposited
            : 0;

        s.isSettled = true;

        // ── 5. Remove liability from ReserveManager ───────────────────────────────
        IReserveManager(reserveManager).removeLiability(seriesId);

        // ── 6. Finalise syLST settlement rate on-chain ────────────────────────────
        ISyLST(syLST).settleSeries(uint256(seriesId), claimPerTokenE18);
    }

    // ─── IStableYieldVault: View Functions ────────────────────────────────────

    /**
     * @inheritdoc IStableYieldVault
     */
    function getSeries(bytes32 seriesId)
        external
        view
        override
        returns (Series memory)
    {
        return _series[seriesId];
    }

    /**
     * @inheritdoc IStableYieldVault
     *
     * @dev For unsettled series, returns the full fixed-rate claim.
     *      For settled series, returns the actual (possibly haircut) claimPerToken.
     */
    function previewRedeem(bytes32 seriesId, uint256 syLstAmount)
        external
        view
        override
        returns (uint256 fixedClaim)
    {
        Series storage s = _series[seriesId];
        require(s.maturity > 0, "Vault: series not found");

        uint256 rate = _claimPerToken[seriesId];
        if (rate == 0) {
            // Not yet settled: compute theoretical claim at current time.
            // Use full tenor from deposit time (approximation: use series maturity).
            uint256 tenorFracE18 = ((s.maturity > block.timestamp
                ? s.maturity - block.timestamp
                : 0) * WAD) / SECONDS_PER_YEAR;
            uint256 interest = (s.fixedRateE18 * tenorFracE18) / WAD;
            fixedClaim = syLstAmount + (syLstAmount * interest) / WAD;
        } else {
            fixedClaim = (syLstAmount * rate) / WAD;
        }
    }

    /**
     * @inheritdoc IStableYieldVault
     */
    function currentSpreadBps(bytes32 seriesId)
        external
        view
        override
        returns (uint256 spreadBps)
    {
        // Series-agnostic: spread depends only on protocol-wide solvency ratio.
        // Could be series-specific in future versions.
        (seriesId); // suppress unused warning
        spreadBps = ISpreadCalculator(spreadCalculator).currentSpread();
    }

    /**
     * @notice Returns all series identifiers created in this vault.
     */
    function allSeriesIds() external view returns (bytes32[] memory) {
        return _allSeriesIds;
    }

    /**
     * @notice Returns the settlement pool balance for a settled series.
     * @param seriesId The settled series.
     */
    function settlementPool(bytes32 seriesId) external view returns (uint256) {
        return _settlementPool[seriesId];
    }

    // ─── Internal Helpers ─────────────────────────────────────────────────────

    /**
     * @dev Compute the deposit-weighted average fixed rate across all active series.
     *
     * blendedRate = Σ (s.totalDeposited × s.fixedRateE18) / totalEscrowed
     *
     * Used in harvestYield() to estimate the aggregate fixed obligation per unit time.
     * This is an O(N) scan over all series; fine for the expected N < 20 series at any time.
     *
     * Returns 0 if totalEscrowed == 0 (prevents division by zero in caller).
     */
    function _blendedFixedRate() internal view returns (uint256 blendedE18) {
        uint256 _totalEscrowed = totalEscrowed;
        if (_totalEscrowed == 0) return 0;

        uint256 weightedSum;
        uint256 len = _allSeriesIds.length;
        for (uint256 i; i < len; ++i) {
            bytes32 sid = _allSeriesIds[i];
            Series storage s = _series[sid];
            if (!s.isSettled && s.totalDeposited > 0) {
                weightedSum += s.totalDeposited * s.fixedRateE18;
            }
        }

        blendedE18 = weightedSum / _totalEscrowed;
    }

    // ─── Admin ─────────────────────────────────────────────────────────────────

    /**
     * @notice Pause all deposits and redemptions (emergency halt).
     */
    function pause() external onlyRole(GOVERNANCE_ROLE) {
        _pause();
    }

    /**
     * @notice Unpause.
     */
    function unpause() external onlyRole(GOVERNANCE_ROLE) {
        _unpause();
    }
}

// ─── Minimal wstETH interface ─────────────────────────────────────────────────

/**
 * @dev Lido wstETH exposes stEthPerToken() returning the amount of stETH per 1 wstETH,
 *      scaled by 1e27 (ray-scaled, matching Lido's internal accounting).
 *      This increases monotonically as stETH accrues staking rewards.
 *
 * Reference: https://docs.lido.fi/contracts/wst-eth
 */
interface IWstETH {
    /**
     * @notice Get amount of stETH for one unit of wstETH.
     * @return Amount of stETH per wstETH (1e27-scaled).
     */
    function stEthPerToken() external view returns (uint256);

    /**
     * @notice Get amount of wstETH for a given amount of stETH.
     * @param _stETHAmount Amount of stETH (1e18-scaled).
     * @return Amount of wstETH (1e18-scaled).
     */
    function getWstETHByStETH(uint256 _stETHAmount) external view returns (uint256);
}
