// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/**
 * @title IStableYieldVault
 * @author StakeYield Finance
 * @notice Interface for the core StableYield vault that accepts wstETH deposits,
 *         mints syLST fixed-rate claims, and handles maturity redemptions.
 *
 * @dev Architecture Overview
 * ─────────────────────────
 * Users deposit wstETH into a series (identified by seriesId / quarterly maturity).
 * The vault mints ERC-1155 syLST tokens representing a fixed-rate claim payable at maturity.
 * The escrowed wstETH earns the floating stETH staking yield; the spread between
 * floating and fixed flows into (or out of) the ReserveManager.
 *
 * Fixed-rate payoff for a deposit of X wstETH at rate r_fixed for tenor T (years):
 *   Claim at maturity = X · (1 + r_fixed · T)    [simple interest, 365-day basis]
 *
 * The gap is settled at harvest / maturity:
 *   If r_float > r_fixed → surplus → reserve
 *   If r_float < r_fixed → deficit → drawn from reserve
 */
interface IStableYieldVault {
    // ─── Events ────────────────────────────────────────────────────────────────

    /**
     * @notice Emitted when a new quarterly maturity series is created.
     * @param seriesId      Unique identifier for the series (keccak256 of label, e.g. "2026Q3")
     * @param maturity      Unix timestamp when the series matures
     * @param fixedRateE18  Annualised fixed rate, scaled by 1e18 (e.g. 2.5% = 0.025e18)
     */
    event SeriesCreated(bytes32 indexed seriesId, uint256 maturity, uint256 fixedRateE18);

    /**
     * @notice Emitted when a series is closed (no new deposits accepted).
     * @param seriesId The series that was closed.
     */
    event SeriesClosed(bytes32 indexed seriesId);

    /**
     * @notice Emitted on every wstETH deposit.
     * @param seriesId       The target series.
     * @param depositor      Address that deposited wstETH.
     * @param wstEthAmount   Amount of wstETH deposited.
     * @param syLstMinted    Amount of syLST (ERC-1155 tokenId = seriesId) minted.
     * @param claimAtMaturity Fixed-rate wstETH amount redeemable at maturity.
     */
    event Deposited(
        bytes32 indexed seriesId,
        address indexed depositor,
        uint256 wstEthAmount,
        uint256 syLstMinted,
        uint256 claimAtMaturity
    );

    /**
     * @notice Emitted on redemption of mature syLST.
     * @param seriesId      The matured series.
     * @param redeemer      Address redeeming syLST.
     * @param syLstBurned   Amount of syLST burned.
     * @param wstEthReturned wstETH returned to the redeemer.
     */
    event Redeemed(
        bytes32 indexed seriesId,
        address indexed redeemer,
        uint256 syLstBurned,
        uint256 wstEthReturned
    );

    /**
     * @notice Emitted when yield is harvested from wstETH appreciation.
     * @param wstEthGain    The increase in wstETH value since last harvest.
     * @param toReserve     Net amount routed to (positive) or drawn from (negative as uint, see event flags) the reserve.
     */
    event YieldHarvested(uint256 wstEthGain, uint256 toReserve);

    // ─── Structs ───────────────────────────────────────────────────────────────

    /**
     * @notice Full metadata for a series.
     * @param maturity          Unix timestamp of maturity.
     * @param fixedRateE18      Annualised fixed rate (1e18 = 100%).
     * @param totalDeposited    Total wstETH deposited into this series.
     * @param totalClaims       Total wstETH owed at maturity (Σ principal · (1 + r · T)).
     * @param totalSyLst        Total syLST tokens in circulation for this series.
     * @param isOpen            Whether new deposits are accepted.
     * @param isSettled         Whether the series has been settled at maturity.
     */
    struct Series {
        uint256 maturity;
        uint256 fixedRateE18;
        uint256 totalDeposited;
        uint256 totalClaims;
        uint256 totalSyLst;
        bool isOpen;
        bool isSettled;
    }

    // ─── Governance ────────────────────────────────────────────────────────────

    /**
     * @notice Creates a new quarterly maturity series.
     * @dev Only callable by GOVERNANCE_ROLE. The maturity must be in the future and
     *      standardised to a quarterly boundary (enforced off-chain by convention).
     * @param seriesLabel       Human-readable label, e.g. "2026Q3". Used to derive seriesId.
     * @param maturityTimestamp Unix timestamp for maturity (must be > block.timestamp).
     * @param fixedRateE18      Annualised fixed rate scaled by 1e18.
     * @return seriesId         keccak256 hash of seriesLabel used as ERC-1155 tokenId.
     */
    function createSeries(
        string calldata seriesLabel,
        uint256 maturityTimestamp,
        uint256 fixedRateE18
    ) external returns (bytes32 seriesId);

    /**
     * @notice Closes a series to new deposits without settling it.
     * @dev Only callable by GOVERNANCE_ROLE.
     * @param seriesId The series to close.
     */
    function closeSeries(bytes32 seriesId) external;

    // ─── Core User Actions ────────────────────────────────────────────────────

    /**
     * @notice Deposit wstETH into an open series and receive syLST.
     * @dev
     *  - The caller must approve this contract to spend `wstEthAmount` of wstETH beforehand.
     *  - syLST minted = wstEthAmount (1:1 with principal, not with claim).
     *  - The maturity claim is stored per-token: each syLST token is redeemable for
     *    wstEthAmount · (1 + fixedRate · tenor/365) / totalSyLst at maturity — but since
     *    each user's syLST represents their exact share, the simplest accounting is to
     *    track claimPerToken for the series once it is settled.
     *
     * @param seriesId      Target series identifier.
     * @param wstEthAmount  Amount of wstETH to deposit (must be > 0).
     * @return syLstMinted  Amount of syLST minted to the caller.
     */
    function deposit(bytes32 seriesId, uint256 wstEthAmount) external returns (uint256 syLstMinted);

    /**
     * @notice Redeem syLST for wstETH after the series has matured and been settled.
     * @dev
     *  - Reverts if block.timestamp < series.maturity.
     *  - Burns syLST and transfers wstETH at the settled claimPerToken rate.
     *  - If the reserve was short, the effective rate may be reduced (haircut) — this
     *    is handled transparently by the settlement process.
     *
     * @param seriesId   The matured series.
     * @param syLstAmount Amount of syLST to burn and redeem.
     * @return wstEthOut  wstETH transferred to the caller.
     */
    function redeem(bytes32 seriesId, uint256 syLstAmount) external returns (uint256 wstEthOut);

    /**
     * @notice Harvests wstETH appreciation since last harvest across all active series,
     *         computing surplus/deficit vs fixed obligations and routing to ReserveManager.
     * @dev Callable by anyone (keeper-friendly); protected against reentrancy.
     *      The wstETH/ETH exchange rate is fetched from the wstETH contract.
     */
    function harvestYield() external;

    /**
     * @notice Settle a matured series: finalise claimPerToken and transfer collateral.
     * @dev Called by governance or keeper. Triggers reserve top-up or surplus deposit.
     * @param seriesId The series to settle (must be past maturity).
     */
    function settleSeries(bytes32 seriesId) external;

    // ─── View Functions ────────────────────────────────────────────────────────

    /**
     * @notice Returns full metadata for a series.
     */
    function getSeries(bytes32 seriesId) external view returns (Series memory);

    /**
     * @notice Computes the wstETH claim a given syLST balance will yield at maturity,
     *         using the current wstETH/stETH exchange rate for a floating-rate estimate.
     * @param seriesId     The series.
     * @param syLstAmount  Amount of syLST tokens.
     * @return fixedClaim  Fixed-rate wstETH owed at maturity.
     */
    function previewRedeem(bytes32 seriesId, uint256 syLstAmount)
        external
        view
        returns (uint256 fixedClaim);

    /**
     * @notice Returns the current dynamic spread applied to deposits for a given series,
     *         as basis points (100 bp = 1%).
     */
    function currentSpreadBps(bytes32 seriesId) external view returns (uint256 spreadBps);

    /**
     * @notice Returns the address of the wstETH token.
     */
    function wstETH() external view returns (address);

    /**
     * @notice Returns the address of the SyLST ERC-1155 token contract.
     */
    function syLST() external view returns (address);

    /**
     * @notice Returns the address of the ReserveManager.
     */
    function reserveManager() external view returns (address);
}
