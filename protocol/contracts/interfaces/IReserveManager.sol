// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/**
 * @title IReserveManager
 * @author StakeYield Finance
 * @notice Interface for the ReserveManager: the actuarial backstop that tracks the
 *         protocol's wstETH reserve, monitors solvency, routes surplus to SYLD stakers,
 *         and orchestrates emergency recapitalisation.
 *
 * @dev Solvency Model
 * ──────────────────
 * The solvency ratio κ is defined as:
 *
 *   κ = reserve / total_liabilities
 *
 * where:
 *   reserve           = wstETH held in this contract
 *   total_liabilities = Σ_{all active series} (notional_i · (1 + r_i · T_remaining_i))
 *                       i.e., the sum of all outstanding fixed-rate obligations
 *
 * Target solvency κ_target: the ratio at which the protocol is considered healthy.
 *   Surplus above κ_target is distributed to SYLD stakers.
 *
 * Emergency threshold κ_emergency: if κ falls below this, the protocol enters
 *   emergency mode, minting SYLD to auction for wstETH and recapitalise.
 *
 * Spread interaction:
 *   The SpreadCalculator reads κ from this contract when computing the dynamic
 *   spread charged on new deposits. As κ drops, the spread rises exponentially,
 *   charging new depositors a higher fixed-rate margin that flows to the reserve.
 */
interface IReserveManager {
    // ─── Events ────────────────────────────────────────────────────────────────

    /**
     * @notice Emitted when wstETH is deposited into the reserve (e.g. from yield harvest).
     * @param amount      wstETH added to reserve.
     * @param newReserve  Total reserve after deposit.
     * @param newKappa    New solvency ratio (1e18-scaled).
     */
    event ReserveDeposited(uint256 amount, uint256 newReserve, uint256 newKappa);

    /**
     * @notice Emitted when wstETH is drawn from the reserve (e.g. to cover a deficit series).
     * @param amount      wstETH withdrawn.
     * @param newReserve  Total reserve after withdrawal.
     * @param newKappa    New solvency ratio (1e18-scaled).
     */
    event ReserveWithdrawn(uint256 amount, uint256 newReserve, uint256 newKappa);

    /**
     * @notice Emitted when surplus above κ_target is distributed to SYLD stakers.
     * @param surplusWstEth  wstETH distributed.
     * @param recipient      Address that received the surplus (e.g. SYLD staking contract).
     */
    event SurplusDistributed(uint256 surplusWstEth, address indexed recipient);

    /**
     * @notice Emitted when the protocol enters emergency mode due to κ < κ_emergency.
     * @param kappaAtTrigger  Solvency ratio that triggered the event.
     * @param syldMinted      SYLD tokens minted for the emergency auction.
     */
    event EmergencyModeActivated(uint256 kappaAtTrigger, uint256 syldMinted);

    /**
     * @notice Emitted when emergency recapitalisation completes.
     * @param wstEthRaised  wstETH added to reserve from the SYLD auction.
     */
    event EmergencyRecapitalised(uint256 wstEthRaised);

    /**
     * @notice Emitted when liability tracking is updated for a series.
     * @param seriesId      The affected series.
     * @param oldLiability  Previous liability amount for this series.
     * @param newLiability  Updated liability amount.
     */
    event LiabilityUpdated(bytes32 indexed seriesId, uint256 oldLiability, uint256 newLiability);

    // ─── Reserve Management ────────────────────────────────────────────────────

    /**
     * @notice Deposit wstETH into the reserve.
     * @dev Only callable by VAULT_ROLE. Caller must have already transferred wstETH to
     *      this contract (pull model), or this function may pull via transferFrom — see impl.
     * @param amount wstETH to add to the reserve.
     */
    function depositReserve(uint256 amount) external;

    /**
     * @notice Withdraw wstETH from the reserve to cover a fixed-rate obligation shortfall.
     * @dev Only callable by VAULT_ROLE. Reverts if withdrawal would push κ below κ_emergency.
     * @param amount    wstETH to withdraw.
     * @param recipient Address to receive the wstETH.
     */
    function withdrawReserve(uint256 amount, address recipient) external;

    /**
     * @notice Distribute surplus reserve (κ > κ_target) to the SYLD staking contract.
     * @dev The surplus is max(0, reserve − κ_target · total_liabilities).
     *      Callable by KEEPER_ROLE or governance.
     * @return distributed  wstETH actually distributed.
     */
    function distributeSurplus() external returns (uint256 distributed);

    // ─── Liability Accounting ──────────────────────────────────────────────────

    /**
     * @notice Register or update a series' outstanding fixed-rate liability.
     * @dev Called by the vault when a series is created, deposits change, or the series settles.
     *      Liability for series i = Σ deposits_i · (1 + r_i · T_remaining_i / 365)
     * @param seriesId   The series identifier.
     * @param liability  New total liability for this series in wstETH.
     */
    function updateLiability(bytes32 seriesId, uint256 liability) external;

    /**
     * @notice Remove a series' liability once it has fully settled (all tokens redeemed).
     * @param seriesId The series to remove from liability tracking.
     */
    function removeLiability(bytes32 seriesId) external;

    // ─── Emergency Recapitalisation ────────────────────────────────────────────

    /**
     * @notice Trigger emergency mode: mint SYLD and begin auction to raise wstETH.
     * @dev Reverts if κ >= κ_emergency. Only callable by GOVERNANCE_ROLE or automatically
     *      triggered by the vault when solvency is critically low.
     * @param syldToMint   Amount of SYLD to mint and offer at auction.
     * @return auctionId   Identifier for the resulting SYLD/wstETH auction.
     */
    function triggerEmergencyMint(uint256 syldToMint) external returns (uint256 auctionId);

    /**
     * @notice Finalise an emergency auction and deposit the raised wstETH into reserve.
     * @dev Called by the auction contract after completion.
     * @param auctionId    The auction to finalise.
     * @param wstEthRaised wstETH proceeds from the auction.
     */
    function finaliseEmergencyAuction(uint256 auctionId, uint256 wstEthRaised) external;

    // ─── View Functions ────────────────────────────────────────────────────────

    /**
     * @notice Current wstETH reserve held by this contract.
     */
    function totalReserve() external view returns (uint256);

    /**
     * @notice Sum of all outstanding fixed-rate obligations across all active series.
     * @dev Updated lazily on deposit/settlement; may be slightly stale for time-decay.
     */
    function totalLiabilities() external view returns (uint256);

    /**
     * @notice Current solvency ratio κ = reserve / liabilities, 1e18-scaled.
     * @dev Returns type(uint256).max when liabilities == 0 (fully solvent).
     */
    function kappa() external view returns (uint256);

    /**
     * @notice Target solvency ratio κ_target, 1e18-scaled.
     */
    function kappaTarget() external view returns (uint256);

    /**
     * @notice Emergency solvency threshold κ_emergency, 1e18-scaled.
     */
    function kappaEmergency() external view returns (uint256);

    /**
     * @notice Liability tracked for a specific series.
     * @param seriesId The series identifier.
     */
    function seriesLiability(bytes32 seriesId) external view returns (uint256);

    /**
     * @notice Whether the protocol is currently in emergency mode.
     */
    function isEmergency() external view returns (bool);
}
