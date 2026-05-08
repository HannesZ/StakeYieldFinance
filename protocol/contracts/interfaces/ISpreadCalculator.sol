// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/**
 * @title ISpreadCalculator
 * @author StakeYield Finance
 * @notice Interface for the dynamic spread calculator applied to new deposits.
 *
 * @dev Spread Formula
 * ──────────────────
 * The protocol charges a spread s on top of the base fixed rate to compensate the
 * reserve for solvency risk. The spread function is:
 *
 *   s(κ) = s_base · (1 + α · max(0, κ_target/κ − 1)^β)
 *
 * where:
 *   κ         = current solvency ratio = reserve / liabilities  (1e18-scaled)
 *   κ_target  = target solvency ratio                           (1e18-scaled)
 *   s_base    = minimum spread charged in all conditions        (basis points)
 *   α         = amplification coefficient                       (1e18-scaled)
 *   β         = exponent controlling convexity                  (1e18-scaled)
 *
 * Behaviour:
 *   - When κ >= κ_target: spread = s_base (no extra penalty)
 *   - When κ < κ_target:  spread rises super-linearly, creating strong incentives
 *     for new deposits (higher fixed rate offered = more spread income for reserve)
 *   - As κ → 0:           spread → ∞ (hard floor from κ_critical prevents singularity)
 *
 * Fixed-point Implementation Note:
 *   All intermediate calculations are performed in 1e18 fixed-point.
 *   The result is returned in basis points (1 bp = 0.01%).
 *   Power function (x^β for fractional β) is approximated via exp(β · ln(x)).
 *
 * Example:
 *   s_base = 25 bp, α = 2.0, β = 2.0, κ_target = 1.2, κ = 0.8
 *   excess = κ_target/κ - 1 = 1.2/0.8 - 1 = 0.5
 *   s = 25 · (1 + 2.0 · 0.5^2.0) = 25 · (1 + 0.5) = 37.5 bp
 */
interface ISpreadCalculator {
    // ─── Events ────────────────────────────────────────────────────────────────

    /**
     * @notice Emitted when spread parameters are updated by governance.
     * @param sBaseBps      New base spread in basis points.
     * @param alphaE18      New α, 1e18-scaled.
     * @param betaE18       New β, 1e18-scaled.
     * @param kappaTargetE18 New κ_target, 1e18-scaled.
     * @param kappaCriticalE18 New κ_critical floor, 1e18-scaled.
     */
    event ParametersUpdated(
        uint256 sBaseBps,
        uint256 alphaE18,
        uint256 betaE18,
        uint256 kappaTargetE18,
        uint256 kappaCriticalE18
    );

    // ─── Structs ───────────────────────────────────────────────────────────────

    /**
     * @notice Packed spread parameters for efficient storage and reading.
     */
    struct SpreadParams {
        uint256 sBaseBps;         // base spread: minimum, in basis points
        uint256 alphaE18;         // α: amplification, 1e18-scaled
        uint256 betaE18;          // β: convexity exponent, 1e18-scaled
        uint256 kappaTargetE18;   // κ_target: healthy solvency ratio, 1e18-scaled
        uint256 kappaCriticalE18; // κ_critical: minimum κ before getSpread reverts, 1e18-scaled
    }

    // ─── Core Computation ─────────────────────────────────────────────────────

    /**
     * @notice Compute the dynamic spread for a given solvency ratio.
     * @dev Pure view — no state modifications. Safe to call from any contract.
     *      Reverts if kappaE18 <= kappaCriticalE18 (protocol in critical state).
     *
     * Formula: s(κ) = s_base · (1 + α · max(0, κ_target/κ − 1)^β)
     *
     * @param kappaE18  Current solvency ratio, 1e18-scaled.
     *                  E.g. 1.2 = 1.2e18 (reserve is 120% of liabilities).
     * @return spreadBps Spread in basis points (100 bp = 1%).
     */
    function getSpread(uint256 kappaE18) external view returns (uint256 spreadBps);

    /**
     * @notice Compute the spread using current on-chain κ from the ReserveManager.
     * @dev Convenience wrapper: reads kappa() from the stored ReserveManager address.
     * @return spreadBps Spread in basis points.
     */
    function currentSpread() external view returns (uint256 spreadBps);

    // ─── Parameter Management ─────────────────────────────────────────────────

    /**
     * @notice Update spread parameters.
     * @dev Only callable by GOVERNANCE_ROLE.
     * @param params New parameter set. All fields validated (e.g. beta >= 1e18).
     */
    function setParameters(SpreadParams calldata params) external;

    /**
     * @notice Returns the current spread parameter set.
     */
    function getParameters() external view returns (SpreadParams memory);
}
