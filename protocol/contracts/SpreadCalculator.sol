// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {AccessControl} from "@openzeppelin/contracts/access/AccessControl.sol";
import {ISpreadCalculator} from "./interfaces/ISpreadCalculator.sol";
import {IReserveManager} from "./interfaces/IReserveManager.sol";

/**
 * @title SpreadCalculator
 * @author StakeYield Finance
 * @notice Computes the dynamic deposit spread as a function of protocol solvency κ.
 *
 * @dev Formula
 * ────────────
 *   s(κ) = s_base · (1 + α · max(0, κ_target/κ − 1)^β)
 *
 * All rates are in 1e18 fixed-point; the output is in basis points (bp).
 *
 * Power approximation:
 *   Solidity lacks a native float power function. We compute x^β for x > 0, β > 0 via:
 *     x^β = exp(β · ln(x))
 *   using the Babylonian / Taylor-series approximations from PRBMath-style inline math.
 *   For simplicity and auditability we use the OpenZeppelin PRBMath library patterns
 *   but implement a compact version inline to avoid extra dependencies.
 *
 * Precision note:
 *   All intermediate multiplications are bounded by the constraint that both operands
 *   are reasonable solvency ratios (< 100e18) and parameters (α < 100e18, β < 5e18),
 *   so overflow is not a practical concern within normal operating parameters.
 */
contract SpreadCalculator is ISpreadCalculator, AccessControl {
    // ─── Roles ─────────────────────────────────────────────────────────────────

    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");

    // ─── State ──────────────────────────────────────────────────────────────────

    /// @notice Spread parameters, packed in a single storage slot where possible.
    SpreadParams private _params;

    /// @notice The ReserveManager used by currentSpread().
    IReserveManager public reserveManager;

    // ─── Constants ─────────────────────────────────────────────────────────────

    uint256 private constant WAD = 1e18;

    // Basis points scaling: 100 bp = 1% = 0.01. To convert bp to WAD: bp * 1e14.
    // To express s_base = 25 bp as WAD fraction: 25 * 1e14 = 0.0025e18.
    // We keep s_base in raw bp for readability and convert inside getSpread().

    // ─── Constructor ────────────────────────────────────────────────────────────

    /**
     * @param admin           Address granted DEFAULT_ADMIN_ROLE and GOVERNANCE_ROLE.
     * @param _reserveManager Address of the ReserveManager (for currentSpread()).
     * @param initialParams   Initial spread parameter set.
     */
    constructor(
        address admin,
        address _reserveManager,
        SpreadParams memory initialParams
    ) {
        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);

        reserveManager = IReserveManager(_reserveManager);
        _validateAndStore(initialParams);
    }

    // ─── ISpreadCalculator ─────────────────────────────────────────────────────

    /**
     * @inheritdoc ISpreadCalculator
     *
     * @dev Step-by-step:
     *  1. If κ >= κ_target → excess = 0 → s = s_base (no penalty)
     *  2. Else compute excess = κ_target/κ − 1  (WAD-scaled)
     *  3. Compute excess^β via exp(β · ln(excess))
     *  4. s = s_base · (1 + α · excess^β)  in basis points
     *
     * Critical guard: revert if κ <= κ_critical to prevent division by near-zero
     * values that would cause extreme spread values that could mislead callers.
     */
    function getSpread(uint256 kappaE18) external view override returns (uint256 spreadBps) {
        SpreadParams memory p = _params;

        require(kappaE18 > p.kappaCriticalE18, "SpreadCalculator: kappa at/below critical");

        // If solvency is at or above target, return base spread — no penalty applied.
        if (kappaE18 >= p.kappaTargetE18) {
            return p.sBaseBps;
        }

        // excess = κ_target / κ − 1  (WAD-scaled)
        // Both are WAD: (κ_target * WAD) / κ gives the ratio in WAD, then subtract WAD.
        uint256 excess = (p.kappaTargetE18 * WAD) / kappaE18 - WAD;

        // penalty = α · excess^β
        // We compute excess^β = exp(β · ln(excess)) using inline fixed-point math.
        uint256 excessPowBeta = _powWad(excess, p.betaE18);

        // penalty in WAD = α * excessPowBeta / WAD
        uint256 penaltyWad = (p.alphaE18 * excessPowBeta) / WAD;

        // s = s_base * (1 + penalty)  in basis points
        // (1 + penaltyWad) in WAD = WAD + penaltyWad
        spreadBps = (p.sBaseBps * (WAD + penaltyWad)) / WAD;

        // Safety: never exceed a sane maximum (e.g. 10000 bp = 100%) to prevent
        // overflow and protect against extreme parameter misconfiguration.
        if (spreadBps > 10_000) spreadBps = 10_000;
    }

    /**
     * @inheritdoc ISpreadCalculator
     */
    function currentSpread() external view override returns (uint256 spreadBps) {
        uint256 kappaE18 = reserveManager.kappa();
        return this.getSpread(kappaE18);
    }

    /**
     * @inheritdoc ISpreadCalculator
     */
    function setParameters(SpreadParams calldata params)
        external
        override
        onlyRole(GOVERNANCE_ROLE)
    {
        _validateAndStore(params);
        emit ParametersUpdated(
            params.sBaseBps,
            params.alphaE18,
            params.betaE18,
            params.kappaTargetE18,
            params.kappaCriticalE18
        );
    }

    /**
     * @inheritdoc ISpreadCalculator
     */
    function getParameters() external view override returns (SpreadParams memory) {
        return _params;
    }

    // ─── Internal ──────────────────────────────────────────────────────────────

    /**
     * @dev Validate and store parameter set.
     */
    function _validateAndStore(SpreadParams memory p) internal {
        require(p.sBaseBps > 0 && p.sBaseBps <= 1_000, "SpreadCalculator: invalid sBase");
        require(p.betaE18 >= WAD, "SpreadCalculator: beta must be >= 1");
        require(p.kappaTargetE18 > p.kappaCriticalE18, "SpreadCalculator: target <= critical");
        require(p.kappaCriticalE18 > 0, "SpreadCalculator: critical must be > 0");
        _params = p;
    }

    /**
     * @dev Fixed-point power: x^y where both x and y are WAD-scaled.
     *      Returns result in WAD.
     *
     *      For integer exponents, uses repeated squaring.
     *      For fractional exponents, uses exp(y * ln(x)) via Taylor series.
     *
     * @param x WAD-scaled base (must be > 0).
     * @param y WAD-scaled exponent (must be >= WAD, i.e., >= 1.0).
     * @return result WAD-scaled x^y.
     */
    function _powWad(uint256 x, uint256 y) internal pure returns (uint256 result) {
        if (x == 0) return 0;
        if (y == WAD) return x; // x^1 = x

        // Split y into integer and fractional parts
        uint256 yInt = y / WAD;
        uint256 yFrac = y % WAD;

        // Compute x^yInt by repeated squaring (all WAD-scaled)
        result = WAD;
        uint256 base = x;
        uint256 n = yInt;
        while (n > 0) {
            if (n % 2 == 1) {
                result = (result * base) / WAD;
            }
            base = (base * base) / WAD;
            n /= 2;
        }

        // For the fractional part, use exp(frac * ln(x))
        if (yFrac > 0) {
            int256 lnX = _lnWad(int256(x));
            int256 fracLnX = (int256(yFrac) * lnX) / int256(WAD);
            uint256 fracPow = uint256(_expWad(fracLnX));
            result = (result * fracPow) / WAD;
        }
    }

    /**
     * @dev Compute natural log of x (WAD-scaled) using a Taylor series.
     *      ln(x) = ln(a * 2^k) = k*ln(2) + ln(a) where a ∈ [1, 2).
     *      For a ∈ [1, 2), we use ln(a) = ln(1 + t) where t = a - 1 ∈ [0, 1)
     *      and Taylor: ln(1+t) = t - t²/2 + t³/3 - t⁴/4 + ...
     *
     *      Works for x > 0 (WAD-scaled, so x = 1e18 means 1.0).
     */
    function _lnWad(int256 x) internal pure returns (int256) {
        require(x > 0, "SpreadCalculator: ln(x) undefined for x <= 0");

        // Find k such that x = a * 2^k where a ∈ [WAD, 2*WAD)
        // i.e., shift x until it's in [1e18, 2e18)
        int256 k = 0;
        int256 a = x;

        // Scale up if a < WAD (x < 1.0)
        while (a < int256(WAD)) {
            a = a * 2;
            k -= 1;
        }
        // Scale down if a >= 2*WAD (x >= 2.0)
        while (a >= int256(2 * WAD)) {
            a = a / 2;
            k += 1;
        }

        // Now a ∈ [WAD, 2*WAD), compute ln(a/WAD) using Taylor series
        // t = (a - WAD), t is WAD-scaled in [0, WAD)
        int256 t = a - int256(WAD);

        // ln(1+t/WAD) = t/WAD - (t/WAD)^2/2 + (t/WAD)^3/3 - ...
        // In WAD arithmetic: result = t - t*t/(2*WAD) + t*t*t/(3*WAD^2) - ...
        int256 result = 0;
        int256 term = t; // t^1
        // 20 terms for good precision
        for (uint256 i = 1; i <= 20; i++) {
            if (i % 2 == 1) {
                result += term / int256(i);
            } else {
                result -= term / int256(i);
            }
            term = (term * t) / int256(WAD);
        }

        // Add k * ln(2)
        // ln(2) * 1e18 = 693147180559945309
        int256 LN2 = 693147180559945309;
        result += k * LN2;

        return result;
    }

    /**
     * @dev Compute exp(x) where x is a signed WAD integer.
     *      Uses Taylor series: exp(x) = 1 + x + x²/2! + x³/3! + ...
     *      With range reduction: exp(x) = 2^k * exp(r) where r ∈ [-ln2/2, ln2/2).
     *
     *      Accurate for |x| < 20e18 (sufficient for spread calculations).
     */
    function _expWad(int256 x) internal pure returns (int256) {
        // For very negative x, result is ~0
        if (x < -41 * int256(WAD)) return 0;
        require(x < 135 * int256(WAD), "SpreadCalculator: exp overflow");

        // Range reduction: express x = k * ln(2) + r where r ∈ [-ln2/2, ln2/2)
        int256 LN2 = 693147180559945309; // ln(2) * 1e18

        // k = round(x / ln2)
        int256 k;
        if (x >= 0) {
            k = (x + LN2 / 2) / LN2;
        } else {
            k = (x - LN2 / 2) / LN2;
        }
        int256 r = x - k * LN2;

        // Compute exp(r) using Taylor series (r is small, |r| < ln2/2 ≈ 0.347)
        // exp(r) = 1 + r/1! + r²/2! + r³/3! + ... + r^n/n!
        int256 result = int256(WAD);
        int256 term = int256(WAD);
        for (uint256 i = 1; i <= 20; i++) {
            term = (term * r) / (int256(i) * int256(WAD));
            result += term;
        }

        // Multiply by 2^k
        if (k >= 0) {
            result = result << uint256(k);
        } else {
            result = result >> uint256(-k);
        }

        return result;
    }
}
