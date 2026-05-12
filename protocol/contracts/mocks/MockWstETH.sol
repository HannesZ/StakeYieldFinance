// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";

/**
 * @title MockWstETH
 * @notice Test mock for Lido's wstETH token.
 *
 * @dev Simulates wstETH with an adjustable stEthPerToken() exchange rate.
 *      The initial rate is 1.15e18 (representing ~15% appreciation since genesis).
 *      Rate can be adjusted via setStEthPerToken() to simulate yield accrual in tests.
 *
 * Note: Lido's stEthPerToken() is WAD-scaled (1e18).
 * This mock matches that convention so the vault's harvest math works correctly.
 */
contract MockWstETH is ERC20 {
    // ─── State ──────────────────────────────────────────────────────────────────

    /// @notice stETH per wstETH exchange rate, WAD-scaled (1e18).
    ///         Starts at 1.15e18 representing ~15% staking appreciation since genesis.
    uint256 private _stEthPerToken = 1.15e18;

    // ─── Constructor ────────────────────────────────────────────────────────────

    constructor() ERC20("Wrapped Staked ETH (Mock)", "wstETH") {}

    // ─── Exchange Rate ───────────────────────────────────────────────────────────

    /**
     * @notice Returns the amount of stETH backing one wstETH.
     * @return stETH per wstETH, WAD-scaled (1e18). Increases monotonically in production.
     */
    function stEthPerToken() external view returns (uint256) {
        return _stEthPerToken;
    }

    /**
     * @notice Set the stETH-per-wstETH exchange rate (test manipulation only).
     * @param newRate New rate, ray-scaled (1e27). Must be >= current rate in realistic tests.
     */
    function setStEthPerToken(uint256 newRate) external {
        _stEthPerToken = newRate;
    }

    /**
     * @notice Convenience: get amount of wstETH for a given amount of stETH.
     * @param _stETHAmount Amount of stETH (1e18-scaled).
     * @return Amount of wstETH (1e18-scaled).
     */
    function getWstETHByStETH(uint256 _stETHAmount) external view returns (uint256) {
        return (_stETHAmount * 1e18) / _stEthPerToken;
    }

    // ─── Mint ────────────────────────────────────────────────────────────────────

    /**
     * @notice Mint wstETH to any address (test helper).
     * @param to     Recipient.
     * @param amount Amount of wstETH to mint (1e18-scaled).
     */
    function mint(address to, uint256 amount) external {
        _mint(to, amount);
    }
}
