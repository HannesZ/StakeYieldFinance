// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import {ERC20Permit} from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Permit.sol";
import {ERC20Votes} from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Votes.sol";
import {AccessControl} from "@openzeppelin/contracts/access/AccessControl.sol";
import {Nonces} from "@openzeppelin/contracts/utils/Nonces.sol";

/**
 * @title SYLDToken
 * @author StakeYield Finance
 * @notice Governance token for the StakeYield Finance protocol.
 *
 * @dev Token Design
 * ─────────────────
 * SYLD is a standard ERC-20 with:
 *   - ERC-20Votes (on-chain delegation + snapshot voting power for governance)
 *   - ERC-20Permit (gasless approvals via EIP-2612)
 *   - Role-based minting (MINTER_ROLE granted to ReserveManager for emergency recapitalisation)
 *
 * Economic Role:
 *   - SYLD stakers receive wstETH surplus distributions when κ > κ_target
 *   - In emergencies (κ < κ_emergency), new SYLD is minted and auctioned for wstETH
 *     to recapitalise the reserve. This dilutes existing holders but preserves protocol
 *     solvency — akin to a last-resort insurance fund.
 *   - Governance votes determine: spread parameters, series creation, κ thresholds.
 *
 * Minting Policy:
 *   - Initial supply minted to treasury at deployment.
 *   - Additional minting only via MINTER_ROLE, restricted to ReserveManager.
 *   - No hard cap: the governance token supply is elastic as a backstop mechanism.
 *     However, governance should pass proposals that minimise unnecessary dilution.
 *
 * Security Considerations:
 *   - MINTER_ROLE is highly privileged. It should be held by an immutable ReserveManager
 *     or a multi-sig with time-lock delay.
 *   - Emergency mints are expected to be rare; each triggers an on-chain event
 *     and an auction for accountability.
 */
contract SYLDToken is ERC20, ERC20Permit, ERC20Votes, AccessControl {
    // ─── Roles ─────────────────────────────────────────────────────────────────

    /// @notice Can mint new SYLD tokens. Granted to ReserveManager for emergency backstop.
    bytes32 public constant MINTER_ROLE = keccak256("MINTER_ROLE");

    /// @notice Can burn SYLD (e.g. for buy-back programmes).
    bytes32 public constant BURNER_ROLE = keccak256("BURNER_ROLE");

    // ─── Events ────────────────────────────────────────────────────────────────

    /// @notice Emitted on any programmatic mint (excluding initial supply).
    event Minted(address indexed to, uint256 amount, string reason);

    /// @notice Emitted on any programmatic burn.
    event Burned(address indexed from, uint256 amount, string reason);

    // ─── Constructor ────────────────────────────────────────────────────────────

    /**
     * @param admin           Address granted DEFAULT_ADMIN_ROLE (multi-sig / timelock).
     * @param treasury        Address that receives the initial token supply.
     * @param initialSupply   Initial SYLD minted to treasury (18 decimals).
     */
    constructor(
        address admin,
        address treasury,
        uint256 initialSupply
    )
        ERC20("StakeYield Finance", "SYLD")
        ERC20Permit("StakeYield Finance")
    {
        require(admin != address(0), "SYLD: zero admin");
        require(treasury != address(0), "SYLD: zero treasury");

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(MINTER_ROLE, admin); // admin can grant to ReserveManager

        if (initialSupply > 0) {
            _mint(treasury, initialSupply);
        }
    }

    // ─── Minting & Burning ─────────────────────────────────────────────────────

    /**
     * @notice Mint SYLD to `to` with an on-chain reason string.
     * @dev Only callable by MINTER_ROLE. The reason string is emitted in an event
     *      for transparency (e.g. "emergency-recapitalisation-auction-42").
     * @param to     Recipient address.
     * @param amount Amount of SYLD to mint (18 decimals).
     * @param reason Human-readable rationale (stored in event log only).
     */
    function mint(address to, uint256 amount, string calldata reason)
        external
        onlyRole(MINTER_ROLE)
    {
        require(to != address(0), "SYLD: mint to zero");
        require(amount > 0, "SYLD: zero mint");
        _mint(to, amount);
        emit Minted(to, amount, reason);
    }

    /**
     * @notice Burn SYLD from `from`.
     * @dev The caller must be BURNER_ROLE, OR the token holder burning their own tokens.
     *      Token holders can always self-burn (no role required for self).
     * @param from   Address whose tokens to burn.
     * @param amount Amount of SYLD to burn.
     * @param reason Human-readable rationale.
     */
    function burn(address from, uint256 amount, string calldata reason)
        external
    {
        if (from != msg.sender) {
            _checkRole(BURNER_ROLE);
        }
        require(amount > 0, "SYLD: zero burn");
        _burn(from, amount);
        emit Burned(from, amount, reason);
    }

    // ─── ERC-20 Votes Overrides ────────────────────────────────────────────────

    /**
     * @dev Required override: ERC20Votes needs to know the clock mode.
     *      We use block timestamps (EIP-6372 mode = "timestamp") for compatibility
     *      with most governor frameworks and to avoid block reorganisation issues.
     */
    function clock() public view override returns (uint48) {
        return uint48(block.timestamp);
    }

    /**
     * @dev EIP-6372 clock mode descriptor.
     */
    // solhint-disable-next-line func-name-mixedcase
    function CLOCK_MODE() public pure override returns (string memory) {
        return "mode=timestamp";
    }

    /**
     * @dev Override required by Solidity for multiple inheritance.
     *      _update is the internal transfer/mint/burn hook in OZ v5.
     */
    function _update(address from, address to, uint256 value)
        internal
        override(ERC20, ERC20Votes)
    {
        super._update(from, to, value);
    }

    /**
     * @dev Override required by Solidity for Nonces used by both ERC20Permit and ERC20Votes.
     */
    function nonces(address owner)
        public
        view
        override(ERC20Permit, Nonces)
        returns (uint256)
    {
        return super.nonces(owner);
    }
}
