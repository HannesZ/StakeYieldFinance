// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {AccessControl} from "@openzeppelin/contracts/access/AccessControl.sol";
import {ReentrancyGuard} from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import {Pausable} from "@openzeppelin/contracts/utils/Pausable.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";

import {IReserveManager} from "./interfaces/IReserveManager.sol";
import {SYLDToken} from "./SYLDToken.sol";

/**
 * @title ReserveManager
 * @author StakeYield Finance
 * @notice Holds the protocol reserve in wstETH, tracks solvency, distributes surplus
 *         to SYLD stakers, and orchestrates emergency recapitalisation via SYLD auctions.
 *
 * @dev Solvency Model
 * ──────────────────
 *   κ = reserve / total_liabilities
 *
 *   κ >= κ_target:   Protocol healthy. Surplus may be distributed to SYLD stakers.
 *   κ_emergency < κ < κ_target: Elevated risk. Higher spread charged on new deposits
 *                               to direct income toward reserve recovery.
 *   κ <= κ_emergency: Emergency. SYLD minted and auctioned for wstETH to recapitalise.
 *
 * Liability Accounting:
 *   The vault calls updateLiability() whenever deposits or redemptions change the
 *   outstanding fixed-rate obligation for a series. The ReserveManager holds the
 *   authoritative liability ledger.
 *
 *   total_liabilities = Σ seriesLiability[seriesId]
 *
 *   Each series' liability = Σ deposits_i · (1 + r_i · T_remaining_i / 365)
 *   This is the wstETH the protocol OWES to depositors at that series' maturity.
 *
 * Reserve Accounting:
 *   The reserve is the wstETH balance held in this contract, minus any pending
 *   auction proceeds. It grows via:
 *     - Surplus yield harvested by the vault (r_float > r_fixed)
 *   It shrinks via:
 *     - Deficit coverage (r_float < r_fixed)
 *     - Surplus distributions to SYLD stakers
 *
 * Emergency Auction:
 *   A simplified on-chain Dutch auction is scoped here. In production, governance
 *   may replace this with an external Gnosis Auction integration or similar.
 */
contract ReserveManager is IReserveManager, AccessControl, ReentrancyGuard, Pausable {
    using SafeERC20 for IERC20;

    // ─── Roles ─────────────────────────────────────────────────────────────────

    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 public constant VAULT_ROLE      = keccak256("VAULT_ROLE");
    bytes32 public constant KEEPER_ROLE     = keccak256("KEEPER_ROLE");

    // ─── Immutables ─────────────────────────────────────────────────────────────

    /// @notice wstETH token contract.
    IERC20 public immutable wstETH;

    /// @notice Protocol governance token — minted in emergencies.
    SYLDToken public immutable syld;

    /// @notice Address of the SYLD staking contract (receives surplus distributions).
    address public syldStaking;

    // ─── Solvency Parameters ────────────────────────────────────────────────────

    /// @notice Target solvency ratio (1e18-scaled). Default 1.2e18 (120%).
    uint256 public override kappaTarget;

    /// @notice Emergency solvency threshold (1e18-scaled). Default 1.05e18 (105%).
    uint256 public override kappaEmergency;

    // ─── Reserve & Liability State ──────────────────────────────────────────────

    /// @notice Per-series outstanding fixed-rate obligations.
    mapping(bytes32 => uint256) private _seriesLiability;

    /// @notice Sum of all active series liabilities.
    uint256 private _totalLiabilities;

    /// @notice Tracked reserve balance (for deposit verification).
    uint256 private _trackedReserve;

    // ─── Emergency Auction State ────────────────────────────────────────────────

    struct Auction {
        uint256 syldAmount;
        uint256 minWstEthRaise;
        uint256 deadline;
        bool    finalised;
    }

    mapping(uint256 => Auction) public auctions;
    uint256 public nextAuctionId;

    /// @notice Whether the protocol is in emergency mode.
    bool private _isEmergency;

    // ─── WAD Constant ──────────────────────────────────────────────────────────

    uint256 private constant WAD = 1e18;

    // ─── Constructor ────────────────────────────────────────────────────────────

    /**
     * @param admin           Address with DEFAULT_ADMIN_ROLE + GOVERNANCE_ROLE.
     * @param _wstETH         wstETH token address.
     * @param _syld           SYLDToken address (this contract needs MINTER_ROLE on it).
     * @param _syldStaking    Address receiving surplus wstETH distributions.
     * @param _kappaTarget    Target solvency ratio, 1e18-scaled (e.g. 1.2e18).
     * @param _kappaEmergency Emergency threshold, 1e18-scaled (e.g. 1.05e18).
     */
    constructor(
        address admin,
        address _wstETH,
        address _syld,
        address _syldStaking,
        uint256 _kappaTarget,
        uint256 _kappaEmergency
    ) {
        require(admin != address(0), "RM: zero admin");
        require(_wstETH != address(0), "RM: zero wstETH");
        require(_syld != address(0), "RM: zero SYLD");
        require(_kappaTarget > _kappaEmergency, "RM: target <= emergency");
        require(_kappaEmergency > WAD, "RM: emergency must be > 1.0");

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);

        wstETH          = IERC20(_wstETH);
        syld            = SYLDToken(_syld);
        syldStaking     = _syldStaking;
        kappaTarget     = _kappaTarget;
        kappaEmergency  = _kappaEmergency;
    }

    // ─── IReserveManager: Reserve Management ──────────────────────────────────

    /**
     * @inheritdoc IReserveManager
     * @dev Vault pulls wstETH into this contract first, then calls depositReserve().
     *      Uses wstETH.balanceOf(address(this)) as the source of truth.
     */
    function depositReserve(uint256 amount)
        external
        override
        onlyRole(VAULT_ROLE)
        nonReentrant
        whenNotPaused
    {
        require(amount > 0, "RM: zero deposit");

        // Vault must have already transferred wstETH to this contract.
        // We verify the actual balance exceeds our tracked reserve by at least `amount`.
        uint256 balance = wstETH.balanceOf(address(this));
        require(balance >= _trackedReserve + amount, "RM: wstETH not transferred");

        // Update tracked reserve to match actual balance.
        _trackedReserve = balance;

        uint256 newKappa = kappa();
        emit ReserveDeposited(amount, totalReserve(), newKappa);

        // If we were in emergency mode and κ recovered, exit emergency.
        if (_isEmergency && newKappa > kappaEmergency) {
            _isEmergency = false;
        }
    }

    /**
     * @inheritdoc IReserveManager
     * @dev Reverts if post-withdrawal κ < κ_emergency to prevent destabilisation.
     */
    function withdrawReserve(uint256 amount, address recipient)
        external
        override
        onlyRole(VAULT_ROLE)
        nonReentrant
        whenNotPaused
    {
        require(amount > 0, "RM: zero withdrawal");
        require(recipient != address(0), "RM: zero recipient");

        uint256 currentReserve = totalReserve();
        require(currentReserve >= amount, "RM: insufficient reserve");

        // Post-withdrawal solvency check.
        // Compute kappa after withdrawal and ensure it doesn't breach emergency threshold.
        uint256 newReserve = currentReserve - amount;
        uint256 liabs = _totalLiabilities;
        if (liabs > 0) {
            uint256 newKappa = (newReserve * WAD) / liabs;
            // Allow withdrawal below emergency only if liabilities = 0 or in explicit emergency.
            // In normal operation, vault should never withdraw below κ_emergency.
            require(
                newKappa >= kappaEmergency || liabs == 0,
                "RM: withdrawal would breach emergency threshold"
            );
        }

        wstETH.safeTransfer(recipient, amount);
        _trackedReserve = wstETH.balanceOf(address(this));

        emit ReserveWithdrawn(amount, totalReserve(), kappa());
    }

    /**
     * @inheritdoc IReserveManager
     * @dev
     *  Surplus = max(0, reserve − κ_target · liabilities)
     *  Distributes surplus to the SYLD staking contract.
     *
     * Callable by KEEPER_ROLE or governance. Designed to be called periodically
     * (e.g. by a Chainlink Automation job).
     */
    function distributeSurplus()
        external
        override
        onlyRole(KEEPER_ROLE)
        nonReentrant
        whenNotPaused
        returns (uint256 distributed)
    {
        uint256 reserve  = totalReserve();
        uint256 liabs    = _totalLiabilities;
        if (liabs == 0) return 0;

        // Target reserve = κ_target * liabilities / 1e18
        uint256 targetReserve = (kappaTarget * liabs) / WAD;
        if (reserve <= targetReserve) return 0;

        distributed = reserve - targetReserve;

        // Transfer surplus to SYLD staking contract.
        address staking = syldStaking;
        require(staking != address(0), "RM: staking not set");

        wstETH.safeTransfer(staking, distributed);
        _trackedReserve = wstETH.balanceOf(address(this));

        emit SurplusDistributed(distributed, staking);
    }

    // ─── IReserveManager: Liability Accounting ────────────────────────────────

    /**
     * @inheritdoc IReserveManager
     */
    function updateLiability(bytes32 seriesId, uint256 liability)
        external
        override
        onlyRole(VAULT_ROLE)
    {
        uint256 old = _seriesLiability[seriesId];

        // Update aggregate — handle both increases and decreases safely.
        if (liability > old) {
            _totalLiabilities += liability - old;
        } else {
            _totalLiabilities -= old - liability;
        }

        _seriesLiability[seriesId] = liability;

        emit LiabilityUpdated(seriesId, old, liability);
    }

    /**
     * @inheritdoc IReserveManager
     */
    function removeLiability(bytes32 seriesId)
        external
        override
        onlyRole(VAULT_ROLE)
    {
        uint256 liability = _seriesLiability[seriesId];
        if (liability == 0) return;

        _totalLiabilities -= liability;
        delete _seriesLiability[seriesId];

        emit LiabilityUpdated(seriesId, liability, 0);
    }

    // ─── IReserveManager: Emergency Recapitalisation ──────────────────────────

    /**
     * @inheritdoc IReserveManager
     * @dev Mints `syldToMint` SYLD tokens to this contract and records an auction entry.
     *      In production, integrate with a decentralised auction (e.g. Gnosis Auction).
     *      Here we store the auction state and emit an event for off-chain bots to settle.
     *
     * The minimum wstETH to raise is set to close the gap to κ_emergency level:
     *   minRaise = max(0, κ_emergency · liabilities − reserve)
     */
    function triggerEmergencyMint(uint256 syldToMint)
        external
        override
        onlyRole(GOVERNANCE_ROLE)
        nonReentrant
        returns (uint256 auctionId)
    {
        require(kappa() < kappaEmergency, "RM: not in emergency");
        require(syldToMint > 0, "RM: zero SYLD to mint");

        // Mint SYLD to this contract for auction.
        syld.mint(address(this), syldToMint, "emergency-recapitalisation");

        // Compute minimum raise to restore κ_emergency.
        uint256 minRaise = 0;
        uint256 reserve = totalReserve();
        uint256 liabs   = _totalLiabilities;
        if (liabs > 0) {
            uint256 emergencyTarget = (kappaEmergency * liabs) / WAD;
            if (reserve < emergencyTarget) {
                minRaise = emergencyTarget - reserve;
            }
        }

        auctionId = nextAuctionId++;
        auctions[auctionId] = Auction({
            syldAmount:    syldToMint,
            minWstEthRaise: minRaise,
            deadline:      block.timestamp + 3 days,
            finalised:     false
        });

        _isEmergency = true;

        emit EmergencyModeActivated(kappa(), syldToMint);
    }

    /**
     * @inheritdoc IReserveManager
     * @dev Called by the auction settlement contract (granted VAULT_ROLE or a separate AUCTION_ROLE).
     *      Verifies wstETH has been transferred to this contract before finalising.
     */
    function finaliseEmergencyAuction(uint256 auctionId, uint256 wstEthRaised)
        external
        override
        onlyRole(GOVERNANCE_ROLE)
        nonReentrant
    {
        Auction storage auction = auctions[auctionId];
        require(!auction.finalised, "RM: already finalised");
        require(block.timestamp <= auction.deadline, "RM: auction expired");
        require(wstEthRaised >= auction.minWstEthRaise, "RM: insufficient raise");

        auction.finalised = true;

        // Verify wstETH is in this contract (transferred by auction settlement).
        require(
            wstETH.balanceOf(address(this)) >= totalReserve() + wstEthRaised,
            "RM: wstETH not received"
        );

        // Burn unsold SYLD if any (simplification: assume all SYLD sold).
        // In production: check SYLD balance and burn remainder.

        emit EmergencyRecapitalised(wstEthRaised);

        // Check if we've recovered above emergency threshold.
        if (kappa() >= kappaEmergency) {
            _isEmergency = false;
        }
    }

    // ─── IReserveManager: View Functions ─────────────────────────────────────

    /**
     * @inheritdoc IReserveManager
     * @dev The reserve is the wstETH ERC-20 balance of this contract.
     *      This automatically includes any auction proceeds that have been transferred in.
     */
    function totalReserve() public view override returns (uint256) {
        return wstETH.balanceOf(address(this));
    }

    /**
     * @inheritdoc IReserveManager
     */
    function totalLiabilities() external view override returns (uint256) {
        return _totalLiabilities;
    }

    /**
     * @inheritdoc IReserveManager
     * @dev Returns type(uint256).max when liabilities == 0 (fully solvent, no obligations).
     */
    function kappa() public view override returns (uint256) {
        uint256 liabs = _totalLiabilities;
        if (liabs == 0) return type(uint256).max;
        return (totalReserve() * WAD) / liabs;
    }

    /**
     * @inheritdoc IReserveManager
     */
    function seriesLiability(bytes32 seriesId) external view override returns (uint256) {
        return _seriesLiability[seriesId];
    }

    /**
     * @inheritdoc IReserveManager
     */
    function isEmergency() external view override returns (bool) {
        return _isEmergency;
    }

    // ─── Governance ────────────────────────────────────────────────────────────

    /**
     * @notice Update solvency thresholds.
     * @param newTarget    New κ_target (must be > newEmergency and > WAD).
     * @param newEmergency New κ_emergency (must be > WAD).
     */
    function setKappaThresholds(uint256 newTarget, uint256 newEmergency)
        external
        onlyRole(GOVERNANCE_ROLE)
    {
        require(newTarget > newEmergency, "RM: target <= emergency");
        require(newEmergency > WAD, "RM: emergency must be > 1.0");
        kappaTarget    = newTarget;
        kappaEmergency = newEmergency;
    }

    /**
     * @notice Update the SYLD staking contract address.
     * @param _syldStaking New staking contract address.
     */
    function setSyldStaking(address _syldStaking)
        external
        onlyRole(GOVERNANCE_ROLE)
    {
        require(_syldStaking != address(0), "RM: zero staking");
        syldStaking = _syldStaking;
    }

    /**
     * @notice Pause the contract (emergency halt).
     */
    function pause() external onlyRole(GOVERNANCE_ROLE) {
        _pause();
    }

    /**
     * @notice Unpause the contract.
     */
    function unpause() external onlyRole(GOVERNANCE_ROLE) {
        _unpause();
    }
}
