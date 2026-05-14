// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {IERC20}    from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";

import {IStableYieldVault} from "./interfaces/IStableYieldVault.sol";

/**
 * @title ZapRouter
 * @author StakeYield Finance
 * @notice Convenience router that lets users deposit plain ETH into StakeYield
 *         in a single transaction: ETH → stETH → wstETH → vault.depositFor().
 *
 * ════════════════════════════════════════════════════════════════════════════════
 * FLOW
 * ════════════════════════════════════════════════════════════════════════════════
 *
 *   1. User sends ETH via depositETH(seriesId) { value: X }
 *   2. Router calls Lido stETH.submit{value: X}(address(0)) → receives X stETH
 *   3. Router approves wstETH to spend the stETH
 *   4. Router calls wstETH.wrap(stETHAmount) → receives Y wstETH
 *   5. Router approves vault to spend the wstETH
 *   6. Router calls vault.depositFor(seriesId, Y, msg.sender) → syLST to user
 *   7. Any dust (rounding) is returned to the user
 *
 * ════════════════════════════════════════════════════════════════════════════════
 * GAS
 * ════════════════════════════════════════════════════════════════════════════════
 *
 *   Extra overhead vs direct wstETH deposit:
 *     - Lido submit():    ~70k gas
 *     - stETH approve:    ~45k gas (could be 0 after first tx with infinite approval)
 *     - wstETH wrap():    ~55k gas
 *     - wstETH approve:   ~45k gas (could be 0 after first tx with infinite approval)
 *     Total overhead: ~100–215k gas ≈ $0.50–$2.00 at typical L1 gas prices.
 *
 *   The router uses infinite approvals on first use and caches them, so repeat
 *   deposits only pay for submit() + wrap() overhead (~125k gas).
 *
 * ════════════════════════════════════════════════════════════════════════════════
 * SECURITY
 * ════════════════════════════════════════════════════════════════════════════════
 *
 *   - Stateless: no funds are stored between transactions.
 *   - Non-upgradeable: immutable references, no admin functions.
 *   - No reentrancy risk: all external calls are to trusted Lido + vault contracts.
 *   - Dust sweep at end ensures no ETH/stETH/wstETH is stuck in the router.
 */
contract ZapRouter {
    using SafeERC20 for IERC20;

    // ─── Immutables ─────────────────────────────────────────────────────────────

    /// @notice Lido stETH token (receives ETH via submit()).
    ILidoStETH public immutable stETH;

    /// @notice Lido wstETH token (wraps stETH).
    ILidoWstETH public immutable wstETH;

    /// @notice StakeYield vault (accepts wstETH deposits).
    IStableYieldVault public immutable vault;

    // ─── Events ─────────────────────────────────────────────────────────────────

    event ZapDeposit(
        bytes32 indexed seriesId,
        address indexed depositor,
        uint256 ethAmount,
        uint256 wstEthDeposited,
        uint256 syLstMinted
    );

    // ─── Constructor ────────────────────────────────────────────────────────────

    /**
     * @param _stETH  Lido stETH address.
     * @param _wstETH Lido wstETH address.
     * @param _vault  StakeYield vault address.
     */
    constructor(address _stETH, address _wstETH, address _vault) {
        require(_stETH  != address(0), "Zap: zero stETH");
        require(_wstETH != address(0), "Zap: zero wstETH");
        require(_vault  != address(0), "Zap: zero vault");

        stETH  = ILidoStETH(_stETH);
        wstETH = ILidoWstETH(_wstETH);
        vault  = IStableYieldVault(_vault);

        // Pre-approve wstETH contract to spend stETH (infinite approval).
        IERC20(_stETH).forceApprove(_wstETH, type(uint256).max);
        // Pre-approve vault to spend wstETH (infinite approval).
        IERC20(_wstETH).forceApprove(_vault, type(uint256).max);
    }

    // ─── Zap: ETH → stETH → wstETH → Vault ────────────────────────────────────

    /**
     * @notice Deposit ETH into a StakeYield series in one transaction.
     * @dev The caller sends ETH as msg.value. The router converts to wstETH and
     *      deposits into the vault on behalf of the caller. syLST is minted
     *      directly to msg.sender.
     *
     *      Gas overhead: ~125–215k gas on top of the base vault.deposit() cost.
     *      The user pays for the extra gas; the protocol bears no cost.
     *
     * @param seriesId Target series identifier.
     * @return syLstMinted Amount of syLST minted to the caller.
     */
    function depositETH(bytes32 seriesId)
        external
        payable
        returns (uint256 syLstMinted)
    {
        require(msg.value > 0, "Zap: zero ETH");

        // ── 1. ETH → stETH via Lido submit() ───────────────────────────────────
        //    submit() returns the amount of stETH shares received.
        //    The stETH balance of this contract increases by ~msg.value (minus 1 wei rounding).
        uint256 stEthBefore = IERC20(address(stETH)).balanceOf(address(this));
        stETH.submit{value: msg.value}(address(0));
        uint256 stEthReceived = IERC20(address(stETH)).balanceOf(address(this)) - stEthBefore;

        require(stEthReceived > 0, "Zap: stETH submit failed");

        // ── 2. stETH → wstETH via wrap() ────────────────────────────────────────
        uint256 wstEthBefore = IERC20(address(wstETH)).balanceOf(address(this));
        wstETH.wrap(stEthReceived);
        uint256 wstEthReceived = IERC20(address(wstETH)).balanceOf(address(this)) - wstEthBefore;

        require(wstEthReceived > 0, "Zap: wstETH wrap failed");

        // ── 3. wstETH → Vault depositFor() ──────────────────────────────────────
        syLstMinted = vault.depositFor(seriesId, wstEthReceived, msg.sender);

        // ── 4. Sweep any dust back to the user ──────────────────────────────────
        _sweepDust(msg.sender);

        emit ZapDeposit(seriesId, msg.sender, msg.value, wstEthReceived, syLstMinted);
    }

    // ─── Internal ───────────────────────────────────────────────────────────────

    /**
     * @dev Return any leftover stETH/wstETH/ETH dust to the user.
     *      Lido's submit() and wrap() can leave 1-2 wei of rounding dust.
     */
    function _sweepDust(address to) internal {
        uint256 stEthDust = IERC20(address(stETH)).balanceOf(address(this));
        if (stEthDust > 0) {
            IERC20(address(stETH)).safeTransfer(to, stEthDust);
        }

        uint256 wstEthDust = IERC20(address(wstETH)).balanceOf(address(this));
        if (wstEthDust > 0) {
            IERC20(address(wstETH)).safeTransfer(to, wstEthDust);
        }

        uint256 ethDust = address(this).balance;
        if (ethDust > 0) {
            (bool ok, ) = to.call{value: ethDust}("");
            require(ok, "Zap: ETH refund failed");
        }
    }

    /// @dev Accept ETH refunds from Lido (if any).
    receive() external payable {}
}

// ─── Minimal Lido Interfaces ────────────────────────────────────────────────

/**
 * @dev Lido stETH: submit ETH to receive stETH (rebasing ERC-20).
 *      Reference: https://docs.lido.fi/contracts/lido
 */
interface ILidoStETH {
    /**
     * @notice Submit ETH to Lido and receive stETH.
     * @param _referral Referral address (use address(0) if none).
     * @return Amount of stETH shares received.
     */
    function submit(address _referral) external payable returns (uint256);
}

/**
 * @dev Lido wstETH: wrap stETH to receive non-rebasing wstETH.
 *      Reference: https://docs.lido.fi/contracts/wst-eth
 */
interface ILidoWstETH {
    /**
     * @notice Wrap stETH into wstETH.
     * @param _stETHAmount Amount of stETH to wrap.
     * @return Amount of wstETH received.
     */
    function wrap(uint256 _stETHAmount) external returns (uint256);

    /**
     * @notice stETH per 1 wstETH (1e18-scaled).
     */
    function stEthPerToken() external view returns (uint256);
}
