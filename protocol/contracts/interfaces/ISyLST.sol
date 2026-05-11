// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {IERC1155} from "@openzeppelin/contracts/token/ERC1155/IERC1155.sol";

/**
 * @title ISyLST
 * @author StakeYield Finance
 * @notice Interface for the SyLST token: an ERC-1155 where each tokenId corresponds to
 *         a distinct quarterly maturity series of the StableYield vault.
 *
 * @dev Design rationale — ERC-1155 vs per-series ERC-20
 * ───────────────────────────────────────────────────────
 * Using ERC-1155 instead of deploying a new ERC-20 per quarter:
 *  - Reduces deployment gas (one contract instead of N).
 *  - Aggregated approval flow; holders approve the single SyLST contract.
 *  - Batch transfers allow efficient liquidation across multiple maturities.
 *  - Marketplaces (OpenSea, 1inch) support ERC-1155 natively.
 *
 * Token semantics:
 *  - tokenId  = seriesId (bytes32 cast to uint256)
 *  - balance  = syLST owned for that series
 *  - Each token is redeemable 1:1 with the deposited principal PLUS
 *    the accrued fixed-rate yield at maturity.
 *    Formally: 1 syLST[seriesId] redeems for `claimPerToken(seriesId)` wstETH
 *    once the series is settled.
 *
 * Only the vault (VAULT_ROLE) can mint or burn tokens, preventing counterfeit claims.
 */
interface ISyLST is IERC1155 {
    // ─── Events ────────────────────────────────────────────────────────────────

    /**
     * @notice Emitted when the vault registers metadata for a new series tokenId.
     * @param tokenId           ERC-1155 tokenId (uint256(seriesId)).
     * @param seriesId          Original bytes32 series identifier.
     * @param maturityTimestamp Unix timestamp of series maturity.
     */
    event SeriesRegistered(
        uint256 indexed tokenId,
        bytes32 indexed seriesId,
        uint256 maturityTimestamp
    );

    /**
     * @notice Emitted when the vault finalises the redemption rate for a settled series.
     * @param tokenId        ERC-1155 tokenId.
     * @param claimPerToken  wstETH redeemable per unit of syLST (1e18-scaled).
     */
    event SeriesSettled(uint256 indexed tokenId, uint256 claimPerToken);

    // ─── Structs ───────────────────────────────────────────────────────────────

    /**
     * @notice Per-series metadata stored on the token contract.
     * @param maturityTimestamp Unix timestamp when the series matures.
     * @param claimPerTokenE18  wstETH per syLST at redemption (set at settlement, 0 before).
     * @param settled           True once claimPerToken is finalised.
     */
    struct SeriesMeta {
        uint256 maturityTimestamp;
        uint256 claimPerTokenE18; // set at series settlement
        bool settled;
    }

    // ─── Vault-only Mutations ──────────────────────────────────────────────────

    /**
     * @notice Register metadata for a new series tokenId.
     * @dev Must be called by the vault immediately after governance creates the series.
     *      Reverts if tokenId is already registered.
     * @param tokenId           uint256(seriesId)
     * @param maturityTimestamp Unix maturity timestamp.
     */
    function registerSeries(
        uint256 tokenId,
        uint256 maturityTimestamp
    ) external;

    /**
     * @notice Finalise the redemption rate for a settled series.
     * @dev Called by the vault during settleSeries(). Can only be set once per tokenId.
     * @param tokenId          uint256(seriesId)
     * @param claimPerTokenE18 wstETH redeemable per syLST, 1e18-scaled.
     *                         E.g. if deposit rate = 2.5% for 1 year, claimPerToken = 1.025e18.
     */
    function settleSeries(uint256 tokenId, uint256 claimPerTokenE18) external;

    /**
     * @notice Mint `amount` syLST tokens for `tokenId` to `to`.
     * @dev Only callable by VAULT_ROLE. No cap other than totalSupply per series.
     * @param to       Recipient address.
     * @param tokenId  ERC-1155 tokenId.
     * @param amount   Number of tokens to mint.
     * @param data     Optional calldata forwarded to recipient (ERC-1155 hook).
     */
    function mint(address to, uint256 tokenId, uint256 amount, bytes calldata data) external;

    /**
     * @notice Burn `amount` syLST tokens of `tokenId` from `from`.
     * @dev Only callable by VAULT_ROLE. Used during redemption.
     * @param from     Token holder.
     * @param tokenId  ERC-1155 tokenId.
     * @param amount   Number of tokens to burn.
     */
    function burn(address from, uint256 tokenId, uint256 amount) external;

    // ─── View Functions ────────────────────────────────────────────────────────

    /**
     * @notice Returns metadata for a registered series.
     * @param tokenId uint256(seriesId)
     */
    function seriesMeta(uint256 tokenId) external view returns (SeriesMeta memory);

    /**
     * @notice Returns the total supply of syLST for a given series tokenId.
     * @param tokenId uint256(seriesId)
     */
    function totalSupply(uint256 tokenId) external view returns (uint256);

    /**
     * @notice Convenience helper: whether a series is past its maturity timestamp.
     * @param tokenId uint256(seriesId)
     */
    function isMature(uint256 tokenId) external view returns (bool);

    /**
     * @notice Convenience helper: whether a series has been settled by the vault.
     * @param tokenId uint256(seriesId)
     */
    function isSettled(uint256 tokenId) external view returns (bool);
}
