// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {ERC1155} from "@openzeppelin/contracts/token/ERC1155/ERC1155.sol";
import {ERC1155Supply} from "@openzeppelin/contracts/token/ERC1155/extensions/ERC1155Supply.sol";
import {AccessControl} from "@openzeppelin/contracts/access/AccessControl.sol";
import {ISyLST} from "./interfaces/ISyLST.sol";
import {IERC165} from "@openzeppelin/contracts/utils/introspection/IERC165.sol";

/**
 * @title SyLST
 * @author StakeYield Finance
 * @notice ERC-1155 token representing fixed-rate claims against the StableYield vault.
 *         Each tokenId corresponds to a distinct quarterly maturity series.
 *
 * @dev ERC-1155 Design Rationale
 * ──────────────────────────────
 * One SyLST contract houses all maturity series. Each series is a distinct tokenId
 * (uint256(seriesId)) with its own supply, metadata, and redemption rate.
 *
 * Token Semantics:
 *   - Minting: only the vault (VAULT_ROLE) can mint, at deposit time.
 *   - Burning: only the vault can burn, at redemption time.
 *   - Transfers: unrestricted — syLST is fully transferable on secondary markets.
 *   - Redemption: after maturity, 1 syLST redeems for `claimPerTokenE18/1e18` wstETH.
 *
 * On-chain metadata:
 *   - Each tokenId maps to a SeriesMeta struct with maturity, fixed rate, and
 *     the post-settlement claimPerToken rate.
 *   - The ERC-1155 `uri()` returns a base URL; off-chain metadata resolves per tokenId.
 *
 * Settlement:
 *   When a series matures and the vault calls settleSeries(), the claimPerTokenE18 is
 *   locked immutably. Subsequent transfers of syLST for settled series carry an
 *   implicit wstETH value that any DEX or marketplace can read from this contract.
 */
contract SyLST is ISyLST, ERC1155, ERC1155Supply, AccessControl {
    // ─── Roles ─────────────────────────────────────────────────────────────────

    /// @notice Only the vault may mint, burn, register, and settle series.
    bytes32 public constant VAULT_ROLE = keccak256("VAULT_ROLE");

    // ─── State ──────────────────────────────────────────────────────────────────

    /// @notice Series metadata indexed by tokenId (= uint256(seriesId)).
    mapping(uint256 => SeriesMeta) private _seriesMeta;

    /// @notice Set of registered tokenIds for enumeration.
    uint256[] private _registeredTokenIds;

    // ─── Constructor ────────────────────────────────────────────────────────────

    /**
     * @param admin   Address granted DEFAULT_ADMIN_ROLE (can grant VAULT_ROLE).
     * @param baseUri Base URI for ERC-1155 metadata (e.g. "https://api.stakeyield.finance/syLST/{id}.json").
     */
    constructor(address admin, string memory baseUri) ERC1155(baseUri) {
        _grantRole(DEFAULT_ADMIN_ROLE, admin);
    }

    // ─── ISyLST: Vault-only Mutations ─────────────────────────────────────────

    /**
     * @inheritdoc ISyLST
     * @dev Reverts if tokenId is already registered to prevent accidental overwrite.
     *      fixedRateE18 is no longer stored here; rates are tracked per-deposit in the vault.
     */
    function registerSeries(
        uint256 tokenId,
        uint256 maturityTimestamp
    ) external override onlyRole(VAULT_ROLE) {
        require(
            _seriesMeta[tokenId].maturityTimestamp == 0,
            "SyLST: series already registered"
        );
        require(maturityTimestamp > block.timestamp, "SyLST: maturity in the past");

        _seriesMeta[tokenId] = SeriesMeta({
            maturityTimestamp: maturityTimestamp,
            claimPerTokenE18: 0,
            settled: false
        });
        _registeredTokenIds.push(tokenId);

        emit SeriesRegistered(tokenId, bytes32(tokenId), maturityTimestamp);
    }

    /**
     * @inheritdoc ISyLST
     * @dev claimPerTokenE18 is immutable once set. Reverts on double-settlement.
     *      The vault is responsible for ensuring claimPerTokenE18 correctly reflects
     *      the settled fixed-rate obligation after reserve top-up or haircut.
     */
    function settleSeries(uint256 tokenId, uint256 claimPerTokenE18)
        external
        override
        onlyRole(VAULT_ROLE)
    {
        SeriesMeta storage meta = _seriesMeta[tokenId];
        require(meta.maturityTimestamp > 0, "SyLST: series not registered");
        require(!meta.settled, "SyLST: already settled");
        require(block.timestamp >= meta.maturityTimestamp, "SyLST: not yet mature");
        require(claimPerTokenE18 > 0, "SyLST: zero claim rate");

        meta.claimPerTokenE18 = claimPerTokenE18;
        meta.settled = true;

        emit SeriesSettled(tokenId, claimPerTokenE18);
    }

    /**
     * @inheritdoc ISyLST
     */
    function mint(
        address to,
        uint256 tokenId,
        uint256 amount,
        bytes calldata data
    ) external override onlyRole(VAULT_ROLE) {
        require(_seriesMeta[tokenId].maturityTimestamp > 0, "SyLST: unregistered series");
        require(!_seriesMeta[tokenId].settled, "SyLST: series already settled");
        require(to != address(0), "SyLST: mint to zero address");
        require(amount > 0, "SyLST: zero mint");

        _mint(to, tokenId, amount, data);
    }

    /**
     * @inheritdoc ISyLST
     */
    function burn(address from, uint256 tokenId, uint256 amount)
        external
        override
        onlyRole(VAULT_ROLE)
    {
        require(amount > 0, "SyLST: zero burn");
        _burn(from, tokenId, amount);
    }

    // ─── ISyLST: View Functions ───────────────────────────────────────────────

    /**
     * @inheritdoc ISyLST
     */
    function seriesMeta(uint256 tokenId)
        external
        view
        override
        returns (SeriesMeta memory)
    {
        return _seriesMeta[tokenId];
    }

    /**
     * @inheritdoc ISyLST
     * @dev Delegates to ERC1155Supply.totalSupply(tokenId).
     */
    function totalSupply(uint256 tokenId)
        public
        view
        override(ERC1155Supply, ISyLST)
        returns (uint256)
    {
        return super.totalSupply(tokenId);
    }

    /**
     * @inheritdoc ISyLST
     */
    function isMature(uint256 tokenId) external view override returns (bool) {
        return block.timestamp >= _seriesMeta[tokenId].maturityTimestamp
            && _seriesMeta[tokenId].maturityTimestamp > 0;
    }

    /**
     * @inheritdoc ISyLST
     */
    function isSettled(uint256 tokenId) external view override returns (bool) {
        return _seriesMeta[tokenId].settled;
    }

    /**
     * @notice Returns all registered tokenIds (series).
     */
    function allTokenIds() external view returns (uint256[] memory) {
        return _registeredTokenIds;
    }

    // ─── ERC-165 ───────────────────────────────────────────────────────────────

    /**
     * @dev Required override for multiple inheritance (ERC1155 + AccessControl).
     */
    function supportsInterface(bytes4 interfaceId)
        public
        view
        override(ERC1155, AccessControl, IERC165)
        returns (bool)
    {
        return super.supportsInterface(interfaceId);
    }

    // ─── Internal Overrides ────────────────────────────────────────────────────

    /**
     * @dev ERC1155Supply hook override required by Solidity for multiple inheritance.
     */
    function _update(
        address from,
        address to,
        uint256[] memory ids,
        uint256[] memory values
    ) internal override(ERC1155, ERC1155Supply) {
        super._update(from, to, ids, values);
    }
}
