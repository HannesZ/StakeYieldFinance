/**
 * StakeYield Finance — Contract ABIs & Addresses
 *
 * ABIs extracted from protocol interface definitions.
 * Addresses: Hoodi testnet deployed, mainnet placeholders.
 */

// ─── Addresses ──────────────────────────────────────────────────────────────

export const ADDRESSES = {
  hoodi: {
    stableYieldVault: "0x18849aDE3838DA311bfD79e55F3bB0F4Ee470E01" as `0x${string}`,
    syLST: "0xD787DD8Db0a1F40D2CAC9d2da138F26B4159C398" as `0x${string}`,
    reserveManager: "0xADf826DF9f5d260FA60202c6520f3ECB530a0a72" as `0x${string}`,
    spreadCalculator: "0x883Af902FeBEd81fD03F93d1B0aDA6A53e3DeF1a" as `0x${string}`,
    syldToken: "0xec9a36E563aE03D78851d5A44DE44D45e137413D" as `0x${string}`,
    wstETH: "0x7E99eE3C66636DE415D2d7C880938F2f40f94De4" as `0x${string}`,
    zapRouter: "0x634c5EA80f4280c170234ab42C2506514A72D313" as `0x${string}`,
  },
  mainnet: {
    stableYieldVault: "0x0000000000000000000000000000000000000000" as `0x${string}`,
    syLST: "0x0000000000000000000000000000000000000000" as `0x${string}`,
    reserveManager: "0x0000000000000000000000000000000000000000" as `0x${string}`,
    spreadCalculator: "0x0000000000000000000000000000000000000000" as `0x${string}`,
    wstETH: "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0" as `0x${string}`,
  },
} as const;

// ─── StableYieldVault ABI ───────────────────────────────────────────────────

export const STABLE_YIELD_VAULT_ABI = [
  // Events
  {
    type: "event",
    name: "SeriesCreated",
    inputs: [
      { name: "seriesId", type: "bytes32", indexed: true },
      { name: "maturity", type: "uint256", indexed: false },
      { name: "fixedRateE18", type: "uint256", indexed: false },
    ],
  },
  {
    type: "event",
    name: "Deposited",
    inputs: [
      { name: "seriesId", type: "bytes32", indexed: true },
      { name: "depositor", type: "address", indexed: true },
      { name: "wstEthAmount", type: "uint256", indexed: false },
      { name: "syLstMinted", type: "uint256", indexed: false },
      { name: "claimAtMaturityStEth", type: "uint256", indexed: false },
    ],
  },
  {
    type: "event",
    name: "Redeemed",
    inputs: [
      { name: "seriesId", type: "bytes32", indexed: true },
      { name: "redeemer", type: "address", indexed: true },
      { name: "syLstBurned", type: "uint256", indexed: false },
      { name: "wstEthReturned", type: "uint256", indexed: false },
    ],
  },
  {
    type: "event",
    name: "YieldHarvested",
    inputs: [
      { name: "wstEthGain", type: "uint256", indexed: false },
      { name: "toReserve", type: "uint256", indexed: false },
    ],
  },
  // Read functions
  {
    type: "function",
    name: "getSeries",
    stateMutability: "view",
    inputs: [{ name: "seriesId", type: "bytes32" }],
    outputs: [
      {
        name: "",
        type: "tuple",
        components: [
          { name: "maturity", type: "uint256" },
          { name: "totalDeposited", type: "uint256" },
          { name: "totalClaimsStEth", type: "uint256" },
          { name: "totalSyLst", type: "uint256" },
          { name: "weightedRateSum", type: "uint256" },
          { name: "totalStEthDeposited", type: "uint256" },
          { name: "isOpen", type: "bool" },
          { name: "isSettled", type: "bool" },
        ],
      },
    ],
  },
  {
    type: "function",
    name: "previewRedeem",
    stateMutability: "view",
    inputs: [
      { name: "seriesId", type: "bytes32" },
      { name: "syLstAmount", type: "uint256" },
    ],
    outputs: [{ name: "fixedClaim", type: "uint256" }],
  },
  {
    type: "function",
    name: "currentSpreadBps",
    stateMutability: "view",
    inputs: [{ name: "seriesId", type: "bytes32" }],
    outputs: [{ name: "spreadBps", type: "uint256" }],
  },
  {
    type: "function",
    name: "wstETH",
    stateMutability: "view",
    inputs: [],
    outputs: [{ name: "", type: "address" }],
  },
  {
    type: "function",
    name: "syLST",
    stateMutability: "view",
    inputs: [],
    outputs: [{ name: "", type: "address" }],
  },
  {
    type: "function",
    name: "reserveManager",
    stateMutability: "view",
    inputs: [],
    outputs: [{ name: "", type: "address" }],
  },
  {
    type: "function",
    name: "computeFixedRate",
    stateMutability: "view",
    inputs: [],
    outputs: [{ name: "fixedRateE18", type: "uint256" }],
  },
  {
    type: "function",
    name: "stakingAPR",
    stateMutability: "view",
    inputs: [],
    outputs: [{ name: "", type: "uint256" }],
  },
  {
    type: "function",
    name: "getDeposits",
    stateMutability: "view",
    inputs: [
      { name: "seriesId", type: "bytes32" },
      { name: "user", type: "address" },
    ],
    outputs: [{
      name: "",
      type: "tuple[]",
      components: [
        { name: "wstEthAmount", type: "uint256" },
        { name: "stEthValue", type: "uint256" },
        { name: "fixedRateE18", type: "uint256" },
        { name: "depositTimestamp", type: "uint256" },
        { name: "claimAtMaturityStEth", type: "uint256" },
      ],
    }],
  },
  {
    type: "function",
    name: "getUserClaim",
    stateMutability: "view",
    inputs: [
      { name: "seriesId", type: "bytes32" },
      { name: "user", type: "address" },
    ],
    outputs: [{ name: "totalClaimStEth", type: "uint256" }],
  },
  // Write functions
  {
    type: "function",
    name: "deposit",
    stateMutability: "nonpayable",
    inputs: [
      { name: "seriesId", type: "bytes32" },
      { name: "wstEthAmount", type: "uint256" },
    ],
    outputs: [{ name: "syLstMinted", type: "uint256" }],
  },
  {
    type: "function",
    name: "redeem",
    stateMutability: "nonpayable",
    inputs: [
      { name: "seriesId", type: "bytes32" },
      { name: "syLstAmount", type: "uint256" },
    ],
    outputs: [{ name: "wstEthOut", type: "uint256" }],
  },
  {
    type: "function",
    name: "harvestYield",
    stateMutability: "nonpayable",
    inputs: [],
    outputs: [],
  },
  {
    type: "function",
    name: "totalStEthObligationBase",
    stateMutability: "view",
    inputs: [],
    outputs: [{ name: "", type: "uint256" }],
  },
] as const;

// ─── SyLST (ERC-1155) ABI ──────────────────────────────────────────────────

export const SY_LST_ABI = [
  {
    type: "function",
    name: "balanceOf",
    stateMutability: "view",
    inputs: [
      { name: "account", type: "address" },
      { name: "id", type: "uint256" },
    ],
    outputs: [{ name: "", type: "uint256" }],
  },
  {
    type: "function",
    name: "balanceOfBatch",
    stateMutability: "view",
    inputs: [
      { name: "accounts", type: "address[]" },
      { name: "ids", type: "uint256[]" },
    ],
    outputs: [{ name: "", type: "uint256[]" }],
  },
  {
    type: "function",
    name: "totalSupply",
    stateMutability: "view",
    inputs: [{ name: "tokenId", type: "uint256" }],
    outputs: [{ name: "", type: "uint256" }],
  },
  {
    type: "function",
    name: "seriesMeta",
    stateMutability: "view",
    inputs: [{ name: "tokenId", type: "uint256" }],
    outputs: [
      {
        name: "",
        type: "tuple",
        components: [
          { name: "maturityTimestamp", type: "uint256" },
          { name: "fixedRateE18", type: "uint256" },
          { name: "claimPerTokenE18", type: "uint256" },
          { name: "settled", type: "bool" },
        ],
      },
    ],
  },
  {
    type: "function",
    name: "isMature",
    stateMutability: "view",
    inputs: [{ name: "tokenId", type: "uint256" }],
    outputs: [{ name: "", type: "bool" }],
  },
  {
    type: "function",
    name: "isSettled",
    stateMutability: "view",
    inputs: [{ name: "tokenId", type: "uint256" }],
    outputs: [{ name: "", type: "bool" }],
  },
] as const;

// ─── ReserveManager ABI ─────────────────────────────────────────────────────

export const RESERVE_MANAGER_ABI = [
  {
    type: "function",
    name: "totalReserve",
    stateMutability: "view",
    inputs: [],
    outputs: [{ name: "", type: "uint256" }],
  },
  {
    type: "function",
    name: "totalLiabilities",
    stateMutability: "view",
    inputs: [],
    outputs: [{ name: "", type: "uint256" }],
  },
  {
    type: "function",
    name: "kappa",
    stateMutability: "view",
    inputs: [],
    outputs: [{ name: "", type: "uint256" }],
  },
  {
    type: "function",
    name: "kappaTarget",
    stateMutability: "view",
    inputs: [],
    outputs: [{ name: "", type: "uint256" }],
  },
  {
    type: "function",
    name: "kappaEmergency",
    stateMutability: "view",
    inputs: [],
    outputs: [{ name: "", type: "uint256" }],
  },
  {
    type: "function",
    name: "seriesLiability",
    stateMutability: "view",
    inputs: [{ name: "seriesId", type: "bytes32" }],
    outputs: [{ name: "", type: "uint256" }],
  },
  {
    type: "function",
    name: "isEmergency",
    stateMutability: "view",
    inputs: [],
    outputs: [{ name: "", type: "bool" }],
  },
  // Events
  {
    type: "event",
    name: "ReserveDeposited",
    inputs: [
      { name: "amount", type: "uint256", indexed: false },
      { name: "newReserve", type: "uint256", indexed: false },
      { name: "newKappa", type: "uint256", indexed: false },
    ],
  },
  {
    type: "event",
    name: "ReserveWithdrawn",
    inputs: [
      { name: "amount", type: "uint256", indexed: false },
      { name: "newReserve", type: "uint256", indexed: false },
      { name: "newKappa", type: "uint256", indexed: false },
    ],
  },
] as const;

// ─── SpreadCalculator ABI ───────────────────────────────────────────────────

export const SPREAD_CALCULATOR_ABI = [
  {
    type: "function",
    name: "getSpread",
    stateMutability: "view",
    inputs: [{ name: "kappaE18", type: "uint256" }],
    outputs: [{ name: "spreadBps", type: "uint256" }],
  },
  {
    type: "function",
    name: "currentSpread",
    stateMutability: "view",
    inputs: [],
    outputs: [{ name: "spreadBps", type: "uint256" }],
  },
  {
    type: "function",
    name: "getParameters",
    stateMutability: "view",
    inputs: [],
    outputs: [
      {
        name: "",
        type: "tuple",
        components: [
          { name: "sBaseBps", type: "uint256" },
          { name: "alphaE18", type: "uint256" },
          { name: "betaE18", type: "uint256" },
          { name: "kappaTargetE18", type: "uint256" },
          { name: "kappaCriticalE18", type: "uint256" },
        ],
      },
    ],
  },
] as const;

// ─── wstETH ABI (ERC-20 + stEthPerToken) ────────────────────────────────────

export const WSTETH_ABI = [
  {
    type: "function",
    name: "stEthPerToken",
    stateMutability: "view",
    inputs: [],
    outputs: [{ name: "", type: "uint256" }],
  },
  {
    type: "function",
    name: "approve",
    stateMutability: "nonpayable",
    inputs: [
      { name: "spender", type: "address" },
      { name: "amount", type: "uint256" },
    ],
    outputs: [{ name: "", type: "bool" }],
  },
  {
    type: "function",
    name: "allowance",
    stateMutability: "view",
    inputs: [
      { name: "owner", type: "address" },
      { name: "spender", type: "address" },
    ],
    outputs: [{ name: "", type: "uint256" }],
  },
  {
    type: "function",
    name: "balanceOf",
    stateMutability: "view",
    inputs: [{ name: "account", type: "address" }],
    outputs: [{ name: "", type: "uint256" }],
  },
  {
    type: "function",
    name: "decimals",
    stateMutability: "view",
    inputs: [],
    outputs: [{ name: "", type: "uint8" }],
  },
  {
    type: "function",
    name: "symbol",
    stateMutability: "view",
    inputs: [],
    outputs: [{ name: "", type: "string" }],
  },
] as const;

// ─── ZapRouter ABI ──────────────────────────────────────────────────────────

export const ZAP_ROUTER_ABI = [
  {
    type: "function",
    name: "depositETH",
    stateMutability: "payable",
    inputs: [
      { name: "seriesId", type: "bytes32" },
    ],
    outputs: [{ name: "syLstMinted", type: "uint256" }],
  },
  {
    type: "function",
    name: "stETH",
    stateMutability: "view",
    inputs: [],
    outputs: [{ name: "", type: "address" }],
  },
  {
    type: "function",
    name: "wstETH",
    stateMutability: "view",
    inputs: [],
    outputs: [{ name: "", type: "address" }],
  },
  {
    type: "function",
    name: "vault",
    stateMutability: "view",
    inputs: [],
    outputs: [{ name: "", type: "address" }],
  },
  {
    type: "event",
    name: "ZapDeposit",
    inputs: [
      { name: "seriesId", type: "bytes32", indexed: true },
      { name: "depositor", type: "address", indexed: true },
      { name: "ethAmount", type: "uint256", indexed: false },
      { name: "wstEthDeposited", type: "uint256", indexed: false },
      { name: "syLstMinted", type: "uint256", indexed: false },
    ],
  },
] as const;

// ─── StableYieldVault depositFor ABI (append to main ABI where needed) ──────

export const VAULT_DEPOSIT_FOR_ABI = [
  {
    type: "function",
    name: "depositFor",
    stateMutability: "nonpayable",
    inputs: [
      { name: "seriesId", type: "bytes32" },
      { name: "wstEthAmount", type: "uint256" },
      { name: "beneficiary", type: "address" },
    ],
    outputs: [{ name: "syLstMinted", type: "uint256" }],
  },
] as const;

// ─── ERC-20 (generic) ABI subset ────────────────────────────────────────────

export const ERC20_ABI = [
  {
    type: "function",
    name: "approve",
    stateMutability: "nonpayable",
    inputs: [
      { name: "spender", type: "address" },
      { name: "amount", type: "uint256" },
    ],
    outputs: [{ name: "", type: "bool" }],
  },
  {
    type: "function",
    name: "allowance",
    stateMutability: "view",
    inputs: [
      { name: "owner", type: "address" },
      { name: "spender", type: "address" },
    ],
    outputs: [{ name: "", type: "uint256" }],
  },
  {
    type: "function",
    name: "balanceOf",
    stateMutability: "view",
    inputs: [{ name: "account", type: "address" }],
    outputs: [{ name: "", type: "uint256" }],
  },
  {
    type: "function",
    name: "decimals",
    stateMutability: "view",
    inputs: [],
    outputs: [{ name: "", type: "uint8" }],
  },
  {
    type: "function",
    name: "symbol",
    stateMutability: "view",
    inputs: [],
    outputs: [{ name: "", type: "string" }],
  },
] as const;
