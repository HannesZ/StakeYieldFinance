# StakeYield Finance — Contract Architecture

## Overview

Fixed-rate wstETH vault protocol using quarterly maturity series and dynamic solvency-based spreads.

## Contract Map

```
contracts/
├── interfaces/
│   ├── IStableYieldVault.sol   — Main vault interface
│   ├── ISyLST.sol              — ERC-1155 syLST token interface
│   ├── IReserveManager.sol     — Reserve & solvency tracking interface
│   └── ISpreadCalculator.sol   — Dynamic spread formula interface
├── StableYieldVault.sol        — Core vault (deposit, harvest, settle, redeem)
├── SyLST.sol                   — ERC-1155 fixed-rate claim token (multi-series)
├── ReserveManager.sol          — Solvency backstop & surplus distribution
├── SpreadCalculator.sol        — s(κ) = s_base · (1 + α · max(0, κ_t/κ − 1)^β)
└── SYLDToken.sol               — Governance token (ERC-20Votes, emergency mint)
```

## Protocol Flow

```
User
 │
 ├─ deposit(seriesId, X wstETH)
 │     └─ Vault mints X syLST[seriesId]
 │     └─ Vault escrows X wstETH
 │     └─ Spread portion → ReserveManager
 │     └─ Liability registered: X · (1 + r_fixed · T)
 │
 ├─ [hold syLST, trade on secondary markets]
 │
 └─ redeem(seriesId, X syLST)  [after maturity + settlement]
       └─ Burns X syLST
       └─ Returns X · claimPerToken wstETH

Keeper
 ├─ harvestYield()              [periodic, e.g. daily]
 │     └─ float > fixed → surplus wstETH → ReserveManager
 │     └─ float < fixed → deficit covered by ReserveManager
 │
 └─ settleSeries(seriesId)     [after maturity]
       └─ Finalises claimPerToken on syLST contract
       └─ Funds settlement pool from vault + reserve top-up

Governance
 ├─ createSeries(label, maturity, rate)
 ├─ setParameters() on SpreadCalculator
 ├─ setKappaThresholds() on ReserveManager
 └─ triggerEmergencyMint()     [if κ < κ_emergency]
```

## Solvency Ratio (κ)

```
κ = reserve / total_liabilities

κ ≥ κ_target   (e.g. 1.2) → Protocol healthy, spread = s_base
κ_emergency < κ < κ_target → Elevated spread charges new deposits
κ ≤ κ_emergency (e.g. 1.05) → Emergency: SYLD minted & auctioned for wstETH
```

## Spread Formula

```
s(κ) = s_base · (1 + α · max(0, κ_target/κ − 1)^β)

Example (s_base=25bp, α=2.0, β=2.0, κ_target=1.2, κ=0.9):
  excess = 1.2/0.9 − 1 = 0.333
  s = 25 · (1 + 2 · 0.333²) = 25 · 1.222 = 30.6 bp
```

## Key Design Decisions

| Decision | Rationale |
|---|---|
| ERC-1155 for syLST | One contract for all series, cheaper than N ERC-20 deployments |
| wstETH only | Non-rebasing, simpler yield accounting |
| No early exit | Simplifies vault math; sell syLST on AMMs instead |
| Simple interest | Auditable, predictable at short tenors |
| Physical settlement | Surplus/deficit settled in wstETH at harvest for transparent reserve accounting |
| Blended rate for harvest | Gas-efficient approximation, accurate at high harvest frequency |

## Access Control Roles

| Role | Holder | Permissions |
|---|---|---|
| `DEFAULT_ADMIN_ROLE` | Timelock / multi-sig | Grant/revoke all roles |
| `GOVERNANCE_ROLE` | DAO / multi-sig | Create series, set parameters, emergency actions |
| `KEEPER_ROLE` | Automation (Chainlink) | harvestYield, settleSeries, distributeSurplus |
| `VAULT_ROLE` | StableYieldVault | Mint/burn syLST, deposit/withdraw reserve |
| `MINTER_ROLE` | ReserveManager | Mint SYLD for emergency recapitalisation |

## Deployment Order

1. Deploy `SYLDToken` (admin, treasury, initialSupply)
2. Deploy `SyLST` (admin, baseUri)
3. Deploy `ReserveManager` (admin, wstETH, syld, staking, κ_target, κ_emergency)
4. Deploy `SpreadCalculator` (admin, reserveManager, initialParams)
5. Deploy `StableYieldVault` (admin, wstETH, syLST, reserveManager, spreadCalculator)
6. Grant `VAULT_ROLE` on `SyLST` → `StableYieldVault`
7. Grant `VAULT_ROLE` on `ReserveManager` → `StableYieldVault`
8. Grant `KEEPER_ROLE` on `StableYieldVault` → keeper automation address
9. Grant `KEEPER_ROLE` on `ReserveManager` → keeper automation address
10. Grant `MINTER_ROLE` on `SYLDToken` → `ReserveManager`
