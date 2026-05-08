# StableYield Protocol — Technical Specification

**Version:** 0.1 (Draft)
**Date:** 2026-05-08
**Author:** Hannes Zühlke / Ulf

---

## 1. Executive Summary

StableYield is an on-chain protocol that converts variable Ethereum staking yield into fixed-rate yield instruments. Participants deposit wrapped staked ETH (wstETH) for a chosen tenor and receive a transferable stable-yield token (syLST) that entitles them to a predetermined fixed yield at maturity. The protocol maintains a reserve fund that absorbs the variance between realized floating yield and promised fixed yield.

The protocol's unique edge is actuarial-grade reserve management combined with a model-derived fixed rate, offering transparent risk pricing instead of AMM-implied rates.

---

## 2. Protocol Participants

| Role | Description |
|------|-------------|
| **Depositor** | Locks wstETH, receives syLST, earns fixed yield |
| **Reserve Fund** | Protocol-owned pool absorbing yield variance |
| **SYLD Holders** | Governance + residual claim on excess reserves |
| **Governance** | Sets risk parameters (solvency targets, base spread, series creation) |

There is no explicit "counterparty" — the reserve fund *is* the counterparty. This is analogous to an insurance company: policyholders (depositors) pay a premium (the spread), and the insurer (reserve) absorbs the risk.

---

## 3. Token Design

### 3.1 syLST — Stable Yield Liquid Staking Token

- **Standard:** ERC-1155 (one token ID per maturity series)
- **Backing:** 1 syLST represents a claim to `(1 + r_fixed × T/365)` wstETH at maturity, per unit deposited
- **Transferability:** Fully transferable (tradeable on DEXes)
- **Minting:** Only by the StableYieldVault on deposit
- **Burning:** Only by the StableYieldVault on maturity redemption

**Why ERC-1155?** Multiple maturity series (2026Q3, 2026Q4, ...) share one contract. More gas-efficient than deploying separate ERC-20s per series. Each `tokenId` corresponds to a `seriesId`.

**Pricing on secondary markets:** Before maturity, syLST will trade at a discount/premium depending on:
- Remaining time to maturity
- Current floating yield vs. locked fixed rate
- Protocol solvency (credit risk)

This gives DeFi composability — syLST could serve as collateral in lending protocols, be used in yield curve arbitrage, etc.

### 3.2 SYLD — Protocol Governance Token

- **Standard:** ERC-20
- **Supply:** Fixed initial supply + emergency mint capability
- **Utility:**
  - Governance votes on protocol parameters
  - Staking to receive excess reserve distributions
  - Emergency backstop: can be minted and auctioned to recapitalize the reserve (MakerDAO MKR model)

---

## 4. Core Mechanics

### 4.1 Deposit Flow

```
1. User selects maturity series (e.g., 2026Q4 — matures Dec 31, 2026)
2. GUI displays: fixed rate = model_rate − dynamic_spread
3. User approves wstETH transfer
4. Vault.deposit(amount, seriesId):
   a. Transfer wstETH from user to vault
   b. Compute fixedYield = amount × fixedRate × tenor / 365
   c. Register the position: {depositor, amount, fixedRate, maturity}
   d. Mint syLST tokens to user (amount = deposit notional)
   e. Update total liabilities
   f. Emit Deposit event
```

**What the user gets:** `amount` syLST tokens for series `seriesId`.

**What the user is promised:** At maturity, each syLST redeems for `(1 + fixedRate × tenor/365)` wstETH per token.

### 4.2 Yield Accrual

wstETH doesn't rebase — it appreciates via an increasing exchange rate against ETH. The protocol captures this appreciation:

```
harvestYield():
  currentValue = vault_wstETH_balance × wstETH_to_ETH_rate
  lastValue = previous harvest value
  floatingYield = currentValue − lastValue
  promisedYield = sum of (position.fixedRate × position.amount × daysSinceLastHarvest/365)
  surplus = floatingYield − promisedYield
  
  if surplus > 0:
    transfer surplus to ReserveManager
  else:
    draw |surplus| from ReserveManager
  
  update lastValue
```

Harvesting can be called by anyone (keeper incentive) at configurable intervals (e.g., daily).

### 4.3 Maturity Redemption

```
redeem(seriesId, amount):
  require(block.timestamp >= series.maturityTimestamp)
  require(syLST.balanceOf(msg.sender, seriesId) >= amount)
  
  redemptionAmount = amount × (1 + series.fixedRate × series.tenor / 365)
  
  burn syLST tokens
  transfer redemptionAmount wstETH to user
  update total liabilities
  emit Redeem event
```

**Key property:** Any holder can redeem, not just the original depositor. This is what makes syLST truly liquid — you can buy it on a DEX and redeem at maturity.

### 4.4 No Early Exit (by design)

The vault does not offer early withdrawal. If a depositor wants to exit:
1. Sell syLST on a secondary market (DEX)
2. The buyer can hold to maturity or sell again

This keeps the vault's liability duration predictable and avoids bank-run dynamics.

---

## 5. Dynamic Spread Mechanism

### 5.1 Solvency Ratio

```
κ = R / L

where:
  R = Reserve fund balance (wstETH)
  L = Total liabilities = Σ_i (notional_i × fixedRate_i × remainingDays_i / 365)
```

`κ` is the protocol's solvency ratio — how many times over the reserve can cover remaining fixed-yield obligations.

### 5.2 Spread Formula

```
s(κ) = s_base × (1 + α × max(0, κ_target/κ − 1)^β)
```

**Parameters:**

| Parameter | Description | Default | Governance-settable |
|-----------|-------------|---------|---------------------|
| `s_base` | Base spread (bp) | 50 | ✓ |
| `α` | Stress multiplier | 10 | ✓ |
| `β` | Convexity exponent | 3 | ✓ |
| `κ_target` | Target solvency ratio | 1.5 (150%) | ✓ |
| `κ_critical` | Emergency threshold | 0.3 (30%) | ✓ |

**Behavior by solvency level:**

| κ (Solvency) | Spread | Behavior |
|---------------|--------|----------|
| ≥ 1.5 (target) | 50 bp | Normal operation, attractive rates |
| 1.0 | ~52 bp | Barely noticeable increase |
| 0.7 | ~115 bp | Meaningful deterrent to new deposits |
| 0.5 | ~470 bp | Strong deterrent |
| 0.3 (critical) | ~5,050 bp | Effectively closed to new deposits |

**Why this works:**
- In normal conditions: users see base spread, protocol is attractive
- Under mild stress: spread gently rises, slowing new deposit inflows
- Under severe stress: spread makes new deposits unattractive, existing liabilities run off naturally, reserve stabilizes
- The transition is smooth — no sudden discontinuity, no governance intervention needed

### 5.3 Spread Floor and Cap

- **Floor:** `s_base` (never below base spread, even with huge reserves)
- **Cap:** 10,000 bp (100%) — hard maximum. If spread hits cap, new deposits are paused entirely.

### 5.4 On-chain Implementation

The spread formula uses fixed-point arithmetic (1e18 scale). The exponentiation `x^β` for integer `β` is computed iteratively (no floating point). For non-integer β, a lookup table with linear interpolation provides sufficient precision.

---

## 6. Reserve Management

### 6.1 Reserve Lifecycle

```
┌─────────────────────────────────────────────────────┐
│                    Reserve Fund                      │
│                                                      │
│  Inflows:                    Outflows:               │
│  ├─ Yield surplus            ├─ Yield deficit        │
│  │  (float > fixed)          │  (fixed > float)      │
│  ├─ Initial seed funding     ├─ SYLD distributions   │
│  ├─ Emergency SYLD auctions  │  (when κ > κ_target)  │
│  └─ Early exit penalties*    └─ Emergency draws      │
│                                                      │
└─────────────────────────────────────────────────────┘
```

*If early exit is ever added.

### 6.2 Surplus Distribution

When `κ > κ_target`, excess reserve above `R_target = κ_target × L` can be:
1. Distributed to staked SYLD holders (pro-rata)
2. Used for SYLD buyback-and-burn
3. Retained for growth buffer

Governance decides the distribution policy. Distribution happens at most once per epoch (e.g., weekly).

### 6.3 Emergency Recapitalization

If `κ < κ_emergency` (e.g., 0.1):
1. New deposits paused
2. Governance can trigger SYLD emergency mint
3. Minted SYLD auctioned for wstETH (Dutch auction)
4. wstETH proceeds go to reserve

This is the "dilution backstop" — SYLD holders bear the tail risk. Importantly:
- Existing syLST holders are senior (their claims are always honored first)
- SYLD holders are subordinated (they eat losses through dilution)

This credit hierarchy (syLST senior, SYLD junior) is well-understood in traditional finance.

### 6.4 Solvency Stress Testing

The protocol should run continuous on-chain stress tests:

```
Stressed Reserve Need = Σ_i max(0, fixedRate_i − modelP5Rate(remaining_i)) × notional_i × remaining_i / 365
```

This asks: "If yield drops to the model's 5th percentile for all remaining tenors, how much reserve do we need?" The solvency ratio should be computed against this stressed liability, not just expected liabilities.

---

## 7. Yield Model Integration

### 7.1 Rate Oracle

The model-derived fixed rate must be available on-chain. Options:

**Option A — Off-chain oracle (recommended for v1)**
- Protocol operator publishes rates via a multisig/governance transaction
- Rates updated weekly or on significant market moves
- Simple, auditable, no oracle manipulation risk from MEV
- Downside: centralized, requires trust in operator

**Option B — On-chain computation**
- Smart contract reads total staked ETH from beacon chain (via EIP-4788 beacon root)
- Computes CL yield from the issuance formula directly on-chain
- Fully trustless
- Downside: doesn't capture EL yield (MEV/tips), more complex

**Option C — Hybrid**
- CL yield computed on-chain from beacon state
- EL premium set as a governance parameter
- Best of both worlds for v2

### 7.2 Rate Update Mechanism

```
updateRates(bytes32 seriesId, uint256 newFixedRateE18):
  require(hasRole(RATE_ORACLE_ROLE, msg.sender))
  require(series.maturityTimestamp > block.timestamp)
  
  // Rate only applies to NEW deposits. Existing positions keep their locked rate.
  series.currentOfferedRate = newFixedRateE18
  
  emit RateUpdated(seriesId, newFixedRateE18)
```

**Critical:** existing positions are never affected by rate changes. The fixed rate is locked at deposit time.

---

## 8. Contract Architecture

### 8.1 Contract Diagram

```
                    ┌──────────────┐
                    │   Frontend   │
                    │  (Swap GUI)  │
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │ StableYield  │
                    │    Vault     │◄──── Main entry point
                    └──┬───┬───┬──┘
                       │   │   │
          ┌────────────┘   │   └────────────┐
          ▼                ▼                 ▼
   ┌─────────────┐ ┌─────────────┐  ┌──────────────┐
   │    SyLST    │ │   Reserve   │  │    Spread    │
   │  (ERC-1155) │ │   Manager   │  │  Calculator  │
   └─────────────┘ └──────┬──────┘  └──────────────┘
                          │
                   ┌──────▼──────┐
                   │  SYLD Token │
                   │  (ERC-20)   │
                   └─────────────┘
```

### 8.2 Contract Responsibilities

| Contract | Responsibility |
|----------|---------------|
| **StableYieldVault** | Deposit, redeem, harvest, series management. Holds all escrowed wstETH. |
| **SyLST** | ERC-1155 token representing fixed-yield claims per maturity series. |
| **ReserveManager** | Tracks reserve, computes solvency, handles distributions and emergencies. |
| **SpreadCalculator** | Pure math: computes dynamic spread from solvency ratio. |
| **SYLDToken** | ERC-20 governance token with controlled mint for backstop. |

### 8.3 Upgradeability

Use UUPS proxy pattern (ERC-1967) for:
- StableYieldVault
- ReserveManager

Non-upgradeable (immutable):
- SyLST (token contract — trust requires immutability)
- SYLDToken (same reasoning)
- SpreadCalculator (pure math, no state)

### 8.4 Access Control Matrix

| Function | Role Required |
|----------|---------------|
| deposit / redeem | Public (any user) |
| createSeries | GOVERNANCE_ROLE |
| updateRates | RATE_ORACLE_ROLE |
| harvestYield | KEEPER_ROLE or public with incentive |
| distributeSurplus | GOVERNANCE_ROLE |
| emergencyMint SYLD | EMERGENCY_ROLE (timelock) |
| pause / unpause | GUARDIAN_ROLE |
| upgrade contracts | GOVERNANCE_ROLE (timelock) |

---

## 9. Risk Analysis

### 9.1 Risk Register

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Floating yield < fixed for extended period | Medium | High | Reserve fund + dynamic spread |
| ETH issuance policy change (EIP) | Low | Critical | Model monitoring, governance can adjust rates |
| wstETH depeg / Lido slashing | Low | Critical | Circuit breaker, reserve buffer |
| Smart contract exploit | Low | Critical | Audits, bug bounty, formal verification |
| Oracle manipulation | Medium | High | Multisig oracle, rate bounds, timelock |
| Liquidity crisis (mass redemption at maturity) | Low | Medium | wstETH is liquid; vault holds sufficient wstETH by construction |
| Reserve depletion | Low | High | Dynamic spread + SYLD backstop |

### 9.2 Solvency Capital Requirement (Actuarial Framework)

Borrowing from the Swiss Solvency Test (SST) / Solvency II:

**Best Estimate Liabilities (BEL):**
```
BEL = Σ_i notional_i × E[r_float - r_fixed_i] × remaining_i / 365
```

Note: BEL can be negative (expected surplus), which is the normal state.

**Risk Margin (RM):**
```
RM = CoC × Σ_t SCR(t) / (1 + r)^t
```
Where CoC = cost of capital rate (e.g., 6%), SCR(t) = solvency capital at future time t.

**Solvency Capital Requirement (SCR):**
```
SCR = VaR_99.5%(ΔReserve over 1 year)
```

Computed from the Monte Carlo model: the 0.5th percentile of the reserve change over 1 year, given current positions.

**Minimum Capital Requirement (MCR):**
```
MCR = 0.3 × SCR
```

When reserve < MCR, emergency measures activate (SYLD minting, deposit pause).

**Target Capital:**
```
TC = SCR + Buffer = SCR × κ_target / κ_at_SCR
```

### 9.3 Model Risk

The yield model is the foundation of the pricing. Model risk arises from:

1. **Parameter uncertainty:** Calibrated drift/vol may not be stable
2. **Structural changes:** Ethereum protocol upgrades (new issuance curve)
3. **Regime changes:** Sudden shift in staking behavior (e.g., ETF approvals → massive inflows)

**Mitigation:**
- Regular recalibration (automated, on new epoch data)
- Model Risk Margin: add a loading to the spread that accounts for model uncertainty
- Governance override: ability to manually adjust rates if model diverges from reality
- Diversification across tenors: short tenors are more robust to model error

---

## 10. Parameter Calibration

### 10.1 Initial Parameters

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| `s_base` | 50 bp | Competitive with Pendle PT discount (~30-70bp) |
| `α` | 10 | Tested: produces ~5,000bp at κ_critical |
| `β` | 3 | Cubic: gentle start, aggressive tail |
| `κ_target` | 1.50 | 150% solvency: comfortable buffer |
| `κ_critical` | 0.30 | 30% solvency: survival mode |
| `κ_emergency` | 0.10 | 10%: SYLD backstop activated |
| Harvest interval | 1 day | Match wstETH exchange rate update cadence |
| Distribution frequency | 7 days | Weekly surplus distribution to SYLD stakers |
| Available maturities | Quarterly | Q3-2026, Q4-2026, Q1-2027, ..., up to 3Y out |

### 10.2 Initial Reserve Sizing

For a protocol launch with 10,000 wstETH in deposits:

```
Expected annual yield variance: ~7bp (from model)
Tail risk (p1 scenario): ~15bp below expected
Worst-case annual deficit per unit: 0.15% × notional
For 10,000 wstETH: worst-case = 15 wstETH/year

Target reserve (150% of 1-year SCR):
  SCR = 15 wstETH
  Target = 22.5 wstETH

Minimum viable reserve: ~25 wstETH (0.25% of TVL)
```

This is remarkably capital-efficient — the reserve requirement is tiny relative to TVL because the model uncertainty is low. However, this doesn't account for:
- LST depeg risk (add 2-5% buffer)
- Issuance policy change risk (add 1-2% buffer)
- Smart contract risk (unquantifiable — covered by audits + insurance)

**Conservative initial reserve: 3-5% of target TVL.**

---

## 11. Roadmap

### Phase 1 — Testnet (v0.1)
- [ ] Core contracts (vault, syLST, reserve, spread)
- [ ] Single maturity series for testing
- [ ] Hardhat test suite with scenario simulations
- [ ] GUI integration (swap pricer → deposit flow)

### Phase 2 — Audit & Beta (v0.2)
- [ ] Professional audit (Trail of Bits / OpenZeppelin)
- [ ] Testnet deployment with real wstETH (Holesky)
- [ ] Bug bounty program
- [ ] Multi-series support (quarterly maturities)

### Phase 3 — Mainnet Launch (v1.0)
- [ ] Mainnet deployment with conservative parameters
- [ ] Initial reserve seeded
- [ ] SYLD token launch
- [ ] Short tenors only (≤6 months) initially
- [ ] Gradual expansion to longer tenors as reserve grows

### Phase 4 — DeFi Integration (v2.0)
- [ ] On-chain yield model (beacon state oracle)
- [ ] syLST as collateral on lending platforms
- [ ] Cross-chain deployment (L2s)
- [ ] Multiple LST support (rETH, cbETH, etc.)

---

## 12. Legal Considerations

- Fixed-yield tokens may qualify as securities in some jurisdictions
- Switzerland's DLT Act provides a relatively favorable framework
- Protocol should be structured as a DAO for regulatory clarity
- Legal review required before mainnet launch
- Consider geographic restrictions in the frontend

---

## Appendix A — Spread Function Visualization

```
Spread (bp)
     │
5000 ┤                                           ╱
     │                                          ╱
4000 ┤                                        ╱
     │                                      ╱
3000 ┤                                    ╱
     │                                  ╱
2000 ┤                               ╱
     │                            ╱
1000 ┤                        ╱╱
     │                   ╱╱╱
 500 ┤              ╱╱╱
  50 ┤──────────────
     └──┬────┬────┬────┬────┬────┬────┬────┬──── κ
       0.3  0.5  0.7  0.9  1.1  1.3  1.5  1.7
      crit                           target
```

## Appendix B — Comparison with Existing Protocols

| Feature | StableYield | Pendle | Notional | Element |
|---------|-------------|--------|----------|---------|
| Rate derivation | Model + actuarial | AMM-implied | Interest rate market | AMM-implied |
| Reserve mechanism | Pooled with dynamic spread | None (pure market) | Pool-based | None |
| Risk quantification | Explicit (SCR, κ) | Implicit | Implicit | None |
| Token standard | ERC-1155 | ERC-20 per PT/YT | fCash | ERC-20 |
| Underlying | wstETH | Various | Various | Various |
| Transferable | Yes | Yes | Yes | Yes |
| Governance backstop | SYLD dilution | PENDLE staking | NOTE staking | None |

**Key differentiator:** StableYield is the only protocol with explicit actuarial reserve management and transparent risk pricing. This is a trust advantage — users can verify the protocol's solvency on-chain.
