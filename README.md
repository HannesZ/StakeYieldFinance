# StakeYield Finance

**Fixed-rate yield on staked ETH.**

StakeYield converts Ethereum's variable staking yield into fixed-rate instruments. Deposit ETH (or wstETH), choose a maturity, and receive a guaranteed fixed APR — no counterparty needed. The protocol's reserve fund absorbs the variance between floating and fixed rates, priced with actuarial-grade risk models.

> 🚧 **Testnet only.** Deployed on [Hoodi](https://hoodi.ethpandaops.io/) (Lido's testnet). Not audited. Not production-ready.

---

## How It Works

```
User deposits ETH ──→ ZapRouter wraps to wstETH ──→ Vault locks deposit
                                                      │
                                             mints syLST (ERC-1155)
                                                      │
                        ┌─────────────────────────────┘
                        ▼
              At maturity: redeem syLST for wstETH
              (principal + fixed yield in stETH terms)
```

1. **Deposit** — Lock ETH or wstETH into a quarterly series (e.g. 2026Q4). The vault snapshots the wstETH/stETH rate and computes your fixed-rate claim at maturity.
2. **Hold** — Your syLST token represents a fixed-rate claim. It's an ERC-1155 — transferable, composable, tradeable on secondary markets.
3. **Redeem** — At maturity, burn syLST to receive wstETH worth your principal + accrued fixed yield.

The spread between the floating staking APR and the offered fixed rate flows to the reserve, maintaining protocol solvency. When floating yield exceeds the fixed rate, the surplus grows the reserve. When it falls short, the reserve covers the gap.

## Architecture

| Contract | Purpose |
|---|---|
| **StableYieldVault** | Core vault — deposits, redemptions, yield harvesting |
| **SyLST** | ERC-1155 fixed-rate claim tokens (one token ID per series) |
| **ReserveManager** | Solvency ratio (κ), reserve accounting, emergency backstop |
| **SpreadCalculator** | Dynamic spread `s(κ)` — widens when reserve is thin, tightens when healthy |
| **SYLDToken** | ERC-20 governance token with emergency mint capability |
| **ZapRouter** | ETH → stETH → wstETH → deposit in one transaction |

### Key Design Choices

- **No counterparty.** The reserve fund *is* the counterparty — like an insurance company absorbing variance.
- **Per-deposit rate locking.** Each deposit gets the rate at time of commitment, not a series-wide static rate.
- **Model-derived pricing.** Fixed rates come from Monte Carlo simulation over historical staking APR, not AMM-implied pricing.
- **stETH-denominated claims.** Yields are computed in stETH (≈ ETH), avoiding wstETH wrapping confusion.
- **Simple interest.** No compounding — straightforward to audit and verify.

## Repo Structure

```
├── protocol/           # Solidity contracts, tests, deploy scripts
│   ├── contracts/      # Core contracts + interfaces + mocks
│   ├── test/           # 128 Hardhat tests
│   ├── scripts/        # Deployment & migration scripts
│   └── SPEC.md         # Full technical specification
├── website/            # Next.js frontend (wagmi + viem)
│   ├── app/            # Pages: landing, dashboard, deposit, protocol health
│   ├── components/     # UI components
│   └── hooks/          # React hooks (series data, exchange rates, activity)
├── scripts/            # Rate model pipeline
│   ├── compute_offered_rate.py   # Monte Carlo rate calibration
│   └── update_rate.sh            # Push model rate on-chain
├── data/               # Model outputs (projections, rate parameters)
└── research/           # Analysis, theory, beacon chain data tools
```

## Testnet Deployment (Hoodi)

| Contract | Address |
|---|---|
| StableYieldVault | [`0x18849aDE…470E01`](https://hoodi.etherscan.io/address/0x18849aDE3838DA311bfD79e55F3bB0F4Ee470E01) |
| SyLST (ERC-1155) | [`0xD787DD8D…9C398`](https://hoodi.etherscan.io/address/0xD787DD8Db0a1F40D2CAC9d2da138F26B4159C398) |
| ReserveManager | [`0xADf826DF…0a72`](https://hoodi.etherscan.io/address/0xADf826DF9f5d260FA60202c6520f3ECB530a0a72) |
| SpreadCalculator | [`0x883Af902…F1a`](https://hoodi.etherscan.io/address/0x883Af902FeBEd81fD03F93d1B0aDA6A53e3DeF1a) |
| ZapRouter | [`0x634c5EA8…D313`](https://hoodi.etherscan.io/address/0x634c5EA80f4280c170234ab42C2506514A72D313) |
| SYLDToken | [`0xec9a36E5…3D`](https://hoodi.etherscan.io/address/0xec9a36E563aE03D78851d5A44DE44D45e137413D) |

Uses real Lido wstETH on Hoodi: [`0x7E99eE3C…De4`](https://hoodi.etherscan.io/address/0x7E99eE3C66636DE415D2d7C880938F2f40f94De4)

Active series: **2026Q4** (matures Dec 31, 2026)

## Getting Started

### Prerequisites

- Node.js ≥ 18
- Python ≥ 3.10 (for rate model scripts)

### Protocol (contracts)

```bash
cd protocol
npm install
npx hardhat compile
npx hardhat test          # 128 tests
```

### Website (frontend)

```bash
cd website
npm install
npm run dev               # http://localhost:3000
```

### Deploy to Hoodi

```bash
cd protocol
cp .env.example .env      # add DEPLOYER_KEY, HOODI_RPC_URL
npx hardhat run scripts/deploy-hoodi.ts --network hoodi
npx hardhat run scripts/deploy-zap-router.ts --network hoodi
```

### Update the offered rate

```bash
# 1. Run Monte Carlo calibration
python3 scripts/compute_offered_rate.py

# 2. Push to chain
cd protocol
npx hardhat run scripts/push-model-rate.ts --network hoodi
```

## Rate Model

The offered fixed rate is derived from:

1. **Historical data** — Daily staking APR from the Beacon Chain (backfilled via CL API).
2. **Monte Carlo simulation** — 2,000 paths projected over the series tenor using calibrated drift + volatility.
3. **On-chain formula** — `fixedRate = stakingAPR − spread(κ)`, where the spread widens when the reserve is thin.

This produces rates that reflect actual risk, not market sentiment or AMM dynamics.

## Frontend

The dashboard shows all values in **ETH-equivalent** to avoid confusion between wstETH/stETH/syLST denominations:

- **You Deposited** / **You'll Receive** / **Your Yield** — all in ≈ ETH
- Token amounts shown as secondary text for on-chain verification
- Live-ticking accrued interest
- Deposit page supports both ETH (via ZapRouter) and wstETH (direct)
- Testnet precision toggle (4 → 10 decimals) for observing live changes

## License

MIT

---

*Built by [Johannes Zühlke](https://github.com/HannesZ).*
