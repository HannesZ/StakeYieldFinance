# StakeYieldFinance Website & Dashboard Plan

## Tech Stack
- **Framework:** Next.js 14+ (App Router)
- **Styling:** Tailwind CSS + shadcn/ui components
- **Web3:** wagmi v2 + viem (MetaMask, WalletConnect, Coinbase Wallet)
- **Charts:** Recharts or lightweight Chart.js
- **Deployment:** Vercel (free tier to start)

---

## Pages

### 1. Landing Page (`/`)
- Hero: "Fixed-Rate Yield on Ethereum Staking"
- How it works (3-step visual)
- Current rates by maturity (live from chain)
- TVL counter
- CTA → Connect wallet → Deposit

### 2. Deposit / Swap Page (`/deposit`)
- Connect wallet (MetaMask, WalletConnect)
- Select maturity series (dropdown: Q3-2026, Q4-2026, etc.)
- Input wstETH amount
- Display: fixed rate offered, effective rate after spread, maturity date, expected payout
- Approve + Deposit flow (2-tx)
- Confirmation with syLST balance update

### 3. Portfolio Dashboard (`/dashboard`)
- Connected wallet's positions:
  - Per-series: syLST balance, locked rate, maturity date, expected wstETH payout
  - Accrued interest (estimated based on elapsed time)
  - Time to maturity countdown
- Redeem button (active only after maturity)
- Transaction history (deposits, redemptions)

### 4. Protocol Health (`/protocol`)
- **Reserve Status:**
  - Reserve balance (wstETH)
  - Total liabilities
  - Solvency ratio κ (big gauge/dial)
  - κ vs κ_target vs κ_emergency (visual)
- **Series Overview:**
  - All active series: deposits, fixed rate, maturity, syLST supply
- **Dynamic Spread:**
  - Current spread (bp)
  - Spread curve visualization (s vs κ)
- **Yield Model:**
  - Current floating APR
  - Model-predicted APR (from last oracle update)
  - Yield curve chart (if available on-chain or via API)
- **Token Stats:**
  - SYLD price, market cap, staking APY
  - syLST secondary market stats (if DEX listed)

### 5. Docs / About (`/docs`)
- Link to SPEC.md / GitHub
- Whitepaper / memo
- FAQ
- Audit reports (when available)

---

## Implementation Phases

### Phase A — Scaffold (Week 1)
- [ ] Init Next.js + Tailwind + shadcn/ui
- [ ] wagmi provider setup (Hoodi testnet + Mainnet config)
- [ ] Basic layout: nav, footer, connect wallet button
- [ ] Landing page with static content

### Phase B — Deposit Flow (Week 2)
- [ ] Contract ABIs generated from Hardhat artifacts
- [ ] Read: active series, current rates, spread
- [ ] Write: approve wstETH, deposit into vault
- [ ] Transaction status toasts

### Phase C — Dashboard (Week 3)
- [ ] Read user's syLST balances (ERC-1155 balanceOfBatch)
- [ ] Compute accrued interest from on-chain data
- [ ] Redeem flow (post-maturity)
- [ ] Position cards with countdown timers

### Phase D — Protocol Health (Week 4)
- [ ] Read ReserveManager: reserve, liabilities, κ
- [ ] Solvency gauge component
- [ ] SpreadCalculator: current spread, spread curve
- [ ] Series table
- [ ] Optional: subgraph or event indexer for historical data

### Phase E — Polish & Deploy (Week 5)
- [ ] Responsive design
- [ ] Error handling, edge cases
- [ ] SEO, OG tags
- [ ] Deploy to Vercel
- [ ] Custom domain (stakeyieldfinance.com or similar)

---

## Directory Structure
```
website/
├── app/
│   ├── page.tsx          # Landing
│   ├── deposit/page.tsx  # Deposit flow
│   ├── dashboard/page.tsx # User portfolio
│   ├── protocol/page.tsx  # Protocol health
│   └── layout.tsx
├── components/
│   ├── ConnectButton.tsx
│   ├── DepositForm.tsx
│   ├── PositionCard.tsx
│   ├── SolvencyGauge.tsx
│   ├── SpreadCurve.tsx
│   └── SeriesTable.tsx
├── lib/
│   ├── contracts.ts      # ABIs + addresses
│   ├── wagmi.ts          # Provider config
│   └── utils.ts
├── public/
└── tailwind.config.ts
```
