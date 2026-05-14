# Memo: Ethereum Staking Yield Curves, Discount Factors & Present Values

**Project:** StakeYieldFinance  
**Author:** Hannes Zühlke / Ulf 🐺  
**Date:** 2026-03-07  
**Status:** Draft — graphs to be added once backfill data is available

---

## 1. Overview

This memo derives a yield-curve framework for Ethereum staking returns. Unlike traditional fixed-income markets where par swap rates are observable, here the "risk-free rate" is the **consensus-layer reward rate** (APR) paid to active validators. The curve is constructed by:

1. Observing historical beacon-chain state (active validators, queues, rewards)
2. Projecting future validator counts (deterministic queue unwind + stochastic diffusion)
3. Deriving epoch-by-epoch APR paths → discount factors → zero-coupon rates

---

## 2. Data Pipeline

### 2.1 Beacon Chain State Variables

From `history.csv`, each epoch/slot provides:

| Variable | Column | Description |
|----------|--------|-------------|
| $N_\text{active}(t)$ | `active_count` / `active_eth` | Active validator set size |
| $Q_\text{entry}(t)$ | `entry_count` / `entry_eth` | Entry (activation) queue |
| $Q_\text{exit}(t)$ | `exit_count` / `exit_eth` | Exit (withdrawal) queue |
| $Q_\text{pending}(t)$ | `pending_deposits_count` / `pending_deposits_eth` | Pending deposit queue |

### 2.2 Churn Limit (Protocol Constraint)

The activation churn limit governs how fast validators enter/exit per epoch:

$$
\text{churn limit}(e) = \begin{cases}
\max\left(4,\; \left\lfloor \dfrac{N_\text{active}}{65536} \right\rfloor\right) & \text{if } e < e_\text{Deneb} \\[6pt]
\min\left(8,\; \max\left(4,\; \left\lfloor \dfrac{N_\text{active}}{65536} \right\rfloor\right)\right) & \text{if } e \ge e_\text{Deneb}
\end{cases}
$$

where $e_\text{Deneb} = 269{,}568$.

### 2.3 Derived Flows

From consecutive epoch snapshots:

$$
\text{activations}(t) = \max\Big(\Delta N_\text{active} + \max(\Delta Q_\text{exit},\, 0),\;\; -\Delta Q_\text{entry}\Big)
$$

$$
\text{new entry queue}(t) = \text{activations}(t) + \Delta Q_\text{entry}
$$

$$
\text{new exit queue}(t) = \text{activations}(t) - \Delta N_\text{active}
$$

These flows characterise the demand/supply dynamics of the validator set.

---

## 3. Staking APR Model

### 3.1 Base Reward per Increment

The protocol defines a *base reward per increment* $b$, the fundamental unit of all validator rewards:

$$
b = \left\lfloor \frac{\texttt{EFFECTIVE\_BALANCE\_INCREMENT} \times \texttt{BASE\_REWARD\_FACTOR}}{\left\lfloor\sqrt{B_\text{total}}\right\rfloor} \right\rfloor
$$

where $B_\text{total} = \sum_{i} \bar{e}_i$ is the total active balance in Gwei (sum of all active validators' effective balances), $\texttt{EFFECTIVE\_BALANCE\_INCREMENT} = 10^9$ Gwei (= 1 ETH), and $\texttt{BASE\_REWARD\_FACTOR} = 64$.

A validator with $n$ increments of effective balance (i.e. $n$ ETH effective balance, max 32) can earn at most $n \cdot b$ Gwei per epoch. This maximum is allocated across duties according to fixed weights (attestations 84.4%, block proposals 12.5%, sync committees 3.1%; see Appendix B for the full breakdown).

### 3.2 Participation Rate

In practice, not all validators perform perfectly. Attestation rewards are scaled by the fraction of the validator set that voted correctly. We capture this with a single **participation rate** $p \in (0, 1]$:

$$
\text{reward per epoch} = p \cdot n \cdot b
$$

Historically, $p \approx 0.995$ on mainnet (participation has been consistently above 99% since the Merge). For projections, a conservative estimate of $p = 0.99$ is used.

### 3.3 Annualised Yield $r_d$

Converting from Gwei-per-epoch to an annual percentage rate, the validator's effective balance $n$ cancels out:

$$
r_d = \frac{p \cdot n \cdot b \cdot 225 \times 365}{n \times 10^9} = \frac{p \cdot b \cdot 82{,}125}{10^9}
$$

Substituting the definition of $b$:

$$
\boxed{r_d = \frac{p \cdot \texttt{BASE\_REWARD\_FACTOR} \cdot 82{,}125}{\sqrt{B_\text{total}}} = \frac{p \times 5{,}256{,}000}{\sqrt{B_\text{total}}}}
$$

where 225 = epochs per day ($86400\text{s} / 384\text{s}$) and $82{,}125 = 225 \times 365$. The APR depends only on $B_\text{total}$, not on any individual validator's balance.

### 3.4 Simplified Form ($N$-based)

If all $N$ validators have the maximum 32 ETH effective balance ($B_\text{total} = 32 \times 10^9 \times N$):

$$
r_d = \frac{p \times 5{,}256{,}000}{\sqrt{32 \times 10^9 \times N}} \propto N^{-1/2}
$$

Example: at $N = 1{,}000{,}000$ and $p = 0.99$, $r_d \approx 2.91\%$.

### 3.5 Implementation

In practice, $B_\text{total}$ is read directly from the beacon chain state (not all validators have exactly 32 ETH effective balance), so we use the exact formula from Section 3.3. The resulting $r_d$ feeds into the discount factor calculation in Section 5.

---

## 4. Validator Count Projection

### 4.1 Phase 1: Deterministic Queue Unwind

Starting from the last observed state $(N_0,\; Q_{\text{entry},0},\; Q_{\text{exit},0},\; Q_{\text{pending},0})$, we simulate epoch-by-epoch:

$$
Q_\text{exit}(e+1) = \max\big(0,\; Q_\text{exit}(e) - d_\text{rate}\big)
$$

$$
a(e) = \min\big(a_\text{rate},\; Q_\text{entry}(e) + Q_\text{pending}(e)\big)
$$

$$
N_\text{active}(e+1) = N_\text{active}(e) + a(e)
$$

with entry and pending queues draining accordingly. This runs until all queues are empty at **queue-clear day** $d^*$.

**Time constants:**

- 225 epochs/day ($= 86400\text{s} \;/\; 384\text{s}$)
- Default activation/drain rate: 8 validators/epoch (post-Deneb cap)

### 4.2 Phase 2: Jump-Diffusion (Merton Model)

After $d^*$, a pure Gaussian random walk would assume deposit demand evolves smoothly — but in practice, large deposit surges (e.g. an LST protocol onboarding thousands of validators, or a staking wave triggered by price appreciation) arrive as discrete shocks. To capture this, we model the validator count as a **Merton jump-diffusion process** (see Appendix C for full derivation and calibration):

The deposit flow into the pending queue follows:

$$
D(d) = \mu_D + \sigma_D \cdot Z_d^{(D)} + \sum_{k=1}^{Y_d} J_k
$$

where:
- $\mu_D$, $\sigma_D$ are the baseline deposit drift and volatility
- $Z_d^{(D)} \sim \mathcal{N}(0,1)$ is the diffusion component
- $Y_d \sim \text{Poisson}(\lambda)$ is the jump count on day $d$ (typically 0 or 1)
- $J_k \sim \mathcal{N}(\mu_J, \sigma_J^2)$ is the jump size (excess above baseline)

Simultaneously, validators exit at rate:

$$
X(d) = \mu_X + \sigma_X \cdot Z_d^{(X)}
$$

Both deposit and exit flows feed into their respective queues, which drain through the churn limit epoch-by-epoch (same mechanics as Phase 1). A jump represents a **large deposit event** — the deposited validators enter the pending queue and must activate through the churn limit, creating a **secondary queue-unwind period** mid-simulation.

**Calibrated parameters (from 45-day beacon chain sample, slots 13.57M–13.9M):**

| Symbol | Calibrated Value | Meaning |
|--------|-----------------|---------|
| $\mu_D$ | 1,702 | Baseline daily deposits to pending queue |
| $\sigma_D$ | 703 | Daily deposit volatility |
| $\lambda$ | 0.0444 | Jump intensity (~16 jumps/year) |
| $\mu_J$ | 1,981 | Mean jump excess above baseline |
| $\sigma_J$ | 500 | Jump size std dev (estimated; only 2 observed jumps) |
| $\mu_X$ | 2,374 | Mean daily exit flow |
| $\sigma_X$ | 1,202 | Daily exit volatility |

**Calibration methodology** (threshold-based; see Appendix C.5 for alternatives):
1. Daily deposit flow estimated as $\Delta Q_\text{pending} + \min(1800, Q_\text{pending})$ (queue change + drained activations)
2. Trimmed mean ± 2σ threshold separates normal days from jump days
3. Exit flow estimated from $\text{activations} - \Delta N_\text{active}$

The net daily change $\mathbb{E}[\Delta N] = \mu_D + \lambda \mu_J - \mu_X = 1702 + 88 - 2374 = -584$, consistent with the observed net decline of ~574 validators/day during this period. Note that this reflects the **current exit-heavy regime**; a period with rising ETH prices or new staking products would shift the balance toward net growth.

This produces $M$ simulated paths of $N(d)$ for $d = 0, \ldots, D$, with realistic deposit-surge dynamics. See Appendix C for the theoretical background and calibration methodology.

---

## 5. Yield Curve Construction

### 5.1 From APR Paths to Discount Factors

Given a simulated APR path $\{r_d\}_{d=0}^{D}$, the **continuously-compounded discount factor** to day $T$ is:

$$
P(0, T) = \exp\left(-\sum_{d=1}^{T} \frac{r_d}{365}\right)
$$

This is the staking-implied present value of 1 ETH received at day $T$.

### 5.2 Zero-Coupon Spot Rate

The **annualised zero rate** to maturity $T$ (in years, $T_y = T/365$):

$$
z(T_y) = -\frac{\ln P(0, T)}{T_y}
$$

### 5.3 Monte Carlo Term Structure

For each tenor $T \in \{30, 90, 180, 365, 730, 1825\}$ days:

1. Compute $P^{(m)}(0, T)$ for each simulation $m = 1, \ldots, M$
2. Report percentiles:

$$
\hat{P}_{50}(T) = \text{median}\big\{P^{(m)}(0,T)\big\}
$$

$$
z_{50}(T_y) = -\frac{\ln \hat{P}_{50}(T)}{T_y}
$$

The 5th and 95th percentiles give confidence bands.

### 5.4 Worked Example (Calibrated Jump-Diffusion with Exits)

Starting from the beacon state as of slot 13,898,451 (epoch 434,326):

- $N_0 = 949{,}054$ active validators (~37.8M ETH active)
- $Q_\text{pending} = 43{,}999$ (~3M ETH)
- $Q_\text{entry} = 0$, $Q_\text{exit} = 2$

**Phase 1 (deterministic):** The pending queue drains at 8 validators/epoch × 225 epochs/day = 1,800/day, clearing in $d^* = 25$ days. During this phase the active count rises monotonically from 949,054 to ~993,053.

**Phase 2 (jump-diffusion with exits):** From day 25 onward, 2,000 simulations are run with parameters calibrated from the 45-day beacon chain sample (see Section 4.2). The model captures the current **exit-heavy regime** where ~2,374 validators leave per day vs ~1,790 arriving (including jumps), producing a net decline of ~584/day. Deposit surges ($\lambda = 0.0444$, ~16/year) periodically inject ~2,000 extra validators into the pending queue, creating temporary activation episodes.

**Results (2,000 simulations, 3-year term, calibrated parameters):**

| Tenor | $P_{5}(0,T)$ | $P_{50}(0,T)$ | $P_{95}(0,T)$ | $z_{5}$ | $z_{50}$ | $z_{95}$ |
|-------|-------------|---------------|-------------|---------|----------|---------|
| 30d   | 0.997091    | 0.997092      | 0.997092    | 3.54%   | 3.54%    | 3.54%   |
| 90d   | 0.991329    | 0.991344      | 0.991355    | 3.53%   | 3.53%    | 3.52%   |
| 180d  | 0.982715    | 0.982773      | 0.982806    | 3.54%   | 3.52%    | 3.52%   |
| 365d  | 0.965170    | 0.965352      | 0.965463    | 3.55%   | 3.53%    | 3.51%   |
| 730d  | 0.931237    | 0.931788      | 0.932120    | 3.56%   | 3.53%    | 3.51%   |
| 1095d | 0.898275    | 0.899295      | 0.899906    | 3.58%   | 3.54%    | 3.52%   |

**Projected active validator count at 1 year:**
- 5th percentile: 962,875
- Median: 982,073
- 95th percentile: 992,757
- (Current: 949,054)

**Key observations:**

1. **The term structure slopes upward** — the 5th percentile zero rate rises from 3.54% at 30d to 3.58% at 3y. This reflects the interplay between exits (which reduce the active set and *increase* APR) and deposits (which grow it and *compress* APR). In the current exit-heavy regime, the APR has upward pressure at longer horizons.

2. **Meaningful confidence bands** — the 5th–95th spread at 3 years is ~6bp, driven by uncertainty in both deposit jumps and exit flow volatility. Scenarios with above-average exits produce higher APR (lower discount factor), while deposit-surge-heavy scenarios compress it.

3. **The median curve is gently upward-sloping** — from 3.54% at 30d to 3.54% at 3y. Despite net negative drift, the deterministic queue unwind in the first 25 days pushes the active count up to ~993k, and the subsequent stochastic phase only partially erodes that gain.

4. **Regime sensitivity is high.** The current calibration reflects a period of net validator exits. If the deposit/exit balance shifts (e.g. ETH price rally driving new deposits), the curve shape would change qualitatively. This highlights the importance of periodic recalibration as more data accumulates.

5. **Short-sample caveat.** The calibration uses only 45 days of data with 2 observed deposit jumps. The jump parameters ($\lambda$, $\mu_J$, $\sigma_J$) are therefore estimated with wide uncertainty. As the backfill extends further back in time, these estimates will become more robust.

---

## 6. Present Value of Cash Flows

### 6.1 Single Cash Flow

For a cash flow $C$ at day $T$:

$$
\text{PV}^{(m)} = C \cdot P^{(m)}(0, T)
$$

Report $\text{PV}_{p5}$, $\text{PV}_{p50}$, $\text{PV}_{p95}$ across simulations.

### 6.2 Cash Flow Schedule

For a schedule $\{(T_i, C_i)\}_{i=1}^{K}$:

$$
\text{PV}^{(m)} = \sum_{i=1}^{K} C_i \cdot P^{(m)}(0, T_i)
$$

### 6.3 Interpretation

These present values answer: *"What is the current value of future ETH flows, discounted at the staking opportunity cost?"*

A validator choosing between:

- **(A)** Staking $X$ ETH now and earning rewards, vs.
- **(B)** Receiving $C$ ETH at time $T$

should compare $C \cdot P(0,T)$ against $X$.

---

## 7. Traditional Bootstrap (Reference)

For comparison, the project also implements a classical **swap-rate bootstrap** (see `swap_pricing.py` and `bootstrap_zero_coupon.md`).

Given par swap rates $S_1, S_2, \ldots, S_N$ with accrual $\alpha = 1/f$:

$$
P(0, t_N) = \frac{1 - S_N \displaystyle\sum_{i=1}^{N-1} \alpha \cdot P(0, t_i)}{1 + S_N \cdot \alpha}
$$

with base case:

$$
P(0, t_1) = \frac{1}{1 + S_1 \cdot \alpha}
$$

This serves as a validation tool: if hypothetical staking swap rates were quoted, one could bootstrap a curve and compare it against the simulation-based approach.

---

## 8. Distribution Fitting of Queue Flows

The `beacon_chain_modelling.py` script fits parametric distributions to the observed deposit/exit flows:

| Distribution | Notation | Parameters |
|-------------|----------|------------|
| Normal | $\mathcal{N}(\mu, \sigma^2)$ | mean, variance |
| Gamma | $\text{Gamma}(a, s)$ | shape, scale |
| Lognormal | $\text{LogN}(\sigma_s, s)$ | shape, scale |

Goodness-of-fit is assessed via **Kolmogorov–Smirnov tests**. These fits can inform the stochastic model parameters ($\mu$, $\sigma$) in Section 4.2 with empirical data rather than assumptions.

---

## 9. Graphs (Placeholder)

*To be populated once the backfill completes and results are generated.*

### 9.1 Historical Beacon Chain State
- [ ] Active validator count over time
- [ ] Entry/exit queue sizes over time
- [ ] Churn limit evolution

### 9.2 Staking APR
- [ ] Historical implied APR from active validator count
- [ ] APR model fit vs. observed

### 9.3 Yield Curve
- [ ] Zero-coupon rate term structure (median + 5th/95th percentile bands)
- [ ] Discount factor term structure
- [ ] Comparison across different drift/vol assumptions

### 9.4 Queue Flow Distributions
- [ ] Histogram of newly queued entries with fitted distributions
- [ ] Histogram of newly queued exits with fitted distributions
- [ ] QQ-plots for best-fit distribution

### 9.5 Present Value Sensitivity
- [ ] PV of 100 ETH as a function of tenor
- [ ] PV distribution (histogram) for selected tenors
- [ ] Sensitivity to drift and volatility parameters

---

## 10. Code Reference

| Component | File | Description |
|-----------|------|-------------|
| Backfill pipeline | `scripts/backfill_pipeline.py` | Resumable chunked data collection |
| Yield curve CLI | `scripts/yield_curve_cli.py` | Curve + PV computation with Monte Carlo |
| Swap bootstrap | `swap_pricing.py` | Classical zero-coupon bootstrap from par rates |
| Beacon modelling | `beacon_chain_modelling.py` | Queue flow analysis + distribution fitting |
| Beacon data helpers | `beacon_queues/` | API wrappers, history management, churn analysis |

---

## Appendix A: Key Constants

| Constant | Value | Source |
|----------|-------|--------|
| Seconds per slot | 12 | Consensus spec |
| Slots per epoch | 32 | Consensus spec |
| Seconds per epoch | 384 | $12 \times 32$ |
| Epochs per day | 225 | $86400 / 384$ |
| `MIN_PER_EPOCH_CHURN_LIMIT` | 4 | Consensus spec |
| `CHURN_LIMIT_QUOTIENT` | 65,536 | Consensus spec |
| `MAX_PER_EPOCH_ACTIVATION_CHURN_LIMIT` | 8 | Deneb upgrade |
| `MAX_PER_EPOCH_EXIT_CHURN_LIMIT` | 16 | Deneb upgrade |
| Deneb activation epoch | 269,568 | Mainnet |
| `BASE_REWARD_FACTOR` | 64 | Consensus spec |
| `EFFECTIVE_BALANCE_INCREMENT` | 1 ETH ($10^9$ Gwei) | Consensus spec |

---

## Appendix B: Consensus-Layer Reward Derivation

This appendix derives the yield formula used in Section 3 from the Ethereum consensus specification.

### B.1 Base Reward per Increment

The protocol defines a fundamental unit of reward $b$, computed per epoch:

$$
b = \left\lfloor \frac{\texttt{EFFECTIVE\_BALANCE\_INCREMENT} \times \texttt{BASE\_REWARD\_FACTOR}}{\left\lfloor\sqrt{B_\text{total}}\right\rfloor} \right\rfloor
$$

where $B_\text{total} = \sum_{i} \bar{e}_i$ is the total active balance in Gwei.

> **Spec:** [`get_base_reward_per_increment()`](https://github.com/ethereum/consensus-specs/blob/dev/specs/altair/beacon-chain.md#get_base_reward_per_increment), [`get_base_reward()`](https://github.com/ethereum/consensus-specs/blob/dev/specs/altair/beacon-chain.md#get_base_reward)

### B.2 Reward Allocation by Duty

A validator with $n$ increments of effective balance earns at most $n \cdot b$ Gwei per epoch, allocated by fixed weights:

| Duty | Weight | Symbol | Share |
|------|--------|--------|-------|
| Timely source vote | 14 | $W_s$ | 21.9% |
| Timely target vote | 26 | $W_t$ | 40.6% |
| Timely head vote | 14 | $W_h$ | 21.9% |
| Sync committee | 2 | $W_y$ | 3.1% |
| Block proposer | 8 | $W_p$ | 12.5% |
| **Total** | **64** | $W_\Sigma$ | **100%** |

Attestation duties (source + target + head = 84.4%) are earned deterministically every epoch. Proposer and sync committee rewards are stochastic but converge to their expected values over time.

> **Spec:** [Incentivization weights](https://github.com/ethereum/consensus-specs/blob/dev/specs/altair/beacon-chain.md#incentivization-weights)

### B.3 Total Issuance

Under ideal conditions (100% participation), the protocol issues $B_\text{total} \cdot b$ Gwei per epoch across all validators.

> **Spec:** [Issuance](https://eth2book.info/capella/part2/incentives/issuance/), [Rewards](https://eth2book.info/capella/part2/incentives/rewards/)

### B.4 Participation Rate

Attestation rewards are scaled by the fraction of validators that voted correctly:

$$
\text{actual reward (source)} = \frac{W_s}{W_\Sigma} \cdot n \cdot b \cdot \frac{\text{attesting balance (source)}}{B_\text{total}}
$$

We capture this with a single participation rate $p$:

$$
\text{reward per epoch} = p \cdot n \cdot b
$$

> **Spec:** [`get_flag_index_deltas()`](https://github.com/ethereum/consensus-specs/blob/dev/specs/altair/beacon-chain.md#get_flag_index_deltas)

### B.5 Annualisation

Converting from Gwei-per-epoch to annual yield (the $n$ cancels):

$$
r_d = \frac{p \cdot n \cdot b \cdot 225 \times 365}{n \times 10^9} = \frac{p \cdot b \cdot 82{,}125}{10^9}
$$

Substituting the definition of $b$:

$$
r_d = \frac{p \cdot \texttt{BASE\_REWARD\_FACTOR} \cdot 82{,}125}{\sqrt{B_\text{total}}} = \frac{p \times 5{,}256{,}000}{\sqrt{B_\text{total}}}
$$

### B.6 Spec References (Consensus Layer)

| Document | URL |
|----------|-----|
| Consensus specs (Altair) | https://github.com/ethereum/consensus-specs/blob/dev/specs/altair/beacon-chain.md |
| `get_base_reward_per_increment()` | https://github.com/ethereum/consensus-specs/blob/dev/specs/altair/beacon-chain.md#get_base_reward_per_increment |
| `get_base_reward()` | https://github.com/ethereum/consensus-specs/blob/dev/specs/altair/beacon-chain.md#get_base_reward |
| `get_flag_index_deltas()` | https://github.com/ethereum/consensus-specs/blob/dev/specs/altair/beacon-chain.md#get_flag_index_deltas |
| Incentivization weights | https://github.com/ethereum/consensus-specs/blob/dev/specs/altair/beacon-chain.md#incentivization-weights |
| Annotated spec (rewards) | https://eth2book.info/capella/part2/incentives/rewards/ |
| Annotated spec (issuance) | https://eth2book.info/capella/part2/incentives/issuance/ |

---

## Appendix C: Jump-Diffusion Model for Validator Dynamics

This appendix provides the theoretical background for the jump-diffusion model used in Section 4.2. It is intended to be self-contained for readers without prior exposure to jump processes.

### C.1 Motivation: Why Not Pure Diffusion?

A geometric Brownian motion (GBM) or arithmetic random walk assumes that the validator count evolves **continuously** — small increments each day, never large sudden changes. This is a poor model of reality:

- **Deposit contract events are lumpy.** A single entity (LST protocol, institutional staker, whale) can deposit thousands of validators in a single transaction batch.
- **Market sentiment creates clustering.** Bull markets trigger deposit waves; bear markets trigger exit waves. These arrive as discrete regime shifts, not smooth trends.
- **The activation queue creates memory.** A large deposit doesn't affect APR immediately — it enters the pending queue and drains over days/weeks. This lagged, chunky effect is fundamentally non-Gaussian.

Empirically, daily changes in the pending deposit queue exhibit **heavy tails** and **positive skew** — characteristics of a process with occasional large jumps superimposed on smaller day-to-day fluctuations.

### C.2 The Merton Jump-Diffusion Model

The **Merton (1976)** jump-diffusion model was originally developed for option pricing on assets whose prices can jump discontinuously (e.g. on earnings announcements). We adapt it here for the validator count.

#### C.2.1 Continuous-Time Formulation

In continuous time, the validator count $N(t)$ follows:

$$
dN(t) = \mu \, dt + \sigma \, dW(t) + J \, dP(t)
$$

where:
- $\mu$ is the **drift** (expected net growth per unit time, excluding jumps)
- $\sigma$ is the **diffusion volatility** (day-to-day noise amplitude)
- $W(t)$ is a standard **Wiener process** (Brownian motion)
- $P(t)$ is a **Poisson process** with intensity $\lambda$ (arrivals per unit time)
- $J$ is the random **jump size**, drawn independently each time $P$ increments

The Poisson process $P(t)$ counts the number of jump events up to time $t$. In a small interval $dt$:

$$
\Pr[dP = 1] = \lambda \, dt, \qquad \Pr[dP = 0] = 1 - \lambda \, dt
$$

The three components — drift, diffusion, and jumps — are mutually independent.

#### C.2.2 Discrete-Time Implementation

For daily simulation (as used in the CLI), the discrete-time version is:

$$
N(d+1) = N(d) + \mu + \sigma \cdot Z_d + \sum_{k=1}^{Y_d} J_k
$$

where:
- $Z_d \sim \mathcal{N}(0, 1)$ — standard normal (diffusion)
- $Y_d \sim \text{Poisson}(\lambda)$ — number of jumps on day $d$
- $J_k \sim \mathcal{N}(\mu_J, \sigma_J^2)$ — size of the $k$-th jump, i.i.d.

When $\lambda$ is small (e.g. 0.02), most days have $Y_d = 0$ (no jump), some days have $Y_d = 1$, and $Y_d \geq 2$ is very rare ($\Pr[Y_d \geq 2] \approx \lambda^2/2 \approx 0.02\%$).

#### C.2.3 Compound Poisson Component

The sum $\sum_{k=1}^{Y_d} J_k$ is a **compound Poisson random variable**. Its properties:

$$
\mathbb{E}\left[\sum_{k=1}^{Y_d} J_k\right] = \lambda \cdot \mu_J
$$

$$
\text{Var}\left[\sum_{k=1}^{Y_d} J_k\right] = \lambda \cdot (\sigma_J^2 + \mu_J^2)
$$

The total expected daily change (including jumps) is therefore:

$$
\mathbb{E}[\Delta N] = \mu + \lambda \cdot \mu_J
$$

$$
\text{Var}[\Delta N] = \sigma^2 + \lambda \cdot (\sigma_J^2 + \mu_J^2)
$$

With the default parameters ($\mu = 8$, $\lambda = 0.02$, $\mu_J = 5000$):

$$
\mathbb{E}[\Delta N] = 8 + 0.02 \times 5000 = 108 \text{ validators/day}
$$

This means the jump component contributes significantly to average growth — which is realistic, since most net validator growth historically comes from large coordinated deposits, not individual stakers.

### C.3 Integration with Queue Mechanics

A critical feature of our implementation is that **jumps don't instantly become active validators**. When a jump of size $J$ occurs:

1. $J$ validators are added to the **pending deposit queue** $Q_\text{pending}$
2. They drain through the churn limit at $a_\text{rate} = 8$ validators/epoch (1,800/day)
3. While the queue is non-zero, the activation is **deterministic** (same mechanics as Phase 1)
4. Only after the queue clears does the stochastic diffusion resume

This is a departure from classical Merton (where jumps affect the price instantly). Here, a jump is a **deposit event** that creates a temporary regime of deterministic queue processing. The effect on APR is:

- **Immediate**: the market knows a large deposit is pending, but APR doesn't change yet
- **Lagged**: as validators activate over the following days/weeks, $N_\text{active}$ rises and APR compresses
- **Duration**: a 5,000-validator jump creates a queue lasting $\lceil 5000 / 1800 \rceil \approx 3$ days

Multiple jumps can **stack**: if a second jump arrives before the first queue clears, the queues add up, extending the activation period.

### C.4 Statistical Properties

#### C.4.1 Distribution of Daily Changes

The unconditional distribution of $\Delta N$ is a **mixture**:

$$
f_{\Delta N}(x) = (1 - \lambda) \cdot \phi\!\left(\frac{x - \mu}{\sigma}\right) + \lambda \cdot \phi\!\left(\frac{x - \mu - \mu_J}{\sqrt{\sigma^2 + \sigma_J^2}}\right) + O(\lambda^2)
$$

where $\phi$ is the standard normal density. This is a **two-component Gaussian mixture** (to first order in $\lambda$):
- With probability $1 - \lambda$: a "normal" day, $\Delta N \sim \mathcal{N}(\mu, \sigma^2)$
- With probability $\lambda$: a "jump" day, $\Delta N \sim \mathcal{N}(\mu + \mu_J, \sigma^2 + \sigma_J^2)$

This mixture has **heavier tails** and **positive skew** compared to a pure normal, matching the empirical observation that large deposit days are rare but impactful.

#### C.4.2 Kurtosis and Skewness

The excess kurtosis of the daily changes is approximately:

$$
\kappa_\text{excess} \approx \frac{\lambda \cdot \mu_J^4}{(\sigma^2 + \lambda(\sigma_J^2 + \mu_J^2))^2}
$$

With default parameters, $\kappa_\text{excess} \approx 56$, indicating extremely heavy tails — consistent with the lumpy nature of validator deposits.

### C.5 Parameter Calibration

The jump-diffusion has five free parameters: $\mu$, $\sigma$, $\lambda$, $\mu_J$, $\sigma_J$. We calibrate them from historical beacon chain data.

#### C.5.1 Method 1: Threshold-Based Separation

The simplest approach:

1. Compute daily changes $\Delta N_d$ from `history.csv`
2. Define a threshold $\tau$ (e.g. 2 standard deviations of the trimmed series)
3. Days with $|\Delta N_d| > \tau$ are classified as "jump days"
4. Fit $\mu$, $\sigma$ to the non-jump days (pure diffusion)
5. Fit $\lambda$ = (number of jump days) / (total days), and $\mu_J$, $\sigma_J$ to the jump-day magnitudes

This is intuitive but somewhat arbitrary in the choice of $\tau$.

#### C.5.2 Method 2: Maximum Likelihood (EM Algorithm)

More rigorous: treat the model as a **mixture model** and use the Expectation-Maximisation (EM) algorithm:

**E-step:** For each day $d$, compute the posterior probability that it was a jump day:

$$
w_d = \frac{\lambda \cdot \phi\!\left(\frac{\Delta N_d - \mu - \mu_J}{\sqrt{\sigma^2 + \sigma_J^2}}\right)}{(1-\lambda) \cdot \phi\!\left(\frac{\Delta N_d - \mu}{\sigma}\right) + \lambda \cdot \phi\!\left(\frac{\Delta N_d - \mu - \mu_J}{\sqrt{\sigma^2 + \sigma_J^2}}\right)}
$$

**M-step:** Update parameters using weighted MLE:

$$
\lambda = \frac{1}{D}\sum_d w_d, \qquad \mu = \frac{\sum_d (1-w_d) \Delta N_d}{\sum_d (1-w_d)}, \qquad \text{etc.}
$$

Iterate until convergence. This jointly estimates all five parameters without a threshold.

#### C.5.3 Method 3: Direct Queue Observation

The most reliable for our specific application: directly observe large deposits in the beacon chain data:

1. Track daily changes in $Q_\text{pending}$: $\Delta Q_\text{pending}(d)$
2. Large positive changes indicate deposit events (new validators arriving)
3. The jump intensity $\lambda$ is the frequency of days with $\Delta Q_\text{pending} > \tau$
4. The jump size distribution is fit to the magnitudes $\{\Delta Q_\text{pending}(d) : \Delta Q_\text{pending}(d) > \tau\}$

This is the preferred method as it directly measures what we're modelling: arrivals to the deposit queue.

### C.6 Exit Flow Model

The full model includes a stochastic exit process alongside deposits:

$$
X(d) = \mu_X + \sigma_X \cdot Z_d^{(X)}, \qquad Z_d^{(X)} \sim \mathcal{N}(0,1)
$$

Exit flow $X(d)$ is added to the exit queue each day, which drains through the exit churn limit (up to 16/epoch post-Deneb, but typically limited by queue size). Exiting validators are removed from the active set.

The exit flow is calibrated independently from deposits. During the observation period, exits significantly exceeded deposits ($\mu_X = 2{,}374 > \mu_D + \lambda\mu_J = 1{,}790$), producing a net contraction of the validator set. This is consistent with Ethereum's post-peak validator dynamics as some stakers exit after the Shanghai/Capella withdrawal enablement.

**Net daily active change:**

$$
\mathbb{E}[\Delta N_\text{active}] = \mu_D + \lambda \mu_J - \mu_X
$$

The sign of this expression determines the long-run trend: positive means the active set grows (APR compresses over time), negative means it shrinks (APR rises). The yield curve shape follows accordingly — an upward-sloping zero rate curve indicates the market expects APR to *increase* (validator set shrinking), while downward-sloping indicates *decreasing* APR (validator set growing).

### C.7 Comparison: Pure Diffusion vs. Jump-Diffusion with Exits

| Property | Pure Diffusion | Jump-Diffusion + Exits |
|----------|---------------|----------------------|
| Daily $\Delta N$ distribution | Normal | Mixture (heavy-tailed) |
| Large deposit events | Not modelled | Captured by jumps |
| Exit dynamics | Not modelled | Stochastic exit flow |
| Queue re-activation | Never (post-$d^*$) | Yes, triggered by jumps |
| APR path | Smooth, monotone | Punctuated compression + exit-driven rise |
| Yield curve shape | Flat after $d^*$ | Shaped by deposit/exit balance |
| Confidence bands | Narrow, symmetric | Wide, regime-dependent |
| Calibration data needed | $\mu$, $\sigma$ only | $\mu_D$, $\sigma_D$, $\lambda$, $\mu_J$, $\sigma_J$, $\mu_X$, $\sigma_X$ |
| Computational cost | $O(M \cdot D)$ | $O(M \cdot D)$ (same, with queue sim overhead) |

### C.8 Limitations and Extensions

**Limitations of the current model:**

1. **Symmetric jumps.** We model jumps as normal, but deposit surges are inherently one-sided (positive). A **lognormal** or **exponential** jump size distribution may be more appropriate. The current implementation clips negative jumps to zero.
2. **Constant intensity.** Jump arrival rate $\lambda$ is fixed, but in reality deposit waves correlate with ETH price, macro sentiment, and protocol upgrades. A **Hawkes process** (self-exciting point process) or regime-switching $\lambda$ would capture clustering.
3. **No exit jumps.** The model focuses on deposit surges. Exit surges (e.g. post-Shanghai withdrawal waves) could be modelled with a separate jump process on the exit queue.
4. **Independence.** Jumps are assumed independent of the diffusion. In practice, a large deposit event might temporarily suppress further deposits (queue congestion) or attract more (FOMO).

**Potential extensions:**

- **Hawkes process** for jump arrivals: $\lambda(t) = \lambda_0 + \sum_{t_i < t} \alpha \cdot e^{-\beta(t-t_i)}$, where past jumps temporarily increase the arrival rate
- **Regime-switching** drift: $\mu$ alternates between "growth" and "contraction" regimes with Markov transition probabilities
- **Correlated exit jumps**: model entry and exit surges as dependent processes
- **ETH-price-conditional** intensity: $\lambda = f(\text{ETH price return})$

### C.9 References

| Reference | Description |
|-----------|-------------|
| Merton, R.C. (1976). "Option pricing when underlying stock returns are discontinuous." *Journal of Financial Economics*, 3(1-2), 125-144. | Original jump-diffusion model for asset prices |
| Kou, S.G. (2002). "A Jump-Diffusion Model for Option Pricing." *Management Science*, 48(8), 1086-1101. | Double-exponential jump-diffusion (asymmetric jumps) |
| Cont, R. and Tankov, P. (2004). *Financial Modelling with Jump Processes.* Chapman & Hall/CRC. | Comprehensive textbook on jump processes in finance |
| Hawkes, A.G. (1971). "Spectra of some self-exciting and mutually exciting point processes." *Biometrika*, 58(1), 83-90. | Self-exciting point processes (potential extension) |
