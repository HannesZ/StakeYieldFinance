# Testnet Deployment Plan

## Network: Hoodi (Lido's Active Testnet)

**Why not Sepolia?** Lido's Sepolia deployment is **fully deprecated**. Their active, maintained testnet is **Hoodi** (Chain ID: 560048). Hoodi has a full Lido V3 deployment with real stETH/wstETH accumulation (rebase oracle active).

### Key Hoodi Addresses (Lido)
| Contract | Address |
|----------|---------|
| stETH | `0x3508A952176b3c15387C97BE809eaffB1982176a` |
| wstETH | `0x7E99eE3C66636DE415D2d7C880938F2f40f94De4` |
| Withdrawal Queue | `0xfe56573178f1bcdf53F01A6E9977670dcBBD9186` |
| Staking Router | `0xCc820558B39ee15C7C45B59390B503b83fb499A8` |

### Dual-Track Strategy
We can also keep Sepolia support using our `MockWstETH.sol` (simulated rebase) for CI/quick iteration, and deploy to Hoodi for realistic yield accumulation testing.

---

## Steps to Hoodi Deployment

### Phase 0 — Prerequisites
- [ ] Get Hoodi ETH from faucet
- [ ] Add Hoodi network to hardhat.config.ts (RPC: https://rpc.hoodi.ethpandaops.io, chainID: 560048)
- [ ] Stake Hoodi ETH on Lido testnet (https://stake-hoodi.testnet.fi) to get wstETH
- [ ] Create `.env` entries for HOODI_RPC_URL, DEPLOYER_KEY

### Phase 1 — Contract Deployment
- [ ] Update deploy script to accept Hoodi wstETH address
- [ ] Deploy SYLD Token → note address
- [ ] Deploy SyLST → note address
- [ ] Deploy ReserveManager → note address
- [ ] Deploy SpreadCalculator → note address
- [ ] Deploy StableYieldVault → note address
- [ ] Wire roles: Vault gets VAULT_ROLE on SyLST + ReserveManager
- [ ] Create first test series (e.g. 2026Q4, maturing Dec 31 2026)
- [ ] Verify all contracts on Hoodi Etherscan

### Phase 2 — End-to-End Test
- [ ] Approve wstETH → Vault
- [ ] Deposit wstETH into 2026Q4 series
- [ ] Verify syLST minted, liability registered
- [ ] Wait for wstETH rate to increase (Lido oracle rebase)
- [ ] Call harvestYield() → observe surplus flowing to reserve
- [ ] Check solvency ratio κ
- [ ] Simulate deficit scenario (if possible via governance rate override)

### Phase 3 — Monitoring
- [ ] Set up a simple dashboard reading on-chain state
- [ ] Track: TVL, reserve balance, κ, active series, syLST supply
- [ ] Log harvest events

---

## Fallback: Sepolia with MockWstETH
If Hoodi proves difficult (faucet, tooling, etc.), Sepolia remains viable using MockWstETH with simulated rebase. The mock already exists in `contracts/mocks/MockWstETH.sol`. We just need to call `setStEthPerToken()` periodically to simulate yield.

## Sepolia with Real (Deprecated) Lido
Sepolia wstETH exists at `0xB82381A3fBD3FaFA77B3a7bE693342618240067b` but the oracle/rebase may no longer update. Worth checking if the rate still moves — if so, it's a simpler option.
