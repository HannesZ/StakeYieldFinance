#!/bin/bash
# Update the model-derived fixed rate: compute → push on-chain
#
# Usage:
#   ./scripts/update_rate.sh              # default: 2026Q4, 2000 sims
#   ./scripts/update_rate.sh --sims 5000  # more simulations
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "═══════════════════════════════════════════════════════════"
echo "  StakeYield Finance — Model Rate Update Pipeline"
echo "═══════════════════════════════════════════════════════════"
echo ""

# Step 1: Activate Python venv and compute rate
echo "▸ Step 1: Computing model rate..."
cd "$PROJECT_ROOT"
source .venv/bin/activate
python3 scripts/compute_offered_rate.py "$@"

# Step 2: Push to chain
echo ""
echo "▸ Step 2: Pushing rate on-chain..."
cd "$PROJECT_ROOT/protocol"
npx hardhat run scripts/push-model-rate.ts --network hoodi

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  Done."
echo "═══════════════════════════════════════════════════════════"
