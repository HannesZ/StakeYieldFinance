#!/bin/bash
# Fast backward backfill: 1 sample per day, back to Deneb (Mar 2024)
# ~686 daily queries × ~15s each ≈ 2-3 hours
cd "$(dirname "$0")"
source .venv/bin/activate
export PYTHONPATH=$PWD:$PWD/beacon_queues
export BEACON_NODE_URL_1=http://192.168.1.123:5051
python3 scripts/fast_backward_backfill.py \
    --start-slot 8626176 \
    >> backfill_backward.stdout 2>&1
