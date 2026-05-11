#!/bin/bash
source .venv/bin/activate
export PYTHONPATH=$PWD:$PWD/beacon_queues
python3 scripts/backfill_pipeline.py --target-slot 14295899 --chunk-epochs 100 --past-chunks 0 --sleep 0.0 >> backfill.stdout 2>&1
