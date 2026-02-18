import csv
import os
import shutil

DATA_DIR = "data"
HISTORY_CSV = os.path.join(DATA_DIR, "history.csv")


def archive_base_csv(base_csv: str) -> str:
    """Move the base (unenriched) CSV to the archive folder. Returns the archived path."""
    archive_dir = os.path.join(os.path.dirname(base_csv) or ".", "archive")
    os.makedirs(archive_dir, exist_ok=True)
    archived_path = os.path.join(archive_dir, os.path.basename(base_csv))
    shutil.move(base_csv, archived_path)
    print(f"📦 Base CSV archived to {archived_path}")
    return archived_path


def store_enriched_csv(enriched_csv: str, start_slot: int, end_slot: int) -> str:
    """Move the enriched CSV into a per-run subfolder under data/. Returns the new path."""
    run_dir = os.path.join(DATA_DIR, f"slots_{start_slot}_{end_slot}")
    os.makedirs(run_dir, exist_ok=True)
    run_csv = os.path.join(run_dir, os.path.basename(enriched_csv))
    shutil.move(enriched_csv, run_csv)
    print(f"📁 Enriched CSV moved to {run_csv}")
    return run_csv


def append_to_history(run_csv: str):
    """Append new rows from run_csv to the unified history file, skipping duplicate slots."""
    history_exists = os.path.exists(HISTORY_CSV) and os.path.getsize(HISTORY_CSV) > 0

    existing_slots = set()
    if history_exists:
        with open(HISTORY_CSV, newline="") as f:
            for row in csv.DictReader(f):
                if row.get("slot"):
                    existing_slots.add(row["slot"])

    new_rows = 0
    with open(run_csv, newline="") as src:
        reader = csv.DictReader(src)
        with open(HISTORY_CSV, "a", newline="") as dst:
            writer = csv.DictWriter(dst, fieldnames=reader.fieldnames or [])
            if not history_exists:
                writer.writeheader()
            for row in reader:
                if row.get("slot") not in existing_slots:
                    writer.writerow(row)
                    new_rows += 1

    print(f"📜 History updated: {new_rows} new rows added → {HISTORY_CSV}")


def historise(base_csv: str, enriched_csv: str, start_slot: int, end_slot: int):
    """Run all historisation steps after a completed enrichment run."""
    archive_base_csv(base_csv)
    run_csv = store_enriched_csv(enriched_csv, start_slot, end_slot)
    append_to_history(run_csv)
