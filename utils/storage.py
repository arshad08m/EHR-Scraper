"""
utils/storage.py — saves order records to JSONL (Phase 1) or MongoDB (Phase 2).

Phase 1 (no MONGO_CONNECTION_STRING set):
  Appends to data/orders_output.jsonl (one JSON object per line).

Phase 2 (MONGO_CONNECTION_STRING in config/.env):
  Upserts into MongoDB Atlas on order_number — no other code changes needed.

Full document schema stored per record:
  # From WorldView table row
  order_number, received_date, received_time, client_name,
  location, status, reviewed, doc_type, date_batch

  # From popup document capture
  document_base64, document_mime_type, document_url,
  document_size_bytes, document_filename

  # Extracted from popup visible text (bonus — no OCR needed)
  mrn_from_doc, dob_from_doc, patient_name_from_doc,
  npi_from_doc, physician_name_from_doc,
  order_number_from_doc, order_date_from_doc,
  primary_diagnosis_from_doc, certification_period_from_doc,
  payer_source_from_doc

  # After enrichment join with master Excel
  patient_id, physician_npi, patient_match, physician_match

  # System
  scraped_at
"""

import json
from datetime import datetime, timezone
from rich.console import Console

from config.settings import settings

console = Console()
_mongo_col = None


def _get_mongo():
    global _mongo_col
    if _mongo_col is not None:
        return _mongo_col
    if not settings.MONGO_URI:
        return None
    try:
        from pymongo import MongoClient
        client = MongoClient(settings.MONGO_URI, serverSelectionTimeoutMS=5_000)
        client.admin.command("ping")
        col = client[settings.MONGO_DB][settings.MONGO_COLLECTION]
        col.create_index("order_number", unique=True)
        _mongo_col = col
        console.log("[green]✓ Connected to MongoDB Atlas[/green]")
        return col
    except Exception as e:
        console.log(f"[red]MongoDB unavailable ({e}) — using JSONL[/red]")
        return None


def save_order(record: dict):
    """Upsert a single order record."""
    record["scraped_at"] = datetime.now(timezone.utc).isoformat()
    col = _get_mongo()
    if col is not None:
        col.update_one(
            {"order_number": record["order_number"]},
            {"$set": record},
            upsert=True,
        )
    else:
        settings.JSONL_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        with open(settings.JSONL_OUTPUT, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def save_batch(records: list[dict]):
    for r in records:
        save_order(r)
    console.log(f"[cyan]  Saved {len(records)} records[/cyan]")
