"""
utils/storage.py — saves order records to local JSON (Phase 1) or MongoDB (Phase 2).

Phase 1 (no MONGO_CONNECTION_STRING set):
    Upserts into data/orders_output.json (JSON array of records).
    If data/orders_output.json does not exist but data/orders_output.jsonl exists,
    it is read for backward-compatible migration.

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
from pathlib import Path
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
        console.log(f"[red]MongoDB unavailable ({e}) — using local JSON[/red]")
        return None


def _read_json_array(path: Path) -> list[dict]:
    if not path.exists():
        return []

    try:
        with open(path, encoding="utf-8") as f:
            raw = json.load(f)
        if isinstance(raw, list):
            return [r for r in raw if isinstance(r, dict)]
    except Exception:
        return []
    return []


def _read_jsonl_lines(path: Path) -> list[dict]:
    if not path.exists():
        return []

    records: list[dict] = []
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                    if isinstance(row, dict):
                        records.append(row)
                except Exception:
                    continue
    except Exception:
        return []
    return records


def _load_local_records() -> list[dict]:
    records = _read_json_array(settings.JSON_OUTPUT)
    if settings.JSON_OUTPUT.exists():
        return records

    legacy = _read_jsonl_lines(settings.JSONL_OUTPUT)
    if legacy:
        console.log(
            f"[yellow]Migrating local records from {settings.JSONL_OUTPUT} -> {settings.JSON_OUTPUT}[/yellow]"
        )
    return legacy


def _write_local_records(records: list[dict]):
    settings.JSON_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = settings.JSON_OUTPUT.with_suffix(".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, separators=(",", ":"))
        f.flush()
    tmp_path.replace(settings.JSON_OUTPUT)


def _upsert_local_records(incoming_records: list[dict]):
    existing = _load_local_records()
    index_by_order = {
        str(r.get("order_number")): i
        for i, r in enumerate(existing)
        if isinstance(r, dict) and r.get("order_number")
    }

    for record in incoming_records:
        key = str(record.get("order_number") or "").strip()
        if key and key in index_by_order:
            existing[index_by_order[key]] = record
            continue

        if key:
            index_by_order[key] = len(existing)
        existing.append(record)

    _write_local_records(existing)


def save_order(record: dict):
    """Upsert a single order record."""
    save_batch([record])


def save_batch(records: list[dict]):
    if not records:
        return

    stamped_records: list[dict] = []
    for record in records:
        stamped = dict(record)
        stamped["scraped_at"] = datetime.now(timezone.utc).isoformat()
        stamped_records.append(stamped)

    col = _get_mongo()
    if col is not None:
        for record in stamped_records:
            col.update_one(
                {"order_number": record["order_number"]},
                {"$set": record},
                upsert=True,
            )
    else:
        _upsert_local_records(stamped_records)

    console.log(f"[cyan]  Saved {len(records)} records[/cyan]")
