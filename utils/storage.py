"""
utils/storage.py — saves order records to local JSON or MongoDB using canonical storage_uid.

Local mode (no MONGO_CONNECTION_STRING set):
    Upserts into data/orders_output.json (JSON array of records) by storage_uid.
    If data/orders_output.json does not exist but data/orders_output.jsonl exists,
    it is read for backward-compatible migration.

Mongo mode (MONGO_CONNECTION_STRING in config/.env):
    Upserts into MongoDB Atlas by storage_uid.
    Legacy unique index on order_number is migrated when possible.

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
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from rich.console import Console

from config.settings import settings

console = Console()
_mongo_col = None


def _to_int(v, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _clean_value(v) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _fallback_row_digest(record: dict) -> str:
    fingerprint = "|".join(
        _clean_value(record.get(k)).lower()
        for k in (
            "source",
            "client_name",
            "location",
            "received_date",
            "received_time",
            "status",
            "reviewed",
        )
    )
    return hashlib.sha1(fingerprint.encode("utf-8")).hexdigest()[:20]


def _derive_storage_uid(record: dict) -> str:
    client_doc_id = _to_int(record.get("client_document_id"), 0)
    if client_doc_id > 0:
        return f"cdoc:{client_doc_id}"

    employee_doc_id = _to_int(record.get("employee_document_id"), 0)
    if employee_doc_id > 0:
        return f"edoc:{employee_doc_id}"

    doc_reference_id = _to_int(record.get("doc_reference_id"), 0)
    order_id = _to_int(record.get("order_id"), 0)
    if doc_reference_id > 0 and order_id > 0:
        return f"dref:{doc_reference_id}:oid:{order_id}"
    if doc_reference_id > 0:
        return f"dref:{doc_reference_id}"

    worldview_id = _to_int(record.get("worldview_id"), 0)
    if worldview_id > 0 and order_id > 0:
        return f"wv:{worldview_id}:{order_id}"

    order_number = _clean_value(record.get("order_number"))
    received_date = _clean_value(record.get("received_date"))
    client_name = _clean_value(record.get("client_name"))
    if order_number and received_date:
        return f"ord:{order_number}:{received_date}:{client_name.lower()}"
    if order_number:
        return f"ord:{order_number}"

    row_dom_id = _clean_value(record.get("row_dom_id"))
    if row_dom_id:
        return f"dom:{row_dom_id}"

    return f"row:{_fallback_row_digest(record)}"


def _normalize_record(record: dict) -> dict:
    normalized = dict(record or {})
    storage_uid = _clean_value(normalized.get("storage_uid"))
    if not storage_uid:
        storage_uid = _derive_storage_uid(normalized)
    normalized["storage_uid"] = storage_uid
    return normalized


def _ensure_mongo_indexes(col):
    """Migrate old uniqueness to storage_uid without breaking existing collections."""
    try:
        info = col.index_information()
    except Exception:
        info = {}

    for name, details in info.items():
        if name == "_id_":
            continue

        keys = details.get("key") or []
        is_unique = bool(details.get("unique"))
        if is_unique and keys == [("order_number", 1)]:
            try:
                col.drop_index(name)
                console.log("[yellow]Dropped legacy Mongo unique index on order_number[/yellow]")
            except Exception as e:
                console.log(f"[yellow]Could not drop legacy order_number index: {e}[/yellow]")

    try:
        col.create_index("storage_uid", unique=True, name="storage_uid_unique")
    except Exception as e:
        console.log(f"[yellow]Could not ensure storage_uid unique index: {e}[/yellow]")


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
        _ensure_mongo_indexes(col)
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
        json.dump(records, f, ensure_ascii=False, indent=2)
        f.flush()
    tmp_path.replace(settings.JSON_OUTPUT)


def _upsert_local_records(incoming_records: list[dict]):
    existing = [_normalize_record(r) for r in _load_local_records() if isinstance(r, dict)]
    index_by_uid = {
        str(r.get("storage_uid")): i
        for i, r in enumerate(existing)
        if isinstance(r, dict) and r.get("storage_uid")
    }

    for raw_record in incoming_records:
        record = _normalize_record(raw_record)
        key = str(record.get("storage_uid") or "").strip()
        if key and key in index_by_uid:
            existing[index_by_uid[key]] = record
            continue

        if key:
            index_by_uid[key] = len(existing)
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
        stamped = _normalize_record(record)
        stamped["scraped_at"] = datetime.now(timezone.utc).isoformat()
        stamped_records.append(stamped)

    col = _get_mongo()
    if col is not None:
        for record in stamped_records:
            col.update_one(
                {"storage_uid": record["storage_uid"]},
                {"$set": record},
                upsert=True,
            )
    else:
        _upsert_local_records(stamped_records)

    console.log(f"[cyan]  Saved {len(records)} records[/cyan]")
