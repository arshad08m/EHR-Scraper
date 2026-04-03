"""
utils/storage.py — saves order records to Supabase, MongoDB, or local JSON.

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
_supabase_client = None

STRICT_COLUMNS_READ = [
    "client_id",
    "document_id",
    "doc_identity",
    "storage_uid",
    "file_id",
    "date_batch",
    "scraped_at",
    "source",
    "row_dom_id",
    "worldview_id",
    "order_id",
    "change_order_type",
    "reference_id",
    "is_echart_order_format",
    "cg_task_id",
    "admit_no",
    "intake_id",
    "episode_id",
    "payer_id",
    "library_form_id",
    "order_type",
    "initial_order_in_regular_format",
    "requested_by",
    "form_485_id",
    "poc",
    "doc_reference_id",
    "client_document_id",
    "employee_document_id",
    "employee_id",
    "staff_type",
    "query_485_string",
    "received_date",
    "received_time",
    "client_name",
    "location",
    "order_number",
    "status",
    "reviewed",
    "doc_type",
    "has_view",
    "document_popup_url",
    "document_path",
    "document_url",
    "document_file_path",
    "document_storage_url",
    "document_base64",
    "document_mime_type",
    "document_size_bytes",
    "document_filename",
    "mrn_from_doc",
    "dob_from_doc",
    "patient_name_from_doc",
    "npi_from_doc",
    "physician_name_from_doc",
    "order_number_from_doc",
    "order_date_from_doc",
    "primary_diagnosis_from_doc",
    "certification_period_from_doc",
    "payer_source_from_doc",
    "patient_id",
    "physician_npi",
    "patient_match",
    "physician_match",
    "upload_status",
    "uploaded_to_patient_db",
    "uploaded_at",
    "upload_attempt_count",
    "upload_run_id",
    "upload_error",
]

# doc_identity is GENERATED in Postgres for many setups; do not write it.
STRICT_COLUMNS_WRITE = [c for c in STRICT_COLUMNS_READ if c not in {"doc_identity"}]


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


def _derive_document_id(record: dict) -> str:
    client_doc_id = _to_int(record.get("client_document_id"), 0)
    if client_doc_id > 0:
        return f"cdoc:{client_doc_id}"

    employee_doc_id = _to_int(record.get("employee_document_id"), 0)
    if employee_doc_id > 0:
        return f"edoc:{employee_doc_id}"

    doc_reference_id = _to_int(record.get("doc_reference_id"), 0)
    if doc_reference_id > 0:
        return f"dref:{doc_reference_id}"

    storage_uid = _clean_value(record.get("storage_uid"))
    if storage_uid:
        return storage_uid
    return f"row:{_fallback_row_digest(record)}"


def _derive_doc_identity(record: dict) -> str:
    client_id = _to_int(record.get("client_id"), 0)
    document_id = _clean_value(record.get("document_id")) or _derive_document_id(record)
    return f"{client_id}::{document_id}".lower()


def _normalize_record(record: dict) -> dict:
    normalized = dict(record or {})
    storage_uid = _clean_value(normalized.get("storage_uid"))
    if not storage_uid:
        storage_uid = _derive_storage_uid(normalized)
    normalized["storage_uid"] = storage_uid
    normalized["client_id"] = _to_int(normalized.get("client_id"), 0)
    normalized["document_id"] = _clean_value(normalized.get("document_id")) or _derive_document_id(normalized)
    normalized["doc_identity"] = _derive_doc_identity(normalized)
    normalized.setdefault("upload_status", "pending")
    normalized.setdefault("uploaded_to_patient_db", False)
    normalized.setdefault("uploaded_at", None)
    normalized.setdefault("upload_attempt_count", 0)
    normalized.setdefault("upload_run_id", None)
    normalized.setdefault("upload_error", None)
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


def _supabase_enabled() -> bool:
    return bool(settings.SUPABASE_URL and settings.SUPABASE_SERVICE_ROLE_KEY)


def _get_supabase():
    global _supabase_client
    if _supabase_client is not None:
        return _supabase_client
    if not _supabase_enabled():
        return None
    try:
        from supabase import create_client
        _supabase_client = create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_ROLE_KEY)
        return _supabase_client
    except Exception as e:
        console.log(f"[red]Supabase unavailable ({e})[/red]")
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


def _to_supabase_row(record: dict) -> dict:
    normalized = _normalize_record(record)
    row = {
        "client_id": _to_int(normalized.get("client_id"), 0),
        "document_id": _clean_value(normalized.get("document_id")) or _derive_document_id(normalized),
        "storage_uid": _clean_value(normalized.get("storage_uid")),
        "file_id": _clean_value(normalized.get("file_id")) or None,
        "date_batch": _clean_value(normalized.get("date_batch")) or None,
        "scraped_at": normalized.get("scraped_at"),
        "source": _clean_value(normalized.get("source")) or None,
        "row_dom_id": _clean_value(normalized.get("row_dom_id")) or None,
        "worldview_id": _to_int(normalized.get("worldview_id"), 0),
        "order_id": _to_int(normalized.get("order_id"), 0),
        "change_order_type": _to_int(normalized.get("change_order_type"), 0),
        "reference_id": _to_int(normalized.get("reference_id"), 0),
        "is_echart_order_format": bool(normalized.get("is_echart_order_format", False)),
        "cg_task_id": _to_int(normalized.get("cg_task_id"), 0),
        "admit_no": _to_int(normalized.get("admit_no"), 0),
        "intake_id": _to_int(normalized.get("intake_id"), 0),
        "episode_id": _to_int(normalized.get("episode_id"), 0),
        "payer_id": _to_int(normalized.get("payer_id"), 0),
        "library_form_id": _to_int(normalized.get("library_form_id"), 0),
        "order_type": _clean_value(normalized.get("order_type")) or None,
        "initial_order_in_regular_format": bool(normalized.get("initial_order_in_regular_format", False)),
        "requested_by": bool(normalized.get("requested_by", False)),
        "form_485_id": _to_int(normalized.get("form_485_id"), 0),
        "poc": _to_int(normalized.get("poc"), 0),
        "doc_reference_id": _to_int(normalized.get("doc_reference_id"), 0),
        "client_document_id": _to_int(normalized.get("client_document_id"), 0),
        "employee_document_id": _to_int(normalized.get("employee_document_id"), 0),
        "employee_id": _to_int(normalized.get("employee_id"), 0),
        "staff_type": _to_int(normalized.get("staff_type"), 0),
        "query_485_string": _clean_value(normalized.get("query_485_string")) or None,
        "received_date": _clean_value(normalized.get("received_date")) or None,
        "received_time": _clean_value(normalized.get("received_time")) or None,
        "client_name": _clean_value(normalized.get("client_name")) or None,
        "location": _clean_value(normalized.get("location")) or None,
        "order_number": _clean_value(normalized.get("order_number")) or None,
        "status": _clean_value(normalized.get("status")) or None,
        "reviewed": _clean_value(normalized.get("reviewed")) or None,
        "doc_type": _clean_value(normalized.get("doc_type")) or None,
        "has_view": bool(normalized.get("has_view", False)),
        "document_popup_url": _clean_value(normalized.get("document_popup_url")) or None,
        "document_path": _clean_value(normalized.get("document_path")) or None,
        "document_url": _clean_value(normalized.get("document_url")) or None,
        "document_file_path": _clean_value(normalized.get("document_file_path")) or None,
        "document_storage_url": _clean_value(normalized.get("document_storage_url")) or None,
        "document_base64": _clean_value(normalized.get("document_base64")) or None,
        "document_mime_type": _clean_value(normalized.get("document_mime_type")) or None,
        "document_size_bytes": _to_int(normalized.get("document_size_bytes"), 0),
        "document_filename": _clean_value(normalized.get("document_filename")) or None,
        "mrn_from_doc": _clean_value(normalized.get("mrn_from_doc")) or None,
        "dob_from_doc": _clean_value(normalized.get("dob_from_doc")) or None,
        "patient_name_from_doc": _clean_value(normalized.get("patient_name_from_doc")) or None,
        "npi_from_doc": _clean_value(normalized.get("npi_from_doc")) or None,
        "physician_name_from_doc": _clean_value(normalized.get("physician_name_from_doc")) or None,
        "order_number_from_doc": _clean_value(normalized.get("order_number_from_doc")) or None,
        "order_date_from_doc": _clean_value(normalized.get("order_date_from_doc")) or None,
        "primary_diagnosis_from_doc": _clean_value(normalized.get("primary_diagnosis_from_doc")) or None,
        "certification_period_from_doc": _clean_value(normalized.get("certification_period_from_doc")) or None,
        "payer_source_from_doc": _clean_value(normalized.get("payer_source_from_doc")) or None,
        "patient_id": _clean_value(normalized.get("patient_id")) or None,
        "physician_npi": _clean_value(normalized.get("physician_npi")) or None,
        "patient_match": _clean_value(normalized.get("patient_match")) or None,
        "physician_match": _clean_value(normalized.get("physician_match")) or None,
        "upload_status": _clean_value(normalized.get("upload_status")) or "pending",
        "uploaded_to_patient_db": bool(normalized.get("uploaded_to_patient_db", False)),
        "uploaded_at": normalized.get("uploaded_at"),
        "upload_attempt_count": _to_int(normalized.get("upload_attempt_count"), 0),
        "upload_run_id": _clean_value(normalized.get("upload_run_id")) or None,
        "upload_error": _clean_value(normalized.get("upload_error")) or None,
    }
    return {k: v for k, v in row.items() if k in STRICT_COLUMNS_WRITE}


def _upsert_supabase_records(records: list[dict]) -> bool:
    client = _get_supabase()
    if client is None:
        return False

    rows = [_to_supabase_row(r) for r in records if isinstance(r, dict)]
    if not rows:
        return True

    try:
        (
            client.table(settings.SUPABASE_ORDERS_TABLE)
            .upsert(rows, on_conflict="client_id,document_id")
            .execute()
        )
        return True
    except Exception as e:
        console.log(f"[red]Supabase upsert failed ({e}); falling back[/red]")
        return False


def _rows_from_supabase_payload(rows: list[dict]) -> list[dict]:
    out: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized = _normalize_record({k: row.get(k) for k in STRICT_COLUMNS_READ if k in row})
        # Keep upload-tracking values from table as source of truth.
        for key in (
            "upload_status",
            "uploaded_to_patient_db",
            "uploaded_at",
            "upload_attempt_count",
            "upload_run_id",
            "upload_error",
        ):
            if key in row:
                normalized[key] = row.get(key)
        out.append(normalized)
    return out


def load_records(
    month_keys: set[str] | None = None,
    pending_upload_only: bool = False,
    limit: int | None = None,
) -> list[dict]:
    client = _get_supabase()
    if client is not None:
        try:
            query = (
                client.table(settings.SUPABASE_ORDERS_TABLE)
                .select(",".join(STRICT_COLUMNS_READ))
            )
            if month_keys:
                query = query.in_("date_batch", sorted(month_keys))
            if pending_upload_only:
                query = query.eq("uploaded_to_patient_db", False)
            if limit and limit > 0:
                query = query.limit(limit)
            response = query.execute()
            rows = getattr(response, "data", None) or []
            return _rows_from_supabase_payload(rows)
        except Exception as e:
            console.log(f"[yellow]Supabase read failed ({e}); using local fallback[/yellow]")

    if limit and limit > 0:
        return _load_local_records()[:limit]
    return _load_local_records()


def mark_uploaded_to_patient_db(
    *,
    client_id: int,
    document_id: str,
    upload_run_id: str | None = None,
) -> bool:
    client = _get_supabase()
    if client is None:
        return False
    try:
        (
            client.table(settings.SUPABASE_ORDERS_TABLE)
            .update(
                {
                    "upload_status": "uploaded",
                    "uploaded_to_patient_db": True,
                    "uploaded_at": datetime.now(timezone.utc).isoformat(),
                    "upload_run_id": upload_run_id,
                    "upload_error": None,
                }
            )
            .eq("client_id", _to_int(client_id, 0))
            .eq("document_id", _clean_value(document_id))
            .execute()
        )
        return True
    except Exception as e:
        console.log(f"[yellow]Could not mark uploaded in Supabase: {e}[/yellow]")
        return False


def mark_upload_failed(
    *,
    client_id: int,
    document_id: str,
    error: str,
    upload_run_id: str | None = None,
) -> bool:
    client = _get_supabase()
    if client is None:
        return False
    try:
        (
            client.rpc(
                "increment_upload_attempt",
                {
                    "p_client_id": _to_int(client_id, 0),
                    "p_document_id": _clean_value(document_id),
                    "p_upload_error": str(error or "")[:1000],
                    "p_upload_run_id": upload_run_id,
                },
            ).execute()
        )
        return True
    except Exception:
        # fallback when RPC does not exist yet
        try:
            (
                client.table(settings.SUPABASE_ORDERS_TABLE)
                .update(
                    {
                        "upload_status": "failed",
                        "uploaded_to_patient_db": False,
                        "upload_error": str(error or "")[:1000],
                        "upload_run_id": upload_run_id,
                    }
                )
                .eq("client_id", _to_int(client_id, 0))
                .eq("document_id", _clean_value(document_id))
                .execute()
            )
            return True
        except Exception as e:
            console.log(f"[yellow]Could not mark upload failure in Supabase: {e}[/yellow]")
            return False


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

    if _upsert_supabase_records(stamped_records):
        console.log(f"[cyan]  Saved {len(records)} records to Supabase[/cyan]")
        return

    col = _get_mongo()
    if col is not None:
        for record in stamped_records:
            col.update_one(
                {"storage_uid": record["storage_uid"]},
                {"$set": record},
                upsert=True,
            )
        console.log(f"[cyan]  Saved {len(records)} records to MongoDB[/cyan]")
    else:
        _upsert_local_records(stamped_records)
        console.log(f"[cyan]  Saved {len(records)} records to local JSON[/cyan]")
