# Kantime WorldView Document Scraper

Scrapes order metadata and clinical documents from Kantime’s WorldView UI, saves each captured document under `data/pdfs/`, optionally extracts structured metadata from the document (regex or Ollama vision), and persists rows to **Supabase (Postgres)**, **MongoDB Atlas**, or **local JSON**—in that order of preference when credentials are set.

---

## Project structure

```
kantime_doc_scraper/
│
├── main.py                      # CLI entry point (scrape months, retry failed)
├── inspector.py                 # Run first on a new tenant to discover selector IDs
├── migrate_json_to_supabase.py  # One-shot upsert from data/orders_output.json → Supabase
├── requirements.txt
├── .gitignore
├── README.md
├── PDF_STORAGE_STRATEGY.md      # Notes for moving PDFs off disk (Supabase Storage)
├── kantime_runtime_flow.svg   # High-level flow diagram (reference)
│
├── config/
│   ├── settings.py              # Loads all settings from config/.env
│   ├── .env.example             # Template — copy to .env
│   └── .env                     # Your secrets (git-ignored)
│
├── core/
│   ├── auth.py                  # Login and session handling
│   ├── scraper.py               # Filters, pagination, GetData API, row capture orchestration
│   └── document_handler.py      # Popup/direct download, PDF save, metadata extraction
│
├── utils/
│   ├── checkpoint.py            # Resume state + failed-order keys (data/checkpoint.json)
│   ├── storage.py               # Supabase / Mongo / local JSON upserts (canonical keys)
│   └── enrichment.py            # Join orders JSON with master Excel (patients + NPIs)
│
├── data/                        # Created at runtime (mostly git-ignored)
│   ├── orders_output.json       # Local fallback output when Supabase/Mongo not configured
│   ├── checkpoint.json          # Scrape resume + failed order identifiers
│   └── pdfs/                    # Downloaded PDFs / screenshots per document
│
├── logs/                        # Runtime logs (e.g. inspector_report.txt)
├── scripts/                     # Ad-hoc / sample assets (e.g. archived JSON, spreadsheets)
└── venv/                        # Local virtualenv (if used; git-ignored)
```

---

## Storage backends (priority)

| Priority | When it is used |
|----------|-----------------|
| **1. Supabase** | `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` are set in `config/.env`. Rows upsert into the table named by `SUPABASE_ORDERS_TABLE` (default `orders_ingest`). Uniqueness is enforced on `(client_id, document_id)`. |
| **2. MongoDB** | `MONGO_CONNECTION_STRING` is set (and Supabase is not configured or unavailable). Upsert by `storage_uid`. |
| **3. Local JSON** | Neither Supabase nor Mongo is configured. Writes to `data/orders_output.json`. |

Checkpointing (`data/checkpoint.json`) is separate from order storage and always local.

---

## Quick start (macOS / Linux)

```bash
# 1. Virtual environment
python3 -m venv venv && source venv/bin/activate   # Windows: venv\Scripts\activate

# 2. Dependencies
pip install -r requirements.txt
playwright install chromium

# 3. Credentials
cp config/.env.example config/.env
# Edit config/.env — at minimum KANTIME_USERNAME and KANTIME_PASSWORD

# 4. (Optional) Supabase — create table in SQL editor to match your schema, then set:
#     SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, SUPABASE_ORDERS_TABLE=orders_ingest

# 5. Run inspector on a new tenant (discovers selector IDs)
python3 inspector.py
# → logs/inspector_report.txt

# 6. Scrape one month
python3 main.py --month 2026-03

# 7. Resume after interrupt — rerun the same command (checkpoint advances)

# 8. Retry only previously failed captures
python3 main.py --retry-failed

# 9. Headless browser
python3 main.py --month 2026-03 --headless

# 10. Test mode: cap number of documents (per month run)
MAX_DOCS=5 python3 main.py --month 2026-03
```

---

## Supabase and migration

1. Configure `config/.env` with `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, and optionally `SUPABASE_ORDERS_TABLE`.
2. Apply your DDL in the Supabase SQL editor (table such as `orders_ingest` with columns for each field you persist; `doc_identity` is often a **generated** column—do not send it in API upserts).
3. Backfill from an existing local export:

```bash
python3 migrate_json_to_supabase.py
```

Reads `data/orders_output.json` and upserts via the same storage layer as the scraper.

See `PDF_STORAGE_STRATEGY.md` for a future path to store PDF binaries in Supabase Storage instead of only local `data/pdfs/`.

### `orders_ingest` table schema (Supabase / Postgres)

The scraper reads and writes this table when `SUPABASE_*` is configured. The physical table name comes from **`SUPABASE_ORDERS_TABLE`** in `config/.env` (default **`orders_ingest`**).

| Concept | Details |
|--------|---------|
| **Business key** | One row per **`(client_id, document_id)`**. The app upserts with `on_conflict=client_id,document_id`. Re-runs update the same row; they do not delete history. |
| **Stable ID** | **`storage_uid`** — canonical string id for the document (also used by Mongo/local JSON). Unique in Postgres. |
| **`doc_identity`** | Often defined as a **generated** column in Postgres (e.g. `lower(client_id::text \|\| '::' \|\| document_id)`). **Do not** send this field in insert/upsert from clients; the DB computes it. The Python client excludes it from writes (see `STRICT_COLUMNS_WRITE` in `utils/storage.py`). |
| **Upload pipeline** | Use **`uploaded_to_patient_db`**, **`upload_status`**, and related columns so downstream jobs skip already-uploaded documents and retry failures safely. |

#### Columns (authoritative list)

Types below are the **intended Postgres types** for backends implementing or validating the table. The scraper maps values in `utils/storage.py` (`STRICT_COLUMNS_READ` / `STRICT_COLUMNS_WRITE`). Integer fields may be **`0`** when unknown (not nullable in app for some keys).

| Column | Type | Notes |
|--------|------|--------|
| `client_id` | `bigint` | Kantime client; part of primary key |
| `document_id` | `text` | Logical document id (e.g. `cdoc:123`, `edoc:…`); part of primary key |
| `doc_identity` | `text` | **Generated** (optional); do not write from API |
| `storage_uid` | `text` | Unique canonical uid |
| `file_id` | `text` | Filename stem used for capture |
| `date_batch` | `text` | Scrape batch, e.g. `2026-03` |
| `scraped_at` | `timestamptz` | ISO timestamp when row was saved |
| `source` | `text` | e.g. `getdata_api` |
| `row_dom_id` | `text` | DOM row id when scraped from table UI |
| `worldview_id` | `bigint` | |
| `order_id` | `bigint` | |
| `change_order_type` | `bigint` | |
| `reference_id` | `bigint` | |
| `is_echart_order_format` | `boolean` | |
| `cg_task_id` | `bigint` | |
| `admit_no` | `bigint` | |
| `intake_id` | `bigint` | |
| `episode_id` | `bigint` | |
| `payer_id` | `bigint` | |
| `library_form_id` | `bigint` | |
| `order_type` | `text` | |
| `initial_order_in_regular_format` | `boolean` | |
| `requested_by` | `boolean` | |
| `form_485_id` | `bigint` | |
| `poc` | `bigint` | |
| `doc_reference_id` | `bigint` | |
| `client_document_id` | `bigint` | |
| `employee_document_id` | `bigint` | |
| `employee_id` | `bigint` | |
| `staff_type` | `bigint` | |
| `query_485_string` | `text` | |
| `received_date` | `text` | ISO date string from API |
| `received_time` | `text` | |
| `client_name` | `text` | |
| `location` | `text` | |
| `order_number` | `text` | |
| `status` | `text` | e.g. Mapped / Unmapped |
| `reviewed` | `text` | |
| `doc_type` | `text` | e.g. `client_document` |
| `has_view` | `boolean` | |
| `document_popup_url` | `text` | Viewer URL for retry |
| `document_path` | `text` | Server path from API |
| `document_url` | `text` | Resolved URL used during capture |
| `document_file_path` | `text` | Local path under `data/pdfs/` |
| `document_storage_url` | `text` | Reserved for Supabase Storage / CDN URL |
| `document_base64` | `text` | Rarely populated |
| `document_mime_type` | `text` | |
| `document_size_bytes` | `bigint` | |
| `document_filename` | `text` | |
| `mrn_from_doc` | `text` | Extracted metadata |
| `dob_from_doc` | `text` | |
| `patient_name_from_doc` | `text` | |
| `npi_from_doc` | `text` | |
| `physician_name_from_doc` | `text` | |
| `order_number_from_doc` | `text` | |
| `order_date_from_doc` | `text` | |
| `primary_diagnosis_from_doc` | `text` | |
| `certification_period_from_doc` | `text` | |
| `payer_source_from_doc` | `text` | |
| `patient_id` | `text` | From enrichment |
| `physician_npi` | `text` | |
| `patient_match` | `text` | Enrichment match label |
| `physician_match` | `text` | |
| `upload_status` | `text` | e.g. `pending`, `uploaded`, `failed` |
| `uploaded_to_patient_db` | `boolean` | **false** until patient DB confirms |
| `uploaded_at` | `timestamptz` | |
| `upload_attempt_count` | `integer` | |
| `upload_run_id` | `text` | Idempotency / audit for upload jobs |
| `upload_error` | `text` | Last error message |

Optional server-side columns (not sent by every scraper build): **`created_at`**, **`updated_at`** (`timestamptz`), maintained by triggers if you add them.

#### Reference DDL (Postgres)

Use as a starting point; align with your existing Supabase project if the table already exists.

```sql
create table if not exists public.orders_ingest (
  client_id bigint not null,
  document_id text not null,
  doc_identity text generated always as (lower(client_id::text || '::' || document_id)) stored,
  storage_uid text not null,
  file_id text,
  date_batch text,
  scraped_at timestamptz,
  source text,
  row_dom_id text,
  worldview_id bigint,
  order_id bigint,
  change_order_type bigint,
  reference_id bigint,
  is_echart_order_format boolean,
  cg_task_id bigint,
  admit_no bigint,
  intake_id bigint,
  episode_id bigint,
  payer_id bigint,
  library_form_id bigint,
  order_type text,
  initial_order_in_regular_format boolean,
  requested_by boolean,
  form_485_id bigint,
  poc bigint,
  doc_reference_id bigint,
  client_document_id bigint,
  employee_document_id bigint,
  employee_id bigint,
  staff_type bigint,
  query_485_string text,
  received_date text,
  received_time text,
  client_name text,
  location text,
  order_number text,
  status text,
  reviewed text,
  doc_type text,
  has_view boolean,
  document_popup_url text,
  document_path text,
  document_url text,
  document_file_path text,
  document_storage_url text,
  document_base64 text,
  document_mime_type text,
  document_size_bytes bigint,
  document_filename text,
  mrn_from_doc text,
  dob_from_doc text,
  patient_name_from_doc text,
  npi_from_doc text,
  physician_name_from_doc text,
  order_number_from_doc text,
  order_date_from_doc text,
  primary_diagnosis_from_doc text,
  certification_period_from_doc text,
  payer_source_from_doc text,
  patient_id text,
  physician_npi text,
  patient_match text,
  physician_match text,
  upload_status text not null default 'pending',
  uploaded_to_patient_db boolean not null default false,
  uploaded_at timestamptz,
  upload_attempt_count integer not null default 0,
  upload_run_id text,
  upload_error text,
  primary key (client_id, document_id)
);

create unique index if not exists orders_ingest_storage_uid_uidx
  on public.orders_ingest (storage_uid);
```

---

## Ollama vision extraction (optional)

When **both** are enabled in `config/.env`:

- `ENABLE_DATA_EXTRACTION=true`
- `OLLAMA_ENABLED=true`

metadata is extracted with a local Ollama vision model from the saved document file (ollama-only path in that mode).

```bash
ollama pull qwen2.5vl:7b
ollama serve
ENABLE_DATA_EXTRACTION=true OLLAMA_ENABLED=true python3 main.py --month 2026-03
```

Relevant variables: `OLLAMA_MODEL`, `OLLAMA_URL`, `OLLAMA_TIMEOUT_SECONDS`, `OLLAMA_MAX_PAGES`.

---

## Popup capture behaviour (summary)

- “View” opens a document viewer; the scraper tries direct PDF download endpoints, then rendered asset fetch, then screenshot fallback.
- Files are written under **`data/pdfs/`** with names such as `cdoc-<id>.pdf`.

---

## Example record shape (conceptual)

The full column list and types are in the **`orders_ingest` table schema** subsection under [Supabase and migration](#supabase-and-migration). Each row combines API fields, capture paths, optional OCR/enrichment fields, and upload-tracking columns for your patient-DB pipeline.

---

## MongoDB Atlas (alternative to Supabase)

```bash
# In config/.env (leave Supabase blank if you want Mongo):
MONGO_CONNECTION_STRING=mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/
```

The app upserts by `storage_uid`. You can import a JSON array export if needed:

```bash
mongoimport --uri "$MONGO_CONNECTION_STRING" \
  --db kantime_ehr --collection orders \
  --jsonArray --file data/orders_output.json
```

---

## Enrichment (Excel join)

```bash
python3 -m utils.enrichment \
  --orders data/orders_output.json \
  --master /path/to/master_patients_physicians.xlsx \
  --output data/orders_enriched.json
```

---

## Troubleshooting

| Problem | What to try |
|--------|-------------|
| Login fails | Verify `config/.env`; run `inspector.py` |
| Display / Next button not found | Run `inspector.py`, adjust selectors in `core/scraper.py` if your tenant differs |
| No PDF saved | Normal for some rows; fallbacks run automatically |
| Supabase upsert errors on `doc_identity` | Ensure the client does not send values for generated columns; see current `utils/storage.py` |
| Ollama returns empty metadata | Confirm `ollama serve`, model pulled, both extraction flags true |
| Rate limiting | Increase `REQUEST_DELAY_SECONDS` in `config/.env` |
| Session expired | Scraper attempts re-login when opening WorldView |

---

## Environment reference

Copy `config/.env.example` to `config/.env` and set:

- **Kantime:** `KANTIME_USERNAME`, `KANTIME_PASSWORD`, optional URL overrides
- **Storage:** `SUPABASE_*` and/or `MONGO_*`
- **Scraper:** `REQUEST_DELAY_SECONDS`, `MAX_RETRIES`, `MAX_DOCS`, `PAGE_SIZE`, `CAPTURE_CONCURRENCY`, `DIRECT_DOWNLOAD_ONLY`, `ENABLE_DATA_EXTRACTION`
- **Ollama:** `OLLAMA_*` variables as in `.env.example`
