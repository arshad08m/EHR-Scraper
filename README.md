# Kantime WorldView Document Scraper

Scrapes order metadata and clinical documents from Kantime's WorldView UI,
stores each captured document as a file under data/pdfs, extracts structured metadata from each popup,
and stores everything in MongoDB Atlas or local JSON with canonical per-document uniqueness.

---

## Project Structure

```
kantime_scraper/
│
├── main.py                    ← Entry point (run this)
├── inspector.py               ← Run FIRST to discover selector IDs
├── requirements.txt
├── .gitignore
├── README.md
│
├── config/
│   ├── settings.py            ← All config (reads from .env)
│   └── .env                   ← YOUR credentials (git-ignored)
│
├── core/
│   ├── auth.py                ← Login, session keep-alive, auto re-login
│   ├── scraper.py             ← Filters, pagination, row extraction
│   └── document_handler.py   ← Popup capture + file save + metadata extraction
│
├── utils/
│   ├── checkpoint.py          ← Page-level resume on any crash
│   ├── storage.py             ← Canonical storage_uid upsert (local JSON + MongoDB)
│   └── enrichment.py         ← Join orders with master Excel (patients + NPIs)
│
├── data/                      ← Auto-created at runtime (git-ignored)
│   ├── orders_output.json     ← Output records with document_file_path
│   ├── checkpoint.json        ← Resume state + failed orders
│   └── pdfs/                  ← Saved PDFs/screenshots per document identity
├── logs/                      ← Auto-created at runtime (git-ignored)
└── docs/
    └── MASTER_PROMPT.md       ← Paste into new Claude sessions to restore context
```

---

## Quick Start (Mac)

```bash
# 1. Virtual environment
python3 -m venv venv && source venv/bin/activate

# 2. Dependencies
pip install -r requirements.txt
playwright install chromium

# 3. Credentials
cp config/.env.example config/.env
# Edit config/.env — add KANTIME_USERNAME + KANTIME_PASSWORD

# 4. Run inspector FIRST (finds your tenant's exact selector IDs)
python3 inspector.py
# → Saves logs/inspector_report.txt
# → Share this file to confirm selectors before live run

# 5. Scrape March 2026
python3 main.py --month 2026-03

# 6. Resume after any crash (auto-picks up from last completed page)
python3 main.py --month 2026-03

# 7. Optional test mode: process only N docs
MAX_DOCS=5 python3 main.py --month 2026-03

# 8. Optional feature flag (default false)
# When false, current execution remains unchanged.
ENABLE_DATA_EXTRACTION=false python3 main.py --month 2026-03

# 9. Optional Ollama vision extraction (uses local model)
# Requires BOTH flags = true (no regex fallback in this mode)
ENABLE_DATA_EXTRACTION=true OLLAMA_ENABLED=true python3 main.py --month 2026-03
```

## Ollama Vision Extraction (Optional)

When both flags are enabled:
- `ENABLE_DATA_EXTRACTION=true`
- `OLLAMA_ENABLED=true`

the scraper extracts metadata using local Ollama vision from saved document files.
In this mode, extraction is **ollama-only** (no regex fallback).

Recommended default model:
- `qwen2.5vl:7b`

Setup:

```bash
# Install and start Ollama, then pull model
ollama pull qwen2.5vl:7b
ollama serve

# Run scraper with Ollama extraction enabled
ENABLE_DATA_EXTRACTION=true OLLAMA_ENABLED=true python3 main.py --month 2026-03
```

Config flags (in `config/.env`):
- `OLLAMA_ENABLED` (default: false)
- `OLLAMA_MODEL` (default: qwen2.5vl:7b)
- `OLLAMA_URL` (default: http://localhost:11434)
- `OLLAMA_TIMEOUT_SECONDS` (default: 90)
- `OLLAMA_MAX_PAGES` (default: 2)

---

## Popup behaviour (confirmed)

When "View" is clicked on any order row:
- A **new Mac browser window** opens (kantimehealth.net)
- The physician order form is rendered as a **scanned image**
- Primary attempt is **Cmd+S** (Mac save shortcut) to save PDF
- If no new PDF appears, fallback sequence is used:
  S2 fetch rendered doc src → S3 fetch direct document endpoint → S4 screenshot
- Saved files are written to **data/pdfs/** with client-document-led names (for example `cdoc-123456.pdf`)

---

## Output schema (per record)

```json
{
  "order_number":                 "P-24076",
  "received_date":                "2026-01-14",
  "received_time":                "12:11 PM",
  "client_name":                  "NEWELL, KATHERINE",
  "location":                     "Buda",
  "status":                       "Unmapped",
  "reviewed":                     "Yes",
  "doc_type":                     "client_document",
  "date_batch":                   "2026-01",
  "storage_uid":                  "cdoc:123456",

  "document_file_path":           "data/pdfs/cdoc-123456.pdf",
  "document_url":                 "https://...",

  "mrn_from_doc":                 "8981",
  "dob_from_doc":                 "07/02/1949",
  "patient_name_from_doc":        "Newell, Katherine",
  "npi_from_doc":                 "1881610152",
  "physician_name_from_doc":      "WARREN ALBRECHT MD",
  "order_number_from_doc":        "P-24076",
  "order_date_from_doc":          "01/09/2026",
  "primary_diagnosis_from_doc":   "I10 - Essential (primary) hypertension",
  "certification_period_from_doc":"01/06/2026-03/06/2026",
  "payer_source_from_doc":        "BCBS MEDICARE (EP)",

  "patient_id":                   "MRN-008981",
  "physician_npi":                "1881610152",
  "patient_match":                true,
  "physician_match":              true,
  "scraped_at":                   "2026-03-28T10:00:00+00:00"
}
```

---

## Switching to MongoDB Atlas

```bash
# Add to config/.env:
MONGO_CONNECTION_STRING=mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/

# No other changes needed — storage.py auto-switches and uses canonical `storage_uid` upserts.

# Import existing JSON output:
mongoimport --uri "$MONGO_CONNECTION_STRING" \
  --db kantime_ehr --collection orders \
  --jsonArray --file data/orders_output.json

# Legacy JSONL import (older runs):
# mongoimport --uri "$MONGO_CONNECTION_STRING" \
#   --db kantime_ehr --collection orders \
#   --file data/orders_output.jsonl
```

---

## Enrichment join

```bash
python3 -m utils.enrichment \
  --orders data/orders_output.json \
  --master /path/to/master_patients_physicians.xlsx \
  --output data/orders_enriched.json
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| Login fails | Check config/.env; run inspector.py to see login form IDs |
| Display button not found | Run inspector.py, update `_find_button()` in core/scraper.py |
| No PDF saved after Cmd+S | This is expected on some rows; scraper auto-falls back to S3 endpoint fetch |
| Only last PDF remains in data/pdfs | Fixed in current implementation; rerun with fresh checkpoint/output reset |
| How to toggle extraction flag | Set ENABLE_DATA_EXTRACTION=true/false in config/.env (false keeps current execution) |
| Ollama extraction returns empty metadata | Verify `ollama serve` is running, model is pulled, and both flags are true |
| Rate limited | Increase REQUEST_DELAY_SECONDS in config/.env (try 3.0) |
| Session expires | Handled automatically — script re-logs in |
| Only Mapped orders exported | Confirm Status dropdown set to ALL in `_apply_filters()` |
# EHR-Scraper
