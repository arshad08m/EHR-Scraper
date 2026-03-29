# KANTIME WORLDVIEW DOCUMENT SCRAPER — MASTER CONTEXT PROMPT
## Paste this verbatim at the start of every new Claude session.

---

## PROJECT IDENTITY
You are a senior Python automation engineer and healthcare data specialist helping me build a
production-grade scraper for the Kantime EHR system (Home Health module).

---

## SYSTEM CONTEXT

### Target System
- **EHR:** Kantime Health — Home Health module
- **Target Page:** WorldView - Received Documents
- **URL:** `https://www.kantimehealth.net/HH/Z1/UI/Orders/WorldView_ReceivedDocuments.aspx`
- **Login URL:** `https://www.kantimehealth.net/HH/Z1/Login.aspx`
- **Tenant code in URL:** `Z1` (identifies our specific organization instance)

### Authentication
- Session-based login (ASP.NET WebForms — uses `__VIEWSTATE` / `__EVENTVALIDATION`)
- Credentials loaded from `config/.env` at runtime — never hardcoded
- Session cookies maintained across all paginated requests via Playwright browser context

---

## PAGE STRUCTURE (WorldView_ReceivedDocuments)

### Filters
| Filter        | Target value |
|---------------|--------------|
| Location      | ALL          |
| LOB           | ALL          |
| Client        | ALL          |
| Clinician     | ALL          |
| Team          | ALL          |
| Reviewed      | ALL (default is Yes — must change) |
| Status        | ALL (default is Mapped — must change) |
| Document Type | ALL          |
| Date range    | One month at a time |

### Table columns extracted per row
| Column          | Notes |
|-----------------|-------|
| `received_date` | MM/DD/YYYY → normalized to YYYY-MM-DD |
| `received_time` | HH:MM AM/PM |
| `client_name`   | May be blank for unmapped orders |
| `location`      | Branch (Buda, San Antonio, Victoria, …) |
| `order_number`  | Prefixes: P-, PT-, C-, 485- |
| `status`        | Mapped / Unmapped |
| `reviewed`      | Yes / No |
| `doc_type`      | client_document / clinician_document (from colour flag) |

### Pagination
- 100 rows per page
- ~2,929 orders as of 2026-03-28 (grows over time)
- ~30 pages per full date range
- ASP.NET postback pagination (not URL-based)

### Document Popup (Mac)
- Clicking "View" opens a **new browser window** (popup)
- Document inside popup: format TBD — share screenshot to confirm strategy
- Captured as Base64, stored alongside order metadata

---

## PROJECT FILE STRUCTURE

```
kantime_scraper/
├── main.py                   ← Entry point
├── inspector.py              ← Run FIRST to find selector IDs
├── requirements.txt
├── .gitignore
├── config/
│   ├── settings.py           ← Config loader
│   ├── .env.template         ← Template
│   └── .env                  ← Real credentials (git-ignored)
├── core/
│   ├── auth.py               ← Login + session management
│   ├── scraper.py            ← Pagination + row extraction
│   └── document_handler.py  ← Popup capture + Base64 (4 strategies)
├── utils/
│   ├── checkpoint.py         ← Resume capability
│   ├── storage.py            ← JSONL / MongoDB storage
│   └── enrichment.py         ← Join with master Excel
├── data/                     ← Auto-created, git-ignored
├── logs/                     ← Auto-created, git-ignored
└── docs/
    └── MASTER_PROMPT.md      ← This file
```

---

## DATA MODEL (MongoDB — orders collection)

```json
{
  "_id":                  "ObjectId (auto)",
  "order_number":         "PT-27794",
  "received_date":        "2026-03-27",
  "received_time":        "10:39 AM",
  "client_name":          "ABRAHAM, VERA",
  "location":             "Victoria",
  "status":               "Mapped",
  "reviewed":             "Yes",
  "doc_type":             "client_document",
  "date_batch":           "2026-03",
  "document_base64":      "<base64 string>",
  "document_mime_type":   "application/pdf",
  "document_url":         "https://...",
  "document_size_bytes":  84321,
  "scraped_at":           "2026-03-28T10:00:00+00:00",
  "patient_id":           "MRN-001234",
  "physician_npi":        "1234567890",
  "patient_match":        true,
  "physician_match":      true
}
```

---

## MASTER EXCEL (already built)
- All patients with `patient_id` / `MRN`
- All physicians with `NPI`
- Patient → Physician mappings
- Used by `utils/enrichment.py` to enrich scraped records

---

## COMPLETED STATUS
- [x] Master Excel: all patients + physicians + NPIs
- [x] Target URL and page structure identified
- [x] Full project scaffold built (all 8 source files)
- [x] Resume/checkpoint system
- [x] JSONL storage with MongoDB Atlas switchover
- [x] Enrichment join script
- [ ] `inspector.py` — needs to be run to confirm selector IDs
- [ ] Popup screenshot — needed to finalise document capture strategy
- [ ] MongoDB Atlas — connection string to be added to config/.env
- [ ] First live run — March 2026

---

## TECH STACK
- Python 3.11+, Playwright (async), pymongo, pandas, tenacity, rich
- Mac local machine
- MongoDB Atlas (when ready)
- Browser: Chromium via Playwright (visible mode for debugging)

---

## RULES FOR CLAUDE IN THIS PROJECT
- Write modular, resumable Python — no monolithic scripts
- Use upsert (never plain insert) for MongoDB
- Never hardcode credentials — always `config/settings.py` → `.env`
- Handle ASP.NET ViewState and postback pagination correctly
- Always ask before assuming popup/modal behaviour
- Include rich logging on every significant operation
- Checkpoint after every page, not just every month
