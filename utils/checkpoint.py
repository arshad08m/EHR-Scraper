"""
utils/checkpoint.py — tracks progress so any crash is fully resumable.

Checkpoint file (data/checkpoint.json) structure:
{
  "completed_months": ["2026-01", "2026-02"],
  "in_progress": {
    "month":       "2026-03",
    "last_page":   4,
    "total_pages": 30,
    "rows_saved":  387,
    "updated_at":  "2026-03-28T10:00:00"
  },
  "failed_orders": ["P-24076", "C-25963"]
}
"""

import json
from datetime import datetime, timezone
from rich.console import Console

from config.settings import settings

console  = Console()
_CP_FILE = settings.CHECKPOINT_FILE


def _load() -> dict:
    if _CP_FILE.exists():
        try:
            with open(_CP_FILE) as f:
                content = f.read().strip()
                if not content:  # Empty file
                    return {"completed_months": [], "in_progress": None, "failed_orders": []}
                return json.loads(content)
        except (json.JSONDecodeError, IOError):
            return {"completed_months": [], "in_progress": None, "failed_orders": []}
    return {"completed_months": [], "in_progress": None, "failed_orders": []}


def _save(data: dict):
    _CP_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(_CP_FILE, "w") as f:
        json.dump(data, f, indent=2)


def is_month_done(month: str) -> bool:
    return month in _load().get("completed_months", [])


def mark_month_done(month: str):
    data = _load()
    if month not in data["completed_months"]:
        data["completed_months"].append(month)
    data["in_progress"] = None
    _save(data)
    console.log(f"[green]✓ Month {month} marked complete[/green]")


def update_progress(month: str, page: int, total_pages: int, rows_saved: int):
    data = _load()
    data["in_progress"] = {
        "month":       month,
        "last_page":   page,
        "total_pages": total_pages,
        "rows_saved":  rows_saved,
        "updated_at":  datetime.now(timezone.utc).isoformat(),
    }
    _save(data)


def get_resume_page(month: str) -> int:
    data = _load()
    ip   = data.get("in_progress")
    if ip and ip.get("month") == month:
        page = ip.get("last_page", 1)
        console.log(f"[yellow]↩ Resuming {month} from page {page}[/yellow]")
        return page
    return 1


def mark_order_failed(order_number: str):
    data = _load()
    if order_number not in data["failed_orders"]:
        data["failed_orders"].append(order_number)
    _save(data)


def get_failed_orders() -> list:
    return _load().get("failed_orders", [])


def clear_failed(order_number: str):
    data = _load()
    data["failed_orders"] = [o for o in data["failed_orders"] if o != order_number]
    _save(data)
