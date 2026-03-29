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
    tmp_path = _CP_FILE.with_suffix(".tmp")
    with open(tmp_path, "w") as f:
        json.dump(data, f, indent=2)
        f.flush()
    tmp_path.replace(_CP_FILE)


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
        "active_page": None,
        "processed_row_uids": [],
        "processed_rows_in_active_page": 0,
        "updated_at":  datetime.now(timezone.utc).isoformat(),
    }
    _save(data)


def update_partial_progress(
    month: str,
    active_page: int,
    total_pages: int,
    rows_saved: int,
    processed_row_uids: list[str],
):
    """
    Save row-level progress for a partially processed page.
    """
    data = _load()
    unique_uids = list(dict.fromkeys(processed_row_uids or []))
    data["in_progress"] = {
        "month":       month,
        "last_page":   max(0, active_page - 1),
        "active_page": active_page,
        "processed_row_uids": unique_uids,
        "processed_rows_in_active_page": len(unique_uids),
        "total_pages": total_pages,
        "rows_saved":  rows_saved,
        "updated_at":  datetime.now(timezone.utc).isoformat(),
    }
    _save(data)


def get_resume_state(month: str) -> dict:
    """
    Return resume page and any row-level progress on that page.
    """
    data = _load()
    ip = data.get("in_progress")
    if not ip or ip.get("month") != month:
        return {
            "page": 1,
            "rows_saved": 0,
            "active_page": None,
            "processed_row_uids": [],
            "total_pages": 0,
        }

    last_page = int(ip.get("last_page", 0) or 0)
    total_pages = int(ip.get("total_pages", 0) or 0)
    rows_saved = int(ip.get("rows_saved", 0) or 0)
    active_page = int(ip.get("active_page", 0) or 0)
    processed_row_uids = ip.get("processed_row_uids") or []
    if not isinstance(processed_row_uids, list):
        processed_row_uids = []

    if last_page < 0:
        console.log(f"[yellow]↩ Invalid checkpoint page ({last_page}); restarting {month} from page 1[/yellow]")
        return {
            "page": 1,
            "rows_saved": rows_saved,
            "active_page": None,
            "processed_row_uids": [],
            "total_pages": total_pages,
        }

    if total_pages > 0 and last_page > total_pages:
        console.log(
            f"[yellow]↩ Invalid checkpoint range ({last_page}>{total_pages}); restarting {month} from page 1[/yellow]"
        )
        return {
            "page": 1,
            "rows_saved": rows_saved,
            "active_page": None,
            "processed_row_uids": [],
            "total_pages": total_pages,
        }

    if active_page > 0:
        if total_pages > 0 and active_page > total_pages:
            console.log(
                f"[yellow]↩ Invalid active page ({active_page}>{total_pages}); resuming from page {max(1, last_page + 1)}[/yellow]"
            )
        else:
            console.log(
                f"[yellow]↩ Resuming {month} from page {active_page} "
                f"(partial: {len(processed_row_uids)} rows already done)[/yellow]"
            )
            return {
                "page": active_page,
                "rows_saved": rows_saved,
                "active_page": active_page,
                "processed_row_uids": processed_row_uids,
                "total_pages": total_pages,
            }

    next_page = max(1, last_page + 1)
    if total_pages > 0:
        next_page = min(next_page, total_pages)

    console.log(f"[yellow]↩ Resuming {month} from page {next_page}[/yellow]")
    return {
        "page": next_page,
        "rows_saved": rows_saved,
        "active_page": None,
        "processed_row_uids": [],
        "total_pages": total_pages,
    }


def get_resume_page(month: str) -> int:
    return int(get_resume_state(month).get("page", 1) or 1)


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
