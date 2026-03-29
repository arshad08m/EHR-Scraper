"""
core/scraper.py — filter application, pagination, row extraction.

ASP.NET WebForms note:
  Pagination uses __doPostBack() server-side postbacks, not URL changes.
  We click the Next (>) button and wait for networkidle after each page.
"""

import asyncio
import re
from datetime import datetime
from playwright.async_api import Page, BrowserContext
from rich.console import Console

from config.settings import settings
from core.auth import ensure_logged_in
from core.document_handler import capture_document
from utils.checkpoint import (
    is_month_done, mark_month_done, update_progress,
    get_resume_page, mark_order_failed,
)
from utils.storage import save_batch

console = Console()


# ── Public entry point ────────────────────────────────────────────────────────

async def scrape_month(context: BrowserContext, year: int, month: int):
    month_key            = f"{year}-{month:02d}"
    start_date, end_date = _month_range(year, month)

    if is_month_done(month_key):
        console.log(f"[dim]Month {month_key} already complete — skipping.[/dim]")
        return

    console.rule(f"[bold cyan]Scraping {month_key}  ({start_date} → {end_date})[/bold cyan]")

    page = await context.new_page()
    try:
        await page.goto(settings.WORLDVIEW_URL, wait_until="networkidle", timeout=30_000)
        await ensure_logged_in(page)
        await _apply_filters(page, start_date, end_date)

        total_pages = await _get_total_pages(page)
        resume_from = get_resume_page(month_key)
        console.log(f"Pages: {total_pages}  |  Resuming from: {resume_from}")

        rows_saved = 0
        stopped_by_max_docs = False

        for page_num in range(1, total_pages + 1):

            # Fast-forward pages already done (resume case)
            if page_num < resume_from:
                await _goto_next_page(page)
                await asyncio.sleep(0.3)
                continue

            await ensure_logged_in(page)
            rows = await _extract_rows(page)
            console.log(f"  Page {page_num}/{total_pages} — {len(rows)} rows")

            enriched = []
            for idx, row in enumerate(rows):
                order_num = row.get("order_number", f"row_{idx}")
                console.log(f"    [{idx+1}/{len(rows)}] {order_num}")
                try:
                    doc_data = await _capture_document(context, page, idx, order_num)
                    row.update(doc_data)
                except Exception as e:
                    console.log(f"    [red]Doc capture failed ({order_num}): {e}[/red]")
                    mark_order_failed(order_num)
                    row.update({"document_file_path": None})

                row["date_batch"] = month_key
                enriched.append(row)
                await asyncio.sleep(settings.REQUEST_DELAY)
                
                # Check if we've hit the MAX_DOCS limit (testing mode)
                total_processed = rows_saved + len(enriched)
                if settings.MAX_DOCS > 0 and total_processed >= settings.MAX_DOCS:
                    console.log(f"[yellow]Reached MAX_DOCS={settings.MAX_DOCS} (testing limit)[/yellow]")
                    break

            save_batch(enriched)
            rows_saved += len(enriched)
            update_progress(month_key, page_num, total_pages, rows_saved)
            
            # Break outer loop if we've hit MAX_DOCS
            if settings.MAX_DOCS > 0 and rows_saved >= settings.MAX_DOCS:
                console.log(f"[yellow]✓ Test complete: {rows_saved} docs scraped[/yellow]")
                stopped_by_max_docs = True
                break

            if page_num < total_pages:
                await _goto_next_page(page)
                await asyncio.sleep(settings.REQUEST_DELAY)

        if stopped_by_max_docs:
            console.log(f"[yellow]Test mode stopped early at {rows_saved} rows — month not marked complete.[/yellow]")
        else:
            mark_month_done(month_key)
            console.log(f"[bold green]✓ {month_key} complete — {rows_saved} rows saved.[/bold green]")

    finally:
        await page.close()


# ── Filters ───────────────────────────────────────────────────────────────────

async def _apply_filters(page: Page, start_date: str, end_date: str):
    console.log(f"Applying filters: ALL statuses, {start_date} → {end_date}")

    await _safe_select(page, [
        "#MainContent_ddl_status",
        "#ddlStatus",
        "select[id*='ddl_status']",
        "select[id*='Status']",
        "select[id*='status']",
    ], "0")
    await _safe_select(page, [
        "#MainContent_ddl_Review",
        "#ddlReviewed",
        "select[id*='ddl_Review']",
        "select[id*='Review']",
        "select[id*='review']",
    ], "0")

    cb = page.locator("#chk_RcvdDate").first
    if await cb.count() > 0 and not await cb.is_checked():
        await cb.check()

    await _safe_fill(page, [
        "#MainContent_txtRcvdDateFrom",
        "#txtFromDate",
        "input[id*='RcvdDateFrom']",
        "input[id*='FromDate']",
    ], start_date)
    await _safe_fill(page, [
        "#MainContent_txtRcvdDateTo",
        "#txtToDate",
        "input[id*='RcvdDateTo']",
        "input[id*='ToDate']",
    ], end_date)

    display = await _find_button(page, [
        "MainContent_btn_display",
        "btn_display",
        "Display",
        "display",
        "btnDisplay",
        "Search",
    ])
    if not display:
        raise RuntimeError("Display button not found. Run inspector.py to find the correct ID.")

    try:
        async with page.expect_response(
            lambda r: "WorldView_ReceivedDocuments.aspx/GetData" in r.url and r.request.method == "POST",
            timeout=20_000,
        ):
            await display.click()
    except Exception:
        await display.click()
    await page.wait_for_load_state("networkidle", timeout=20_000)
    console.log("[green]✓ Filters applied[/green]")


# ── Row extraction ────────────────────────────────────────────────────────────

async def _extract_rows(page: Page) -> list[dict]:
    rows_data = await page.evaluate("""
    () => {
        const rows = Array.from(document.querySelectorAll('#tableData tr')).filter(tr =>
            tr.querySelectorAll('td').length >= 10
        );
        return rows.map(tr => {
            const cells = Array.from(tr.querySelectorAll('td'));
            // Expected column order (0-indexed):
            // 0=checkbox, 1=date, 2=time, 3=colour-flag, 4=client/clinician,
            // 5=location, 6=order#, 7=status, 8=reviewed, 9=view
            const colorCell = cells[3];
            let docType = 'unknown';
            if (colorCell) {
                const bg = window.getComputedStyle(colorCell).backgroundColor || colorCell.style.backgroundColor || '';
                docType = (bg.includes('182, 255, 0') || bg.includes('0, 128') || bg.includes('green'))
                          ? 'client_document' : 'clinician_document';
            }
            return {
                received_date:  (cells[1]  || {innerText:''}).innerText.trim(),
                received_time:  (cells[2]  || {innerText:''}).innerText.trim(),
                client_name:    (cells[4]  || {innerText:''}).innerText.trim(),
                location:       (cells[5]  || {innerText:''}).innerText.trim(),
                order_number:   (cells[6]  || {innerText:''}).innerText.trim(),
                status:         (cells[7]  || {innerText:''}).innerText.trim(),
                reviewed:       (cells[8]  || {innerText:''}).innerText.trim(),
                doc_type:       docType,
            };
        });
    }
    """)

    for row in rows_data:
        row["received_date"] = _iso_date(row.get("received_date", ""))

    return [r for r in rows_data if r.get("order_number")]


def _iso_date(s: str) -> str:
    try:
        return datetime.strptime(s, "%m/%d/%Y").strftime("%Y-%m-%d")
    except Exception:
        return s


# ── Document capture ──────────────────────────────────────────────────────────

async def _capture_document(context: BrowserContext, page: Page, row_idx: int, order_number: str = None) -> dict:
    view_btns = page.locator(
        "#tableData tr td a:has-text('View'), #tableData tr td input[value='View']"
    )
    count = await view_btns.count()
    if row_idx >= count:
        raise IndexError(f"View button index {row_idx} out of range (total {count})")
    return await capture_document(context, view_btns.nth(row_idx), order_number)


# ── Pagination ────────────────────────────────────────────────────────────────

async def _get_total_pages(page: Page) -> int:
    try:
        txt = await page.locator("text=/Page \\d+ of \\d+/i").first.inner_text(timeout=5_000)
        m   = re.search(r"of\s+(\d+)", txt)
        if m:
            return int(m.group(1))
    except Exception:
        pass
    return 1


async def _goto_next_page(page: Page):
    for sel in ["input[value='>']", "a:has-text('>')", "input[title='Next']",
                "a[title='Next']", ".pager-next", "#btnNext", "#Next", "input#Next"]:
        el = page.locator(sel).first
        if await el.count() > 0:
            try:
                async with page.expect_response(
                    lambda r: "WorldView_ReceivedDocuments.aspx/GetData" in r.url and r.request.method == "POST",
                    timeout=15_000,
                ):
                    await el.click()
            except Exception:
                await el.click()
            await page.wait_for_load_state("networkidle", timeout=15_000)
            return
    raise RuntimeError("Next Page button not found — run inspector.py.")


# ── ASP.NET helper utils ──────────────────────────────────────────────────────

def _month_range(year: int, month: int) -> tuple[str, str]:
    import calendar
    last = calendar.monthrange(year, month)[1]
    return f"{month:02d}/01/{year}", f"{month:02d}/{last:02d}/{year}"


async def _safe_select(page: Page, selectors: list, value: str):
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                await el.select_option(value=value)
                return
        except Exception:
            continue
    console.log(f"  [yellow]Could not set dropdown: {selectors}[/yellow]")


async def _safe_fill(page: Page, selectors: list, value: str):
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                await el.fill(value)
                return
        except Exception:
            continue
    console.log(f"  [yellow]Could not fill field: {selectors}[/yellow]")


async def _find_button(page: Page, labels: list):
    for label in labels:
        for sel in [f"input[value='{label}']", f"button:has-text('{label}')",
                    f"input[id*='{label}']",   f"a:has-text('{label}')",
                    f"#{label}"]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    return el
            except Exception:
                continue
    return None
