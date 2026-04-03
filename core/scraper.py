"""
core/scraper.py — filter application, pagination, row extraction.

ASP.NET WebForms note:
  Pagination uses __doPostBack() server-side postbacks, not URL changes.
  We click the Next (>) button and wait for networkidle after each page.
"""

import asyncio
import hashlib
import json
import re
import signal
from pathlib import Path
from datetime import datetime
from urllib.parse import urljoin, urlparse
from playwright.async_api import Page, BrowserContext, Response
from rich.console import Console

from config.settings import settings
from core.auth import ensure_logged_in
from core.document_handler import (
    capture_document,
    capture_document_from_url,
    capture_document_direct_from_url,
)
from utils.checkpoint import (
    is_month_done, mark_month_done, update_progress,
    update_partial_progress, get_resume_state, mark_order_failed,
    get_failed_orders, clear_failed,
)
from utils.storage import save_batch, load_records

console = Console()
_STOP_REQUESTED = False


def _handle_stop_signal(signum, frame):
    del signum, frame
    global _STOP_REQUESTED
    _STOP_REQUESTED = True


def _install_stop_handler():
    try:
        signal.signal(signal.SIGINT, _handle_stop_signal)
    except Exception:
        # Some environments do not allow replacing signal handlers.
        pass


# ── Public entry point ────────────────────────────────────────────────────────

async def scrape_month(context: BrowserContext, year: int, month: int):
    global _STOP_REQUESTED
    _STOP_REQUESTED = False
    _install_stop_handler()

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
        page_data = await _apply_filters(page, start_date, end_date)
        if not page_data:
            page_data = await _request_getdata_page(page, 1, source="direct-page-1")

        total_pages = (page_data or {}).get("total_pages") or await _get_total_pages(page)
        resume_state = get_resume_state(month_key)
        resume_from = int(resume_state.get("page", 1) or 1)
        resume_active_page = resume_state.get("active_page")
        resume_processed_row_uids = set(resume_state.get("processed_row_uids") or [])
        console.log(f"Pages: {total_pages}  |  Resuming from: {resume_from}")

        rows_saved = int(resume_state.get("rows_saved", 0) or 0)
        run_rows_saved = 0
        stopped_by_max_docs = False
        stopped_by_user = False

        for page_num in range(1, total_pages + 1):

            if _STOP_REQUESTED:
                stopped_by_user = True
                console.log("[yellow]Stop requested (Ctrl+C). Saving progress and stopping gracefully.[/yellow]")
                break

            # Fast-forward pages already done (resume case)
            if page_num < resume_from:
                page_data = await _goto_next_page(page)
                await asyncio.sleep(0.3)
                continue

            await ensure_logged_in(page)
            current_page_data = page_data
            if not current_page_data:
                current_page_data = await _request_getdata_page(page, page_num, source=f"direct-page-{page_num}")

            rows = []
            response_count = None
            if current_page_data and current_page_data.get("rows") is not None:
                rows = current_page_data.get("rows") or []
                response_count = current_page_data.get("response_count")
                api_total_pages = current_page_data.get("total_pages")
                api_total_records = current_page_data.get("total_records")
                if api_total_pages and api_total_pages != total_pages:
                    console.log(
                        f"[yellow]API total_pages changed {total_pages} -> {api_total_pages}; using API value.[/yellow]"
                    )
                    total_pages = api_total_pages
                if api_total_records:
                    console.log(f"[dim]API total_records={api_total_records} on page {page_num}[/dim]")
                console.log(f"[cyan]Using API response for page {page_num}: {len(rows)} rows.[/cyan]")

            if response_count is not None and len(rows) != response_count:
                raise RuntimeError(
                    f"Page {page_num} mapping mismatch: api_count={response_count}, mapped={len(rows)}"
                )

            if not rows:
                console.log(
                    f"[yellow]API rows unavailable for page {page_num}; using DOM diagnostic fallback.[/yellow]"
                )
                rows = await _extract_rows(page, page_num)

            expected_rows = await _expected_rows_on_page(page)
            if expected_rows and response_count is not None and response_count != expected_rows:
                console.log(
                    f"[yellow]Header/API mismatch on page {page_num}: header={expected_rows}, api={response_count}. Proceeding with API count.[/yellow]"
                )

            if response_count is not None and response_count < 100 and page_num < total_pages:
                console.log(
                    f"[yellow]API returned {response_count} rows on non-last page {page_num}. Continuing per API source of truth.[/yellow]"
                )

            page_processed_uids = set()

            rows_to_process = rows
            if page_num == resume_from and resume_active_page == page_num and resume_processed_row_uids:
                page_processed_uids = set(resume_processed_row_uids)
                rows_to_process = [
                    r for r in rows
                    if not any(uid in page_processed_uids for uid in _row_uid_aliases(r))
                ]
                skipped = len(rows) - len(rows_to_process)
                if skipped > 0:
                    console.log(
                        f"[yellow]Resume skip on page {page_num}: {skipped} rows already processed in checkpoint.[/yellow]"
                    )

            if settings.MAX_DOCS > 0:
                remaining = max(0, settings.MAX_DOCS - run_rows_saved)
                rows_to_process = rows_to_process[:remaining]

            total_to_process = len(rows_to_process)
            console.log(f"  Page {page_num}/{total_pages} — {total_to_process} rows to process ({len(rows)} total)")

            enriched = []
            concurrency = max(1, int(settings.CAPTURE_CONCURRENCY or 1))
            semaphore = asyncio.Semaphore(concurrency)

            for batch_start in range(0, total_to_process, concurrency):
                if _STOP_REQUESTED:
                    stopped_by_user = True
                    console.log("[yellow]Stop requested (Ctrl+C). Finishing current batch save...[/yellow]")
                    break

                batch_rows = rows_to_process[batch_start:batch_start + concurrency]
                tasks = []
                for offset, row in enumerate(batch_rows):
                    idx = batch_start + offset
                    order_label = str(row.get("order_number") or "").strip()
                    if not order_label:
                        order_label = f"row_{page_num}_{idx + 1}"
                    file_id = _file_id(row, page_num=page_num, row_idx=idx)
                    console.log(f"    [{idx+1}/{total_to_process}] {order_label}")
                    tasks.append(
                        _capture_row_with_semaphore(
                            semaphore,
                            context,
                            page,
                            row,
                            idx,
                            order_label,
                            file_id,
                            month_key,
                        )
                    )

                batch_results = await asyncio.gather(*tasks, return_exceptions=False)
                for result_row, order_label, failed_key, err in batch_results:
                    if err:
                        console.log(f"    [red]Doc capture failed ({order_label}): {err}[/red]")
                        result_row["document_file_path"] = None

                    if err or not result_row.get("document_file_path"):
                        mark_order_failed(failed_key)

                    enriched.append(result_row)
                    page_processed_uids.add(_row_uid(result_row))

                if settings.REQUEST_DELAY > 0:
                    await asyncio.sleep(settings.REQUEST_DELAY)

            if settings.MAX_DOCS > 0 and run_rows_saved + len(enriched) >= settings.MAX_DOCS:
                console.log(f"[yellow]Reached MAX_DOCS={settings.MAX_DOCS} (testing limit)[/yellow]")

            save_batch(enriched)
            rows_saved += len(enriched)
            run_rows_saved += len(enriched)
            page_fully_processed = len(page_processed_uids) >= len(rows)

            # Checkpoint should only advance on fully processed pages.
            # This prevents partial runs from skipping remaining rows on resume.
            if page_fully_processed:
                update_progress(month_key, page_num, total_pages, rows_saved)
            else:
                update_partial_progress(
                    month_key,
                    page_num,
                    total_pages,
                    rows_saved,
                    sorted(page_processed_uids),
                )
                console.log(
                    f"[yellow]Partial page {page_num}: {len(page_processed_uids)}/{len(rows)} rows checkpointed.[/yellow]"
                )

            if settings.MAX_DOCS > 0:
                console.log("[dim]Checkpoint saved in MAX_DOCS test mode.[/dim]")
            
            # Break outer loop if we've hit MAX_DOCS
            if settings.MAX_DOCS > 0 and run_rows_saved >= settings.MAX_DOCS:
                console.log(f"[yellow]✓ Test complete: {run_rows_saved} docs scraped[/yellow]")
                stopped_by_max_docs = True
                break

            if stopped_by_user:
                break

            if page_num < total_pages:
                page_data = await _goto_next_page(page)
                await asyncio.sleep(settings.REQUEST_DELAY)
            else:
                page_data = None

        if stopped_by_max_docs:
            console.log(f"[yellow]Test mode stopped early at {rows_saved} rows — month not marked complete.[/yellow]")
        elif stopped_by_user:
            console.log(f"[yellow]Stopped by user — progress saved at {rows_saved} rows.[/yellow]")
        else:
            mark_month_done(month_key)
            console.log(f"[bold green]✓ {month_key} complete — {rows_saved} rows saved.[/bold green]")

    finally:
        await page.close()


async def retry_failed_orders(
    context: BrowserContext,
    month_keys: set[str] | None = None,
) -> dict:
    """
    Fast retry path for failed orders only.
    Retries direct download using stored row metadata without paging through UI tables.
    """
    failed = get_failed_orders()
    if not failed:
        console.log("[green]No failed orders to retry.[/green]")
        return {"attempted": 0, "recovered": 0, "remaining": 0}

    records = _load_local_output_records()
    if not records:
        console.log("[yellow]No local output records found for failed-order retry.[/yellow]")
        return {"attempted": 0, "recovered": 0, "remaining": len(failed)}

    failed_set = {str(v).strip() for v in failed if str(v).strip()}
    candidates = []
    for record in records:
        if not isinstance(record, dict):
            continue
        retry_keys = _retry_keys_for_record(record)
        matched_keys = sorted(retry_keys & failed_set)
        if not matched_keys:
            continue
        batch = str(record.get("date_batch") or "").strip()
        if month_keys and batch not in month_keys:
            continue
        candidates.append((record, matched_keys))

    if not candidates:
        console.log("[yellow]No matching failed records found in local output for selected scope.[/yellow]")
        return {"attempted": 0, "recovered": 0, "remaining": len(failed)}

    recovered = 0
    attempted = 0
    recovered_keys: set[str] = set()
    failed_again_keys: set[str] = set()

    for record, matched_keys in candidates:
        order_label = str(record.get("order_number") or "").strip()
        if not order_label:
            order_label = _clean_value(record.get("storage_uid")) or "unknown-order"
        file_id = _file_id(record)
        popup_url = str(record.get("document_popup_url") or record.get("document_url") or "").strip()
        document_path = str(record.get("document_path") or "").strip()
        if not popup_url:
            failed_again_keys.update(matched_keys)
            continue

        attempted += 1
        console.log(f"[cyan]Retry failed order {order_label} ({attempted}/{len(candidates)})[/cyan]")
        doc_result = await capture_document_direct_from_url(
            context,
            popup_url,
            file_id,
            document_path=document_path,
            query_485_string=record.get("query_485_string"),
            enable_popup_probe=True,
        )
        file_path = doc_result.get("document_file_path")
        if not file_path:
            await _ensure_retry_session(context)
            console.log(f"[dim]Retry fallback: opening popup capture for {order_label}[/dim]")
            try:
                popup_result = await capture_document_from_url(context, popup_url, file_id)
                if popup_result and popup_result.get("document_file_path"):
                    doc_result = popup_result
                    file_path = popup_result.get("document_file_path")
            except Exception as e:
                console.log(f"[dim]Retry popup fallback failed: {type(e).__name__}[/dim]")

        if file_path:
            record.update(doc_result)
            record["file_id"] = file_id
            record["storage_uid"] = _storage_uid(record)
            save_batch([record])
            recovered_keys.update(matched_keys)
            recovered += 1
            console.log(f"[green]Recovered failed order {order_label}[/green]")
        else:
            failed_again_keys.update(matched_keys)
            console.log(f"[yellow]Still failed after retry: {order_label}[/yellow]")

    for key in sorted(recovered_keys - failed_again_keys):
        clear_failed(key)

    remaining = len(get_failed_orders())
    console.log(
        f"[bold cyan]Failed-order retry complete: attempted={attempted}, recovered={recovered}, remaining={remaining}[/bold cyan]"
    )
    return {"attempted": attempted, "recovered": recovered, "remaining": remaining}


# ── Filters ───────────────────────────────────────────────────────────────────

async def _apply_filters(page: Page, start_date: str, end_date: str) -> dict | None:
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
        ) as info:
            await display.click()
        response = await info.value
    except Exception:
        await display.click()
        response = None
    await page.wait_for_load_state("networkidle", timeout=20_000)
    console.log("[green]✓ Filters applied[/green]")
    if response:
        return await _parse_getdata_response(response, page.url, source="display")
    return None


# ── Row extraction ────────────────────────────────────────────────────────────

async def _extract_rows(page: Page, page_num: int) -> list[dict]:
    expected_rows = await _expected_rows_on_page(page)
    loaded_rows = await _wait_for_rows_to_settle(page, expected_rows)
    if expected_rows and loaded_rows < expected_rows:
        console.log(
            f"[yellow]Expected {expected_rows} rows by header, but only {loaded_rows} loaded in DOM.[/yellow]"
        )

    rows_data = await page.evaluate("""
    () => {
        const rows = Array.from(document.querySelectorAll('#tableData tr')).filter(tr =>
            tr.querySelectorAll('td').length >= 11
        );
        return rows.map(tr => {
            const cells = Array.from(tr.querySelectorAll('td'));
            // Expected column order (0-indexed):
            // 0=checkbox, 1=date, 2=time, 3=colour-flag, 4=client/clinician,
            // 5=location, 6=order#, 7=status-colour, 8=status,
            // 9=review-colour, 10=reviewed, 11=view
            const colorCell = cells[3];
            let docType = 'unknown';
            if (colorCell) {
                const bg = window.getComputedStyle(colorCell).backgroundColor || colorCell.style.backgroundColor || '';
                docType = (bg.includes('182, 255, 0') || bg.includes('0, 128') || bg.includes('green'))
                          ? 'client_document' : 'clinician_document';
            }

            const hasView = Array.from(tr.querySelectorAll('a, input')).some(el => {
                if (el.tagName === 'A') {
                    return /view/i.test((el.textContent || '').trim());
                }
                if (el.tagName === 'INPUT') {
                    return /view/i.test((el.value || '').trim());
                }
                return false;
            });

            return {
                row_dom_id:     tr.id || '',
                received_date:  (cells[1]  || {innerText:''}).innerText.trim(),
                received_time:  (cells[2]  || {innerText:''}).innerText.trim(),
                client_name:    (cells[4]  || {innerText:''}).innerText.trim(),
                location:       (cells[5]  || {innerText:''}).innerText.trim(),
                order_number:   (cells[6]  || {innerText:''}).innerText.trim(),
                status:         (cells[8]  || {innerText:''}).innerText.trim(),
                reviewed:       (cells[10] || {innerText:''}).innerText.trim(),
                doc_type:       docType,
                has_view:       hasView,
            };
        });
    }
    """)

    for idx, row in enumerate(rows_data, start=1):
        row["received_date"] = _iso_date(row.get("received_date", ""))
        if not str(row.get("order_number") or "").strip():
            row["order_number"] = f"row_{page_num}_{idx}"

    rows = rows_data

    if expected_rows and len(rows) < expected_rows:
        api_rows = await _extract_rows_via_getdata(page, page_num)
        if api_rows and len(api_rows) >= len(rows):
            console.log(
                f"[cyan]Using GetData fallback for page {page_num}: {len(api_rows)} rows.[/cyan]"
            )
            return api_rows

    return rows


async def _extract_rows_via_getdata(page: Page, page_num: int) -> list[dict]:
    parsed = await _request_getdata_page(page, page_num, source=f"fallback-page-{page_num}")
    if not parsed:
        console.log("[yellow]GetData fallback parse failed: empty payload[/yellow]")
        return []
    return parsed.get("rows", [])


async def _request_getdata_page(page: Page, page_num: int, source: str) -> dict | None:
    payload = await _build_getdata_payload(page, page_num)
    api_url = _get_getdata_url(page.url)
    console.log(
        "[dim]GetData payload "
        f"source={source} PageNo={payload.get('PageNo')} "
        f"Reviewed={payload.get('Reviewed')} sortKey={payload.get('sortKey')} "
        f"sortDirection={payload.get('sortDirection')} PageSize={payload.get('PageSize', 'missing')}[/dim]"
    )

    try:
        resp = await page.context.request.post(
            api_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json; charset=utf-8"},
            timeout=20_000,
        )
    except Exception as e:
        console.log(f"[yellow]GetData request failed ({source}): {type(e).__name__}[/yellow]")
        return None

    if not resp.ok:
        console.log(f"[yellow]GetData HTTP {resp.status} ({source})[/yellow]")
        return None

    return await _parse_getdata_response(resp, page.url, source=source)


async def _build_getdata_payload(page: Page, page_num: int) -> dict:
    return await page.evaluate(
        """
        (pageNo, pageSize) => {
            const val = (selector, fallback = '') => {
                const el = document.querySelector(selector);
                if (!el) return fallback;
                const raw = (el.value ?? '').toString().trim();
                return raw || fallback;
            };

            const normalize = (v, fallback = '0') => {
                if (v === undefined || v === null) return fallback;
                const s = String(v).trim();
                if (!s || s === 'ALL' || s === 'undefined') return fallback;
                return s;
            };

            const useRcvdDate = document.querySelector('#chk_RcvdDate')?.checked ?? true;

            let sortKey = normalize(val('#MainContent_hdn_lastSortKey', '1'), '1');
            let sortDirection = normalize(val('#MainContent_hdn_lastSortDirection', '0'), '0');
            if (sortKey === '0') sortKey = '1';

            if (typeof window.GetFilterConditionsForDisplay === 'function') {
                try {
                    const raw = window.GetFilterConditionsForDisplay(sortKey, sortDirection, pageNo);
                    const parsed = (typeof raw === 'string') ? JSON.parse(raw) : raw;
                    if (parsed && typeof parsed === 'object') {
                        parsed.PageNo = pageNo;
                        parsed.PageSize = pageSize;
                        parsed.Reviewed = '0';
                        return parsed;
                    }
                } catch (e) {
                    // Fall back to explicit payload construction below.
                }
            }

            let fromDate = val('#MainContent_txtRcvdDateFrom', '');
            let toDate = val('#MainContent_txtRcvdDateTo', '');
            if (!useRcvdDate) {
                fromDate = '';
                toDate = '';
            }

            return {
                Location: normalize(val('#MainContent_hdn_LocationIDs', '0'), '0'),
                Lob: normalize(val('#MainContent_hdn_LOB', '0'), '0'),
                status: normalize(val('#MainContent_ddl_status', '0'), '0'),
                RcvdDateFrom: fromDate,
                RcvdDateTo: toDate,
                Team: normalize(val('#MainContent_hdn_StaffTeamID', '0'), '0'),
                Reviewed: '0',
                Client: normalize(val('#_HIDDEN_CLIENT_ID', '0'), '0'),
                sortKey: sortKey,
                sortDirection: sortDirection,
                DocumentType: normalize(val('#MainContent_hdnDocumentType', '0'), '0'),
                Caregiver: normalize(val('#hdn_Clinician', '0'), '0'),
                PageNo: pageNo,
                PageSize: pageSize,
            };
        }
        """,
        page_num,
        settings.PAGE_SIZE,
    )


def _decode_getdata_response_data(body: dict) -> dict | None:
    data = body.get("d") if isinstance(body, dict) else None
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception:
            return None
    if isinstance(data, dict):
        return data
    return None


async def _parse_getdata_response(response: Response, current_url: str, source: str) -> dict | None:
    try:
        body = await response.json()
    except Exception:
        return None

    data = _decode_getdata_response_data(body)
    if not data:
        return None

    records = data.get("objRcvdList") or []
    total_pages = _to_int(data.get("Totalpages"), 0) or None
    total_records = _to_int(data.get("TotalRecords"), 0) or None
    origin = _origin_from_url(current_url)
    rows = [_map_getdata_record(r, origin) for r in records if isinstance(r, dict)]
    for idx, row in enumerate(rows, start=1):
        if not str(row.get("order_number") or "").strip():
            row["order_number"] = f"row_{source}_{idx}"

    console.log(
        f"[dim]Parsed GetData ({source}): raw={len(records)} mapped={len(rows)} "
        f"Totalpages={total_pages} TotalRecords={total_records}[/dim]"
    )

    return {
        "source": source,
        "rows": rows,
        "response_count": len(rows),
        "total_pages": total_pages,
        "total_records": total_records,
    }


def _origin_from_url(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


def _get_getdata_url(current_url: str) -> str:
    if "WorldView_ReceivedDocuments.aspx" in current_url:
        return re.sub(
            r"WorldView_ReceivedDocuments\.aspx.*$",
            "WorldView_ReceivedDocuments.aspx/GetData",
            current_url,
        )
    return f"{settings.WORLDVIEW_URL}/GetData"


def _to_int(v, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _to_bool(v) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "y"}
    return False


def _clean_value(v) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _derive_order_number(record: dict, client_doc_id: int, employee_doc_id: int) -> str:
    for key in ("OrderNo", "OrderNumber", "order_number", "Order_No"):
        val = _clean_value(record.get(key))
        if val:
            return val

    for key, prefix in (("CGTaskID", "CG"), ("ReferenceId", "REF"), ("OrderId", "OID"), ("WorldViewId", "WVID")):
        val = _clean_value(record.get(key))
        if val and val != "0":
            return f"{prefix}-{val}"

    if client_doc_id > 0:
        return f"DOC-{client_doc_id}"
    if employee_doc_id > 0:
        return f"EMPDOC-{employee_doc_id}"

    return ""


def _fallback_row_digest(row: dict) -> str:
    fingerprint = "|".join(
        _clean_value(row.get(k)).lower()
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


def _storage_uid(row: dict) -> str:
    client_doc_id = _to_int(row.get("client_document_id"), 0)
    doc_reference_id = _to_int(row.get("doc_reference_id"), 0)
    employee_doc_id = _to_int(row.get("employee_document_id"), 0)
    worldview_id = _to_int(row.get("worldview_id"), 0)
    order_id = _to_int(row.get("order_id"), 0)

    if client_doc_id > 0:
        return f"cdoc:{client_doc_id}"
    if employee_doc_id > 0:
        return f"edoc:{employee_doc_id}"
    if doc_reference_id > 0 and order_id > 0:
        return f"dref:{doc_reference_id}:oid:{order_id}"
    if doc_reference_id > 0:
        return f"dref:{doc_reference_id}"
    if worldview_id > 0 and order_id > 0:
        return f"wv:{worldview_id}:{order_id}"

    order_number = _clean_value(row.get("order_number"))
    received_date = _clean_value(row.get("received_date"))
    client_name = _clean_value(row.get("client_name"))
    if order_number and received_date:
        return f"ord:{order_number}:{received_date}:{client_name.lower()}"
    if order_number:
        return f"ord:{order_number}"

    row_dom_id = _clean_value(row.get("row_dom_id"))
    if row_dom_id:
        return f"dom:{row_dom_id}"

    return f"row:{_fallback_row_digest(row)}"


def _legacy_row_uid(row: dict) -> str:
    client_doc_id = _to_int(row.get("client_document_id"), 0)
    doc_reference_id = _to_int(row.get("doc_reference_id"), 0)
    employee_doc_id = _to_int(row.get("employee_document_id"), 0)
    worldview_id = _to_int(row.get("worldview_id"), 0)
    order_id = _to_int(row.get("order_id"), 0)

    if client_doc_id > 0 and doc_reference_id > 0:
        return f"cdoc:{doc_reference_id}:{client_doc_id}"
    if employee_doc_id > 0:
        return f"edoc:{employee_doc_id}"
    if worldview_id > 0 and order_id > 0:
        return f"wv:{worldview_id}:{order_id}"

    order_number = _clean_value(row.get("order_number"))
    received_date = _clean_value(row.get("received_date"))
    client_name = _clean_value(row.get("client_name"))
    if order_number and received_date:
        return f"ord:{order_number}:{received_date}:{client_name.lower()}"
    if order_number:
        return f"ord:{order_number}"

    row_dom_id = _clean_value(row.get("row_dom_id"))
    if row_dom_id:
        return f"dom:{row_dom_id}"

    return f"row:{_fallback_row_digest(row)}"


def _row_uid(row: dict) -> str:
    return _storage_uid(row)


def _row_uid_aliases(row: dict) -> set[str]:
    """Return canonical + legacy UID forms for mixed-checkpoint resume compatibility."""
    aliases = {_row_uid(row), _legacy_row_uid(row)}
    return {uid for uid in aliases if uid}


def _file_id(row: dict, page_num: int | None = None, row_idx: int | None = None) -> str:
    client_doc_id = _to_int(row.get("client_document_id"), 0)
    if client_doc_id > 0:
        return f"cdoc-{client_doc_id}"

    employee_doc_id = _to_int(row.get("employee_document_id"), 0)
    if employee_doc_id > 0:
        return f"edoc-{employee_doc_id}"

    order_number = _clean_value(row.get("order_number"))
    if order_number:
        return order_number

    row_dom_id = _clean_value(row.get("row_dom_id"))
    if row_dom_id:
        return f"dom-{row_dom_id}"

    if page_num is not None and row_idx is not None:
        return f"row_{page_num}_{row_idx + 1}"

    return _storage_uid(row).replace(":", "-")


def _failed_retry_key(row: dict) -> str:
    return f"uid:{_storage_uid(row)}"


def _retry_keys_for_record(record: dict) -> set[str]:
    keys: set[str] = set()
    storage_uid = _clean_value(record.get("storage_uid")) or _storage_uid(record)
    if storage_uid:
        keys.add(f"uid:{storage_uid}")

    order_number = _clean_value(record.get("order_number"))
    if order_number:
        keys.add(order_number)
    return keys


def _map_getdata_record(record: dict, origin: str) -> dict:
    client_doc_id = _to_int(record.get("clientDocumentId"), 0)
    employee_doc_id = _to_int(record.get("EmployeeDocumentID"), 0)
    doc_reference = _to_int(record.get("DocReferenceId"), 0)
    document_path = str(record.get("DocumentPath") or "").strip()
    order_number = _derive_order_number(record, client_doc_id, employee_doc_id)

    popup_url = None
    if client_doc_id > 0 and doc_reference > 0:
        popup_url = (
            f"{origin}/HH/Z1/UI/Common/DocumentViewer.aspx"
            f"?Reference={doc_reference}&documentId={client_doc_id}"
        )
    elif employee_doc_id > 0:
        popup_url = (
            f"{origin}/HH/Z1/UI/Common/DocumentViewer.aspx"
            f"?Reference=12&documentId={employee_doc_id}"
        )
    elif document_path:
        popup_url = urljoin(f"{origin}/", document_path)

    return {
        "source": "getdata_api",
        "row_dom_id": "",
        "worldview_id": _to_int(record.get("WorldViewId"), 0),
        "order_id": _to_int(record.get("OrderId"), 0),
        "change_order_type": _to_int(record.get("ChangeOrderType"), 0),
        "reference_id": _to_int(record.get("ReferenceId"), 0),
        "is_echart_order_format": _to_bool(record.get("iseChartOrderFormat")),
        "cg_task_id": _to_int(record.get("CGTaskID"), 0),
        "client_id": _to_int(record.get("ClientId"), 0),
        "admit_no": _to_int(record.get("AdmitNo"), 0),
        "intake_id": _to_int(record.get("IntakeID"), 0),
        "episode_id": _to_int(record.get("EpisodeId"), 0),
        "payer_id": _to_int(record.get("PayerID"), 0),
        "library_form_id": _to_int(record.get("libraryFormID"), 0),
        "order_type": _clean_value(record.get("OrderType")),
        "initial_order_in_regular_format": _to_bool(record.get("InitialOrderInRegularFormat")),
        "requested_by": _to_bool(record.get("RequestedBy")),
        "form_485_id": _to_int(record.get("Form485ID"), 0),
        "poc": _to_int(record.get("poc"), 0),
        "doc_reference_id": doc_reference,
        "client_document_id": client_doc_id,
        "employee_document_id": employee_doc_id,
        "employee_id": _to_int(record.get("EmployeeID"), 0),
        "staff_type": _to_int(record.get("StaffType"), 0),
        "document_path": document_path,
        "query_485_string": _clean_value(record.get("_485QueryString")),
        "received_date": _iso_date(str(record.get("RecievedDate") or "").strip()),
        "received_time": str(record.get("Time") or "").strip(),
        "client_name": _clean_value(record.get("client") or record.get("ClientName")),
        "location": _clean_value(record.get("Location")),
        "order_number": order_number,
        "status": "Mapped" if _to_bool(record.get("status")) else "Unmapped",
        "reviewed": "Yes" if _to_bool(record.get("Reviewed")) else "No",
        "doc_type": (
            "client_document"
            if client_doc_id > 0
            else ("clinician_document" if employee_doc_id > 0 else "unknown")
        ),
        "has_view": bool(popup_url),
        "document_popup_url": popup_url,
    }


async def _expected_rows_on_page(page: Page) -> int | None:
    locator = page.locator("#RecordsCount").first
    if await locator.count() == 0:
        return None

    try:
        txt = (await locator.inner_text(timeout=5_000)).strip()
    except Exception:
        return None

    m = re.search(r"Showing\s+(\d+)\s*-\s*(\d+)\s+of", txt, flags=re.IGNORECASE)
    if not m:
        return None

    start = int(m.group(1))
    end = int(m.group(2))
    if end < start:
        return None
    return end - start + 1


async def _wait_for_rows_to_settle(page: Page, expected_rows: int | None = None) -> int:
    rows_by_id = page.locator("#tableData tr[id^='RowID']")
    rows_any = page.locator("#tableData tr")
    deadline = asyncio.get_running_loop().time() + 15
    last_count = -1
    stable_hits = 0

    while asyncio.get_running_loop().time() < deadline:
        count = await rows_by_id.count()
        if count == 0:
            count = await rows_any.count()

        if expected_rows and count >= expected_rows:
            return count

        if expected_rows:
            await asyncio.sleep(0.25)
            continue

        if count > 0 and count == last_count:
            stable_hits += 1
        else:
            stable_hits = 0
            last_count = count

        if count > 0 and stable_hits >= 3:
            return count

        await asyncio.sleep(0.25)

    final_count = await rows_by_id.count()
    if final_count == 0:
        final_count = await rows_any.count()
    return final_count


def _iso_date(s: str) -> str:
    try:
        return datetime.strptime(s, "%m/%d/%Y").strftime("%Y-%m-%d")
    except Exception:
        return s


# ── Document capture ──────────────────────────────────────────────────────────

async def _capture_document(
    context: BrowserContext,
    page: Page,
    row: dict,
    row_idx: int,
    file_id: str = None,
) -> dict:
    popup_url = (row or {}).get("document_popup_url")
    if popup_url:
        if settings.DIRECT_DOWNLOAD_ONLY:
            return await capture_document_direct_from_url(
                context,
                popup_url,
                file_id,
                document_path=(row or {}).get("document_path"),
                query_485_string=(row or {}).get("query_485_string"),
            )
        return await capture_document_from_url(context, popup_url, file_id)

    if settings.DIRECT_DOWNLOAD_ONLY:
        return {"document_file_path": None}

    row_dom_id = (row or {}).get("row_dom_id")
    if row_dom_id:
        row_locator = page.locator(f"#tableData tr#{row_dom_id}").first
        if await row_locator.count() > 0:
            row_view = row_locator.locator("a:has-text('View'), input[value='View']").first
            if await row_view.count() > 0:
                return await capture_document(context, row_view, file_id)
            return {"document_file_path": None}

    view_btns = page.locator(
        "#tableData tr td a:has-text('View'), #tableData tr td input[value='View']"
    )
    count = await view_btns.count()
    if row_idx >= count:
        return {"document_file_path": None}
    return await capture_document(context, view_btns.nth(row_idx), file_id)


def _load_local_output_records() -> list[dict]:
    return load_records()


async def _ensure_retry_session(context: BrowserContext):
    """Open a probe page and re-authenticate if session has expired."""
    page = await context.new_page()
    try:
        await page.goto(settings.WORLDVIEW_URL, wait_until="networkidle", timeout=30_000)
        await ensure_logged_in(page)
    finally:
        await page.close()


async def _capture_row_with_semaphore(
    semaphore: asyncio.Semaphore,
    context: BrowserContext,
    page: Page,
    row: dict,
    row_idx: int,
    order_label: str,
    file_id: str,
    month_key: str,
) -> tuple[dict, str, str, str | None]:
    result_row = dict(row)
    err = None

    async with semaphore:
        try:
            doc_data = await _capture_document(context, page, result_row, row_idx, file_id)
            result_row.update(doc_data)
        except Exception as e:
            err = str(e)

    result_row["file_id"] = file_id
    result_row["storage_uid"] = _storage_uid(result_row)
    result_row["date_batch"] = month_key
    return result_row, order_label, _failed_retry_key(result_row), err


# ── Pagination ────────────────────────────────────────────────────────────────

async def _get_total_pages(page: Page) -> int:
    # Prefer the records header, e.g.:
    # "Showing 1 - 100 of 1127 Orders, Page 1 of 12"
    try:
        header = await page.locator("#RecordsCount").first.inner_text(timeout=5_000)
        m = re.search(r"Page\s+\d+\s+of\s+(\d+)", header, flags=re.IGNORECASE)
        if m:
            return int(m.group(1))
    except Exception:
        pass

    # Fallback for tenants where page info is rendered outside #RecordsCount.
    try:
        txt = await page.locator("text=/Page\\s+\\d+\\s+of\\s+\\d+/i").first.inner_text(timeout=5_000)
        m = re.search(r"Page\s+\d+\s+of\s+(\d+)", txt, flags=re.IGNORECASE)
        if m:
            return int(m.group(1))
    except Exception:
        pass

    # Final fallback to in-page JS value if exposed.
    try:
        total_pages = await page.evaluate(
            """
            () => {
                const v = window.totalPages;
                if (typeof v === 'number') return v;
                const parsed = parseInt(v || '0', 10);
                return Number.isFinite(parsed) ? parsed : 0;
            }
            """
        )
        if isinstance(total_pages, int) and total_pages > 0:
            return total_pages
    except Exception:
        pass

    return 1


async def _goto_next_page(page: Page) -> dict | None:
    for sel in ["input[value='>']", "a:has-text('>')", "input[title='Next']",
                "a[title='Next']", ".pager-next", "#btnNext", "#Next", "input#Next"]:
        el = page.locator(sel).first
        if await el.count() > 0:
            try:
                async with page.expect_response(
                    lambda r: "WorldView_ReceivedDocuments.aspx/GetData" in r.url and r.request.method == "POST",
                    timeout=15_000,
                ) as info:
                    await el.click()
                response = await info.value
            except Exception:
                await el.click()
                response = None
            if response:
                parsed = await _parse_getdata_response(response, page.url, source="next")
                if parsed:
                    return parsed
            await page.wait_for_load_state("networkidle", timeout=15_000)
            return None
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
