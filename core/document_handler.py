"""
core/document_handler.py — captures the Kantime WorldView popup document.

CONFIRMED POPUP BEHAVIOUR (screenshot 2026-03-28):
  - "View" click opens a NEW Mac browser window (separate Playwright page)
  - URL bar shows: kantimehealth.net
  - Document rendered inline as a scanned image (physician order form)
  - Toolbar at bottom of document has 4 buttons:
      [zoom-]  [zoom+]  [save]  [download-arrow]
  - The download arrow button triggers a file download

CAPTURE STRATEGY (in order):
  1. PRIMARY   — Click download button, intercept Playwright download stream
  2. FALLBACK1 — Fetch the rendered img/embed src with session cookies
  3. FALLBACK2 — Fetch popup URL if it IS the document endpoint
  4. FALLBACK3 — Full-page screenshot (never loses data)

BONUS — Structured metadata extracted from popup visible text:
  mrn_from_doc, dob_from_doc, patient_name_from_doc,
  npi_from_doc, physician_name_from_doc,
  order_number_from_doc, order_date_from_doc,
  primary_diagnosis_from_doc, certification_period_from_doc,
  payer_source_from_doc
"""

import asyncio
import base64
import io
import json
import re
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

from playwright.async_api import Page, BrowserContext
from tenacity import retry, stop_after_attempt, wait_exponential
from rich.console import Console

from config.settings import settings

console = Console()

_META_KEYS = [
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
]

_DOWNLOAD_BTN_SELECTORS = [
    # ACTUAL STRUCTURE (Chrome PDF Viewer / Kantime):
    # <viewer-download-controls id="downloads">
    #   <cr-icon-button>...</cr-icon-button>
    # </viewer-download-controls>
    
    # Web Components (most specific)
    "viewer-download-controls button",
    "viewer-download-controls cr-icon-button",
    "#downloads button",
    "#downloads cr-icon-button",
    "#downloads",
    "viewer-download-controls",
    
    # Buttons/icons with download-related attributes
    "cr-icon-button[title*='Download' i]",
    "cr-icon-button[aria-label*='Download' i]",
    "cr-icon-button[aria-label*='download' i]",
    
    # Standard buttons with download title/label
    "button[title*='Download' i]",
    "button[aria-label*='Download' i]",
    "a[title*='Download' i]",
    "a[aria-label*='download' i]",
    
    # Toolbar structure (id="end" section contains downloads + print + more)
    "#toolbar #end cr-icon-button:nth-child(3)",  # print is 3rd, download is inside viewer-download-controls
    "#toolbar #end viewer-download-controls button",
    "#toolbar #end viewer-download-controls",
    
    # PDF viewer toolbar icons (generic)
    "[iron-icon*='download' i]",
    "[icon*='download' i]",
    
    # Fallback: any button in downloads area
    "#downloads button",
    "#downloads a",
    
    # Last resort: buttons in end section
    "div#end button:not([disabled])",
    "div#end cr-icon-button:not([disabled])",
]

_DOC_IMAGE_SELECTORS = [
    "img[src*='Document']",
    "img[src*='document']",
    "img[src*='Order']",
    "img[src*='order']",
    "img[src*='GetFile']",
    "img[src*='getfile']",
    "img[src*='View']",
    "img[src*='Download']",
    "embed[src]",
    "object[data]",
    "img",
]

_MIME_MAP = {
    ".pdf":  "application/pdf",
    ".jpg":  "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png":  "image/png",
    ".tiff": "image/tiff",
    ".tif":  "image/tiff",
    ".bmp":  "image/bmp",
}


@retry(
    stop=stop_after_attempt(1),  # No retries - S2/S3/S4 fallbacks are fast enough
    wait=wait_exponential(multiplier=0.5, min=1, max=3),
)
async def capture_document(context: BrowserContext, view_btn_locator, order_number: str = None) -> dict:
    """
    Click the View button, wait for the new popup window, capture PDF to disk.
    Returns dict with document_file_path (not Base64) + metadata extracted from popup.
    """
    async with context.expect_page() as popup_info:
        await view_btn_locator.click()

    popup: Page = await popup_info.value
    await popup.wait_for_load_state("networkidle", timeout=20_000)
    popup_url = popup.url
    console.log(f"  [dim]Popup: {popup_url}[/dim]")

    doc_result  = await _capture_with_fallbacks(context, popup, popup_url, order_number)
    meta_result = await _extract_metadata_by_flag(popup, doc_result.get("document_file_path"))

    await popup.close()
    return {**doc_result, **meta_result}


async def capture_document_from_url(context: BrowserContext, popup_url: str, order_number: str = None) -> dict:
    """
    Capture a document when we already know the popup URL (GetData fallback path).
    """
    popup = await context.new_page()
    try:
        await popup.goto(popup_url, wait_until="networkidle", timeout=20_000)
        resolved_url = popup.url
        console.log(f"  [dim]Popup URL: {resolved_url}[/dim]")

        doc_result = await _capture_with_fallbacks(context, popup, resolved_url, order_number)
        meta_result = await _extract_metadata_by_flag(popup, doc_result.get("document_file_path"))
        return {**doc_result, **meta_result}
    finally:
        await popup.close()


async def capture_document_direct_from_url(
    context: BrowserContext,
    popup_url: str,
    order_number: str = None,
    document_path: str | None = None,
    query_485_string: str | None = None,
    enable_popup_probe: bool = False,
) -> dict:
    """
    Fast path: download directly from authenticated endpoints without opening popup.
    If direct fetch fails, caller may mark order failed and continue.
    """
    console.log(f"  [dim]Direct capture: {popup_url}[/dim]")
    try:
        doc_result = await _try_popup_url(
            context,
            popup_url,
            order_number,
            query_485_string=query_485_string,
        )
    except Exception as e:
        console.log(f"  [dim]Direct capture error: {type(e).__name__}[/dim]")
        doc_result = None

    if not doc_result and document_path:
        try:
            doc_result = await _try_document_path_url(
                context,
                popup_url,
                document_path,
                order_number,
                query_485_string=query_485_string,
            )
        except Exception as e:
            console.log(f"  [dim]Direct path fallback error: {type(e).__name__}[/dim]")
            doc_result = None

    if not doc_result and enable_popup_probe:
        try:
            doc_result = await _try_popup_probe_src(context, popup_url, order_number)
        except Exception as e:
            console.log(f"  [dim]Direct popup-probe error: {type(e).__name__}[/dim]")
            doc_result = None

    if not doc_result:
        return {
            "document_file_path": None,
            "document_url": popup_url,
        }

    return {
        **doc_result,
        "document_url": popup_url,
    }


async def _extract_metadata_by_flag(popup: Page, document_file_path: str | None = None) -> dict:
    """
    Feature-flag entrypoint for metadata extraction.
    If both ENABLE_DATA_EXTRACTION and OLLAMA_ENABLED are true,
    use Ollama vision extraction (ollama-only mode).
    Otherwise, keep current regex extraction behavior unchanged.
    """
    use_ollama = settings.ENABLE_DATA_EXTRACTION and settings.OLLAMA_ENABLED
    if not use_ollama:
        # Preserve legacy/current shape: only include fields that were actually parsed.
        return _drop_empty_metadata_values(await _extract_popup_metadata(popup))

    console.log(f"  [dim]Metadata: Ollama enabled ({settings.OLLAMA_MODEL})[/dim]")
    if not document_file_path:
        console.log("  [yellow]Ollama extraction skipped: no document_file_path[/yellow]")
        return _empty_metadata()

    meta = await _extract_popup_metadata_ollama(document_file_path)
    if meta:
        console.log("  [cyan]Metadata extracted via Ollama[/cyan]")
        return meta

    console.log("  [yellow]Ollama extraction failed; no regex fallback (ollama-only mode)[/yellow]")
    return _empty_metadata()


async def _extract_popup_metadata_ollama(document_file_path: str) -> dict:
    max_pages = max(1, int(settings.OLLAMA_MAX_PAGES))
    images = await asyncio.to_thread(_render_doc_pages_for_ollama, document_file_path, max_pages)
    if not images:
        return {}

    prompt = _build_ollama_metadata_prompt()
    raw1 = await asyncio.to_thread(_call_ollama_chat, prompt, images, None)
    parsed1 = _parse_ollama_metadata(raw1)
    if _is_valid_ollama_metadata(parsed1):
        return _normalize_ollama_metadata(parsed1)

    retry_prompt = (
        "Your previous output was invalid. Return ONLY a JSON object with exact keys and no extra text.\n"
        f"Previous output:\n{raw1[:2000]}"
    )
    raw2 = await asyncio.to_thread(_call_ollama_chat, retry_prompt, images, raw1)
    parsed2 = _parse_ollama_metadata(raw2)
    if _is_valid_ollama_metadata(parsed2):
        return _normalize_ollama_metadata(parsed2)

    return {}


def _render_doc_pages_for_ollama(document_file_path: str, max_pages: int) -> list[str]:
    path = Path(document_file_path)
    if not path.exists():
        console.log(f"  [dim]Ollama input missing: {document_file_path}[/dim]")
        return []

    ext = path.suffix.lower()
    if ext in {".png", ".jpg", ".jpeg", ".bmp", ".tif", ".tiff", ".webp"}:
        return [base64.b64encode(path.read_bytes()).decode("utf-8")]

    if ext != ".pdf":
        console.log(f"  [dim]Ollama input unsupported extension: {ext}[/dim]")
        return []

    try:
        import pypdfium2 as pdfium
    except Exception:
        console.log("  [red]pypdfium2 not installed. Add it to requirements and reinstall.[/red]")
        return []

    try:
        doc = pdfium.PdfDocument(str(path))
        page_count = len(doc)
        render_count = min(max_pages, page_count)
        images: list[str] = []

        for i in range(render_count):
            page = doc[i]
            bitmap = page.render(scale=2.0)
            pil_img = bitmap.to_pil()
            buf = io.BytesIO()
            pil_img.save(buf, format="PNG")
            images.append(base64.b64encode(buf.getvalue()).decode("utf-8"))

        return images
    except Exception as e:
        console.log(f"  [dim]PDF render error for Ollama: {type(e).__name__}[/dim]")
        return []


def _build_ollama_metadata_prompt() -> str:
    return (
        "Extract medical order metadata from the provided document image(s).\n"
        "Return ONLY a valid JSON object with these exact keys and no extra text:\n"
        "{\n"
        "  \"mrn_from_doc\": string|null,\n"
        "  \"dob_from_doc\": string|null,\n"
        "  \"patient_name_from_doc\": string|null,\n"
        "  \"npi_from_doc\": string|null,\n"
        "  \"physician_name_from_doc\": string|null,\n"
        "  \"order_number_from_doc\": string|null,\n"
        "  \"order_date_from_doc\": string|null,\n"
        "  \"primary_diagnosis_from_doc\": string|null,\n"
        "  \"certification_period_from_doc\": string|null,\n"
        "  \"payer_source_from_doc\": string|null\n"
        "}\n"
        "Use null when a field is missing or uncertain."
    )


def _call_ollama_chat(prompt: str, images_b64: list[str], previous_output: str | None) -> str:
    url = f"{settings.OLLAMA_URL.rstrip('/')}/api/chat"
    content = prompt
    if previous_output:
        content += "\nRetry with corrected JSON format."

    payload = {
        "model": settings.OLLAMA_MODEL,
        "stream": False,
        "format": "json",
        "messages": [
            {
                "role": "user",
                "content": content,
                "images": images_b64,
            }
        ],
        "options": {"temperature": 0},
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url=url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=settings.OLLAMA_TIMEOUT_SECONDS) as resp:
            body = resp.read().decode("utf-8")
        parsed = json.loads(body)
        return str(parsed.get("message", {}).get("content", "")).strip()
    except urllib.error.URLError as e:
        console.log(f"  [red]Ollama request failed: {e}[/red]")
    except Exception as e:
        console.log(f"  [red]Ollama parse error: {type(e).__name__}[/red]")
    return ""


def _parse_ollama_metadata(raw: str) -> dict:
    if not raw:
        return {}

    text = raw.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\n", "", text)
        text = re.sub(r"\n```$", "", text)

    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        pass

    m = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not m:
        return {}

    try:
        parsed = json.loads(m.group(0))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _is_valid_ollama_metadata(meta: dict) -> bool:
    if not isinstance(meta, dict):
        return False
    for k in _META_KEYS:
        if k not in meta:
            return False
    filled = 0
    for k in _META_KEYS:
        v = meta.get(k)
        if v not in (None, "", "null", "None"):
            filled += 1
    return filled > 0


def _normalize_ollama_metadata(meta: dict) -> dict:
    out = {}
    for k in _META_KEYS:
        v = meta.get(k)
        if v is None:
            out[k] = None
            continue
        s = str(v).strip()
        if not s or s.lower() in {"null", "none", "n/a", "na"}:
            out[k] = None
        else:
            out[k] = s
    return out


def _empty_metadata() -> dict:
    return {k: None for k in _META_KEYS}


def _drop_empty_metadata_values(meta: dict) -> dict:
    """Remove null/empty metadata keys to keep legacy sparse output shape."""
    out = {}
    for k, v in (meta or {}).items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        out[k] = v
    return out


# ── Strategy 1: Use Cmd+S on Mac to save PDF ─────────────────────────────────

async def _try_download_button(context: BrowserContext, popup: Page, order_number: str = None):
    """
    Press Cmd+S on Mac to trigger PDF download via save dialog.
    Saves to data/pdfs/ and returns file path.
    """
    try:
        import platform
        
        console.log("  [dim]S1: Pressing Cmd+S to save PDF...[/dim]")
        
        # Track current PDFs so we only accept files created/updated by this Cmd+S.
        pdf_dir = Path(settings.PDF_DIR)
        pdf_dir.mkdir(parents=True, exist_ok=True)
        before_mtime = {}
        for p in pdf_dir.glob("*.pdf"):
            try:
                before_mtime[p] = p.stat().st_mtime
            except FileNotFoundError:
                continue

        # Trigger the system save dialog
        if platform.system() == "Darwin":  # macOS
            await popup.keyboard.press("Meta+S")
        else:  # Windows/Linux
            await popup.keyboard.press("Control+S")
        
        # Wait for download
        await popup.wait_for_timeout(1000)
        
        # Wait up to 5 seconds for a newly created or modified PDF.
        for attempt in range(10):
            changed_files = []
            for p in pdf_dir.glob("*.pdf"):
                try:
                    mtime = p.stat().st_mtime
                except FileNotFoundError:
                    continue
                old_mtime = before_mtime.get(p)
                if old_mtime is None or mtime > old_mtime + 0.001:
                    changed_files.append((mtime, p))

            if changed_files:
                latest_pdf = max(changed_files, key=lambda t: t[0])[1]

                if order_number:
                    safe_order = re.sub(r"[^A-Za-z0-9_.-]", "_", order_number)
                    dest_path = pdf_dir / f"{safe_order}.pdf"
                    # Copy bytes so source download file can be reused by the viewer.
                    dest_path.write_bytes(latest_pdf.read_bytes())
                    console.log(f"  [green]✓ S1: Saved PDF ({dest_path.name})[/green]")
                    return {"document_file_path": str(dest_path)}

                console.log(f"  [green]✓ S1: Saved PDF ({latest_pdf.name})[/green]")
                return {"document_file_path": str(latest_pdf)}
            
            await popup.wait_for_timeout(500)
        
        console.log("  [dim]S1: No new PDF detected after Cmd+S[/dim]")
        
    except Exception as e:
        console.log(f"  [dim]S1: Cmd+S failed: {type(e).__name__}[/dim]")
    return None


async def _find_last_toolbar_button(popup: Page):
    """Not used - S1 uses Cmd+S instead."""
    return None


# ── Strategy 2: fetch rendered document img/embed src ────────────────────────

async def _try_fetch_doc_src(context: BrowserContext, popup: Page, order_number: str = None):
    try:
        src = await popup.evaluate(
            """
            (sels) => {
                for (const sel of sels) {
                    try {
                        const el = document.querySelector(sel);
                        if (el) {
                            const s = el.src || el.data || el.getAttribute('src');
                            if (s && s.length > 10) return s;
                        }
                    } catch (e) {}
                }
                return null;
            }
            """,
            _DOC_IMAGE_SELECTORS,
        )
    except Exception as e:
        console.log(f"  [dim]S2: selector probe failed: {type(e).__name__}[/dim]")
        src = None

    if not src:
        # Retry-only probe path sometimes renders the document in iframe/embed without matching
        # the initial selectors. Collect additional candidate URLs and probe them directly.
        extra_candidates = []
        try:
            extra_candidates = await popup.evaluate(
                """
                () => {
                    const out = [];
                    const seen = new Set();
                    const add = (u) => {
                        if (!u || typeof u !== 'string') return;
                        const s = u.trim();
                        if (!s || seen.has(s)) return;
                        seen.add(s);
                        out.push(s);
                    };

                    document.querySelectorAll('iframe[src], embed[src], object[data], a[href], img[src]').forEach((el) => {
                        add(el.getAttribute('src') || el.getAttribute('data') || el.getAttribute('href'));
                    });

                    return out;
                }
                """
            )
        except Exception as e:
            console.log(f"  [dim]S2: candidate probe failed: {type(e).__name__}[/dim]")

        for candidate in extra_candidates:
            resolved_candidate = urljoin(popup.url, candidate)
            if not resolved_candidate:
                continue

            console.log(f"  [dim]S2: probing candidate {resolved_candidate[:50]}...[/dim]")
            raw_candidate = await _fetch_authenticated(context, resolved_candidate, timeout=5_000)
            if raw_candidate and raw_candidate.startswith(b"%PDF"):
                pdf_dir = Path(settings.PDF_DIR)
                pdf_dir.mkdir(parents=True, exist_ok=True)
                filename = f"{order_number or 'doc'}.pdf"
                filepath = pdf_dir / filename
                filepath.write_bytes(raw_candidate)
                console.log(f"  [green]✓ S2: Downloaded PDF {len(raw_candidate):,} bytes ({filename})[/green]")
                return {"document_file_path": str(filepath)}

        console.log("  [dim]S2: no doc src found[/dim]")
        return None

    resolved = src
    if src.startswith("/"):
        from urllib.parse import urlparse
        p = urlparse(popup.url)
        resolved = f"{p.scheme}://{p.netloc}{src}"

    console.log(f"  [dim]S2: fetching {resolved[:50]}...[/dim]")
    raw = await _fetch_authenticated(context, resolved, timeout=5_000)
    if raw:
        # Save to file
        pdf_dir = Path(settings.PDF_DIR)
        pdf_dir.mkdir(parents=True, exist_ok=True)
        ext = _get_ext_from_mime(_guess_mime(resolved))
        filename = f"{order_number or 'doc'}{ext}"
        filepath = pdf_dir / filename
        filepath.write_bytes(raw)
        console.log(f"  [green]✓ S2: Saved {len(raw):,} bytes ({filename})[/green]")
        return {"document_file_path": str(filepath)}
    return None


# ── Strategy 3: popup URL is the document ────────────────────────────────────

async def _try_popup_url(
    context: BrowserContext,
    popup_url: str,
    order_number: str = None,
    query_485_string: str | None = None,
):
    """Try to fetch the actual PDF from the document viewer endpoint."""
    
    import re
    ref_match = re.search(r'Reference=([^&]+)', popup_url)
    doc_match = re.search(r'documentId=([^&]+)', popup_url)
    
    if ref_match and doc_match:
        reference = ref_match.group(1)
        doc_id = doc_match.group(1)
        
        # Try common PDF download endpoints
        pdf_endpoints = [
            f"https://www.kantimehealth.net/HH/Z1/UI/Common/DocumentViewer.aspx?Reference={reference}&documentId={doc_id}&pdf=true",
            f"https://www.kantimehealth.net/HH/Z1/UI/Common/GetDocument.aspx?Reference={reference}&documentId={doc_id}",
            f"https://www.kantimehealth.net/HH/Z1/UI/Common/DownloadDocument.ashx?Reference={reference}&documentId={doc_id}",
            popup_url,
        ]

        token = str(query_485_string or "").strip()
        if token:
            tokenized = []
            for u in pdf_endpoints:
                sep = "&" if "?" in u else "?"
                tokenized.append(f"{u}{sep}_485QueryString={token}")
                tokenized.append(f"{u}{sep}q={token}")
            pdf_endpoints.extend(tokenized)
        
        for pdf_url in pdf_endpoints:
            console.log(f"  [dim]S3: Trying {pdf_url[-40:]}[/dim]")
            raw = await _fetch_authenticated(context, pdf_url, timeout=5_000)
            if raw and raw.startswith(b"%PDF"):
                # Save to file
                pdf_dir = Path(settings.PDF_DIR)
                pdf_dir.mkdir(parents=True, exist_ok=True)
                filename = f"{order_number or 'doc'}.pdf"
                filepath = pdf_dir / filename
                filepath.write_bytes(raw)
                console.log(f"  [green]✓ S3: Downloaded PDF {len(raw):,} bytes ({filename})[/green]")
                return {"document_file_path": str(filepath)}
            if raw:
                console.log("  [dim]S3: response was not a PDF (missing %PDF header)[/dim]")
    
    return None


async def _try_popup_probe_src(context: BrowserContext, popup_url: str, order_number: str = None):
    """
    Retry-only fallback: open popup page and fetch the rendered document src directly.
    This avoids Cmd+S and keeps recovery fast for endpoints that require viewer initialization.
    """
    popup = await context.new_page()
    try:
        await popup.goto(popup_url, wait_until="networkidle", timeout=20_000)
        console.log("  [dim]S3c: Popup probe for document src[/dim]")
        return await _try_fetch_doc_src(context, popup, order_number)
    finally:
        await popup.close()


async def _try_document_path_url(
    context: BrowserContext,
    popup_url: str,
    document_path: str,
    order_number: str = None,
    query_485_string: str | None = None,
):
    path = str(document_path or "").strip()
    if not path:
        return None

    parsed_popup = urlparse(popup_url)
    origin = f"{parsed_popup.scheme}://{parsed_popup.netloc}" if parsed_popup.scheme and parsed_popup.netloc else ""

    candidates = []
    seen = set()

    def _add(url: str):
        if not url or url in seen:
            return
        seen.add(url)
        candidates.append(url)

    def _add_tokenized(url: str):
        _add(url)
        token = str(query_485_string or "").strip()
        if not token:
            return
        sep = "&" if "?" in url else "?"
        _add(f"{url}{sep}_485QueryString={token}")
        _add(f"{url}{sep}q={token}")

    normalized = path.lstrip("/")
    normalized_encoded = normalized.replace("$", "%24")

    # Try raw/encoded absolute path on origin.
    if path.startswith("http://") or path.startswith("https://"):
        _add_tokenized(path)
    elif origin:
        _add_tokenized(f"{origin}/{normalized}")
        _add_tokenized(f"{origin}/{normalized_encoded}")

        # Try app-root prefix inferred from popup URL (e.g. /HH/Z1).
        parts = [p for p in parsed_popup.path.split("/") if p]
        if len(parts) >= 2:
            app_root = "/" + "/".join(parts[:2])
            _add_tokenized(f"{origin}{app_root}/{normalized}")
            _add_tokenized(f"{origin}{app_root}/{normalized_encoded}")

    # Try configured base URL variants.
    base = str(settings.BASE_URL or "").rstrip("/")
    if base:
        _add_tokenized(f"{base}/{normalized}")
        _add_tokenized(f"{base}/{normalized_encoded}")

    for candidate in candidates:
        console.log(f"  [dim]S3b: Trying document_path URL {candidate[-60:]}[/dim]")
        raw = await _fetch_authenticated(context, candidate, timeout=5_000)
        if raw and raw.startswith(b"%PDF"):
            pdf_dir = Path(settings.PDF_DIR)
            pdf_dir.mkdir(parents=True, exist_ok=True)
            filename = f"{order_number or 'doc'}.pdf"
            filepath = pdf_dir / filename
            filepath.write_bytes(raw)
            console.log(f"  [green]✓ S3b: Downloaded PDF {len(raw):,} bytes ({filename})[/green]")
            return {"document_file_path": str(filepath)}

        if raw:
            console.log("  [dim]S3b: response was not a PDF (missing %PDF header)[/dim]")

    return None


# ── Strategy 4: screenshot fallback ──────────────────────────────────────────

async def _screenshot_fallback(popup: Page, order_number: str = None):
    console.log("  [yellow]S4: Taking screenshot fallback[/yellow]")
    try:
        raw = await popup.screenshot(full_page=True, type="png", timeout=5_000)
        # Save screenshot to file
        pdf_dir = Path(settings.PDF_DIR)
        pdf_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{order_number or 'doc'}_screenshot.png"
        filepath = pdf_dir / filename
        filepath.write_bytes(raw)
        console.log(f"  [green]✓ S4: Saved screenshot ({filename})[/green]")
        return {"document_file_path": str(filepath)}
    except Exception as e:
        console.log(f"  [red]S4 screenshot failed: {type(e).__name__}[/red]")
    return None


# ── Orchestrator ─────────────────────────────────────────────────────────────

async def _capture_with_fallbacks(context: BrowserContext, popup: Page, popup_url: str, order_number: str = None) -> dict:
    result = None

    r = await _try_download_button(context, popup, order_number)
    if r:
        result = r
    
    if not result:
        try:
            r = await _try_fetch_doc_src(context, popup, order_number)
            if r:
                result = r
        except Exception as e:
            console.log(f"  [dim]S2 error: {type(e).__name__}[/dim]")
    
    if not result:
        try:
            r = await _try_popup_url(context, popup_url, order_number)
            if r:
                result = r
        except Exception as e:
            console.log(f"  [dim]S3 error: {type(e).__name__}[/dim]")
    
    if not result:
        console.log("  [yellow]S1-S3 all failed, using S4: screenshot[/yellow]")
        try:
            result = await _screenshot_fallback(popup, order_number)
        except Exception as e:
            console.log(f"  [red]S4 ALSO failed: {type(e).__name__}[/red]")
            result = None

    if result is None:
        console.log(f"  [red]All capture strategies failed[/red]")
        return {
            "document_file_path": None,
            "document_url":       popup_url,
        }

    return {
        **result,
        "document_url": popup_url,
    }


# ── Metadata extraction from popup text ──────────────────────────────────────

async def _extract_popup_metadata(popup: Page) -> dict:
    """
    Parse the physician order text visible in the popup.
    All fields confirmed present from screenshot.
    """
    try:
        text = await popup.inner_text("body")
    except Exception:
        return {}

    meta = {}

    # MRN  e.g. "(MR#: 8981)"
    m = re.search(r"MR#?:?\s*(\d+)", text, re.IGNORECASE)
    if m:
        meta["mrn_from_doc"] = m.group(1).strip()

    # DOB
    m = re.search(r"DOB:?\s*([\d/]+)", text, re.IGNORECASE)
    if m:
        meta["dob_from_doc"] = m.group(1).strip()

    # Patient name  e.g. "Newell, Katherine (MR#:"
    m = re.search(r"([A-Z][a-zA-Z]+,\s+[A-Z][a-zA-Z\s]+?)\s*[\(\n].*?MR#?", text, re.DOTALL)
    if m:
        meta["patient_name_from_doc"] = m.group(1).strip()

    # NPI — exactly 10 digits after label
    m = re.search(r"\bNPI:?\s*#?\s*(\d{10})\b", text, re.IGNORECASE)
    if m:
        meta["npi_from_doc"] = m.group(1).strip()

    # Physician name — after "Physician" section heading
    m = re.search(
        r"Physician\s*\n\s*([A-Z][A-Za-z\s,\.]+(?:MD|DO|NP|PA|ARNP)?)\b",
        text, re.IGNORECASE
    )
    if m:
        meta["physician_name_from_doc"] = m.group(1).strip()

    # Order number  e.g. "Order No: P-24076"
    m = re.search(r"Order\s*No\.?:?\s*([\w\-]+)", text, re.IGNORECASE)
    if m:
        meta["order_number_from_doc"] = m.group(1).strip()

    # Order date
    m = re.search(r"Order\s*Date:?\s*([\d/]+)", text, re.IGNORECASE)
    if m:
        meta["order_date_from_doc"] = m.group(1).strip()

    # Primary diagnosis
    m = re.search(r"Primary\s*Diagnosis:?\s*([^\n]{3,80})", text, re.IGNORECASE)
    if m:
        meta["primary_diagnosis_from_doc"] = m.group(1).strip()

    # Certification period
    m = re.search(r"Certification\s*Period:?\s*([\d/\s\-]+)", text, re.IGNORECASE)
    if m:
        meta["certification_period_from_doc"] = m.group(1).strip()

    # Payer source
    m = re.search(r"Payer\s*Source:?\s*([^\n]{3,60})", text, re.IGNORECASE)
    if m:
        meta["payer_source_from_doc"] = m.group(1).strip()

    if meta:
        console.log(f"  [cyan]Metadata extracted: {list(meta.keys())}[/cyan]")

    return meta


# ── Helpers ──────────────────────────────────────────────────────────────────

async def _fetch_authenticated(context: BrowserContext, url: str, timeout: int = 5_000):
    try:
        resp = await context.request.get(url, timeout=timeout)
        if resp.ok:
            return await resp.body()
        console.log(f"  [dim]S2/S3: HTTP {resp.status} for {url[:60]}[/dim]")
    except Exception as e:
        console.log(f"  [dim]S2/S3: {type(e).__name__} for {url[:60]}[/dim]")
    return None


async def _find_element(page: Page, selectors: list):
    for sel in selectors:
        try:
            el = page.locator(sel).first
            count = await el.count()
            if count > 0:
                # For web components without direct click, try to find clickable child
                if "viewer-" in sel or "cr-icon-button" in sel:
                    # Try to find a button inside, or the element itself
                    child = page.locator(f"{sel} button").first
                    child_count = await child.count()
                    if child_count > 0:
                        return child
                return el
        except Exception:
            continue
    return None


def _guess_mime(name: str) -> str:
    n = (name or "").lower()
    for ext, mime in _MIME_MAP.items():
        if n.endswith(ext) or f"{ext}?" in n:
            return mime
    return "application/octet-stream"


def _get_ext_from_mime(mime: str) -> str:
    """Get file extension from bake mime type."""
    mime_to_ext = {v: k for k, v in _MIME_MAP.items()}
    return mime_to_ext.get(mime, ".bin")
