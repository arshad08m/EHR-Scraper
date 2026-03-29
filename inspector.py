"""
inspector.py — Run this BEFORE the main scraper.

Logs every dropdown, input field, button, table structure, and pagination
element on the WorldView page for YOUR specific Kantime tenant instance.

Usage:
    python inspector.py

Output: logs/inspector_report.txt
Share this file so selector IDs in core/scraper.py can be confirmed/fixed.
"""

import asyncio
from playwright.async_api import async_playwright
from rich.console import Console

from config.settings import settings
from core.auth import login

console = Console()


async def inspect():
    settings.validate()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context()
        page    = await context.new_page()

        await login(page)
        await page.goto(settings.WORLDVIEW_URL, wait_until="networkidle")

        console.print(f"Page title : {await page.title()}")
        console.print(f"URL        : {page.url}")

        lines = []

        # ── Dropdowns ─────────────────────────────────────────────────────────
        dropdowns = await page.evaluate("""
        () => Array.from(document.querySelectorAll('select')).map(s => ({
            id: s.id, name: s.name,
            options: Array.from(s.options).map(o => ({value: o.value, text: o.text})).slice(0, 8)
        }))
        """)
        lines.append("=== DROPDOWNS ===")
        for d in dropdowns:
            lines.append(f"  id='{d['id']}'  name='{d['name']}'")
            for o in d["options"]:
                lines.append(f"    option  value='{o['value']}'  →  '{o['text']}'")

        # ── Input fields ──────────────────────────────────────────────────────
        inputs = await page.evaluate("""
        () => Array.from(document.querySelectorAll('input')).map(i => ({
            id: i.id, name: i.name, type: i.type, value: i.value
        }))
        """)
        lines.append("\n=== INPUT FIELDS ===")
        for i in inputs:
            lines.append(f"  id='{i['id']}'  name='{i['name']}'  type='{i['type']}'  value='{i['value']}'")

        # ── Buttons ───────────────────────────────────────────────────────────
        buttons = await page.evaluate("""
        () => Array.from(document.querySelectorAll(
            'input[type=submit], input[type=button], button, a.btn'
        )).map(b => ({ id: b.id, text: b.value || b.innerText, href: b.href || '' }))
        """)
        lines.append("\n=== BUTTONS ===")
        for b in buttons:
            lines.append(f"  id='{b['id']}'  text='{b['text']}'  href='{b['href']}'")

        # ── Pagination ────────────────────────────────────────────────────────
        pager = await page.evaluate("""
        () => {
            const el = document.querySelector(
                '[class*="pager"], [id*="pager"], [class*="Pager"], [id*="Pager"]'
            );
            return el ? el.outerHTML.substring(0, 800) : 'NOT FOUND';
        }
        """)
        lines.append(f"\n=== PAGINATION ELEMENT ===\n{pager}")

        # ── Record count text ─────────────────────────────────────────────────
        page_info = await page.evaluate("""
        () => Array.from(document.querySelectorAll('*')).filter(el =>
            el.children.length === 0 &&
            /showing/i.test(el.textContent) &&
            /orders/i.test(el.textContent)
        ).map(el => ({ tag: el.tagName, id: el.id, text: el.textContent.trim() }))
        """)
        lines.append("\n=== RECORD COUNT TEXT ===")
        for p in page_info:
            lines.append(f"  <{p['tag']}> id='{p['id']}' → {p['text']}")

        # ── First 5 table rows ────────────────────────────────────────────────
        rows = await page.evaluate("""
        () => Array.from(document.querySelectorAll('table tr')).slice(0, 5)
              .map(r => r.outerHTML.substring(0, 500))
        """)
        lines.append("\n=== FIRST 5 TABLE ROWS (truncated HTML) ===")
        for r in rows:
            lines.append(r)

        report = "\n".join(lines)
        import os; os.makedirs("logs", exist_ok=True)
        with open("logs/inspector_report.txt", "w") as f:
            f.write(report)

        console.print(report)
        console.print("\n[green]✓ Report saved to logs/inspector_report.txt[/green]")
        console.print("Browser stays open 60 s for manual inspection...")
        await asyncio.sleep(60)
        await browser.close()


if __name__ == "__main__":
    asyncio.run(inspect())
