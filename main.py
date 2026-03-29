"""
main.py — Kantime WorldView Document Scraper entry point.

Usage:
    python main.py                              # scrape default months
    python main.py --month 2026-03             # scrape one month
    python main.py --month 2026-01 2026-02     # scrape multiple months
    python main.py --retry-failed              # retry previously failed orders
    python main.py --month 2026-03 --headless  # run without visible browser
"""

import asyncio
import argparse
from playwright.async_api import async_playwright
from rich.console import Console

from config.settings import settings
from core.auth import login
from core.scraper import scrape_month
from utils.checkpoint import get_failed_orders

console = Console()

DEFAULT_MONTHS = [
    (2026, 3),  # March 2026 — starting point
]


def parse_args():
    parser = argparse.ArgumentParser(description="Kantime WorldView Document Scraper")
    parser.add_argument("--month",         nargs="+",     help="Month(s) as YYYY-MM, e.g. 2026-03")
    parser.add_argument("--retry-failed",  action="store_true", help="Retry previously failed orders")
    parser.add_argument("--headless",      action="store_true", help="Run browser headless (default: visible)")
    return parser.parse_args()


async def main():
    args = parse_args()

    try:
        settings.validate()
    except EnvironmentError as e:
        console.print(f"[bold red]Configuration error:[/bold red] {e}")
        console.print("Copy config/.env.template → config/.env and fill in credentials.")
        return

    months = DEFAULT_MONTHS
    if args.month:
        months = []
        for m in args.month:
            try:
                year, month = map(int, m.split("-"))
                months.append((year, month))
            except ValueError:
                console.print(f"[red]Invalid month format: {m} — expected YYYY-MM[/red]")
                return

    console.rule("[bold cyan]Kantime WorldView Document Scraper[/bold cyan]")
    console.print(f"Months : {[f'{y}-{mo:02d}' for y, mo in months]}")
    console.print(f"Browser: {'headless' if args.headless else 'visible'}")
    console.print(f"Storage: {'MongoDB Atlas' if settings.MONGO_URI else 'Local JSONL (data/orders_output.jsonl)'}")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=args.headless,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            accept_downloads=True,
        )

        # Login once — session shared across all month runs
        login_page = await context.new_page()
        await login(login_page)
        await login_page.close()

        for year, month in months:
            try:
                await scrape_month(context, year, month)
            except Exception as e:
                console.print(f"[bold red]Fatal error in {year}-{month:02d}: {e}[/bold red]")
                console.print("Checkpoint saved — rerun the same command to resume.")
                break

        await browser.close()

    console.rule("[bold green]Run complete[/bold green]")
    failed = get_failed_orders()
    if failed:
        console.print(f"[yellow]{len(failed)} failed orders in utils/checkpoint.json — run --retry-failed[/yellow]")


if __name__ == "__main__":
    asyncio.run(main())
