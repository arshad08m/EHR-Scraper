"""
core/auth.py — Kantime login and session management.

Handles ASP.NET WebForms specifics:
  - Tries multiple common ASP.NET control ID patterns for login fields
  - Detects session expiry by URL redirect to Login.aspx
  - Auto re-login triggered by scraper on session expiry
"""

import asyncio
import os
from playwright.async_api import Page
from rich.console import Console

from config.settings import settings

console = Console()

_LOGIN_FRAGMENT   = "Login.aspx"
_SUCCESS_FRAGMENT = "WorldView"
_LOGIN_URL_MARKERS = (
    "login.aspx",
    "/accounts/authorize",
    "/identity/",
)


async def login(page: Page) -> bool:
    """
    Navigate to login page, fill credentials, submit.
    Returns True on success, raises RuntimeError on failure.
    """
    console.log("[bold]Navigating to Kantime login...[/bold]")
    await page.goto(settings.LOGIN_URL, wait_until="networkidle", timeout=30_000)

    user_field   = await _find(page, [
        "#txtUserName", "#UserName",
        "#ctl00_ContentPlaceHolder1_txtUserName",
        "input[name*='UserName']", "input[name*='user' i]",
        "input[name*='email' i]", "input[id*='user' i]",
        "input[id*='email' i]", "input[autocomplete='username']",
        "input[type='email']", "input[type='text']",
    ])
    pass_field   = await _find(page, [
        "#txtPassword", "#Password",
        "#ctl00_ContentPlaceHolder1_txtPassword",
        "input[name*='Password']", "input[name*='pass' i]",
        "input[id*='pass' i]", "input[autocomplete='current-password']",
        "input[type='password']",
    ])
    submit_btn   = await _find(page, [
        "#btnLogin", "#LoginButton",
        "#ctl00_ContentPlaceHolder1_btnLogin",
        "input[type='submit']", "button[type='submit']",
        "input[type='button']", "button",
        "input[value*='login' i]", "button:has-text('Login')",
        "button:has-text('Sign in')",
    ])

    if not (user_field and pass_field and submit_btn):
        await _write_login_debug(page)
        raise RuntimeError(
            "Could not find login form fields. "
            "Check logs/login_debug.txt for discovered fields and frame info."
        )

    await user_field.fill(settings.USERNAME)
    await pass_field.fill(settings.PASSWORD)

    console.log("Submitting credentials...")
    try:
        async with page.expect_navigation(wait_until="networkidle", timeout=30_000):
            await submit_btn.click()
    except Exception:
        # Some tenant variants submit via JS without a full navigation.
        await submit_btn.click()
        await page.wait_for_timeout(2_000)

    if _LOGIN_FRAGMENT in page.url:
        snippet = (await page.text_content("body") or "")[:300]
        raise RuntimeError(f"Login failed — check credentials. Page: {snippet}")

    console.log(f"[green]✓ Logged in. URL: {page.url}[/green]")
    return True


async def ensure_logged_in(page: Page) -> bool:
    """
    Returns True if session is alive.
    If not, re-logs in and returns False so caller can re-apply filters.
    """
    if _is_auth_page_url(page.url) or await _looks_like_login_form(page):
        console.log("[yellow]Session expired — re-logging in...[/yellow]")
        await login(page)
        await page.goto(settings.WORLDVIEW_URL, wait_until="networkidle", timeout=30_000)
        return False
    return True


def _is_auth_page_url(url: str) -> bool:
    lowered = str(url or "").lower()
    return any(marker in lowered for marker in _LOGIN_URL_MARKERS)


async def _looks_like_login_form(page: Page) -> bool:
    # Some tenants route to auth URLs that do not contain Login.aspx.
    # A password field on the root page is a reliable signal that auth is required.
    selectors = [
        "input[type='password']",
        "input[name*='password' i]",
        "#txtPassword",
        "#Password",
    ]
    for sel in selectors:
        try:
            if await page.locator(sel).count() > 0:
                return True
        except Exception:
            continue
    return False


async def _find(page: Page, selectors: list):
    roots = [page] + list(page.frames)
    for root in roots:
        for sel in selectors:
            try:
                el = root.locator(sel).first
                if await el.count() > 0:
                    return el
            except Exception:
                continue
    return None


async def _write_login_debug(page: Page):
    os.makedirs("logs", exist_ok=True)
    lines = []

    for idx, frame in enumerate(page.frames):
        frame_name = frame.name or "(no-name)"
        frame_url = frame.url or "(no-url)"
        lines.append(f"=== FRAME {idx} ===")
        lines.append(f"name: {frame_name}")
        lines.append(f"url : {frame_url}")

        try:
            inputs = await frame.evaluate(
                """
                () => Array.from(document.querySelectorAll('input')).map(i => ({
                    id: i.id || '',
                    name: i.name || '',
                    type: i.type || '',
                    value: (i.value || '').slice(0, 40)
                }))
                """
            )
            buttons = await frame.evaluate(
                """
                () => Array.from(document.querySelectorAll('button, input[type=submit], input[type=button]'))
                      .map(b => ({
                          id: b.id || '',
                          name: b.name || '',
                          text: (b.innerText || b.value || '').trim().slice(0, 80)
                      }))
                """
            )
        except Exception as e:
            lines.append(f"error: could not inspect frame ({e})")
            lines.append("")
            continue

        lines.append("inputs:")
        for item in inputs:
            lines.append(
                f"  id='{item['id']}' name='{item['name']}' type='{item['type']}' value='{item['value']}'"
            )

        lines.append("buttons:")
        for item in buttons:
            lines.append(
                f"  id='{item['id']}' name='{item['name']}' text='{item['text']}'"
            )

        lines.append("")

    with open("logs/login_debug.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
