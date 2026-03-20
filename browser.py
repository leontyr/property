"""
Playwright browser session with stealth mode for Cloudflare bypass.

Zoopla uses Next.js App Router (RSC), NOT __NEXT_DATA__.
Data is in self.__next_f.push([1, "..."]) script tags.
"""
import asyncio
import json
import random
import logging
import re
from contextlib import asynccontextmanager
from typing import Optional

from playwright.async_api import async_playwright, Page, BrowserContext, Browser
from playwright_stealth import Stealth

logger = logging.getLogger(__name__)

LAUNCH_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-infobars",
    "--window-size=1920,1080",
    "--start-maximized",
    "--disable-extensions",
    "--lang=en-GB",
]

VIEWPORT = {"width": 1920, "height": 1080}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


class ZooplaBrowser:
    """Manages a single Playwright browser context reused across all scraping stages."""

    def __init__(self, headless: bool = True):
        self.headless = headless
        self._playwright = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None

    async def start(self):
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=self.headless,
            args=LAUNCH_ARGS,
        )
        self._context = await self._browser.new_context(
            viewport=VIEWPORT,
            user_agent=USER_AGENT,
            locale="en-GB",
            timezone_id="Europe/London",
            java_script_enabled=True,
            extra_http_headers={
                "Accept-Language": "en-GB,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
            },
        )
        self._stealth = Stealth(
            navigator_languages_override=("en-GB", "en"),
            navigator_platform_override="Win32",
        )
        self._page = await self._context.new_page()
        await self._stealth.apply_stealth_async(self._page)
        logger.info("Browser started (headless=%s)", self.headless)

    async def stop(self):
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        logger.info("Browser stopped")

    async def warm_up(self):
        """Visit zoopla homepage to establish session cookies."""
        logger.info("Warming up session on zoopla.co.uk...")
        await self._page.goto("https://www.zoopla.co.uk/", wait_until="domcontentloaded", timeout=30000)
        await self._human_delay(2.0, 3.5)
        try:
            accept_btn = self._page.locator('button:has-text("Accept all")')
            if await accept_btn.count() > 0:
                await accept_btn.first.click()
                await self._human_delay(0.5, 1.0)
                logger.info("Accepted cookie banner")
        except Exception:
            pass

    async def _navigate(self, url: str, wait_selector: str = None, selector_timeout: int = 8000, retries: int = 3) -> str:
        """Navigate and return full page HTML. Optionally wait for a specific selector."""
        for attempt in range(1, retries + 1):
            try:
                logger.info("Navigating to %s (attempt %d)", url, attempt)
                await self._page.goto(url, wait_until="domcontentloaded", timeout=30000)

                # Check for Cloudflare challenge
                if await self._is_cloudflare_challenge():
                    logger.warning("Cloudflare challenge detected, waiting...")
                    await self._page.wait_for_function(
                        "() => !document.title.includes('Just a moment') && !document.title.includes('Attention Required')",
                        timeout=25000,
                    )
                    logger.info("Cloudflare challenge resolved")

                # Wait for specific element if provided
                if wait_selector:
                    await self._page.wait_for_selector(wait_selector, timeout=selector_timeout)
                else:
                    # Generic wait: RSC payload should appear quickly
                    await asyncio.sleep(3)

                await self._human_delay(1.0, 2.5)
                return await self._page.content()

            except Exception as e:
                logger.warning("Attempt %d failed for %s: %s", attempt, url, e)
                if attempt == retries:
                    raise
                await asyncio.sleep(5 * attempt)

    async def get_rsc_payload(self, url: str, wait_selector: str = None, selector_timeout: int = 8000) -> str:
        """
        Navigate to URL and extract concatenated RSC payload text.
        Zoopla uses Next.js App Router (self.__next_f.push) instead of __NEXT_DATA__.
        Returns the full RSC text, which is RSC wire format lines like:
            KEY:{json}
            KEY:Tlength,text
        """
        html = await self._navigate(url, wait_selector=wait_selector, selector_timeout=selector_timeout)
        return self._extract_rsc_from_html(html)

    async def get_page_content(self, url: str, wait_selector: str = None, selector_timeout: int = 8000, retries: int = 3) -> str:
        """Navigate to URL and return full rendered HTML."""
        return await self._navigate(url, wait_selector=wait_selector, selector_timeout=selector_timeout, retries=retries)

    @staticmethod
    def _extract_rsc_from_html(html: str) -> str:
        """Extract and concatenate all self.__next_f.push([1, "..."]) payloads from HTML."""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
        chunks = []
        for s in soup.find_all("script"):
            text = s.string or ""
            if "self.__next_f" not in text:
                continue
            m = re.search(r'self\.__next_f\.push\(\[1,"(.+)"\]\)', text, re.DOTALL)
            if m:
                try:
                    decoded = m.group(1).encode().decode('unicode_escape')
                    chunks.append(decoded)
                except Exception:
                    chunks.append(m.group(1))
        return "\n".join(chunks)

    async def _is_cloudflare_challenge(self) -> bool:
        title = await self._page.title()
        return "just a moment" in title.lower() or "attention required" in title.lower()

    @staticmethod
    async def _human_delay(min_s: float = 1.5, max_s: float = 3.5):
        await asyncio.sleep(random.uniform(min_s, max_s))

    @property
    def page(self) -> Page:
        return self._page


@asynccontextmanager
async def browser_session(headless: bool = True):
    """Context manager: start browser, warm up, yield, then stop."""
    b = ZooplaBrowser(headless=headless)
    await b.start()
    await b.warm_up()
    try:
        yield b
    finally:
        await b.stop()
