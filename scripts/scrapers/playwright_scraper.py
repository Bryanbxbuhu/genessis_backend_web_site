"""
Playwright-backed page text scraper for sync-time data collection.

This module intentionally does not run during report generation. It is imported
by sync scripts only when a configured source needs JavaScript rendering.
"""

from __future__ import annotations

import re
from typing import Optional


def _clean_text(value: str) -> str:
    """Normalize browser text into compact, readable plain text."""
    if not value:
        return ""

    lines = []
    previous = None
    for raw_line in value.splitlines():
        line = re.sub(r"\s+", " ", raw_line).strip()
        if not line or line == previous:
            continue
        lines.append(line)
        previous = line

    return "\n".join(lines).strip()


async def scrape_page(
    url: str,
    wait_selector: Optional[str] = None,
    *,
    city_name: Optional[str] = None,
    timeout_seconds: float = 10.0,
) -> str | None:
    """
    Render a page with headless Chromium and return clean main-content text.

    Returns None on any failure so callers can decide whether to skip, retry,
    or record a warning. Playwright is imported lazily so the sync can continue
    when the dependency or browser binary is missing.
    """
    city_label = city_name or "unknown city"
    timeout_ms = max(1, int(float(timeout_seconds or 10.0) * 1000))
    browser = None

    print(f"   Fetching Playwright page for {city_label}: {url}")

    try:
        from playwright.async_api import TimeoutError as PlaywrightTimeoutError
        from playwright.async_api import async_playwright
    except ImportError:
        print(
            f"   WARN Playwright is not installed; skipping Playwright page for {city_label}: {url}"
        )
        return None

    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                )
            )

            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            except PlaywrightTimeoutError:
                print(f"   WARN Playwright timed out loading {city_label}: {url}")
                return None

            if wait_selector:
                try:
                    await page.wait_for_selector(wait_selector, timeout=timeout_ms)
                except PlaywrightTimeoutError:
                    print(
                        f"   WARN Playwright wait selector timed out for {city_label}: "
                        f"{wait_selector} at {url}"
                    )
                    return None

            try:
                await page.wait_for_load_state("networkidle", timeout=timeout_ms)
            except PlaywrightTimeoutError:
                # Some government pages keep analytics or long-polling requests open.
                pass

            await page.wait_for_timeout(500)

            text = await page.evaluate(
                """
                () => {
                    const removeSelectors = [
                        'script', 'style', 'noscript', 'template', 'iframe',
                        'svg', 'canvas', 'header', 'footer', 'nav', 'aside',
                        'form', '[role="navigation"]', '[role="banner"]',
                        '[role="contentinfo"]', '[aria-hidden="true"]',
                        '.advertisement', '.advertising', '.ads', '.ad',
                        '.cookie', '.cookies', '.cookie-banner',
                        '.newsletter', '.subscribe', '.social', '.share',
                        '.sidebar', '.site-footer', '.site-header',
                        '.floating-bar', '.back-to-top', '#feedback',
                        '.breadcrumb', '.breadcrumbs', '.iw-breadcrumb',
                        '.social-media-container'
                    ];

                    document
                        .querySelectorAll(removeSelectors.join(','))
                        .forEach((node) => node.remove());

                    const skipElement = (element) => {
                        if (!element || !element.tagName) return false;
                        const tag = element.tagName.toLowerCase();
                        if (['script', 'style', 'noscript', 'template', 'iframe', 'svg', 'canvas'].includes(tag)) {
                            return true;
                        }
                        const attrs = [
                            element.id || '',
                            element.className || '',
                            element.getAttribute('role') || ''
                        ].join(' ').toLowerCase();
                        return /(^|\\s)(ad|ads|advertisement|banner|cookie|footer|header|nav|newsletter|social|subscribe)(\\s|$)/.test(attrs);
                    };

                    const collectText = (node) => {
                        if (!node) return '';
                        if (node.nodeType === Node.TEXT_NODE) {
                            return node.textContent || '';
                        }
                        if (
                            node.nodeType !== Node.ELEMENT_NODE &&
                            node.nodeType !== Node.DOCUMENT_FRAGMENT_NODE
                        ) {
                            return '';
                        }

                        if (node.nodeType === Node.ELEMENT_NODE) {
                            const element = node;
                            if (skipElement(element)) return '';
                            const style = window.getComputedStyle(element);
                            if (
                                style &&
                                (style.display === 'none' || style.visibility === 'hidden')
                            ) {
                                return '';
                            }
                        }

                        const parts = [];
                        if (node.shadowRoot) {
                            parts.push(collectText(node.shadowRoot));
                        }
                        for (const child of node.childNodes || []) {
                            parts.push(collectText(child));
                        }
                        return parts.join('\\n');
                    };

                    const candidates = [
                        'main',
                        'article',
                        '[role="main"]',
                        '#main-content',
                        '#mainContent',
                        '.main-content',
                        '.main-body-content-container',
                        '.page-intro-text',
                        '#content',
                        '.content'
                    ];

                    for (const selector of candidates) {
                        const element = document.querySelector(selector);
                        const text = collectText(element).trim();
                        if (text.length >= 80) {
                            return text;
                        }
                    }

                    return collectText(document.body).trim();
                }
                """
            )

            cleaned = _clean_text(text or "")
            if not cleaned:
                print(f"   WARN Playwright returned no usable text for {city_label}: {url}")
                return None

            print(
                f"   OK Playwright scraped {len(cleaned)} characters for {city_label}: {url}"
            )
            return cleaned

    except Exception as exc:
        print(f"   WARN Playwright failed for {city_label}: {url} ({type(exc).__name__}: {exc})")
        return None
    finally:
        if browser is not None:
            try:
                await browser.close()
            except Exception:
                pass
