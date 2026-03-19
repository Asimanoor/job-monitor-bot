"""
Quick scraper validation script.

Usage:
    python test_scraper.py
    python test_scraper.py --url https://example.com/careers
"""

from __future__ import annotations

import argparse

import requests

from config_loader import ConfigLoader
from monitor import LINKS_FILE, _extract_openings_from_html


def _pick_default_url() -> str:
    urls = ConfigLoader.load_urls(LINKS_FILE)
    return urls[0] if urls else ""


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Playwright + fallback scraper on a career page")
    parser.add_argument("--url", default="", help="Career page URL to test")
    parser.add_argument("--timeout", type=int, default=30, help="Playwright timeout in seconds")
    args = parser.parse_args()

    url = args.url.strip() or _pick_default_url()
    if not url:
        print("No URL provided and links.txt is empty.")
        return 1

    scraper = None
    playwright_import_error = ""
    try:
        from mcp_scraper import LocalPlaywrightScraper

        scraper = LocalPlaywrightScraper(headless=True, timeout_ms=max(5, args.timeout) * 1000)
    except Exception as exc:
        playwright_import_error = str(exc)

    result = scraper.scrape_job_sync(url) if scraper is not None else {"ok": False, "error": playwright_import_error}

    html = ""
    scraper_used = "playwright"
    resolved_url = url

    if result.get("ok") and result.get("html"):
        html = str(result.get("html") or "")
        resolved_url = str(result.get("final_url") or url)
        scraper_used = str(result.get("scraper") or "playwright")
    else:
        print(f"Playwright failed: {result.get('error', 'unknown error')} -> falling back to requests")
        response = requests.get(url, timeout=max(5, args.timeout))
        response.raise_for_status()
        html = response.text
        resolved_url = response.url
        scraper_used = "requests"

    page_title, openings = _extract_openings_from_html(resolved_url, html, max_positions=30)

    print("=" * 60)
    print(f"URL           : {url}")
    print(f"Resolved URL  : {resolved_url}")
    print(f"Scraper used  : {scraper_used}")
    print(f"Page title    : {page_title}")
    print(f"Openings found: {len(openings)}")
    print("=" * 60)

    for idx, opening in enumerate(openings[:10], start=1):
        title = opening.get("title", "")
        link = opening.get("link", "")
        print(f"{idx:02d}. {title} -> {link}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Interrupted by user")
        raise SystemExit(130)
