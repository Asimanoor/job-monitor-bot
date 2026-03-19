"""
Career Scraper Stack
────────────────────
Simplified multi-strategy scraper for dynamic career websites:
  1) Local Playwright (primary — handles JS-heavy pages)
  2) Deterministic requests+BeautifulSoup fallback

Design goals:
  - Zero paid scraper APIs
  - Graceful degradation if Playwright is missing
  - Pagination-aware crawling (max 3 pages)
  - Consistent output schema for monitor.py
"""

from __future__ import annotations

import asyncio
import importlib
import json
import logging
import random
import re
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from job_scraper import extract_job_postings, is_valid_job_posting

log = logging.getLogger(__name__)

_DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_6) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]

_ATS_DOMAIN_HINTS = (
    "lever.co",
    "workable.com",
    "ashbyhq.com",
    "breezy.hr",
    "greenhouse.io",
    "applytojob.com",
    "myworkdayjobs.com",
    "smartrecruiters.com",
    "jobvite.com",
    "sapsf.eu",
    "successfactors.com",
)


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def _opening_fingerprint(opening: dict[str, str]) -> str:
    title = _normalize_whitespace(opening.get("title", "")).lower()
    link = (opening.get("link", "") or "").strip().lower()
    return f"{title}|{link}"


def _iter_jsonld_nodes(payload: object):
    if isinstance(payload, list):
        for item in payload:
            yield from _iter_jsonld_nodes(item)
        return
    if isinstance(payload, dict):
        yield payload
        graph = payload.get("@graph")
        if isinstance(graph, list):
            for node in graph:
                yield from _iter_jsonld_nodes(node)


def _extract_openings_from_jsonld(base_url: str, soup: BeautifulSoup, max_openings: int) -> list[dict[str, str]]:
    openings: list[dict[str, str]] = []
    seen: set[str] = set()

    for script in soup.find_all("script", attrs={"type": re.compile(r"application/ld\+json", re.IGNORECASE)}):
        raw = (script.string or script.get_text(" ", strip=True) or "").strip()
        if not raw:
            continue

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue

        for node in _iter_jsonld_nodes(payload):
            if not isinstance(node, dict):
                continue
            node_type = node.get("@type")
            node_types = node_type if isinstance(node_type, list) else [node_type]
            normalized_types = {str(t).strip().lower() for t in node_types if t}
            if "jobposting" not in normalized_types:
                continue

            title = _normalize_whitespace(str(node.get("title") or ""))
            link = str(node.get("url") or node.get("sameAs") or "").strip()
            if not title:
                continue
            if link:
                link = urljoin(base_url, link)

            if not is_valid_job_posting({"title": title, "apply_link": link, "job_url": link}):
                continue

            item = {"title": title, "link": link or base_url}
            fp = _opening_fingerprint(item)
            if fp in seen:
                continue
            seen.add(fp)
            openings.append(item)
            if len(openings) >= max_openings:
                return openings

    return openings


def _extract_openings_from_html(base_url: str, html: str, max_openings: int) -> tuple[str, list[dict[str, str]]]:
    soup = BeautifulSoup(html or "", "html.parser")
    page_title = _normalize_whitespace(soup.title.get_text(" ", strip=True)) if soup.title else ""

    openings: list[dict[str, str]] = []
    seen: set[str] = set()

    strict_jobs = extract_job_postings(html, base_url, max_results=max_openings)
    for job in strict_jobs:
        item = {
            "title": _normalize_whitespace(str(job.get("title") or "")),
            "link": str(job.get("apply_link") or job.get("job_url") or "").strip(),
        }
        if not item["title"] or not item["link"]:
            continue
        fp = _opening_fingerprint(item)
        if fp in seen:
            continue
        seen.add(fp)
        openings.append(item)
        if len(openings) >= max_openings:
            return page_title, openings

    for item in _extract_openings_from_jsonld(base_url, soup, max_openings=max_openings):
        fp = _opening_fingerprint(item)
        if fp in seen:
            continue
        seen.add(fp)
        openings.append(item)
        if len(openings) >= max_openings:
            return page_title, openings

    return page_title, openings


def _registrable_domain(host: str) -> str:
    normalized_host = (host or "").lower().split(":")[0]
    labels = [label for label in normalized_host.split(".") if label]
    if len(labels) >= 2:
        return ".".join(labels[-2:])
    return normalized_host.strip()


def _is_related_pagination_domain(base_host: str, candidate_host: str) -> bool:
    """Allow same host, same root domain, or known ATS hosts for related career pages."""
    left = (base_host or "").lower().strip()
    right = (candidate_host or "").lower().strip()
    if not right:
        return False

    if left == right:
        return True

    left_root = _registrable_domain(left)
    right_root = _registrable_domain(right)
    if left_root and left_root == right_root:
        return True

    if any(right.endswith(hint) for hint in _ATS_DOMAIN_HINTS):
        return True

    return False


def _extract_pagination_links(base_url: str, html: str, max_links: int = 5) -> list[str]:
    """Extract likely pagination/related-career links with controlled cross-domain support."""
    soup = BeautifulSoup(html or "", "html.parser")
    base_parsed = urlparse(base_url)
    base_host = base_parsed.netloc.lower().split(":")[0]

    _PAGINATION_TEXT_PATTERN = re.compile(
        r"\b(next|older|more|view more|load more|page|\d+)\b",
        re.IGNORECASE,
    )

    candidates: list[str] = []
    seen: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        href = (anchor.get("href") or "").strip()
        text = _normalize_whitespace(anchor.get_text(" ", strip=True)).lower()
        rel = " ".join(anchor.get("rel") or []).lower()

        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue

        absolute = urljoin(base_url, href)
        parsed = urlparse(absolute)
        absolute_lower = absolute.lower()

        if parsed.scheme not in {"http", "https"}:
            continue

        if any(
            bad in absolute_lower
            for bad in (
                "/blog", "/news", "/insight", "/service",
                "/privacy", "/cookie", "/terms", "/sitemap",
            )
        ):
            continue

        candidate_host = parsed.netloc.lower().split(":")[0]
        if not _is_related_pagination_domain(base_host, candidate_host):
            continue

        query_keys = {k.lower() for k in parse_qs(parsed.query).keys()}
        path_lower = parsed.path.lower()
        host_lower = candidate_host
        likely_page = (
            "next" in rel
            or bool(_PAGINATION_TEXT_PATTERN.search(text))
            or "page=" in absolute.lower()
            or "offset=" in absolute.lower()
            or "/page/" in path_lower
            or path_lower.endswith("/jobs")
            or path_lower.endswith("/careers")
            or "/jobs" in path_lower
            or "/career" in path_lower
            or any(host_lower.endswith(hint) for hint in _ATS_DOMAIN_HINTS)
            or {"page", "offset", "start"}.intersection(query_keys)
        )

        if not likely_page:
            continue

        token = absolute.lower().strip()
        if token in seen:
            continue
        seen.add(token)
        candidates.append(absolute)

        if len(candidates) >= max_links:
            break

    return candidates


class LocalPlaywrightScraper:
    """Scrape dynamic pages using a local Chromium browser (headless by default)."""

    def __init__(
        self,
        headless: bool = True,
        timeout_ms: int = 30_000,
        wait_until: str = "domcontentloaded",
        user_agents: list[str] | None = None,
    ) -> None:
        self.headless = bool(headless)
        self.timeout_ms = max(5_000, int(timeout_ms))
        self.wait_until = wait_until
        self.user_agents = user_agents or list(_DEFAULT_USER_AGENTS)

    async def _scrape_page(self, page, url: str) -> dict[str, Any]:
        result: dict[str, Any] = {
            "ok": False,
            "url": url,
            "final_url": url,
            "title": "",
            "html": "",
            "status_code": None,
            "error": "",
        }

        response = await page.goto(url, timeout=self.timeout_ms, wait_until=self.wait_until)
        if response is not None:
            result["status_code"] = response.status

        try:
            await page.wait_for_load_state("networkidle", timeout=min(self.timeout_ms, 10_000))
        except Exception:
            pass

        result["final_url"] = str(page.url or url)
        result["title"] = (await page.title()) or ""
        result["html"] = await page.content()
        result["ok"] = bool(result["html"])
        return result

    async def scrape_job(self, url: str) -> dict[str, Any]:
        """Scrape a single page using Playwright."""
        result: dict[str, Any] = {
            "ok": False,
            "scraper": "playwright",
            "url": url,
            "final_url": url,
            "title": "",
            "html": "",
            "json_ld": [],
            "status_code": None,
            "error": "",
        }

        browser = None
        context = None
        try:
            playwright_async_api = importlib.import_module("playwright.async_api")
            async_playwright = getattr(playwright_async_api, "async_playwright")

            ua = random.choice(self.user_agents) if self.user_agents else _DEFAULT_USER_AGENTS[0]

            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(headless=self.headless)
                context = await browser.new_context(
                    user_agent=ua,
                    locale="en-US",
                    java_script_enabled=True,
                )
                page = await context.new_page()
                single = await self._scrape_page(page, url)

                result.update(single)
                raw_json_ld = await page.locator("script[type='application/ld+json']").all_text_contents()
                parsed_json_ld: list[Any] = []
                for block in raw_json_ld:
                    text = (block or "").strip()
                    if not text:
                        continue
                    try:
                        parsed_json_ld.append(json.loads(text))
                    except json.JSONDecodeError:
                        continue
                result["json_ld"] = parsed_json_ld
                result["ok"] = bool(result.get("html"))

        except Exception as exc:
            result["error"] = str(exc)
            log.warning("Playwright scrape failed for %s: %s", url, exc)
        finally:
            try:
                if context is not None:
                    await context.close()
            except Exception:
                pass
            try:
                if browser is not None:
                    await browser.close()
            except Exception:
                pass

        return result

    async def scrape_site_openings(self, url: str, max_pages: int = 3, max_openings: int = 50) -> dict[str, Any]:
        """Crawl a career site with pagination using Playwright and collect openings."""
        output: dict[str, Any] = {
            "ok": False,
            "scraper": "playwright",
            "url": url,
            "final_url": url,
            "page_title": "",
            "pages_visited": [],
            "openings": [],
            "error": "",
        }

        browser = None
        context = None
        try:
            playwright_async_api = importlib.import_module("playwright.async_api")
            async_playwright = getattr(playwright_async_api, "async_playwright")

            queue: list[str] = [url]
            seen_pages: set[str] = set()
            seen_openings: set[str] = set()
            aggregated: list[dict[str, str]] = []

            ua = random.choice(self.user_agents) if self.user_agents else _DEFAULT_USER_AGENTS[0]

            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(headless=self.headless)
                context = await browser.new_context(
                    user_agent=ua,
                    locale="en-US",
                    java_script_enabled=True,
                )
                page = await context.new_page()

                while queue and len(seen_pages) < max(1, int(max_pages)):
                    target = queue.pop(0)
                    token = target.lower().strip()
                    if token in seen_pages:
                        continue
                    seen_pages.add(token)

                    page_data = await self._scrape_page(page, target)
                    if not page_data.get("ok"):
                        continue

                    final_url = str(page_data.get("final_url") or target)
                    html = str(page_data.get("html") or "")
                    title = str(page_data.get("title") or "")
                    output["pages_visited"].append(final_url)
                    if not output.get("page_title") and title:
                        output["page_title"] = title

                    _, openings = _extract_openings_from_html(final_url, html, max_openings=max_openings)
                    for opening in openings:
                        fp = _opening_fingerprint(opening)
                        if fp in seen_openings:
                            continue
                        seen_openings.add(fp)
                        aggregated.append(opening)
                        if len(aggregated) >= max(1, int(max_openings)):
                            break

                    if len(aggregated) >= max(1, int(max_openings)):
                        break

                    for nxt in _extract_pagination_links(final_url, html):
                        nxt_token = nxt.lower().strip()
                        if nxt_token not in seen_pages and nxt not in queue:
                            queue.append(nxt)

            output["openings"] = aggregated
            output["final_url"] = output["pages_visited"][0] if output["pages_visited"] else url
            output["ok"] = bool(output["pages_visited"])
            return output

        except Exception as exc:
            output["error"] = str(exc)
            log.warning("Playwright pagination scrape failed for %s: %s", url, exc)
            return output
        finally:
            try:
                if context is not None:
                    await context.close()
            except Exception:
                pass
            try:
                if browser is not None:
                    await browser.close()
            except Exception:
                pass

    def scrape_job_sync(self, url: str) -> dict[str, Any]:
        try:
            return asyncio.run(self.scrape_job(url))
        except RuntimeError:
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(self.scrape_job(url))
            finally:
                loop.close()

    def scrape_site_openings_sync(self, url: str, max_pages: int = 3, max_openings: int = 50) -> dict[str, Any]:
        try:
            return asyncio.run(self.scrape_site_openings(url, max_pages=max_pages, max_openings=max_openings))
        except RuntimeError:
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(self.scrape_site_openings(url, max_pages=max_pages, max_openings=max_openings))
            finally:
                loop.close()

    def scrape(self, url: str) -> dict[str, Any]:
        return self.scrape_job_sync(url)


class RequestsFallbackScraper:
    """Deterministic requests+BeautifulSoup scraper with pagination crawling."""

    def __init__(self, timeout_seconds: int = 15) -> None:
        self.timeout_seconds = max(5, int(timeout_seconds))

    def scrape_site_openings_sync(self, url: str, max_pages: int = 3, max_openings: int = 50) -> dict[str, Any]:
        session = requests.Session()
        queue: list[str] = [url]
        seen_pages: set[str] = set()
        seen_openings: set[str] = set()
        collected: list[dict[str, str]] = []
        pages_visited: list[str] = []
        page_title = ""

        headers = {
            "User-Agent": _DEFAULT_USER_AGENTS[0],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }

        while queue and len(pages_visited) < max(1, int(max_pages)):
            target = queue.pop(0)
            token = target.lower().strip()
            if token in seen_pages:
                continue
            seen_pages.add(token)

            try:
                resp = session.get(target, headers=headers, timeout=self.timeout_seconds, allow_redirects=True)
                resp.raise_for_status()
                final_url = str(resp.url or target)
                html = resp.text or ""
            except Exception:
                continue

            pages_visited.append(final_url)
            extracted_title, openings = _extract_openings_from_html(final_url, html, max_openings=max_openings)
            if not page_title and extracted_title:
                page_title = extracted_title

            for opening in openings:
                fp = _opening_fingerprint(opening)
                if fp in seen_openings:
                    continue
                seen_openings.add(fp)
                collected.append(opening)
                if len(collected) >= max(1, int(max_openings)):
                    break

            if len(collected) >= max(1, int(max_openings)):
                break

            for nxt in _extract_pagination_links(final_url, html):
                nxt_token = nxt.lower().strip()
                if nxt_token not in seen_pages and nxt not in queue:
                    queue.append(nxt)

        return {
            "ok": bool(pages_visited),
            "scraper": "requests_bs4",
            "url": url,
            "final_url": pages_visited[0] if pages_visited else url,
            "page_title": page_title,
            "pages_visited": pages_visited,
            "openings": collected,
            "error": "",
        }


class LangchainCareerScraper:
    """
    Compatibility placeholder for the "langchain" strategy.

    The unit tests monkeypatch this scraper's `scrape_site_openings_sync`,
    but production needs a best-effort implementation. We reuse the same
    deterministic requests+BS4 crawling used by RequestsFallbackScraper.
    """

    def __init__(self, timeout_seconds: int = 15) -> None:
        self.timeout_seconds = max(5, int(timeout_seconds))

    def scrape_site_openings_sync(self, url: str, max_pages: int = 3, max_openings: int = 50) -> dict[str, Any]:
        session = requests.Session()
        queue: list[str] = [url]
        seen_pages: set[str] = set()
        seen_openings: set[str] = set()
        collected: list[dict[str, str]] = []
        pages_visited: list[str] = []
        page_title = ""

        headers = {
            "User-Agent": _DEFAULT_USER_AGENTS[0],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }

        while queue and len(pages_visited) < max(1, int(max_pages)):
            target = queue.pop(0)
            token = target.lower().strip()
            if token in seen_pages:
                continue
            seen_pages.add(token)

            try:
                resp = session.get(target, headers=headers, timeout=self.timeout_seconds, allow_redirects=True)
                resp.raise_for_status()
                final_url = str(resp.url or target)
                html = resp.text or ""
            except Exception:
                continue

            pages_visited.append(final_url)
            extracted_title, openings = _extract_openings_from_html(final_url, html, max_openings=max_openings)
            if not page_title and extracted_title:
                page_title = extracted_title

            for opening in openings:
                fp = _opening_fingerprint(opening)
                if fp in seen_openings:
                    continue
                seen_openings.add(fp)
                collected.append(opening)
                if len(collected) >= max(1, int(max_openings)):
                    break

            if len(collected) >= max(1, int(max_openings)):
                break

            for nxt in _extract_pagination_links(final_url, html):
                nxt_token = nxt.lower().strip()
                if nxt_token not in seen_pages and nxt not in queue:
                    queue.append(nxt)

        return {
            "ok": bool(pages_visited),
            "scraper": "langchain",
            "url": url,
            "final_url": pages_visited[0] if pages_visited else url,
            "page_title": page_title,
            "pages_visited": pages_visited,
            "openings": collected,
            "error": "",
        }


class CrewAICareerScraper:
    """
    Deterministic requests-based implementation that matches the unit tests.

    The real project used CrewAI; here we keep the public API and behavior:
      - Crawl pagination links
      - Extract job openings from HTML via `_extract_openings_from_html`
      - Deduplicate openings
      - Provide `_crewai_normalize` hook (tests monkeypatch it)
    """

    def __init__(self, timeout_seconds: int = 15) -> None:
        self.timeout_seconds = max(5, int(timeout_seconds))

    def _crewai_normalize(self, openings: list[dict[str, str]]) -> list[dict[str, str]]:
        """Hook for tests/normalization; by default it's identity."""
        return openings

    def scrape_site_openings_sync(self, url: str, max_pages: int = 3, max_openings: int = 50) -> dict[str, Any]:
        session = requests.Session()
        queue: list[str] = [url]
        seen_pages: set[str] = set()
        pages_visited: list[str] = []
        seen_openings: set[str] = set()
        aggregated: list[dict[str, str]] = []
        page_title = ""

        headers = {
            "User-Agent": _DEFAULT_USER_AGENTS[0],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }

        while queue and len(seen_pages) < max(1, int(max_pages)):
            target = queue.pop(0)
            token = target.lower().strip()
            if token in seen_pages:
                continue
            seen_pages.add(token)

            try:
                resp = session.get(target, headers=headers, timeout=self.timeout_seconds, allow_redirects=True)
                resp.raise_for_status()
                final_url = str(resp.url or target)
                html = resp.text or ""
            except Exception:
                continue

            pages_visited.append(final_url)
            extracted_title, openings = _extract_openings_from_html(final_url, html, max_openings=max_openings)
            if extracted_title and not page_title:
                page_title = extracted_title

            openings = self._crewai_normalize(openings)
            for opening in openings:
                fp = _opening_fingerprint(opening)
                if fp in seen_openings:
                    continue
                seen_openings.add(fp)
                aggregated.append(opening)
                if len(aggregated) >= max(1, int(max_openings)):
                    break

            if len(aggregated) >= max(1, int(max_openings)):
                break

            for nxt in _extract_pagination_links(final_url, html):
                nxt_token = nxt.lower().strip()
                if nxt_token not in seen_pages and nxt not in queue:
                    queue.append(nxt)

        return {
            "ok": bool(pages_visited),
            "scraper": "crewai",
            "url": url,
            "final_url": pages_visited[0] if pages_visited else url,
            "page_title": page_title,
            "pages_visited": pages_visited,
            "openings": aggregated,
            "error": "",
        }


class MultiStrategyCareerScraper:
    """Try Playwright → LangChain → CrewAI → requests fallback."""

    def __init__(
        self,
        headless: bool = True,
        timeout_ms: int = 30_000,
        enable_playwright: bool = True,
        # Keep these params for backward compatibility but ignore them
        enable_langchain: bool = False,
        enable_crewai: bool = False,
    ) -> None:
        self.playwright = LocalPlaywrightScraper(headless=headless, timeout_ms=timeout_ms)
        self.langchain = LangchainCareerScraper(timeout_seconds=max(5, int(timeout_ms // 1000)))
        self.crewai = CrewAICareerScraper(timeout_seconds=max(5, int(timeout_ms // 1000)))
        self.requests_fallback = RequestsFallbackScraper(timeout_seconds=max(5, int(timeout_ms // 1000)))
        self.enable_playwright = bool(enable_playwright)
        self.enable_langchain = bool(enable_langchain)
        self.enable_crewai = bool(enable_crewai)

    def _requests_fallback(self, url: str, max_pages: int, max_openings: int) -> dict[str, Any]:
        """Indirection for unit tests monkeypatching."""
        return self.requests_fallback.scrape_site_openings_sync(url, max_pages=max_pages, max_openings=max_openings)

    def scrape_site_openings_sync(self, url: str, max_pages: int = 3, max_openings: int = 50) -> dict[str, Any]:
        errors: list[str] = []

        # Strategy 1: Playwright (for JS-heavy pages)
        if self.enable_playwright:
            try:
                result = self.playwright.scrape_site_openings_sync(
                    url, max_pages=max_pages, max_openings=max_openings
                )
                if result.get("ok"):
                    if errors:
                        result["fallback_errors"] = errors
                    return result
                error_text = str(result.get("error") or "Playwright did not return usable data")
                errors.append(f"playwright: {error_text}")
            except Exception as exc:
                errors.append(f"playwright: {exc}")

        # Strategy 2: LangChain
        if self.enable_langchain:
            try:
                result = self.langchain.scrape_site_openings_sync(
                    url, max_pages=max_pages, max_openings=max_openings
                )
                if result.get("ok"):
                    if errors:
                        result["fallback_errors"] = errors
                    return result
                error_text = str(result.get("error") or "LangChain did not return usable data")
                errors.append(f"langchain: {error_text}")
            except Exception as exc:
                errors.append(f"langchain: {exc}")

        # Strategy 3: CrewAI
        if self.enable_crewai:
            try:
                result = self.crewai.scrape_site_openings_sync(
                    url, max_pages=max_pages, max_openings=max_openings
                )
                if result.get("ok"):
                    if errors:
                        result["fallback_errors"] = errors
                    return result
                error_text = str(result.get("error") or "CrewAI did not return usable data")
                errors.append(f"crewai: {error_text}")
            except Exception as exc:
                errors.append(f"crewai: {exc}")

        # Strategy 4: requests + BeautifulSoup fallback
        fallback = self._requests_fallback(url, max_pages=max_pages, max_openings=max_openings)
        fallback["fallback_errors"] = errors
        if not fallback.get("ok") and not fallback.get("error"):
            fallback["error"] = "; ".join(errors) if errors else "No strategy succeeded"
        return fallback

    def scrape_job_sync(self, url: str) -> dict[str, Any]:
        """Scrape a single page (Playwright first, then requests fallback)."""
        if self.enable_playwright:
            try:
                result = self.playwright.scrape_job_sync(url)
                if result.get("ok"):
                    return result
            except Exception:
                pass

        # Fallback: use requests
        try:
            headers = {
                "User-Agent": _DEFAULT_USER_AGENTS[0],
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
            resp = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
            resp.raise_for_status()
            return {
                "ok": True,
                "scraper": "requests_bs4",
                "url": url,
                "final_url": str(resp.url or url),
                "html": resp.text or "",
                "error": "",
            }
        except Exception as exc:
            return {
                "ok": False,
                "scraper": "requests_bs4",
                "url": url,
                "final_url": url,
                "html": "",
                "error": str(exc),
            }
