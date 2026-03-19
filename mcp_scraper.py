"""
Robust Career Scraper Stack
───────────────────────────
Multi-strategy scraper for dynamic career websites using:
  1) Local Playwright (primary)
  2) LangChain loader crawl (secondary)
  3) CrewAI-assisted normalization (tertiary enhancement)
  4) Deterministic requests+BeautifulSoup fallback

Design goals:
  - Zero paid scraper APIs
  - Graceful degradation if optional packages are missing
  - Pagination-aware crawling
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

log = logging.getLogger(__name__)

_DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_6) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]

_OPENING_KEYWORDS = [
    "engineer", "developer", "scientist", "analyst", "intern", "internship",
    "associate", "junior", "graduate", "trainee", "entry", "new grad",
    "software", "backend", "frontend", "full stack", "data", "ai", "ml",
    "qa", "sre", "devops", "python", "security", "architect",
]

_OPENING_IGNORE_PHRASES = [
    "privacy", "cookie", "terms", "sign in", "log in", "login", "about us",
    "contact us", "home", "learn more", "view all", "read more", "subscribe",
    "press", "investor", "culture", "benefits", "team", "faq",
]

_PAGINATION_TEXT_PATTERN = re.compile(
    r"\b(next|older|more|view more|load more|jobs|careers|page|\d+)\b",
    re.IGNORECASE,
)

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
)


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def _looks_like_opening_title(text: str) -> bool:
    clean = _normalize_whitespace(text)
    if not clean:
        return False
    lower = clean.lower()
    if len(clean) < 4 or len(clean) > 180:
        return False
    if any(bad in lower for bad in _OPENING_IGNORE_PHRASES):
        return False
    return any(keyword in lower for keyword in _OPENING_KEYWORDS)


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

    for item in _extract_openings_from_jsonld(base_url, soup, max_openings=max_openings):
        fp = _opening_fingerprint(item)
        if fp in seen:
            continue
        seen.add(fp)
        openings.append(item)
        if len(openings) >= max_openings:
            return page_title, openings

    for anchor in soup.find_all("a", href=True):
        title = _normalize_whitespace(anchor.get_text(" ", strip=True))
        if not _looks_like_opening_title(title):
            continue

        href = (anchor.get("href") or "").strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue

        absolute_link = urljoin(base_url, href)
        parsed = urlparse(absolute_link)
        if parsed.scheme not in {"http", "https"}:
            continue

        item = {"title": title, "link": absolute_link}
        fp = _opening_fingerprint(item)
        if fp in seen:
            continue

        seen.add(fp)
        openings.append(item)
        if len(openings) >= max_openings:
            break

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


def _extract_pagination_links(base_url: str, html: str, max_links: int = 12) -> list[str]:
    """Extract likely pagination/related-career links with controlled cross-domain support."""
    soup = BeautifulSoup(html or "", "html.parser")
    base_parsed = urlparse(base_url)
    base_host = base_parsed.netloc.lower().split(":")[0]

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

        if parsed.scheme not in {"http", "https"}:
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
            or "start=" in absolute.lower()
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

        # Some websites keep network connections open forever, do not fail hard.
        try:
            await page.wait_for_load_state("networkidle", timeout=min(self.timeout_ms, 10_000))
        except Exception:
            # intentionally tolerant
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

        except Exception as exc:  # pragma: no cover - runtime/browser dependent
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

    async def scrape_site_openings(self, url: str, max_pages: int = 6, max_openings: int = 200) -> dict[str, Any]:
        """
        Crawl a career site with pagination using Playwright and collect openings.
        """
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

        except Exception as exc:  # pragma: no cover
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

    def scrape_site_openings_sync(self, url: str, max_pages: int = 6, max_openings: int = 200) -> dict[str, Any]:
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


class LangChainCareerScraper:
    """Optional LangChain-based crawler; falls back gracefully if dependencies are missing."""

    def scrape_site_openings_sync(self, url: str, max_pages: int = 6, max_openings: int = 200) -> dict[str, Any]:
        output = {
            "ok": False,
            "scraper": "langchain",
            "url": url,
            "final_url": url,
            "page_title": "",
            "pages_visited": [],
            "openings": [],
            "error": "",
        }

        try:
            loader_module = importlib.import_module("langchain_community.document_loaders")
            RecursiveUrlLoader = getattr(loader_module, "RecursiveUrlLoader")
        except Exception as exc:
            output["error"] = f"LangChain unavailable: {exc}"
            return output

        try:
            loader = RecursiveUrlLoader(
                url=url,
                max_depth=2,
                use_async=False,
                extractor=lambda text: text,
                timeout=20,
            )
            docs = loader.load()

            seen_openings: set[str] = set()
            collected: list[dict[str, str]] = []

            for doc in docs[: max(1, int(max_pages))]:
                source = str((doc.metadata or {}).get("source") or url)
                page_html = str(doc.page_content or "")
                if not page_html.strip():
                    continue

                output["pages_visited"].append(source)
                page_title, openings = _extract_openings_from_html(source, page_html, max_openings=max_openings)
                if not output["page_title"] and page_title:
                    output["page_title"] = page_title

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

            output["openings"] = collected
            output["final_url"] = output["pages_visited"][0] if output["pages_visited"] else url
            output["ok"] = bool(output["pages_visited"])
            return output

        except Exception as exc:
            output["error"] = str(exc)
            return output


class CrewAICareerScraper:
    """
    CrewAI-enhanced normalizer.

    This layer keeps extraction deterministic and uses CrewAI only as an optional
    post-processing enhancement when available/configured.
    """

    def __init__(self, timeout_seconds: int = 20) -> None:
        self.timeout_seconds = max(5, int(timeout_seconds))

    def _deterministic_crawl(self, url: str, max_pages: int, max_openings: int) -> dict[str, Any]:
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
            "scraper": "crewai",
            "url": url,
            "final_url": pages_visited[0] if pages_visited else url,
            "page_title": page_title,
            "pages_visited": pages_visited,
            "openings": collected,
            "error": "",
        }

    def _crewai_normalize(self, openings: list[dict[str, str]]) -> list[dict[str, str]]:
        """
        Optionally use CrewAI to normalize noisy titles.
        If CrewAI is not available/configured, return deterministic input untouched.
        """
        try:
            crewai_module = importlib.import_module("crewai")
            Agent = getattr(crewai_module, "Agent")
            Crew = getattr(crewai_module, "Crew")
            Process = getattr(crewai_module, "Process")
            Task = getattr(crewai_module, "Task")
        except Exception:
            return openings

        if not openings:
            return openings

        # CrewAI without a configured model can fail; keep this best-effort only.
        raw = json.dumps(openings[:80], ensure_ascii=False)
        prompt = (
            "Normalize these scraped job openings into a JSON list with keys title and link. "
            "Keep only real job openings, remove navigation/marketing noise.\n"
            f"INPUT: {raw}"
        )

        try:
            agent = Agent(
                role="Career Opening Data Cleaner",
                goal="Return clean structured openings",
                backstory="Expert in extracting structured job posting signals.",
                allow_delegation=False,
                verbose=False,
            )
            task = Task(description=prompt, expected_output="Valid JSON list", agent=agent)
            crew = Crew(agents=[agent], tasks=[task], process=Process.sequential, verbose=False)
            result = crew.kickoff()
            content = str(result)

            parsed = json.loads(content)
            if not isinstance(parsed, list):
                return openings

            cleaned: list[dict[str, str]] = []
            seen: set[str] = set()
            for item in parsed:
                if not isinstance(item, dict):
                    continue
                title = _normalize_whitespace(str(item.get("title") or ""))
                link = str(item.get("link") or "").strip()
                if not title:
                    continue
                obj = {"title": title, "link": link}
                fp = _opening_fingerprint(obj)
                if fp in seen:
                    continue
                seen.add(fp)
                cleaned.append(obj)
            return cleaned or openings
        except Exception:
            return openings

    def scrape_site_openings_sync(self, url: str, max_pages: int = 6, max_openings: int = 200) -> dict[str, Any]:
        base = self._deterministic_crawl(url, max_pages=max_pages, max_openings=max_openings)
        if not base.get("ok"):
            return base

        normalized = self._crewai_normalize(base.get("openings", []))
        base["openings"] = normalized[: max(1, int(max_openings))]
        return base


class MultiStrategyCareerScraper:
    """Try Playwright -> LangChain -> CrewAI -> deterministic fallback."""

    def __init__(
        self,
        headless: bool = True,
        timeout_ms: int = 30_000,
        enable_playwright: bool = True,
        enable_langchain: bool = True,
        enable_crewai: bool = True,
    ) -> None:
        self.playwright = LocalPlaywrightScraper(headless=headless, timeout_ms=timeout_ms)
        self.langchain = LangChainCareerScraper()
        self.crewai = CrewAICareerScraper(timeout_seconds=max(5, int(timeout_ms // 1000)))

        self.enable_playwright = bool(enable_playwright)
        self.enable_langchain = bool(enable_langchain)
        self.enable_crewai = bool(enable_crewai)

    def _requests_fallback(self, url: str, max_pages: int, max_openings: int) -> dict[str, Any]:
        return self.crewai._deterministic_crawl(url, max_pages=max_pages, max_openings=max_openings) | {
            "scraper": "requests_bs4"
        }

    def scrape_site_openings_sync(self, url: str, max_pages: int = 6, max_openings: int = 200) -> dict[str, Any]:
        strategies: list[tuple[str, Any, bool]] = [
            ("playwright", self.playwright, self.enable_playwright),
            ("langchain", self.langchain, self.enable_langchain),
            ("crewai", self.crewai, self.enable_crewai),
        ]

        errors: list[str] = []

        for name, engine, enabled in strategies:
            if not enabled:
                continue
            try:
                result = engine.scrape_site_openings_sync(url, max_pages=max_pages, max_openings=max_openings)
                if result.get("ok"):
                    if errors:
                        result["fallback_errors"] = errors
                    return result
                error_text = str(result.get("error") or f"{name} did not return usable data")
                errors.append(f"{name}: {error_text}")
            except Exception as exc:
                errors.append(f"{name}: {exc}")

        fallback = self._requests_fallback(url, max_pages=max_pages, max_openings=max_openings)
        fallback["fallback_errors"] = errors
        if not fallback.get("ok") and not fallback.get("error"):
            fallback["error"] = "; ".join(errors) if errors else "No strategy succeeded"
        return fallback

