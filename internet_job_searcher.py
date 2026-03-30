"""
Internet Fallback URL Discovery
───────────────────────────────
Resilient multi-engine fallback that discovers career page URLs for companies.

Pipeline:
    DuckDuckGo → Bing → Google → Direct ATS probing → Cache fallback

Key reliability features:
    - timeout=(5, 30) (connect, read)
    - retries=3 with backoff [2s, 5s, 10s]
    - rotating browser-like headers / user-agent
    - 403/429/timeout penalty sleep (2–8s)
    - async parallel company search (up to 5 concurrent)
    - failure suppression (one warning per company+engine, not per query)
    - JSON cache reuse (`cache/search_cache.json`, TTL 24h)
    - optional proxy support via HTTP_PROXY / HTTPS_PROXY

NOTE:
    This module returns discovered career URLs only.
    Job extraction is handled by the main scraping pipeline.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import requests
from bs4 import BeautifulSoup

try:  # Optional dependency; code gracefully falls back if unavailable.
    import aiohttp  # type: ignore
except Exception:  # pragma: no cover
    aiohttp = None  # type: ignore

log = logging.getLogger(__name__)


_CACHE_TTL_SECONDS = 24 * 60 * 60
_RETRY_BACKOFF_SECONDS = [2, 5, 10]
_MAX_COMPANY_CONCURRENCY = 5

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Edg/126.0.0.0 Safari/537.36",
]

_JOB_SITE_KEYWORDS = {
    "career",
    "careers",
    "job",
    "jobs",
    "hiring",
    "position",
    "positions",
    "opening",
    "openings",
    "vacancy",
    "vacancies",
    "apply",
    "recruit",
    "recruitment",
}

_CAREER_URL_HINTS = {
    "career",
    "careers",
    "job",
    "jobs",
    "opening",
    "openings",
    "vacancy",
    "vacancies",
    "apply",
    "recruit",
    "recruitment",
    "lever.co",
    "greenhouse.io",
    "workable.com",
    "breezy.hr",
    "ashbyhq.com",
    "myworkdayjobs.com",
    "workdayjobs.com",
    "smartrecruiters.com",
    "jobvite.com",
    "applytojob.com",
}

_NON_CAREER_URL_HINTS = {
    "blog",
    "news",
    "article",
    "privacy",
    "terms",
    "about",
    "contact",
    "pricing",
    "feature",
    "product",
    "service",
}


def _run_async(coro):
    """Run async coroutine from sync context safely."""
    try:
        return asyncio.run(coro)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()


class SearchCache:
    """Simple JSON cache for per-company search results."""

    def __init__(self, cache_file: str, ttl_seconds: int = _CACHE_TTL_SECONDS) -> None:
        self.cache_file = cache_file
        self.ttl_seconds = max(60, int(ttl_seconds))
        self._data: dict[str, dict[str, Any]] = {}
        self._load()

    @staticmethod
    def _company_key(company_name: str) -> str:
        cleaned = re.sub(r"[^a-z0-9]+", " ", (company_name or "").strip().lower())
        return " ".join(cleaned.split())

    def _load(self) -> None:
        if not os.path.isfile(self.cache_file):
            self._data = {}
            return
        try:
            with open(self.cache_file, "r", encoding="utf-8") as f:
                raw = json.load(f)
            self._data = raw if isinstance(raw, dict) else {}
        except Exception:
            self._data = {}

    def _save(self) -> None:
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(self._data, f, ensure_ascii=False, indent=2)
        except Exception as exc:
            log.debug("Search cache save failed: %s", exc)

    def get_fresh(self, company_name: str, max_results: int) -> list[dict[str, str]]:
        key = self._company_key(company_name)
        item = self._data.get(key) or {}
        ts = float(item.get("timestamp", 0) or 0)
        if ts <= 0:
            return []
        if (time.time() - ts) > self.ttl_seconds:
            return []
        results = item.get("results")
        if not isinstance(results, list):
            return []
        return [r for r in results if isinstance(r, dict)][:max_results]

    def get_stale(self, company_name: str, max_results: int) -> list[dict[str, str]]:
        key = self._company_key(company_name)
        item = self._data.get(key) or {}
        results = item.get("results")
        if not isinstance(results, list):
            return []
        return [r for r in results if isinstance(r, dict)][:max_results]

    def set(self, company_name: str, engine: str, results: list[dict[str, str]]) -> None:
        key = self._company_key(company_name)
        self._data[key] = {
            "timestamp": time.time(),
            "engine": str(engine or ""),
            "results": [r for r in results if isinstance(r, dict)],
        }
        self._save()


class CompanyNameExtractor:
    """Extracts company names from career page URLs."""

    @staticmethod
    def extract_from_url(url: str) -> str | None:
        """Extract company name from URL using various heuristics."""
        try:
            parsed = urlparse(url)
        except Exception:
            return None

        host = (parsed.netloc or "").lower().strip()
        if host.startswith("www."):
            host = host[4:]

        # Try to extract from domain
        host = host.split(":")[0]

        # Try query parameters
        query = parse_qs(parsed.query or "")
        for key in ("company", "organization", "org", "tenant", "client"):
            if key in query and query[key]:
                return query[key][0].strip()

        # ATS platform patterns
        if host.endswith("lever.co"):
            path_parts = [p for p in (parsed.path or "").split("/") if p.strip()]
            if path_parts:
                return path_parts[0].strip()

        if host.endswith("ashbyhq.com"):
            path_parts = [p for p in (parsed.path or "").split("/") if p.strip()]
            if path_parts:
                return path_parts[0].strip()

        if host.endswith("workable.com"):
            path_parts = [p for p in (parsed.path or "").split("/") if p.strip()]
            if path_parts:
                return path_parts[0].strip()

        if host.endswith("breezy.hr"):
            first_label = host.split(".")[0]
            if first_label and first_label not in {"careers", "jobs"}:
                return first_label.strip()

        # Try first domain label
        labels = [label for label in host.split(".") if label]
        if len(labels) >= 2:
            # Skip generic domain labels
            skip_labels = {"careers", "jobs", "hr", "apply", "recruit", "com", "io"}
            for label in labels:
                if label.lower() not in skip_labels:
                    return label.strip()

        return None


class InternetJobSearcher:
    """Search internet for job openings related to companies."""

    def __init__(
        self,
        timeout_seconds: int = 10,
        headers: dict | None = None,
        query_variants_limit: int = 5,
        provider_fail_threshold: int = 3,
        provider_block_cooldown_seconds: int = 1800,
        enable_bing_fallback: bool = True,
        enable_google_fallback: bool = True,
        retries: int = 3,
        retry_backoff_seconds: list[int] | None = None,
        cache_ttl_seconds: int = _CACHE_TTL_SECONDS,
        cache_file: str | None = None,
    ):
        # Required reliability timeout tuple: (connect, read)
        self.connect_timeout = 5
        self.read_timeout = max(30, int(timeout_seconds))
        self.timeout_tuple = (self.connect_timeout, self.read_timeout)

        self.headers = headers or self._default_headers()
        # Keep only top 5 query variants (requirement: strict query reduction).
        self.query_variants_limit = 5
        self.provider_fail_threshold = max(1, int(provider_fail_threshold))
        self.provider_block_cooldown_seconds = max(60, int(provider_block_cooldown_seconds))
        self.enable_bing_fallback = bool(enable_bing_fallback)
        self.enable_google_fallback = bool(enable_google_fallback)
        self.retries = max(1, int(retries))
        self.retry_backoff_seconds = list(retry_backoff_seconds or _RETRY_BACKOFF_SECONDS)

        self._provider_blocked_until: dict[str, float] = {
            "duckduckgo": 0.0,
            "bing": 0.0,
            "google": 0.0,
            "ats": 0.0,
        }

        self.http_proxy = os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy") or ""
        self.https_proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy") or ""

        root = os.path.dirname(os.path.abspath(__file__))
        default_cache = os.path.join(root, "cache", "search_cache.json")
        self.cache = SearchCache(cache_file or default_cache, ttl_seconds=cache_ttl_seconds)

    @staticmethod
    def _is_timeout_error(exc: Exception) -> bool:
        msg = str(exc).lower()
        timeout_tokens = ["timeout", "timed out", "connecttimeout", "readtimeout", "sock_read"]
        return any(token in msg for token in timeout_tokens)

    @staticmethod
    def _now_epoch() -> float:
        return time.time()

    def _provider_is_temporarily_blocked(self, provider: str) -> bool:
        return self._now_epoch() < float(self._provider_blocked_until.get(provider, 0.0) or 0.0)

    def _mark_provider_blocked(self, provider: str, reason: str) -> None:
        blocked_until = self._now_epoch() + float(self.provider_block_cooldown_seconds)
        self._provider_blocked_until[provider] = max(
            float(self._provider_blocked_until.get(provider, 0.0) or 0.0),
            blocked_until,
        )
        log.warning(
            "%s temporarily blocked (%s). Cooling down for %ds.",
            provider,
            reason,
            self.provider_block_cooldown_seconds,
        )

    async def _sleep_penalty(self) -> None:
        await asyncio.sleep(random.uniform(2.0, 8.0))

    def search_temporarily_unavailable(self) -> bool:
        engines = ["duckduckgo"]
        if self.enable_bing_fallback:
            engines.append("bing")
        if self.enable_google_fallback:
            engines.append("google")
        engines.append("ats")
        return all(self._provider_is_temporarily_blocked(name) for name in engines)

    @staticmethod
    def _default_headers() -> dict[str, str]:
        """Return browser-like base headers."""
        return {
            "Accept": "text/html",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }

    def _build_headers(self) -> dict[str, str]:
        h = dict(self.headers)
        h["User-Agent"] = random.choice(_USER_AGENTS)
        h["Accept"] = h.get("Accept") or "text/html"
        h["Accept-Language"] = h.get("Accept-Language") or "en-US,en;q=0.9"
        h["Connection"] = h.get("Connection") or "keep-alive"
        return h

    def _proxy_for_url(self, url: str) -> str | None:
        scheme = (urlparse(url).scheme or "https").lower()
        if scheme == "https" and self.https_proxy:
            return self.https_proxy
        if self.http_proxy:
            return self.http_proxy
        return None

    @staticmethod
    def _clean_company_query(company_name: str) -> str:
        cleaned = re.sub(r"[\-_+]+", " ", (company_name or "").strip())
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned

    @classmethod
    def _build_company_query_variants(cls, company_name: str) -> list[str]:
        # Intentionally reduced set to avoid rate limits (top 5 only).
        company = cls._clean_company_query(company_name)
        return [
            f'"{company}" careers',
            f'"{company}" jobs pakistan',
            f'"{company}" associate software engineer',
            f'"{company}" site:lever.co',
            f'"{company}" site:greenhouse.io',
        ]

    @staticmethod
    def _decode_bing_u_param(encoded: str) -> str:
        raw = (encoded or "").strip()
        if not raw:
            return ""
        # Bing often returns u=a1<base64>
        if raw.startswith("a1"):
            raw = raw[2:]
        # URL-safe padding
        padding = "=" * (-len(raw) % 4)
        try:
            decoded = base64.urlsafe_b64decode((raw + padding).encode("utf-8")).decode("utf-8", errors="ignore")
            return decoded.strip()
        except Exception:
            return ""

    @staticmethod
    def _normalize_result_url(url: str) -> str:
        raw = (url or "").strip()
        if not raw:
            return ""

        if "duckduckgo.com/l/?" in raw:
            parsed = urlparse(raw)
            q = parse_qs(parsed.query)
            uddg_values = q.get("uddg") or []
            if uddg_values and uddg_values[0]:
                raw = unquote(uddg_values[0])

        parsed = urlparse(raw)
        if parsed.netloc.endswith("bing.com") and parsed.path.startswith("/ck/a"):
            q = parse_qs(parsed.query)
            encoded_u = (q.get("u") or [""])[0]
            decoded = InternetJobSearcher._decode_bing_u_param(encoded_u)
            if decoded:
                raw = decoded

        parsed = urlparse(raw)
        if parsed.netloc.endswith("google.com") and parsed.path.startswith("/url"):
            q = parse_qs(parsed.query)
            target = (q.get("q") or [""])[0]
            if target:
                raw = target

        parsed = urlparse(raw)
        if parsed.scheme not in {"http", "https"}:
            return ""

        return parsed._replace(fragment="").geturl()

    @staticmethod
    def _dedupe_results(results: list[dict[str, str]], max_results: int) -> list[dict[str, str]]:
        merged: list[dict[str, str]] = []
        seen: set[str] = set()
        for item in results:
            key = f"{str(item.get('url', '')).lower()}|{str(item.get('title', '')).lower()}"
            if not key or key in seen:
                continue
            seen.add(key)
            merged.append(item)
            if len(merged) >= max_results:
                break
        return merged

    @staticmethod
    def _domain_key(url: str) -> str:
        try:
            host = (urlparse(url).netloc or "").lower().strip()
        except Exception:
            return ""
        if host.startswith("www."):
            host = host[4:]
        return host

    def extract_career_urls_from_results(
        self,
        search_results: list[dict[str, str]],
        max_urls: int = 12,
    ) -> list[str]:
        """Extract normalized career-like URLs from search result documents."""
        urls: list[str] = []
        seen_url: set[str] = set()
        seen_domain: set[str] = set()

        for result in search_results:
            raw_url = str(result.get("url") or "").strip()
            normalized = self._normalize_result_url(raw_url)
            if not normalized:
                continue

            lowered = normalized.lower()
            if any(token in lowered for token in _NON_CAREER_URL_HINTS):
                continue

            if not any(token in lowered for token in _CAREER_URL_HINTS):
                continue

            canonical = normalized.rstrip("/")
            domain = self._domain_key(canonical)

            if canonical in seen_url:
                continue
            # Keep only one URL per domain to avoid same-domain variants.
            if domain and domain in seen_domain:
                continue

            seen_url.add(canonical)
            if domain:
                seen_domain.add(domain)
            urls.append(canonical)

            if len(urls) >= max_urls:
                break

        return urls

    @staticmethod
    def _extract_title_and_snippet(soup: BeautifulSoup) -> tuple[str, str]:
        title = ""
        snippet = ""
        if soup.title:
            title = soup.title.get_text(" ", strip=True)
        meta_desc = soup.find("meta", attrs={"name": re.compile(r"description", re.IGNORECASE)})
        if meta_desc:
            snippet = str(meta_desc.get("content") or "").strip()
        return title, snippet

    def _parse_duckduckgo_results(self, html: str, query: str, max_results: int) -> list[dict[str, str]]:
        soup = BeautifulSoup(html or "", "html.parser")
        rows: list[dict[str, str]] = []

        cards = soup.select("div.result") or soup.select(".result.results_links.results_links_deep")
        for card in cards:
            if len(rows) >= max_results:
                break
            link_elem = card.select_one("a.result__a") or card.select_one("h2.result__title a")
            if link_elem is None:
                continue

            title = link_elem.get_text(" ", strip=True)
            href = self._normalize_result_url(str(link_elem.get("href") or ""))
            if not title or not href:
                continue

            snippet_elem = card.select_one("a.result__snippet") or card.select_one("div.result__snippet")
            snippet = snippet_elem.get_text(" ", strip=True) if snippet_elem else ""
            rows.append(
                {
                    "title": title,
                    "url": href,
                    "snippet": snippet,
                    "source": "duckduckgo",
                    "query": query,
                }
            )

        return self._dedupe_results(rows, max_results)

    def _parse_bing_results(self, html: str, query: str, max_results: int) -> list[dict[str, str]]:
        soup = BeautifulSoup(html or "", "html.parser")
        rows: list[dict[str, str]] = []
        for card in soup.select("li.b_algo"):
            if len(rows) >= max_results:
                break

            link_elem = card.select_one("h2 a")
            if link_elem is None:
                continue

            title = link_elem.get_text(" ", strip=True)
            href = self._normalize_result_url(str(link_elem.get("href") or ""))
            if not title or not href:
                continue

            snippet_elem = card.select_one("div.b_caption p") or card.select_one("p")
            snippet = snippet_elem.get_text(" ", strip=True) if snippet_elem else ""
            rows.append(
                {
                    "title": title,
                    "url": href,
                    "snippet": snippet,
                    "source": "bing",
                    "query": query,
                }
            )

        return self._dedupe_results(rows, max_results)

    def _parse_google_results(self, html: str, query: str, max_results: int) -> list[dict[str, str]]:
        soup = BeautifulSoup(html or "", "html.parser")
        rows: list[dict[str, str]] = []

        link_nodes = soup.select("div.yuRUbf a") or soup.select("div.g a")
        for link_elem in link_nodes:
            if len(rows) >= max_results:
                break

            href = self._normalize_result_url(str(link_elem.get("href") or ""))
            if not href:
                continue

            title = link_elem.get_text(" ", strip=True)
            if not title:
                parent = link_elem.find_parent("div")
                if parent is not None:
                    h3 = parent.select_one("h3")
                    if h3 is not None:
                        title = h3.get_text(" ", strip=True)
            if not title:
                continue

            snippet = ""
            parent = link_elem.find_parent("div")
            if parent is not None:
                snippet_node = parent.select_one("div.VwiC3b") or parent.select_one("span.aCOpRe")
                if snippet_node is not None:
                    snippet = snippet_node.get_text(" ", strip=True)

            rows.append(
                {
                    "title": title,
                    "url": href,
                    "snippet": snippet,
                    "source": "google",
                    "query": query,
                }
            )

        return self._dedupe_results(rows, max_results)

    async def _fetch_once_async(
        self,
        url: str,
        params: dict[str, str],
        aio_session: Any | None = None,
    ) -> tuple[int, str]:
        headers = self._build_headers()
        proxy = self._proxy_for_url(url)

        if aiohttp is not None and aio_session is not None:
            timeout = aiohttp.ClientTimeout(
                total=self.connect_timeout + self.read_timeout + 5,
                connect=self.connect_timeout,
                sock_connect=self.connect_timeout,
                sock_read=self.read_timeout,
            )
            async with aio_session.get(
                url,
                params=params,
                headers=headers,
                timeout=timeout,
                proxy=proxy,
            ) as resp:
                body = await resp.text(errors="ignore")
                return int(resp.status), body

        # Fallback path if aiohttp isn't available.
        def _request_sync() -> tuple[int, str]:
            proxies = {}
            if self.http_proxy:
                proxies["http"] = self.http_proxy
            if self.https_proxy:
                proxies["https"] = self.https_proxy
            response = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=self.timeout_tuple,
                allow_redirects=True,
                proxies=proxies or None,
            )
            return int(response.status_code), str(response.text or "")

        return await asyncio.to_thread(_request_sync)

    async def _fetch_with_retries(
        self,
        provider: str,
        url: str,
        params: dict[str, str],
        aio_session: Any | None = None,
    ) -> tuple[str, str | None]:
        """Fetch URL with retries/backoff and normalized error codes."""
        max_attempts = 1 + self.retries
        last_error_code: str | None = None

        for attempt in range(max_attempts):
            try:
                status, body = await self._fetch_once_async(url, params, aio_session=aio_session)

                if status in {403, 429}:
                    last_error_code = "blocked" if status == 403 else "rate_limited"
                    await self._sleep_penalty()
                    if attempt < self.retries:
                        backoff = self.retry_backoff_seconds[min(attempt, len(self.retry_backoff_seconds) - 1)]
                        await asyncio.sleep(float(backoff))
                        continue
                    self._mark_provider_blocked(provider, f"HTTP {status}")
                    return "", last_error_code

                if status >= 500:
                    last_error_code = f"http_{status}"
                    if attempt < self.retries:
                        backoff = self.retry_backoff_seconds[min(attempt, len(self.retry_backoff_seconds) - 1)]
                        await asyncio.sleep(float(backoff))
                        continue
                    return "", last_error_code

                if status >= 400:
                    return "", f"http_{status}"

                return body, None

            except Exception as exc:
                if self._is_timeout_error(exc):
                    last_error_code = "timeout"
                    await self._sleep_penalty()
                else:
                    last_error_code = "request_error"

                if attempt < self.retries:
                    backoff = self.retry_backoff_seconds[min(attempt, len(self.retry_backoff_seconds) - 1)]
                    await asyncio.sleep(float(backoff))
                    continue

                if last_error_code in {"timeout", "request_error"}:
                    self._mark_provider_blocked(provider, last_error_code)
                return "", last_error_code

        return "", last_error_code or "request_error"

    async def _run_provider_queries(
        self,
        provider: str,
        company_name: str,
        queries: list[str],
        max_results: int,
        aio_session: Any | None = None,
    ) -> tuple[list[dict[str, str]], int, list[str]]:
        if self._provider_is_temporarily_blocked(provider):
            return [], 0, ["blocked"]

        endpoint = {
            "duckduckgo": "https://duckduckgo.com/html/",
            "bing": "https://www.bing.com/search",
            "google": "https://www.google.com/search",
        }.get(provider, "")
        if not endpoint:
            return [], 0, ["unsupported_provider"]

        parser = {
            "duckduckgo": self._parse_duckduckgo_results,
            "bing": self._parse_bing_results,
            "google": self._parse_google_results,
        }[provider]

        merged: list[dict[str, str]] = []
        attempted = 0
        errors: list[str] = []
        failure_streak = 0

        for query in queries[: self.query_variants_limit]:
            if len(merged) >= max_results:
                break

            attempted += 1
            params = {"q": query}
            if provider == "duckduckgo":
                params["kl"] = "us-en"
            elif provider == "bing":
                params["setlang"] = "en-US"
            elif provider == "google":
                params["hl"] = "en"

            html, error_code = await self._fetch_with_retries(
                provider,
                endpoint,
                params,
                aio_session=aio_session,
            )
            if error_code is not None:
                errors.append(error_code)
                failure_streak += 1
                if error_code in {"blocked", "rate_limited"}:
                    break
                if failure_streak >= self.provider_fail_threshold:
                    break
                continue

            failure_streak = 0
            parsed = parser(html, query=query, max_results=max_results)
            merged = self._dedupe_results(merged + parsed, max_results=max_results)

        if not merged and errors:
            compressed = ", ".join(sorted(set(errors)))
            log.warning("Search engine failed for company %s via %s (%s)", company_name, provider, compressed)

        return merged[:max_results], attempted, errors

    @staticmethod
    def _ats_candidate_urls(company_name: str) -> list[str]:
        raw = (company_name or "").strip().lower()
        slug_dash = re.sub(r"[^a-z0-9]+", "-", raw).strip("-")
        slug = re.sub(r"[^a-z0-9]+", "", raw)
        candidates = [
            f"https://{slug}.lever.co",
            f"https://jobs.{slug}.com",
            f"https://boards.greenhouse.io/{slug}",
            f"https://{slug}.breezy.hr",
            f"https://apply.workable.com/{slug}",
            f"https://{slug}.workdayjobs.com",
        ]
        if slug_dash and slug_dash != slug:
            candidates.extend(
                [
                    f"https://{slug_dash}.lever.co",
                    f"https://boards.greenhouse.io/{slug_dash}",
                    f"https://apply.workable.com/{slug_dash}",
                ]
            )
        # Dedup preserve order
        seen: set[str] = set()
        unique: list[str] = []
        for u in candidates:
            if u not in seen:
                seen.add(u)
                unique.append(u)
        return unique

    async def _probe_ats(
        self,
        company_name: str,
        max_results: int,
        aio_session: Any | None = None,
    ) -> tuple[list[dict[str, str]], int, list[str]]:
        if self._provider_is_temporarily_blocked("ats"):
            return [], 0, ["blocked"]

        urls = self._ats_candidate_urls(company_name)
        attempted = 0
        errors: list[str] = []
        rows: list[dict[str, str]] = []

        async def _probe(url: str) -> dict[str, str] | None:
            nonlocal attempted
            attempted += 1

            html, error_code = await self._fetch_with_retries(
                "ats",
                url,
                {},
                aio_session=aio_session,
            )
            if error_code is not None:
                errors.append(error_code)
                return None

            lower = (html or "").lower()
            if not any(token in lower for token in _JOB_SITE_KEYWORDS):
                return None

            soup = BeautifulSoup(html or "", "html.parser")
            title, snippet = self._extract_title_and_snippet(soup)
            title = title or f"{self._clean_company_query(company_name)} careers"

            return {
                "title": title,
                "url": self._normalize_result_url(url),
                "snippet": snippet or "Direct ATS discovery",
                "source": "ats",
                "query": url,
            }

        tasks = [_probe(u) for u in urls]
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        for item in responses:
            if isinstance(item, dict):
                rows.append(item)

        rows = self._dedupe_results(rows, max_results=max_results)
        if not rows and errors:
            compressed = ", ".join(sorted(set(errors)))
            log.warning("Search engine failed for company %s via ats (%s)", company_name, compressed)
        return rows, attempted, errors

    async def _search_company_openings_async(
        self,
        company_name: str,
        max_results: int = 10,
    ) -> tuple[list[dict[str, str]], dict[str, Any]]:
        """Search with resilient fallback chain and return (results, metrics)."""
        started_at = time.monotonic()
        company = self._clean_company_query(company_name)

        fresh = self.cache.get_fresh(company, max_results=max_results)
        if fresh:
            metrics = {
                "company": company,
                "engine_used": "cache",
                "queries_attempted": 0,
                "results_found": len(fresh),
                "fallback_used": False,
                "jobs_extracted": 0,
                "duration_ms": int((time.monotonic() - started_at) * 1000),
            }
            return fresh, metrics

        engine_used = "none"
        queries_attempted = 0
        fallback_used = False
        results: list[dict[str, str]] = []

        async def _execute(aio_session: Any | None) -> tuple[list[dict[str, str]], dict[str, Any]]:
            nonlocal engine_used, queries_attempted, fallback_used, results
            queries = self._build_company_query_variants(company)

            ddg_results, attempted, _errors = await self._run_provider_queries(
                "duckduckgo",
                company_name=company,
                queries=queries,
                max_results=max_results,
                aio_session=aio_session,
            )
            queries_attempted += attempted
            if ddg_results:
                engine_used = "duckduckgo"
                results = ddg_results

            if not results and self.enable_bing_fallback:
                fallback_used = True
                bing_results, attempted, _errors = await self._run_provider_queries(
                    "bing",
                    company_name=company,
                    queries=queries,
                    max_results=max_results,
                    aio_session=aio_session,
                )
                queries_attempted += attempted
                if bing_results:
                    engine_used = "bing"
                    results = bing_results

            if not results and self.enable_google_fallback:
                fallback_used = True
                google_results, attempted, _errors = await self._run_provider_queries(
                    "google",
                    company_name=company,
                    queries=queries,
                    max_results=max_results,
                    aio_session=aio_session,
                )
                queries_attempted += attempted
                if google_results:
                    engine_used = "google"
                    results = google_results

            if not results:
                fallback_used = True
                ats_results, attempted, _errors = await self._probe_ats(
                    company,
                    max_results=max_results,
                    aio_session=aio_session,
                )
                queries_attempted += attempted
                if ats_results:
                    engine_used = "ats"
                    results = ats_results

            if not results:
                stale = self.cache.get_stale(company, max_results=max_results)
                if stale:
                    fallback_used = True
                    engine_used = "cache_stale"
                    results = stale

            if results and engine_used not in {"cache", "cache_stale"}:
                self.cache.set(company, engine=engine_used, results=results)

            metrics = {
                "company": company,
                "engine_used": engine_used,
                "queries_attempted": int(queries_attempted),
                "results_found": len(results),
                "fallback_used": bool(fallback_used),
                "jobs_extracted": 0,
                "duration_ms": int((time.monotonic() - started_at) * 1000),
            }
            return results[:max_results], metrics

        if aiohttp is not None:
            timeout = aiohttp.ClientTimeout(total=self.connect_timeout + self.read_timeout + 5)
            async with aiohttp.ClientSession(trust_env=True, timeout=timeout) as local_session:
                return await _execute(local_session)

        return await _execute(None)

    # ── compatibility wrappers (sync API) ──────────────────────────────
    def search_duckduckgo(self, company_name: str, max_results: int = 10) -> list[dict[str, str]]:
        async def _run() -> list[dict[str, str]]:
            queries = self._build_company_query_variants(company_name)
            results, _attempted, _errors = await self._run_provider_queries(
                "duckduckgo",
                company_name=company_name,
                queries=queries,
                max_results=max_results,
            )
            return results

        return _run_async(_run())

    def search_bing(self, company_name: str, max_results: int = 10) -> list[dict[str, str]]:
        async def _run() -> list[dict[str, str]]:
            queries = self._build_company_query_variants(company_name)
            results, _attempted, _errors = await self._run_provider_queries(
                "bing",
                company_name=company_name,
                queries=queries,
                max_results=max_results,
            )
            return results

        return _run_async(_run())

    def search_google(self, company_name: str, max_results: int = 10) -> list[dict[str, str]]:
        async def _run() -> list[dict[str, str]]:
            queries = self._build_company_query_variants(company_name)
            results, _attempted, _errors = await self._run_provider_queries(
                "google",
                company_name=company_name,
                queries=queries,
                max_results=max_results,
            )
            return results

        return _run_async(_run())

    def search_company_openings(self, company_name: str, max_results: int = 10) -> list[dict[str, str]]:
        results, _metrics = _run_async(self._search_company_openings_async(company_name, max_results=max_results))
        return results

    def extract_job_links_from_results(
        self,
        search_results: list[dict[str, str]],
        company_name: str,
        min_relevance_score: float = 0.5,
    ) -> list[dict[str, str]]:
        """
        Filter search results for likely job posting pages.
        Returns list of dicts with 'title', 'url', 'description'.
        """
        job_keywords = {
            "career", "careers", "job", "jobs", "hiring", "position", "positions",
            "opening", "openings", "vacancy", "vacancies", "opportunity", "opportunities",
            "work with us", "join", "apply", "recruitment", "recruit"
        }
        non_job_keywords = {
            "blog", "news", "article", "post", "press", "privacy", "terms",
            "about us", "contact", "products", "services", "pricing", "feature"
        }

        filtered = []

        for result in search_results:
            title = result.get("title", "").lower()
            snippet = result.get("snippet", "").lower()
            result_url = result.get("url", "").lower()

            # Exclude non-job pages
            if any(bad in snippet or bad in title for bad in non_job_keywords):
                continue

            # Must have at least one job keyword
            job_score = sum(
                1 for keyword in job_keywords
                if keyword in title or keyword in snippet or keyword in result_url
            )

            if job_score >= 1:
                filtered.append({
                    "title": result.get("title", ""),
                    "url": result.get("url", ""),
                    "description": result.get("snippet", ""),
                    "source": result.get("source", "search"),
                    "relevance_score": job_score / len(job_keywords),
                })

        # Sort by relevance
        filtered.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)
        return filtered[:10]  # Return top 10


class JobOpeningDeduplicator:
    """Deduplicates job openings based on URL and title similarity."""

    @staticmethod
    def dedupe_key(opening: dict[str, str]) -> str:
        """Generate a deduplication key for a job opening."""
        url = opening.get("url", "").lower().strip()
        title = opening.get("title", "").lower().strip()
        return f"{url}|{title}"

    @staticmethod
    def dedupe_openings(openings: list[dict[str, str]]) -> list[dict[str, str]]:
        """Remove duplicate openings."""
        seen = set()
        unique = []

        for opening in openings:
            key = JobOpeningDeduplicator.dedupe_key(opening)
            if key not in seen:
                seen.add(key)
                unique.append(opening)

        return unique


def search_internet_for_companies(
    links_file: str,
    max_companies: int = 50,
    max_results_per_company: int = 5,
    timeout_seconds: int = 10,
    allowed_companies_file: str = "companies_pakistan.txt",
    query_variants_limit: int = 5,
    provider_fail_threshold: int = 3,
    provider_block_cooldown_seconds: int = 1800,
    enable_bing_fallback: bool = True,
    max_empty_companies_before_abort: int = 5,
    inter_company_delay_seconds: float = 0.5,
) -> list[str]:
    """
    Discover career URLs via internet fallback for companies in links.txt.

    Returns:
      list[str] of normalized career page URLs.
    """
    discovered_urls: list[str] = []

    # Read links from file
    if not os.path.isfile(links_file):
        log.warning("links.txt not found at %s", links_file)
        return discovered_urls

    try:
        with open(links_file, "r", encoding="utf-8") as f:
            links = [line.strip() for line in f if line.strip()]
    except Exception as exc:
        log.error("Failed to read links.txt: %s", exc)
        return discovered_urls

    extractor = CompanyNameExtractor()
    searcher = InternetJobSearcher(
        timeout_seconds=timeout_seconds,
        query_variants_limit=query_variants_limit,
        provider_fail_threshold=provider_fail_threshold,
        provider_block_cooldown_seconds=provider_block_cooldown_seconds,
        enable_bing_fallback=enable_bing_fallback,
    )
    empty_companies = 0

    allowed_normalized: set[str] = set()
    if allowed_companies_file and os.path.isfile(allowed_companies_file):
        try:
            with open(allowed_companies_file, "r", encoding="utf-8") as f:
                for line in f:
                    val = re.sub(r"[^a-z0-9]", "", line.strip().lower())
                    if val:
                        allowed_normalized.add(val)
            log.info("Loaded %d allowed companies from %s", len(allowed_normalized), allowed_companies_file)
        except Exception as exc:
            log.warning("Could not read allowed companies file %s: %s", allowed_companies_file, exc)

    candidate_pairs: list[tuple[str, str]] = []
    for company_url in links[:max_companies]:
        company_name = extractor.extract_from_url(company_url)
        if not company_name:
            continue
        if len(company_name) < 2 or len(company_name) > 100:
            continue

        company_key = re.sub(r"[^a-z0-9]", "", company_name.strip().lower())
        if allowed_normalized and company_key not in allowed_normalized:
            continue
        candidate_pairs.append((company_name, company_url))

    if not candidate_pairs:
        return discovered_urls

    async def _run_all() -> tuple[list[str], list[dict[str, Any]]]:
        gathered_urls: list[str] = []
        metrics_rows: list[dict[str, Any]] = []
        nonlocal empty_companies

        async def _company_task(company_name: str, company_url: str) -> tuple[list[str], dict[str, Any]]:
            log.info("Searching for openings: %s (from %s)", company_name, company_url)

            search_results, metrics = await searcher._search_company_openings_async(
                company_name,
                max_results=max_results_per_company * 2,
            )

            discovered = searcher.extract_career_urls_from_results(
                search_results,
                max_urls=max_results_per_company,
            )

            metrics["jobs_extracted"] = len(discovered)
            log.info(
                "Search metrics | company=%s | engine=%s | queries_attempted=%d | results_found=%d | jobs_extracted=%d",
                company_name,
                metrics.get("engine_used", "none"),
                int(metrics.get("queries_attempted", 0) or 0),
                int(metrics.get("results_found", 0) or 0),
                int(metrics.get("jobs_extracted", 0) or 0),
            )
            return discovered, metrics

        # Process in batches of 5 concurrent companies.
        batch_size = _MAX_COMPANY_CONCURRENCY
        for start in range(0, len(candidate_pairs), batch_size):
            if searcher.search_temporarily_unavailable():
                log.warning("Internet providers are in cooldown. Stopping remaining company batches.")
                break

            batch = candidate_pairs[start: start + batch_size]
            task_outputs = await asyncio.gather(
                *[_company_task(company_name, company_url) for company_name, company_url in batch],
                return_exceptions=True,
            )

            for output in task_outputs:
                if isinstance(output, Exception):
                    empty_companies += 1
                    continue

                company_urls, metrics = output
                metrics_rows.append(metrics)
                if not company_urls:
                    empty_companies += 1
                else:
                    gathered_urls.extend([u for u in company_urls if u])
                    empty_companies = 0

            if empty_companies >= max(1, int(max_empty_companies_before_abort)):
                log.warning(
                    "Stopping internet company search after %d consecutive empty companies.",
                    empty_companies,
                )
                break

            # Small spacing between batches to reduce burst pressure.
            delay = max(0.0, float(inter_company_delay_seconds))
            if delay > 0 and (start + batch_size) < len(candidate_pairs):
                await asyncio.sleep(delay)

        # Dedupe global URLs with same-domain suppression.
        deduped_urls: list[str] = []
        seen_url: set[str] = set()
        seen_domain: set[str] = set()
        for raw in gathered_urls:
            normalized = searcher._normalize_result_url(str(raw).strip())
            if not normalized:
                continue
            canonical = normalized.rstrip("/")
            domain = searcher._domain_key(canonical)
            if canonical in seen_url:
                continue
            if domain and domain in seen_domain:
                continue
            seen_url.add(canonical)
            if domain:
                seen_domain.add(domain)
            deduped_urls.append(canonical)

        return deduped_urls, metrics_rows

    discovered_urls, metrics_rows = _run_async(_run_all())

    companies_searched = len(metrics_rows)
    jobs_found = len(discovered_urls)
    fallback_used_count = sum(1 for m in metrics_rows if bool(m.get("fallback_used", False)))

    engine_counts: dict[str, int] = {}
    for m in metrics_rows:
        engine = str(m.get("engine_used", "none") or "none")
        engine_counts[engine] = engine_counts.get(engine, 0) + 1
    engine_summary = ", ".join(f"{k}:{v}" for k, v in sorted(engine_counts.items())) or "none"

    log.info("Search engine used: %s", engine_summary)
    log.info("Companies searched: %d", companies_searched)
    log.info("Fallback URLs found: %d", jobs_found)
    log.info("Fallback used: %d", fallback_used_count)

    return discovered_urls


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
    )

    results = search_internet_for_companies("links.txt", max_companies=10)
    print(json.dumps(results, indent=2))
