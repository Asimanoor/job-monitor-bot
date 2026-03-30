"""
Internet Job Searcher
─────────────────────
Searches the internet (Google, Bing) for job openings related to companies
listed in links.txt and extracts new job openings to append to Google Sheets.

This module:
1. Reads company links from links.txt
2. Extracts company names from URLs
3. Searches Google/Bing for "[Company] careers jobs" queries
4. Parses search results for job opening links
5. Validates and deduplicates openings
6. Appends new openings to Google Sheets
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)


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

    def __init__(self, timeout_seconds: int = 10, headers: dict | None = None):
        self.timeout = timeout_seconds
        self.session = requests.Session()
        self.headers = headers or self._default_headers()
        self.session.headers.update(self.headers)

    @staticmethod
    def _default_headers() -> dict[str, str]:
        """Return sensible default headers to avoid blocking."""
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }

    @staticmethod
    def _clean_company_query(company_name: str) -> str:
        cleaned = re.sub(r"[\-_+]+", " ", (company_name or "").strip())
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned

    @classmethod
    def _build_company_query_variants(cls, company_name: str) -> list[str]:
        company = cls._clean_company_query(company_name)
        return [
            f'"{company}" careers',
            f'"{company}" jobs pakistan',
            f'"{company}" careers lahore',
            f'"{company}" jobs site:lever.co',
            f'"{company}" jobs site:greenhouse.io',
            f'"{company}" jobs site:workday',
            f'"{company}" jobs site:applytojob.com',
            f'"{company}" jobs site:workable.com',
            f'"{company}" associate software engineer',
            f'"{company}" associate data science',
            f'"{company}" associate ai engineer',
            f'"{company}" fresh graduate software engineer',
            f'"{company}" ai engineer "{company}"',
            f'"{company}" ai engineer',
            f'"{company}" machine learning engineer',
        ]

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
        if parsed.scheme not in {"http", "https"}:
            return ""

        return parsed._replace(fragment="").geturl()

    def _search_duckduckgo_query(self, query: str, max_results: int = 10) -> list[dict[str, str]]:
        """Execute one DuckDuckGo query and parse result cards robustly."""
        results: list[dict[str, str]] = []
        seen: set[str] = set()

        try:
            url = "https://duckduckgo.com/html/"
            params = {"q": query, "kl": "us-en"}

            resp = self.session.get(url, params=params, timeout=self.timeout)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.content, "html.parser")

            cards = soup.select("div.result")
            if not cards:
                cards = soup.select(".result.results_links.results_links_deep")

            for card in cards:
                if len(results) >= max_results:
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

                dedupe_key = f"{href.lower()}|{title.lower()}"
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)

                results.append(
                    {
                        "title": title,
                        "url": href,
                        "snippet": snippet,
                        "source": "duckduckgo",
                        "query": query,
                    }
                )
        except Exception as exc:
            log.warning("DuckDuckGo query failed for '%s': %s", query, exc)

        return results

    def search_google(self, company_name: str, max_results: int = 10) -> list[dict[str, str]]:
        """
        Search Google for company job openings.
        Returns list of dicts with 'title', 'url', 'snippet'.
        """
        query = f"{company_name} careers jobs hiring"
        results = []

        try:
            # Use Google's search endpoint (this is a fallback; real approach uses Selenium/Playwright)
            # For now, we'll return empty and rely on DuckDuckGo as fallback
            log.info("Searching Google for: %s", query)
        except Exception as exc:
            log.warning("Google search failed: %s", exc)

        return results

    def search_duckduckgo(self, company_name: str, max_results: int = 10) -> list[dict[str, str]]:
        """
        Search DuckDuckGo for company job openings.
        DuckDuckGo is more accessible than Google for scraping.
        """
        query_variants = self._build_company_query_variants(company_name)

        merged: list[dict[str, str]] = []
        seen: set[str] = set()

        for query in query_variants:
            partial = self._search_duckduckgo_query(query, max_results=max(3, max_results))
            for item in partial:
                key = f"{item.get('url', '').lower()}|{item.get('title', '').lower()}"
                if key in seen:
                    continue
                seen.add(key)
                merged.append(item)
                if len(merged) >= max_results:
                    break
            if len(merged) >= max_results:
                break

        log.info("DuckDuckGo: found %d results for '%s'", len(merged), company_name)
        return merged[:max_results]

    def search_bing(self, company_name: str, max_results: int = 10) -> list[dict[str, str]]:
        """Fallback search path using Bing HTML results."""
        results: list[dict[str, str]] = []
        seen: set[str] = set()
        queries = self._build_company_query_variants(company_name)

        for query in queries:
            if len(results) >= max_results:
                break
            try:
                resp = self.session.get(
                    "https://www.bing.com/search",
                    params={"q": query, "setlang": "en-US"},
                    timeout=self.timeout,
                )
                resp.raise_for_status()
                soup = BeautifulSoup(resp.content, "html.parser")

                for card in soup.select("li.b_algo"):
                    if len(results) >= max_results:
                        break

                    link_elem = card.select_one("h2 a")
                    if link_elem is None:
                        continue

                    title = link_elem.get_text(" ", strip=True)
                    href = self._normalize_result_url(str(link_elem.get("href") or ""))
                    if not title or not href:
                        continue

                    snippet_elem = card.select_one("p")
                    snippet = snippet_elem.get_text(" ", strip=True) if snippet_elem else ""

                    key = f"{href.lower()}|{title.lower()}"
                    if key in seen:
                        continue
                    seen.add(key)

                    results.append(
                        {
                            "title": title,
                            "url": href,
                            "snippet": snippet,
                            "source": "bing",
                            "query": query,
                        }
                    )
            except Exception as exc:
                log.warning("Bing query failed for '%s' (%s): %s", company_name, query, exc)

        return results

    def search_company_openings(self, company_name: str, max_results: int = 10) -> list[dict[str, str]]:
        """Search across providers and return deduplicated result links."""
        ddg_results = self.search_duckduckgo(company_name, max_results=max_results)
        if ddg_results:
            return ddg_results[:max_results]

        bing_results = self.search_bing(company_name, max_results=max_results)
        return bing_results[:max_results]

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
) -> list[dict[str, Any]]:
    """
    Search internet for job openings from companies in links.txt.

    Returns list of dicts with:
      - company_name (str)
      - company_url (str)
      - found_openings (list of dicts with title, url, description)
      - timestamp (str)
    """
    results = []

    # Read links from file
    if not os.path.isfile(links_file):
        log.warning("links.txt not found at %s", links_file)
        return results

    try:
        with open(links_file, "r", encoding="utf-8") as f:
            links = [line.strip() for line in f if line.strip()]
    except Exception as exc:
        log.error("Failed to read links.txt: %s", exc)
        return results

    extractor = CompanyNameExtractor()
    searcher = InternetJobSearcher(timeout_seconds=timeout_seconds)
    deduplicator = JobOpeningDeduplicator()

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

    for idx, company_url in enumerate(links[:max_companies]):
        try:
            company_name = extractor.extract_from_url(company_url)
            if not company_name:
                log.debug("Could not extract company name from %s", company_url)
                continue

            company_key = re.sub(r"[^a-z0-9]", "", company_name.strip().lower())
            if allowed_normalized and company_key not in allowed_normalized:
                log.debug("Skipping company not in allowed dataset: %s", company_name)
                continue

            if len(company_name) < 2 or len(company_name) > 100:
                continue

            # Search for openings
            log.info("Searching for openings: %s (from %s)", company_name, company_url)
            search_results = searcher.search_company_openings(
                company_name,
                max_results=max_results_per_company * 2
            )

            if not search_results:
                log.debug("No search results for %s", company_name)
                continue

            # Filter and extract job links
            job_links = searcher.extract_job_links_from_results(
                search_results,
                company_name
            )

            if job_links:
                # Deduplicate
                deduped = deduplicator.dedupe_openings(job_links)

                results.append({
                    "company_name": company_name,
                    "company_url": company_url,
                    "found_openings": deduped[:max_results_per_company],
                    "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "opening_count": len(deduped),
                })

            # Avoid rate limiting
            if idx < len(links) - 1:
                time.sleep(1)

        except Exception as exc:
            log.warning("Error searching for %s: %s", company_url, exc)
            continue

    return results


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
    )

    results = search_internet_for_companies("links.txt", max_companies=10)
    print(json.dumps(results, indent=2))
