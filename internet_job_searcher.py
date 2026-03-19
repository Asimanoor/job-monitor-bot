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
from urllib.parse import urlparse, urljoin, parse_qs

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
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

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
        query = f"{company_name} careers jobs hiring"
        results = []

        try:
            url = "https://duckduckgo.com/html/"
            params = {"q": query, "kl": "us-en"}

            resp = self.session.get(url, params=params, timeout=self.timeout)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.content, "html.parser")

            # DuckDuckGo HTML search results: <div class="result">
            for idx, result_div in enumerate(soup.find_all("div", class_="result")):
                if idx >= max_results:
                    break

                try:
                    # Extract title and link
                    title_elem = result_div.find("a", class_="result__title")
                    if not title_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    result_url = title_elem.get("href", "").strip()

                    if not title or not result_url:
                        continue

                    # Extract snippet
                    snippet_elem = result_div.find("a", class_="result__snippet")
                    snippet = snippet_elem.get_text(
                        strip=True) if snippet_elem else ""

                    results.append({
                        "title": title,
                        "url": result_url,
                        "snippet": snippet,
                        "source": "duckduckgo"
                    })

                except Exception as exc:
                    log.debug("Failed to parse DuckDuckGo result: %s", exc)
                    continue

            log.info("DuckDuckGo: found %d results for '%s'", len(results), company_name)

        except Exception as exc:
            log.warning("DuckDuckGo search failed for '%s': %s", company_name, exc)

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

    for idx, company_url in enumerate(links[:max_companies]):
        try:
            company_name = extractor.extract_from_url(company_url)
            if not company_name:
                log.debug("Could not extract company name from %s", company_url)
                continue

            if len(company_name) < 2 or len(company_name) > 100:
                continue

            # Search for openings
            log.info("Searching for openings: %s (from %s)", company_name, company_url)
            search_results = searcher.search_duckduckgo(
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
