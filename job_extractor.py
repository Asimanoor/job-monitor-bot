"""
Job Extraction + Job-Details Enrichment
─────────────────────────────────────────
This module focuses on two things:
  1) Extract "job cards/openings" from a career-page HTML document
  2) Given an apply/job link, fetch the job details page and extract a
     description snippet suitable for Google Sheets / Telegram

Goals:
 - Strict job-only validation (reuse `job_scraper.py` rules)
 - Avoid noise from nav/blog/services/images (heuristic DOM extraction)
 - No paid APIs
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

from job_scraper import extract_job_postings

log = logging.getLogger(__name__)


_TRACKING_QUERY_PREFIXES = (
    "utm_",
    "fbclid",
    "gclid",
    "mc_",
    "hs",
    "__hs",
)


def normalize_apply_link(url: str) -> str:
    """
    Canonicalize an apply link for stable deduping.

    - Drop tracking query params (utm_*, fbclid, gclid, etc.)
    - Remove fragments
    - Lowercase scheme+host
    - Keep the rest as-is
    """
    if not url:
        return ""
    parsed = urlparse(url.strip())
    if parsed.scheme not in {"http", "https"}:
        return url.strip()

    filtered_query: list[tuple[str, str]] = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        key_l = (key or "").lower()
        if key_l.startswith(_TRACKING_QUERY_PREFIXES):
            continue
        filtered_query.append((key, value))

    new_query = urlencode(filtered_query, doseq=True)
    normalized_path = re.sub(r"/+", "/", parsed.path or "/")

    return urlunparse(
        (
            parsed.scheme.lower(),
            (parsed.netloc or "").lower(),
            normalized_path,
            parsed.params,
            new_query,
            "",
        )
    )


def stable_job_dedupe_key(title: str, company: str, apply_link: str) -> str:
    """
    Stable hash for dedupe across runs:
      title + company + normalized apply link.
    """
    raw = f"{(title or '').strip().lower()}|{(company or '').strip().lower()}|{normalize_apply_link(apply_link)}"
    return hashlib.sha256(raw.encode("utf-8", errors="ignore")).hexdigest()


def _iter_jsonld_nodes(payload: object):
    """Yield JSON-LD nodes from dict/list payloads, including @graph nodes."""
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


def _extract_job_description_from_jsonld(html: str) -> str:
    """
    Prefer structured job description text from JSON-LD.

    This typically avoids nav/blog/promotions noise that happens with DOM
    heuristics.
    """
    soup = BeautifulSoup(html or "", "html.parser")
    ld_type_re = re.compile(r"application/ld\+json", re.IGNORECASE)

    for script in soup.find_all("script", attrs={"type": ld_type_re}):
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

            raw_type = node.get("@type")
            node_types = raw_type if isinstance(raw_type, list) else [raw_type]
            node_types_norm = {
                str(t).strip().lower()
                for t in node_types
                if t is not None and str(t).strip()
            }
            if "jobposting" not in node_types_norm:
                continue

            desc = node.get("description") or node.get("jobDescription") or ""
            desc = str(desc or "").strip()
            if not desc:
                continue

            # JSON-LD may embed HTML; strip to plain text.
            desc_text = BeautifulSoup(desc, "html.parser").get_text(" ", strip=True)
            desc_text = re.sub(r"\s+", " ", desc_text).strip()
            if len(desc_text) >= 120:
                return desc_text

    return ""


def extract_description_from_job_html(html: str, base_url: str = "") -> str:
    """
    Extract job description text with DOM heuristics.
    Returns a single cleaned snippet (not HTML).
    """
    # 1) JSON-LD is the most job-specific and least noisy.
    jsonld_desc = _extract_job_description_from_jsonld(html)
    if jsonld_desc:
        jsonld_desc = re.sub(r"Related Jobs.*$", "", jsonld_desc, flags=re.IGNORECASE)
        jsonld_desc = re.sub(r"Apply.*$", "", jsonld_desc, flags=re.IGNORECASE)
        return jsonld_desc

    soup = BeautifulSoup(html or "", "html.parser")

    # Remove non-content tags
    for tag in soup(["script", "style", "noscript", "svg", "canvas", "img", "picture"]):
        tag.extract()

    # Candidate selectors (prefer job-specific containers)
    selectors = [
        "#job-description",
        "[id*='job-description' i]",
        "[class*='job-description' i]",
        "[data-testid*='job-description' i]",
        "[data-automation-id*='job' i]",
        "[class*='description' i]",
        "[id*='description' i]",
        "article.job",
        "div[role='main']",
    ]

    _job_anchor_patterns = [
        re.compile(r"\bresponsibilit", re.IGNORECASE),
        re.compile(r"\brequirements", re.IGNORECASE),
        re.compile(r"\bqualifications", re.IGNORECASE),
        re.compile(r"\babout (the )?role", re.IGNORECASE),
        re.compile(r"\bjob summary", re.IGNORECASE),
        re.compile(r"\bjob description", re.IGNORECASE),
        re.compile(r"\brole overview", re.IGNORECASE),
        re.compile(r"\bwhat (you'?ll|you will) do", re.IGNORECASE),
    ]

    def _looks_like_job_text(text: str) -> bool:
        return any(p.search(text or "") for p in _job_anchor_patterns)

    chosen_text = ""
    for sel in selectors:
        node = soup.select_one(sel)
        if not node:
            continue
        text = node.get_text(" ", strip=True)
        text = re.sub(r"\s+", " ", text).strip()
        # Require some length to avoid selecting generic navigation blocks.
        if len(text) >= 200:
            chosen_text = text
            break

    if not chosen_text:
        # Last-resort: only return full-page text if it still looks like a job
        # description (otherwise nav/blog/promos can pollute outputs).
        candidate = soup.get_text(" ", strip=True)
        candidate = re.sub(r"\s+", " ", candidate).strip()
        if len(candidate) >= 200 and _looks_like_job_text(candidate):
            chosen_text = candidate

    if not chosen_text:
        return ""

    # Cleanup common boilerplate patterns
    chosen_text = re.sub(r"Related Jobs.*$", "", chosen_text, flags=re.IGNORECASE)
    chosen_text = re.sub(r"Apply.*$", "", chosen_text, flags=re.IGNORECASE)

    return chosen_text


def _extract_date_like_text(html: str) -> str:
    """
    Lightweight posted-date extraction from the job details HTML.
    Returns an ISO-ish date string when possible, else ''.
    """
    soup = BeautifulSoup(html or "", "html.parser")
    for time in soup.find_all("time"):
        dt = time.get("datetime") or time.get("data-posted") or ""
        dt = str(dt).strip()
        if dt:
            # Try to parse common formats; if it starts with YYYY-MM-DD keep it.
            m = re.search(r"\b\d{4}-\d{2}-\d{2}\b", dt)
            if m:
                return m.group(0)
    # Fallback to regex
    text = soup.get_text(" ", strip=True)
    m = re.search(r"\b\d{4}-\d{2}-\d{2}\b", text)
    return m.group(0) if m else ""


def fetch_job_description_snippet(
    apply_link: str,
    *,
    scraper: Any | None = None,
    timeout_seconds: int = 20,
    min_chars: int = 120,
    max_chars: int = 1200,
) -> dict[str, str]:
    """
    Fetch job detail page HTML and extract a description snippet.

    Returns:
      {
        description: str,
        posted_date: str
      }
    """
    apply_link = normalize_apply_link(apply_link)
    if not apply_link:
        return {"description": "", "posted_date": ""}

    html = ""
    resolved_url = apply_link

    try:
        if scraper is not None and hasattr(scraper, "scrape_job_sync"):
            # MultiStrategyCareerScraper already uses playwright for JS-heavy pages.
            result = scraper.scrape_job_sync(apply_link)
            if isinstance(result, dict) and result.get("ok") and result.get("html"):
                html = str(result.get("html") or "")
                resolved_url = str(result.get("final_url") or resolved_url)
        if not html:
            resp = requests.get(apply_link, timeout=timeout_seconds, allow_redirects=True, headers={
                "User-Agent": "Mozilla/5.0"
            })
            resp.raise_for_status()
            html = resp.text or ""
            resolved_url = str(resp.url or resolved_url)
    except Exception as exc:
        log.warning("Job details fetch failed: %s (%s)", apply_link, exc)
        return {"description": "", "posted_date": ""}

    description_full = extract_description_from_job_html(html, base_url=resolved_url)
    if len(description_full) < min_chars:
        # If we didn't capture much, return empty description so monitor can skip.
        return {"description": "", "posted_date": _extract_date_like_text(html)}

    return {
        "description": description_full[:max_chars],
        "posted_date": _extract_date_like_text(html),
    }


def extract_jobs_from_career_page(page_html: str, page_url: str, *, max_results: int = 50) -> list[dict[str, Any]]:
    """
    Wrapper around `job_scraper.extract_job_postings` with stable keys.
    """
    jobs = extract_job_postings(page_html, page_url, max_results=max_results)
    normalized: list[dict[str, Any]] = []
    for j in jobs:
        if not isinstance(j, dict):
            continue
        normalized.append(
            {
                "title": j.get("title", ""),
                "company": j.get("company", ""),
                "location": j.get("location", ""),
                "type": j.get("type", ""),
                "apply_link": j.get("apply_link", ""),
                "posted_date": j.get("posted_date", ""),
                "source_url": j.get("source_url", page_url),
                "job_url": j.get("job_url", j.get("apply_link", "")),
            }
        )
    return normalized


def job_dict_for_sheet(
    *,
    title: str,
    company: str,
    location: str,
    job_type: str,
    posted_date: str,
    apply_link: str,
    description: str,
    matched_role: str,
    match_score: float,
    source_url: str,
) -> dict[str, str]:
    """
    Convert an extracted/enriched job into the dict shape expected by
    `NotificationManager._append_to_sheet()` / GoogleSheetsClient.append_job_row().
    """
    return {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "job_title": title or "",
        "employer_name": company or "",
        "location": location or "",
        "job_type": job_type or "",
        "posted_at": (posted_date or "")[:10],
        "apply_link": apply_link or "",
        "description": description or "",
        "matched_as": matched_role or "",
        "filter_keyword": matched_role or "",
        "ai_score": str(int(round(match_score)) if match_score is not None else ""),
        "source_url": source_url or "",
        "status": "New",
    }


__all__ = [
    "normalize_apply_link",
    "stable_job_dedupe_key",
    "extract_description_from_job_html",
    "fetch_job_description_snippet",
    "extract_jobs_from_career_page",
    "job_dict_for_sheet",
]

