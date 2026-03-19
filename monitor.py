"""
Job Monitoring System
─────────────────────
Main orchestrator script connecting config, API, filtering, and notifications.
"""

import argparse
import asyncio
import hashlib
import json
import logging
import os
import re
import sys
import time
import requests
from datetime import datetime, timezone
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from job_scraper import extract_job_postings, is_valid_job_posting

from config_loader import ConfigLoader
from filter_engine import FilterEngine
from groq_client import GroqClient
from jsearch_client import JSearchClient, JSearchRateLimitError
from job_extractor import (
    fetch_job_description_snippet,
    job_dict_for_sheet,
    normalize_apply_link,
    stable_job_dedupe_key,
)
from role_filter import filter_jobs_by_role, matches_target_role
from state_manager import StateManager

try:
    from internet_job_searcher import search_internet_for_companies
except Exception:  # pragma: no cover - handled gracefully at runtime
    search_internet_for_companies = None

try:
    from mcp_scraper import MultiStrategyCareerScraper
except Exception:  # pragma: no cover - handled gracefully at runtime
    MultiStrategyCareerScraper = None

# ── Paths ────────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LINKS_FILE = os.path.join(SCRIPT_DIR, "links.txt")
JOBS_FILE = os.path.join(SCRIPT_DIR, "jobs.txt")
FILTERS_FILE = os.path.join(SCRIPT_DIR, "filters.txt")
STATE_FILE = os.path.join(SCRIPT_DIR, "state.json")
PAUSE_FILE = os.path.join(SCRIPT_DIR, "pause.txt")
LOG_DIR = os.path.join(SCRIPT_DIR, "logs")

# ── Logging ──────────────────────────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(
            LOG_DIR, "monitor.log"), encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

_OPENING_KEYWORDS = [
    "engineer", "developer", "scientist", "analyst", "intern", "internship",
    "associate", "junior", "graduate", "trainee", "entry", "new grad",
    "software", "backend", "frontend", "full stack", "data", "ai", "ml",
    "qa", "sre", "devops", "python",
]

_OPENING_IGNORE_PHRASES = [
    "privacy", "cookie", "terms", "sign in", "log in", "login", "about us",
    "contact us", "home", "learn more", "view all", "read more", "subscribe",
    "press", "investor", "culture", "benefits", "team",
]

_COMPANY_STOP_WORDS = {
    "www", "careers", "career", "jobs", "job", "apply", "app", "team",
    "global", "group", "inc", "llc", "ltd", "limited", "co", "company",
    "com", "io", "net", "org", "ai", "pk", "eu", "hr", "site",
}


def _normalize_company_candidate(value: str) -> str:
    text = re.sub(r"[\-_+]+", " ", (value or "").strip().lower())
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    tokens = [tok for tok in text.split() if tok and tok not in _COMPANY_STOP_WORDS and len(tok) > 1]
    if not tokens:
        return ""
    return " ".join(tokens[:4])


def extract_company_hints_from_urls(urls: list[str], max_companies: int = 25) -> list[str]:
    """Derive likely company names from links.txt URLs and ATS URL patterns."""
    discovered: list[str] = []
    seen: set[str] = set()

    for raw_url in urls:
        try:
            parsed = urlparse(raw_url)
        except Exception:
            continue

        host = (parsed.netloc or "").lower().strip()
        host = host.split(":")[0]
        if host.startswith("www."):
            host = host[4:]

        query = parse_qs(parsed.query or "")
        candidates: list[str] = []

        for key in ("company", "organization", "org", "tenant", "client"):
            values = query.get(key) or []
            for v in values:
                if v:
                    candidates.append(v)

        path_segments = [seg for seg in (parsed.path or "").split("/") if seg.strip()]

        # ATS-aware extraction patterns.
        if host.endswith("lever.co") and path_segments:
            candidates.append(path_segments[0])
        if host.endswith("workable.com") and path_segments:
            candidates.append(path_segments[0])
        if host.endswith("ashbyhq.com") and path_segments:
            candidates.append(path_segments[0])
        if host.endswith("applytojob.com") and path_segments:
            candidates.append(path_segments[0])
        if host.endswith("breezy.hr"):
            first_label = host.split(".")[0]
            if first_label:
                candidates.append(first_label)

        if not candidates:
            labels = [label for label in host.split(".") if label]
            if len(labels) >= 3:
                candidates.append(labels[0])
            elif len(labels) >= 2:
                candidates.append(labels[0])

        for candidate in candidates:
            normalized = _normalize_company_candidate(candidate)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            discovered.append(normalized)
            if len(discovered) >= max(1, int(max_companies)):
                return discovered

    return discovered


def _company_matches_employer(company_hint: str, employer_name: str) -> bool:
    company_tokens = [t for t in _normalize_company_candidate(company_hint).split() if t]
    employer_norm = _normalize_company_candidate(employer_name)
    if not company_tokens or not employer_norm:
        return False
    return all(token in employer_norm for token in company_tokens[:2])


def build_jsearch_query_plan(
    titles: list[str],
    locations: list[str],
    company_hints: list[str],
    allowed_queries: int,
    company_targeted_enabled: bool = True,
    company_max_queries: int = 4,
) -> list[dict[str, str]]:
    """Build company-first JSearch plan (links.txt companies first, then generic market search)."""
    plan: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    budget = max(0, int(allowed_queries))

    if budget <= 0:
        return plan

    clean_locations = [str(loc).strip() for loc in (locations or []) if str(loc).strip()]

    if company_targeted_enabled:
        company_budget = min(max(0, int(company_max_queries)), budget)
        for company in company_hints:
            if len(plan) >= company_budget:
                break
            query = f"{company} careers jobs"
            key = (query.lower().strip(), "")
            if key in seen:
                continue
            seen.add(key)
            plan.append(
                {
                    "query": query,
                    "location": "",
                    "source": "JSEARCH_COMPANY_TARGETED",
                    "company": company,
                }
            )

    for title in titles:
        if len(plan) >= budget:
            break

        targets = clean_locations or [""]
        for location in targets:
            if len(plan) >= budget:
                break

            key = (title.lower().strip(), location.lower().strip())
            if key in seen:
                continue
            seen.add(key)
            plan.append(
                {
                    "query": title,
                    "location": location,
                    "source": "JSEARCH_API",
                    "company": "",
                }
            )

    return plan


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def _looks_like_opening_title(text: str) -> bool:
    clean = _normalize_whitespace(text)
    if not clean:
        return False

    lower = clean.lower()
    if len(clean) < 4 or len(clean) > 160:
        return False
    if any(bad in lower for bad in _OPENING_IGNORE_PHRASES):
        return False
    return any(keyword in lower for keyword in _OPENING_KEYWORDS)


def _opening_fingerprint(opening: dict[str, str]) -> str:
    title = _normalize_whitespace(opening.get("title", "")).lower()
    link = (opening.get("link", "") or "").strip().lower()
    return f"{title}|{link}"


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


def _extract_openings_from_jsonld(base_url: str, soup: BeautifulSoup, max_positions: int = 50) -> list[dict[str, str]]:
    """Extract JobPosting entries from JSON-LD scripts when available."""
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

            raw_type = node.get("@type")
            node_types = raw_type if isinstance(raw_type, list) else [raw_type]
            node_types_norm = {str(t).strip().lower() for t in node_types if t}
            if "jobposting" not in node_types_norm:
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

            if len(openings) >= max_positions:
                return openings

    return openings


def _extract_openings_from_html(base_url: str, html: str, max_positions: int = 50) -> tuple[str, list[dict[str, str]]]:
    """Extract probable career openings from anchors on the page."""
    soup = BeautifulSoup(html or "", "html.parser")
    page_title = _normalize_whitespace(soup.title.get_text(" ", strip=True)) if soup.title else ""

    openings: list[dict[str, str]] = []
    seen: set[str] = set()

    strict_jobs = extract_job_postings(html, base_url, max_results=max_positions)
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

        if len(openings) >= max_positions:
            return page_title, openings

    # Prefer structured data first when present.
    for item in _extract_openings_from_jsonld(base_url, soup, max_positions=max_positions):
        fp = _opening_fingerprint(item)
        if fp in seen:
            continue
        seen.add(fp)
        openings.append(item)

        if len(openings) >= max_positions:
            return page_title, openings

    return page_title, openings


def _as_bool(value: object, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _run_async(coro):
    """Run async coroutines safely from sync entry-points."""
    try:
        return asyncio.run(coro)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()


def _monitor_single_url_sync(
    url: str,
    old_hash: str | None,
    previous_fingerprints: set[str],
    scraper: object | None,
    max_pages_per_site: int,
    max_openings_per_page: int,
    headers: dict[str, str],
) -> dict[str, object]:
    """Process one URL and return normalized monitoring outcome."""
    try:
        page_html = ""
        resolved_url = url
        new_hash = ""
        scraper_used = "requests"
        scraper_error = ""
        scraped_openings: list[dict[str, str]] = []
        scraped_page_title = ""
        pages_visited: list[str] = []

        if scraper is not None and hasattr(scraper, "scrape_site_openings_sync"):
            try:
                site_result = scraper.scrape_site_openings_sync(
                    url,
                    max_pages=max(1, int(max_pages_per_site)),
                    max_openings=max(1, int(max_openings_per_page)),
                )

                if isinstance(site_result, dict) and site_result.get("ok"):
                    resolved_url = str(site_result.get("final_url") or url)
                    scraper_used = str(site_result.get("scraper") or "multi_strategy")
                    scraped_page_title = str(site_result.get("page_title") or "")
                    pages_visited = [
                        str(page_url)
                        for page_url in (site_result.get("pages_visited") or [])
                        if str(page_url).strip()
                    ]
                    scraped_openings = [
                        item for item in (site_result.get("openings") or []) if isinstance(item, dict)
                    ]

                    hash_payload = json.dumps(
                        {
                            "resolved_url": resolved_url,
                            "pages_visited": pages_visited,
                            "openings": scraped_openings,
                            "page_title": scraped_page_title,
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                    new_hash = hashlib.sha256(hash_payload.encode("utf-8", errors="ignore")).hexdigest()
                else:
                    scraper_error = str(
                        (site_result or {}).get("error")
                        if isinstance(site_result, dict)
                        else "unknown scrape_site_openings_sync error"
                    )
            except Exception as exc:
                scraper_error = str(exc)

        if scraper is not None and hasattr(scraper, "scrape_job_sync") and not new_hash:
            try:
                scraped = scraper.scrape_job_sync(url)
                if isinstance(scraped, dict) and scraped.get("ok") and scraped.get("html"):
                    page_html = str(scraped.get("html") or "")
                    resolved_url = str(scraped.get("final_url") or url)
                    new_hash = hashlib.sha256(page_html.encode("utf-8", errors="ignore")).hexdigest()
                    scraper_used = str(scraped.get("scraper") or "playwright")
                else:
                    scraper_error = str((scraped or {}).get("error") if isinstance(scraped, dict) else "unknown error")
            except Exception as exc:
                scraper_error = str(exc)

        if not new_hash:
            resp = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
            resp.raise_for_status()
            page_html = resp.text or ""
            resolved_url = str(resp.url or url)
            new_hash = hashlib.sha256(resp.content).hexdigest()
            scraper_used = "requests"

        if old_hash is not None and new_hash == old_hash:
            return {
                "url": url,
                "new_hash": new_hash,
                "status": "unchanged",
                "new_fingerprints": list(previous_fingerprints),
            }

        change_type = "new_url_tracked" if old_hash is None else "content_changed"
        if scraped_openings:
            page_title = scraped_page_title
            openings = scraped_openings
        else:
            page_title, openings = _extract_openings_from_html(
                resolved_url,
                page_html,
                max_positions=max(1, int(max_openings_per_page)),
            )

        if not openings:
            return {
                "url": url,
                "new_hash": new_hash,
                "status": "ignored_no_jobs",
                "new_fingerprints": [],
                "scraper_error": scraper_error,
            }

        current_fingerprints = {_opening_fingerprint(o) for o in openings}
        new_openings = [o for o in openings if _opening_fingerprint(o) not in previous_fingerprints]

        if change_type == "content_changed" and not new_openings:
            return {
                "url": url,
                "new_hash": new_hash,
                "status": "no_new_openings",
                "new_fingerprints": list(current_fingerprints),
            }

        event = {
            "url": url,
            "resolved_url": resolved_url,
            "domain": urlparse(url).netloc,
            "change_type": change_type,
            "page_title": page_title,
            "openings": openings,
            "new_openings": new_openings,
            "total_openings": len(openings),
            "new_openings_count": len(new_openings),
            "old_hash": old_hash or "",
            "new_hash": new_hash,
            "source": "links.txt",
            "scraper_used": scraper_used,
            "pages_visited": pages_visited,
        }

        return {
            "url": url,
            "new_hash": new_hash,
            "status": change_type,
            "event": event,
            "new_fingerprints": list(current_fingerprints),
        }
    except requests.RequestException as exc:
        return {"url": url, "status": "error", "error": f"Request error — {exc}"}
    except Exception as exc:
        return {"url": url, "status": "error", "error": f"Unexpected monitor error — {exc}"}


async def _monitor_urls_async(
    urls: list[str],
    old_hashes: dict[str, str | None],
    opening_fingerprints: dict[str, set[str]],
    scraper: object | None,
    max_pages_per_site: int,
    max_openings_per_page: int,
    concurrency: int,
) -> list[dict[str, object]]:
    sem = asyncio.Semaphore(max(1, int(concurrency)))
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    async def _worker(target_url: str) -> dict[str, object]:
        async with sem:
            return await asyncio.to_thread(
                _monitor_single_url_sync,
                target_url,
                old_hashes.get(target_url),
                opening_fingerprints.get(target_url, set()),
                scraper,
                max_pages_per_site,
                max_openings_per_page,
                headers,
            )

    return await asyncio.gather(*[_worker(url) for url in urls])


def monitor_urls(
    state_mgr: StateManager,
    scraper: object | None = None,
    max_pages_per_site: int = 6,
    max_openings_per_page: int = 50,
    concurrency: int = 4,
    return_activity: bool = False,
) -> list[dict[str, object]] | tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Check URLs from links.txt for content changes and extract opening details."""
    urls = ConfigLoader.load_urls(LINKS_FILE)
    if not urls:
        log.warning("No valid URLs to monitor.")
        return ([], []) if return_activity else []

    change_events: list[dict[str, object]] = []
    activity_rows: list[dict[str, object]] = []
    errors = []

    old_hashes = {url: state_mgr.get_url_hash(url) for url in urls}
    opening_fingerprints = {
        url: state_mgr.get_url_opening_fingerprints(url)
        for url in urls
    }

    results = _run_async(
        _monitor_urls_async(
            urls=urls,
            old_hashes=old_hashes,
            opening_fingerprints=opening_fingerprints,
            scraper=scraper,
            max_pages_per_site=max_pages_per_site,
            max_openings_per_page=max_openings_per_page,
            concurrency=concurrency,
        )
    )

    for result in results:
        target_url = str(result.get("url", ""))
        status = str(result.get("status", "error"))
        event = result.get("event") if isinstance(result.get("event"), dict) else {}

        activity_rows.append(
            {
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                "url": target_url,
                "domain": str(event.get("domain") or urlparse(target_url).netloc),
                "status": status,
                "change_type": str(event.get("change_type") or status),
                "total_openings": int(event.get("total_openings", 0) or 0),
                "new_openings_count": int(event.get("new_openings_count", 0) or 0),
                "scraper_used": str(event.get("scraper_used") or ""),
                "pages_visited": event.get("pages_visited") if isinstance(event.get("pages_visited"), list) else [],
                "error": str(result.get("error") or result.get("scraper_error") or ""),
                "notes": "URL monitor cycle audit",
            }
        )

        if status == "error":
            log.error("%s: %s", target_url, result.get("error", "unknown error"))
            errors.append(target_url)
            continue

        if status == "unchanged":
            log.info("✅  No change:       %s", target_url)
        elif status == "ignored_no_jobs":
            log.info("⚪  Ignored change (no valid job postings): %s", target_url)
        elif status == "no_new_openings":
            log.info("✅  No new openings:  %s", target_url)
        elif status == "new_url_tracked":
            event = result.get("event") if isinstance(result.get("event"), dict) else {}
            log.info(
                "🆕  New URL tracked: %s (openings detected: %d)",
                target_url,
                int(event.get("total_openings", 0) or 0),
            )
        elif status == "content_changed":
            event = result.get("event") if isinstance(result.get("event"), dict) else {}
            log.info(
                "🔄  Change detected: %s (openings: %d, new openings: %d)",
                target_url,
                int(event.get("total_openings", 0) or 0),
                int(event.get("new_openings_count", 0) or 0),
            )

        new_hash = str(result.get("new_hash", "")).strip()
        if new_hash:
            state_mgr.set_url_hash(target_url, new_hash)

        new_fps = result.get("new_fingerprints", [])
        if isinstance(new_fps, list):
            state_mgr.set_url_opening_fingerprints(target_url, new_fps)

        if isinstance(event, dict):
            change_events.append(event)

    print("\n" + "=" * 60)
    print("  URL MONITORING SUMMARY")
    print(f"  Total    : {len(urls)}")
    print(f"  Changed  : {len(change_events)}")
    print(f"  Errors   : {len(errors)}")
    print("=" * 60 + "\n")
    if return_activity:
        return change_events, activity_rows
    return change_events


async def _execute_jsearch_plan_async(
    jsearch: JSearchClient,
    query_plan: list[dict[str, str]],
    concurrency: int,
) -> list[dict[str, object]]:
    """Execute JSearch query plan concurrently and return ordered results."""
    if not query_plan:
        return []

    sem = asyncio.Semaphore(max(1, int(concurrency)))
    abort_event = asyncio.Event()

    async def _worker(task: dict[str, str]) -> dict[str, object]:
        query_text = str(task.get("query") or "").strip()
        location_text = str(task.get("location") or "").strip()
        shown_query = f"{query_text} in {location_text}" if location_text else query_text

        if not query_text:
            return {
                "task": task,
                "raw_results": [],
                "shown_query": shown_query,
                "attempted": False,
                "rate_limited": False,
                "skipped": "empty_query",
            }

        if abort_event.is_set():
            return {
                "task": task,
                "raw_results": [],
                "shown_query": shown_query,
                "attempted": False,
                "rate_limited": False,
                "skipped": "rate_limit_abort",
            }

        if hasattr(jsearch, "is_temporarily_rate_limited") and jsearch.is_temporarily_rate_limited():
            cooldown = jsearch.remaining_rate_limit_cooldown() if hasattr(jsearch, "remaining_rate_limit_cooldown") else 0
            log.warning(
                "Skipping JSearch query due to active 429 cooldown (%ds remaining): %s",
                cooldown,
                shown_query,
            )
            return {
                "task": task,
                "raw_results": [],
                "shown_query": shown_query,
                "attempted": False,
                "rate_limited": True,
                "skipped": "cooldown",
            }

        async with sem:
            if abort_event.is_set():
                return {
                    "task": task,
                    "raw_results": [],
                    "shown_query": shown_query,
                    "attempted": False,
                    "rate_limited": False,
                    "skipped": "rate_limit_abort",
                }

            log.info("🔍  Searching: %s", shown_query)
            try:
                raw_results = await asyncio.to_thread(jsearch.search_jobs, query_text, location_text)
            except JSearchRateLimitError as exc:
                abort_event.set()
                log.error("JSearch rate limit triggered for '%s': %s", shown_query, exc)
                return {
                    "task": task,
                    "raw_results": [],
                    "shown_query": shown_query,
                    "attempted": True,
                    "rate_limited": True,
                    "skipped": "rate_limited",
                }
            except Exception as exc:
                log.warning("JSearch async worker failed for '%s': %s", shown_query, exc)
                raw_results = []

        return {
            "task": task,
            "raw_results": raw_results if isinstance(raw_results, list) else [],
            "shown_query": shown_query,
            "attempted": True,
            "rate_limited": False,
            "skipped": "",
        }

    return await asyncio.gather(*[_worker(task) for task in query_plan])


async def _run_sources_async(
    state_mgr: StateManager,
    scraper: object | None,
    max_pages_per_site: int,
    max_openings_per_page: int,
    monitor_concurrency: int,
    jsearch: JSearchClient,
    query_plan: list[dict[str, str]],
    jsearch_concurrency: int,
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    """Run links.txt monitoring and JSearch queries concurrently."""
    monitor_task = asyncio.to_thread(
        monitor_urls,
        state_mgr,
        scraper,
        max_pages_per_site,
        max_openings_per_page,
        monitor_concurrency,
        True,
    )
    jsearch_task = _execute_jsearch_plan_async(
        jsearch=jsearch,
        query_plan=query_plan,
        concurrency=jsearch_concurrency,
    )

    monitor_result, jsearch_result = await asyncio.gather(
        monitor_task,
        jsearch_task,
        return_exceptions=True,
    )

    url_change_events: list[dict[str, object]] = []
    activity_rows: list[dict[str, object]] = []
    query_results: list[dict[str, object]] = []

    if isinstance(monitor_result, Exception):
        log.error("URL monitoring crashed: %s", monitor_result)
    elif isinstance(monitor_result, tuple) and len(monitor_result) == 2:
        maybe_changes, maybe_activity = monitor_result
        if isinstance(maybe_changes, list):
            url_change_events = maybe_changes
        if isinstance(maybe_activity, list):
            activity_rows = maybe_activity
    elif isinstance(monitor_result, list):
        url_change_events = monitor_result

    if isinstance(jsearch_result, Exception):
        log.error("JSearch async execution crashed: %s", jsearch_result)
    elif isinstance(jsearch_result, list):
        query_results = jsearch_result

    return url_change_events, query_results, activity_rows


async def _notify_url_changes_async(
    notifier: object,
    url_change_events: list[dict[str, object]],
    record_to_sheets: bool = True,
) -> None:
    """Send URL-change alert and record rows concurrently."""
    if not url_change_events:
        return

    tasks = [
        asyncio.to_thread(notifier.send_url_change_alert, url_change_events),
    ]
    if record_to_sheets:
        tasks.append(
            asyncio.to_thread(
                notifier.record_url_changes_in_sheet, url_change_events
            )
        )
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, Exception):
            log.error("URL change notification sub-task failed: %s", result)


async def _search_internet_for_companies_async(
    sheets_client: object | None,
    links_file: str,
    max_companies: int = 20,
    max_results_per_company: int = 3,
) -> bool:
    """Search internet for job openings from companies in links.txt and log to Google Sheets."""
    if sheets_client is None or search_internet_for_companies is None:
        log.info("Internet job search skipped (Sheets or search module unavailable).")
        return False

    try:
        log.info("Starting internet search for job openings from links.txt companies...")

        # Run search in thread to avoid blocking
        results = await asyncio.to_thread(
            search_internet_for_companies,
            links_file,
            max_companies,
            max_results_per_company,
        )

        if not results:
            log.info("No internet search results obtained.")
            return False

        # Convert search results to career opening format for Google Sheets
        opening_rows = []
        for search_result in results:
            company_name = search_result.get("company_name", "")
            company_url = search_result.get("company_url", "")
            timestamp = search_result.get("timestamp", "")
            found_openings = search_result.get("found_openings", [])

            for opening in found_openings:
                row = {
                    "timestamp": timestamp,
                    "job_title": opening.get("title", ""),
                    "company": company_name,
                    "location": "",  # Not available from internet search
                    "type": "",  # Not available
                    "apply_link": opening.get("url", ""),
                    "posted_date": "",  # Not available
                    "source_url": company_url,
                    "status": "New",
                    "description": opening.get("description", ""),
                }
                if row.get("apply_link") and row.get("job_title"):
                    opening_rows.append(row)

        if opening_rows:
            # Append to Google Sheets
            appended_count = sheets_client.append_career_opening_rows(opening_rows)
            log.info(
                "Internet search: appended %d job openings to Google Sheets from %d companies.",
                appended_count,
                len(results),
            )
            return appended_count > 0

        return False

    except Exception as exc:
        log.warning("Internet search for companies failed: %s", exc)
        return False


def _decode_base64_creds() -> str:
    """Read GOOGLE_CREDENTIALS_JSON from env. Try base64 decode if needed."""
    import base64
    creds_file = os.path.join(SCRIPT_DIR, "credentials.json")
    if os.path.isfile(creds_file):
        with open(creds_file, "r", encoding="utf-8") as f:
            content = f.read().strip()
        if content.startswith("{"):
            log.info("Loaded Google credentials from credentials.json file.")
            return content

    raw = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    if not raw:
        return ""
    if raw.startswith("{"):
        return raw
    try:
        decoded = base64.b64decode(raw).decode("utf-8")
        if decoded.startswith("{"):
            log.info("Decoded base64 Google credentials from env var.")
            return decoded
    except Exception:
        pass
    return raw


def _build_notification_manager(
    config: ConfigLoader,
    ai_client: GroqClient | None = None,
):
    """Build NotificationManager from environment setup."""
    from notification_manager import NotificationManager

    sheets_client = None
    creds_json = _decode_base64_creds()
    sheet_id = os.environ.get("GOOGLE_SHEET_ID", "").strip()
    sa_email = os.environ.get("GOOGLE_SERVICE_ACCOUNT_EMAIL", "").strip()

    if creds_json and sheet_id:
        try:
            from google_sheets_client import GoogleSheetsClient
            sheets_client = GoogleSheetsClient(creds_json, sheet_id, sa_email)
        except Exception as exc:
            log.warning("Google Sheets init failed (non-fatal): %s", exc)

    email_notifier = None
    if os.environ.get("EMAIL_SENDER") or os.environ.get("SMTP_SENDER_EMAIL"):
        try:
            from email_notifier import EmailNotifier
            email_notifier = EmailNotifier()
        except Exception as exc:
            log.warning("Email notifier init failed (non-fatal): %s", exc)

    sheet_link = f"https://docs.google.com/spreadsheets/d/{sheet_id}" if sheet_id else ""

    return NotificationManager(
        sheets_client=sheets_client,
        email_notifier=email_notifier,
        sheet_link=sheet_link,
        ai_client=ai_client,
        url_change_alert_max_events=int(config.get("url_change_alert_max_events", 12)),
        url_change_max_events_per_cycle=int(config.get("url_change_max_events_per_cycle", 40)),
        url_change_max_openings_per_event=int(config.get("url_change_max_openings_per_event", 15)),
        url_change_max_openings_per_cycle=int(config.get("url_change_max_openings_per_cycle", 180)),
        url_change_log_baseline_openings=_as_bool(config.get("url_change_log_baseline_openings", False), default=False),
    )


def _run_single_cycle(args: argparse.Namespace, cycle_number: int = 1) -> None:
    log.info("=" * 60)
    log.info(
        "  Job Monitor cycle #%d starting%s",
        cycle_number,
        " (DRY RUN)" if args.dry_run else "",
    )
    log.info("=" * 60)

    # 1. Check for pause
    if os.path.isfile(PAUSE_FILE):
        log.info("⏸️  pause.txt found — exiting gracefully (Paused by user).")
        return

    # 2. Load config and state
    config = ConfigLoader()
    state_mgr = StateManager(STATE_FILE, config.get("max_notified_ids"))

    log.info(
        "Effective JSearch settings: concurrency=%s, fail_fast_on_429=%s, retries=%s, cooldown=%ss",
        config.get("jsearch_async_concurrency", 1),
        config.get("jsearch_fail_fast_on_429", True),
        config.get("jsearch_rate_limit_retries", 1),
        config.get("jsearch_rate_limit_cooldown_seconds", 900),
    )
    log.info(
        "Effective URL->Sheets settings: max_events=%s, max_openings_per_event=%s, max_openings_per_cycle=%s, baseline_openings=%s",
        config.get("url_change_max_events_per_cycle", 200),
        config.get("url_change_max_openings_per_event", 300),
        config.get("url_change_max_openings_per_cycle", 5000),
        config.get("url_change_log_baseline_openings", True),
    )

    log.info("API usage this month: %d calls",
             state_mgr.state.get("api_usage", {}).get("count", 0))

    # 3. Build clients
    groq_ai = GroqClient(
        min_call_interval_seconds=config.get("groq_min_call_interval_seconds", 1.2),
        state_manager=state_mgr,
        daily_limit=config.get("groq_daily_limit", 500),
        safety_buffer=config.get("groq_safety_buffer", 50),
    )
    filter_engine = FilterEngine(
        fuzzy_threshold=config.get("fuzzy_match_threshold"),
        max_age_days=config.get("job_max_age_days"),
        ai_client=groq_ai,
        ai_confidence_threshold=config.get("ai_confidence_threshold", 70),
    )
    jsearch = JSearchClient(
        timeout=config.get("request_timeout"),
        fail_fast_on_429=_as_bool(config.get("jsearch_fail_fast_on_429", True), default=True),
        rate_limit_cooldown_seconds=int(config.get("jsearch_rate_limit_cooldown_seconds", 900)),
        max_retries=max(1, int(config.get("jsearch_rate_limit_retries", 1))),
    )

    career_scraper = None
    enable_playwright = _as_bool(config.get("enable_playwright_scraper", True), default=True)
    enable_langchain = _as_bool(config.get("enable_langchain_scraper", True), default=True)
    enable_crewai = _as_bool(config.get("enable_crewai_scraper", True), default=True)

    if enable_playwright or enable_langchain or enable_crewai:
        if MultiStrategyCareerScraper is None:
            log.warning("Multi-strategy scraper package unavailable; using requests fallback only.")
        else:
            try:
                career_scraper = MultiStrategyCareerScraper(
                    headless=_as_bool(config.get("playwright_headless", True), default=True),
                    timeout_ms=max(5, int(config.get("playwright_timeout_seconds", 30))) * 1000,
                    enable_playwright=enable_playwright,
                    enable_langchain=enable_langchain,
                    enable_crewai=enable_crewai,
                )
            except Exception as exc:
                log.warning("Failed to initialize career scraper stack; using requests fallback: %s", exc)

    if os.path.isfile(PAUSE_FILE):
        log.info("⏸️  pause.txt created by external logic — exiting gracefully.")
        return

    notifier = None
    try:
        notifier = _build_notification_manager(config=config, ai_client=groq_ai)
    except Exception as exc:
        log.error("Failed to build NotificationManager: %s", exc)

    if args.test_mode:
        try:
            from test_notifications import run_all_tests

            test_results = run_all_tests(notifier=notifier)
            passed = [k for k, v in test_results.items() if v]
            failed = [k for k, v in test_results.items() if not v]

            log.info("Test mode completed. Passed: %s", ", ".join(passed) if passed else "none")
            if failed:
                log.warning("Test mode failures: %s", ", ".join(failed))
        except Exception as exc:
            log.error("Test mode failed to run notification checks: %s", exc)
        return

    if args.health_check_only:
        if notifier:
            health = notifier.health_check()
            for ch, status in health.items():
                log.info("Health %s: %s", ch, status)
        return

    # 4. Prepare search inputs and API query budget
    titles = ConfigLoader.load_job_titles(JOBS_FILE)
    filters = ConfigLoader.load_filters(FILTERS_FILE)
    locations = config.get("search_locations")
    monitored_urls = ConfigLoader.load_urls(LINKS_FILE)
    company_hints = extract_company_hints_from_urls(
        monitored_urls,
        max_companies=int(config.get("company_targeted_max_companies", 25)),
    )

    enable_jsearch_api = _as_bool(config.get("enable_jsearch_api", False), default=False)
    if not enable_jsearch_api:
        log.info("JSearch API disabled (enable_jsearch_api=false). Using links.txt scraping only.")
        query_plan = []
        allowed_this_run = 0
    else:
        jsearch_monthly_limit = int(config.get("jsearch_monthly_limit", 200))
        jsearch_safety_buffer = int(config.get("jsearch_safety_buffer", 10))
        jsearch_max_queries_per_run = int(config.get("jsearch_max_queries_per_run", 3))

        remaining_monthly = state_mgr.get_remaining_api_requests(jsearch_monthly_limit)
        allowed_this_run = max(
            0,
            min(
                jsearch_max_queries_per_run,
                remaining_monthly - jsearch_safety_buffer,
            ),
        )

        log.info(
            "JSearch budget: used=%d/%d, remaining=%d, allowed_this_run=%d",
            state_mgr.get_api_usage_count(),
            jsearch_monthly_limit,
            remaining_monthly,
            allowed_this_run,
        )

        if state_mgr.should_skip_due_to_rate_limit(jsearch_monthly_limit, jsearch_safety_buffer) or allowed_this_run <= 0:
            log.error(
                "JSearch quota protection active. Skipping API calls this run (monthly limit=%d, safety buffer=%d).",
                jsearch_monthly_limit,
                jsearch_safety_buffer,
            )
            titles = []
            allowed_this_run = 0

        query_plan = build_jsearch_query_plan(
            titles=titles,
            locations=locations if isinstance(locations, list) else [],
            company_hints=company_hints,
            allowed_queries=allowed_this_run,
            company_targeted_enabled=_as_bool(config.get("company_targeted_search_enabled", True), default=True),
            company_max_queries=int(config.get("company_targeted_max_queries_per_run", 4)),
        )

    all_qualified_jobs = []

    # 5. Run links.txt scraping + JSearch API side-by-side
    url_change_events, query_results, monitor_activity_rows = _run_async(
        _run_sources_async(
            state_mgr=state_mgr,
            scraper=career_scraper,
            max_pages_per_site=int(config.get("link_scraper_max_pages", 8)),
            max_openings_per_page=int(
                config.get(
                    "link_scraper_max_openings_per_site",
                    config.get("playwright_max_openings_per_page", 80),
                )
            ),
            monitor_concurrency=max(1, int(config.get("url_monitor_async_concurrency", 4))),
            jsearch=jsearch,
            query_plan=query_plan,
            jsearch_concurrency=max(1, int(config.get("jsearch_async_concurrency", 2))),
        )
    )
    state_mgr.save_state()

    queries_used = 0
    company_query_count = 0
    generic_query_count = 0

    for query_result in query_results:
        task = query_result.get("task") if isinstance(query_result.get("task"), dict) else {}
        query_text = str(task.get("query") or "").strip()
        if not query_text:
            continue

        attempted = bool(query_result.get("attempted", True))
        if not attempted:
            continue

        source_tag = str(task.get("source") or "JSEARCH_API")
        target_company = str(task.get("company") or "").strip()
        raw_results = query_result.get("raw_results") if isinstance(query_result.get("raw_results"), list) else []

        state_mgr.track_api_usage()
        queries_used += 1

        if source_tag == "JSEARCH_COMPANY_TARGETED":
            company_query_count += 1
        else:
            generic_query_count += 1

        for raw in raw_results:
            job = filter_engine.qualify_job(raw, titles, filters)
            if not job:
                continue

            employer_name = str(job.get("employer_name") or "")
            if source_tag == "JSEARCH_COMPANY_TARGETED":
                if target_company and _company_matches_employer(target_company, employer_name):
                    job["source"] = "JSEARCH_COMPANY_TARGETED"
                    job["notes"] = f"Target company from links.txt: {target_company}"
                else:
                    job["source"] = "JSEARCH_OTHER_COMPANY_DISCOVERY"
                    if target_company:
                        job["notes"] = f"Discovered while searching links.txt company: {target_company}"
            else:
                job["source"] = "JSEARCH_API"

            all_qualified_jobs.append(job)

    if query_plan:
        log.info(
            "JSearch query execution summary: total=%d, company_targeted=%d, generic=%d",
            queries_used,
            company_query_count,
            generic_query_count,
        )

    # Sort by relevance score
    all_qualified_jobs.sort(key=lambda j: j.get("score", 0), reverse=True)

    # Apply limit
    if args.job_limit > 0:
        all_qualified_jobs = all_qualified_jobs[:args.job_limit]

    # ── NEW: build job list from links.txt change events ────────────────
    # This avoids noisy JSearch results and ensures we only notify jobs
    # actually present on the career pages listed in `links.txt`.
    url_based_qualified_jobs: list[dict[str, object]] = []
    url_based_new_jobs: list[dict[str, object]] = []
    url_based_new_job_ids: list[str] = []
    url_based_seen_ids: set[str] = set()

    # Cache job-details fetches per apply link (many job cards share links).
    job_details_cache: dict[str, dict[str, str]] = {}
    details_fetch_count = 0
    details_fetch_cap = int(config.get("job_details_max_per_cycle", 20))
    min_desc_chars = int(config.get("job_description_min_chars", 120))
    url_change_log_baseline_openings = _as_bool(config.get("url_change_log_baseline_openings", True), default=True)

    def _event_openings_to_consider(event: dict[str, object]) -> list[dict[str, object]]:
        change_type = str(event.get("change_type") or "content_changed")
        openings = event.get("new_openings")
        if isinstance(openings, list) and openings:
            return [o for o in openings if isinstance(o, dict)]
        if change_type == "new_url_tracked" and url_change_log_baseline_openings:
            base_openings = event.get("openings")
            if isinstance(base_openings, list):
                return [o for o in base_openings if isinstance(o, dict)]
        return []

    for event in url_change_events:
        if len(url_based_new_jobs) >= details_fetch_cap:
            break

        event_url = str(event.get("url") or "")
        event_domain = str(event.get("domain") or "")

        for opening in _event_openings_to_consider(event):
            opening_title = str(opening.get("title") or "").strip()
            opening_link = str(opening.get("link") or opening.get("apply_link") or opening.get("job_url") or "").strip()

            if not opening_title or not opening_link:
                continue

            apply_link_norm = normalize_apply_link(opening_link)
            if not apply_link_norm:
                continue

            company = str(opening.get("company") or event_domain or "").strip()
            location = str(opening.get("location") or "").strip()
            job_type = str(opening.get("type") or opening.get("job_type") or "").strip()
            posted_date_hint = str(opening.get("posted_date") or opening.get("posted_at") or "").strip()

            # Structural validation before doing network calls.
            if not is_valid_job_posting(
                {
                    "title": opening_title,
                    "apply_link": apply_link_norm,
                    "job_url": apply_link_norm,
                    "source_url": event_url,
                }
            ):
                continue

            # Title-only role filter: cheap prefilter.
            matched, matched_role, _score = matches_target_role(
                opening_title,
                description="",
                target_roles=titles,
                exclude_senior=True,
            )
            if not matched or not matched_role:
                continue

            # Fetch description snippet from the job details page (apply link).
            if apply_link_norm in job_details_cache:
                snippet = job_details_cache[apply_link_norm]
            else:
                if details_fetch_count >= details_fetch_cap:
                    # Hard cap to keep runtime inside GitHub Actions budget.
                    break
                snippet = fetch_job_description_snippet(
                    apply_link_norm,
                    scraper=career_scraper,
                    timeout_seconds=int(config.get("request_timeout", 15)),
                    min_chars=min_desc_chars,
                    max_chars=1500,
                )
                job_details_cache[apply_link_norm] = snippet
                details_fetch_count += 1

            description = str(snippet.get("description") or "").strip()
            if len(description) < min_desc_chars:
                continue

            # Title + description semantic/keyword verification.
            matched2, matched_role2, match_score = matches_target_role(
                opening_title,
                description=description,
                target_roles=titles,
                exclude_senior=True,
            )
            if not matched2 or not matched_role2:
                continue

            # Build stable job key and dedupe vs state.json.
            job_id = stable_job_dedupe_key(opening_title, company, apply_link_norm)
            job_row = job_dict_for_sheet(
                title=opening_title,
                company=company or event_domain,
                location=location or "",
                job_type=job_type or "",
                posted_date=str(snippet.get("posted_date") or posted_date_hint or ""),
                apply_link=apply_link_norm,
                description=description,
                matched_role=matched_role2,
                match_score=float(match_score or 0),
                source_url=event_url or str(event.get("resolved_url") or ""),
            )
            job_row["job_id"] = job_id  # internal use for state dedupe

            url_based_qualified_jobs.append(job_row)

            if job_id in url_based_seen_ids:
                continue

            if state_mgr.is_new_job(job_id):
                url_based_seen_ids.add(job_id)
                url_based_new_jobs.append(job_row)
                url_based_new_job_ids.append(job_id)
            # Stop if we've filled the per-cycle job cap for notifications.
            if len(url_based_new_jobs) >= details_fetch_cap:
                break

        if len(url_based_new_jobs) >= details_fetch_cap:
            break

    # Prefer the URL-based pipeline (mission requirement: no noisy discovery).
    if url_based_new_jobs:
        all_qualified_jobs = url_based_qualified_jobs
        new_jobs = url_based_new_jobs
    else:
        # Fallback to existing JSearch path only if URL-based extraction found nothing
        # AND JSearch is enabled.
        if enable_jsearch_api:
            new_jobs = filter_engine.deduplicate_jobs(all_qualified_jobs, state_mgr)
        else:
            new_jobs = []

    # Local audit output (title + description + link).
    if new_jobs:
        try:
            output_dir = os.path.join(SCRIPT_DIR, "job_outputs")
            os.makedirs(output_dir, exist_ok=True)
            stamp = time.strftime("%Y%m%d_%H%M%S", time.gmtime())

            md_path = os.path.join(output_dir, f"new_jobs_{stamp}.md")
            json_path = os.path.join(output_dir, f"new_jobs_{stamp}.json")

            with open(md_path, "w", encoding="utf-8") as f:
                f.write(f"# New Jobs ({stamp} UTC)\n\n")
                for j in new_jobs:
                    f.write(f"## {j.get('job_title','').strip()}\n")
                    f.write(f"- Apply: {j.get('apply_link','').strip()}\n")
                    f.write(f"- Company: {j.get('employer_name','').strip()}\n")
                    if j.get("location"):
                        f.write(f"- Location: {j.get('location','').strip()}\n")
                    f.write("\n")
                    desc = str(j.get("description", "")).strip()
                    if desc:
                        f.write(desc + "\n\n")
                    else:
                        f.write("(No description captured)\n\n")

            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(new_jobs, f, indent=2, ensure_ascii=False)

            log.info("Wrote job output files: %s, %s", md_path, json_path)
        except Exception as exc:
            log.warning("Failed to write job output files: %s", exc)

    print("\n" + "=" * 60)
    print("  EXTRACTED JOB RESULTS")
    print(f"  Qualified jobs     : {len(all_qualified_jobs)}")
    print(f"  New (not notified) : {len(new_jobs)}")
    print("=" * 60 + "\n")

    # 6. Notify (links.txt changes first, then JSearch jobs)
    if url_change_events or new_jobs:
        if args.dry_run:
            log.info("DRY RUN: would notify %d URL changes + %d new jobs.",
                     len(url_change_events), len(new_jobs))
        elif notifier is not None:
            try:
                if url_change_events:
                    # Mission mode: avoid noisy "URL changes / Career Openings Log"
                    # writes when we only want validated job postings.
                    _run_async(
                        _notify_url_changes_async(
                            notifier,
                            url_change_events,
                            record_to_sheets=_as_bool(
                                config.get("record_url_changes_to_sheets", False),
                                default=False,
                            ),
                        )
                    )
                if new_jobs:
                    notify_result = notifier.notify_new_jobs(new_jobs)
                    # Mark as notified only if at least one channel succeeded.
                    if notify_result and any(bool(v) for v in notify_result.values()):
                        for job_id in url_based_new_job_ids:
                            state_mgr.mark_as_notified(job_id)
                        state_mgr.save_state()
            except Exception as exc:
                log.error("Notification pipeline crashed: %s", exc)
    else:
        log.info("Nothing to report — no URL changes and no new job matches.")

    if not args.dry_run and notifier is not None and monitor_activity_rows:
        record_search_activity = _as_bool(config.get("record_search_activity_to_sheets", True), default=True)
        if record_search_activity and hasattr(notifier, "record_search_activity_in_sheet"):
            try:
                notifier.record_search_activity_in_sheet(monitor_activity_rows)
            except Exception as exc:
                log.warning("Search activity logging failed (non-fatal): %s", exc)

    # 7. Search internet for additional job openings from companies in links.txt
    if not args.dry_run:
        try:
            sheets_client = None
            if notifier is not None and hasattr(notifier, "_sheets"):
                sheets_client = notifier._sheets

            if sheets_client is not None:
                enable_internet_search = _as_bool(
                    config.get("enable_internet_company_search", True),
                    default=True,
                )
                if enable_internet_search:
                    _run_async(
                        _search_internet_for_companies_async(
                            sheets_client=sheets_client,
                            links_file=LINKS_FILE,
                            max_companies=int(config.get("internet_search_max_companies", 15)),
                            max_results_per_company=int(config.get("internet_search_max_results_per_company", 3)),
                        )
                    )
        except Exception as exc:
            log.warning("Internet company search failed (non-fatal): %s", exc)

    log.info("Job Monitor cycle #%d finished.", cycle_number)


def run_repeating_pipeline(
    run_once_callable,
    interval_seconds: float,
    max_cycles: int = 0,
    sleep_fn=time.sleep,
) -> int:
    """Run monitor cycles once or repeatedly with robust exception handling."""
    cycles = 0
    interval = max(0.0, float(interval_seconds))
    max_runs = max(0, int(max_cycles))

    while True:
        cycles += 1
        try:
            run_once_callable(cycles)
        except Exception as exc:
            log.error("Cycle #%d failed unexpectedly: %s", cycles, exc, exc_info=True)

        if interval <= 0:
            break

        if max_runs > 0 and cycles >= max_runs:
            break

        log.info(
            "Next cycle scheduled in %.2f hour(s) (%.0f seconds).",
            interval / 3600,
            interval,
        )
        sleep_fn(interval)

    return cycles


def _run_test_url(url: str) -> None:
    """Scrape a single URL, apply role filtering, and print results. No Sheets/Telegram."""
    from config_loader import ConfigLoader

    print(f"\n{'=' * 60}")
    print(f"  TEST URL: {url}")
    print(f"{'=' * 60}\n")

    config = ConfigLoader()
    titles = ConfigLoader.load_job_titles(JOBS_FILE)

    # Try multi-strategy scraper
    scraper = None
    if MultiStrategyCareerScraper is not None:
        try:
            scraper = MultiStrategyCareerScraper(headless=True, timeout_ms=30_000)
        except Exception as exc:
            log.warning("Scraper init failed: %s", exc)

    if scraper is not None:
        result = scraper.scrape_site_openings_sync(url, max_pages=3, max_openings=50)
    else:
        # Fallback: requests + BS4
        try:
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0"}
            resp = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
            resp.raise_for_status()
            page_html = resp.text
            all_jobs = extract_job_postings(page_html, url, max_results=50)
            result = {"ok": True, "openings": [{"title": j.get("title", ""), "link": j.get("apply_link", "")} for j in all_jobs]}
        except Exception as exc:
            print(f"  ❌ Failed to fetch URL: {exc}")
            return

    if not result.get("ok"):
        print(f"  ❌ Scrape failed: {result.get('error', 'unknown')}")
        return

    all_openings = result.get("openings", [])
    print(f"  📋 Total openings extracted: {len(all_openings)}")

    # Apply role filtering
    filtered = filter_jobs_by_role(all_openings, target_roles=titles, min_score=50.0, exclude_senior=True)
    print(f"  ✅ After role filtering: {len(filtered)}")

    if not all_openings:
        print("  (No openings found on this page)")
        return

    print(f"\n{'─' * 60}")
    print("  ALL EXTRACTED OPENINGS:")
    print(f"{'─' * 60}")
    for i, opening in enumerate(all_openings[:30], 1):
        title = opening.get("title", "?")
        link = opening.get("link", "?")
        matched, role, score = matches_target_role(title, target_roles=titles)
        status = f"✅ {role} ({score:.0f})" if matched else "❌ filtered out"
        print(f"  {i:3d}. {title}")
        print(f"       Link: {link}")
        print(f"       Status: {status}")

    if filtered:
        print(f"\n{'─' * 60}")
        print("  ROLE-FILTERED RESULTS (these would be synced to Sheets):")
        print(f"{'─' * 60}")
        # Fetch descriptions for a small preview set only.
        preview_limit = 8
        for i, job in enumerate(filtered[:preview_limit], 1):
            title = str(job.get("title", "")).strip()
            link = str(job.get("link", "")).strip()
            link_norm = normalize_apply_link(link)

            description_preview = ""
            if link_norm:
                try:
                    snippet = fetch_job_description_snippet(
                        link_norm,
                        scraper=scraper,
                        timeout_seconds=int(config.get("request_timeout", 15)),
                        min_chars=int(config.get("job_description_min_chars", 120)),
                        max_chars=700,
                    )
                    description_preview = str(snippet.get("description", "")).strip()
                except Exception as exc:
                    log.warning("Job description preview failed: %s (%s)", link_norm, exc)

            print(f"  {i:3d}. {title}")
            print(f"       Matched: {job.get('matched_role', '?')} (score: {job.get('match_score', 0)})")
            print(f"       Link: {link_norm or link}")
            if description_preview:
                print(f"       Desc: {description_preview[:220]}...")
            else:
                print(f"       Desc: (not captured)")

    print(f"\n{'=' * 60}\n")


def main() -> None:
    load_dotenv(os.path.join(SCRIPT_DIR, ".env"))

    parser = argparse.ArgumentParser(description="Job Monitor Orchestrator")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run without sending notifications.")
    parser.add_argument("--test-mode", action="store_true",
                        help="Run standalone notification channel tests and exit.")
    parser.add_argument("--job-limit", type=int, default=0,
                        help="Max jobs to process.")
    parser.add_argument("--health-check-only",
                        action="store_true", help="Only run health check.")
    parser.add_argument(
        "--every-hours",
        type=float,
        default=0.0,
        help="Repeat monitor run every N hours (e.g., 12 for twice daily).",
    )
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=0,
        help="Optional cap for repeated mode (0 = unlimited). Useful for testing.",
    )
    parser.add_argument(
        "--test-url",
        type=str,
        default="",
        help="Test scraping a single URL: extract and role-filter openings, print results, then exit.",
    )
    args = parser.parse_args()

    if args.every_hours > 0 and (args.test_mode or args.health_check_only):
        log.info("Ignoring --every-hours in test/health-check mode (single cycle only).")
        args.every_hours = 0.0

    # ── --test-url: scrape a single URL, apply role filter, print results ──
    if args.test_url:
        _run_test_url(args.test_url)
        return

    run_repeating_pipeline(
        lambda cycle: _run_single_cycle(args, cycle_number=cycle),
        interval_seconds=max(0.0, float(args.every_hours)) * 3600,
        max_cycles=args.max_cycles,
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Interrupted by user.")
    except Exception as exc:
        log.critical("Unhandled exception: %s", exc, exc_info=True)
    sys.exit(0)
