"""
Job Monitoring System
─────────────────────
Main orchestrator script connecting config, API, filtering, and notifications.
"""

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
import requests
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup
from dotenv import load_dotenv

from config_loader import ConfigLoader
from filter_engine import FilterEngine
from groq_client import GroqClient
from jsearch_client import JSearchClient
from state_manager import StateManager
from telegram_bot import TelegramBot

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

    # Prefer structured data first when present.
    for item in _extract_openings_from_jsonld(base_url, soup, max_positions=max_positions):
        fp = _opening_fingerprint(item)
        if fp in seen:
            continue
        seen.add(fp)
        openings.append(item)

        if len(openings) >= max_positions:
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

        if len(openings) >= max_positions:
            break

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


def monitor_urls(
    state_mgr: StateManager,
    scraper: object | None = None,
    max_pages_per_site: int = 6,
    max_openings_per_page: int = 50,
) -> list[dict[str, object]]:
    """Check URLs from links.txt for content changes and extract opening details."""
    urls = ConfigLoader.load_urls(LINKS_FILE)
    if not urls:
        log.warning("No valid URLs to monitor.")
        return []

    change_events: list[dict[str, object]] = []
    errors = []

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    with requests.Session() as session:
        for url in urls:
            try:
                page_html = ""
                resolved_url = url
                new_hash = ""
                scraper_used = "requests"
                scraper_error = ""
                scraped_openings: list[dict[str, str]] = []
                scraped_page_title = ""
                pages_visited: list[str] = []

                # Prefer full-site (pagination-aware) scraping when available.
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
                                str(page_url) for page_url in (site_result.get("pages_visited") or []) if str(page_url).strip()
                            ]
                            scraped_openings = [
                                item for item in (site_result.get("openings") or [])
                                if isinstance(item, dict)
                            ]

                            # Change detection key based on extracted openings + visited pages.
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
                            scraper_error = str((site_result or {}).get("error") if isinstance(site_result, dict) else "unknown scrape_site_openings_sync error")
                    except Exception as exc:
                        scraper_error = str(exc)

                if scraper is not None and hasattr(scraper, "scrape_job_sync"):
                    if not new_hash:
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
                    if scraper_error:
                        log.warning("Preferred scraper failed for %s (%s). Falling back to requests.", url, scraper_error)

                    resp = session.get(url, headers=headers,
                                       timeout=15, allow_redirects=True)
                    resp.raise_for_status()
                    page_html = resp.text or ""
                    resolved_url = str(resp.url or url)
                    new_hash = hashlib.sha256(resp.content).hexdigest()
                    scraper_used = "requests"

                old_hash = state_mgr.get_url_hash(url)
                change_type = ""
                if old_hash is None:
                    change_type = "new_url_tracked"
                elif new_hash != old_hash:
                    change_type = "content_changed"
                else:
                    log.info("✅  No change:       %s", url)

                if change_type:
                    if scraped_openings:
                        page_title = scraped_page_title
                        openings = scraped_openings
                    else:
                        page_title, openings = _extract_openings_from_html(
                            resolved_url,
                            page_html,
                            max_positions=max(1, int(max_openings_per_page)),
                        )

                    previous_fingerprints = state_mgr.get_url_opening_fingerprints(url)
                    current_fingerprints = {_opening_fingerprint(o) for o in openings}
                    new_openings = [o for o in openings if _opening_fingerprint(o) not in previous_fingerprints]

                    state_mgr.set_url_opening_fingerprints(url, list(current_fingerprints))

                    if change_type == "new_url_tracked":
                        log.info(
                            "🆕  New URL tracked: %s (openings detected: %d)",
                            url,
                            len(openings),
                        )
                    else:
                        log.info(
                            "🔄  Change detected: %s (openings: %d, new openings: %d)",
                            url,
                            len(openings),
                            len(new_openings),
                        )

                    change_events.append(
                        {
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
                    )

                state_mgr.set_url_hash(url, new_hash)
            except requests.RequestException as exc:
                log.error("Request error for %s — %s", url, exc)
                errors.append(url)
            except Exception as exc:
                log.error("Unexpected monitor error for %s — %s", url, exc)
                errors.append(url)

    print("\n" + "=" * 60)
    print("  URL MONITORING SUMMARY")
    print(f"  Total    : {len(urls)}")
    print(f"  Changed  : {len(change_events)}")
    print(f"  Errors   : {len(errors)}")
    print("=" * 60 + "\n")
    return change_events


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


def _build_notification_manager(telegram_bot: TelegramBot, ai_client: GroqClient | None = None):
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
        telegram_bot=telegram_bot,
        sheets_client=sheets_client,
        email_notifier=email_notifier,
        sheet_link=sheet_link,
        ai_client=ai_client,
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

    log.info("API usage this month: %d calls",
             state_mgr.state.get("api_usage", {}).get("count", 0))

    # 3. Build clients
    telegram = TelegramBot()
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
    jsearch = JSearchClient(timeout=config.get("request_timeout"))

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

    # Process telegram commands (e.g. /status, /pause) immediately
    telegram.process_updates(state_mgr)

    if os.path.isfile(PAUSE_FILE):
        log.info("⏸️  pause.txt created by bot — exiting gracefully.")
        return

    notifier = None
    try:
        notifier = _build_notification_manager(telegram, ai_client=groq_ai)
    except Exception as exc:
        log.error("Failed to build NotificationManager: %s", exc)

    # Re-process /status so that it can access `notifier.health_check()`
    telegram.process_updates(state_mgr, notifier=notifier)

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

    # 4. Monitor URLs (links.txt) using Playwright first, then requests fallback.
    try:
        url_change_events = monitor_urls(
            state_mgr,
            scraper=career_scraper,
            max_pages_per_site=int(config.get("link_scraper_max_pages", 8)),
            max_openings_per_page=int(
                config.get(
                    "link_scraper_max_openings_per_site",
                    config.get("playwright_max_openings_per_page", 80),
                )
            ),
        )
        state_mgr.save_state()
    except Exception as exc:
        log.error("URL monitoring crashed: %s", exc)
        url_change_events = []

    # 5. Search jobs from jobs.txt using JSearch API
    titles = ConfigLoader.load_job_titles(JOBS_FILE)
    filters = ConfigLoader.load_filters(FILTERS_FILE)
    locations = config.get("search_locations")
    monitored_urls = ConfigLoader.load_urls(LINKS_FILE)
    company_hints = extract_company_hints_from_urls(
        monitored_urls,
        max_companies=int(config.get("company_targeted_max_companies", 25)),
    )

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

    all_qualified_jobs = []

    # Add an inter-query delay as a defensive practice.
    rate_limit_delay = 1

    query_plan = build_jsearch_query_plan(
        titles=titles,
        locations=locations if isinstance(locations, list) else [],
        company_hints=company_hints,
        allowed_queries=allowed_this_run,
        company_targeted_enabled=_as_bool(config.get("company_targeted_search_enabled", True), default=True),
        company_max_queries=int(config.get("company_targeted_max_queries_per_run", 4)),
    )

    queries_used = 0
    company_query_count = 0
    generic_query_count = 0

    for task in query_plan:
        query_text = str(task.get("query") or "").strip()
        location_text = str(task.get("location") or "").strip()
        source_tag = str(task.get("source") or "JSEARCH_API")
        target_company = str(task.get("company") or "").strip()

        if not query_text:
            continue

        shown_query = f"{query_text} in {location_text}" if location_text else query_text
        log.info("🔍  Searching: %s", shown_query)

        state_mgr.track_api_usage()
        queries_used += 1

        if source_tag == "JSEARCH_COMPANY_TARGETED":
            company_query_count += 1
        else:
            generic_query_count += 1

        raw_results = jsearch.search_jobs(query_text, location_text)

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

        time.sleep(rate_limit_delay)

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

    # Deduplicate against state.json to find truly NEW jobs
    new_jobs = filter_engine.deduplicate_jobs(all_qualified_jobs, state_mgr)
    state_mgr.save_state()

    print("\n" + "=" * 60)
    print("  JSEARCH JOB RESULTS")
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
                    notifier.send_url_change_alert(url_change_events)
                    notifier.record_url_changes_in_sheet(url_change_events)
                if new_jobs:
                    notifier.notify_new_jobs(new_jobs)
            except Exception as exc:
                log.error("Notification pipeline crashed: %s", exc)
    else:
        log.info("Nothing to report — no URL changes and no new job matches.")

    # 7. Commit State
    if not args.dry_run:
        state_mgr.commit_to_github()

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
    args = parser.parse_args()

    if args.every_hours > 0 and (args.test_mode or args.health_check_only):
        log.info("Ignoring --every-hours in test/health-check mode (single cycle only).")
        args.every_hours = 0.0

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
