"""
Job Monitoring System
─────────────────────
Main orchestrator script connecting config, API, filtering, and notifications.
"""

import argparse
import hashlib
import logging
import os
import sys
import time
import requests
from dotenv import load_dotenv

from config_loader import ConfigLoader
from filter_engine import FilterEngine
from groq_client import GroqClient
from jsearch_client import JSearchClient
from state_manager import StateManager
from telegram_bot import TelegramBot

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


def monitor_urls(state_mgr: StateManager) -> list[str]:
    """Check URLs from links.txt for content changes."""
    urls = ConfigLoader.load_urls(LINKS_FILE)
    if not urls:
        log.warning("No valid URLs to monitor.")
        return []

    changed_urls = []
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
                resp = session.get(url, headers=headers,
                                   timeout=15, allow_redirects=True)
                resp.raise_for_status()
                new_hash = hashlib.sha256(resp.content).hexdigest()

                old_hash = state_mgr.get_url_hash(url)
                if old_hash is None:
                    log.info("🆕  New URL tracked: %s", url)
                elif new_hash != old_hash:
                    log.info("🔄  Change detected: %s", url)
                    changed_urls.append(url)
                else:
                    log.info("✅  No change:       %s", url)

                state_mgr.set_url_hash(url, new_hash)
            except requests.RequestException as exc:
                log.error("Request error for %s — %s", url, exc)
                errors.append(url)

    print("\n" + "=" * 60)
    print("  URL MONITORING SUMMARY")
    print(f"  Total    : {len(urls)}")
    print(f"  Changed  : {len(changed_urls)}")
    print(f"  Errors   : {len(errors)}")
    print("=" * 60 + "\n")
    return changed_urls


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


def main() -> None:
    load_dotenv(os.path.join(SCRIPT_DIR, ".env"))

    # 1. Parse arguments
    parser = argparse.ArgumentParser(description="Job Monitor Orchestrator")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run without sending notifications.")
    parser.add_argument("--test-mode", action="store_true",
                        help="Run standalone notification channel tests and exit.")
    parser.add_argument("--job-limit", type=int, default=0,
                        help="Max jobs to process.")
    parser.add_argument("--health-check-only",
                        action="store_true", help="Only run health check.")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("  Job Monitor starting%s", " (DRY RUN)" if args.dry_run else "")
    log.info("=" * 60)

    # 2. Check for pause
    if os.path.isfile(PAUSE_FILE):
        log.info("⏸️  pause.txt found — exiting gracefully (Paused by user).")
        return

    # 3. Load config and state
    config = ConfigLoader()
    state_mgr = StateManager(STATE_FILE, config.get("max_notified_ids"))

    log.info("API usage this month: %d calls",
             state_mgr.state.get("api_usage", {}).get("count", 0))

    # 4. Build clients
    telegram = TelegramBot()
    groq_ai = GroqClient()
    filter_engine = FilterEngine(
        fuzzy_threshold=config.get("fuzzy_match_threshold"),
        max_age_days=config.get("job_max_age_days"),
        ai_client=groq_ai,
        ai_confidence_threshold=config.get("ai_confidence_threshold", 70),
    )
    jsearch = JSearchClient(timeout=config.get("request_timeout"))

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

    # 5. Monitor URLs
    try:
        changed_urls = monitor_urls(state_mgr)
        state_mgr.save_state()
    except Exception as exc:
        log.error("URL monitoring crashed: %s", exc)
        changed_urls = []

    # 6. Search jobs
    titles = ConfigLoader.load_job_titles(JOBS_FILE)
    filters = ConfigLoader.load_filters(FILTERS_FILE)
    locations = config.get("search_locations")

    if state_mgr.should_skip_due_to_rate_limit():
        log.error(
            "API Limit approaching 500/month. Skipping JSearch to avoid quota exhaustion.")
        titles = []

    all_qualified_jobs = []

    # Add an inter-query delay as a defensive practice (Warning in Section B)
    rate_limit_delay = 1

    for title in titles:
        for location in locations:
            query = f"{title} in {location}"
            log.info("🔍  Searching: %s", query)

            state_mgr.track_api_usage()
            raw_results = jsearch.search_jobs(title, location)

            for raw in raw_results:
                job = filter_engine.qualify_job(raw, titles, filters)
                if job:
                    all_qualified_jobs.append(job)

            time.sleep(rate_limit_delay)

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

    # 7. Notify
    if changed_urls or new_jobs:
        if args.dry_run:
            log.info("DRY RUN: would notify %d URL changes + %d new jobs.",
                     len(changed_urls), len(new_jobs))
        elif notifier is not None:
            try:
                if changed_urls:
                    notifier.send_url_change_alert(changed_urls)
                if new_jobs:
                    notifier.notify_new_jobs(new_jobs)
            except Exception as exc:
                log.error("Notification pipeline crashed: %s", exc)
    else:
        log.info("Nothing to report — no URL changes and no new job matches.")

    # 8. Commit State
    if not args.dry_run:
        state_mgr.commit_to_github()

    log.info("Job Monitor finished.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Interrupted by user.")
    except Exception as exc:
        log.critical("Unhandled exception: %s", exc, exc_info=True)
    sys.exit(0)
