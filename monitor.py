"""
Job Monitoring System
Monitors career page URLs for changes by comparing SHA256 hashes.
Sends Telegram notifications when changes are detected.
"""

import hashlib
import json
import logging
import os
import sys

import requests

# ── Configuration ────────────────────────────────────────────────────────────
LINKS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "links.txt")
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "state.json")
REQUEST_TIMEOUT = 15  # seconds

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Helpers ──────────────────────────────────────────────────────────────────
def load_urls(filepath: str) -> list[str]:
    """Read URLs from a text file, one per line. Blank lines are skipped."""
    if not os.path.isfile(filepath):
        log.error("Links file not found: %s", filepath)
        sys.exit(1)

    with open(filepath, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]
    log.info("Loaded %d URLs from %s", len(urls), filepath)
    return urls


def load_state(filepath: str) -> dict[str, str]:
    """Load previously stored hashes from state.json. Returns {} on first run."""
    if not os.path.isfile(filepath):
        return {}
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as exc:
        log.warning("Could not read state file (%s). Starting fresh.", exc)
        return {}


def save_state(filepath: str, state: dict[str, str]) -> None:
    """Persist the current URL → hash mapping to state.json."""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
    log.info("State saved to %s", filepath)


def fetch_hash(url: str) -> str | None:
    """Fetch a URL and return the SHA-256 hex digest of its body, or None on error."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return hashlib.sha256(resp.content).hexdigest()
    except requests.RequestException as exc:
        log.error("Failed to fetch %s — %s", url, exc)
        return None


# ── Telegram Notification ────────────────────────────────────────────────────
def send_telegram_notification(changed_urls: list[str]) -> None:
    """
    Send a Telegram message listing the changed URLs.
    Reads TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID from environment variables.
    Silently logs and returns on any failure so the workflow never crashes.
    """
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        log.warning("Telegram credentials not set. Skipping notification.")
        return

    url_list = "\n".join(f"• {u}" for u in changed_urls)
    message = (
        "🚨 Job Page Update Detected!\n\n"
        "The following pages have changed:\n"
        f"{url_list}\n\n"
        "Please check immediately."
    )

    api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "disable_web_page_preview": True,
    }

    try:
        resp = requests.post(api_url, json=payload, timeout=10)
        if resp.ok:
            log.info("✅ Telegram notification sent successfully.")
        else:
            log.warning("Telegram API returned %s: %s", resp.status_code, resp.text)
    except requests.RequestException as exc:
        log.warning("Failed to send Telegram notification: %s", exc)


# ── Main Logic ───────────────────────────────────────────────────────────────
def monitor() -> list[str]:
    """
    Run one monitoring cycle:
      1. Read URLs from links.txt
      2. Fetch each page and compute its SHA-256 hash
      3. Compare against the last-known hash in state.json
      4. Report any changes and update state.json

    Returns a list of URLs whose content has changed.
    """
    urls = load_urls(LINKS_FILE)
    state = load_state(STATE_FILE)
    changed_urls: list[str] = []
    errors: list[str] = []

    for url in urls:
        new_hash = fetch_hash(url)

        if new_hash is None:
            errors.append(url)
            continue  # keep old hash in state; skip comparison

        old_hash = state.get(url)

        if old_hash is None:
            log.info("🆕  New URL tracked: %s", url)
        elif new_hash != old_hash:
            log.info("🔄  Change detected: %s", url)
            changed_urls.append(url)
        else:
            log.info("✅  No change:       %s", url)

        # Always update to the latest hash
        state[url] = new_hash

    # Persist updated state
    save_state(STATE_FILE, state)

    # ── Summary ──────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  MONITORING SUMMARY")
    print("=" * 60)
    print(f"  Total URLs checked : {len(urls)}")
    print(f"  Changed            : {len(changed_urls)}")
    print(f"  Errors / Unreachable: {len(errors)}")
    print("=" * 60)

    if changed_urls:
        print("\n  ⚡ Changed URLs:")
        for u in changed_urls:
            print(f"     • {u}")

    if errors:
        print("\n  ⚠️  Unreachable URLs:")
        for u in errors:
            print(f"     • {u}")

    print()

    # ── Telegram notification ────────────────────────────────────────────
    if changed_urls:
        send_telegram_notification(changed_urls)

    return changed_urls


if __name__ == "__main__":
    changed = monitor()
    # Always exit 0 so the GitHub Action workflow doesn't crash
    sys.exit(0)
