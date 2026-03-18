#!/usr/bin/env python3
"""
Weekly Report Generator
───────────────────────
Queries the Google Sheet for last 7 days' activity and sends a
summary email.  Designed to run standalone via GitHub Actions on
Sundays at 9 AM PKT (cron: '0 4 * * 0').

Usage:
    python weekly_report.py
"""

from __future__ import annotations

import logging
import os
import sys
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("weekly_report")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(SCRIPT_DIR, ".env"))


def main() -> None:
    log.info("=" * 50)
    log.info("  Weekly Report Generator")
    log.info("=" * 50)

    # ── 1. Decode Google credentials (reuse logic from monitor) ──────────
    import base64

    creds_json = ""
    creds_file = os.path.join(SCRIPT_DIR, "credentials.json")
    if os.path.isfile(creds_file):
        with open(creds_file, "r", encoding="utf-8") as f:
            content = f.read().strip()
        if content.startswith("{"):
            creds_json = content

    if not creds_json:
        raw = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
        if raw.startswith("{"):
            creds_json = raw
        elif raw:
            try:
                creds_json = base64.b64decode(raw).decode("utf-8")
            except Exception:
                pass

    sheet_id = os.environ.get("GOOGLE_SHEET_ID", "").strip()

    if not creds_json or not sheet_id:
        log.error("GOOGLE_CREDENTIALS_JSON and GOOGLE_SHEET_ID are required.")
        sys.exit(0)  # Don't crash CI

    # ── 2. Connect to Google Sheets ──────────────────────────────────────
    try:
        from google_sheets_client import GoogleSheetsClient
        client = GoogleSheetsClient(
            credentials_json=creds_json,
            sheet_id=sheet_id,
            service_account_email=os.environ.get(
                "GOOGLE_SERVICE_ACCOUNT_EMAIL", ""),
        )
    except Exception as exc:
        log.error("Failed to connect to Google Sheets: %s", exc)
        sys.exit(0)

    # ── 3. Gather stats ─────────────────────────────────────────────────
    stats = client.get_weekly_stats()
    log.info(
        "Stats: found=%d, applied=%d, interviews=%d",
        stats["total_found"], stats["total_applied"], stats["total_interviews"],
    )
    log.info("Top companies: %s", stats["top_companies"])
    log.info("Top keywords: %s", stats["top_keywords"])

    # ── 4. Run weekly archive ────────────────────────────────────────────
    import json

    state_file = os.path.join(SCRIPT_DIR, "state.json")
    state: dict = {}
    if os.path.isfile(state_file):
        with open(state_file, "r", encoding="utf-8") as f:
            state = json.load(f)

    archived = client.archive_old_jobs(state)
    if archived > 0:
        log.info("Archived %d old jobs.", archived)
        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)

    # ── 5. Send email report ────────────────────────────────────────────
    if os.environ.get("EMAIL_SENDER") or os.environ.get("SMTP_SENDER_EMAIL"):
        try:
            from email_notifier import EmailNotifier
            from email_templates import (
                render_weekly_report_html,
                render_weekly_report_plain,
            )

            sheet_link = f"https://docs.google.com/spreadsheets/d/{sheet_id}"

            html = render_weekly_report_html(
                stats["total_found"],
                stats["total_applied"],
                stats["total_interviews"],
                stats["top_companies"],
                stats["top_keywords"],
                sheet_link,
            )
            plain = render_weekly_report_plain(
                stats["total_found"],
                stats["total_applied"],
                stats["total_interviews"],
                stats["top_companies"],
                stats["top_keywords"],
                sheet_link,
            )

            notifier = EmailNotifier()
            subject = (
                f"📊 Weekly Job Report — "
                f"Found: {stats['total_found']} | "
                f"Applied: {stats['total_applied']} | "
                f"Interviews: {stats['total_interviews']}"
            )
            success = notifier._send(subject, html, plain)
            log.info("Email sent: %s", success)
        except Exception as exc:
            log.error("Failed to send weekly report email: %s", exc)
    else:
        log.info("Email not configured — skipping report email.")

    # ── 6. Send Telegram summary ─────────────────────────────────────────
    tg_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    tg_chat = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if tg_token and tg_chat:
        try:
            import requests
            text = (
                "📊 Weekly Job Report\n\n"
                f"Found: {stats['total_found']} | "
                f"Applied: {stats['total_applied']} | "
                f"Interviews: {stats['total_interviews']}\n\n"
                f"Top Companies: {', '.join(stats['top_companies'][:5]) or 'None'}\n"
                f"Top Keywords: {', '.join(stats['top_keywords'][:5]) or 'None'}"
            )
            if archived > 0:
                text += f"\n\n🗄️ Archived {archived} old jobs"

            api_url = f"https://api.telegram.org/bot{tg_token}/sendMessage"
            requests.post(api_url, json={
                "chat_id": tg_chat, "text": text,
                "disable_web_page_preview": True,
            }, timeout=10)
        except Exception as exc:
            log.warning("Telegram summary failed: %s", exc)

    log.info("Weekly report complete.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log.critical("Unhandled error: %s", exc, exc_info=True)
    sys.exit(0)
