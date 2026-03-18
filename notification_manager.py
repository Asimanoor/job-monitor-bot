"""
Notification Manager
────────────────────
Orchestrates all notification channels:
  1. Telegram  (fastest, best-effort)
  2. Google Sheets  (persistent record, always attempted)
  3. Email  (emergency fallback if both Telegram AND Sheets fail)
  4. failed_alerts.json  (last resort if ALL channels fail)
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Protocol

from telegram_bot import TelegramBot

log = logging.getLogger(__name__)

# Retry config for Sheets 429
_SHEETS_RETRY_DELAY = 30
_SHEETS_MAX_RETRIES = 1

FAILED_ALERTS_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "failed_alerts.json"
)


class SheetsClient(Protocol):
    def append_job_row(self, job_data: dict) -> bool: ...
    def update_job_status(self, job_apply_link: str,
                          new_status: str, notes: str = "") -> bool: ...

    def get_pending_jobs(self) -> list[dict]: ...
    def health_check(self) -> bool: ...


class Emailer(Protocol):
    def send_job_alert_email(
        self, jobs: list, search_query: str, sheet_link: str) -> bool: ...
    def send_daily_summary(self, total_found: int,
                           total_new: int, sheet_link: str) -> bool: ...

    def send_health_warning(self, health_results: dict) -> bool: ...
    def health_check(self) -> bool: ...
    @property
    def is_configured(self) -> bool: ...


class NotificationManager:
    """Orchestrate Telegram, Google Sheets, and Email notification channels."""

    def __init__(
        self,
        telegram_bot: TelegramBot | None = None,
        sheets_client: SheetsClient | None = None,
        email_notifier: Emailer | None = None,
        sheet_link: str = "",
        ai_client: Any | None = None,
    ) -> None:
        self._tg = telegram_bot
        self._sheets = sheets_client
        self._email = email_notifier
        self._sheet_link = sheet_link
        self._ai = ai_client

    def _enrich_jobs_with_ai(self, jobs: list[dict[str, Any]]) -> None:
        """Attach AI summary + cover-letter points, preserving graceful fallback behavior."""
        if self._ai is None:
            return

        for job in jobs:
            title = str(job.get("job_title", ""))
            company = str(job.get("employer_name") or job.get("company") or "")
            description = str(job.get("description", ""))

            if not job.get("ai_summary"):
                try:
                    job["ai_summary"] = self._ai.summarize_job(title, company, description)
                except Exception as exc:
                    log.warning("AI summary generation failed for '%s': %s", title, exc)

            if not job.get("cover_letter_points"):
                try:
                    job["cover_letter_points"] = self._ai.generate_cover_letter_points(
                        title,
                        company,
                        description,
                    )
                except Exception as exc:
                    log.warning("AI cover-letter point generation failed for '%s': %s", title, exc)

    # ── Sheets helpers ───────────────────────────────────────────────────
    def _append_to_sheet(self, job: dict) -> bool:
        if self._sheets is None:
            return False

        job_data = {
            "timestamp":        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "job_title":        job.get("job_title", ""),
            "company":          job.get("employer_name", ""),
            "location":         job.get("location", job.get("job_location", "")),
            "job_type":         job.get("job_type", job.get("job_employment_type", "")),
            "posted_date":      (job.get("posted_at", "") or "")[:10],
            "apply_link":       job.get("apply_link", ""),
            "description":      (job.get("description", "") or "")[:500],
            "matched_keywords": job.get("matched_as", ""),
            "status":           "New",
            "notes":            "",
            "ai_score":         job.get("ai_score", ""),
        }

        for attempt in range(_SHEETS_MAX_RETRIES + 1):
            success = self._sheets.append_job_row(job_data)
            if success:
                return True
            if attempt < _SHEETS_MAX_RETRIES:
                log.warning("Sheet append failed. Retrying in %ds…",
                            _SHEETS_RETRY_DELAY)
                time.sleep(_SHEETS_RETRY_DELAY)

        return False

    # ── failed_alerts.json fallback ──────────────────────────────────────
    @staticmethod
    def _save_failed_alerts(jobs: list[dict]) -> None:
        """Last-resort: dump jobs to a local JSON file so nothing is lost."""
        try:
            existing: list = []
            if os.path.isfile(FAILED_ALERTS_FILE):
                with open(FAILED_ALERTS_FILE, "r", encoding="utf-8") as f:
                    existing = json.load(f)
                if not isinstance(existing, list):
                    existing = []

            entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "jobs": jobs,
            }
            existing.append(entry)

            with open(FAILED_ALERTS_FILE, "w", encoding="utf-8") as f:
                json.dump(existing, f, indent=2, ensure_ascii=False)
            log.warning(
                "⚠️  Saved %d jobs to %s as last-resort fallback.",
                len(jobs), FAILED_ALERTS_FILE,
            )
        except Exception as exc:
            log.error("Failed to write failed_alerts.json: %s", exc)

    # ── Public API ───────────────────────────────────────────────────────
    def notify_new_jobs(
        self,
        jobs: list[dict[str, Any]],
        search_query: str = "",
    ) -> dict[str, bool]:
        """
        Send notifications for a batch of new jobs across all channels.
        Priority:
          1. Telegram (inline buttons for Apply)
          2. Google Sheets (persistent record)
          3. Email (fallback if both Telegram + Sheets fail)
          4. failed_alerts.json (last resort if ALL fail)
        """
        result = {"telegram": False, "sheet": False, "email": False}

        if not jobs:
            log.info("No jobs to notify — skipping all channels.")
            return result

        # Enrich only after jobs are confirmed as new and before channel dispatch.
        self._enrich_jobs_with_ai(jobs)

        # ── 1. Telegram with inline buttons ──────────────────────────────
        if self._tg:
            result["telegram"] = self._tg.send_job_alert(jobs)
        else:
            log.info("Telegram not configured — skipping.")

        # ── 2. Google Sheets ─────────────────────────────────────────────
        if self._sheets is not None:
            sheet_ok = 0
            for j in jobs:
                if self._append_to_sheet(j):
                    sheet_ok += 1
            result["sheet"] = sheet_ok > 0
            if sheet_ok < len(jobs):
                log.warning("Sheets: %d/%d rows appended.",
                            sheet_ok, len(jobs))
        else:
            log.info("Google Sheets not configured — skipping.")

        # ── 3. Email (fallback) ──────────────────────────────────────────
        if not result["telegram"] and not result["sheet"]:
            log.warning(
                "Both Telegram and Sheets failed — attempting email fallback.")
            if self._email is not None and self._email.is_configured:
                result["email"] = self._email.send_job_alert_email(
                    jobs, search_query, self._sheet_link
                )
            else:
                log.warning(
                    "Email not configured — no email fallback available.")

        # ── 4. Last resort: local file ───────────────────────────────────
        if not any(result.values()):
            log.error(
                "ALL notification channels failed! Saving to failed_alerts.json.")
            self._save_failed_alerts(jobs)

        log.info("Notification results: %s", result)
        return result

    def send_url_change_alert(self, changed_urls: list[str]) -> bool:
        if not changed_urls:
            return True
        url_list = "\n".join(f"• {u}" for u in changed_urls)
        text = (
            "🚨 <b>Job Page Update Detected!</b>\n\n"
            "The following pages have changed:\n"
            f"{url_list}\n\n"
            "Please check immediately."
        )
        if self._tg:
            return self._tg.send_message(text)
        return False

    def update_job_in_sheet(
        self, apply_link: str, status: str, notes: str = "",
    ) -> bool:
        if self._sheets is None:
            log.warning("Sheets not configured — cannot update job status.")
            return False

        for attempt in range(_SHEETS_MAX_RETRIES + 1):
            success = self._sheets.update_job_status(apply_link, status, notes)
            if success:
                return True
            if attempt < _SHEETS_MAX_RETRIES:
                log.warning("Sheet update failed. Retrying in %ds…",
                            _SHEETS_RETRY_DELAY)
                time.sleep(_SHEETS_RETRY_DELAY)

        return False

    def health_check(self) -> dict[str, bool | str]:
        """Test connectivity for all configured channels."""
        status: dict[str, bool | str] = {}

        # Telegram
        if self._tg and self._tg.bot_token and self._tg.chat_id:
            status["telegram"] = True
        else:
            status["telegram"] = "not configured"

        # Google Sheets
        if self._sheets is not None:
            try:
                status["sheet"] = self._sheets.health_check()
            except Exception as exc:
                log.error("Sheets health check error: %s", exc)
                status["sheet"] = False
        else:
            status["sheet"] = "not configured"

        # Email
        if self._email is not None and self._email.is_configured:
            try:
                status["email"] = self._email.health_check()
            except Exception as exc:
                log.error("Email health check error: %s", exc)
                status["email"] = False
        else:
            status["email"] = "not configured"

        log.info("Health check: %s", status)

        # If any channel failed, try to send a warning email
        has_failures = any(v is False for v in status.values())
        if has_failures and self._email is not None and self._email.is_configured:
            try:
                self._email.send_health_warning(status)
            except Exception as exc:
                log.warning("Could not send health warning email: %s", exc)

        return status
