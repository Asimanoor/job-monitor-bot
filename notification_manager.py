"""
Notification Manager
────────────────────
Orchestrates all notification channels:
  1. Google Sheets  (persistent record, always attempted)
  2. Email  (fallback if Sheets fails)
  3. failed_alerts.json  (last resort if ALL channels fail)
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Protocol
from urllib.parse import urlparse

from job_scraper import is_valid_job_posting
from role_filter import matches_target_role

log = logging.getLogger(__name__)

# Retry config for Sheets 429
_SHEETS_RETRY_DELAY = 30
_SHEETS_MAX_RETRIES = 1

FAILED_ALERTS_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "failed_alerts.json"
)


class SheetsClient(Protocol):
    def append_job_row(self, job_data: dict) -> bool: ...
    def append_url_change_row(self, change_data: dict) -> bool: ...
    def append_career_opening_row(self, opening_data: dict) -> bool: ...
    def append_search_activity_row(self, activity_data: dict) -> bool: ...
    def append_url_change_rows(self, change_rows: list[dict]) -> int: ...
    def append_career_opening_rows(self, opening_rows: list[dict]) -> int: ...
    def append_search_activity_rows(self, activity_rows: list[dict]) -> int: ...
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
        sheets_client: SheetsClient | None = None,
        email_notifier: Emailer | None = None,
        sheet_link: str = "",
        ai_client: Any | None = None,
        url_change_alert_max_events: int = 20,
        url_change_max_events_per_cycle: int = 200,
        url_change_max_openings_per_event: int = 300,
        url_change_max_openings_per_cycle: int = 5000,
        url_change_log_baseline_openings: bool = True,
    ) -> None:
        self._sheets = sheets_client
        self._email = email_notifier
        self._sheet_link = sheet_link
        self._ai = ai_client
        self._url_change_alert_max_events = max(1, int(url_change_alert_max_events))
        self._url_change_max_events_per_cycle = max(1, int(url_change_max_events_per_cycle))
        self._url_change_max_openings_per_event = max(1, int(url_change_max_openings_per_event))
        self._url_change_max_openings_per_cycle = max(1, int(url_change_max_openings_per_cycle))
        self._url_change_log_baseline_openings = bool(url_change_log_baseline_openings)

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

        source_tag = str(job.get("source") or "JSEARCH_API").strip() or "JSEARCH_API"
        notes_raw = str(job.get("notes") or "").strip()
        source_note = f"Source: {source_tag}"
        notes = notes_raw
        if source_note.lower() not in notes_raw.lower():
            notes = f"{notes_raw} | {source_note}".strip(" |")

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
            "notes":            notes,
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

    @staticmethod
    def _normalize_url_change_events(changed_urls: list[Any]) -> list[dict[str, Any]]:
        """Normalize incoming URL change payloads (legacy list[str] or structured list[dict])."""
        events: list[dict[str, Any]] = []

        for item in changed_urls:
            if isinstance(item, str):
                url = item.strip()
                if not url:
                    continue
                events.append(
                    {
                        "url": url,
                        "domain": urlparse(url).netloc,
                        "change_type": "content_changed",
                        "page_title": "",
                        "openings": [],
                        "new_openings": [],
                        "total_openings": 0,
                        "new_openings_count": 0,
                    }
                )
                continue

            if isinstance(item, dict):
                url = str(item.get("url", "")).strip()
                if not url:
                    continue

                openings = item.get("openings") if isinstance(item.get("openings"), list) else []
                new_openings = item.get("new_openings") if isinstance(item.get("new_openings"), list) else []

                events.append(
                    {
                        "url": url,
                        "domain": str(item.get("domain") or urlparse(url).netloc),
                        "change_type": str(item.get("change_type") or "content_changed"),
                        "page_title": str(item.get("page_title") or ""),
                        "scraper_used": str(item.get("scraper_used") or "unknown"),
                        "openings": openings,
                        "new_openings": new_openings,
                        "total_openings": int(item.get("total_openings") or len(openings)),
                        "new_openings_count": int(item.get("new_openings_count") or len(new_openings)),
                    }
                )

        return events

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
          1. Google Sheets (persistent record)
          2. Email (fallback if Sheets fails)
          3. failed_alerts.json (last resort if ALL fail)
        """
        result = {"sheet": False, "email": False}

        if not jobs:
            log.info("No jobs to notify — skipping all channels.")
            return result

        # Enrich only after jobs are confirmed as new and before channel dispatch.
        self._enrich_jobs_with_ai(jobs)

        # ── 1. Google Sheets ─────────────────────────────────────────────
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
        if not result["sheet"]:
            log.warning(
                "Google Sheets failed — attempting email fallback.")
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

    def send_url_change_alert(self, changed_urls: list[Any]) -> bool:
        events = self._normalize_url_change_events(changed_urls)
        if not events:
            return True

        lines: list[str] = []
        sample_positions: list[str] = []

        for event in events[: self._url_change_alert_max_events]:
            url = str(event.get("url", ""))
            change_type = str(event.get("change_type", "content_changed"))
            total = int(event.get("total_openings", 0) or 0)
            new_count = int(event.get("new_openings_count", 0) or 0)

            if new_count > 0:
                lines.append(f"• {url} (new openings: {new_count}, detected: {total})")
            else:
                lines.append(f"• {url} ({change_type})")

            new_openings = event.get("new_openings") if isinstance(event.get("new_openings"), list) else []
            for opening in new_openings[:2]:
                if isinstance(opening, dict):
                    title = str(opening.get("title", "")).strip()
                    link = str(opening.get("link", "")).strip()
                    if title and link:
                        job_payload = {
                            "title": title,
                            "apply_link": link,
                            "job_url": link,
                            "source_url": url,
                        }
                        if not is_valid_job_posting(job_payload):
                            continue
                        matched, _role, _score = matches_target_role(title, description="")
                        if not matched:
                            continue
                        sample_positions.append(f"• {title} — {link}")

        # URL changes are logged to Google Sheets only (no Telegram alerts)
        # Proceed to record_url_changes_in_sheet()
        return True

    def record_url_changes_in_sheet(self, changed_urls: list[Any]) -> bool:
        """Persist validated job openings extracted from monitored career URLs."""
        events = self._normalize_url_change_events(changed_urls)
        if not events or self._sheets is None:
            return False

        if len(events) > self._url_change_max_events_per_cycle:
            log.warning(
                "URL change events exceed cap (%d > %d). Truncating this cycle for quota safety.",
                len(events),
                self._url_change_max_events_per_cycle,
            )
            events = events[: self._url_change_max_events_per_cycle]

        change_payloads: list[dict[str, Any]] = []
        opening_payloads: list[dict[str, Any]] = []
        global_seen_tokens: set[str] = set()
        remaining_opening_budget = self._url_change_max_openings_per_cycle

        for event in events:
            now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            url = str(event.get("url", ""))
            domain = str(event.get("domain") or urlparse(url).netloc or "Unknown")
            page_title = str(event.get("page_title", ""))
            change_type = str(event.get("change_type", "content_changed"))
            scraper_used = str(event.get("scraper_used") or "unknown")

            new_openings = event.get("new_openings") if isinstance(event.get("new_openings"), list) else []
            all_openings = event.get("openings") if isinstance(event.get("openings"), list) else []
            total_openings = int(event.get("total_openings", len(all_openings)) or 0)
            new_openings_count = int(event.get("new_openings_count", len(new_openings)) or 0)

            change_payloads.append(
                {
                    "timestamp": now,
                    "url": url,
                    "domain": domain,
                    "change_type": change_type,
                    "page_title": page_title,
                    "total_openings": total_openings,
                    "new_openings_count": new_openings_count,
                    "new_opening_titles_preview": [
                        str(opening.get("title", "")).strip()
                        for opening in new_openings[:5]
                        if isinstance(opening, dict)
                    ],
                    "notes": f"Job-only monitor (source=links.txt, scraper={scraper_used}, cap={self._url_change_max_openings_per_event}/event)",
                }
            )

            openings_to_log = new_openings if new_openings else (
                all_openings if (change_type == "new_url_tracked" and self._url_change_log_baseline_openings) else []
            )
            openings_to_log = openings_to_log[: self._url_change_max_openings_per_event]

            if remaining_opening_budget <= 0:
                openings_to_log = []
            elif len(openings_to_log) > remaining_opening_budget:
                openings_to_log = openings_to_log[:remaining_opening_budget]

            remaining_opening_budget -= len(openings_to_log)

            seen_tokens: set[str] = set()
            for opening in openings_to_log:
                if not isinstance(opening, dict):
                    continue

                opening_title = str(opening.get("title", "")).strip()
                opening_link = str(opening.get("link", "")).strip()
                job_payload = {
                    "title": opening_title,
                    "company": domain,
                    "location": str(opening.get("location", "")).strip(),
                    "type": str(opening.get("type", "")).strip(),
                    "apply_link": opening_link,
                    "posted_date": str(opening.get("posted_date", "")).strip(),
                    "source_url": url,
                    "job_url": opening_link,
                }

                if not is_valid_job_posting(job_payload):
                    continue

                # Apply role filtering — only log openings matching target roles
                matched, _, _ = matches_target_role(opening_title)
                if not matched:
                    continue

                token = f"{job_payload['title'].lower()}|{job_payload['apply_link'].lower()}"
                if token in seen_tokens or token in global_seen_tokens:
                    continue
                seen_tokens.add(token)
                global_seen_tokens.add(token)

                opening_payloads.append(
                    {
                        "timestamp": now,
                        "job_title": job_payload["title"],
                        "company": job_payload["company"],
                        "location": job_payload["location"],
                        "type": job_payload["type"],
                        "apply_link": job_payload["apply_link"],
                        "posted_date": job_payload["posted_date"],
                        "source_url": job_payload["source_url"],
                        "status": "New" if opening in new_openings else "Tracked",
                    }
                )

        appended_changes = 0
        append_change_rows_fn = getattr(self._sheets, "append_url_change_rows", None)
        if callable(append_change_rows_fn):
            try:
                appended_changes = int(append_change_rows_fn(change_payloads))
            except Exception as exc:
                log.warning("Batch URL change append failed, falling back to single rows: %s", exc)

        if appended_changes == 0 and change_payloads:
            append_url_change_fn = getattr(self._sheets, "append_url_change_row", None)
            if callable(append_url_change_fn):
                for payload in change_payloads:
                    if bool(append_url_change_fn(payload)):
                        appended_changes += 1

        appended_openings = 0
        append_opening_rows_fn = getattr(self._sheets, "append_career_opening_rows", None)
        if callable(append_opening_rows_fn):
            try:
                appended_openings = int(append_opening_rows_fn(opening_payloads))
            except Exception as exc:
                log.warning("Batch opening append failed, falling back to single rows: %s", exc)

        if appended_openings == 0 and opening_payloads:
            append_opening_fn = getattr(self._sheets, "append_career_opening_row", None)
            if callable(append_opening_fn) and len(opening_payloads) <= 10:
                for payload in opening_payloads:
                    if bool(append_opening_fn(payload)):
                        appended_openings += 1
            elif callable(append_opening_fn):
                log.warning(
                    "Skipping single-row fallback for %d opening rows to avoid quota spikes.",
                    len(opening_payloads),
                )

        if appended_changes < len(change_payloads):
            log.warning("Sheets URL change logging partial success: %d/%d", appended_changes, len(change_payloads))

        log.info(
            "Sheets URL logging summary: change rows=%d/%d, validated opening rows=%d",
            appended_changes,
            len(change_payloads),
            appended_openings,
        )
        return appended_openings > 0 or appended_changes > 0

    def record_search_activity_in_sheet(self, activity_rows: list[dict[str, Any]]) -> bool:
        """Persist per-URL monitor activity (searched/changed/ignored/error) for full audit history."""
        if self._sheets is None or not activity_rows:
            return False

        payloads: list[dict[str, Any]] = []
        for row in activity_rows:
            if not isinstance(row, dict):
                continue

            url = str(row.get("url", "")).strip()
            if not url:
                continue

            payloads.append(
                {
                    "timestamp": str(row.get("timestamp") or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")),
                    "url": url,
                    "domain": str(row.get("domain") or urlparse(url).netloc or ""),
                    "status": str(row.get("status") or ""),
                    "change_type": str(row.get("change_type") or ""),
                    "total_openings": int(row.get("total_openings", 0) or 0),
                    "new_openings_count": int(row.get("new_openings_count", 0) or 0),
                    "scraper_used": str(row.get("scraper_used") or ""),
                    "pages_visited": row.get("pages_visited") or 0,
                    "error": str(row.get("error") or ""),
                    "notes": str(row.get("notes") or ""),
                }
            )

        if not payloads:
            return False

        appended = 0
        append_batch_fn = getattr(self._sheets, "append_search_activity_rows", None)
        if callable(append_batch_fn):
            try:
                appended = int(append_batch_fn(payloads))
            except Exception as exc:
                log.warning("Batch search activity append failed, falling back to single rows: %s", exc)

        if appended == 0:
            append_single_fn = getattr(self._sheets, "append_search_activity_row", None)
            if callable(append_single_fn):
                for payload in payloads:
                    if bool(append_single_fn(payload)):
                        appended += 1

        if appended < len(payloads):
            log.warning("Sheets search activity logging partial success: %d/%d", appended, len(payloads))
        else:
            log.info("Sheets search activity logging appended %d rows.", appended)

        return appended > 0

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

        # Telegram channel is not wired in this manager variant.
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
