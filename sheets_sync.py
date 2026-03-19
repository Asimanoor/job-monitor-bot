"""
Sheets Sync Helper
───────────────────
Small wrapper around `google_sheets_client.GoogleSheetsClient` to:
  - Map internal job dicts to the sheet row schema
  - Deduplicate via StateManager (optional)
  - Rely on GoogleSheetsClient's built-in 429 retry

Note: The main orchestrator currently uses `NotificationManager.notify_new_jobs()`.
This module exists as a reusable, testable building block per project deliverables.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable

log = logging.getLogger(__name__)


def _now_utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def map_job_to_sheet_row(job: dict[str, Any]) -> dict[str, Any]:
    """
    Map monitor/job dict format to GoogleSheetsClient.append_job_row() expected keys.
    """
    return {
        "timestamp": job.get("timestamp") or _now_utc_stamp(),
        "job_title": job.get("job_title", ""),
        "company": job.get("company") or job.get("employer_name") or "",
        "location": job.get("location", "") or job.get("job_location", ""),
        "job_type": job.get("job_type", "") or job.get("job_employment_type", ""),
        "posted_date": (job.get("posted_date") or job.get("posted_at") or "")[:10],
        "apply_link": job.get("apply_link", ""),
        "description": job.get("description", "")[:500],
        "matched_keywords": job.get("matched_as", job.get("matched_keywords", "")) or "",
        "status": job.get("status", "New"),
        "notes": job.get("notes", ""),
        "ai_score": job.get("ai_score", ""),
    }


def append_new_jobs_to_sheets(
    *,
    sheets_client: Any,
    jobs: list[dict[str, Any]],
    state_mgr: Any | None = None,
    job_id_getter: Callable[[dict[str, Any]], str] | None = None,
) -> int:
    """
    Append job rows to Google Sheets.

    Args:
      sheets_client: instance with `append_job_row(job_data: dict) -> bool`
      jobs: list of job dicts
      state_mgr: optional StateManager-like object with `is_new_job(job_id)` and `mark_as_notified(job_id)`
      job_id_getter: function returning stable job_id used for StateManager dedupe

    Returns:
      Number of successful appends.
    """
    if sheets_client is None:
        return 0

    appended = 0

    for job in jobs:
        job_id = job_id_getter(job) if job_id_getter is not None else job.get("job_id", "")
        if state_mgr is not None and job_id and not state_mgr.is_new_job(job_id):
            continue

        job_data = map_job_to_sheet_row(job)
        ok = sheets_client.append_job_row(job_data)
        if ok:
            appended += 1
            if state_mgr is not None and job_id:
                state_mgr.mark_as_notified(job_id)
        else:
            log.warning("Sheets append failed for '%s' (%s)", job_data.get("job_title"), job_data.get("apply_link"))

    return appended


__all__ = [
    "map_job_to_sheet_row",
    "append_new_jobs_to_sheets",
]

