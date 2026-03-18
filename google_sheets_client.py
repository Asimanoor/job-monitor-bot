"""
Google Sheets Client
────────────────────
Manages job records in a Google Sheet using gspread + google-auth.

Sheet column layout (A–L):
  A: Timestamp
  B: Job Title
  C: Company
  D: Location
  E: Job Type
  F: Posted Date
  G: Apply Link        ← used as unique key for lookups
  H: Description
  I: Matched Keywords
  J: Status            ← 'New', 'Applied', 'Rejected', 'Interviewing'
  K: Notes
    L: AI_Score

Conditional formatting hint (set up manually in Google Sheets):
  If Status == 'Applied'  → row background = light green  (#d9ead3)
  If Status == 'Rejected' → row background = light red    (#f4cccc)
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

import gspread
from google.oauth2.service_account import Credentials

log = logging.getLogger(__name__)

# Google Sheets API scopes
_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Retry config for quota errors
_QUOTA_RETRY_DELAY = 30   # seconds
_QUOTA_MAX_RETRIES = 1

# Auto-archive: jobs older than this many days (with non-'New' status) are archived
AUTO_ARCHIVE_DAYS = 30


class GoogleSheetsClient:
    """Read/write job records to a Google Sheet."""

    def __init__(
        self,
        credentials_json: str,
        sheet_id: str,
        service_account_email: str = "",
    ) -> None:
        """
        Authenticate and open the target spreadsheet.

        Args:
            credentials_json: Full service-account JSON key as a string.
            sheet_id:         The spreadsheet ID (from its URL).
            service_account_email: Optional — logged for debugging; the
                                   email is already embedded in the JSON key.

        Raises:
            ValueError:  If credentials_json is empty or not valid JSON.
            gspread.exceptions.SpreadsheetNotFound: If sheet_id is wrong or
                the service account hasn't been granted access.
        """
        if not credentials_json or not credentials_json.strip():
            raise ValueError("credentials_json is empty")

        try:
            creds_dict = json.loads(credentials_json)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"credentials_json is not valid JSON: {exc}") from exc

        if not isinstance(creds_dict, dict):
            raise ValueError("credentials_json must be a JSON object")

        creds = Credentials.from_service_account_info(
            creds_dict, scopes=_SCOPES)
        self._gc = gspread.authorize(creds)

        try:
            self._spreadsheet = self._gc.open_by_key(sheet_id)
        except gspread.exceptions.SpreadsheetNotFound:
            log.error(
                "Spreadsheet '%s' not found. "
                "Make sure the service account (%s) has Editor access.",
                sheet_id, service_account_email or creds_dict.get(
                    "client_email", "?"),
            )
            raise
        except gspread.exceptions.APIError as exc:
            log.error("Sheets API error during init: %s", exc)
            raise

        self._ws = self._spreadsheet.sheet1
        log.info(
            "GoogleSheetsClient ready — sheet '%s' (%s), worksheet '%s'",
            self._spreadsheet.title, sheet_id, self._ws.title,
        )

    # ── helpers ──────────────────────────────────────────────────────────
    def _retry_on_quota(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        """Execute *func* and retry once on 429 quota errors."""
        for attempt in range(_QUOTA_MAX_RETRIES + 1):
            try:
                return func(*args, **kwargs)
            except gspread.exceptions.APIError as exc:
                status = getattr(exc, "response", None)
                code = status.status_code if status is not None else None
                if code == 429 and attempt < _QUOTA_MAX_RETRIES:
                    log.warning(
                        "Sheets API quota hit (429). Sleeping %ds before retry…",
                        _QUOTA_RETRY_DELAY,
                    )
                    time.sleep(_QUOTA_RETRY_DELAY)
                    continue
                raise

    def _ensure_header_row(self) -> None:
        """Create the header row if the sheet is empty."""
        try:
            first = self._ws.row_values(1)
        except Exception:
            first = []

        headers = [
            "Timestamp", "Job Title", "Company", "Location",
            "Job Type", "Posted Date", "Apply Link", "Description",
            "Matched Keywords", "Status", "Notes", "AI_Score",
        ]

        if not first or first[0] != "Timestamp":
            self._retry_on_quota(self._ws.update, "A1:L1", [headers])
            log.info("Created header row in worksheet.")
            return

        # Upgrade legacy header that does not include AI_Score.
        if len(first) < len(headers) or first[:11] == headers[:11]:
            if len(first) < 12 or first[11] != "AI_Score":
                merged = (first + headers)[0:12]
                merged[11] = "AI_Score"
                self._retry_on_quota(self._ws.update, "A1:L1", [merged])
                log.info("Updated header row to include AI_Score column.")

    # ── public API ───────────────────────────────────────────────────────
    def append_job_row(self, job_data: dict) -> bool:
        """
        Append a new job row to the sheet.

        Expected job_data keys (all optional — missing ones become ''):
            timestamp, job_title, company, location, job_type,
            posted_date, apply_link, description, matched_keywords,
            status, notes

        Returns:
            True on success, False on failure.
        """
        try:
            self._ensure_header_row()

            ts = job_data.get("timestamp") or datetime.now(timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            )
            row = [
                ts,
                job_data.get("job_title", ""),
                job_data.get("company", ""),
                job_data.get("location", ""),
                job_data.get("job_type", ""),
                job_data.get("posted_date", ""),
                job_data.get("apply_link", ""),
                (job_data.get("description") or "")[:500],
                job_data.get("matched_keywords", ""),
                job_data.get("status", "New"),
                job_data.get("notes", ""),
                job_data.get("ai_score", ""),
            ]

            self._retry_on_quota(self._ws.append_row, row,
                                 value_input_option="USER_ENTERED")
            log.info("Appended row for '%s' at '%s'", job_data.get(
                "job_title"), job_data.get("company"))
            return True

        except gspread.exceptions.APIError as exc:
            log.error("Sheets API error appending row: %s", exc)
        except Exception as exc:
            log.error("Unexpected error appending row: %s", exc)
        return False

    def update_job_status(
        self,
        job_apply_link: str,
        new_status: str,
        notes: str = "",
    ) -> bool:
        """
        Find a row by apply_link (column G) and update its Status + Notes.

        Args:
            job_apply_link: The URL to search for in column G.
            new_status:     New value for column J (e.g. 'Applied', 'Rejected').
            notes:          Optional text for column K.

        Returns:
            True if the row was found and updated, False otherwise.
        """
        if not job_apply_link:
            log.warning("update_job_status called with empty apply_link.")
            return False

        try:
            # Find the cell in column G (Apply Link)
            cell = self._retry_on_quota(
                self._ws.find, job_apply_link, in_column=7
            )
            if cell is None:
                log.warning("No row found with apply_link: %s", job_apply_link)
                return False

            row_num = cell.row
            # Column J = 10 (Status), Column K = 11 (Notes)
            updates: list[dict] = [
                {"range": f"J{row_num}", "values": [[new_status]]},
            ]
            if notes:
                updates.append({"range": f"K{row_num}", "values": [[notes]]})

            self._retry_on_quota(self._ws.batch_update,
                                 updates, value_input_option="USER_ENTERED")
            log.info("Updated row %d → Status='%s', Notes='%s'",
                     row_num, new_status, notes)
            return True

        except gspread.exceptions.CellNotFound:
            log.warning("No row found with apply_link: %s", job_apply_link)
        except gspread.exceptions.APIError as exc:
            log.error("Sheets API error updating status: %s", exc)
        except Exception as exc:
            log.error("Unexpected error updating status: %s", exc)
        return False

    def get_pending_jobs(self) -> list[dict]:
        """
        Return all rows where Status (column J) == 'New' as a list of dicts.

        Returns:
            List of dicts with keys matching the header row.
            Empty list on error or if no pending jobs.
        """
        try:
            records = self._retry_on_quota(self._ws.get_all_records)
            if not isinstance(records, list):
                return []
            pending = [r for r in records if str(
                r.get("Status", "")).strip().lower() == "new"]
            log.info("Found %d pending ('New') jobs out of %d total.",
                     len(pending), len(records))
            return pending

        except gspread.exceptions.APIError as exc:
            log.error("Sheets API error getting pending jobs: %s", exc)
        except Exception as exc:
            log.error("Unexpected error getting pending jobs: %s", exc)
        return []

    def health_check(self) -> bool:
        """Quick check: can we read the sheet title? Returns True/False."""
        try:
            _ = self._retry_on_quota(self._ws.title)
            return True
        except Exception as exc:
            log.error("Sheets health check failed: %s", exc)
            return False

    def row_count(self) -> int:
        """Return the number of data rows (excluding header)."""
        try:
            return max(0, self._ws.row_count - 1)
        except Exception:
            return -1

    # ── archive ──────────────────────────────────────────────────────────
    def archive_old_jobs(
        self,
        state: dict,
        archive_days: int = AUTO_ARCHIVE_DAYS,
        delete_instead: bool = False,
    ) -> int:
        """
        Move (or delete) old rows where Status != 'New' and
        Posted Date > archive_days ago to a separate 'Archive' sheet.

        Runs at most once per 7 days (tracked via state['last_archive_date']).

        Args:
            state:           dict with 'last_archive_date' key.
            archive_days:    Number of days before a job is archivable.
            delete_instead:  If True, delete rows instead of moving to Archive.

        Returns:
            Number of rows archived/deleted.
        """
        from datetime import timedelta

        # Rate-limit: once per week
        last_archive = state.get("last_archive_date", "")
        if last_archive:
            try:
                last_dt = datetime.fromisoformat(last_archive)
                if (datetime.now(timezone.utc) - last_dt) < timedelta(days=7):
                    log.info("Archive skipped — last run was %s.", last_archive)
                    return 0
            except Exception:
                pass

        try:
            records = self._retry_on_quota(self._ws.get_all_values)
            if not records or len(records) < 2:
                return 0

            header = records[0]
            cutoff = datetime.now(timezone.utc).date() - \
                timedelta(days=archive_days)
            rows_to_archive = []

            # Identify rows to archive (iterate in reverse for safe deletion)
            for i in range(len(records) - 1, 0, -1):
                row = records[i]
                status = (row[9] if len(row) > 9 else "").strip()
                posted = (row[5] if len(row) > 5 else "").strip()

                if status.lower() == "new":
                    continue  # never archive "New"

                if not posted:
                    continue

                try:
                    posted_date = datetime.fromisoformat(posted[:10]).date()
                except Exception:
                    continue

                if posted_date < cutoff:
                    # 1-indexed row number
                    rows_to_archive.append((i + 1, row))

            if not rows_to_archive:
                log.info("No rows eligible for archiving.")
                state["last_archive_date"] = datetime.now(
                    timezone.utc).isoformat()
                return 0

            if not delete_instead:
                # Create or get Archive worksheet
                try:
                    archive_ws = self._spreadsheet.worksheet("Archive")
                except gspread.exceptions.WorksheetNotFound:
                    archive_ws = self._spreadsheet.add_worksheet(
                        title="Archive", rows=100, cols=12
                    )
                    self._retry_on_quota(archive_ws.append_row, header,
                                         value_input_option="USER_ENTERED")

                # Append rows to Archive
                for _row_num, row_data in rows_to_archive:
                    self._retry_on_quota(archive_ws.append_row, row_data,
                                         value_input_option="USER_ENTERED")

            # Delete rows from main sheet (reverse order to preserve indices)
            for row_num, _ in rows_to_archive:
                try:
                    self._retry_on_quota(self._ws.delete_rows, row_num)
                except Exception as exc:
                    log.warning("Failed to delete row %d: %s", row_num, exc)

            state["last_archive_date"] = datetime.now(timezone.utc).isoformat()
            action = "deleted" if delete_instead else "archived"
            log.info("Archived %d jobs (%s).", len(rows_to_archive), action)
            return len(rows_to_archive)

        except Exception as exc:
            log.error("Archive failed: %s", exc)
            return 0

    # ── filter views ─────────────────────────────────────────────────────
    def create_filter_views(self) -> dict[str, str]:
        """
        Create predefined filter views in the sheet.

        Returns:
            Dict of view_name → direct URL to that filter view.
            Returns empty dict on error.
        """
        try:
            sheet_id = self._ws.id
            spreadsheet_id = self._spreadsheet.id
            base_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"

            views: dict[str, str] = {}

            # Get all records to build view links
            records = self._retry_on_quota(self._ws.get_all_records)
            total_rows = len(records) + 1  # +1 for header

            # Since gspread doesn't support filter views via API directly,
            # we return pre-built filter URLs using Google Sheets URL params
            # Format: #gid=SHEET_ID&fvid=FILTER_VIEW_ID (manual setup needed)
            # Instead, provide useful filtered-by-formula approach links

            # Build convenience links with range filters
            views["Pending Applications"] = (
                f"{base_url}/edit#gid={sheet_id}"
                f"&range=A1:L{total_rows}"
            )
            views["All Jobs"] = f"{base_url}/edit#gid={sheet_id}"

            log.info("Filter view links generated: %s", list(views.keys()))
            return views

        except Exception as exc:
            log.error("Failed to generate filter view links: %s", exc)
            return {}

    def get_weekly_stats(self) -> dict:
        """
        Gather stats for the weekly report:
        total_found, applied, interviewing, top_companies, top_keywords.
        Only considers rows from the last 7 days.
        """
        from collections import Counter
        from datetime import timedelta

        stats = {
            "total_found": 0,
            "total_applied": 0,
            "total_interviews": 0,
            "top_companies": [],
            "top_keywords": [],
        }

        try:
            records = self._retry_on_quota(self._ws.get_all_records)
            if not records:
                return stats

            cutoff = datetime.now(timezone.utc).date() - timedelta(days=7)
            companies: Counter = Counter()
            keywords: Counter = Counter()

            for r in records:
                ts_raw = str(r.get("Timestamp", ""))[:10]
                try:
                    ts_date = datetime.fromisoformat(ts_raw).date()
                except Exception:
                    continue

                if ts_date < cutoff:
                    continue

                stats["total_found"] += 1
                status = str(r.get("Status", "")).strip().lower()
                if status == "applied":
                    stats["total_applied"] += 1
                elif status == "interviewing":
                    stats["total_interviews"] += 1

                company = str(r.get("Company", "")).strip()
                if company:
                    companies[company] += 1

                kw = str(r.get("Matched Keywords", "")).strip()
                if kw:
                    for k in kw.split(","):
                        k = k.strip()
                        if k:
                            keywords[k] += 1

            stats["top_companies"] = [c for c, _ in companies.most_common(10)]
            stats["top_keywords"] = [k for k, _ in keywords.most_common(10)]
            return stats

        except Exception as exc:
            log.error("Failed to get weekly stats: %s", exc)
            return stats
