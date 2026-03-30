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
import re
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import gspread
from classifier import safe_sheet_title_from_url
from dedup import build_job_hash, normalize_location, normalize_title, normalize_url
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
_APPEND_BATCH_SIZE = 50

# Auto-archive: jobs older than this many days (with non-'New' status) are archived
AUTO_ARCHIVE_DAYS = 30

URL_CHANGES_WORKSHEET = "URL Changes Log"
URL_CHANGES_HEADERS = [
    "Timestamp",
    "Career Page URL",
    "Domain",
    "Change Type",
    "Page Title",
    "Total Openings Detected",
    "New Openings Detected",
    "New Opening Titles (Preview)",
    "Notes",
]

CAREER_OPENINGS_WORKSHEET = "Career Openings Log"
CAREER_OPENINGS_HEADERS = [
    "Timestamp",
    "Job Title",
    "Company",
    "Location",
    "Type",
    "Apply Link",
    "Posted Date",
    "Source URL",
    "Status",
]

SEARCH_ACTIVITY_WORKSHEET = "Search Activity Log"
SEARCH_ACTIVITY_HEADERS = [
    "Timestamp",
    "Run ID",
    "Run Iteration",
    "Career Page URL",
    "Domain",
    "Status",
    "Change Type",
    "Total Openings Detected",
    "New Openings Detected",
    "Scraper Used",
    "Pages Visited",
    "Error",
    "Notes",
]

ASSOCIATE_ROLES_WORKSHEET = "Associate Roles"
ASSOCIATE_ROLES_HEADERS = [
    "Timestamp",
    "Run ID",
    "Run Iteration",
    "Job Title",
    "Company",
    "Location",
    "Type",
    "Apply Link",
    "Posted Date",
    "Source URL",
    "Matched Role",
    "Status",
    "Notes",
    "Hash ID",
    "First Seen",
    "Last Seen",
    "Closed Date",
    "Status Color",
]

ALL_OPENINGS_WORKSHEET = "All_Openings"
ALL_OPENINGS_HEADERS = [
    "Iteration",
    "Status",
    "Company",
    "Title",
    "Location",
    "Experience",
    "URL",
    "First Seen",
    "Last Seen",
    "Closed Date",
    "Run ID",
    "Timestamp",
]

NEW_OPENINGS_WORKSHEET = "New_Openings"
NEW_OPENINGS_HEADERS = [
    "Timestamp",
    "Run ID",
    "Run Iteration",
    "Company",
    "Role",
    "Location",
    "Apply URL",
    "Status",
    "Hash ID",
    "Status Color",
]

_STATUS_COLORS = {
    "NEW": "#d9ead3",
    "UPDATED": "#eeeeee",
    "EXISTING": "#ffffff",
    "ACTIVE": "#ffffff",
    "CLOSED": "#f4cccc",
}

_ITERATION_OLD_ACTIVE_COLOR = "#eeeeee"
_RUN_DIVIDER_COLOR = "#d9e8fb"
_RUN_SUMMARY_COLOR = "#fff2cc"

_TRACKING_QUERY_PREFIXES = (
    "utm_",
    "fbclid",
    "gclid",
    "mc_",
    "hs",
    "__hs",
)

_ASSOCIATE_KEYWORDS = (
    "associate",
    "junior",
    "graduate",
    "trainee",
    "entry",
    "entry-level",
    "intern",
    "fresher",
    "new grad",
)


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
        self._worksheet_cache: dict[str, Any] = {self._ws.title: self._ws}
        self._header_initialized: set[str] = set()
        self._primary_header_checked = False
        self._primary_dedupe_cache: set[str] | None = None
        self._career_opening_dedupe_cache: set[str] | None = None
        self._associate_dedupe_cache: set[str] | None = None
        self._worksheet_dedupe_caches: dict[str, set[str]] = {}
        self._status_formatting_applied: set[str] = set()
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
        """Ensure primary worksheet header exists without read-heavy checks."""
        if self._primary_header_checked:
            return

        headers = [
            "Timestamp", "Job Title", "Company", "Location",
            "Job Type", "Posted Date", "Apply Link", "Description",
            "Matched Keywords", "Status", "Notes", "AI_Score",
        ]

        self._retry_on_quota(self._ws.update, "A1:L1", [headers])

        self._primary_header_checked = True

    @staticmethod
    def _column_letter(index: int) -> str:
        """Convert 1-based column index to sheet letter (1->A, 27->AA)."""
        result = ""
        n = max(1, int(index))
        while n > 0:
            n, rem = divmod(n - 1, 26)
            result = chr(65 + rem) + result
        return result

    def _get_or_create_worksheet(self, title: str, headers: list[str], rows: int = 2000):
        """Fetch worksheet by title or create it, then enforce header row."""
        cached = self._worksheet_cache.get(title)
        if cached is not None:
            self._ensure_status_conditional_formatting(cached, headers)
            return cached

        created = False
        try:
            ws = self._spreadsheet.worksheet(title)
        except gspread.exceptions.WorksheetNotFound:
            ws = self._spreadsheet.add_worksheet(
                title=title,
                rows=max(rows, 100),
                cols=max(len(headers), 9),
            )
            created = True

        if title not in self._header_initialized:
            end_col = self._column_letter(len(headers))
            self._retry_on_quota(ws.update, f"A1:{end_col}1", [headers])
            self._header_initialized.add(title)

        self._worksheet_cache[title] = ws
        if created or title not in self._status_formatting_applied:
            self._ensure_status_conditional_formatting(ws, headers)

        return ws

    @staticmethod
    def _normalize_text(value: str) -> str:
        return " ".join(str(value or "").strip().lower().split())

    @staticmethod
    def _status_color(status: str) -> str:
        return _STATUS_COLORS.get(str(status or "").upper(), "#ffffff")

    @staticmethod
    def _hex_to_rgb_components(hex_color: str) -> tuple[float, float, float]:
        raw = str(hex_color or "").strip().lstrip("#")
        if len(raw) != 6:
            return (1.0, 1.0, 1.0)
        try:
            r = int(raw[0:2], 16) / 255.0
            g = int(raw[2:4], 16) / 255.0
            b = int(raw[4:6], 16) / 255.0
            return (r, g, b)
        except Exception:
            return (1.0, 1.0, 1.0)

    def _ensure_status_conditional_formatting(self, ws: Any, headers: list[str]) -> None:
        """Apply status-based row color rules once per worksheet.

        Non-fatal by design: if formatting fails, row writes continue.
        """
        title = str(getattr(ws, "title", "") or "").strip()
        if not title or title in self._status_formatting_applied:
            return

        status_col_idx = None
        iteration_col_idx = None
        for idx, header in enumerate(headers, start=1):
            header_norm = str(header or "").strip().lower()
            if header_norm == "status":
                status_col_idx = idx
            elif header_norm == "iteration":
                iteration_col_idx = idx
        if status_col_idx is None:
            self._status_formatting_applied.add(title)
            return

        status_col_letter = self._column_letter(status_col_idx)
        iteration_col_letter = self._column_letter(iteration_col_idx) if iteration_col_idx else ""
        total_columns = max(1, len(headers))

        try:
            metadata = self._retry_on_quota(self._spreadsheet.fetch_sheet_metadata)
            existing_formulas: set[str] = set()
            if isinstance(metadata, dict):
                for sheet_meta in metadata.get("sheets", []):
                    props = sheet_meta.get("properties") if isinstance(sheet_meta, dict) else {}
                    if not isinstance(props, dict) or int(props.get("sheetId", -1)) != int(ws.id):
                        continue
                    conditional_rules = sheet_meta.get("conditionalFormats") if isinstance(sheet_meta, dict) else []
                    if not isinstance(conditional_rules, list):
                        continue
                    for rule in conditional_rules:
                        if not isinstance(rule, dict):
                            continue
                        boolean_rule = rule.get("booleanRule") if isinstance(rule.get("booleanRule"), dict) else {}
                        condition = boolean_rule.get("condition") if isinstance(boolean_rule.get("condition"), dict) else {}
                        if str(condition.get("type") or "") != "CUSTOM_FORMULA":
                            continue
                        values = condition.get("values") if isinstance(condition.get("values"), list) else []
                        if not values:
                            continue
                        user_formula = ""
                        first = values[0]
                        if isinstance(first, dict):
                            user_formula = str(first.get("userEnteredValue") or "").strip()
                        if user_formula:
                            existing_formulas.add(user_formula)

            if iteration_col_idx is not None:
                formula_color_pairs = [
                    (f'=${status_col_letter}2="NEW"', _STATUS_COLORS["NEW"]),
                    (f'=${status_col_letter}2="CLOSED"', _STATUS_COLORS["CLOSED"]),
                    (
                        f'=AND(${status_col_letter}2="ACTIVE",${iteration_col_letter}2<MAX(${iteration_col_letter}:${iteration_col_letter}))',
                        _ITERATION_OLD_ACTIVE_COLOR,
                    ),
                ]
            else:
                formula_color_pairs = [
                    (f'=${status_col_letter}2="{status}"', hex_color)
                    for status, hex_color in _STATUS_COLORS.items()
                ]

            requests_payload: list[dict[str, Any]] = []
            insert_index = len(existing_formulas)
            for formula, hex_color in formula_color_pairs:
                if formula in existing_formulas:
                    continue
                r, g, b = self._hex_to_rgb_components(hex_color)
                requests_payload.append(
                    {
                        "addConditionalFormatRule": {
                            "rule": {
                                "ranges": [
                                    {
                                        "sheetId": ws.id,
                                        "startRowIndex": 1,
                                        "startColumnIndex": 0,
                                        "endColumnIndex": total_columns,
                                    }
                                ],
                                "booleanRule": {
                                    "condition": {
                                        "type": "CUSTOM_FORMULA",
                                        "values": [{"userEnteredValue": formula}],
                                    },
                                    "format": {
                                        "backgroundColor": {
                                            "red": r,
                                            "green": g,
                                            "blue": b,
                                        }
                                    },
                                },
                            },
                            "index": insert_index,
                        }
                    }
                )
                insert_index += 1

            if requests_payload:
                self._retry_on_quota(self._spreadsheet.batch_update, {"requests": requests_payload})
        except Exception as exc:
            log.warning("Could not apply status formatting for worksheet '%s': %s", title, exc)

        self._status_formatting_applied.add(title)

    @staticmethod
    def _normalize_url(value: str) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""

        parsed = urlparse(raw)
        if parsed.scheme not in {"http", "https"}:
            return raw.lower()

        filtered_query: list[tuple[str, str]] = []
        for key, item in parse_qsl(parsed.query, keep_blank_values=True):
            key_l = (key or "").lower()
            if key_l.startswith(_TRACKING_QUERY_PREFIXES):
                continue
            filtered_query.append((key, item))

        normalized_path = re.sub(r"/+", "/", parsed.path or "/")
        query = urlencode(filtered_query, doseq=True)
        return urlunparse(
            (
                parsed.scheme.lower(),
                (parsed.netloc or "").lower(),
                normalized_path,
                parsed.params,
                query,
                "",
            )
        )

    @classmethod
    def _build_primary_dedupe_key(cls, row_data: dict) -> str:
        apply_link = cls._normalize_url(row_data.get("apply_link", ""))
        if apply_link:
            return f"link|{apply_link}"

        title = cls._normalize_text(row_data.get("job_title", ""))
        company = cls._normalize_text(row_data.get("company", ""))
        if title or company:
            return f"title_company|{title}|{company}"
        return ""

    @classmethod
    def _build_opening_dedupe_key(cls, row_data: dict) -> str:
        apply_link = cls._normalize_url(
            row_data.get("apply_link") or row_data.get("position_link") or ""
        )
        title_raw = row_data.get("job_title") or row_data.get("position_title") or ""
        title = normalize_title(str(title_raw or ""))
        company = cls._normalize_text(row_data.get("company") or row_data.get("domain") or "")
        location = normalize_location(str(row_data.get("location") or ""), fallback_url=apply_link)

        fingerprint = f"fp|{company}|{title}|{location}"
        if apply_link:
            return f"title_link|{fingerprint}|{apply_link}"
        if title:
            return f"title_only|{fingerprint}"
        return ""

    @classmethod
    def _is_associate_role_title(cls, title: str) -> bool:
        title_norm = cls._normalize_text(title)
        return any(keyword in title_norm for keyword in _ASSOCIATE_KEYWORDS)

    def _hydrate_primary_dedupe_cache(self) -> set[str]:
        if self._primary_dedupe_cache is not None:
            return self._primary_dedupe_cache

        tokens: set[str] = set()
        try:
            all_rows = self._retry_on_quota(self._ws.get_all_records)
            if isinstance(all_rows, list):
                for record in all_rows:
                    if not isinstance(record, dict):
                        continue
                    token = self._build_primary_dedupe_key(
                        {
                            "apply_link": record.get("Apply Link", ""),
                            "job_title": record.get("Job Title", ""),
                            "company": record.get("Company", ""),
                        }
                    )
                    if token:
                        tokens.add(token)
        except Exception as exc:
            log.warning("Could not hydrate primary dedupe cache: %s", exc)

        self._primary_dedupe_cache = tokens
        return tokens

    def _hydrate_career_opening_dedupe_cache(self, ws: Any) -> set[str]:
        if self._career_opening_dedupe_cache is not None:
            return self._career_opening_dedupe_cache

        tokens: set[str] = set()
        try:
            all_rows = self._retry_on_quota(ws.get_all_records)
            if isinstance(all_rows, list):
                for record in all_rows:
                    if not isinstance(record, dict):
                        continue
                    token = self._build_opening_dedupe_key(
                        {
                            "job_title": record.get("Job Title", ""),
                            "apply_link": record.get("Apply Link", ""),
                        }
                    )
                    if token:
                        tokens.add(token)
        except Exception as exc:
            log.warning("Could not hydrate career-opening dedupe cache: %s", exc)

        self._career_opening_dedupe_cache = tokens
        return tokens

    def _hydrate_associate_dedupe_cache(self, ws: Any) -> set[str]:
        if self._associate_dedupe_cache is not None:
            return self._associate_dedupe_cache

        tokens: set[str] = set()
        try:
            all_rows = self._retry_on_quota(ws.get_all_records)
            if isinstance(all_rows, list):
                for record in all_rows:
                    if not isinstance(record, dict):
                        continue
                    token = self._opening_change_token(
                        {
                            "status": record.get("Status", ""),
                            "hash_id": record.get("Hash ID", ""),
                            "role": record.get("Job Title", ""),
                            "apply_url": record.get("Apply Link", ""),
                        }
                    )
                    if token:
                        tokens.add(token)
        except Exception as exc:
            log.warning("Could not hydrate associate-role dedupe cache: %s", exc)

        self._associate_dedupe_cache = tokens
        return tokens

    @classmethod
    def _opening_change_hash_id(cls, item: dict) -> str:
        return build_job_hash(
            str(item.get("company", "")),
            str(item.get("role") or item.get("job_title") or item.get("position_title") or ""),
            str(item.get("location", "Not Specified")),
            str(item.get("apply_url") or item.get("apply_link") or item.get("position_link") or ""),
        )

    @classmethod
    def _opening_change_token(cls, item: dict) -> str:
        status = cls._normalize_text(item.get("status") or "")
        role = cls._normalize_text(item.get("role") or item.get("title") or item.get("job_title") or item.get("position_title") or "")
        company = cls._normalize_text(item.get("company") or item.get("domain") or "")
        location = cls._normalize_text(item.get("location") or "not specified")
        link = normalize_url(str(item.get("apply_url") or item.get("url") or item.get("apply_link") or item.get("position_link") or ""))
        return f"{status}|{company}|{role}|{location}|{link}"

    def _hydrate_worksheet_dedupe_cache(self, ws: Any, cache_key: str, token_builder) -> set[str]:
        existing = self._worksheet_dedupe_caches.get(cache_key)
        if existing is not None:
            return existing

        tokens: set[str] = set()
        try:
            rows = self._retry_on_quota(ws.get_all_records)
            if isinstance(rows, list):
                for record in rows:
                    if not isinstance(record, dict):
                        continue
                    token = token_builder(record)
                    if token:
                        tokens.add(token)
        except Exception as exc:
            log.warning("Could not hydrate dedupe cache for %s: %s", cache_key, exc)

        self._worksheet_dedupe_caches[cache_key] = tokens
        return tokens

    @staticmethod
    def _company_opening_row(item: dict) -> list[Any]:
        ts = item.get("timestamp") or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        run_id = item.get("run_id") or ""
        run_iteration = int(item.get("run_iteration", 0) or 0)
        role = item.get("role") or item.get("title") or item.get("job_title") or item.get("position_title") or ""
        company = item.get("company") or item.get("domain") or ""
        location = item.get("location") or "Not Specified"
        experience = item.get("experience") or "Not Specified"
        apply_url = item.get("apply_url") or item.get("url") or item.get("apply_link") or item.get("position_link") or ""
        status = str(item.get("status") or "ACTIVE").upper()
        first_seen = item.get("first_seen") or ""
        last_seen = item.get("last_seen") or ""
        closed_at = item.get("closed_at") or ""

        return [
            run_iteration,
            status,
            company,
            role,
            location,
            experience,
            apply_url,
            first_seen,
            last_seen,
            closed_at,
            run_id,
            ts,
        ]

    @staticmethod
    def _new_opening_row(item: dict) -> list[Any]:
        ts = item.get("timestamp") or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        status = str(item.get("status") or "NEW").upper()
        hash_id = item.get("hash_id") or GoogleSheetsClient._opening_change_hash_id(item)
        status_color = item.get("status_color") or GoogleSheetsClient._status_color(status)
        return [
            ts,
            item.get("run_id") or "",
            int(item.get("run_iteration", 0) or 0),
            item.get("company") or item.get("domain") or "",
            item.get("role") or item.get("job_title") or item.get("position_title") or "",
            item.get("location") or "Not Specified",
            item.get("apply_url") or item.get("apply_link") or item.get("position_link") or "",
            status,
            hash_id,
            status_color,
        ]

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

            token = self._build_primary_dedupe_key(job_data)
            if token:
                existing = self._hydrate_primary_dedupe_cache()
                if token in existing:
                    log.info(
                        "Skipping duplicate primary-sheet row for '%s' (%s)",
                        job_data.get("job_title", ""),
                        job_data.get("apply_link", ""),
                    )
                    return True

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
            if token:
                self._hydrate_primary_dedupe_cache().add(token)
            log.info("Appended row for '%s' at '%s'", job_data.get(
                "job_title"), job_data.get("company"))

            # Mirror associate-level roles to dedicated worksheet.
            if self._is_associate_role_title(str(job_data.get("job_title", ""))):
                self.append_associate_opening_row(
                    {
                        "timestamp": ts,
                        "run_id": "",
                        "run_iteration": 0,
                        "job_title": job_data.get("job_title", ""),
                        "company": job_data.get("company", ""),
                        "location": job_data.get("location", ""),
                        "type": job_data.get("job_type", ""),
                        "apply_link": job_data.get("apply_link", ""),
                        "posted_date": job_data.get("posted_date", ""),
                        "source_url": "",
                        "matched_role": job_data.get("matched_keywords", ""),
                        "status": job_data.get("status", "New"),
                        "notes": job_data.get("notes", ""),
                        "hash_id": "",
                        "first_seen": "",
                        "last_seen": "",
                        "closed_at": "",
                        "status_color": "",
                    }
                )
            return True

        except gspread.exceptions.APIError as exc:
            log.error("Sheets API error appending row: %s", exc)
        except Exception as exc:
            log.error("Unexpected error appending row: %s", exc)
        return False

    def _append_rows_optimized(self, ws: Any, rows: list[list[Any]]) -> int:
        """Append multiple rows efficiently, falling back to single-row appends if needed."""
        if not rows:
            return 0

        append_rows_fn = getattr(ws, "append_rows", None)
        if callable(append_rows_fn):
            for i in range(0, len(rows), _APPEND_BATCH_SIZE):
                chunk = rows[i: i + _APPEND_BATCH_SIZE]
                self._retry_on_quota(append_rows_fn, chunk, value_input_option="USER_ENTERED")
            return len(rows)

        appended = 0
        for row in rows:
            self._retry_on_quota(ws.append_row, row, value_input_option="USER_ENTERED")
            appended += 1
        return appended

    def _next_append_row_number(self, ws: Any) -> int:
        values = self._retry_on_quota(ws.get_all_values)
        if not isinstance(values, list):
            return 2
        return len(values) + 1

    def _apply_row_background(self, ws: Any, row_number: int, total_columns: int, hex_color: str) -> None:
        if row_number <= 1:
            return
        r, g, b = self._hex_to_rgb_components(hex_color)
        request = {
            "requests": [
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": ws.id,
                            "startRowIndex": row_number - 1,
                            "endRowIndex": row_number,
                            "startColumnIndex": 0,
                            "endColumnIndex": max(1, int(total_columns)),
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {
                                    "red": r,
                                    "green": g,
                                    "blue": b,
                                }
                            }
                        },
                        "fields": "userEnteredFormat.backgroundColor",
                    }
                }
            ]
        }
        self._retry_on_quota(self._spreadsheet.batch_update, request)

    def _append_colored_marker_row(self, ws: Any, row: list[Any], color: str, headers: list[str]) -> bool:
        try:
            row_number = self._next_append_row_number(ws)
            self._retry_on_quota(ws.append_row, row, value_input_option="USER_ENTERED")
            self._apply_row_background(ws, row_number=row_number, total_columns=len(headers), hex_color=color)
            return True
        except Exception as exc:
            log.warning("Failed to append colored marker row in worksheet '%s': %s", getattr(ws, "title", "?"), exc)
            return False

    @staticmethod
    def _build_url_change_row(change_data: dict) -> list[Any]:
        preview_titles = change_data.get("new_opening_titles_preview", "")
        if isinstance(preview_titles, list):
            preview_titles = " | ".join(str(t) for t in preview_titles[:5])

        return [
            change_data.get("timestamp", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")),
            change_data.get("url", ""),
            change_data.get("domain", ""),
            change_data.get("change_type", ""),
            change_data.get("page_title", ""),
            change_data.get("total_openings", 0),
            change_data.get("new_openings_count", 0),
            str(preview_titles),
            change_data.get("notes", ""),
        ]

    @staticmethod
    def _build_career_opening_row(opening_data: dict) -> list[Any]:
        title = opening_data.get("job_title") or opening_data.get("position_title") or ""
        company = opening_data.get("company") or opening_data.get("domain") or ""
        location = opening_data.get("location") or ""
        job_type = opening_data.get("type") or opening_data.get("job_type") or ""
        apply_link = opening_data.get("apply_link") or opening_data.get("position_link") or ""
        posted_date = opening_data.get("posted_date") or ""
        source_url = opening_data.get("source_url") or opening_data.get("career_url") or ""
        status = opening_data.get("status") or ("New" if bool(opening_data.get("is_new", True)) else "Tracked")

        return [
            opening_data.get("timestamp", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")),
            title,
            company,
            location,
            job_type,
            apply_link,
            posted_date,
            source_url,
            status,
        ]

    @staticmethod
    def _build_associate_opening_row(opening_data: dict) -> list[Any]:
        status = str(opening_data.get("status", "New") or "New").upper()
        status_color = opening_data.get("status_color") or GoogleSheetsClient._status_color(status)
        return [
            opening_data.get("timestamp", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")),
            opening_data.get("run_id", ""),
            int(opening_data.get("run_iteration", 0) or 0),
            opening_data.get("job_title", ""),
            opening_data.get("company", ""),
            opening_data.get("location", ""),
            opening_data.get("type", ""),
            opening_data.get("apply_link", ""),
            opening_data.get("posted_date", ""),
            opening_data.get("source_url", ""),
            opening_data.get("matched_role", ""),
            status,
            opening_data.get("notes", ""),
            opening_data.get("hash_id", ""),
            opening_data.get("first_seen", ""),
            opening_data.get("last_seen", ""),
            opening_data.get("closed_at", ""),
            status_color,
        ]

    def append_iteration_divider_row(self, *, run_iteration: int, run_timestamp: str, run_id: str = "") -> bool:
        """Append a visual divider row for the current run in All_Openings."""
        try:
            ws = self._get_or_create_worksheet(ALL_OPENINGS_WORKSHEET, ALL_OPENINGS_HEADERS)
            ts = str(run_timestamp or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"))
            short_ts = ts.replace(" UTC", "")
            divider_text = f"===== RUN {int(run_iteration)} | {short_ts} ====="
            row = [
                int(run_iteration),
                "RUN_HEADER",
                "",
                divider_text,
                "",
                "",
                "",
                "",
                "",
                "",
                str(run_id or ""),
                ts,
            ]
            return self._append_colored_marker_row(ws, row=row, color=_RUN_DIVIDER_COLOR, headers=ALL_OPENINGS_HEADERS)
        except Exception as exc:
            log.warning("Failed to append run divider row: %s", exc)
            return False

    def append_iteration_summary_row(
        self,
        *,
        run_iteration: int,
        run_timestamp: str,
        run_id: str,
        jobs_found: int,
        new_jobs: int,
        closed_jobs: int,
    ) -> bool:
        """Append a yellow summary row for the current run in All_Openings."""
        try:
            ws = self._get_or_create_worksheet(ALL_OPENINGS_WORKSHEET, ALL_OPENINGS_HEADERS)
            ts = str(run_timestamp or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"))
            hhmm = ts[11:16] if len(ts) >= 16 else ts
            summary_text = (
                f"Jobs Found: {int(jobs_found)} | New Jobs: {int(new_jobs)} | "
                f"Closed Jobs: {int(closed_jobs)} | Time: {hhmm}"
            )
            row = [
                int(run_iteration),
                "SUMMARY",
                "",
                summary_text,
                "",
                "",
                "",
                "",
                "",
                "",
                str(run_id or ""),
                ts,
            ]
            return self._append_colored_marker_row(ws, row=row, color=_RUN_SUMMARY_COLOR, headers=ALL_OPENINGS_HEADERS)
        except Exception as exc:
            log.warning("Failed to append run summary row: %s", exc)
            return False

    def append_url_change_row(self, change_data: dict) -> bool:
        """Append one URL/page-change event to dedicated worksheet."""
        return self.append_url_change_rows([change_data]) > 0

    def append_url_change_rows(self, change_rows: list[dict]) -> int:
        """Append URL/page-change events in batch. Returns appended row count."""
        if not change_rows:
            return 0
        try:
            ws = self._get_or_create_worksheet(URL_CHANGES_WORKSHEET, URL_CHANGES_HEADERS)
            rows = [self._build_url_change_row(item) for item in change_rows]
            return self._append_rows_optimized(ws, rows)
        except Exception as exc:
            log.error("Failed to append URL change rows: %s", exc)
            return 0

    def append_career_opening_row(self, opening_data: dict) -> bool:
        """Append one detected career opening row to dedicated worksheet."""
        return self.append_career_opening_rows([opening_data]) > 0

    def append_career_opening_rows(self, opening_rows: list[dict]) -> int:
        """Append detected career opening rows in batch. Returns appended row count."""
        if not opening_rows:
            return 0
        try:
            ws = self._get_or_create_worksheet(CAREER_OPENINGS_WORKSHEET, CAREER_OPENINGS_HEADERS)
            existing = self._hydrate_career_opening_dedupe_cache(ws)

            unique_items: list[dict] = []
            for item in opening_rows:
                token = self._build_opening_dedupe_key(item)
                if token and token in existing:
                    continue
                if token:
                    existing.add(token)
                unique_items.append(item)

            if not unique_items:
                return 0

            rows = [self._build_career_opening_row(item) for item in unique_items]
            appended = self._append_rows_optimized(ws, rows)

            # Fan-out associate roles to dedicated worksheet.
            associate_items = [
                {
                    "timestamp": item.get("timestamp", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")),
                    "run_id": item.get("run_id", ""),
                    "run_iteration": int(item.get("run_iteration", 0) or 0),
                    "job_title": item.get("job_title") or item.get("position_title") or "",
                    "company": item.get("company") or item.get("domain") or "",
                    "location": item.get("location", ""),
                    "type": item.get("type") or item.get("job_type") or "",
                    "apply_link": item.get("apply_link") or item.get("position_link") or "",
                    "posted_date": item.get("posted_date", ""),
                    "source_url": item.get("source_url") or item.get("career_url") or "",
                    "matched_role": item.get("matched_role", ""),
                    "status": item.get("status", "New"),
                    "notes": item.get("notes", ""),
                    "hash_id": item.get("hash_id") or GoogleSheetsClient._opening_change_hash_id(item),
                    "first_seen": item.get("first_seen", ""),
                    "last_seen": item.get("last_seen", ""),
                    "closed_at": item.get("closed_at", ""),
                    "status_color": item.get("status_color", ""),
                }
                for item in unique_items
                if self._is_associate_role_title(str(item.get("job_title") or item.get("position_title") or ""))
            ]
            if associate_items:
                self.append_associate_opening_rows(associate_items)

            return appended
        except Exception as exc:
            log.error("Failed to append career opening rows: %s", exc)
            return 0

    def append_associate_opening_row(self, opening_data: dict) -> bool:
        """Append one associate-level role row to dedicated worksheet."""
        return self.append_associate_opening_rows([opening_data]) > 0

    def append_associate_opening_rows(self, opening_rows: list[dict]) -> int:
        """Append associate-level role rows in batch with dedupe."""
        if not opening_rows:
            return 0
        try:
            ws = self._get_or_create_worksheet(ASSOCIATE_ROLES_WORKSHEET, ASSOCIATE_ROLES_HEADERS)
            existing = self._hydrate_associate_dedupe_cache(ws)

            unique_items: list[dict] = []
            for item in opening_rows:
                title = str(item.get("job_title") or item.get("position_title") or "")
                if not self._is_associate_role_title(title):
                    continue

                token = self._opening_change_token(item)
                if token and token in existing:
                    continue
                if token:
                    existing.add(token)
                unique_items.append(item)

            if not unique_items:
                return 0

            rows = [self._build_associate_opening_row(item) for item in unique_items]
            return self._append_rows_optimized(ws, rows)
        except Exception as exc:
            log.error("Failed to append associate opening rows: %s", exc)
            return 0

    def append_all_openings_rows(self, opening_rows: list[dict]) -> int:
        """Append opening change rows into All_Openings worksheet with dedupe."""
        if not opening_rows:
            return 0
        try:
            ws = self._get_or_create_worksheet(ALL_OPENINGS_WORKSHEET, ALL_OPENINGS_HEADERS)
            existing = self._hydrate_worksheet_dedupe_cache(
                ws,
                cache_key=ALL_OPENINGS_WORKSHEET,
                token_builder=lambda row: self._opening_change_token(
                    {
                        "status": row.get("Status", ""),
                        "company": row.get("Company", ""),
                        "role": row.get("Title", ""),
                        "location": row.get("Location", ""),
                        "url": row.get("URL", ""),
                    }
                ),
            )

            unique_items: list[dict] = []
            for item in opening_rows:
                token = self._opening_change_token(item)
                if token and token in existing:
                    continue
                if token:
                    existing.add(token)
                unique_items.append(item)

            if not unique_items:
                return 0

            rows = [self._company_opening_row(item) for item in unique_items]
            return self._append_rows_optimized(ws, rows)
        except Exception as exc:
            log.error("Failed to append all opening rows: %s", exc)
            return 0

    def append_new_openings_rows(self, opening_rows: list[dict]) -> int:
        """Append NEW/UPDATED opening rows into New_Openings worksheet with dedupe."""
        if not opening_rows:
            return 0
        try:
            ws = self._get_or_create_worksheet(NEW_OPENINGS_WORKSHEET, NEW_OPENINGS_HEADERS)
            existing = self._hydrate_worksheet_dedupe_cache(
                ws,
                cache_key=NEW_OPENINGS_WORKSHEET,
                token_builder=lambda row: self._opening_change_token(
                    {
                        "status": row.get("Status", ""),
                        "hash_id": row.get("Hash ID", ""),
                        "role": row.get("Role", ""),
                        "apply_url": row.get("Apply URL", ""),
                    }
                ),
            )

            unique_items: list[dict] = []
            for item in opening_rows:
                status = str(item.get("status") or "").upper()
                if status not in {"NEW", "UPDATED", "ACTIVE"}:
                    continue
                token = self._opening_change_token(item)
                if token and token in existing:
                    continue
                if token:
                    existing.add(token)
                unique_items.append(item)

            if not unique_items:
                return 0

            rows = [self._new_opening_row(item) for item in unique_items]
            return self._append_rows_optimized(ws, rows)
        except Exception as exc:
            log.error("Failed to append new opening rows: %s", exc)
            return 0

    def append_company_opening_rows(self, source_url: str, opening_rows: list[dict]) -> int:
        """Append opening change rows to company-specific worksheet (one per source URL)."""
        if not source_url or not opening_rows:
            return 0
        try:
            sheet_title = safe_sheet_title_from_url(source_url)
            ws = self._get_or_create_worksheet(sheet_title, ALL_OPENINGS_HEADERS)
            cache_key = f"company::{sheet_title}"
            existing = self._hydrate_worksheet_dedupe_cache(
                ws,
                cache_key=cache_key,
                token_builder=lambda row: self._opening_change_token(
                    {
                        "status": row.get("Status", ""),
                        "company": row.get("Company", ""),
                        "role": row.get("Title", ""),
                        "location": row.get("Location", ""),
                        "url": row.get("URL", ""),
                    }
                ),
            )

            unique_items: list[dict] = []
            for item in opening_rows:
                token = self._opening_change_token(item)
                if token and token in existing:
                    continue
                if token:
                    existing.add(token)
                enriched = dict(item)
                enriched["source_url"] = str(item.get("source_url") or source_url)
                unique_items.append(enriched)

            if not unique_items:
                return 0

            rows = [self._company_opening_row(item) for item in unique_items]
            return self._append_rows_optimized(ws, rows)
        except Exception as exc:
            log.error("Failed to append company opening rows for %s: %s", source_url, exc)
            return 0

    def _collect_hash_row_numbers(self, ws: Any) -> dict[str, list[int]]:
        values = self._retry_on_quota(ws.get_all_values)
        if not values or len(values) < 2:
            return {}

        header = [str(h or "").strip() for h in values[0]]
        hash_idx = None
        company_idx = None
        title_idx = None
        location_idx = None
        url_idx = None
        for idx, name in enumerate(header):
            name_l = name.lower()
            if name_l == "hash id":
                hash_idx = idx
            elif name_l == "company":
                company_idx = idx
            elif name_l in {"title", "role", "job title"}:
                title_idx = idx
            elif name_l == "location":
                location_idx = idx
            elif name_l in {"url", "apply url", "apply link"}:
                url_idx = idx

        if hash_idx is None and (company_idx is None or title_idx is None or url_idx is None):
            return {}

        mapping: dict[str, list[int]] = {}
        for row_num in range(2, len(values) + 1):
            row = values[row_num - 1]
            if hash_idx is not None:
                hash_id = str(row[hash_idx] if hash_idx < len(row) else "").strip().lower()
            else:
                company = str(row[company_idx] if company_idx is not None and company_idx < len(row) else "").strip()
                title = str(row[title_idx] if title_idx is not None and title_idx < len(row) else "").strip()
                location = str(row[location_idx] if location_idx is not None and location_idx < len(row) else "Not Specified").strip() or "Not Specified"
                url = str(row[url_idx] if url_idx is not None and url_idx < len(row) else "").strip()
                if not company and not title and not url:
                    continue
                hash_id = build_job_hash(company, title, location, url).strip().lower()
            if not hash_id:
                continue
            mapping.setdefault(hash_id, []).append(row_num)
        return mapping

    def _batch_delete_rows(self, ws: Any, row_numbers: list[int]) -> int:
        unique_rows = sorted({int(r) for r in row_numbers if int(r) > 1}, reverse=True)
        if not unique_rows:
            return 0

        requests = [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": ws.id,
                        "dimension": "ROWS",
                        "startIndex": row_num - 1,
                        "endIndex": row_num,
                    }
                }
            }
            for row_num in unique_rows
        ]

        self._retry_on_quota(self._spreadsheet.batch_update, {"requests": requests})
        return len(unique_rows)

    def delete_openings_by_hash_ids(self, hash_ids: list[str], source_urls: list[str] | None = None) -> dict[str, int]:
        """Delete openings from active sheets by Hash ID using batch row deletion.

        Affected worksheets:
          - All_Openings
          - Associate Roles
          - Company-specific sheets (from source_urls)

        New_Openings is intentionally untouched to preserve history.
        """
        normalized_hashes = {
            str(hash_id).strip().lower()
            for hash_id in (hash_ids or [])
            if str(hash_id).strip()
        }
        if not normalized_hashes:
            return {
                "all_openings": 0,
                "associate_roles": 0,
                "company_rows": 0,
                "company_sheets": 0,
                "total": 0,
            }

        deleted_all = 0
        deleted_associate = 0
        deleted_company = 0
        company_sheet_hits = 0

        try:
            ws_all = self._get_or_create_worksheet(ALL_OPENINGS_WORKSHEET, ALL_OPENINGS_HEADERS)
            hash_rows = self._collect_hash_row_numbers(ws_all)
            rows = [row for hash_id in normalized_hashes for row in hash_rows.get(hash_id, [])]
            deleted_all = self._batch_delete_rows(ws_all, rows)
            if deleted_all:
                self._worksheet_dedupe_caches.pop(ALL_OPENINGS_WORKSHEET, None)
        except Exception as exc:
            log.warning("Failed hash-based deletion in %s: %s", ALL_OPENINGS_WORKSHEET, exc)

        try:
            ws_assoc = self._get_or_create_worksheet(ASSOCIATE_ROLES_WORKSHEET, ASSOCIATE_ROLES_HEADERS)
            hash_rows = self._collect_hash_row_numbers(ws_assoc)
            rows = [row for hash_id in normalized_hashes for row in hash_rows.get(hash_id, [])]
            deleted_associate = self._batch_delete_rows(ws_assoc, rows)
            if deleted_associate:
                self._associate_dedupe_cache = None
        except Exception as exc:
            log.warning("Failed hash-based deletion in %s: %s", ASSOCIATE_ROLES_WORKSHEET, exc)

        candidate_urls = sorted(
            {
                str(url).strip()
                for url in (source_urls or [])
                if str(url).strip()
            }
        )
        for source_url in candidate_urls:
            try:
                sheet_title = safe_sheet_title_from_url(source_url)
                ws_company = self._get_or_create_worksheet(sheet_title, ALL_OPENINGS_HEADERS)
                hash_rows = self._collect_hash_row_numbers(ws_company)
                rows = [row for hash_id in normalized_hashes for row in hash_rows.get(hash_id, [])]
                deleted = self._batch_delete_rows(ws_company, rows)
                if deleted:
                    deleted_company += deleted
                    company_sheet_hits += 1
                    self._worksheet_dedupe_caches.pop(f"company::{sheet_title}", None)
            except Exception as exc:
                log.warning("Failed hash-based deletion in company worksheet for %s: %s", source_url, exc)

        total = deleted_all + deleted_associate + deleted_company
        return {
            "all_openings": deleted_all,
            "associate_roles": deleted_associate,
            "company_rows": deleted_company,
            "company_sheets": company_sheet_hits,
            "total": total,
        }

    @staticmethod
    def _build_search_activity_row(activity_data: dict) -> list[Any]:
        pages_visited = activity_data.get("pages_visited", 0)
        if isinstance(pages_visited, list):
            pages_visited = len(pages_visited)

        return [
            activity_data.get("timestamp", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")),
            activity_data.get("run_id", ""),
            int(activity_data.get("run_iteration", 0) or 0),
            activity_data.get("url", ""),
            activity_data.get("domain", ""),
            activity_data.get("status", ""),
            activity_data.get("change_type", ""),
            activity_data.get("total_openings", 0),
            activity_data.get("new_openings_count", 0),
            activity_data.get("scraper_used", ""),
            pages_visited,
            activity_data.get("error", ""),
            activity_data.get("notes", ""),
        ]

    def append_search_activity_row(self, activity_data: dict) -> bool:
        """Append one monitor activity/audit row to dedicated worksheet."""
        return self.append_search_activity_rows([activity_data]) > 0

    def append_search_activity_rows(self, activity_rows: list[dict]) -> int:
        """Append monitor activity rows in batch. Returns appended row count."""
        if not activity_rows:
            return 0
        try:
            ws = self._get_or_create_worksheet(SEARCH_ACTIVITY_WORKSHEET, SEARCH_ACTIVITY_HEADERS)
            rows = [self._build_search_activity_row(item) for item in activity_rows]
            return self._append_rows_optimized(ws, rows)
        except Exception as exc:
            log.error("Failed to append search activity rows: %s", exc)
            return 0

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
