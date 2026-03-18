"""
Seed Google Sheet with dummy job rows for UI/testing verification.

Safety behavior:
- Will NOT seed if data rows already exist (prevents duplicate test data).
"""

from __future__ import annotations

import base64
import logging
import os
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

from google_sheets_client import GoogleSheetsClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("seed_sheet_data")

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))


def _load_credentials_json() -> str:
    creds_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "credentials.json")
    if os.path.isfile(creds_file):
        with open(creds_file, "r", encoding="utf-8") as f:
            content = f.read().strip()
        if content.startswith("{"):
            return content

    raw = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
    if not raw:
        return ""
    if raw.startswith("{"):
        return raw

    try:
        decoded = base64.b64decode(raw).decode("utf-8")
        if decoded.startswith("{"):
            return decoded
    except Exception:
        pass

    return ""


def _build_dummy_jobs() -> list[dict]:
    statuses = ["New", "Applied", "Interviewing", "Rejected"]
    ai_scores = [45, 70, 85, 95]
    locations = ["Lahore", "Remote", "Islamabad", "Karachi"]
    companies = [
        "Arbisoft",
        "Systems Limited",
        "Contour Software",
        "Careem",
        "VentureDive",
        "Tkxel",
    ]

    base_date = datetime.now(timezone.utc)
    jobs: list[dict] = []

    for i in range(12):
        posted = (base_date - timedelta(days=i % 7)).strftime("%Y-%m-%d")
        title = [
            "Junior Python Developer",
            "Associate AI Engineer",
            "Entry-Level Backend Developer",
            "Graduate Software Engineer",
        ][i % 4]

        jobs.append(
            {
                "timestamp": (base_date - timedelta(hours=i)).strftime("%Y-%m-%d %H:%M:%S UTC"),
                "job_title": title,
                "company": companies[i % len(companies)],
                "location": locations[i % len(locations)],
                "job_type": "Full-time" if i % 3 else "Internship",
                "posted_date": posted,
                "apply_link": f"https://example.com/jobs/dummy-{i+1}",
                "description": (
                    "Dummy seeded row for testing conditional formatting, filters, and status updates. "
                    f"This is sample posting #{i+1}."
                ),
                "matched_keywords": "Python, AI, Entry Level",
                "status": statuses[i % len(statuses)],
                "notes": "Seeded test row",
                "ai_score": ai_scores[i % len(ai_scores)],
            }
        )

    return jobs


def seed_sheet_data() -> bool:
    creds_json = _load_credentials_json()
    sheet_id = os.getenv("GOOGLE_SHEET_ID", "").strip()
    service_account_email = os.getenv("GOOGLE_SERVICE_ACCOUNT_EMAIL", "").strip()

    if not creds_json or not sheet_id:
        log.error("Missing GOOGLE_CREDENTIALS_JSON and/or GOOGLE_SHEET_ID.")
        return False

    try:
        client = GoogleSheetsClient(
            credentials_json=creds_json,
            sheet_id=sheet_id,
            service_account_email=service_account_email,
        )
    except Exception as exc:
        log.error("Failed to initialize GoogleSheetsClient: %s", exc)
        return False

    try:
        existing_rows = client._ws.get_all_values()  # intentional read-only check
        if len(existing_rows) > 1:
            log.info("Sheet already has %d data row(s). Skipping seed to avoid duplication.", len(existing_rows) - 1)
            return True
    except Exception as exc:
        log.warning("Could not inspect existing rows; continuing cautiously: %s", exc)

    seeded = 0
    for row in _build_dummy_jobs():
        if client.append_job_row(row):
            seeded += 1

    log.info("Seeding complete. Added %d dummy row(s).", seeded)
    return seeded > 0


def main() -> None:
    ok = seed_sheet_data()
    if ok:
        log.info("seed_sheet_data finished successfully.")
    else:
        log.error("seed_sheet_data failed.")


if __name__ == "__main__":
    main()
