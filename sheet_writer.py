from __future__ import annotations

import logging
from typing import Any

from classifier import is_associate_role

log = logging.getLogger(__name__)

_REQUIRED_COMPANY_SHEETS = [
	"Confiz",
	"Tkxel",
	"Dubizzlelabs",
	"Sadapay",
	"Venturedive",
]


class SheetWriter:
	"""Coordinate normalized, deduped writes into worksheets in strict order."""

	def __init__(self, sheets_client: Any | None):
		self.sheets = sheets_client

	@staticmethod
	def _status_color(status: str) -> str:
		palette = {
			"NEW": "#d9ead3",
			"UPDATED": "#eeeeee",
			"EXISTING": "#ffffff",
			"ACTIVE": "#ffffff",
			"CLOSED": "#f4cccc",
		}
		return palette.get(str(status or "").upper(), "#ffffff")

	def simulate_counts(
		self,
		*,
		new_jobs: list[dict],
		updated_jobs: list[dict],
		active_jobs: list[dict],
		company_jobs: dict[str, list[dict]],
		closed_jobs: list[dict] | None = None,
		delete_closed_rows: bool = False,
	) -> dict[str, int]:
		associate_count = 0
		for job in new_jobs + updated_jobs + list(closed_jobs or []):
			if is_associate_role(
				str(job.get("title") or job.get("job_title") or ""),
				description=str(job.get("description") or ""),
				department=str(job.get("category") or ""),
			):
				associate_count += 1

		return {
			"new_openings": len(new_jobs),
			"updated_openings": len(updated_jobs),
			"all_openings_snapshot": len(active_jobs),
			"associate_openings": associate_count,
			"company_sheet_rows": sum(len(rows) for rows in company_jobs.values()),
			"closed_openings": len(closed_jobs or []),
			"deleted_rows": len(closed_jobs or []) if delete_closed_rows else 0,
		}

	@staticmethod
	def _row_for_all(
		job: dict,
		status: str = "ACTIVE",
		run_id: str = "",
		run_iteration: int = 0,
		run_timestamp: str = "",
	) -> dict:
		status_norm = str(status or "ACTIVE").upper()
		return {
			"timestamp": job.get("timestamp", run_timestamp),
			"run_id": job.get("run_id", run_id),
			"run_iteration": int(job.get("run_iteration", run_iteration) or 0),
			"company": job.get("company", ""),
			"role": job.get("title") or job.get("job_title") or "",
			"title": job.get("title") or job.get("job_title") or "",
			"location": job.get("location", "Not Specified"),
			"category": job.get("category", "Not Specified"),
			"experience": job.get("experience", "Not Specified"),
			"job_type": job.get("job_type") or job.get("type") or "Not Specified",
			"apply_url": job.get("apply_link") or job.get("apply_url") or "",
			"source_url": job.get("source_url", ""),
			"status": status_norm,
			"hash_id": job.get("hash_id", ""),
			"first_seen": job.get("first_seen", ""),
			"last_seen": job.get("last_seen", ""),
			"closed_at": job.get("closed_at", ""),
			"status_color": SheetWriter._status_color(status_norm),
		}

	def write(
		self,
		*,
		new_jobs: list[dict],
		updated_jobs: list[dict],
		active_jobs: list[dict],
		company_jobs: dict[str, list[dict]],
		closed_jobs: list[dict] | None = None,
		delete_closed_rows: bool = False,
		run_metadata: dict | None = None,
	) -> dict[str, int]:
		if self.sheets is None:
			return {
				"all_openings": 0,
				"new_openings": 0,
				"associate_roles": 0,
				"company_rows": 0,
				"closed_openings": 0,
				"removed_all_openings": 0,
				"removed_associate": 0,
				"removed_company_rows": 0,
				"removed_total": 0,
			}

		# Strict order: Scrape -> Normalize -> Hash -> Compare -> Classify -> Deduplicate -> Write
		run_meta = run_metadata if isinstance(run_metadata, dict) else {}
		run_id = str(run_meta.get("run_id") or "")
		run_iteration = int(run_meta.get("run_iteration", 0) or 0)
		run_timestamp = str(run_meta.get("run_timestamp") or "")

		all_snapshot_rows = [
			self._row_for_all(
				job,
				status="ACTIVE",
				run_id=run_id,
				run_iteration=run_iteration,
				run_timestamp=run_timestamp,
			)
			for job in active_jobs
		]

		delta_rows = [
			self._row_for_all(
				job,
				status="NEW",
				run_id=run_id,
				run_iteration=run_iteration,
				run_timestamp=run_timestamp,
			)
			for job in new_jobs
		] + [
			self._row_for_all(
				job,
				status="UPDATED",
				run_id=run_id,
				run_iteration=run_iteration,
				run_timestamp=run_timestamp,
			)
			for job in updated_jobs
		]

		closed_rows = [
			self._row_for_all(
				job,
				status="CLOSED",
				run_id=run_id,
				run_iteration=run_iteration,
				run_timestamp=run_timestamp,
			)
			for job in (closed_jobs or [])
		]

		all_count = 0
		new_count = 0
		assoc_count = 0
		company_count = 0
		closed_count = 0
		removed_all = 0
		removed_associate = 0
		removed_company = 0
		removed_total = 0

		replace_active = getattr(self.sheets, "replace_active_jobs_rows", None)
		append_all = getattr(self.sheets, "append_all_openings_rows", None)
		if callable(replace_active):
			all_count = int(replace_active(all_snapshot_rows))
		elif callable(append_all):
			all_count = int(append_all(all_snapshot_rows))

		append_new_jobs = getattr(self.sheets, "append_new_jobs_rows", None)
		append_new = getattr(self.sheets, "append_new_openings_rows", None)
		if callable(append_new_jobs):
			new_count = int(append_new_jobs(delta_rows))
		elif callable(append_new):
			new_count = int(append_new(delta_rows))

		append_closed_jobs = getattr(self.sheets, "append_closed_jobs_rows", None)
		if callable(append_closed_jobs):
			closed_count = int(append_closed_jobs(closed_rows))
		else:
			closed_count = len(closed_rows)

		associate_rows: list[dict] = []
		for row in delta_rows + closed_rows:
			if is_associate_role(
				str(row.get("role") or ""),
				description="",
				department=str(row.get("category") or ""),
			):
				associate_rows.append(
					{
						"timestamp": row.get("timestamp", ""),
						"run_id": row.get("run_id", ""),
						"run_iteration": row.get("run_iteration", 0),
						"job_title": row.get("role", ""),
						"company": row.get("company", ""),
						"location": row.get("location", ""),
						"type": row.get("job_type", ""),
						"apply_link": row.get("apply_url", ""),
						"posted_date": "",
						"source_url": row.get("source_url", ""),
						"matched_role": row.get("role", ""),
						"status": row.get("status", "NEW"),
						"notes": "associate pipeline",
						"hash_id": row.get("hash_id", ""),
						"first_seen": row.get("first_seen", ""),
						"last_seen": row.get("last_seen", ""),
						"closed_at": row.get("closed_at", ""),
						"status_color": row.get("status_color", ""),
					}
				)

		append_assoc = getattr(self.sheets, "append_associate_opening_rows", None)
		if callable(append_assoc):
			assoc_count = int(append_assoc(associate_rows))

		append_company = getattr(self.sheets, "append_company_opening_rows", None)
		replace_company = getattr(self.sheets, "replace_company_jobs_rows", None)
		if callable(replace_company):
			new_hashes = {str(item.get("hash_id") or "") for item in new_jobs}
			company_snapshot_rows: dict[str, list[dict]] = {name: [] for name in _REQUIRED_COMPANY_SHEETS}
			for row in all_snapshot_rows:
				company = str(row.get("company") or "").strip()
				if not company:
					continue
				status = "NEW" if str(row.get("hash_id") or "") in new_hashes else "ACTIVE"
				company_snapshot_rows.setdefault(company, []).append(
					{
						"title": row.get("title") or row.get("role") or "",
						"location": row.get("location") or "Not Specified",
						"apply_link": row.get("apply_url") or "",
						"first_seen": row.get("first_seen") or "",
						"status": status,
					}
				)

			for company, rows in company_snapshot_rows.items():
				company_count += int(replace_company(company, rows))
		elif callable(append_company):
			grouped_company_rows: dict[str, list[dict]] = {}
			for source_url, jobs in company_jobs.items():
				rows = [
					self._row_for_all(
						job,
						status=(
							"NEW"
							if str(job.get("status") or "").upper() == "NEW"
							else "CLOSED"
							if str(job.get("status") or "").upper() == "CLOSED"
							else "ACTIVE"
						),
						run_id=run_id,
						run_iteration=run_iteration,
						run_timestamp=run_timestamp,
					)
					for job in jobs
				]
				if rows:
					grouped_company_rows.setdefault(source_url, []).extend(rows)

			for job in closed_rows:
				source_url = str(job.get("source_url") or "").strip()
				if source_url:
					grouped_company_rows.setdefault(source_url, []).append(job)

			for source_url, rows in grouped_company_rows.items():
				company_count += int(append_company(source_url, rows))

		delete_by_hash = getattr(self.sheets, "delete_openings_by_hash_ids", None)
		if callable(delete_by_hash) and delete_closed_rows and closed_jobs:
			hashes = [str(item.get("hash_id") or "").strip() for item in closed_jobs if isinstance(item, dict)]
			source_urls = [str(item.get("source_url") or "").strip() for item in closed_jobs if isinstance(item, dict)]
			result = delete_by_hash(hashes, source_urls)
			if isinstance(result, dict):
				removed_all = int(result.get("all_openings", 0) or 0)
				removed_associate = int(result.get("associate_roles", 0) or 0)
				removed_company = int(result.get("company_rows", 0) or 0)
				removed_total = int(result.get("total", 0) or 0)

		replace_all_companies = getattr(self.sheets, "replace_all_companies_rows", None)
		if callable(replace_all_companies):
			stats: dict[str, dict[str, Any]] = {}
			for row in all_snapshot_rows:
				company = str(row.get("company") or "").strip()
				if not company:
					continue
				entry = stats.setdefault(
					company,
					{
						"company": company,
						"career_url": str(row.get("source_url") or ""),
						"active_roles": 0,
						"new_roles": 0,
						"last_updated": run_timestamp,
					},
				)
				entry["active_roles"] = int(entry.get("active_roles", 0) or 0) + 1

			for row in delta_rows:
				company = str(row.get("company") or "").strip()
				if not company:
					continue
				entry = stats.setdefault(
					company,
					{
						"company": company,
						"career_url": str(row.get("source_url") or ""),
						"active_roles": 0,
						"new_roles": 0,
						"last_updated": run_timestamp,
					},
				)
				entry["new_roles"] = int(entry.get("new_roles", 0) or 0) + 1

			replace_all_companies(list(stats.values()))

		return {
			"all_openings": all_count,
			"new_openings": new_count,
			"associate_roles": assoc_count,
			"company_rows": company_count,
			"closed_openings": closed_count,
			"removed_all_openings": removed_all,
			"removed_associate": removed_associate,
			"removed_company_rows": removed_company,
			"removed_total": removed_total,
		}


__all__ = ["SheetWriter"]
