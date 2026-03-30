from __future__ import annotations

import importlib.util
import hashlib
import logging
import random
import re
import time
from urllib.parse import urlparse, urlunparse
from typing import Any

import requests

from dedup import build_job_key
from job_scraper import extract_job_postings
from role_filter import matches_target_role

try:
	from mcp_scraper import MultiStrategyCareerScraper
except Exception:  # pragma: no cover
	MultiStrategyCareerScraper = None

log = logging.getLogger(__name__)

_ROTATING_UAS = [
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
	"(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
	"(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_6) AppleWebKit/605.1.15 "
	"(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]


def _playwright_available() -> bool:
	return importlib.util.find_spec("playwright") is not None


def _requests_html_available() -> bool:
	return importlib.util.find_spec("requests_html") is not None


class JobScraperEngine:
	"""Unified scraper with anti-bot retries and deterministic fallbacks."""

	def __init__(self, timeout_seconds: int = 20, max_pages: int = 8, max_openings: int = 120):
		self.timeout_seconds = max(5, int(timeout_seconds))
		self.max_pages = max(1, int(max_pages))
		self.max_openings = max(1, int(max_openings))
		self._response_cache: dict[str, dict[str, Any]] = {}

		self._multi_strategy = None
		if MultiStrategyCareerScraper is not None:
			try:
				self._multi_strategy = MultiStrategyCareerScraper(
					headless=True,
					timeout_ms=self.timeout_seconds * 1000,
					enable_playwright=_playwright_available(),
					enable_langchain=True,
					enable_crewai=True,
				)
			except Exception as exc:  # pragma: no cover
				log.warning("Failed to initialize multi-strategy scraper: %s", exc)

	@staticmethod
	def _normalize_opening(job: dict[str, Any], source_url: str) -> dict[str, str]:
		return {
			"title": str(job.get("title") or "").strip(),
			"company": str(job.get("company") or "").strip(),
			"location": str(job.get("location") or "Not Specified").strip() or "Not Specified",
			"type": str(job.get("type") or "Not Specified").strip() or "Not Specified",
			"apply_link": str(job.get("apply_link") or job.get("job_url") or "").strip(),
			"source_url": source_url,
		}

	def _dedupe_openings(self, openings: list[dict[str, str]]) -> list[dict[str, str]]:
		seen_job_ids: set[str] = set()
		seen_hashes: set[str] = set()
		deduped: list[dict[str, str]] = []

		for item in openings:
			job_id = build_job_key(
				item.get("title", ""),
				item.get("location", ""),
				item.get("apply_link", ""),
			)
			if not job_id:
				continue
			if job_id in seen_job_ids or job_id in seen_hashes:
				continue
			seen_job_ids.add(job_id)
			seen_hashes.add(job_id)
			deduped.append(item)

		return deduped

	def _extract_from_html(self, url: str, html: str) -> list[dict[str, str]]:
		jobs = extract_job_postings(html or "", url, max_results=self.max_openings)
		normalized = [self._normalize_opening(job, url) for job in jobs if isinstance(job, dict)]
		return self._dedupe_openings(normalized)

	@staticmethod
	def _detect_platform(url: str) -> str:
		h = (urlparse(url).netloc or "").lower()
		p = (urlparse(url).path or "").lower()
		joined = f"{h}{p}"
		if "workday" in joined:
			return "Workday"
		if "greenhouse" in joined:
			return "Greenhouse"
		if "lever" in joined:
			return "Lever"
		if "ashby" in joined:
			return "Ashby"
		if "workable" in joined:
			return "Workable"
		if "breezy" in joined:
			return "Breezy"
		if "smartrecruiters" in joined:
			return "SmartRecruiters"
		if "zohorecruit" in joined or "zoho" in joined:
			return "ZohoRecruit"
		if "successfactors" in joined or "sapsf" in joined:
			return "SAP SuccessFactors"
		if "icims" in joined:
			return "iCIMS"
		if "applytojob" in joined:
			return "ApplyToJob"
		return "Custom HTML"

	@staticmethod
	def _is_entry_level_experience(text: str) -> bool:
		pattern = re.compile(r"(0\s*-?\s*2|0\s*-?\s*1|1\s*year|fresh|graduate|entry)", re.IGNORECASE)
		return bool(pattern.search(str(text or "")))

	def _is_entry_level_job(self, job: dict[str, str]) -> bool:
		title = str(job.get("title") or "")
		description = str(job.get("description") or "")
		experience = str(job.get("experience") or "")

		matched, _role, _score = matches_target_role(title, description=description, exclude_senior=True)
		if matched:
			return True
		if self._is_entry_level_experience(experience):
			return True
		if self._is_entry_level_experience(description):
			return True
		return False

	def _apply_entry_level_filter(self, openings: list[dict[str, str]]) -> list[dict[str, str]]:
		filtered: list[dict[str, str]] = []
		for item in openings:
			if self._is_entry_level_job(item):
				filtered.append(item)
			if len(filtered) >= self.max_openings:
				break
		return filtered

	def _fetch_with_requests_retries(self, url: str) -> tuple[bool, list[dict[str, str]], str]:
		# Try normal request -> hardened headers -> session -> rotated UA
		strategies: list[tuple[str, dict[str, str], bool]] = [
			("normal", {}, False),
			(
				"headers",
				{
					"User-Agent": _ROTATING_UAS[0],
					"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
					"Accept-Language": "en-US,en;q=0.9",
				},
				False,
			),
			(
				"session",
				{
					"User-Agent": _ROTATING_UAS[0],
					"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
				},
				True,
			),
			(
				"rotated_ua",
				{
					"User-Agent": random.choice(_ROTATING_UAS),
					"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
				},
				True,
			),
		]

		last_error = ""
		backoff_schedule = [2, 5, 10]
		for idx, (name, headers, use_session) in enumerate(strategies[:4], start=1):
			try:
				request_headers = dict(headers)
				cached = self._response_cache.get(url, {}) if isinstance(self._response_cache.get(url), dict) else {}
				if isinstance(cached, dict):
					etag = str(cached.get("etag") or "").strip()
					last_modified = str(cached.get("last_modified") or "").strip()
					if etag:
						request_headers["If-None-Match"] = etag
					if last_modified:
						request_headers["If-Modified-Since"] = last_modified

				if use_session:
					session = requests.Session()
					resp = session.get(url, headers=request_headers, timeout=self.timeout_seconds, allow_redirects=True)
				else:
					resp = requests.get(url, headers=request_headers, timeout=self.timeout_seconds, allow_redirects=True)

				if resp.status_code == 304 and isinstance(cached, dict):
					openings_cached = cached.get("openings") if isinstance(cached.get("openings"), list) else []
					return True, [item for item in openings_cached if isinstance(item, dict)], f"{name}_cached_304"

				if resp.status_code in {403, 429}:
					last_error = f"{name}: blocked status {resp.status_code}"
					if resp.status_code == 429 and idx <= len(backoff_schedule):
						delay = backoff_schedule[idx - 1]
						log.warning("429 detected for %s; retrying with backoff in %ss", url, delay)
						time.sleep(delay)
					continue

				resp.raise_for_status()
				body_text = resp.text or ""
				body_hash = hashlib.sha256(body_text.encode("utf-8", errors="ignore")).hexdigest()
				if isinstance(cached, dict) and str(cached.get("body_hash") or "") == body_hash:
					openings_cached = cached.get("openings") if isinstance(cached.get("openings"), list) else []
					return True, [item for item in openings_cached if isinstance(item, dict)], f"{name}_cached_hash"

				openings = self._extract_from_html(str(resp.url or url), body_text)
				self._response_cache[url] = {
					"etag": str(resp.headers.get("ETag") or "").strip(),
					"last_modified": str(resp.headers.get("Last-Modified") or "").strip(),
					"body_hash": body_hash,
					"openings": openings,
				}
				return True, openings, name
			except Exception as exc:
				last_error = f"{name}: {exc}"
				continue

		return False, [], last_error

	@staticmethod
	def _json_to_openings(payload: Any, source_url: str, max_openings: int) -> list[dict[str, str]]:
		"""Extract openings from common jobs JSON payload shapes."""
		candidates: list[dict[str, Any]] = []

		if isinstance(payload, list):
			candidates = [item for item in payload if isinstance(item, dict)]
		elif isinstance(payload, dict):
			for key in ("jobs", "results", "positions", "openings", "data"):
				value = payload.get(key)
				if isinstance(value, list):
					candidates = [item for item in value if isinstance(item, dict)]
					break

		openings: list[dict[str, str]] = []
		for item in candidates:
			title = str(item.get("title") or item.get("name") or item.get("jobTitle") or "").strip()
			if not title:
				continue
			apply_link = str(
				item.get("url")
				or item.get("applyUrl")
				or item.get("jobUrl")
				or item.get("absolute_url")
				or item.get("hostedUrl")
				or ""
			).strip() or source_url
			openings.append(
				{
					"title": title,
					"company": str(item.get("company") or "").strip(),
					"location": str(item.get("location") or item.get("city") or "Not Specified").strip() or "Not Specified",
					"type": str(item.get("employmentType") or item.get("type") or "Not Specified").strip() or "Not Specified",
					"apply_link": apply_link,
					"source_url": source_url,
				}
			)
			if len(openings) >= max_openings:
				break

		return openings

	def _fallback_jobs_api(self, url: str) -> tuple[bool, list[dict[str, str]], str]:
		"""Probe known jobs API endpoints when HTML extraction returns empty."""
		parsed = urlparse(url)
		if not parsed.scheme or not parsed.netloc:
			return False, [], "api-fallback: invalid URL"

		base = urlunparse((parsed.scheme, parsed.netloc, "", "", "", "")).rstrip("/")
		paths = [
			"/jobs",
			"/api/jobs",
			"/graphql",
			"/workday/jobs",
			"/lever/jobs",
			"/greenhouse/jobs",
			"/breezy/jobs",
			"/workable/jobs",
		]

		for path in paths:
			endpoint = f"{base}{path}"
			try:
				resp = requests.get(endpoint, timeout=self.timeout_seconds, allow_redirects=True)
				if resp.status_code in {403, 404, 429}:
					continue
				resp.raise_for_status()
				content_type = (resp.headers.get("Content-Type") or "").lower()
				if "json" not in content_type and not (resp.text or "").lstrip().startswith(("{", "[")):
					continue
				payload = resp.json()
				openings = self._json_to_openings(payload, str(resp.url or endpoint), self.max_openings)
				if openings:
					return True, self._dedupe_openings(openings), f"api:{path}"
			except Exception:
				continue

		return False, [], "api-fallback: no valid jobs endpoint"

	def _fallback_playwright(self, url: str) -> tuple[bool, list[dict[str, str]], str]:
		if self._multi_strategy is None:
			return False, [], "playwright: unavailable"
		try:
			result = self._multi_strategy.scrape_site_openings_sync(
				url,
				max_pages=self.max_pages,
				max_openings=self.max_openings,
			)
			if not isinstance(result, dict) or not result.get("ok"):
				return False, [], f"playwright: {str((result or {}).get('error') if isinstance(result, dict) else 'failed')}"
			openings = result.get("openings") if isinstance(result.get("openings"), list) else []
			normalized: list[dict[str, str]] = []
			for opening in openings:
				if not isinstance(opening, dict):
					continue
				normalized.append(
					{
						"title": str(opening.get("title") or "").strip(),
						"company": str(opening.get("company") or "").strip(),
						"location": str(opening.get("location") or "Not Specified").strip() or "Not Specified",
						"type": str(opening.get("type") or opening.get("job_type") or "Not Specified").strip() or "Not Specified",
						"apply_link": str(opening.get("link") or opening.get("apply_link") or "").strip(),
						"source_url": str(result.get("final_url") or url),
					}
				)
			return True, self._dedupe_openings(normalized), "playwright"
		except Exception as exc:
			return False, [], f"playwright: {exc}"

	def _fallback_requests_html(self, url: str) -> tuple[bool, list[dict[str, str]], str]:
		if not _requests_html_available():
			return False, [], "requests-html: unavailable"
		try:
			from requests_html import HTMLSession  # type: ignore

			session = HTMLSession()
			resp = session.get(url, timeout=self.timeout_seconds)
			if resp.status_code in {403, 429}:
				return False, [], f"requests-html: blocked status {resp.status_code}"

			resp.html.render(timeout=max(8, self.timeout_seconds), sleep=1)
			html = resp.html.html or ""
			openings = self._extract_from_html(str(resp.url or url), html)
			return True, openings, "requests-html"
		except Exception as exc:
			return False, [], f"requests-html: {exc}"

	def scrape_url_jobs(self, url: str) -> dict[str, Any]:
		detected_platform = self._detect_platform(url)
		log.info("Detected platform: %s (%s)", detected_platform, url)

		# 1) requests retry ladder (max 3 retries worth of robustness in sequence)
		ok, openings, method = self._fetch_with_requests_retries(url)
		if ok:
			filtered = self._apply_entry_level_filter(openings)
			return {
				"ok": True,
				"url": url,
				"method": method,
				"openings": filtered[: self.max_openings],
				"detected_platform": detected_platform,
				"error": "",
			}

		# 2) playwright if available
		ok, openings, method_pw = self._fallback_playwright(url)
		if ok:
			filtered = self._apply_entry_level_filter(openings)
			return {
				"ok": True,
				"url": url,
				"method": method_pw,
				"openings": filtered[: self.max_openings],
				"detected_platform": detected_platform,
				"error": "",
			}

		# 3) requests-html fallback
		ok, openings, method_rh = self._fallback_requests_html(url)
		if ok:
			filtered = self._apply_entry_level_filter(openings)
			return {
				"ok": True,
				"url": url,
				"method": method_rh,
				"openings": filtered[: self.max_openings],
				"detected_platform": detected_platform,
				"error": "",
			}

		# 4) API endpoint fallback
		ok, openings, method_api = self._fallback_jobs_api(url)
		if ok:
			filtered = self._apply_entry_level_filter(openings)
			return {
				"ok": True,
				"url": url,
				"method": method_api,
				"openings": filtered[: self.max_openings],
				"detected_platform": detected_platform,
				"error": "",
			}

		return {
			"ok": False,
			"url": url,
			"method": "failed",
			"openings": [],
			"detected_platform": detected_platform,
			"error": "; ".join(
				[
					method,
					method_pw,
					method_rh,
					method_api,
				]
			),
		}


__all__ = ["JobScraperEngine", "MultiStrategyCareerScraper"]
