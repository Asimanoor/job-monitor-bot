import json
import logging
import os
from datetime import datetime, timezone
from github import Github, GithubException

log = logging.getLogger(__name__)


def _resolve_repo_relative_path(filepath: str) -> str:
    """Return repository-relative POSIX path for GitHub Contents API."""
    raw = (filepath or "").strip()
    if not raw:
        return "state.json"

    normalized = os.path.normpath(raw)

    # Prefer path relative to GITHUB_WORKSPACE when available.
    workspace = os.environ.get("GITHUB_WORKSPACE", "").strip()
    if workspace:
        try:
            rel = os.path.relpath(normalized, os.path.normpath(workspace))
            if rel and not rel.startswith(".."):
                normalized = rel
        except Exception:
            pass

    # Fallback to basename when absolute/local path cannot be made relative.
    if os.path.isabs(normalized):
        normalized = os.path.basename(normalized)

    # GitHub API requires forward slashes and no leading slash.
    repo_path = normalized.replace("\\", "/").lstrip("/")
    return repo_path or "state.json"


class StateManager:
    def __init__(self, filepath: str = "state.json", max_notified_ids: int = 5000):
        self.filepath = filepath
        self.max_notified_ids = max_notified_ids
        self.state = self.load_state()

    def load_state(self) -> dict:
        """Load state from JSON file, returning empty structure on any error."""
        default_state = {
            "notified_job_ids": [],
            "api_usage": {"count": 0, "reset_month": ""},
            "groq_usage": {"count": 0, "reset_day": ""},
            "url_hashes": {},
            "url_openings": {},
            "url_job_snapshots": {},
            "companies": {},
            "job_hashes": {},
            "site_health": {},
            "baseline_initialized": False,
            "monitor_iteration": 0,
            "last_run_id": "",
            "last_run_at": "",
        }

        if not os.path.isfile(self.filepath):
            return default_state
        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                log.warning("state.json root is not a dict. Starting fresh.")
                return default_state

            # Ensure proper keys
            if "notified_job_ids" not in data:
                data["notified_job_ids"] = []
            if "api_usage" not in data:
                data["api_usage"] = {"count": 0, "reset_month": ""}
            if "groq_usage" not in data:
                data["groq_usage"] = {"count": 0, "reset_day": ""}
            if "url_hashes" not in data:
                data["url_hashes"] = {}
            if "url_openings" not in data:
                data["url_openings"] = {}
            if "url_job_snapshots" not in data:
                data["url_job_snapshots"] = {}
            if "companies" not in data:
                data["companies"] = {}
            if "job_hashes" not in data:
                data["job_hashes"] = {}
            if "site_health" not in data:
                data["site_health"] = {}
            if "baseline_initialized" not in data:
                data["baseline_initialized"] = False
            if "monitor_iteration" not in data:
                data["monitor_iteration"] = 0
            if "last_run_id" not in data:
                data["last_run_id"] = ""
            if "last_run_at" not in data:
                data["last_run_at"] = ""

            return data
        except (json.JSONDecodeError, IOError, OSError) as exc:
            log.warning(
                "Could not read state file (%s). Starting fresh: %s", self.filepath, exc)
            return default_state

    def get_monitor_iteration(self) -> int:
        return max(0, int(self.state.get("monitor_iteration", 0) or 0))

    def begin_monitor_run(self, persist: bool = True, now_ts: str = "") -> dict:
        """Create run metadata for the current monitor cycle.

        Returns:
            {
              "run_id": str,
              "run_iteration": int,
              "run_timestamp": str,
            }
        """
        ts = now_ts or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        iteration = self.get_monitor_iteration() + 1
        run_id = f"run-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{iteration:06d}"

        if persist:
            self.state["monitor_iteration"] = iteration
            self.state["last_run_id"] = run_id
            self.state["last_run_at"] = ts

        return {
            "run_id": run_id,
            "run_iteration": iteration,
            "run_timestamp": ts,
        }

    def save_state(self) -> None:
        """Atomically save state: write to temp file then rename."""
        # Prune before saving
        self._prune_notified_ids()

        tmp_path = self.filepath + ".tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(self.state, f, indent=2, ensure_ascii=False)
            # Atomic rename
            if os.path.exists(self.filepath):
                os.replace(tmp_path, self.filepath)
            else:
                os.rename(tmp_path, self.filepath)
            log.info("State saved to %s", self.filepath)
        except OSError as exc:
            log.error("Failed to save state: %s", exc)

    def _normalize_monthly_api_usage(self) -> None:
        from datetime import datetime, timezone

        usage = self.state.get("api_usage", {"count": 0, "reset_month": ""})
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")
        if usage.get("reset_month") != current_month:
            usage = {"count": 0, "reset_month": current_month}
            self.state["api_usage"] = usage

    def _normalize_daily_groq_usage(self) -> None:
        from datetime import datetime, timezone

        usage = self.state.get("groq_usage", {"count": 0, "reset_day": ""})
        current_day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if usage.get("reset_day") != current_day:
            usage = {"count": 0, "reset_day": current_day}
            self.state["groq_usage"] = usage

    def _prune_notified_ids(self) -> None:
        """Keep only the most recent IDs to prevent unbounded growth."""
        ids = self.state.get("notified_job_ids", [])
        if len(ids) > self.max_notified_ids:
            self.state["notified_job_ids"] = ids[-self.max_notified_ids:]
            log.info("Pruned notified_job_ids from %d to %d",
                     len(ids), self.max_notified_ids)

    def is_new_job(self, job_id: str) -> bool:
        """Check if a job ID has NOT been notified yet."""
        return job_id not in self.state.get("notified_job_ids", [])

    def mark_as_notified(self, job_id: str) -> None:
        """Add job ID to notified set (list internally)."""
        ids = self.state.get("notified_job_ids", [])
        if job_id not in ids:
            ids.append(job_id)
        self.state["notified_job_ids"] = ids

    def should_skip_due_to_rate_limit(self, max_requests: int = 200, safety_buffer: int = 10) -> bool:
        """Check if API usage has exceeded the safe limit."""
        usage = self.get_api_usage_count()
        return usage >= max(0, (max_requests - safety_buffer))

    def get_api_usage_count(self) -> int:
        self._normalize_monthly_api_usage()
        return int(self.state.get("api_usage", {}).get("count", 0) or 0)

    def get_remaining_api_requests(self, max_requests: int = 200) -> int:
        used = self.get_api_usage_count()
        return max(0, int(max_requests) - used)

    def get_url_hash(self, url: str) -> str:
        """Get previous hash for URL."""
        return self.state.get("url_hashes", {}).get(url)

    def set_url_hash(self, url: str, hash_val: str) -> None:
        """Save a new hash for URL."""
        hashes = self.state.get("url_hashes", {})
        hashes[url] = hash_val
        self.state["url_hashes"] = hashes

    def get_url_opening_fingerprints(self, url: str) -> set[str]:
        """Return stored opening fingerprints for a URL."""
        all_openings = self.state.get("url_openings", {})
        raw = all_openings.get(url, [])
        if not isinstance(raw, list):
            return set()
        return {str(item).strip() for item in raw if str(item).strip()}

    def set_url_opening_fingerprints(self, url: str, fingerprints: list[str] | set[str], max_per_url: int = 300) -> None:
        """Persist deduplicated opening fingerprints for a URL."""
        cleaned: list[str] = []
        seen: set[str] = set()

        for fp in fingerprints:
            token = str(fp).strip()
            if not token or token in seen:
                continue
            seen.add(token)
            cleaned.append(token)

        if len(cleaned) > max_per_url:
            cleaned = cleaned[:max_per_url]

        all_openings = self.state.get("url_openings", {})
        all_openings[url] = cleaned
        self.state["url_openings"] = all_openings

    def get_url_job_snapshots(self, url: str) -> dict[str, dict]:
        """Return previous normalized snapshot map for a URL."""
        all_snapshots = self.state.get("url_job_snapshots", {})
        raw = all_snapshots.get(url, {})
        if not isinstance(raw, dict):
            return {}

        normalized: dict[str, dict] = {}
        for key, value in raw.items():
            if not isinstance(key, str) or not key.strip():
                continue
            if not isinstance(value, dict):
                continue
            normalized[key.strip()] = value
        return normalized

    def is_baseline_initialized(self) -> bool:
        return bool(self.state.get("baseline_initialized", False))

    def set_baseline_initialized(self, value: bool = True) -> None:
        self.state["baseline_initialized"] = bool(value)

    def has_company_job_state(self) -> bool:
        companies = self.state.get("companies", {})
        return isinstance(companies, dict) and bool(companies)

    def get_company_job_state(self) -> dict[str, dict]:
        companies = self.state.get("companies", {})
        if not isinstance(companies, dict):
            return {}

        cleaned: dict[str, dict] = {}
        for company, payload in companies.items():
            if not isinstance(company, str) or not company.strip() or not isinstance(payload, dict):
                continue
            url = str(payload.get("url") or "").strip()
            jobs = payload.get("jobs") if isinstance(payload.get("jobs"), dict) else {}
            normalized_jobs: dict[str, dict] = {}
            for hash_id, meta in jobs.items():
                if not isinstance(hash_id, str) or not hash_id.strip() or not isinstance(meta, dict):
                    continue
                normalized_jobs[hash_id.strip()] = dict(meta)
            cleaned[company.strip()] = {"url": url, "jobs": normalized_jobs}
        return cleaned

    def set_company_job_state(self, companies_state: dict[str, dict]) -> None:
        cleaned: dict[str, dict] = {}
        if isinstance(companies_state, dict):
            for company, payload in companies_state.items():
                if not isinstance(company, str) or not company.strip() or not isinstance(payload, dict):
                    continue
                url = str(payload.get("url") or "").strip()
                jobs = payload.get("jobs") if isinstance(payload.get("jobs"), dict) else {}
                normalized_jobs: dict[str, dict] = {}
                for hash_id, meta in jobs.items():
                    if not isinstance(hash_id, str) or not hash_id.strip() or not isinstance(meta, dict):
                        continue
                    normalized_jobs[hash_id.strip()] = dict(meta)
                cleaned[company.strip()] = {
                    "url": url,
                    "jobs": normalized_jobs,
                }
        self.state["companies"] = cleaned

    def get_all_tracked_job_hashes(self) -> set[str]:
        hashes: set[str] = set()
        for _company, payload in self.get_company_job_state().items():
            jobs = payload.get("jobs") if isinstance(payload, dict) else {}
            if not isinstance(jobs, dict):
                continue
            for hash_id in jobs.keys():
                if isinstance(hash_id, str) and hash_id.strip():
                    hashes.add(hash_id.strip())
        return hashes

    def get_job_hash_state(self) -> dict[str, dict]:
        """Return normalized hash-keyed job tracking state."""
        raw = self.state.get("job_hashes", {})
        if not isinstance(raw, dict):
            return {}

        cleaned: dict[str, dict] = {}
        for hash_id, payload in raw.items():
            if not isinstance(hash_id, str) or not hash_id.strip() or not isinstance(payload, dict):
                continue
            normalized = {
                "title": str(payload.get("title") or "").strip(),
                "company": str(payload.get("company") or "").strip(),
                "location": str(payload.get("location") or "").strip() or "Not Specified",
                "url": str(payload.get("url") or "").strip(),
                "source_url": str(payload.get("source_url") or "").strip(),
                "first_seen": str(payload.get("first_seen") or "").strip(),
                "last_seen": str(payload.get("last_seen") or "").strip(),
                "status": str(payload.get("status") or "ACTIVE").strip() or "ACTIVE",
                "missing_count": max(0, int(payload.get("missing_count", 0) or 0)),
                "closed_at": str(payload.get("closed_at") or "").strip(),
            }
            cleaned[hash_id.strip()] = normalized
        return cleaned

    def update_job_hash_state(
        self,
        current_jobs: dict[str, dict],
        *,
        missing_threshold: int = 2,
        now_ts: str = "",
        evaluated_source_urls: set[str] | None = None,
    ) -> tuple[list[str], int]:
        """Merge current hash snapshot into state and return hashes eligible for deletion.

        Returns:
            (deletable_hashes, skipped_removal_count)
        """
        existing = self.get_job_hash_state()
        current = current_jobs if isinstance(current_jobs, dict) else {}
        threshold = max(1, int(missing_threshold))
        seen_ts = now_ts or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        evaluated = {
            str(url).strip()
            for url in (evaluated_source_urls or set())
            if str(url).strip()
        }

        next_state: dict[str, dict] = {}
        closed_now: list[str] = []
        skipped = 0

        current_hashes = {str(h).strip() for h in current.keys() if isinstance(h, str) and str(h).strip()}
        existing_hashes = set(existing.keys())

        for hash_id in current_hashes:
            payload = current.get(hash_id, {})
            if not isinstance(payload, dict):
                payload = {}
            prev = existing.get(hash_id, {})

            next_state[hash_id] = {
                "title": str(payload.get("title") or prev.get("title") or "").strip(),
                "company": str(payload.get("company") or prev.get("company") or "").strip(),
                "location": str(payload.get("location") or prev.get("location") or "Not Specified").strip() or "Not Specified",
                "url": str(payload.get("apply_link") or payload.get("apply_url") or payload.get("url") or prev.get("url") or "").strip(),
                "source_url": str(payload.get("source_url") or prev.get("source_url") or "").strip(),
                "first_seen": str(prev.get("first_seen") or seen_ts).strip() or seen_ts,
                "last_seen": seen_ts,
                "status": "ACTIVE",
                "missing_count": 0,
                "closed_at": "",
            }

        for hash_id in sorted(existing_hashes - current_hashes):
            prev = existing.get(hash_id, {})
            source_url = str(prev.get("source_url") or "").strip()

            # Safety: do not mark missing/closed when site scrape failed or was skipped.
            if evaluated and source_url and source_url not in evaluated:
                next_state[hash_id] = {
                    "title": str(prev.get("title") or "").strip(),
                    "company": str(prev.get("company") or "").strip(),
                    "location": str(prev.get("location") or "Not Specified").strip() or "Not Specified",
                    "url": str(prev.get("url") or "").strip(),
                    "source_url": source_url,
                    "first_seen": str(prev.get("first_seen") or "").strip(),
                    "last_seen": str(prev.get("last_seen") or "").strip(),
                    "status": str(prev.get("status") or "ACTIVE") or "ACTIVE",
                    "missing_count": max(0, int(prev.get("missing_count", 0) or 0)),
                    "closed_at": str(prev.get("closed_at") or "").strip(),
                }
                skipped += 1
                continue

            prev_status = str(prev.get("status") or "ACTIVE").upper()
            if prev_status == "CLOSED":
                next_state[hash_id] = {
                    "title": str(prev.get("title") or "").strip(),
                    "company": str(prev.get("company") or "").strip(),
                    "location": str(prev.get("location") or "Not Specified").strip() or "Not Specified",
                    "url": str(prev.get("url") or "").strip(),
                    "source_url": str(prev.get("source_url") or "").strip(),
                    "first_seen": str(prev.get("first_seen") or "").strip(),
                    "last_seen": str(prev.get("last_seen") or "").strip(),
                    "status": "CLOSED",
                    "missing_count": max(0, int(prev.get("missing_count", 0) or 0)),
                    "closed_at": str(prev.get("closed_at") or prev.get("last_seen") or seen_ts).strip() or seen_ts,
                }
                continue

            miss = max(0, int(prev.get("missing_count", 0) or 0)) + 1
            if miss >= threshold:
                next_state[hash_id] = {
                    "title": str(prev.get("title") or "").strip(),
                    "company": str(prev.get("company") or "").strip(),
                    "location": str(prev.get("location") or "Not Specified").strip() or "Not Specified",
                    "url": str(prev.get("url") or "").strip(),
                    "source_url": str(prev.get("source_url") or "").strip(),
                    "first_seen": str(prev.get("first_seen") or "").strip(),
                    "last_seen": seen_ts,
                    "status": "CLOSED",
                    "missing_count": miss,
                    "closed_at": seen_ts,
                }
                if prev_status != "CLOSED":
                    closed_now.append(hash_id)
                continue

            skipped += 1
            next_state[hash_id] = {
                "title": str(prev.get("title") or "").strip(),
                "company": str(prev.get("company") or "").strip(),
                "location": str(prev.get("location") or "Not Specified").strip() or "Not Specified",
                "url": str(prev.get("url") or "").strip(),
                "source_url": str(prev.get("source_url") or "").strip(),
                "first_seen": str(prev.get("first_seen") or "").strip(),
                "last_seen": str(prev.get("last_seen") or "").strip(),
                "status": "MISSING",
                "missing_count": miss,
                "closed_at": "",
            }

        self.state["job_hashes"] = next_state
        return sorted(closed_now), skipped

    def get_site_health(self) -> dict[str, dict]:
        health = self.state.get("site_health", {})
        return health if isinstance(health, dict) else {}

    def update_site_health(self, source_url: str, success: bool, error: str = "", at_ts: str = "") -> None:
        if not source_url or not str(source_url).strip():
            return
        ts = at_ts or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        health = self.get_site_health()
        current = health.get(source_url, {}) if isinstance(health.get(source_url), dict) else {}
        updated = {
            "site_last_success": str(current.get("site_last_success") or ""),
            "site_last_failure": str(current.get("site_last_failure") or ""),
            "last_error": str(current.get("last_error") or ""),
            "last_status": "success" if success else "failure",
        }
        if success:
            updated["site_last_success"] = ts
            updated["last_error"] = ""
        else:
            updated["site_last_failure"] = ts
            updated["last_error"] = str(error or "").strip()

        health[source_url] = updated
        self.state["site_health"] = health

    def cleanup_closed_hash_records(self, max_age_days: int = 30, now_ts: str = "") -> int:
        """Delete CLOSED hash entries older than max_age_days from job_hashes."""
        from datetime import timedelta

        removed = 0
        state = self.get_job_hash_state()
        if not state:
            return 0

        now_dt = datetime.now(timezone.utc)
        if now_ts:
            try:
                now_dt = datetime.strptime(now_ts, "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
            except Exception:
                pass

        keep: dict[str, dict] = {}
        for hash_id, payload in state.items():
            status = str(payload.get("status") or "").upper()
            if status != "CLOSED":
                keep[hash_id] = payload
                continue

            closed_at_raw = str(payload.get("closed_at") or payload.get("last_seen") or "").strip()
            try:
                closed_at = datetime.strptime(closed_at_raw, "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
            except Exception:
                keep[hash_id] = payload
                continue

            if now_dt - closed_at > timedelta(days=max_age_days):
                removed += 1
            else:
                keep[hash_id] = payload

        self.state["job_hashes"] = keep
        return removed

    def set_job_hash_state(self, jobs_by_hash: dict[str, dict], now_ts: str = "") -> None:
        """Replace hash-keyed state directly (used for baseline initialization)."""
        from datetime import datetime, timezone

        ts = now_ts or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        normalized: dict[str, dict] = {}
        if isinstance(jobs_by_hash, dict):
            for hash_id, payload in jobs_by_hash.items():
                if not isinstance(hash_id, str) or not hash_id.strip() or not isinstance(payload, dict):
                    continue
                normalized[hash_id.strip()] = {
                    "title": str(payload.get("title") or "").strip(),
                    "company": str(payload.get("company") or "").strip(),
                    "location": str(payload.get("location") or "Not Specified").strip() or "Not Specified",
                    "url": str(payload.get("apply_link") or payload.get("apply_url") or payload.get("url") or "").strip(),
                    "source_url": str(payload.get("source_url") or "").strip(),
                    "first_seen": ts,
                    "last_seen": ts,
                    "status": "ACTIVE",
                    "missing_count": 0,
                    "closed_at": "",
                }
        self.state["job_hashes"] = normalized

    def set_url_job_snapshots(self, url: str, snapshots: dict[str, dict], max_per_url: int = 1500) -> None:
        """Persist normalized snapshot map for a URL."""
        clean: dict[str, dict] = {}
        if isinstance(snapshots, dict):
            for key, value in snapshots.items():
                if not isinstance(key, str) or not key.strip():
                    continue
                if not isinstance(value, dict):
                    continue
                clean[key.strip()] = value
                if len(clean) >= max(1, int(max_per_url)):
                    break

        all_snapshots = self.state.get("url_job_snapshots", {})
        all_snapshots[url] = clean
        self.state["url_job_snapshots"] = all_snapshots

    def get_last_telegram_update_id(self) -> int:
        """Get last processed telegram update ID."""
        return self.state.get("_tg_last_update_id", 0)

    def update_last_telegram_update_id(self, update_id: int) -> None:
        """Update last processed telegram update ID."""
        self.state["_tg_last_update_id"] = max(
            self.get_last_telegram_update_id(), update_id)

    def track_api_usage(self, amount: int = 1) -> None:
        """Increment API call count for the current month."""
        self._normalize_monthly_api_usage()
        usage = self.state.get("api_usage", {"count": 0, "reset_month": ""})
        usage["count"] += max(0, int(amount))
        self.state["api_usage"] = usage

    def get_groq_usage_count(self) -> int:
        self._normalize_daily_groq_usage()
        return int(self.state.get("groq_usage", {}).get("count", 0) or 0)

    def track_groq_usage(self, amount: int = 1) -> None:
        """Increment GROQ call count for current UTC day."""
        self._normalize_daily_groq_usage()
        usage = self.state.get("groq_usage", {"count": 0, "reset_day": ""})
        usage["count"] += max(0, int(amount))
        self.state["groq_usage"] = usage

    def get_remaining_groq_requests(self, max_requests: int = 500) -> int:
        used = self.get_groq_usage_count()
        return max(0, int(max_requests) - used)

    def should_skip_groq_due_to_rate_limit(self, max_requests: int = 500, safety_buffer: int = 50) -> bool:
        usage = self.get_groq_usage_count()
        return usage >= max(0, (max_requests - safety_buffer))

    def commit_to_github(self) -> bool:
        """Commit the modified state.json back to GitHub."""
        gh_pat = os.environ.get("GH_PAT")
        if not gh_pat:
            log.warning(
                "GH_PAT not found in environment, skipping state.json commit")
            return False

        try:
            repo_name = os.environ.get("GITHUB_REPOSITORY")
            if not repo_name:
                log.warning(
                    "GITHUB_REPOSITORY not set, assuming local run. Skipping direct Github commit.")
                return False

            gh = Github(gh_pat)
            repo = gh.get_repo(repo_name)
            repo_path = _resolve_repo_relative_path(self.filepath)

            # Read current content
            with open(self.filepath, "r", encoding="utf-8") as f:
                content = f.read()

            from datetime import datetime
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            commit_message = f"chore: Update state {timestamp} [skip ci]"

            # Check if file exists in repo to get SHA
            try:
                contents = repo.get_contents(repo_path)
                repo.update_file(contents.path, commit_message,
                                 content, contents.sha)
                log.info("Successfully updated %s in GitHub via PyGithub", repo_path)
            except GithubException as e:
                if e.status == 404:
                    # File doesn't exist yet
                    repo.create_file(repo_path, commit_message, content)
                    log.info("Successfully created %s in GitHub via PyGithub", repo_path)
                else:
                    raise e
            return True
        except Exception as exc:
            log.error("Failed to commit state.json to GitHub: %s", exc)
            return False
