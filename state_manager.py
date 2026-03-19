import json
import logging
import os
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

            return data
        except (json.JSONDecodeError, IOError, OSError) as exc:
            log.warning(
                "Could not read state file (%s). Starting fresh: %s", self.filepath, exc)
            return default_state

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
