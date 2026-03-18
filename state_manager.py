import json
import logging
import os
from github import Github, GithubException

log = logging.getLogger(__name__)


class StateManager:
    def __init__(self, filepath: str = "state.json", max_notified_ids: int = 5000):
        self.filepath = filepath
        self.max_notified_ids = max_notified_ids
        self.state = self.load_state()

    def load_state(self) -> dict:
        """Load state from JSON file, returning empty structure on any error."""
        if not os.path.isfile(self.filepath):
            return {"notified_job_ids": [], "api_usage": {"count": 0, "reset_month": ""}, "url_hashes": {}}
        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                log.warning("state.json root is not a dict. Starting fresh.")
                return {"notified_job_ids": [], "api_usage": {"count": 0, "reset_month": ""}, "url_hashes": {}}

            # Ensure proper keys
            if "notified_job_ids" not in data:
                data["notified_job_ids"] = []
            if "api_usage" not in data:
                data["api_usage"] = {"count": 0, "reset_month": ""}
            if "url_hashes" not in data:
                data["url_hashes"] = {}

            return data
        except (json.JSONDecodeError, IOError, OSError) as exc:
            log.warning(
                "Could not read state file (%s). Starting fresh: %s", self.filepath, exc)
            return {"notified_job_ids": [], "api_usage": {"count": 0, "reset_month": ""}, "url_hashes": {}}

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

    def should_skip_due_to_rate_limit(self, max_requests: int = 500) -> bool:
        """Check if API usage has exceeded the safe limit."""
        usage = self.state.get("api_usage", {}).get("count", 0)
        # buffer of 20
        return usage >= (max_requests - 20)

    def get_url_hash(self, url: str) -> str:
        """Get previous hash for URL."""
        return self.state.get("url_hashes", {}).get(url)

    def set_url_hash(self, url: str, hash_val: str) -> None:
        """Save a new hash for URL."""
        hashes = self.state.get("url_hashes", {})
        hashes[url] = hash_val
        self.state["url_hashes"] = hashes

    def get_last_telegram_update_id(self) -> int:
        """Get last processed telegram update ID."""
        return self.state.get("_tg_last_update_id", 0)

    def update_last_telegram_update_id(self, update_id: int) -> None:
        """Update last processed telegram update ID."""
        self.state["_tg_last_update_id"] = max(
            self.get_last_telegram_update_id(), update_id)

    def track_api_usage(self) -> None:
        """Increment API call count for the current month."""
        from datetime import datetime, timezone

        usage = self.state.get("api_usage", {"count": 0, "reset_month": ""})
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")
        if usage.get("reset_month") != current_month:
            usage = {"count": 0, "reset_month": current_month}
        usage["count"] += 1
        self.state["api_usage"] = usage

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

            # Read current content
            with open(self.filepath, "r", encoding="utf-8") as f:
                content = f.read()

            from datetime import datetime
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            commit_message = f"chore: Update state {timestamp} [skip ci]"

            # Check if file exists in repo to get SHA
            try:
                contents = repo.get_contents(self.filepath)
                repo.update_file(contents.path, commit_message,
                                 content, contents.sha)
                log.info("Successfully updated state.json in GitHub via PyGithub")
            except GithubException as e:
                if e.status == 404:
                    # File doesn't exist yet
                    repo.create_file(self.filepath, commit_message, content)
                    log.info(
                        "Successfully created state.json in GitHub via PyGithub")
                else:
                    raise e
            return True
        except Exception as exc:
            log.error("Failed to commit state.json to GitHub: %s", exc)
            return False
