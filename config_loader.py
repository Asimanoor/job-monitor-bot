import json
import logging
import os
from pathlib import Path

log = logging.getLogger(__name__)


class ConfigLoader:
    def __init__(self, config_path: str = "config.json"):
        self.config_path = Path(config_path)
        self.config = self._load_defaults()
        self._load_from_file()
        self._load_from_env()

    def _load_defaults(self) -> dict:
        return {
            "request_timeout": 15,
            "fuzzy_match_threshold": 70,
            "jsearch_base_url": "https://jsearch.p.rapidapi.com/search",
            "search_locations": ["Pakistan", "Lahore", "Remote"],
            "job_max_age_days": 7,
            "max_notified_ids": 5000,
            "telegram_max_len": 4000,
            "ai_confidence_threshold": 55,
            "jsearch_monthly_limit": 200,
            "jsearch_safety_buffer": 10,
            "jsearch_max_queries_per_run": 3,
            "company_targeted_search_enabled": True,
            "company_targeted_max_companies": 90,
            "company_targeted_max_queries_per_run": 4,
            "groq_daily_limit": 500,
            "groq_safety_buffer": 50,
            "groq_min_call_interval_seconds": 1.2,
            "enable_playwright_scraper": True,
            "enable_langchain_scraper": True,
            "enable_crewai_scraper": True,
            "playwright_headless": True,
            "playwright_timeout_seconds": 30,
            "playwright_max_openings_per_page": 80,
            "link_scraper_max_openings_per_site": 300,
            "link_scraper_max_pages": 8,
        }

    def _load_from_file(self) -> None:
        if not self.config_path.is_file():
            log.info("Config file '%s' not found, using defaults.",
                     self.config_path)
            try:
                with open(self.config_path, "w", encoding="utf-8") as f:
                    json.dump(self.config, f, indent=4)
            except OSError as e:
                log.warning("Could not create default config file: %s", e)
            return

        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                user_config = json.load(f)
                if isinstance(user_config, dict):
                    self.config.update(user_config)
        except (json.JSONDecodeError, OSError) as e:
            log.error("Failed to read config file '%s': %s",
                      self.config_path, e)

    def _load_from_env(self) -> None:
        """Override config with matching environment variables."""
        if "REQUEST_TIMEOUT" in os.environ:
            try:
                self.config["request_timeout"] = int(
                    os.environ["REQUEST_TIMEOUT"])
            except ValueError:
                pass
        if "SEARCH_LOCATIONS" in os.environ:
            self.config["search_locations"] = [loc.strip()
                                               for loc in os.environ["SEARCH_LOCATIONS"].split(",")]
        if "JOB_MAX_AGE_DAYS" in os.environ:
            try:
                self.config["job_max_age_days"] = int(
                    os.environ["JOB_MAX_AGE_DAYS"])
            except ValueError:
                pass
        if "AI_CONFIDENCE_THRESHOLD" in os.environ:
            try:
                self.config["ai_confidence_threshold"] = int(os.environ["AI_CONFIDENCE_THRESHOLD"])
            except ValueError:
                pass
        if "JSEARCH_MONTHLY_LIMIT" in os.environ:
            try:
                self.config["jsearch_monthly_limit"] = int(os.environ["JSEARCH_MONTHLY_LIMIT"])
            except ValueError:
                pass
        if "JSEARCH_SAFETY_BUFFER" in os.environ:
            try:
                self.config["jsearch_safety_buffer"] = int(os.environ["JSEARCH_SAFETY_BUFFER"])
            except ValueError:
                pass
        if "JSEARCH_MAX_QUERIES_PER_RUN" in os.environ:
            try:
                self.config["jsearch_max_queries_per_run"] = int(os.environ["JSEARCH_MAX_QUERIES_PER_RUN"])
            except ValueError:
                pass
        if "COMPANY_TARGETED_SEARCH_ENABLED" in os.environ:
            self.config["company_targeted_search_enabled"] = os.environ["COMPANY_TARGETED_SEARCH_ENABLED"].strip().lower() in {
                "1", "true", "yes", "on"
            }
        if "COMPANY_TARGETED_MAX_COMPANIES" in os.environ:
            try:
                self.config["company_targeted_max_companies"] = int(os.environ["COMPANY_TARGETED_MAX_COMPANIES"])
            except ValueError:
                pass
        if "COMPANY_TARGETED_MAX_QUERIES_PER_RUN" in os.environ:
            try:
                self.config["company_targeted_max_queries_per_run"] = int(os.environ["COMPANY_TARGETED_MAX_QUERIES_PER_RUN"])
            except ValueError:
                pass
        if "GROQ_DAILY_LIMIT" in os.environ:
            try:
                self.config["groq_daily_limit"] = int(os.environ["GROQ_DAILY_LIMIT"])
            except ValueError:
                pass
        if "GROQ_SAFETY_BUFFER" in os.environ:
            try:
                self.config["groq_safety_buffer"] = int(os.environ["GROQ_SAFETY_BUFFER"])
            except ValueError:
                pass
        if "GROQ_MIN_CALL_INTERVAL_SECONDS" in os.environ:
            try:
                self.config["groq_min_call_interval_seconds"] = float(os.environ["GROQ_MIN_CALL_INTERVAL_SECONDS"])
            except ValueError:
                pass
        if "ENABLE_PLAYWRIGHT_SCRAPER" in os.environ:
            self.config["enable_playwright_scraper"] = os.environ["ENABLE_PLAYWRIGHT_SCRAPER"].strip().lower() in {
                "1", "true", "yes", "on"
            }
        if "ENABLE_LANGCHAIN_SCRAPER" in os.environ:
            self.config["enable_langchain_scraper"] = os.environ["ENABLE_LANGCHAIN_SCRAPER"].strip().lower() in {
                "1", "true", "yes", "on"
            }
        if "ENABLE_CREWAI_SCRAPER" in os.environ:
            self.config["enable_crewai_scraper"] = os.environ["ENABLE_CREWAI_SCRAPER"].strip().lower() in {
                "1", "true", "yes", "on"
            }
        if "PLAYWRIGHT_HEADLESS" in os.environ:
            self.config["playwright_headless"] = os.environ["PLAYWRIGHT_HEADLESS"].strip().lower() in {
                "1", "true", "yes", "on"
            }
        if "PLAYWRIGHT_TIMEOUT_SECONDS" in os.environ:
            try:
                self.config["playwright_timeout_seconds"] = int(os.environ["PLAYWRIGHT_TIMEOUT_SECONDS"])
            except ValueError:
                pass
        if "PLAYWRIGHT_MAX_OPENINGS_PER_PAGE" in os.environ:
            try:
                self.config["playwright_max_openings_per_page"] = int(os.environ["PLAYWRIGHT_MAX_OPENINGS_PER_PAGE"])
            except ValueError:
                pass
        if "LINK_SCRAPER_MAX_OPENINGS_PER_SITE" in os.environ:
            try:
                self.config["link_scraper_max_openings_per_site"] = int(os.environ["LINK_SCRAPER_MAX_OPENINGS_PER_SITE"])
            except ValueError:
                pass
        if "LINK_SCRAPER_MAX_PAGES" in os.environ:
            try:
                self.config["link_scraper_max_pages"] = int(os.environ["LINK_SCRAPER_MAX_PAGES"])
            except ValueError:
                pass

    def get(self, key: str, default=None):
        return self.config.get(key, default)

    @classmethod
    def load_lines(cls, filepath: str) -> list[str]:
        """Read non-empty lines from a text file, skipping comments."""
        path = Path(filepath)
        if not path.is_file():
            log.warning("File not found: %s", filepath)
            return []

        lines: list[str] = []
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    stripped = line.strip()
                    if not stripped or stripped == "." or stripped.startswith("#"):
                        continue
                    lines.append(stripped)
        except OSError as e:
            log.error("Error reading %s: %s", filepath, e)
        return lines

    @classmethod
    def load_job_titles(cls, filepath: str = "jobs.txt") -> list[str]:
        titles = cls.load_lines(filepath)
        seen: set[str] = set()
        unique: list[str] = []
        for t in titles:
            key = t.lower()
            if key not in seen:
                seen.add(key)
                unique.append(t)
        log.info("Loaded %d unique job titles from %s", len(unique), filepath)
        return unique

    @classmethod
    def load_filters(cls, filepath: str = "filters.txt") -> list[str]:
        filters = cls.load_lines(filepath)
        log.info("Loaded %d filter keywords from %s", len(filters), filepath)
        return filters

    @classmethod
    def load_urls(cls, filepath: str = "links.txt") -> list[str]:
        raw = cls.load_lines(filepath)
        urls: list[str] = []
        for line in raw:
            if line.startswith(("http://", "https://")):
                urls.append(line)
            else:
                log.warning("Skipping invalid URL: %s", line)
        log.info("Loaded %d URLs from %s", len(urls), filepath)
        return urls
