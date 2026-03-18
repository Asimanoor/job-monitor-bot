import json
import logging
import os
import time
import requests

log = logging.getLogger(__name__)


class JSearchClient:
    def __init__(
        self, api_key: str | None = None, base_url: str = "https://jsearch.p.rapidapi.com/search", timeout: int = 30
    ):
        self.api_key = api_key or os.environ.get("JSEARCH_API_KEY", "").strip()
        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()

    def get_headers(self) -> dict[str, str]:
        if not self.api_key:
            return {}
        return {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": "jsearch.p.rapidapi.com",
        }

    def search_jobs(self, query: str, location: str, page: int = 1, num_pages: int = 1) -> list[dict]:
        """Call JSearch API. Handles rate-limits and network errors via retry logic."""
        headers = self.get_headers()
        if not headers:
            log.warning("JSEARCH_API_KEY not set — skipping API search.")
            return []

        full_query = f"{query} in {location}" if location else query
        params = {"query": full_query, "page": str(
            page), "num_pages": str(num_pages)}

        return self._make_request(params, headers, full_query)

    def _make_request(self, params: dict, headers: dict, query_context: str) -> list[dict]:
        max_retries = 3

        for attempt in range(1, max_retries + 1):
            try:
                resp = self.session.get(
                    self.base_url, headers=headers,
                    params=params, timeout=self.timeout,
                )

                if resp.status_code == 429:
                    wait = 60  # rate limiting backoff
                    log.warning("Rate limited (429). Retrying in %ds… (%d/%d)",
                                wait, attempt, max_retries)
                    time.sleep(wait)
                    continue

                if resp.status_code == 403:
                    log.error(
                        "API returned 403 Forbidden — check JSEARCH_API_KEY validity.")
                    return []

                if resp.status_code >= 500:
                    wait = min(2 ** attempt, 8)
                    log.warning(
                        "API 5xx error. Retrying in %ds... (%d/%d)", wait, attempt, max_retries)
                    time.sleep(wait)
                    continue

                resp.raise_for_status()

                body = resp.json()
                data = body.get("data")
                if data is None:
                    log.warning(
                        "JSearch response has no 'data' key for '%s'", query_context)
                    return []
                if not isinstance(data, list):
                    log.warning(
                        "JSearch 'data' is not a list for '%s'", query_context)
                    return []
                return data

            except json.JSONDecodeError:
                log.error("JSearch returned invalid JSON for '%s'",
                          query_context)
                return []
            except requests.Timeout:
                log.warning(
                    "Timeout during JSearch request. Attempt %d/%d", attempt, max_retries)
            except requests.RequestException as exc:
                log.warning("JSearch RequestException for '%s': %s",
                            query_context, exc)
                if attempt == max_retries:
                    log.error(
                        "JSearch: max retries exceeded for '%s'.", query_context)

        return []
