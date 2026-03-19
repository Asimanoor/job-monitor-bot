import json
import logging
import os
import time
import requests

log = logging.getLogger(__name__)


class JSearchRateLimitError(Exception):
    """Raised when JSearch responds with 429 and fail-fast mode is enabled."""


class JSearchClient:
    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = "https://jsearch.p.rapidapi.com/search",
        timeout: int = 30,
        fail_fast_on_429: bool = True,
        rate_limit_cooldown_seconds: int = 900,
        max_retries: int = 1,
    ):
        self.api_key = api_key or os.environ.get("JSEARCH_API_KEY", "").strip()
        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()
        self.fail_fast_on_429 = bool(fail_fast_on_429)
        self.rate_limit_cooldown_seconds = max(30, int(rate_limit_cooldown_seconds))
        self.max_retries = max(1, int(max_retries))
        self._rate_limited_until = 0.0

    def is_temporarily_rate_limited(self) -> bool:
        """Return True if client is in cooldown window due to recent 429."""
        return time.time() < float(self._rate_limited_until)

    def remaining_rate_limit_cooldown(self) -> int:
        """Return remaining cooldown seconds (0 if not rate-limited)."""
        return max(0, int(self._rate_limited_until - time.time()))

    @staticmethod
    def _parse_retry_after_seconds(resp: requests.Response, fallback_seconds: int) -> int:
        retry_after = str(resp.headers.get("Retry-After", "")).strip()
        if retry_after.isdigit():
            return max(1, int(retry_after))
        return max(1, int(fallback_seconds))

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

        if self.is_temporarily_rate_limited():
            remaining = self.remaining_rate_limit_cooldown()
            log.warning(
                "Skipping JSearch query during active 429 cooldown (%ds remaining): %s",
                remaining,
                query,
            )
            return []

        full_query = f"{query} in {location}" if location else query
        params = {"query": full_query, "page": str(
            page), "num_pages": str(num_pages)}

        return self._make_request(params, headers, full_query)

    def _make_request(self, params: dict, headers: dict, query_context: str) -> list[dict]:
        max_retries = self.max_retries

        for attempt in range(1, max_retries + 1):
            try:
                resp = self.session.get(
                    self.base_url, headers=headers,
                    params=params, timeout=self.timeout,
                )

                if resp.status_code == 429:
                    wait = self._parse_retry_after_seconds(resp, self.rate_limit_cooldown_seconds)
                    self._rate_limited_until = max(self._rate_limited_until, time.time() + wait)

                    if self.fail_fast_on_429:
                        log.error(
                            "Rate limited (429) for '%s'. Entering cooldown for %ds and skipping remaining retries.",
                            query_context,
                            wait,
                        )
                        raise JSearchRateLimitError(f"429 rate limit for '{query_context}'")

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
