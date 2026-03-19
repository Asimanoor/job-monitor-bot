from __future__ import annotations

from typing import Any

import mcp_scraper
from mcp_scraper import (
    CrewAICareerScraper,
    MultiStrategyCareerScraper,
    _extract_openings_from_html,
    _extract_pagination_links,
)


def _empty_result(scraper_name: str, error: str) -> dict[str, Any]:
    return {
        "ok": False,
        "scraper": scraper_name,
        "url": "https://example.com/careers",
        "final_url": "https://example.com/careers",
        "page_title": "",
        "pages_visited": [],
        "openings": [],
        "error": error,
    }


def test_multi_strategy_uses_first_success_and_skips_requests_fallback(monkeypatch):
    scraper = MultiStrategyCareerScraper(
        headless=True,
        timeout_ms=5000,
        enable_playwright=True,
        enable_langchain=True,
        enable_crewai=True,
    )

    monkeypatch.setattr(
        scraper.playwright,
        "scrape_site_openings_sync",
        lambda *_args, **_kwargs: _empty_result("playwright", "playwright failed"),
    )
    monkeypatch.setattr(
        scraper.langchain,
        "scrape_site_openings_sync",
        lambda *_args, **_kwargs: _empty_result("langchain", "langchain failed"),
    )

    crewai_result = {
        "ok": True,
        "scraper": "crewai",
        "url": "https://example.com/careers",
        "final_url": "https://example.com/careers",
        "page_title": "Example Careers",
        "pages_visited": ["https://example.com/careers"],
        "openings": [{"title": "Junior Software Engineer", "link": "https://example.com/jobs/1"}],
        "error": "",
    }
    monkeypatch.setattr(scraper.crewai, "scrape_site_openings_sync", lambda *_args, **_kwargs: crewai_result)

    fallback_calls = {"count": 0}

    def _fallback(*_args, **_kwargs):
        fallback_calls["count"] += 1
        return {
            "ok": True,
            "scraper": "requests_bs4",
            "url": "https://example.com/careers",
            "final_url": "https://example.com/careers",
            "page_title": "",
            "pages_visited": [],
            "openings": [],
            "error": "",
        }

    monkeypatch.setattr(scraper, "_requests_fallback", _fallback)

    result = scraper.scrape_site_openings_sync("https://example.com/careers", max_pages=5, max_openings=50)

    assert result["ok"] is True
    assert result["scraper"] == "crewai"
    assert fallback_calls["count"] == 0
    assert len(result.get("fallback_errors", [])) == 2
    assert result["fallback_errors"][0].startswith("playwright:")
    assert result["fallback_errors"][1].startswith("langchain:")


def test_multi_strategy_falls_back_to_requests_after_all_strategies_fail(monkeypatch):
    scraper = MultiStrategyCareerScraper(
        headless=True,
        timeout_ms=5000,
        enable_playwright=True,
        enable_langchain=True,
        enable_crewai=True,
    )

    monkeypatch.setattr(
        scraper.playwright,
        "scrape_site_openings_sync",
        lambda *_args, **_kwargs: _empty_result("playwright", "playwright fail"),
    )
    monkeypatch.setattr(
        scraper.langchain,
        "scrape_site_openings_sync",
        lambda *_args, **_kwargs: _empty_result("langchain", "langchain fail"),
    )
    monkeypatch.setattr(
        scraper.crewai,
        "scrape_site_openings_sync",
        lambda *_args, **_kwargs: _empty_result("crewai", "crewai fail"),
    )

    fallback_calls = {"count": 0}

    def _fallback(*_args, **_kwargs):
        fallback_calls["count"] += 1
        return {
            "ok": True,
            "scraper": "requests_bs4",
            "url": "https://example.com/careers",
            "final_url": "https://example.com/careers",
            "page_title": "",
            "pages_visited": ["https://example.com/careers"],
            "openings": [{"title": "Graduate QA Engineer", "link": "https://example.com/jobs/2"}],
            "error": "",
        }

    monkeypatch.setattr(scraper, "_requests_fallback", _fallback)

    result = scraper.scrape_site_openings_sync("https://example.com/careers", max_pages=4, max_openings=20)

    assert result["ok"] is True
    assert result["scraper"] == "requests_bs4"
    assert fallback_calls["count"] == 1
    assert len(result.get("fallback_errors", [])) == 3


def test_multi_strategy_collects_raised_exceptions_and_sets_combined_error(monkeypatch):
    scraper = MultiStrategyCareerScraper(
        headless=True,
        timeout_ms=5000,
        enable_playwright=True,
        enable_langchain=True,
        enable_crewai=True,
    )

    def _raise_playwright(*_args, **_kwargs):
        raise RuntimeError("playwright exploded")

    monkeypatch.setattr(scraper.playwright, "scrape_site_openings_sync", _raise_playwright)
    monkeypatch.setattr(
        scraper.langchain,
        "scrape_site_openings_sync",
        lambda *_args, **_kwargs: _empty_result("langchain", "langchain fail"),
    )
    monkeypatch.setattr(
        scraper.crewai,
        "scrape_site_openings_sync",
        lambda *_args, **_kwargs: _empty_result("crewai", "crewai fail"),
    )

    def _fallback(*_args, **_kwargs):
        return {
            "ok": False,
            "scraper": "requests_bs4",
            "url": "https://example.com/careers",
            "final_url": "https://example.com/careers",
            "page_title": "",
            "pages_visited": [],
            "openings": [],
            "error": "",
        }

    monkeypatch.setattr(scraper, "_requests_fallback", _fallback)

    result = scraper.scrape_site_openings_sync("https://example.com/careers", max_pages=2, max_openings=10)

    assert result["ok"] is False
    assert result["scraper"] == "requests_bs4"
    assert len(result.get("fallback_errors", [])) == 3
    assert any("playwright exploded" in entry for entry in result["fallback_errors"])
    assert "playwright:" in result["error"]
    assert "langchain:" in result["error"]
    assert "crewai:" in result["error"]


def test_multi_strategy_with_all_engines_disabled_uses_requests_only(monkeypatch):
    scraper = MultiStrategyCareerScraper(
        headless=True,
        timeout_ms=5000,
        enable_playwright=False,
        enable_langchain=False,
        enable_crewai=False,
    )

    fallback_calls = {"count": 0}

    def _fallback(*_args, **_kwargs):
        fallback_calls["count"] += 1
        return {
            "ok": True,
            "scraper": "requests_bs4",
            "url": "https://example.com/careers",
            "final_url": "https://example.com/careers",
            "page_title": "Careers",
            "pages_visited": ["https://example.com/careers"],
            "openings": [{"title": "Associate Backend Engineer", "link": "https://example.com/jobs/3"}],
            "error": "",
        }

    monkeypatch.setattr(scraper, "_requests_fallback", _fallback)

    result = scraper.scrape_site_openings_sync("https://example.com/careers", max_pages=3, max_openings=10)

    assert result["ok"] is True
    assert result["scraper"] == "requests_bs4"
    assert fallback_calls["count"] == 1
    assert result.get("fallback_errors") == []


def test_extract_pagination_links_keeps_same_domain_and_deduplicates():
    html = """
    <html><body>
      <a href="/careers?page=2">Next</a>
      <a href="/careers?page=2">2</a>
      <a rel="next" href="/careers?page=3">Continue</a>
      <a href="https://external.example/jobs?page=9">Next</a>
      <a href="/about">About us</a>
    </body></html>
    """

    links = _extract_pagination_links("https://example.com/careers", html)

    assert links == [
        "https://example.com/careers?page=2",
        "https://example.com/careers?page=3",
    ]


def test_extract_pagination_links_detects_offset_and_jobs_path_but_skips_invalid_hrefs():
        html = """
        <html><body>
            <a href="/jobs">All jobs</a>
            <a href="/careers?offset=25">Go</a>
            <a href="mailto:careers@example.com">Email</a>
            <a href="javascript:void(0)">Load</a>
            <a href="#section">Section</a>
        </body></html>
        """

        links = _extract_pagination_links("https://example.com/careers", html)

        assert links == [
                "https://example.com/jobs",
                "https://example.com/careers?offset=25",
        ]


def test_extract_pagination_links_allows_known_ats_domain_but_ignores_unrelated_external():
        html = """
        <html><body>
            <a href="https://jobs.lever.co/acme?page=2">Next</a>
            <a href="https://unrelated.example.org/jobs?page=3">Next</a>
        </body></html>
        """

        links = _extract_pagination_links("https://acme.com/careers", html)

        assert links == [
                "https://jobs.lever.co/acme?page=2",
        ]


def test_extract_openings_from_html_handles_jsonld_graph_and_ignores_noise():
        html = """
        <html>
            <head>
                <title>Example Careers</title>
                <script type="application/ld+json">
                    {
                        "@context": "https://schema.org",
                        "@graph": [
                            {
                                "@type": ["Thing", "JobPosting"],
                                "title": "Junior Platform Engineer",
                                "url": "/jobs/junior-platform"
                            }
                        ]
                    }
                </script>
            </head>
            <body>
                <a href="/jobs/junior-platform">Junior Platform Engineer</a>
                <a href="/jobs/grad-data">Graduate Data Analyst</a>
                <a href="/about">About us</a>
            </body>
        </html>
        """

        title, openings = _extract_openings_from_html("https://example.com/careers", html, max_openings=10)

        assert title == "Example Careers"
        assert openings[0] == {
                "title": "Junior Platform Engineer",
                "link": "https://example.com/jobs/junior-platform",
        }
        assert {item["title"] for item in openings} == {
                "Junior Platform Engineer",
                "Graduate Data Analyst",
        }
        assert len(openings) == 2


def test_crewai_deterministic_crawl_follows_pagination_and_dedups_openings(monkeypatch):
    pages = {
        "https://example.com/careers": """
            <html><head><title>Careers</title></head><body>
              <a href='/jobs/1'>Junior Software Engineer</a>
              <a href='/careers?page=2' rel='next'>Next</a>
            </body></html>
        """,
        "https://example.com/careers?page=2": """
            <html><body>
              <a href='/jobs/1'>Junior Software Engineer</a>
              <a href='/jobs/2'>Graduate Data Analyst</a>
            </body></html>
        """,
    }

    class FakeResponse:
        def __init__(self, url: str, text: str, status_code: int = 200):
            self.url = url
            self.text = text
            self.status_code = status_code

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f"HTTP {self.status_code}")

    class FakeSession:
        def __init__(self):
            self.calls: list[str] = []

        def get(self, target: str, headers=None, timeout=None, allow_redirects=True):
            self.calls.append(target)
            html = pages.get(target)
            if html is None:
                return FakeResponse(target, "", 404)
            return FakeResponse(target, html, 200)

    fake_session = FakeSession()
    monkeypatch.setattr(mcp_scraper.requests, "Session", lambda: fake_session)

    scraper = CrewAICareerScraper(timeout_seconds=5)
    monkeypatch.setattr(scraper, "_crewai_normalize", lambda openings: openings)

    result = scraper.scrape_site_openings_sync("https://example.com/careers", max_pages=5, max_openings=10)

    assert result["ok"] is True
    assert result["pages_visited"] == [
        "https://example.com/careers",
        "https://example.com/careers?page=2",
    ]

    titles = {item["title"] for item in result["openings"]}
    assert titles == {"Junior Software Engineer", "Graduate Data Analyst"}
    assert len(result["openings"]) == 2
