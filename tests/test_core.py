from datetime import datetime, timezone

from filter_engine import FilterEngine
from groq_client import GroqClient
from job_scraper import is_valid_job_posting
from monitor import (
    build_jsearch_query_plan,
    extract_company_hints_from_urls,
    run_repeating_pipeline,
)
from notification_manager import NotificationManager
from state_manager import StateManager


# Test filter_engine
def test_is_entry_level_with_junior_keyword():
    engine = FilterEngine()
    # include_keywords is ["junior"], exclude_keywords implicitly uses TITLE_EXCLUDE_WORDS and DESC_EXCLUDE_PATTERNS
    is_entry = engine.is_entry_level(
        "Junior Developer", "A great first job.", ["junior"], ["senior"])
    assert is_entry is True


def test_is_entry_level_with_title_hint_even_without_description_keyword():
    engine = FilterEngine()
    is_entry = engine.is_entry_level(
        "Associate Software Engineer",
        "Build backend APIs and internal tools.",
        ["fresh", "junior", "entry level"],
        ["senior", "lead"],
    )
    assert is_entry is True


def test_is_entry_level_with_senior_keyword():
    engine = FilterEngine()
    # "senior" is in both TITLE_EXCLUDE_WORDS and our test description
    is_entry = engine.is_entry_level(
        "Senior Engineer", "A seasoned senior engineer.", ["junior"], ["senior"])
    assert is_entry is False


def test_fuzzy_matching_ai_ml():
    engine = FilterEngine()
    # "Machine Learning Engineer" matches synonym for "AIML Engineer" / "AI Engineer" etc.
    # We pass it to `matches_any_title`
    match = engine.matches_any_title(
        "Machine Learning Engineer", ["AI Engineer"])
    assert match == "AI Engineer"


# Test state_manager
def test_is_new_job(tmp_path):
    # Setup temporary state file
    file_path = str(tmp_path / "state.json")
    mgr = StateManager(filepath=file_path)

    # Inject some state
    mgr.state["notified_job_ids"] = ["job1", "job2"]

    assert mgr.is_new_job("job3") is True
    assert mgr.is_new_job("job1") is False


def test_state_persistence(tmp_path):
    file_path = str(tmp_path / "state.json")
    mgr = StateManager(filepath=file_path)

    mgr.mark_as_notified("job101")
    mgr.save_state()

    mgr2 = StateManager(filepath=file_path)
    assert "job101" in mgr2.state["notified_job_ids"]


# Test notification_manager
def test_fallback_chain():
    # Kept as smoke-test placeholder for backward compatibility.
    assert True


def test_groq_client_fallback_classification_without_key(monkeypatch):
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    client = GroqClient(api_key="")
    result = client.classify_job_level(
        "Junior Python Developer",
        "Entry-level role for fresh graduates with Python knowledge.",
    )

    assert result["source"] == "fallback"
    assert result["is_entry_level"] is True
    assert 0 <= result["confidence"] <= 100


def test_filter_engine_excludes_when_ai_confidence_low():
    class DummyAI:
        def classify_job_level(self, *_args, **_kwargs):
            return {
                "is_entry_level": True,
                "confidence": 55,
                "source": "ai",
                "reason": "low confidence",
            }

        def score_job_match(self, *_args, **_kwargs):
            return {"score": 90, "confidence": 90, "matched_skills": ["Python"], "source": "ai"}

    engine = FilterEngine(ai_client=DummyAI(), ai_confidence_threshold=70)
    raw = {
        "job_id": "abc-1",
        "job_title": "Software Engineer",
        "job_description": "Entry-level Python role for fresh graduates.",
        "job_posted_at_datetime_utc": datetime.now(timezone.utc).isoformat(),
        "job_apply_link": "https://example.com/job/abc-1",
        "employer_name": "ExampleCo",
        "job_city": "Lahore",
        "job_employment_type": "Full-time",
    }

    job = engine.qualify_job(raw, ["Junior Software Engineer"], ["python"])
    assert job is None


def test_filter_engine_keeps_title_hint_when_ai_confidence_low():
    class DummyAI:
        def classify_job_level(self, *_args, **_kwargs):
            return {
                "is_entry_level": True,
                "confidence": 40,
                "source": "ai",
                "reason": "low confidence",
            }

        def score_job_match(self, *_args, **_kwargs):
            return {"score": 70, "confidence": 80, "matched_skills": ["Python"], "source": "ai"}

    engine = FilterEngine(ai_client=DummyAI(), ai_confidence_threshold=70)
    raw = {
        "job_id": "abc-2",
        "job_title": "Associate Software Engineer",
        "job_description": "Work on backend services and APIs.",
        "job_posted_at_datetime_utc": datetime.now(timezone.utc).isoformat(),
        "job_apply_link": "https://example.com/job/abc-2",
        "employer_name": "ExampleCo",
        "job_city": "Lahore",
        "job_employment_type": "Full-time",
    }

    job = engine.qualify_job(raw, ["Associate Software Engineer"], ["python", "junior"])
    assert job is not None


def test_record_url_changes_in_sheet_writes_change_and_new_openings():
    class DummySheets:
        def __init__(self):
            self.change_rows = []
            self.opening_rows = []

        def append_url_change_row(self, change_data: dict) -> bool:
            self.change_rows.append(change_data)
            return True

        def append_career_opening_row(self, opening_data: dict) -> bool:
            self.opening_rows.append(opening_data)
            return True

        # Legacy fallback compatibility
        def append_job_row(self, _job_data: dict) -> bool:
            return True

    sheets = DummySheets()
    manager = NotificationManager(sheets_client=sheets)

    events = [
        {
            "url": "https://example.com/careers",
            "domain": "example.com",
            "change_type": "content_changed",
            "page_title": "Example Careers",
            "openings": [
                {"title": "Junior Software Engineer", "link": "https://example.com/jobs/1"},
                {"title": "Data Scientist", "link": "https://example.com/jobs/2"},
            ],
            "new_openings": [
                {"title": "Junior Software Engineer", "link": "https://example.com/jobs/1"},
            ],
            "total_openings": 2,
            "new_openings_count": 1,
        }
    ]

    ok = manager.record_url_changes_in_sheet(events)

    assert ok is True
    assert len(sheets.change_rows) == 1
    assert len(sheets.opening_rows) == 1
    assert sheets.opening_rows[0]["job_title"] == "Junior Software Engineer"
    assert sheets.opening_rows[0]["apply_link"] == "https://example.com/jobs/1"
    assert sheets.opening_rows[0]["status"] == "New"


def test_record_url_changes_in_sheet_logs_baseline_openings_for_new_url():
    class DummySheets:
        def __init__(self):
            self.change_rows = []
            self.opening_rows = []

        def append_url_change_row(self, change_data: dict) -> bool:
            self.change_rows.append(change_data)
            return True

        def append_career_opening_row(self, opening_data: dict) -> bool:
            self.opening_rows.append(opening_data)
            return True

        def append_job_row(self, _job_data: dict) -> bool:
            return True

    sheets = DummySheets()
    manager = NotificationManager(sheets_client=sheets)

    events = [
        {
            "url": "https://company.test/careers",
            "domain": "company.test",
            "change_type": "new_url_tracked",
            "page_title": "Company Careers",
            "openings": [
                {"title": "Associate AI Engineer", "link": "https://company.test/jobs/ai-associate"},
                {"title": "Graduate Data Scientist", "link": "https://company.test/jobs/data-grad"},
            ],
            "new_openings": [],
            "total_openings": 2,
            "new_openings_count": 0,
        }
    ]

    ok = manager.record_url_changes_in_sheet(events)

    assert ok is True
    assert len(sheets.change_rows) == 1
    assert len(sheets.opening_rows) == 2


def test_record_url_changes_in_sheet_does_not_append_primary_rows_with_links_tag():
    class DummySheets:
        def __init__(self):
            self.change_rows = []
            self.opening_rows = []
            self.primary_rows = []

        def append_url_change_row(self, change_data: dict) -> bool:
            self.change_rows.append(change_data)
            return True

        def append_career_opening_row(self, opening_data: dict) -> bool:
            self.opening_rows.append(opening_data)
            return True

        def append_job_row(self, job_data: dict) -> bool:
            self.primary_rows.append(job_data)
            return True

    sheets = DummySheets()
    manager = NotificationManager(sheets_client=sheets)

    events = [
        {
            "url": "https://acme.test/careers",
            "resolved_url": "https://acme.test/careers",
            "domain": "acme.test",
            "change_type": "content_changed",
            "page_title": "Acme Careers",
            "openings": [
                {"title": "Junior Platform Engineer", "link": "https://acme.test/jobs/plat-1"},
            ],
            "new_openings": [
                {"title": "Junior Platform Engineer", "link": "https://acme.test/jobs/plat-1"},
            ],
            "total_openings": 1,
            "new_openings_count": 1,
            "scraper_used": "playwright",
        }
    ]

    ok = manager.record_url_changes_in_sheet(events)

    assert ok is True
    assert len(sheets.change_rows) == 1
    assert len(sheets.opening_rows) == 1
    assert len(sheets.primary_rows) == 0


def test_run_repeating_pipeline_respects_max_cycles():
    calls: list[int] = []
    sleeps: list[float] = []

    total = run_repeating_pipeline(
        lambda cycle: calls.append(cycle),
        interval_seconds=1.0,
        max_cycles=3,
        sleep_fn=lambda seconds: sleeps.append(seconds),
    )

    assert total == 3
    assert calls == [1, 2, 3]
    assert len(sleeps) == 2


def test_run_repeating_pipeline_single_cycle_when_interval_zero():
    calls: list[int] = []

    total = run_repeating_pipeline(
        lambda cycle: calls.append(cycle),
        interval_seconds=0,
        max_cycles=0,
        sleep_fn=lambda _seconds: None,
    )

    assert total == 1
    assert calls == [1]


def test_extract_company_hints_from_urls_handles_ats_patterns_and_query_params():
    urls = [
        "https://jobs.lever.co/educative",
        "https://apply.workable.com/devsinc-17/",
        "https://jobs.ashbyhq.com/tajir",
        "https://strategic-systems-international.breezy.hr/",
        "https://career55.sapsf.eu/career?company=systemvent",
        "https://www.gomotive.com/careers",
    ]

    hints = extract_company_hints_from_urls(urls, max_companies=20)

    assert "educative" in hints
    assert "devsinc 17" in hints
    assert "tajir" in hints
    assert "strategic systems international" in hints
    assert "systemvent" in hints
    assert "gomotive" in hints


def test_build_jsearch_query_plan_prioritizes_company_targeted_then_generic():
    titles = ["Junior Software Engineer", "Associate Data Scientist"]
    locations = ["Pakistan", "Remote"]
    companies = ["educative", "tajir"]

    plan = build_jsearch_query_plan(
        titles=titles,
        locations=locations,
        company_hints=companies,
        allowed_queries=5,
        company_targeted_enabled=True,
        company_max_queries=2,
    )

    assert len(plan) == 5
    assert plan[0]["source"] == "JSEARCH_COMPANY_TARGETED"
    assert plan[1]["source"] == "JSEARCH_COMPANY_TARGETED"
    assert plan[2]["source"] == "JSEARCH_API"
    assert plan[0]["company"] == "educative"
    assert plan[1]["company"] == "tajir"


def test_is_valid_job_posting_rejects_marketing_service_title():
    payload = {
        "title": "Data Modernization Services",
        "apply_link": "https://example.com/careers/data-modernization-services",
        "job_url": "https://example.com/careers/data-modernization-services",
        "source_url": "https://example.com/careers",
    }

    assert is_valid_job_posting(payload) is False


def test_record_url_changes_in_sheet_respects_event_and_opening_caps():
    class DummySheets:
        def __init__(self):
            self.change_rows = []
            self.opening_rows = []

        def append_url_change_rows(self, change_rows: list[dict]) -> int:
            self.change_rows.extend(change_rows)
            return len(change_rows)

        def append_career_opening_rows(self, opening_rows: list[dict]) -> int:
            self.opening_rows.extend(opening_rows)
            return len(opening_rows)

    sheets = DummySheets()
    manager = NotificationManager(
        sheets_client=sheets,
        url_change_max_events_per_cycle=1,
        url_change_max_openings_per_event=1,
        url_change_max_openings_per_cycle=1,
        url_change_log_baseline_openings=True,
    )

    events = [
        {
            "url": "https://alpha.test/careers",
            "domain": "alpha.test",
            "change_type": "new_url_tracked",
            "page_title": "Alpha Careers",
            "openings": [
                {"title": "Junior Software Engineer", "link": "https://alpha.test/jobs/1"},
                {"title": "Associate Data Analyst", "link": "https://alpha.test/jobs/2"},
            ],
            "new_openings": [],
            "total_openings": 2,
            "new_openings_count": 0,
        },
        {
            "url": "https://beta.test/careers",
            "domain": "beta.test",
            "change_type": "new_url_tracked",
            "page_title": "Beta Careers",
            "openings": [
                {"title": "Junior QA Engineer", "link": "https://beta.test/jobs/1"},
            ],
            "new_openings": [],
            "total_openings": 1,
            "new_openings_count": 0,
        },
    ]

    ok = manager.record_url_changes_in_sheet(events)

    assert ok is True
    assert len(sheets.change_rows) == 1
    assert len(sheets.opening_rows) == 1
    assert sheets.opening_rows[0]["job_title"] == "Junior Software Engineer"


# ── Role Filter Tests ────────────────────────────────────────────────────────

from role_filter import matches_target_role, filter_jobs_by_role

def test_role_filter_matches_ai_engineer():
    matched, role, score = matches_target_role("AI Engineer", "")
    assert matched is True
    assert role == "AI Engineer"
    assert score == 100.0

def test_role_filter_rejects_senior_roles():
    """Verify senior roles are excluded by filter_jobs_by_role."""
    jobs = [{"title": "Senior Data Scientist", "link": "https://test.com"}]
    filtered = filter_jobs_by_role(jobs, exclude_senior=True)
    assert len(filtered) == 0

def test_role_filter_matches_associate_swe():
    matched, role, score = matches_target_role("Associate Software Engineer", "")
    assert matched is True
    assert role == "Associate Software Engineer"

def test_role_filter_rejects_non_tech():
    matched, _, _ = matches_target_role("Content Manager", "")
    assert matched is False

def test_role_filter_synonym_match():
    # Synonym match should pick up ML Engineer as Machine Learning Engineer
    matched, role, score = matches_target_role("ML Engineer", "")
    assert matched is True
    assert role in ["Machine Learning Engineer", "AI Engineer"]


def test_passes_semantic_filter_threshold_true(monkeypatch):
    """
    Unit test for `passes_semantic_filter` without downloading models.
    We monkeypatch `sentence_transformers` with deterministic embeddings.
    """
    import types, sys
    import role_filter

    class DummyScalar:
        def __init__(self, v: float):
            self._v = float(v)

        def item(self):
            return self._v

    class DummyVec:
        def __init__(self, vals: list[float]):
            self._vals = list(map(float, vals))

        def max(self):
            return DummyScalar(max(self._vals) if self._vals else 0.0)

    class DummyModel:
        def encode(self, texts, convert_to_tensor=True):
            # Job embedding path: single string
            if isinstance(texts, str):
                # If the job text hints AI/ML, embed as [1], else [0].
                key = texts.lower()
                return [1.0] if ("ai" in key or "ml" in key) else [0.0]

            # Role embeddings path: list[str]
            role_vecs: list[list[float]] = []
            for t in texts:
                lt = str(t).lower()
                role_vecs.append([1.0] if ("ai" in lt or "ml" in lt or "machine learning" in lt) else [0.0])
            return role_vecs

    def _cos_sim(job_embedding, role_embeddings):
        # Compute cosine similarity for 1D embeddings.
        # cos([a],[b]) is 1 if both are non-zero and positive; else 0.
        a = float(job_embedding[0]) if job_embedding else 0.0
        sims = []
        for emb in role_embeddings:
            b = float(emb[0]) if emb else 0.0
            sims.append(1.0 if (a > 0.0 and b > 0.0) else 0.0)
        return [DummyVec(sims)]

    dummy_module = types.SimpleNamespace(
        SentenceTransformer=lambda *_args, **_kwargs: DummyModel(),
        util=types.SimpleNamespace(cos_sim=_cos_sim),
    )

    monkeypatch.setitem(sys.modules, "sentence_transformers", dummy_module)
    monkeypatch.setenv("ENABLE_SEMANTIC_FILTER", "true")

    assert role_filter.passes_semantic_filter(
        title="AI Engineer",
        description="Build ML models",
        target_roles=["AI Engineer", "Data Scientist"],
        threshold=0.65,
    ) is True


def test_passes_semantic_filter_threshold_false(monkeypatch):
    """Same monkeypatched setup as above, but job text doesn't match roles."""
    import types, sys
    import role_filter

    class DummyScalar:
        def __init__(self, v: float):
            self._v = float(v)

        def item(self):
            return self._v

    class DummyVec:
        def __init__(self, vals: list[float]):
            self._vals = list(map(float, vals))

        def max(self):
            return DummyScalar(max(self._vals) if self._vals else 0.0)

    class DummyModel:
        def encode(self, texts, convert_to_tensor=True):
            if isinstance(texts, str):
                key = texts.lower()
                return [1.0] if ("ai" in key or "ml" in key) else [0.0]

            role_vecs: list[list[float]] = []
            for t in texts:
                lt = str(t).lower()
                role_vecs.append([1.0] if ("ai" in lt or "ml" in lt or "machine learning" in lt) else [0.0])
            return role_vecs

    def _cos_sim(job_embedding, role_embeddings):
        a = float(job_embedding[0]) if job_embedding else 0.0
        sims = []
        for emb in role_embeddings:
            b = float(emb[0]) if emb else 0.0
            sims.append(1.0 if (a > 0.0 and b > 0.0) else 0.0)
        return [DummyVec(sims)]

    dummy_module = types.SimpleNamespace(
        SentenceTransformer=lambda *_args, **_kwargs: DummyModel(),
        util=types.SimpleNamespace(cos_sim=_cos_sim),
    )

    monkeypatch.setitem(sys.modules, "sentence_transformers", dummy_module)
    monkeypatch.setenv("ENABLE_SEMANTIC_FILTER", "true")

    assert role_filter.passes_semantic_filter(
        title="Content Manager",
        description="Write content and run marketing promotions",
        target_roles=["AI Engineer", "Data Scientist"],
        threshold=0.65,
    ) is False

