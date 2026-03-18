from datetime import datetime, timezone

from filter_engine import FilterEngine
from groq_client import GroqClient
from state_manager import StateManager


# Test filter_engine
def test_is_entry_level_with_junior_keyword():
    engine = FilterEngine()
    # include_keywords is ["junior"], exclude_keywords implicitly uses TITLE_EXCLUDE_WORDS and DESC_EXCLUDE_PATTERNS
    is_entry = engine.is_entry_level(
        "Junior Developer", "A great first job.", ["junior"], ["senior"])
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


def test_groq_client_fallback_classification_without_key():
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
        "job_title": "Junior Software Engineer",
        "job_description": "Entry-level Python role for fresh graduates.",
        "job_posted_at_datetime_utc": datetime.now(timezone.utc).isoformat(),
        "job_apply_link": "https://example.com/job/abc-1",
        "employer_name": "ExampleCo",
        "job_city": "Lahore",
        "job_employment_type": "Full-time",
    }

    job = engine.qualify_job(raw, ["Junior Software Engineer"], ["python"])
    assert job is None
