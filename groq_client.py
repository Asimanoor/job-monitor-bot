"""
GROQ AI Client
──────────────
Provides guarded, rate-limited helpers for:
  - Entry-level classification
  - Job match scoring
  - Job summarization
  - Cover-letter talking points

All methods gracefully fall back to deterministic keyword heuristics when:
  - GROQ_API_KEY is missing
  - groq package is unavailable
  - API request fails / returns malformed output
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any

log = logging.getLogger(__name__)

try:
    from groq import Groq
except Exception:  # pragma: no cover - handled by fallback path
    Groq = None

DEFAULT_MODEL = "llama-3.1-8b-instant"
_MIN_CALL_INTERVAL_SECONDS = 1.2


class GroqClient:
    """Rate-limited AI helper with robust fallback behavior."""

    def __init__(
        self,
        api_key: str | None = None,
        model: str = DEFAULT_MODEL,
        min_call_interval_seconds: float = _MIN_CALL_INTERVAL_SECONDS,
    ) -> None:
        self.api_key = (api_key or os.getenv("GROQ_API_KEY", "")).strip()
        self.model = model
        self.min_call_interval_seconds = max(0.0, float(min_call_interval_seconds))
        self._last_call_at = 0.0

        self._client = None
        self.enabled = bool(self.api_key and Groq is not None)
        if self.enabled:
            try:
                self._client = Groq(api_key=self.api_key)
                log.info("GROQ AI client initialized with model '%s'.", self.model)
            except Exception as exc:  # pragma: no cover - external dependency path
                self.enabled = False
                log.warning("Failed to initialize GROQ client. Using fallback only: %s", exc)
        else:
            log.info("GROQ disabled (missing key or package). Using fallback heuristics.")

    # ── Internal helpers ────────────────────────────────────────────────
    def _throttle(self) -> None:
        if self.min_call_interval_seconds <= 0:
            return
        elapsed = time.monotonic() - self._last_call_at
        remaining = self.min_call_interval_seconds - elapsed
        if remaining > 0:
            time.sleep(remaining)

    @staticmethod
    def _to_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"true", "yes", "1", "entry", "entry-level"}
        return bool(value)

    @staticmethod
    def _clamp_score(value: Any, default: int = 0) -> int:
        try:
            score = int(float(value))
        except (TypeError, ValueError):
            score = default
        return max(0, min(100, score))

    @staticmethod
    def _safe_json_loads(text: str) -> dict[str, Any] | None:
        if not text:
            return None
        try:
            data = json.loads(text)
            return data if isinstance(data, dict) else None
        except json.JSONDecodeError:
            # Best-effort: extract first JSON object if model wrapped output in prose.
            match = re.search(r"\{.*\}", text, flags=re.DOTALL)
            if not match:
                return None
            try:
                data = json.loads(match.group(0))
                return data if isinstance(data, dict) else None
            except json.JSONDecodeError:
                return None

    def _chat_json(self, system_prompt: str, user_prompt: str) -> dict[str, Any] | None:
        if not self.enabled or self._client is None:
            return None

        self._throttle()
        try:
            completion = self._client.chat.completions.create(
                model=self.model,
                temperature=0.1,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                response_format={"type": "json_object"},
            )
            self._last_call_at = time.monotonic()
            content = ""
            if completion.choices and completion.choices[0].message:
                content = completion.choices[0].message.content or ""
            return self._safe_json_loads(content)
        except Exception as exc:  # pragma: no cover - network/API variability
            log.warning("GROQ request failed; using fallback: %s", exc)
            return None

    # ── Fallback logic ──────────────────────────────────────────────────
    def _fallback_classification(self, job_title: str, description: str) -> dict[str, Any]:
        text = f"{job_title} {description}".lower()

        senior_signals = [
            "senior", "sr.", "lead", "manager", "principal", "director",
            "staff", "architect", "head of", "vice president", "vp",
        ]
        entry_signals = [
            "junior", "entry", "entry-level", "associate", "fresh", "graduate",
            "new grad", "trainee", "intern",
        ]

        years_match = re.search(r"\b([3-9]|\d{2})\+?\s*(years?|yrs?)\b", text)
        if any(token in text for token in senior_signals) or years_match:
            reason = "seniority keywords or high-experience requirement detected"
            return {
                "is_entry_level": False,
                "confidence": 90,
                "source": "fallback",
                "reason": reason,
            }

        if any(token in text for token in entry_signals):
            return {
                "is_entry_level": True,
                "confidence": 82,
                "source": "fallback",
                "reason": "entry-level keywords detected",
            }

        # Neutral fallback: upstream keyword filters already pre-screen jobs.
        return {
            "is_entry_level": True,
            "confidence": 72,
            "source": "fallback",
            "reason": "no seniority signals detected",
        }

    @staticmethod
    def _fallback_score(job_description: str, user_skills: list[str]) -> dict[str, Any]:
        text = (job_description or "").lower()
        matched = [skill for skill in user_skills if skill.lower() in text]

        if not user_skills:
            score = 60
        else:
            ratio = len(matched) / max(1, len(user_skills))
            score = int(45 + ratio * 50)

        return {
            "score": max(0, min(100, score)),
            "confidence": 70,
            "matched_skills": matched,
            "source": "fallback",
            "reason": "keyword overlap scoring",
        }

    @staticmethod
    def _fallback_summary(job_title: str, company: str, description: str) -> list[str]:
        desc = (description or "").strip()
        if len(desc) > 140:
            desc_highlight = desc[:140] + "..."
        else:
            desc_highlight = desc or "No description provided."

        bullets = [
            f"Role: {job_title or 'Unknown role'} at {company or 'Unknown company'}.",
            "This role passed your entry-level keyword filters.",
            f"Description highlight: {desc_highlight}",
        ]
        return bullets[:3]

    @staticmethod
    def _fallback_cover_points(job_title: str, company: str, user_profile: str) -> list[str]:
        profile = user_profile or "BSCS final-year student focused on Python, AI, and web development"
        return [
            f"Connect your background to the {job_title or 'role'} responsibilities at {company or 'the company'}.",
            f"Highlight projects that demonstrate {profile} with measurable outcomes.",
            "Emphasize fast learning, ownership, and readiness for entry-level impact from day one.",
        ]

    # ── Public API ──────────────────────────────────────────────────────
    def classify_job_level(self, job_title: str, description: str) -> dict[str, Any]:
        """Classify whether a role is truly entry-level with confidence [0..100]."""
        fallback = self._fallback_classification(job_title, description)

        prompt = (
            "Classify this job as entry-level or not. "
            "Entry-level means fresh/junior/associate/new-grad suitable, and excludes senior/lead/manager roles. "
            "Return strict JSON with keys: is_entry_level (bool), confidence (0-100 int), reason (short string)."
        )
        user_payload = (
            f"Job title: {job_title}\n"
            f"Description: {description[:4000]}"
        )

        data = self._chat_json(prompt, user_payload)
        if not data:
            return fallback

        return {
            "is_entry_level": self._to_bool(data.get("is_entry_level")),
            "confidence": self._clamp_score(data.get("confidence"), fallback["confidence"]),
            "source": "ai",
            "reason": str(data.get("reason") or "AI classification"),
        }

    def score_job_match(
        self,
        job_description: str,
        user_skills: list[str] | None = None,
        job_title: str = "",
    ) -> dict[str, Any]:
        """Score job relevance [0..100] against provided skills."""
        skills = user_skills or [
            "Python", "AI", "Machine Learning", "Data Science", "Web Development",
            "Django", "Flask", "FastAPI", "SQL", "JavaScript",
        ]

        fallback = self._fallback_score(job_description, skills)
        prompt = (
            "Score job-to-candidate fit. "
            "Return strict JSON keys: score (0-100 int), confidence (0-100 int), "
            "matched_skills (array of strings), reason (short string)."
        )
        user_payload = (
            f"Candidate skills: {', '.join(skills)}\n"
            f"Job title: {job_title}\n"
            f"Job description: {job_description[:5000]}"
        )

        data = self._chat_json(prompt, user_payload)
        if not data:
            return fallback

        matched_skills = data.get("matched_skills")
        if not isinstance(matched_skills, list):
            matched_skills = fallback["matched_skills"]

        return {
            "score": self._clamp_score(data.get("score"), fallback["score"]),
            "confidence": self._clamp_score(data.get("confidence"), fallback["confidence"]),
            "matched_skills": [str(s) for s in matched_skills][:10],
            "source": "ai",
            "reason": str(data.get("reason") or "AI scoring"),
        }

    def summarize_job(self, job_title: str, company: str, description: str) -> list[str]:
        """Generate a concise 3-bullet summary for alerts."""
        fallback = self._fallback_summary(job_title, company, description)

        prompt = (
            "Create exactly 3 concise bullets for a Telegram alert. "
            "Keep each bullet under 120 characters. "
            "Return strict JSON: {\"bullets\": [\"...\", \"...\", \"...\"]}."
        )
        user_payload = (
            f"Job title: {job_title}\n"
            f"Company: {company}\n"
            f"Description: {description[:3500]}"
        )

        data = self._chat_json(prompt, user_payload)
        if not data:
            return fallback

        bullets = data.get("bullets")
        if not isinstance(bullets, list) or not bullets:
            return fallback

        clean = [str(b).strip().lstrip("•-").strip() for b in bullets if str(b).strip()]
        if not clean:
            return fallback

        return clean[:3]

    def generate_cover_letter_points(
        self,
        job_title: str,
        company: str,
        description: str,
        user_profile: str = "",
    ) -> list[str]:
        """Generate 3 personalized talking points for cover letters/emails."""
        fallback = self._fallback_cover_points(job_title, company, user_profile)

        prompt = (
            "Generate exactly 3 personalized talking points for a short cover letter. "
            "Tone: professional and concise. "
            "Return strict JSON: {\"points\": [\"...\", \"...\", \"...\"]}."
        )
        user_payload = (
            f"Candidate profile: {user_profile or 'Final-year BSCS student skilled in Python, AI, web development.'}\n"
            f"Job title: {job_title}\n"
            f"Company: {company}\n"
            f"Description: {description[:3500]}"
        )

        data = self._chat_json(prompt, user_payload)
        if not data:
            return fallback

        points = data.get("points")
        if not isinstance(points, list) or not points:
            return fallback

        clean = [str(p).strip().lstrip("•-").strip() for p in points if str(p).strip()]
        if not clean:
            return fallback

        return clean[:3]
