import logging
import re
from datetime import datetime, timedelta, timezone
from thefuzz import fuzz

log = logging.getLogger(__name__)

# Two categories to avoid false positives:
#   TITLE_EXCLUDES  → checked only against the job title (word-boundary match)
#   DESC_EXCLUDES   → checked against the full description (word-boundary match)
TITLE_EXCLUDE_WORDS = [
    "senior", "sr.", "lead", "manager", "principal", "staff",
    "director", "head of", "vp", "vice president", "architect",
]

DESC_EXCLUDE_PATTERNS = [
    r"\b(?:5|6|7|8|10)\+?\s*(?:years?|yrs?)\b",        # "5+ years", "7 yrs"
    r"\bsenior\b",
    r"\blead\b",
    r"\bmanager\b",
    r"\bprincipal\b",
    r"\bdirector\b",
]

# Pre-compile for speed
_DESC_EXCLUDE_RES = [re.compile(p, re.IGNORECASE)
                     for p in DESC_EXCLUDE_PATTERNS]

ENTRY_LEVEL_TITLE_HINTS = [
    "associate", "junior", "jr", "jr.", "graduate",
    "trainee", "intern", "entry", "entry-level", "fresher", "new grad",
]

CONTEXT_ROLE_TERMS = [
    "role", "position", "job", "hiring", "candidate", "responsibilities",
    "requirements", "internship", "graduate", "entry", "associate",
]

TITLE_SYNONYMS: dict[str, list[str]] = {
    "AIML Engineer":    ["AI Engineer", "ML Engineer", "Machine Learning Engineer",
                         "AI/ML Engineer", "Artificial Intelligence Engineer"],
    "AI Engineer":      ["AIML Engineer", "ML Engineer", "Machine Learning Engineer",
                         "Artificial Intelligence Engineer"],
    "Data Science Engineer": ["Data Scientist", "Data Engineer", "Data Analyst",
                              "Machine Learning Engineer"],
    "Data Scientist":   ["Data Science Engineer", "Data Analyst",
                         "Junior Data Scientist"],
    "Software Engineer":     ["SWE", "Software Developer", "Backend Engineer",
                              "Full Stack Engineer", "Fullstack Developer"],
    "Python Developer":      ["Python Engineer", "Backend Python Developer",
                              "Django Developer", "Flask Developer"],
    "Backend Developer":     ["Backend Engineer", "Python Developer",
                              "Node.js Developer", "Server Side Developer"],
    "Frontend Developer":    ["Frontend Engineer", "UI Developer",
                              "React Developer", "Angular Developer"],
    "Full Stack Developer":  ["Fullstack Developer", "Full Stack Engineer",
                              "Fullstack Engineer", "Software Engineer"],
    "Associate Software Engineer": ["Junior Software Engineer", "Trainee Software Engineer",
                                    "Graduate Software Engineer", "Entry Level Software Engineer"],
    "Junior Software Engineer":    ["Associate Software Engineer", "Trainee Software Engineer",
                                    "Graduate Software Engineer"],
    "Graduate Software Engineer":  ["Fresh Graduate Engineer", "Trainee Engineer",
                                    "Associate Software Engineer"],
    "Fresh Graduate":        ["Graduate Trainee", "Management Trainee", "Trainee",
                              "Entry Level", "New Grad"],
}


class FilterEngine:
    def __init__(
        self,
        fuzzy_threshold: int = 70,
        max_age_days: int = 7,
        ai_client=None,
        ai_confidence_threshold: int = 70,
    ):
        self.fuzzy_threshold = fuzzy_threshold
        self.max_age_days = max_age_days
        self.ai_client = ai_client
        self.ai_confidence_threshold = max(0, min(100, ai_confidence_threshold))

    def _word_boundary_match(self, text: str, keywords: list[str]) -> str | None:
        """Check for word-boundary keyword matches to avoid substring false-positives."""
        text_lower = text.lower()
        for kw in keywords:
            pattern = r"\b" + re.escape(kw.lower()) + r"\b"
            if re.search(pattern, text_lower):
                return kw
        return None

    def normalize_title(self, title: str) -> str:
        """Lowercase, collapse whitespace, strip punctuation for comparison."""
        text = title.lower()
        text = re.sub(r"[^\w\s]", " ", text)
        return " ".join(text.split())

    def is_title_match(self, candidate: str, target: str) -> bool:
        """
        Return True if candidate (from API) matches target (from jobs.txt)
        using exact → synonym → fuzzy strategy.
        """
        c_norm = self.normalize_title(candidate)
        t_norm = self.normalize_title(target)

        if c_norm == t_norm:
            return True

        for syn in TITLE_SYNONYMS.get(target, []):
            if self.normalize_title(syn) == c_norm:
                return True

        score = fuzz.token_set_ratio(c_norm, t_norm)
        # Requirement: "Threshold: >= 80% for title, >= 70% for description"
        # But instructions say use config fuzzy_threshold. We'll use 80 for title exact match
        return score >= 80

    def matches_any_title(self, candidate: str, targets: list[str]) -> str | None:
        """Return the first matching target title, or None."""
        if not candidate or not candidate.strip():
            return None
        for t in targets:
            if self.is_title_match(candidate, t):
                return t
        return None

    def is_excluded(self, job_title: str, description: str) -> tuple[bool, str]:
        """
        Return (True, reason) if the job should be EXCLUDED due to seniority.
        """
        hit = self._word_boundary_match(job_title, TITLE_EXCLUDE_WORDS)
        if hit:
            return True, f"title contains '{hit}'"

        desc_lower = description.lower()
        for regex in _DESC_EXCLUDE_RES:
            m = regex.search(desc_lower)
            if m:
                return True, f"description contains '{m.group()}'"

        return False, ""

    def _title_hint_match(self, job_title: str) -> str | None:
        """Return matching entry-level hint from job title if present."""
        title_lower = (job_title or "").lower()
        for hint in ENTRY_LEVEL_TITLE_HINTS:
            pattern = r"\b" + re.escape(hint.lower()) + r"\b"
            if re.search(pattern, title_lower):
                return hint
        return None

    def _contextual_sentence_match(self, description: str, filters: list[str]) -> str | None:
        """Lightweight NLP-ish sentence check for nearby role-context words."""
        if not description or not filters:
            return None

        sentences = [s.strip() for s in re.split(r"[.!?\n]+", description.lower()) if s.strip()]
        for sentence in sentences:
            if not any(term in sentence for term in CONTEXT_ROLE_TERMS):
                continue
            for f in filters:
                kw = f.lower().strip()
                if not kw:
                    continue
                if fuzz.partial_ratio(kw, sentence) >= 85:
                    return f
        return None

    def _resolve_inclusion_match(self, job_title: str, description: str, filters: list[str]) -> tuple[str | None, str | None]:
        """Resolve include keyword and match source for transparent logging."""
        if not filters:
            return "No filter", "no filter"

        title_lower = (job_title or "").lower()
        desc_lower = (description or "").lower()

        # Rule 1: explicit include keyword in title
        for f in filters:
            kw = f.lower().strip()
            if not kw:
                continue
            pattern = r"\b" + re.escape(kw) + r"\b"
            if re.search(pattern, title_lower):
                return f, "title match"

        # Rule 2: explicit include keyword in description
        for f in filters:
            kw = f.lower().strip()
            if not kw:
                continue
            pattern = r"\b" + re.escape(kw) + r"\b"
            if re.search(pattern, desc_lower):
                return f, "description match"

        # Rule 3: secondary title indicator for entry-level roles
        title_hint = self._title_hint_match(job_title)
        if title_hint:
            return title_hint, "title match"

        # Rule 4: contextual sentence similarity (lightweight NLP-ish pass)
        contextual = self._contextual_sentence_match(description, filters)
        if contextual:
            return contextual, "description match"

        # Rule 5: broad fallback fuzzy check on combined text
        combined = f"{job_title} {description}".lower()
        for f in filters:
            if fuzz.token_set_ratio(f.lower(), combined) >= max(70, self.fuzzy_threshold):
                return f, "description match"

        return None, None

    def passes_inclusion_filters(self, job_title: str, description: str, filters: list[str]) -> str | None:
        """
        Return first matching include keyword found in title OR description.
        If filters empty, returns 'No filter'.
        """
        keyword, _source = self._resolve_inclusion_match(job_title, description, filters)
        return keyword

    def is_recent(self, posted_at: str) -> bool:
        """Return True if within max_age_days or missing/unparseable."""
        if not posted_at or not posted_at.strip():
            return True
        try:
            clean = re.sub(r"\.\d+", "", posted_at)
            dt = datetime.fromisoformat(clean.replace("Z", "+00:00"))
            cutoff = datetime.now(timezone.utc) - \
                timedelta(days=self.max_age_days)
            return dt >= cutoff
        except (ValueError, TypeError):
            return True

    def is_entry_level(
        self, job_title: str, description: str, include_keywords: list[str], exclude_keywords: list[str]
    ) -> bool:
        """Check if job is entry level based on keywords."""
        excluded, _ = self.is_excluded(job_title, description)
        if excluded:
            return False

        # Additional explicit excludes passed by caller (legacy behavior support)
        text = f"{job_title} {description}".lower()
        if any((kw or "").lower() in text for kw in exclude_keywords):
            return False

        included, _source = self._resolve_inclusion_match(job_title, description, include_keywords)
        return bool(included)

    def calculate_relevance_score(self, job: dict, search_query: str) -> int:
        """Calculate a relevance score for sorting jobs."""
        score = 0

        # Exact title match
        if search_query.lower() in job.get("job_title", "").lower():
            score += 50

        # Recent postings (< 3 days)
        posted_at = job.get("job_posted_at_datetime_utc", "")
        if posted_at:
            try:
                clean = re.sub(r"\.\d+", "", posted_at)
                dt = datetime.fromisoformat(clean.replace("Z", "+00:00"))
                if datetime.now(timezone.utc) - dt < timedelta(days=3):
                    score += 30
            except (ValueError, TypeError):
                pass

        # Preferred companies
        preferred = ["arbisoft", "systems ltd",
                     "systems limited", "careem", "afat"]
        employer = job.get("employer_name", "").lower()
        if any(p in employer for p in preferred):
            score += 20

        return score

    def qualify_job(self, raw: dict, titles: list[str], filters: list[str]) -> dict | None:
        """Run the full qualification pipeline on a raw JSearch result."""
        job_title = (raw.get("job_title") or "").strip()
        description = (raw.get("job_description") or "").strip()
        posted_at = (raw.get("job_posted_at_datetime_utc") or "").strip()
        job_id = (raw.get("job_id") or "").strip()
        apply_link = (raw.get("job_apply_link") or "").strip()

        if not job_title and not job_id:
            return None

        matched_target = self.matches_any_title(job_title, titles)
        if not matched_target:
            return None

        excluded, reason = self.is_excluded(job_title, description)
        if excluded:
            log.info("   ❌ Excluded '%s' — %s", job_title, reason)
            return None

        filter_match, include_source = self._resolve_inclusion_match(job_title, description, filters)
        if not filter_match:
            log.info(
                "   ❌ Excluded '%s' — no inclusion-filter keyword found", job_title)
            return None

        log.info("   ✅ Included '%s' — Included: %s ('%s')", job_title, include_source, filter_match)

        if not self.is_recent(posted_at):
            log.info("   ❌ Excluded '%s' — older than %d days",
                     job_title, self.max_age_days)
            return None

        ai_level = {
            "is_entry_level": True,
            "confidence": 100,
            "source": "keyword",
            "reason": "AI disabled",
        }
        ai_match = {
            "score": 60,
            "confidence": 100,
            "matched_skills": [],
            "source": "keyword",
            "reason": "AI disabled",
        }

        title_hint = self._title_hint_match(job_title)

        # Secondary AI validation (after all keyword/age checks to save API credits).
        if self.ai_client is not None:
            if title_hint:
                # Preserve clearly entry-level title intents and save AI calls.
                ai_level = {
                    "is_entry_level": True,
                    "confidence": 95,
                    "source": "title-hint",
                    "reason": f"title contains '{title_hint}'",
                }
            else:
                ai_level = self.ai_client.classify_job_level(job_title, description)
                if not ai_level.get("is_entry_level", False):
                    log.info(
                        "   ❌ Excluded '%s' — AI classified as non-entry-level (%s)",
                        job_title,
                        ai_level.get("reason", "n/a"),
                    )
                    return None

                ai_confidence = int(ai_level.get("confidence", 0) or 0)
                if ai_confidence < self.ai_confidence_threshold:
                    log.info(
                        "   ❌ Excluded '%s' — AI confidence %d below threshold %d",
                        job_title,
                        ai_confidence,
                        self.ai_confidence_threshold,
                    )
                    return None

            ai_match = self.ai_client.score_job_match(
                description,
                job_title=job_title,
            )

        job = {
            "job_id":              job_id,
            "job_title":           job_title,
            "employer_name":       (raw.get("employer_name") or "Unknown").strip(),
            "apply_link":          apply_link,
            "posted_at":           posted_at,
            "location":            (raw.get("job_city") or raw.get("job_country") or "").strip(),
            "job_type":            (raw.get("job_employment_type") or "").strip(),
            "description":         description[:500],
            "matched_as":          matched_target,
            "filter_keyword":      filter_match,
            "ai_entry_level":      bool(ai_level.get("is_entry_level", True)),
            "ai_confidence":       int(ai_level.get("confidence", 0) or 0),
            "ai_source":           str(ai_level.get("source", "fallback")),
            "ai_reason":           str(ai_level.get("reason", "")),
            "ai_score":            int(ai_match.get("score", 0) or 0),
            "ai_match_confidence": int(ai_match.get("confidence", 0) or 0),
            "matched_skills":      ai_match.get("matched_skills", []),
            "score":               0,  # assigned later
        }

        keyword_score = self.calculate_relevance_score(job, job_title)
        job["score"] = int(round((keyword_score * 0.4) + (job["ai_score"] * 0.6)))
        return job

    def deduplicate_jobs(self, jobs: list[dict], state_manager) -> list[dict]:
        """
        Removes duplicates by job_id from state manager.
        """
        seen_links: set[str] = set()
        seen_ids: set[str] = set()
        new_jobs: list[dict] = []

        for job in jobs:
            apply_link = job.get("apply_link", "")
            job_id = job.get("job_id", "")

            if apply_link and apply_link in seen_links:
                continue
            if job_id and job_id in seen_ids:
                continue

            if apply_link:
                seen_links.add(apply_link)
            if job_id:
                seen_ids.add(job_id)

            if job_id and state_manager.is_new_job(job_id):
                new_jobs.append(job)
                state_manager.mark_as_notified(job_id)
            elif not job_id and apply_link and state_manager.is_new_job(apply_link):
                new_jobs.append(job)
                state_manager.mark_as_notified(apply_link)

        return new_jobs
