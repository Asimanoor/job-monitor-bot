"""
Role Filter
────────────
Centralized role-matching engine for filtering job postings by target roles.

Uses keyword matching + synonym expansion + fuzzy matching (thefuzz).
No external NLP models — runs in <1 second.
"""

from __future__ import annotations

import os
import inspect
import logging
import re
import threading
from typing import Any

from thefuzz import fuzz

log = logging.getLogger(__name__)

_SEMANTIC_MODEL_CACHE: dict[str, Any] = {}
_SEMANTIC_ROLE_EMBEDDINGS_CACHE: dict[tuple[str, tuple[str, ...]], Any] = {}
_SEMANTIC_CACHE_LOCK = threading.Lock()
_SEMANTIC_WARNED_ONCE = False

# ── Target roles ─────────────────────────────────────────────────────────────
# These are loaded from jobs.txt at runtime, but we keep defaults for fallback.
DEFAULT_TARGET_ROLES = [
    "AI Engineer",
    "AIML Engineer",
    "Machine Learning Engineer",
    "Data Scientist",
    "Data Science Engineer",
    "Junior Data Scientist",
    "Data Analyst",
    "Associate Software Engineer",
    "Software Engineer",
    "Junior Software Engineer",
    "Graduate Software Engineer",
    "Entry Level Software Engineer",
    "Python Developer",
    "Junior Python Developer",
    "Associate Python Developer",
    "Backend Developer",
    "Frontend Developer",
    "Full Stack Developer",
]

# ── Synonym map: canonical title → list of equivalent titles ─────────────────
TITLE_SYNONYMS: dict[str, list[str]] = {
    "AI Engineer": [
        "AIML Engineer", "ML Engineer", "Machine Learning Engineer",
        "Artificial Intelligence Engineer", "AI/ML Engineer",
        "Deep Learning Engineer",
    ],
    "Machine Learning Engineer": [
        "ML Engineer", "AI Engineer", "AIML Engineer",
        "Deep Learning Engineer", "AI/ML Engineer",
    ],
    "Data Scientist": [
        "Data Science Engineer", "Data Analyst", "Junior Data Scientist",
        "Applied Scientist", "Research Scientist",
    ],
    "Data Analyst": [
        "Business Analyst", "Data Science Analyst", "Analytics Engineer",
    ],
    "Software Engineer": [
        "SWE", "Software Developer", "Backend Engineer",
        "Full Stack Engineer", "Fullstack Developer", "Application Developer",
    ],
    "Associate Software Engineer": [
        "Junior Software Engineer", "Trainee Software Engineer",
        "Graduate Software Engineer", "Entry Level Software Engineer",
        "Fresher Software Engineer",
    ],
    "Python Developer": [
        "Python Engineer", "Backend Python Developer",
        "Django Developer", "FastAPI Developer",
    ],
    "Backend Developer": [
        "Backend Engineer", "Server Side Developer",
        "Node.js Developer", "API Developer",
    ],
    "Frontend Developer": [
        "Frontend Engineer", "React Developer",
    ],
    "Full Stack Developer": [
        "Fullstack Developer", "Full Stack Engineer",
        "Fullstack Engineer", "Full-Stack Developer",
    ],
}

# ── Keywords that indicate a role title (not a service/product name) ─────────
ROLE_TITLE_KEYWORDS = {
    "engineer", "developer", "scientist", "analyst",
     "graduate", "associate",
    }

_STRICT_ALLOWED_PHRASES = [
    "associate software engineer",
    "associate ai engineer",
    "associate ml engineer",
    "associate data engineer",
    "junior software engineer",
    "fresh graduate",
    "entry level",
    "entry-level",
    "graduate trainee",
    "0-2 years",
    "0 2 years",
    "1 year",
    "2 years",
]

_STRICT_BASE_ROLES = [
    "software engineer",
    "ai engineer",
    "ml engineer",
    "data engineer",
]

_STRICT_ENTRY_HINTS = [
    "associate",
    "junior",
    "fresh graduate",
    "entry level",
    "entry-level",
    "graduate trainee",
    "0-2 years",
    "0 2 years",
    "1 year",
    "2 years",
]

_STRICT_REJECT_TOKENS = [
    "iii",
    "iv",
    "level",
    "senior",
    "lead",
    "manager",
    "architect",
    "principal",
    "director",
]

# ── Seniority keywords to EXCLUDE (user wants entry/junior roles) ────────────
SENIOR_TITLE_KEYWORDS = [
    "senior", "sr.", "sr ", "lead", "principal", "staff",
    "director", "head of", "vp", "vice president",
    "chief", "cto", "cio", "cfo",
]

SENIOR_DESC_PATTERNS = [
    re.compile(r"\b(?:5|6|7|8|9|10)\+?\s*(?:years?|yrs?)\b", re.IGNORECASE),
    re.compile(r"\bsenior\b", re.IGNORECASE),
    re.compile(r"\blead\b", re.IGNORECASE),
    re.compile(r"\bprincipal\b", re.IGNORECASE),
    re.compile(r"\bdirector\b", re.IGNORECASE),
]

# ── Entry-level hints (boost score for these) ────────────────────────────────
ENTRY_LEVEL_HINTS = [
    "associate", "junior", "jr", "jr.", "graduate", "grad",
    "trainee", "intern", "entry", "entry-level", "fresher",
    "new grad", "fresh", "0-1 years", "0-2 years",
]

# ── Non-job title phrases to reject ──────────────────────────────────────────
NON_JOB_PHRASES = {
    "learn more", "read more", "view all", "get more details",
    "privacy", "policy", "terms", "cookie", "overview", "guide",
    "webinar", "case study", "solutions", "services", "platform overview",
    "developer portal", "disclaimer", "summit", "report", "ebook",
    "sign in", "log in", "login", "about us", "contact us", "home",
    "subscribe", "press", "investor", "culture", "benefits",
    "data modernization", "cloud-native engineering",
    "digital transformation", "ai-first platforms",
    "intelligent business solution", "smart-manufacturing",
    "customer engagement", "enterprise modernization",
    "agentic architecture",
}

# ── Tech keywords that anchor a job as relevant ──────────────────────────────
TECH_KEYWORDS = {
    "python", "javascript", "typescript", "react", "django", "fastapi","c++", "c#","sql", "nosql", "mongodb", "postgresql",
    "aws", "azure", "gcp", "docker", 
    "machine learning", "deep learning", "nlp", 
    "tensorflow", "pytorch", "scikit-learn", "pandas", "numpy",
    "data pipeline", "etl", "spark", "hadoop", 
    "rest api", "graphql", "microservices", "ci/cd",
    "git", "agile", "scrum",
}


def _normalize(text: str) -> str:
    """Lowercase, collapse whitespace, strip punctuation for comparison."""
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    return " ".join(text.split())


def _contains_any_token(text: str, tokens: list[str]) -> bool:
    for token in tokens:
        pattern = r"\b" + re.escape(str(token).lower()) + r"\b"
        if re.search(pattern, text):
            return True
    return False


def _passes_strict_role_policy(title: str) -> bool:
    normalized = _normalize(str(title or ""))
    if not normalized:
        return False

    if _contains_any_token(normalized, _STRICT_REJECT_TOKENS):
        return False

    if any(phrase in normalized for phrase in _STRICT_ALLOWED_PHRASES):
        return True

    has_base_role = any(role in normalized for role in _STRICT_BASE_ROLES)
    has_entry_hint = any(hint in normalized for hint in _STRICT_ENTRY_HINTS)
    return has_base_role and has_entry_hint


def _word_boundary_match(text: str, keywords: list[str]) -> str | None:
    """Return first keyword found via word-boundary match, or None."""
    text_lower = text.lower()
    for kw in keywords:
        pattern = r"\b" + re.escape(kw.lower()) + r"\b"
        if re.search(pattern, text_lower):
            return kw
    return None


def _env_truthy(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def is_non_job_title(title: str) -> bool:
    """Return True if title looks like a service/marketing page, not a job."""
    lower = _normalize(title)
    if not lower:
        return True

    # Too short or too long
    if len(title.strip()) < 5 or len(title.strip()) > 180:
        return True

    # Matches known non-job phrases
    if any(phrase in lower for phrase in NON_JOB_PHRASES):
        return True

    # Pure numbers, punctuation, or single character
    if re.fullmatch(r"[\W\d_]+", title.strip()):
        return True

    return False


def looks_like_role_title(title: str) -> bool:
    """Return True if the title resembles an actual job role title."""
    if is_non_job_title(title):
        return False

    lower = _normalize(title)

    # Check for role title keywords
    return any(
        re.search(r"\b" + re.escape(kw) + r"\b", lower)
        for kw in ROLE_TITLE_KEYWORDS
    )


def is_senior_role(title: str, description: str = "") -> bool:
    """Return True if the role is senior-level (should be excluded for entry-level search)."""
    title_lower = title.lower()

    # Robust title seniority detection (handles 'Sr.', 'Sr ', and common leadership labels).
    if re.search(
        r"\b(senior|sr\.?|lead|principal|staff|director|head|vp|vice president|chief|cto|cio|cfo)\b",
        title_lower,
    ):
        if _word_boundary_match(title, ENTRY_LEVEL_HINTS):
            return False
        return True

    # Check title
    for kw in SENIOR_TITLE_KEYWORDS:
        pattern = r"\b" + re.escape(kw.lower()) + r"\b"
        if re.search(pattern, title_lower):
            # Exception: "Associate" overrides "Senior" if both present
            if _word_boundary_match(title, ENTRY_LEVEL_HINTS):
                return False
            return True

    # Check description for experience requirements
    desc_lower = (description or "").lower()
    for pattern in SENIOR_DESC_PATTERNS:
        if pattern.search(desc_lower):
            # Only exclude if title doesn't have entry-level hints
            if not _word_boundary_match(title, ENTRY_LEVEL_HINTS):
                return True

    return False


def matches_target_role(
    title: str,
    description: str = "",
    target_roles: list[str] | None = None,
    fuzzy_threshold: int = 75,
    exclude_senior: bool = True,
    use_semantic: bool | None = None,
    semantic_threshold: float = 0.65,
) -> tuple[bool, str | None, float]:
    """
    Check if a job title/description matches any target role.

    Returns:
        (matches, matched_role, score) where:
        - matches: True if the job matches a target role
        - matched_role: The target role it matched against (or None)
        - score: Relevance score 0-100
    """
    if not title or not title.strip():
        return False, None, 0.0

    if not _passes_strict_role_policy(title):
        return False, None, 0.0

    # Hard gate: reject obvious service/marketing/non-job strings early.
    # This prevents fuzzy matching from classifying "Data Modernization | ... "
    # as a job role.
    if is_non_job_title(title) or not looks_like_role_title(title):
        return False, None, 0.0

    if exclude_senior and is_senior_role(title, description):
        return False, None, 0.0

    roles = target_roles or DEFAULT_TARGET_ROLES
    title_norm = _normalize(title)
    desc_norm = _normalize(description or "")

    best_score = 0.0
    best_role = None

    for role in roles:
        role_norm = _normalize(role)
        score = 0.0

        # 1. Exact match
        if title_norm == role_norm:
            return True, role, 100.0

        # 2. Title contains role (substring)
        if role_norm in title_norm:
            score = max(score, 90.0)

        # 3. Synonym match
        for canonical, synonyms in TITLE_SYNONYMS.items():
            canonical_norm = _normalize(canonical)
            if canonical_norm == role_norm or role_norm in [_normalize(s) for s in synonyms]:
                all_variants = [canonical] + synonyms
                for variant in all_variants:
                    variant_norm = _normalize(variant)
                    if variant_norm == title_norm or variant_norm in title_norm:
                        score = max(score, 85.0)
                        break

        # 4. Fuzzy match on title
        fuzzy_score = fuzz.token_set_ratio(title_norm, role_norm)
        if fuzzy_score >= 80:
            score = max(score, fuzzy_score * 0.9)

        # 5. Entry-level hint bonus
        if _word_boundary_match(title, ENTRY_LEVEL_HINTS):
            score += 5.0

        # 6. Tech keyword in description bonus
        if desc_norm:
            tech_count = sum(1 for kw in TECH_KEYWORDS if kw in desc_norm)
            score += min(tech_count * 2, 10)  # max +10 from tech keywords

        if score > best_score:
            best_score = score
            best_role = role

    if best_score >= fuzzy_threshold:
        # Optional semantic gate (free, local embeddings). If unavailable, fall back
        # to keyword/fuzzy only.
        enabled = (
            os.environ.get("ENABLE_SEMANTIC_FILTER", "").strip().lower() in {"1", "true", "yes", "on"}
            if use_semantic is None
            else bool(use_semantic)
        )
        # Semantic filtering should only run when we have meaningful body text.
        # Title-only checks are handled by keyword/fuzzy rules above.
        if enabled and len((description or "").strip()) >= 40:
            semantic_ok = passes_semantic_filter(
                title=title,
                description=description,
                target_roles=roles,
                threshold=semantic_threshold,
            )
            if not semantic_ok:
                return False, None, best_score

        return True, best_role, min(best_score, 100.0)

    return False, None, best_score


def _cosine_similarity(vec_a: list[float], vec_b: list[float]) -> float:
    """Cosine similarity without numpy/torch."""
    if not vec_a or not vec_b or len(vec_a) != len(vec_b):
        return -1.0
    dot = 0.0
    norm_a = 0.0
    norm_b = 0.0
    for a, b in zip(vec_a, vec_b):
        dot += a * b
        norm_a += a * a
        norm_b += b * b
    if norm_a <= 0.0 or norm_b <= 0.0:
        return -1.0
    return dot / ((norm_a ** 0.5) * (norm_b ** 0.5))


def passes_semantic_filter(
    title: str,
    description: str,
    target_roles: list[str],
    threshold: float = 0.65,
) -> bool:
    """
    Free semantic filter using local sentence-transformers embeddings.

    If sentence-transformers/model cannot be loaded, the function returns True
    so keyword/fuzzy matching remains the fallback (per requirements).
    """
    if not target_roles:
        return True

    try:
        from sentence_transformers import SentenceTransformer, util  # type: ignore
    except Exception:
        # No dependency / model not available: don't block keyword matches.
        return True

    # Keep third-party model download logs from flooding monitor output.
    logging.getLogger("huggingface_hub").setLevel(logging.ERROR)
    logging.getLogger("sentence_transformers").setLevel(logging.ERROR)
    logging.getLogger("transformers").setLevel(logging.ERROR)

    model_name = os.environ.get("SEMANTIC_MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2")

    global _SEMANTIC_WARNED_ONCE

    with _SEMANTIC_CACHE_LOCK:
        model = _SEMANTIC_MODEL_CACHE.get(model_name)
        if model is None:
            kwargs: dict[str, Any] = {}
            try:
                signature_params = inspect.signature(SentenceTransformer).parameters
            except Exception:
                signature_params = {}

            hf_token = os.environ.get("HF_TOKEN", "").strip()
            if hf_token:
                if "token" in signature_params:
                    kwargs["token"] = hf_token
                elif "use_auth_token" in signature_params:
                    kwargs["use_auth_token"] = hf_token

            if _env_truthy("SEMANTIC_LOCAL_FILES_ONLY", default=False) and "local_files_only" in signature_params:
                kwargs["local_files_only"] = True

            try:
                model = SentenceTransformer(model_name, **kwargs)
            except Exception as exc:
                if not _SEMANTIC_WARNED_ONCE:
                    log.warning(
                        "Semantic filter unavailable (%s). Falling back to keyword/fuzzy-only matching.",
                        exc,
                    )
                    _SEMANTIC_WARNED_ONCE = True
                return True

            _SEMANTIC_MODEL_CACHE[model_name] = model

        roles_key = (model_name, tuple(target_roles))
        role_embeddings = _SEMANTIC_ROLE_EMBEDDINGS_CACHE.get(roles_key)
        if role_embeddings is None:
            try:
                role_embeddings = model.encode(target_roles, convert_to_tensor=True)
            except Exception as exc:
                if not _SEMANTIC_WARNED_ONCE:
                    log.warning(
                        "Semantic role embedding failed (%s). Falling back to keyword/fuzzy-only matching.",
                        exc,
                    )
                    _SEMANTIC_WARNED_ONCE = True
                return True
            _SEMANTIC_ROLE_EMBEDDINGS_CACHE[roles_key] = role_embeddings

        try:
            job_text = f"{title} {description[:250]}".strip()
            job_embedding = model.encode(job_text, convert_to_tensor=True)
            similarities = util.cos_sim(job_embedding, role_embeddings)[0]
            max_sim = float(similarities.max().item()) if hasattr(similarities.max(), "item") else float(similarities.max())
        except Exception as exc:
            if not _SEMANTIC_WARNED_ONCE:
                log.warning(
                    "Semantic scoring failed (%s). Falling back to keyword/fuzzy-only matching.",
                    exc,
                )
                _SEMANTIC_WARNED_ONCE = True
            return True

    return max_sim >= float(threshold)


def compute_match_score(
    title: str,
    description: str = "",
    target_roles: list[str] | None = None,
) -> float:
    """Compute a 0-100 relevance score for a job against target roles."""
    _, _, score = matches_target_role(title, description, target_roles)
    return score


def filter_jobs_by_role(
    jobs: list[dict[str, Any]],
    target_roles: list[str] | None = None,
    min_score: float = 50.0,
    exclude_senior: bool = True,
) -> list[dict[str, Any]]:
    """
    Filter a list of job dicts, keeping only those matching target roles.

    Each job dict should have at least 'title' key. Optionally 'description'.
    Adds 'match_score' and 'matched_role' keys to passing jobs.

    Returns filtered list sorted by match_score descending.
    """
    filtered: list[dict[str, Any]] = []

    for job in jobs:
        title = str(job.get("title", "")).strip()
        description = str(job.get("description", "")).strip()

        if not title:
            continue

        if not _passes_strict_role_policy(title):
            continue

        # Skip non-job titles
        if is_non_job_title(title):
            continue

        # Skip senior roles if requested
        if exclude_senior and is_senior_role(title, description):
            continue

        # Check role match
        matches, matched_role, score = matches_target_role(
            title, description, target_roles
        )

        if matches and score >= min_score:
            job["match_score"] = round(score, 1)
            job["matched_role"] = matched_role
            filtered.append(job)

    # Sort by match score descending
    filtered.sort(key=lambda j: j.get("match_score", 0), reverse=True)
    return filtered


__all__ = [
    "matches_target_role",
    "compute_match_score",
    "filter_jobs_by_role",
    "is_senior_role",
    "is_non_job_title",
    "looks_like_role_title",
    "DEFAULT_TARGET_ROLES",
]
