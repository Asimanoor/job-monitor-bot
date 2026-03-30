from __future__ import annotations

import hashlib
import re
from urllib.parse import urlparse, urlunparse

_TRACKING_QUERY_PREFIXES = (
    "utm_",
    "fbclid",
    "gclid",
    "mc_",
    "hs",
    "__hs",
    "session",
    "sessionid",
    "sid",
)


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def normalize_title(value: str) -> str:
    text = normalize_text(value)
    # Remove bracketed qualifiers: (AI), [ML], {remote}
    text = re.sub(r"\([^)]*\)|\[[^\]]*\]|\{[^}]*\}", " ", text)
    # Normalize separators to spaces: -, |, /
    text = re.sub(r"[|\-/]+", " ", text)
    # Remove remaining punctuation/symbol noise.
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_location(value: str, fallback_url: str = "") -> str:
    loc = normalize_text(value)
    if not loc or loc in {"not specified", "n/a", "na", "none", "-"}:
        if fallback_url:
            url_norm = normalize_url(fallback_url)
            suffix = hashlib.sha1(url_norm.encode("utf-8", errors="ignore")).hexdigest()[:8]
            return f"url-{suffix}"
        return "unknown"

    # Collapse common variants.
    if "remote" in loc:
        return "remote"
    if "lahore" in loc:
        return "lahore"
    if "karachi" in loc:
        return "karachi"
    if "islamabad" in loc:
        return "islamabad"
    if "pakistan" in loc:
        return "pakistan"

    loc = re.sub(r"[^a-z0-9\s]", " ", loc)
    return re.sub(r"\s+", " ", loc).strip()


def normalize_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""

    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"}:
        return normalize_text(raw)

    normalized_path = re.sub(r"/+", "/", parsed.path or "/")
    if normalized_path != "/":
        normalized_path = normalized_path.rstrip("/") or "/"

    # Keep hash model stable: ignore all query strings (tracking/session/filter noise).
    # This intentionally collapses URL variants that differ only by query params.
    query = ""
    return urlunparse(
        (
            parsed.scheme.lower(),
            (parsed.netloc or "").lower(),
            normalized_path,
            parsed.params,
            query,
            "",
        )
    )


def build_job_hash(company: str, title: str, location: str, apply_url: str) -> str:
    payload = "|".join(
        [
            normalize_text(company),
            normalize_title(title),
            normalize_location(location, fallback_url=apply_url),
            normalize_url(apply_url),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8", errors="ignore")).hexdigest()


def build_job_key(title: str, location: str, apply_url: str) -> str:
    link = normalize_url(apply_url)
    if link:
        return f"url|{link}"
    return f"title_location|{normalize_title(title)}|{normalize_location(location, fallback_url=apply_url)}"


def build_title_location_key(title: str, location: str) -> str:
    return f"{normalize_title(title)}|{normalize_location(location)}"
