from __future__ import annotations

import re
from urllib.parse import urlparse

_ASSOCIATE_KEYWORDS = (
    "associate",
    "associate engineer",
    "associate software engineer",
    "associate ai engineer",
    "associate data scientist",
    "junior",
    "entry level",
    "entry-level",
    "graduate",
    "new grad",
    "trainee",
    "apprentice",
    "intern",
    "fresher",
)


def normalize_company_name(value: str, fallback_url: str = "") -> str:
    raw = str(value or "").strip()
    if raw:
        return re.sub(r"\s+", " ", raw)

    parsed = urlparse(fallback_url)
    host = (parsed.netloc or "").lower().strip()
    if host.startswith("www."):
        host = host[4:]
    host = host.split(":")[0]

    path_segments = [seg for seg in (parsed.path or "").split("/") if seg.strip()]
    labels = [label for label in host.split(".") if label]

    candidate = ""
    if host.endswith("lever.co") and path_segments:
        candidate = path_segments[0]
    elif host.endswith("ashbyhq.com") and path_segments:
        candidate = path_segments[0]
    elif host.endswith("workable.com") and path_segments:
        candidate = path_segments[0]
    elif host.endswith("applytojob.com") and labels:
        candidate = labels[0]
    elif host.endswith("breezy.hr") and labels:
        candidate = labels[0]
    elif labels:
        # jobs.careem.com -> careem ; careers.company.com -> company
        if len(labels) >= 3 and labels[0] in {"jobs", "careers", "career", "apply", "job"}:
            candidate = labels[1]
        else:
            candidate = labels[0]

    normalized = (candidate or "unknown").replace("-", " ").replace("_", " ")
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized.title() or "Unknown"


def is_associate_role(title: str, description: str = "", department: str = "") -> bool:
    haystack = " ".join([str(title or ""), str(description or ""), str(department or "")]).lower()
    return any(keyword in haystack for keyword in _ASSOCIATE_KEYWORDS)


def safe_sheet_title_from_url(url: str) -> str:
    host = (urlparse(url).netloc or "unknown").lower().strip()
    if host.startswith("www."):
        host = host[4:]
    path = (urlparse(url).path or "").strip("/").replace("/", "-")
    base = host if not path else f"{host}-{path}"
    clean = re.sub(r"[^A-Za-z0-9 _\-]", "-", base)
    clean = re.sub(r"\s+", " ", clean).strip() or "source"
    title = f"Company_{clean}"
    return title[:95]
