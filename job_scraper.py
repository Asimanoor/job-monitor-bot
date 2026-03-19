from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup
from bs4.element import Tag

JOB_CONTAINER_SELECTORS = [
    ".job-listing",
    "[data-job-id]",
    ".career-item",
    ".job-post",
    ".job-card",
    ".job-result",
    ".jobResultItem",
    "li.job",
    "li.job-item",
    "article.job",
    "tr.job",
    "tr[data-job-id]",
    ".openings-list > *",
    ".jobs-list > *",
]

TITLE_SELECTORS = [
    "h1.job-title",
    "h2.job-title",
    "h3.job-title",
    ".position-title",
    "[data-testid='job-title']",
    "[data-automation-id='jobTitle']",
    ".jobTitle",
    ".job-title",
    "a[href*='job']",
    "a[href*='career']",
    "a[href*='position']",
    "a[href*='opening']",
    "a[href*='vacanc']",
    "a[href*='apply']",
]

COMPANY_SELECTORS = [
    ".company-name",
    "[data-company]",
    ".employer",
    ".job-company",
]

LOCATION_SELECTORS = [
    ".job-location",
    "[data-location]",
    ".location",
    ".job-meta .location",
    "[data-automation-id='jobLocation']",
]

TYPE_SELECTORS = [
    ".job-type",
    ".employment-type",
    "[data-employment-type]",
    "[data-automation-id='employmentType']",
]

APPLY_LINK_SELECTORS = [
    "a.apply-btn[href]",
    ".job-link[href]",
    "[data-apply-url]",
    "a[href*='job']",
    "a[href*='career']",
    "a[href*='position']",
    "a[href*='opening']",
    "a[href*='vacanc']",
    "a[href*='apply']",
]

POSTED_DATE_SELECTORS = [
    ".posted-date",
    "[data-posted]",
    ".job-date",
    "time[datetime]",
    "[data-automation-id='jobPostingDate']",
]

NON_JOB_CLASS_KEYWORDS = {
    "nav",
    "menu",
    "header",
    "footer",
    "sidebar",
    "breadcrumbs",
    "cookie",
    "social",
    "newsletter",
}

NON_JOB_PAGE_KEYWORDS = {
    "blog",
    "news",
    "article",
    "post",
    "privacy",
    "terms",
    "sitemap",
    "services",
    "service",
    "insights",
    "whitepaper",
    "ebook",
    "case-study",
}

JOB_PAGE_HINTS = {
    "job",
    "jobs",
    "career",
    "careers",
    "position",
    "positions",
    "opening",
    "openings",
    "vacancy",
    "apply",
}

JOB_URL_KEYWORDS = {
    "job",
    "jobs",
    "career",
    "careers",
    "position",
    "positions",
    "opening",
    "openings",
    "vacancy",
    "vacancies",
    "apply",
    "requisition",
    "reqid",
    "gh_jid",
    "workdayjobs",
    "lever",
    "workable",
    "ashbyhq",
    "breezy",
    "greenhouse",
    "smartrecruiters",
    "jobvite",
}

GENERIC_BAD_TITLES = {
    "home",
    "careers",
    "career",
    "apply",
    "search",
    "services",
    "insights",
    "news",
    "blogs",
    "learn more",
    "read more",
    "contact us",
}

JOB_TITLE_BAD_TOKENS = {
    "privacy policy",
    "terms",
    "cookie",
    "newsletter",
    "subscribe",
    "investor",
    "services",
    "whitepaper",
    "case study",
    "blog",
}

DATE_PATTERNS = [
    re.compile(r"\b\d{4}-\d{2}-\d{2}\b"),
    re.compile(r"\b\d{2}/\d{2}/\d{4}\b"),
    re.compile(r"\b\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}\b"),
    re.compile(r"\b[A-Za-z]{3,9}\s+\d{1,2},\s+\d{4}\b"),
]

TYPE_PATTERNS = {
    "full-time": re.compile(r"\bfull[\s-]?time\b", re.IGNORECASE),
    "part-time": re.compile(r"\bpart[\s-]?time\b", re.IGNORECASE),
    "contract": re.compile(r"\bcontract\b", re.IGNORECASE),
    "internship": re.compile(r"\bintern(ship)?\b", re.IGNORECASE),
    "temporary": re.compile(r"\btemporary\b", re.IGNORECASE),
    "remote": re.compile(r"\bremote\b", re.IGNORECASE),
}

TRACKING_QUERY_PREFIXES = (
    "utm_",
    "fbclid",
    "gclid",
    "mc_",
    "hs",
    "__hs",
)



def _clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()



def _normalize_company_from_url(url: str) -> str:
    host = (urlparse(url).netloc or "").lower().strip()
    host = host.split(":")[0]
    if host.startswith("www."):
        host = host[4:]

    labels = [label for label in host.split(".") if label]
    if not labels:
        return ""

    candidate = labels[0]
    if candidate in {"jobs", "careers", "career", "job", "apply", "boards", "job-boards"} and len(labels) > 1:
        candidate = labels[1]

    return candidate.replace("-", " ").replace("_", " ").title()



def _canonicalize_url(url: str) -> str:
    parsed = urlparse(url.strip())
    if parsed.scheme not in {"http", "https"}:
        return url.strip()

    filtered_query = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        key_l = key.lower()
        if key_l.startswith(TRACKING_QUERY_PREFIXES):
            continue
        filtered_query.append((key, value))

    new_query = urlencode(filtered_query, doseq=True)
    normalized_path = re.sub(r"/+", "/", parsed.path or "/")

    return urlunparse(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            normalized_path,
            parsed.params,
            new_query,
            "",
        )
    )



def _is_probably_non_job_page(page_url: str, page_title: str) -> bool:
    url_l = page_url.lower()
    title_l = page_title.lower()

    has_job_hint = any(token in url_l or token in title_l for token in JOB_PAGE_HINTS)
    has_non_job_hint = any(token in url_l or token in title_l for token in NON_JOB_PAGE_KEYWORDS)

    return has_non_job_hint and not has_job_hint



def _has_non_job_ancestor(element: Tag) -> bool:
    for parent in element.parents:
        if not isinstance(parent, Tag):
            continue
        classes = " ".join(parent.get("class", [])).lower()
        element_id = str(parent.get("id") or "").lower()
        haystack = f"{classes} {element_id}"
        if any(keyword in haystack for keyword in NON_JOB_CLASS_KEYWORDS):
            return True
    return False



def _select_text(container: Tag, selectors: list[str]) -> str:
    for selector in selectors:
        node = container.select_one(selector)
        if not node:
            continue
        text = _clean_text(node.get_text(" ", strip=True))
        if text:
            return text
    return ""



def _extract_title(container: Tag) -> str:
    if container.name == "a":
        return _clean_text(container.get_text(" ", strip=True))

    title = _select_text(container, TITLE_SELECTORS)
    if title:
        return title

    fallback = container.find(["h1", "h2", "h3", "h4"])
    if fallback:
        return _clean_text(fallback.get_text(" ", strip=True))

    anchor = container.find("a", href=True)
    if anchor:
        return _clean_text(anchor.get_text(" ", strip=True))

    return ""



def _looks_like_job_link(href: str) -> bool:
    lower = href.lower().strip()
    if not lower or lower.startswith(("#", "javascript:", "mailto:", "tel:")):
        return False
    return any(keyword in lower for keyword in JOB_URL_KEYWORDS)



def _extract_apply_link(container: Tag, page_url: str) -> str:
    if container.name == "a":
        href = str(container.get("href") or "").strip()
        if href:
            resolved = urljoin(page_url, href)
            if _looks_like_job_link(resolved):
                return _canonicalize_url(resolved)

    for selector in APPLY_LINK_SELECTORS:
        for node in container.select(selector):
            if not isinstance(node, Tag):
                continue
            href = str(node.get("href") or node.get("data-apply-url") or "").strip()
            if not href:
                continue
            resolved = urljoin(page_url, href)
            if _looks_like_job_link(resolved):
                return _canonicalize_url(resolved)

    for anchor in container.find_all("a", href=True):
        href = str(anchor.get("href") or "").strip()
        resolved = urljoin(page_url, href)
        if _looks_like_job_link(resolved):
            return _canonicalize_url(resolved)

    return ""



def _extract_posted_date(container: Tag) -> str:
    for selector in POSTED_DATE_SELECTORS:
        node = container.select_one(selector)
        if not node:
            continue

        dt_value = _clean_text(str(node.get("datetime") or node.get("data-posted") or ""))
        if dt_value:
            return dt_value[:10]

        text = _clean_text(node.get_text(" ", strip=True))
        if text:
            for pattern in DATE_PATTERNS:
                match = pattern.search(text)
                if match:
                    return match.group(0)

    text = _clean_text(container.get_text(" ", strip=True))
    for pattern in DATE_PATTERNS:
        match = pattern.search(text)
        if match:
            return match.group(0)

    return ""



def _extract_job_type(container: Tag) -> str:
    text = _select_text(container, TYPE_SELECTORS)
    if text:
        return text

    combined = _clean_text(container.get_text(" ", strip=True))
    for label, pattern in TYPE_PATTERNS.items():
        if pattern.search(combined):
            return label.title()

    return ""



def _extract_location(container: Tag) -> str:
    text = _select_text(container, LOCATION_SELECTORS)
    if text:
        return text

    combined = _clean_text(container.get_text(" ", strip=True))
    loc_match = re.search(
        r"\b(remote|hybrid|onsite|on-site|[A-Za-z\s]+,\s*[A-Za-z\s]+)\b",
        combined,
        flags=re.IGNORECASE,
    )
    if loc_match:
        return _clean_text(loc_match.group(0))

    return ""



def _looks_like_valid_title(title: str) -> bool:
    cleaned = _clean_text(title)
    if not cleaned:
        return False

    lower = cleaned.lower()
    if len(cleaned) < 5 or len(cleaned) > 180:
        return False
    if lower in GENERIC_BAD_TITLES:
        return False
    if any(token in lower for token in JOB_TITLE_BAD_TOKENS):
        return False
    if re.fullmatch(r"[\W\d_]+", cleaned):
        return False

    return True



def is_valid_job_posting(data: dict[str, Any]) -> bool:
    title = _clean_text(str(data.get("title") or ""))
    if not _looks_like_valid_title(title):
        return False

    apply_link = _clean_text(str(data.get("apply_link") or ""))
    job_url = _clean_text(str(data.get("job_url") or ""))
    if not apply_link and not job_url:
        return False

    link = (apply_link or job_url).lower()
    if not any(keyword in link for keyword in JOB_URL_KEYWORDS):
        return False

    if any(noise in link for noise in ["/blog", "/news", "/insights", "/services", "/privacy", "/terms"]):
        return False

    return True



def _candidate_containers(soup: BeautifulSoup) -> list[Tag]:
    containers: list[Tag] = []
    seen: set[int] = set()

    for selector in JOB_CONTAINER_SELECTORS:
        for node in soup.select(selector):
            if not isinstance(node, Tag):
                continue
            token = id(node)
            if token in seen:
                continue
            seen.add(token)
            containers.append(node)

    if containers:
        return containers

    # Fallback: infer containers from likely title nodes.
    title_nodes = soup.select(
        "h1, h2, h3, h4, [data-testid='job-title'], [data-automation-id='jobTitle'], a[href*='job'], a[href*='career']"
    )
    for node in title_nodes:
        if not isinstance(node, Tag):
            continue
        if _has_non_job_ancestor(node):
            continue

        parent = node
        for _ in range(4):
            if parent is None:
                break
            if parent.name in {"li", "article", "tr", "section", "div"}:
                break
            parent = parent.parent if isinstance(parent.parent, Tag) else None

        if not isinstance(parent, Tag):
            if node.name == "a":
                parent = node
            else:
                continue

        token = id(parent)
        if token in seen:
            continue
        seen.add(token)
        containers.append(parent)

    return containers



def extract_job_postings(page_html: str, page_url: str, max_results: int = 200) -> list[dict[str, Any]]:
    """Extract ONLY real job postings from career page HTML."""
    soup = BeautifulSoup(page_html or "", "html.parser")
    page_title = _clean_text(soup.title.get_text(" ", strip=True)) if soup.title else ""

    if _is_probably_non_job_page(page_url, page_title):
        return []

    jobs: list[dict[str, Any]] = []
    seen: set[str] = set()
    company_default = _normalize_company_from_url(page_url)

    for container in _candidate_containers(soup):
        if _has_non_job_ancestor(container):
            continue

        title = _extract_title(container)
        if not _looks_like_valid_title(title):
            continue

        apply_link = _extract_apply_link(container, page_url)
        if not apply_link:
            continue

        company = _select_text(container, COMPANY_SELECTORS) or company_default
        location = _extract_location(container) or "Not specified"
        job_type = _extract_job_type(container) or "Not specified"
        posted_date = _extract_posted_date(container)

        job = {
            "title": title,
            "company": company,
            "location": location,
            "type": job_type,
            "apply_link": apply_link,
            "job_url": apply_link,
            "posted_date": posted_date,
            "source_url": page_url,
            "extracted_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        }

        if not is_valid_job_posting(job):
            continue

        fp = f"{title.lower()}|{_canonicalize_url(apply_link)}"
        if fp in seen:
            continue
        seen.add(fp)

        jobs.append(job)
        if len(jobs) >= max(1, int(max_results)):
            break

    return jobs


__all__ = ["extract_job_postings", "is_valid_job_posting"]
